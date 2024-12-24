package com.exchange.core.utils;

import com.exchange.core.processors.TwoStepSlaveProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义线程工厂，支持线程亲和性配置（将线程绑定到特定的 CPU 核心）。
 * <p>
 * 该类确保创建的线程会根据指定的亲和性模式绑定到某些 CPU 核心。
 * - 可以根据物理核心或逻辑核心设置线程亲和性。
 * - 支持避免重复预定同一线程的 CPU 亲和性。
 */
@Slf4j
@RequiredArgsConstructor
public final class AffinityThreadFactory implements ThreadFactory {

    // 存储已请求的亲和性锁，避免重复预定同一个线程的 CPU 核心
    private final Set<Object> affinityReservations = new HashSet<>();

    // 线程亲和性模式，控制如何绑定线程到 CPU 核心
    private final ThreadAffinityMode threadAffinityMode;

    // 用于线程命名的计数器
    private static AtomicInteger threadsCounter = new AtomicInteger();

    /**
     * 创建一个新线程并根据配置的亲和性模式进行绑定。
     *
     * @param runnable 线程执行的任务
     * @return 新创建的线程
     */
    @Override
    public synchronized Thread newThread(@NotNull Runnable runnable) {

        // 如果线程亲和性禁用，使用默认的线程工厂创建线程
        if (threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_DISABLE) {
            return Executors.defaultThreadFactory().newThread(runnable);
        }

        // 如果任务是 TwoStepSlaveProcessor 类型的任务，不进行 CPU 亲和性绑定
        if (runnable instanceof TwoStepSlaveProcessor) {
            log.debug("Skip pinning slave processor: {}", runnable);
            return Executors.defaultThreadFactory().newThread(runnable);
        }

        // 如果该任务已经被预定了 CPU 亲和性，避免重复绑定
        if (affinityReservations.contains(runnable)) {
            log.warn("Task {} was already pinned", runnable);
            // 返回默认线程，不进行重复预定
//            return Executors.defaultThreadFactory().newThread(runnable);
        }

        // 将任务添加到已预定的任务列表中
        affinityReservations.add(runnable);

        // 创建一个新的线程，并执行带有 CPU 亲和性的任务
        return new Thread(() -> executePinned(runnable));
    }

    /**
     * 执行绑定了 CPU 亲和性的任务。
     *
     * @param runnable 执行的任务
     */
    private void executePinned(@NotNull Runnable runnable) {

        try (final AffinityLock lock = getAffinityLockSync()) {

            // 获取一个唯一的线程 ID，并设置线程名称
            final int threadId = threadsCounter.incrementAndGet();
            Thread.currentThread().setName(String.format("Thread-AF-%d-cpu%d", threadId, lock.cpuId()));

            log.debug("{} will be running on thread={} pinned to cpu {}",
                    runnable, Thread.currentThread().getName(), lock.cpuId());

            // 执行任务
            runnable.run();

        } finally {
            log.debug("Removing cpu lock/reservation from {}", runnable);

            // 释放线程的 CPU 亲和性锁
            synchronized (this) {
                affinityReservations.remove(runnable);
            }
        }
    }

    /**
     * 获取与线程绑定的 CPU 亲和性锁。
     *
     * @return AffinityLock
     */
    private synchronized AffinityLock getAffinityLockSync() {
        // 根据亲和性模式选择获取物理核心锁还是逻辑核心锁
        return threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE
                ? AffinityLock.acquireCore()  // 获取物理核心锁
                : AffinityLock.acquireLock(); // 获取逻辑核心锁
    }

    /**
     * 线程亲和性模式，定义了如何将线程绑定到 CPU 核心。
     */
    public enum ThreadAffinityMode {
        // 线程绑定到物理核心
        THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE,

        // 线程绑定到逻辑核心
        THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE,

        // 禁用线程亲和性
        THREAD_AFFINITY_DISABLE
    }
}
