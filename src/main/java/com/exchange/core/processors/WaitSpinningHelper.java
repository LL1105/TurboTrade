package com.exchange.core.processors;

import com.exchange.core.common.constant.CoreWaitStrategy;
import com.exchange.core.utils.ReflectionUtils;
import com.lmax.disruptor.*;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

@Slf4j
public final class WaitSpinningHelper {

    // SequenceBarrier 用于提供序列同步
    private final SequenceBarrier sequenceBarrier;
    // Sequencer 用于提供 Disruptor 中的数据序列号管理
    private final Sequencer sequencer;

    // 自旋限制，决定了自旋等待的次数
    private final int spinLimit;
    // 如果启用 yield，表示自旋的数量达到某个限制后会调用 Thread.yield()
    private final int yieldLimit;

    // 是否使用阻塞模式，阻塞模式下使用 Disruptor 的锁
    private final boolean block;
    // 阻塞模式下的 Disruptor 等待策略
    private final BlockingWaitStrategy blockingDisruptorWaitStrategy;
    // 用于同步的锁
    private final Lock lock;
    // 线程通知条件，配合锁使用
    private final Condition processorNotifyCondition;

    /**
     * 构造函数，初始化 WaitSpinningHelper 的各种参数。
     * 
     * @param ringBuffer 事件存储的 RingBuffer
     * @param sequenceBarrier 用于事件同步的序列屏障
     * @param spinLimit 自旋等待的最大次数
     * @param waitStrategy 等待策略
     */
    public <T> WaitSpinningHelper(RingBuffer<T> ringBuffer, SequenceBarrier sequenceBarrier, int spinLimit, CoreWaitStrategy waitStrategy) {
        this.sequenceBarrier = sequenceBarrier; // 设置序列屏障
        this.spinLimit = spinLimit; // 设置自旋限制
        this.sequencer = extractSequencer(ringBuffer); // 提取 RingBuffer 的 Sequencer
        this.yieldLimit = waitStrategy.isYield() ? spinLimit / 2 : 0; // 如果启用 yield，则设置 yield 限制

        // 判断是否使用阻塞模式
        this.block = waitStrategy.isBlock();
        if (block) {
            // 如果启用阻塞模式，从 Disruptor 中反射获取阻塞等待策略的锁和通知条件
            this.blockingDisruptorWaitStrategy = ReflectionUtils.extractField(AbstractSequencer.class, (AbstractSequencer) sequencer, "waitStrategy");
            this.lock = ReflectionUtils.extractField(BlockingWaitStrategy.class, blockingDisruptorWaitStrategy, "lock");
            this.processorNotifyCondition = ReflectionUtils.extractField(BlockingWaitStrategy.class, blockingDisruptorWaitStrategy, "processorNotifyCondition");
        } else {
            // 非阻塞模式下，lock 和 processorNotifyCondition 为 null
            this.blockingDisruptorWaitStrategy = null;
            this.lock = null;
            this.processorNotifyCondition = null;
        }
    }

    /**
     * 尝试等待直到指定的序列号可用。
     * 
     * @param seq 需要等待的目标序列号
     * @return 可用的序列号
     * @throws AlertException 如果发生警告异常
     * @throws InterruptedException 如果等待中被中断
     */
    public long tryWaitFor(final long seq) throws AlertException, InterruptedException {
        // 检查序列屏障是否有警告
        sequenceBarrier.checkAlert();

        // 初始化自旋计数
        long spin = spinLimit;
        long availableSequence;
        
        // 当可用序列号小于目标序列号，并且剩余自旋次数大于 0 时，进入循环
        while ((availableSequence = sequenceBarrier.getCursor()) < seq && spin > 0) {
            // 如果自旋次数小于 yield 限制并且大于 1，则调用 Thread.yield() 让出 CPU
            if (spin < yieldLimit && spin > 1) {
                Thread.yield();
            } else if (block) {
                // 如果启用阻塞模式，使用锁等待
                lock.lock();
                try {
                    // 再次检查序列屏障的警告
                    sequenceBarrier.checkAlert();
                    // 如果序列屏障没有进展，则进入阻塞等待
                    if (availableSequence == sequenceBarrier.getCursor()) {
                        processorNotifyCondition.await();
                    }
                } finally {
                    // 释放锁
                    lock.unlock();
                }
            }
            // 减少自旋次数
            spin--;
        }

        // 如果可用的序列号小于目标序列号，返回可用序列号，否则返回已发布的最大序列号
        return (availableSequence < seq)
                ? availableSequence
                : sequencer.getHighestPublishedSequence(seq, availableSequence);
    }

    /**
     * 在阻塞模式下，通知所有等待的线程
     */
    public void signalAllWhenBlocking() {
        if (block) {
            // 如果启用了阻塞模式，则调用阻塞等待策略中的 signalAllWhenBlocking 方法
            blockingDisruptorWaitStrategy.signalAllWhenBlocking();
        }
    }

    /**
     * 通过反射提取 RingBuffer 中的 Sequencer。
     * 
     * @param ringBuffer RingBuffer 实例
     * @param <T> 事件类型
     * @return RingBuffer 中的 Sequencer
     */
    private static <T> Sequencer extractSequencer(RingBuffer<T> ringBuffer) {
        try {
            // 使用反射获取 RingBuffer 中的 "sequencer" 字段
            final Field f = ReflectionUtils.getField(RingBuffer.class, "sequencer");
            f.setAccessible(true);
            return (Sequencer) f.get(ringBuffer);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // 如果反射获取失败，抛出异常
            throw new IllegalStateException("Can not access Disruptor internals: ", e);
        }
    }
}
