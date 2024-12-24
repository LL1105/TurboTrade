package com.exchange.core.common.constant;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.function.Supplier;

/**
 * 定义了 Disruptor 模式中的核心等待策略。
 * 每种策略控制消费者线程在等待新事件时的行为。
 * 
 * - BUSY_SPIN：线程在等待事件时不停地自旋，适用于低延迟、高吞吐量场景。
 * - YIELDING：线程在等待时会将 CPU 控制权交给其他线程，适用于中等吞吐量场景。
 * - BLOCKING：线程会被阻塞，直到事件可用，适用于较低吞吐量的场景。
 * - SECOND_STEP_NO_WAIT：特殊情况，不进行等待。
 */
@RequiredArgsConstructor
public enum CoreWaitStrategy {

    // 自旋等待策略：线程在等待下一个事件时会持续检查，适用于低延迟、高吞吐量的场景。
    // 如果事件没有立即可用，线程可能会浪费 CPU 周期。
    BUSY_SPIN(BusySpinWaitStrategy::new, false, false),

    // 让出等待策略：线程在等待时会将 CPU 使用权交给其他线程，减少 CPU 浪费，适用于中等吞吐量场景。
    YIELDING(YieldingWaitStrategy::new, true, false),

    // 阻塞等待策略：线程在没有事件时会被挂起，直到事件到达才会被唤醒，适用于吞吐量要求较低的场景。
    BLOCKING(BlockingWaitStrategy::new, false, true),

    // 特殊情况：不进行任何等待，通常用于某些特殊的场景。
    SECOND_STEP_NO_WAIT(null, false, false);

    // Disruptor 等待策略的工厂方法，用于创建相应的等待策略实例
    @Getter
    private final Supplier<WaitStrategy> disruptorWaitStrategyFactory;

    // 是否使用让出策略（yield），适用于减少 CPU 占用的场景
    @Getter
    private final boolean yield;

    // 是否使用阻塞策略，适用于需要等待和唤醒的场景
    @Getter
    private final boolean block;
}
