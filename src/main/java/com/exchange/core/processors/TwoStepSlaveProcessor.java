package com.exchange.core.processors;

import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.constant.CoreWaitStrategy;
import com.lmax.disruptor.*;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class TwoStepSlaveProcessor implements EventProcessor {

    // 定义常量，用于表示处理器的不同状态
    private static final int IDLE = 0;  // 空闲状态
    private static final int HALTED = IDLE + 1;  // 停止状态
    private static final int RUNNING = HALTED + 1;  // 运行状态

    // 当前状态的原子变量
    private final AtomicInteger running = new AtomicInteger(IDLE);

    // 数据提供者，提供事件数据
    private final DataProvider<OrderCommand> dataProvider;
    
    // 序列屏障，确保顺序访问事件
    private final SequenceBarrier sequenceBarrier;

    // 用于等待和处理事件的帮助工具
    private final WaitSpinningHelper waitSpinningHelper;

    // 事件处理器，用于处理实际的事件数据
    private final SimpleEventHandler eventHandler;

    // 事件序列
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    // 异常处理器
    private final ExceptionHandler<? super OrderCommand> exceptionHandler;

    // 处理器的名称
    private final String name;

    // 下一个需要处理的序列号
    private long nextSequence = -1;

    /**
     * 构造函数，初始化必要的字段。
     * @param ringBuffer           事件环形缓冲区
     * @param sequenceBarrier      序列屏障，确保顺序处理
     * @param eventHandler         事件处理器
     * @param exceptionHandler     异常处理器
     * @param name                 处理器名称
     */
    public TwoStepSlaveProcessor(final RingBuffer<OrderCommand> ringBuffer,
                                 final SequenceBarrier sequenceBarrier,
                                 final SimpleEventHandler eventHandler,
                                 final ExceptionHandler<? super OrderCommand> exceptionHandler,
                                 final String name) {
        this.dataProvider = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.waitSpinningHelper = new WaitSpinningHelper(ringBuffer, sequenceBarrier, 0, CoreWaitStrategy.SECOND_STEP_NO_WAIT);
        this.eventHandler = eventHandler;
        this.exceptionHandler = exceptionHandler;
        this.name = name;
    }

    /**
     * 获取事件处理的序列。
     */
    @Override
    public Sequence getSequence() {
        return sequence;
    }

    /**
     * 停止事件处理器。
     */
    @Override
    public void halt() {
        running.set(HALTED);  // 设置为停止状态
        sequenceBarrier.alert();  // 发出警告，通知其它处理器
    }

    /**
     * 判断当前事件处理器是否正在运行。
     */
    @Override
    public boolean isRunning() {
        return running.get() != IDLE;  // 如果不是空闲状态，说明正在运行
    }

    /**
     * 启动事件处理。该方法会检查当前状态，如果已处于运行状态，则抛出异常。
     * 如果状态是空闲，则开始处理事件。
     */
    @Override
    public void run() {
        if (running.compareAndSet(IDLE, RUNNING)) {
            sequenceBarrier.clearAlert();  // 清除警告，准备开始处理
        } else if (running.get() == RUNNING) {
            throw new IllegalStateException("Thread is already running (S)");
        }

        // 初始化下一个序列号
        nextSequence = sequence.get() + 1L;
    }

    /**
     * 处理事件的循环，直到处理到指定的序列号。
     * @param processUpToSequence 直到该序列号为止的事件将被处理
     */
    public void handlingCycle(final long processUpToSequence) {
        while (true) {
            OrderCommand event = null;
            try {
                // 等待直到可以处理下一个事件
                long availableSequence = waitSpinningHelper.tryWaitFor(nextSequence);

                // 批量处理事件
                while (nextSequence <= availableSequence && nextSequence < processUpToSequence) {
                    event = dataProvider.get(nextSequence);  // 获取事件数据
                    eventHandler.onEvent(nextSequence, event);  // 处理事件
                    nextSequence++;  // 处理下一个序列号的事件
                }

                // 如果已处理完指定范围的事件，退出处理循环
                if (nextSequence == processUpToSequence) {
                    sequence.set(processUpToSequence - 1);  // 更新序列号
                    waitSpinningHelper.signalAllWhenBlocking();  // 通知其它处理器
                    return;
                }

            } catch (final Throwable ex) {
                exceptionHandler.handleEventException(ex, nextSequence, event);  // 处理异常
                sequence.set(nextSequence);  // 更新序列号
                waitSpinningHelper.signalAllWhenBlocking();  // 通知其它处理器
                nextSequence++;  // 继续处理下一个事件
            }
        }
    }

    /**
     * 返回该处理器的字符串表示。
     */
    @Override
    public String toString() {
        return "TwoStepSlaveProcessor{" + name + "}";
    }
}
