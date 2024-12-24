package com.exchange.core.processors;

import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.constant.CoreWaitStrategy;
import com.exchange.core.common.constant.OrderCommandType;
import com.lmax.disruptor.*;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class TwoStepMasterProcessor implements EventProcessor {
    // 定义处理器的状态常量
    private static final int IDLE = 0;        // 空闲状态
    private static final int HALTED = IDLE + 1; // 停止状态
    private static final int RUNNING = HALTED + 1; // 运行状态

    private static final int MASTER_SPIN_LIMIT = 5000; // 主处理器的自旋限制

    private final AtomicInteger running = new AtomicInteger(IDLE); // 记录当前处理器的状态
    private final DataProvider<OrderCommand> dataProvider; // 数据提供者，提供 OrderCommand 数据
    private final SequenceBarrier sequenceBarrier; // 序列屏障，用于同步事件处理
    private final WaitSpinningHelper waitSpinningHelper; // 自旋等待帮助工具
    private final SimpleEventHandler eventHandler; // 事件处理器，用于处理具体事件
    private final ExceptionHandler<OrderCommand> exceptionHandler; // 异常处理器，用于处理事件过程中的异常
    private final String name; // 处理器名称
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE); // 当前事件序列

    @Setter
    private TwoStepSlaveProcessor slaveProcessor; // 从处理器（Slave Processor）

    /**
     * 构造函数，初始化所有需要的参数。
     *
     * @param ringBuffer 事件存储的 RingBuffer
     * @param sequenceBarrier 用于同步的序列屏障
     * @param eventHandler 事件处理器
     * @param exceptionHandler 异常处理器
     * @param coreWaitStrategy 等待策略
     * @param name 处理器名称
     */
    public TwoStepMasterProcessor(final RingBuffer<OrderCommand> ringBuffer,
                                  final SequenceBarrier sequenceBarrier,
                                  final SimpleEventHandler eventHandler,
                                  final ExceptionHandler<OrderCommand> exceptionHandler,
                                  final CoreWaitStrategy coreWaitStrategy,
                                  final String name) {
        this.dataProvider = ringBuffer; // 初始化数据提供者
        this.sequenceBarrier = sequenceBarrier; // 初始化序列屏障
        this.waitSpinningHelper = new WaitSpinningHelper(ringBuffer, sequenceBarrier, MASTER_SPIN_LIMIT, coreWaitStrategy); // 初始化自旋等待工具
        this.eventHandler = eventHandler; // 初始化事件处理器
        this.exceptionHandler = exceptionHandler; // 初始化异常处理器
        this.name = name; // 设置处理器名称
    }

    /**
     * 获取当前处理器的序列
     *
     * @return 当前事件的序列
     */
    @Override
    public Sequence getSequence() {
        return sequence;
    }

    /**
     * 停止当前事件处理器
     */
    @Override
    public void halt() {
        running.set(HALTED); // 设置为停止状态
        sequenceBarrier.alert(); // 触发序列屏障，通知停止
    }

    /**
     * 判断当前处理器是否处于运行状态
     *
     * @return true 表示运行中，false 表示未运行
     */
    @Override
    public boolean isRunning() {
        return running.get() != IDLE; // 只有不处于空闲状态时，才表示正在运行
    }

    /**
     * 启动事件处理器，开始处理事件。
     *
     * @throws IllegalStateException 如果该处理器已在运行中
     */
    @Override
    public void run() {
        if (running.compareAndSet(IDLE, RUNNING)) { // 如果当前是空闲状态，则将其设置为运行状态
            sequenceBarrier.clearAlert(); // 清除序列屏障的警告

            try {
                // 如果处理器正在运行，则进入事件处理过程
                if (running.get() == RUNNING) {
                    processEvents();
                }
            } finally {
                running.set(IDLE); // 处理完成后，将状态设置为 IDLE
            }
        }
    }

    /**
     * 事件处理的核心方法，处理从 RingBuffer 获取的事件。
     */
    private void processEvents() {
        // 设置当前线程的名称，便于调试和日志记录
        Thread.currentThread().setName("Thread-" + name);

        long nextSequence = sequence.get() + 1L; // 获取下一个事件的序列号
        long currentSequenceGroup = 0; // 当前事件组，初始化为 0

        // 等待直到从处理器准备好
        while (!slaveProcessor.isRunning()) {
            Thread.yield(); // 如果从处理器没有准备好，则进行自旋等待
        }

        while (true) {
            OrderCommand cmd = null;
            try {
                // 等待直到有事件可用，并检查是否已达到可用序列
                final long availableSequence = waitSpinningHelper.tryWaitFor(nextSequence);

                // 如果下一个序列号小于或等于可用序列号，表示可以处理事件
                if (nextSequence <= availableSequence) {
                    // 处理所有可用的事件
                    while (nextSequence <= availableSequence) {
                        cmd = dataProvider.get(nextSequence); // 从 RingBuffer 获取事件

                        // 如果事件组改变，发布进度并触发从处理器继续处理
                        if (cmd.eventsGroup != currentSequenceGroup) {
                            publishProgressAndTriggerSlaveProcessor(nextSequence);
                            currentSequenceGroup = cmd.eventsGroup; // 更新当前事件组
                        }

                        // 调用事件处理器处理当前事件
                        boolean forcedPublish = eventHandler.onEvent(nextSequence, cmd);
                        nextSequence++;

                        // 如果强制发布事件，更新序列并通知其他线程
                        if (forcedPublish) {
                            sequence.set(nextSequence - 1); // 更新序列
                            waitSpinningHelper.signalAllWhenBlocking(); // 通知所有等待的线程
                        }

                        // 如果事件是关闭信号，触发关闭操作
                        if (cmd.command == OrderCommandType.SHUTDOWN_SIGNAL) {
                            // 确保所有事件都已处理并与 RingBuffer 对齐，才能正确关闭
                            publishProgressAndTriggerSlaveProcessor(nextSequence);
                        }
                    }
                    sequence.set(availableSequence); // 更新序列
                    waitSpinningHelper.signalAllWhenBlocking(); // 通知所有等待的线程
                }
            } catch (final AlertException ex) {
                // 如果遇到警告异常，表示需要退出处理
                if (running.get() != RUNNING) {
                    break;
                }
            } catch (final Throwable ex) {
                // 处理其他异常
                exceptionHandler.handleEventException(ex, nextSequence, cmd);
                sequence.set(nextSequence); // 更新序列
                waitSpinningHelper.signalAllWhenBlocking(); // 通知等待线程
                nextSequence++; // 增加序列
            }
        }
    }

    /**
     * 发布当前处理进度，并触发从处理器继续执行。
     *
     * @param nextSequence 下一个序列号
     */
    private void publishProgressAndTriggerSlaveProcessor(final long nextSequence) {
        sequence.set(nextSequence - 1); // 更新序列
        waitSpinningHelper.signalAllWhenBlocking(); // 通知其他线程
        slaveProcessor.handlingCycle(nextSequence); // 触发从处理器处理事件
    }

    /**
     * 返回当前处理器的字符串表示形式。
     *
     * @return 处理器的名称
     */
    @Override
    public String toString() {
        return "TwoStepMasterProcessor{" + name + "}";
    }
}
