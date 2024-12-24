package com.exchange.core.processors;

import com.exchange.core.common.MatcherTradeEvent;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.CoreWaitStrategy;
import com.exchange.core.common.constant.OrderCommandType;
import com.lmax.disruptor.*;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

import static com.exchange.core.ExchangeCore.EVENTS_POOLING;


@Slf4j
public final class GroupingProcessor implements EventProcessor {

    // 定义处理器的状态常量
    private static final int IDLE = 0;        // 空闲状态
    private static final int HALTED = IDLE + 1; // 暂停状态
    private static final int RUNNING = HALTED + 1; // 运行状态

    private static final int GROUP_SPIN_LIMIT = 1000; // 每次自旋等待的最大次数

    // L2 数据发布的时间间隔，单位是纳秒 (10 毫秒)
    private static final int L2_PUBLISH_INTERVAL_NS = 10_000_000;

    // 当前处理器的状态，使用原子变量来确保线程安全
    private final AtomicInteger running = new AtomicInteger(IDLE);

    // RingBuffer 和序列屏障，用于事件的生产和消费
    private final RingBuffer<OrderCommand> ringBuffer;
    private final SequenceBarrier sequenceBarrier;

    // 自旋等待帮助工具
    private final WaitSpinningHelper waitSpinningHelper;

    // 当前处理的序列
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    // 共享池，用于存储处理后的交易事件链
    private final SharedPool sharedPool;

    // 配置：每组消息的最大数量和最大持续时间
    private final int msgsInGroupLimit;
    private final long maxGroupDurationNs;

    /**
     * 构造器：初始化 GroupingProcessor
     *
     * @param ringBuffer 事件缓冲区
     * @param sequenceBarrier 序列屏障，用于事件同步
     * @param perfCfg 性能配置
     * @param coreWaitStrategy 等待策略
     * @param sharedPool 共享池
     */
    public GroupingProcessor(RingBuffer<OrderCommand> ringBuffer,
                             SequenceBarrier sequenceBarrier,
                             PerformanceConfiguration perfCfg,
                             CoreWaitStrategy coreWaitStrategy,
                             SharedPool sharedPool) {

        // 验证每组消息的最大数量不得超过 RingBuffer 的四分之一
        if (perfCfg.getMsgsInGroupLimit() > perfCfg.getRingBufferSize() / 4) {
            throw new IllegalArgumentException("msgsInGroupLimit should be less than quarter ringBufferSize");
        }

        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.waitSpinningHelper = new WaitSpinningHelper(ringBuffer, sequenceBarrier, GROUP_SPIN_LIMIT, coreWaitStrategy);
        this.msgsInGroupLimit = perfCfg.getMsgsInGroupLimit();
        this.maxGroupDurationNs = perfCfg.getMaxGroupDurationNs();
        this.sharedPool = sharedPool;
    }

    @Override
    public Sequence getSequence() {
        return sequence; // 返回当前处理的序列
    }

    @Override
    public void halt() {
        running.set(HALTED); // 设置为暂停状态
        sequenceBarrier.alert(); // 发出警告，阻塞的消费者会收到提醒
    }

    @Override
    public boolean isRunning() {
        return running.get() != IDLE; // 检查处理器是否在运行
    }

    /**
     * 启动事件处理
     * 如果当前处理器已在运行，抛出 IllegalStateException
     */
    @Override
    public void run() {
        if (running.compareAndSet(IDLE, RUNNING)) { // 如果当前是空闲状态，切换为运行状态
            sequenceBarrier.clearAlert(); // 清除警报
            try {
                if (running.get() == RUNNING) {
                    processEvents(); // 处理事件
                }
            } finally {
                running.set(IDLE); // 处理完事件后恢复为空闲状态
            }
        } else {
            // 如果处理器已经在运行，抛出异常
            if (running.get() == RUNNING) {
                throw new IllegalStateException("Thread is already running");
            }
        }
    }

    /**
     * 事件处理方法
     * 从 RingBuffer 中获取事件并处理，根据配置进行消息分组、触发数据请求等操作
     */
    private void processEvents() {
        long nextSequence = sequence.get() + 1L; // 下一个处理的事件序列
        long groupCounter = 0; // 消息组的计数器
        long msgsInGroup = 0;  // 当前组中的消息数量
        long groupLastNs = 0;  // 记录上次切换消息组的时间
        long l2dataLastNs = 0; // 记录上次触发 L2 数据请求的时间
        boolean triggerL2DataRequest = false; // 标志位，是否触发 L2 数据请求

        // 交易事件链的目标长度
        final int tradeEventChainLengthTarget = sharedPool.getChainLength();
        MatcherTradeEvent tradeEventHead = null;
        MatcherTradeEvent tradeEventTail = null;
        int tradeEventCounter = 0; // 交易事件计数器

        boolean groupingEnabled = true; // 是否启用分组功能

        while (true) {
            try {

                // 等待并获取下一个可用的序列
                long availableSequence = waitSpinningHelper.tryWaitFor(nextSequence);

                if (nextSequence <= availableSequence) {
                    while (nextSequence <= availableSequence) {
                        final OrderCommand cmd = ringBuffer.get(nextSequence); // 获取命令

                        nextSequence++; // 更新序列

                        // 处理分组控制命令
                        if (cmd.command == OrderCommandType.GROUPING_CONTROL) {
                            groupingEnabled = cmd.orderId == 1; // 如果命令的 orderId 为 1，启用分组
                            cmd.resultCode = CommandResultCode.SUCCESS;
                        }

                        if (!groupingEnabled) {
                            // 如果禁用分组，清空命令中的 matcherEvent 和 marketData
                            cmd.matcherEvent = null;
                            cmd.marketData = null;
                            continue;
                        }

                        // 某些命令（如重置、持久化等）会触发 R2 阶段
                        if (cmd.command == OrderCommandType.RESET
                                || cmd.command == OrderCommandType.PERSIST_STATE_MATCHING
                                || cmd.command == OrderCommandType.GROUPING_CONTROL) {
                            groupCounter++;  // 增加组计数器
                            msgsInGroup = 0;  // 重置当前组中的消息数量
                        }

                        // 报告或二进制命令也会触发 R2 阶段，且仅对于最后一条消息有效
                        if ((cmd.command == OrderCommandType.BINARY_DATA_COMMAND || cmd.command == OrderCommandType.BINARY_DATA_QUERY) && cmd.symbol == -1) {
                            groupCounter++; // 增加组计数器
                            msgsInGroup = 0; // 重置消息数量
                        }

                        cmd.eventsGroup = groupCounter; // 设置当前命令所属的事件组

                        // 如果触发了 L2 数据请求标志，设置标志位
                        if (triggerL2DataRequest) {
                            triggerL2DataRequest = false;
                            cmd.serviceFlags = 1;
                        } else {
                            cmd.serviceFlags = 0;
                        }

                        // 清理关联的事件（如交易事件）
                        if (EVENTS_POOLING && cmd.matcherEvent != null) {

                            // 更新尾节点
                            if (tradeEventTail == null) {
                                tradeEventHead = cmd.matcherEvent;
                            } else {
                                tradeEventTail.nextEvent = cmd.matcherEvent;
                            }

                            tradeEventTail = cmd.matcherEvent;
                            tradeEventCounter++;

                            // 找到链条的最后一个元素，更新尾节点
                            while (tradeEventTail.nextEvent != null) {
                                tradeEventTail = tradeEventTail.nextEvent;
                                tradeEventCounter++;
                            }

                            // 如果链条长度达到目标，提交到共享池
                            if (tradeEventCounter >= tradeEventChainLengthTarget) {
                                tradeEventCounter = 0;
                                sharedPool.putChain(tradeEventHead); // 提交交易事件链
                                tradeEventTail = null;
                                tradeEventHead = null;
                            }

                        }
                        cmd.matcherEvent = null;

                        // TODO: 进一步处理命令，收集到共享缓冲区
                        cmd.marketData = null;

                        msgsInGroup++; // 增加当前组中的消息数量

                        // 如果当前组的消息数量达到上限，切换到下一个组
                        if (msgsInGroup >= msgsInGroupLimit && cmd.command != OrderCommandType.PERSIST_STATE_RISK) {
                            groupCounter++; // 增加组计数器
                            msgsInGroup = 0; // 重置当前组的消息数量
                        }

                    }
                    sequence.set(availableSequence); // 更新序列号
                    waitSpinningHelper.signalAllWhenBlocking(); // 通知所有等待线程
                    groupLastNs = System.nanoTime() + maxGroupDurationNs; // 更新上次分组切换的时间

                } else {
                    final long t = System.nanoTime();
                    // 如果当前组非空且已超过最大分组时间，切换到下一个组
                    if (msgsInGroup > 0 && t > groupLastNs) {
                        groupCounter++;
                        msgsInGroup = 0;
                    }

                    // 如果已超过 L2 数据请求时间间隔，触发 L2 数据请求
                    if (t > l2dataLastNs) {
                        l2dataLastNs = t + L2_PUBLISH_INTERVAL_NS;
                        triggerL2DataRequest = true;
                    }
                }

            } catch (final AlertException ex) {
                if (running.get() != RUNNING) {
                    break; // 如果处理器已经暂停，则退出
                }
            } catch (final Throwable ex) {
                sequence.set(nextSequence); // 更新序列号
                waitSpinningHelper.signalAllWhenBlocking(); // 通知所有等待线程
                nextSequence++; // 更新下一个处理的事件序列
            }
        }
    }

    @Override
    public String toString() {
        return "GroupingProcessor{" +
                "GL=" + msgsInGroupLimit + // 显示每组消息的最大数量
                '}';
    }
}
