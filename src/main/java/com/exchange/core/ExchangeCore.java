package com.exchange.core;

import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.config.ExchangeConfiguration;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.core.common.config.SerializationConfiguration;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.CoreWaitStrategy;
import com.exchange.core.common.constant.OrderCommandType;
import com.exchange.core.orderbook.IOrderBook;
import com.exchange.core.processors.*;
import com.exchange.core.processors.journaling.ISerializationProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.ObjLongConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 交易所核心类。
 * 构建配置并启动 disruptor。
 */
@Slf4j
public final class ExchangeCore {

    private final Disruptor<OrderCommand> disruptor;  // Disruptor实例，用于事件处理

    private final RingBuffer<OrderCommand> ringBuffer; // 环形缓冲区，存储订单命令

    @Getter
    private final ExchangeApi api;  // 提供与外部系统交互的API接口

    private final ISerializationProcessor serializationProcessor; // 序列化处理器

    private final ExchangeConfiguration exchangeConfiguration; // 交易所配置

    // 核心只能启动和停止一次
    private boolean started = false;
    private boolean stopped = false;

    // 启用MatcherTradeEvent池
    public static final boolean EVENTS_POOLING = false;

    /**
     * 交易所核心构造函数。
     * @param resultsConsumer       - 自定义消费者，处理已处理的命令
     * @param exchangeConfiguration - 交易所配置
     */
    @Builder
    public ExchangeCore(final ObjLongConsumer<OrderCommand> resultsConsumer,
                        final ExchangeConfiguration exchangeConfiguration) {

        log.debug("从配置构建交易所核心: {}", exchangeConfiguration);

        this.exchangeConfiguration = exchangeConfiguration;

        final PerformanceConfiguration perfCfg = exchangeConfiguration.getPerformanceCfg();

        final int ringBufferSize = perfCfg.getRingBufferSize();

        final ThreadFactory threadFactory = perfCfg.getThreadFactory();

        final CoreWaitStrategy coreWaitStrategy = perfCfg.getWaitStrategy();

        this.disruptor = new Disruptor<>(
                OrderCommand::new,
                ringBufferSize,
                threadFactory,
                ProducerType.MULTI, // 多个网关线程进行写操作
                coreWaitStrategy.getDisruptorWaitStrategyFactory().get());

        this.ringBuffer = disruptor.getRingBuffer();

        this.api = new ExchangeApi(ringBuffer, perfCfg.getBinaryCommandsLz4CompressorFactory().get());

        final IOrderBook.OrderBookFactory orderBookFactory = perfCfg.getOrderBookFactory();

        final int matchingEnginesNum = perfCfg.getMatchingEnginesNum();
        final int riskEnginesNum = perfCfg.getRiskEnginesNum();

        final SerializationConfiguration serializationCfg = exchangeConfiguration.getSerializationCfg();

        // 创建序列化处理器
        serializationProcessor = serializationCfg.getSerializationProcessorFactory().apply(exchangeConfiguration);

        // 创建共享对象池
        final int poolInitialSize = (matchingEnginesNum + riskEnginesNum) * 8;
        final int chainLength = EVENTS_POOLING ? 1024 : 1;
        final SharedPool sharedPool = new SharedPool(poolInitialSize * 4, poolInitialSize, chainLength);

        // 创建并附加异常处理器
        final DisruptorExceptionHandler<OrderCommand> exceptionHandler = new DisruptorExceptionHandler<>("main", (ex, seq) -> {
            log.error("在序列号={}处抛出异常", seq, ex);
            // TODO: 在发布时重新抛出异常
            ringBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
        });

        disruptor.setDefaultExceptionHandler(exceptionHandler);

        // 为CompletableFuture创建相同的CPU插槽
        final ExecutorService loaderExecutor = Executors.newFixedThreadPool(matchingEnginesNum + riskEnginesNum, threadFactory);

        // 开始创建匹配引擎
        final Map<Integer, CompletableFuture<MatchingEngineRouter>> matchingEngineFutures = IntStream.range(0, matchingEnginesNum)
                .boxed()
                .collect(Collectors.toMap(
                        shardId -> shardId,
                        shardId -> CompletableFuture.supplyAsync(
                                () -> new MatchingEngineRouter(shardId, matchingEnginesNum, serializationProcessor, orderBookFactory, sharedPool, exchangeConfiguration),
                                loaderExecutor)));

        // TODO: 在执行时创建处理器？？

        // 开始创建风险引擎
        final Map<Integer, CompletableFuture<RiskEngine>> riskEngineFutures = IntStream.range(0, riskEnginesNum)
                .boxed()
                .collect(Collectors.toMap(
                        shardId -> shardId,
                        shardId -> CompletableFuture.supplyAsync(
                                () -> new RiskEngine(shardId, riskEnginesNum, serializationProcessor, sharedPool, exchangeConfiguration),
                                loaderExecutor)));

        final EventHandler<OrderCommand>[] matchingEngineHandlers = matchingEngineFutures.values().stream()
                .map(CompletableFuture::join)
                .map(mer -> (EventHandler<OrderCommand>) (cmd, seq, eob) -> mer.processOrder(seq, cmd))
                .toArray(ExchangeCore::newEventHandlersArray);

        final Map<Integer, RiskEngine> riskEngines = riskEngineFutures.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().join()));


        final List<TwoStepMasterProcessor> procR1 = new ArrayList<>(riskEnginesNum);
        final List<TwoStepSlaveProcessor> procR2 = new ArrayList<>(riskEnginesNum);

        // 1. 分组处理器 (G)
        final EventHandlerGroup<OrderCommand> afterGrouping =
                disruptor.handleEventsWith((rb, bs) -> new GroupingProcessor(rb, rb.newBarrier(bs), perfCfg, coreWaitStrategy, sharedPool));

        // 2. [日志处理(J)] 与 风险处理(R1) + 匹配引擎 (ME)

        boolean enableJournaling = serializationCfg.isEnableJournaling();
        final EventHandler<OrderCommand> jh = enableJournaling ? serializationProcessor::writeToJournal : null;

        if (enableJournaling) {
            afterGrouping.handleEventsWith(jh);
        }

        riskEngines.forEach((idx, riskEngine) -> afterGrouping.handleEventsWith(
                (rb, bs) -> {
                    final TwoStepMasterProcessor r1 = new TwoStepMasterProcessor(rb, rb.newBarrier(bs), riskEngine::preProcessCommand, exceptionHandler, coreWaitStrategy, "R1_" + idx);
                    procR1.add(r1);
                    return r1;
                }));

        disruptor.after(procR1.toArray(new TwoStepMasterProcessor[0])).handleEventsWith(matchingEngineHandlers);

        // 3. 匹配引擎 (ME) 后的风险释放 (R2)
        final EventHandlerGroup<OrderCommand> afterMatchingEngine = disruptor.after(matchingEngineHandlers);

        riskEngines.forEach((idx, riskEngine) -> afterMatchingEngine.handleEventsWith(
                (rb, bs) -> {
                    final TwoStepSlaveProcessor r2 = new TwoStepSlaveProcessor(rb, rb.newBarrier(bs), riskEngine::handlerRiskRelease, exceptionHandler, "R2_" + idx);
                    procR2.add(r2);
                    return r2;
                }));


        // 4. 结果处理器 (E) 在匹配引擎后
        final EventHandlerGroup<OrderCommand> mainHandlerGroup = enableJournaling
                ? disruptor.after(arraysAddHandler(matchingEngineHandlers, jh))
                : afterMatchingEngine;

        final ResultsHandler resultsHandler = new ResultsHandler(resultsConsumer);

        mainHandlerGroup.handleEventsWith((cmd, seq, eob) -> {
            resultsHandler.onEvent(cmd, seq, eob);
            api.processResult(seq, cmd); // TODO: 慢？(易变操作)
        });

        // 附加从属处理器到主处理器
        IntStream.range(0, riskEnginesNum).forEach(i -> procR1.get(i).setSlaveProcessor(procR2.get(i)));

        try {
            loaderExecutor.shutdown();
            loaderExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public synchronized void startup() {
        if (!started) {
            log.debug("启动 disruptor...");
            disruptor.start();
            started = true;

            serializationProcessor.replayJournalFullAndThenEnableJouraling(exchangeConfiguration.getInitStateCfg(), api);
        }
    }

    private static final EventTranslator<OrderCommand> SHUTDOWN_SIGNAL_TRANSLATOR = (cmd, seq) -> {
        cmd.command = OrderCommandType.SHUTDOWN_SIGNAL;
        cmd.resultCode = CommandResultCode.NEW;
    };

    /**
     * 关闭 disruptor
     */
    public synchronized void shutdown() {
        shutdown(-1, TimeUnit.MILLISECONDS);
    }

    /**
     * 如果交易所核心无法优雅地停止，将抛出 IllegalStateException。
     *
     * @param timeout  等待所有事件处理的时间。<code>-1</code> 表示无限等待
     * @param timeUnit 超时单位
     */
    public synchronized void shutdown(final long timeout, final TimeUnit timeUnit) {
        if (!stopped) {
            stopped = true;
            // TODO: 首先停止接受新事件
            try {
                log.info("关闭 disruptor...");
                ringBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
                disruptor.shutdown(timeout, timeUnit);
                log.info("Disruptor 停止");
            } catch (TimeoutException e) {
                throw new IllegalStateException("无法优雅地停止 disruptor，可能未处理所有事件。");
            }
        }
    }

    private static EventHandler<OrderCommand>[] arraysAddHandler(EventHandler<OrderCommand>[] handlers, EventHandler<OrderCommand> extraHandler) {
        final EventHandler<OrderCommand>[] result = Arrays.copyOf(handlers, handlers.length + 1);
        result[handlers.length] = extraHandler;
        return result;
    }

    @SuppressWarnings(value = {"unchecked"})
    private static EventHandler<OrderCommand>[] newEventHandlersArray(int size) {
        return new EventHandler[size];
    }
}
