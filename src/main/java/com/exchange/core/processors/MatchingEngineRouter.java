package com.exchange.core.processors;

import com.exchange.core.common.CoreSymbolSpecification;
import com.exchange.core.common.api.binary.BatchAddAccountsCommand;
import com.exchange.core.common.api.binary.BatchAddSymbolsCommand;
import com.exchange.core.common.api.reports.ReportQuery;
import com.exchange.core.common.api.reports.ReportResult;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.config.*;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.OrderCommandType;
import com.exchange.core.common.constant.SymbolType;
import com.exchange.core.orderbook.IOrderBook;
import com.exchange.core.orderbook.OrderBookEventsHelper;
import com.exchange.core.processors.journaling.DiskSerializationProcessorConfiguration;
import com.exchange.core.processors.journaling.ISerializationProcessor;
import com.exchange.core.utils.SerializationUtils;
import com.exchange.core.utils.UnsafeUtils;
import exchange.core2.collections.objpool.ObjectsPool;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;

/**
 * 匹配引擎路由器，用于处理各种与订单簿相关的命令和消息。
 */
@Slf4j
@Getter
public final class MatchingEngineRouter implements WriteBytesMarshallable {

    // 模块标识符，表示撮合引擎的标识符
    public static final ISerializationProcessor.SerializedModuleType MODULE_ME =
            ISerializationProcessor.SerializedModuleType.MATCHING_ENGINE_ROUTER;

    // 匹配引擎的状态
    private final BinaryCommandsProcessor binaryCommandsProcessor;

    // symbol -> 订单簿映射
    private final IntObjectHashMap<IOrderBook> orderBooks;

    // 订单簿工厂
    private final IOrderBook.OrderBookFactory orderBookFactory;

    // 订单簿事件帮助器
    private final OrderBookEventsHelper eventsHelper;

    // 本地对象池，用于存储订单簿
    private final ObjectsPool objectsPool;

    // 根据 symbolId 进行分片
    private final int shardId;
    private final long shardMask;

    // 交易所标识符
    private final String exchangeId;

    // 存储路径
    private final Path folder;

    // 是否启用保证金交易
    private final boolean cfgMarginTradingEnabled;

    // 是否每个命令都发送 L2 数据
    private final boolean cfgSendL2ForEveryCmd;

    // L2 数据刷新深度
    private final int cfgL2RefreshDepth;

    // 序列化处理器
    private final ISerializationProcessor serializationProcessor;

    // 日志配置
    private final LoggingConfiguration loggingCfg;

    // 是否启用调试日志
    private final boolean logDebug;

    /**
     * 构造方法，初始化匹配引擎路由器。
     *
     * @param shardId 区分不同分片的标识符
     * @param numShards 总分片数
     * @param serializationProcessor 序列化处理器
     * @param orderBookFactory 订单簿工厂
     * @param sharedPool 共享池
     * @param exchangeCfg 交易所配置
     */
    public MatchingEngineRouter(final int shardId,
                                final long numShards,
                                final ISerializationProcessor serializationProcessor,
                                final IOrderBook.OrderBookFactory orderBookFactory,
                                final SharedPool sharedPool,
                                final ExchangeConfiguration exchangeCfg) {

        // 确保分片数是 2 的幂
        if (Long.bitCount(numShards) != 1) {
            throw new IllegalArgumentException("无效的分片数 " + numShards + " - 必须是 2 的幂");
        }

        final InitialStateConfiguration initStateCfg = exchangeCfg.getInitStateCfg();

        // 从配置中获取交易所 ID 和存储文件夹路径
        this.exchangeId = initStateCfg.getExchangeId();
        this.folder = Paths.get(DiskSerializationProcessorConfiguration.DEFAULT_FOLDER);

        this.shardId = shardId;
        this.shardMask = numShards - 1;
        this.serializationProcessor = serializationProcessor;
        this.orderBookFactory = orderBookFactory;
        this.eventsHelper = new OrderBookEventsHelper(sharedPool::getChain);

        this.loggingCfg = exchangeCfg.getLoggingCfg();
        this.logDebug = loggingCfg.getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_MATCHING_DEBUG);

        // 初始化对象池配置
        final HashMap<Integer, Integer> objectsPoolConfig = new HashMap<>();
        objectsPoolConfig.put(ObjectsPool.DIRECT_ORDER, 1024 * 1024);
        objectsPoolConfig.put(ObjectsPool.DIRECT_BUCKET, 1024 * 64);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_4, 1024 * 32);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_16, 1024 * 16);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_48, 1024 * 8);
        objectsPoolConfig.put(ObjectsPool.ART_NODE_256, 1024 * 4);
        this.objectsPool = new ObjectsPool(objectsPoolConfig);

        // 加载序列化快照数据，如果没有则初始化新的状态
        if (ISerializationProcessor.canLoadFromSnapshot(serializationProcessor, initStateCfg, shardId, MODULE_ME)) {
            final DeserializedData deserialized = serializationProcessor.loadData(
                    initStateCfg.getSnapshotId(),
                    ISerializationProcessor.SerializedModuleType.MATCHING_ENGINE_ROUTER,
                    shardId,
                    bytesIn -> {
                        if (shardId != bytesIn.readInt()) {
                            throw new IllegalStateException("shardId 错误");
                        }
                        if (shardMask != bytesIn.readLong()) {
                            throw new IllegalStateException("shardMask 错误");
                        }

                        final BinaryCommandsProcessor bcp = new BinaryCommandsProcessor(
                                this::handleBinaryMessage,
                                this::handleReportQuery,
                                sharedPool,
                                exchangeCfg.getReportsQueriesCfg(),
                                bytesIn,
                                shardId + 1024);

                        final IntObjectHashMap<IOrderBook> ob = SerializationUtils.readIntHashMap(
                                bytesIn,
                                bytes -> IOrderBook.create(bytes, objectsPool, eventsHelper, loggingCfg));

                        return DeserializedData.builder().binaryCommandsProcessor(bcp).orderBooks(ob).build();
                    });

            this.binaryCommandsProcessor = deserialized.binaryCommandsProcessor;
            this.orderBooks = deserialized.orderBooks;

        } else {
            this.binaryCommandsProcessor = new BinaryCommandsProcessor(
                    this::handleBinaryMessage,
                    this::handleReportQuery,
                    sharedPool,
                    exchangeCfg.getReportsQueriesCfg(),
                    shardId + 1024);

            this.orderBooks = new IntObjectHashMap<>();
        }

        final OrdersProcessingConfiguration ordersProcCfg = exchangeCfg.getOrdersProcessingCfg();
        this.cfgMarginTradingEnabled = ordersProcCfg.getMarginTradingMode() == OrdersProcessingConfiguration.MarginTradingMode.MARGIN_TRADING_ENABLED;

        final PerformanceConfiguration perfCfg = exchangeCfg.getPerformanceCfg();
        this.cfgSendL2ForEveryCmd = perfCfg.isSendL2ForEveryCmd();
        this.cfgL2RefreshDepth = perfCfg.getL2RefreshDepth();
    }

    /**
     * 处理订单命令。
     *
     * @param seq 序列号
     * @param cmd 订单命令
     */
    public void processOrder(long seq, OrderCommand cmd) {
        final OrderCommandType command = cmd.command;

        // 处理特定类型的命令
        if (command == OrderCommandType.MOVE_ORDER
                || command == OrderCommandType.CANCEL_ORDER
                || command == OrderCommandType.PLACE_ORDER
                || command == OrderCommandType.REDUCE_ORDER
                || command == OrderCommandType.ORDER_BOOK_REQUEST) {
            // 只处理与当前处理器相关的 symbol
            if (symbolForThisHandler(cmd.symbol)) {
                processMatchingCommand(cmd);
            }
        } else if (command == OrderCommandType.BINARY_DATA_QUERY || command == OrderCommandType.BINARY_DATA_COMMAND) {
            // 处理二进制数据命令
            final CommandResultCode resultCode = binaryCommandsProcessor.acceptBinaryFrame(cmd);
            if (shardId == 0) {
                cmd.resultCode = resultCode;
            }
        } else if (command == OrderCommandType.RESET) {
            // 重置所有订单簿
            orderBooks.clear();
            binaryCommandsProcessor.reset();
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }
        } else if (command == OrderCommandType.NOP) {
            // 空操作
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }
        } else if (command == OrderCommandType.PERSIST_STATE_MATCHING) {
            // 持久化匹配引擎状态
            final boolean isSuccess = serializationProcessor.storeData(
                    cmd.orderId,
                    seq,
                    cmd.timestamp,
                    ISerializationProcessor.SerializedModuleType.MATCHING_ENGINE_ROUTER,
                    shardId,
                    this);
            UnsafeUtils.setResultVolatile(cmd, isSuccess, CommandResultCode.ACCEPTED, CommandResultCode.STATE_PERSIST_MATCHING_ENGINE_FAILED);
        }
    }

    /**
     * 处理二进制消息
     *
     * @param message 二进制消息
     */
    private void handleBinaryMessage(Object message) {
        if (message instanceof BatchAddSymbolsCommand) {
            final IntObjectHashMap<CoreSymbolSpecification> symbols = ((BatchAddSymbolsCommand) message).getSymbols();
            symbols.forEach(this::addSymbol);
        } else if (message instanceof BatchAddAccountsCommand) {
            // 处理账户命令（此处为空处理）
        }
    }

    /**
     * 处理报告查询
     *
     * @param reportQuery 报告查询
     * @param <R> 返回结果类型
     * @return 结果
     */
    private <R extends ReportResult> Optional<R> handleReportQuery(ReportQuery<R> reportQuery) {
        return reportQuery.process(this);
    }

    /**
     * 判断给定 symbol 是否属于当前处理器分片。
     *
     * @param symbol symbolId
     * @return 是否属于当前分片
     */
    private boolean symbolForThisHandler(final long symbol) {
        return (shardMask == 0) || ((symbol & shardMask) == shardId);
    }

    /**
     * 添加 symbol 及其对应的订单簿
     *
     * @param spec symbol 配置
     */
    private void addSymbol(final CoreSymbolSpecification spec) {
        // 如果不是货币对且未启用保证金交易，则警告
        if (spec.type != SymbolType.CURRENCY_EXCHANGE_PAIR && !cfgMarginTradingEnabled) {
            log.warn("不允许添加保证金交易符号: {}", spec);
        }

        // 添加订单簿
        if (orderBooks.get(spec.symbolId) == null) {
            orderBooks.put(spec.symbolId, orderBookFactory.create(spec, objectsPool, eventsHelper, loggingCfg));
        } else {
            log.warn("Symbol ID={} 已存在订单簿，无法添加 symbol: {}", spec.symbolId, spec);
        }
    }

    /**
     * 处理订单匹配命令。
     *
     * @param cmd 订单命令
     */
    private void processMatchingCommand(final OrderCommand cmd) {
        final IOrderBook orderBook = orderBooks.get(cmd.symbol);
        if (orderBook == null) {
            cmd.resultCode = CommandResultCode.MATCHING_INVALID_ORDER_BOOK_ID;
        } else {
            cmd.resultCode = IOrderBook.processCommand(orderBook, cmd);

            // 如果命令执行成功，发送市场数据给风控处理器
            if ((cfgSendL2ForEveryCmd || (cmd.serviceFlags & 1) != 0)
                    && cmd.command != OrderCommandType.ORDER_BOOK_REQUEST
                    && cmd.resultCode == CommandResultCode.SUCCESS) {

                cmd.marketData = orderBook.getL2MarketDataSnapshot(cfgL2RefreshDepth);
            }
        }
    }

    /**
     * 序列化当前匹配引擎路由器状态。
     *
     * @param bytes 输出字节流
     */
    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(shardId).writeLong(shardMask);
        binaryCommandsProcessor.writeMarshallable(bytes);

        // 序列化订单簿
        SerializationUtils.marshallIntHashMap(orderBooks, bytes);
    }

    /**
     * 序列化后的数据对象。
     */
    @Builder
    @RequiredArgsConstructor
    private static class DeserializedData {
        private final BinaryCommandsProcessor binaryCommandsProcessor;
        private final IntObjectHashMap<IOrderBook> orderBooks;
    }
}
