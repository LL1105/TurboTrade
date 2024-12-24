package com.exchange.core.processors;

import com.exchange.core.common.*;
import com.exchange.core.common.api.binary.BatchAddAccountsCommand;
import com.exchange.core.common.api.binary.BatchAddSymbolsCommand;
import com.exchange.core.common.api.binary.BinaryDataCommand;
import com.exchange.core.common.api.reports.ReportQuery;
import com.exchange.core.common.api.reports.ReportResult;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.config.ExchangeConfiguration;
import com.exchange.core.common.config.InitialStateConfiguration;
import com.exchange.core.common.config.LoggingConfiguration;
import com.exchange.core.common.config.OrdersProcessingConfiguration;
import com.exchange.core.common.constant.*;
import com.exchange.core.processors.journaling.DiskSerializationProcessorConfiguration;
import com.exchange.core.processors.journaling.ISerializationProcessor;
import com.exchange.core.utils.CoreArithmeticUtils;
import com.exchange.core.utils.SerializationUtils;
import com.exchange.core.utils.UnsafeUtils;
import exchange.core2.collections.objpool.ObjectsPool;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;

/**
 * 风控引擎
 */
@Slf4j
@Getter
public final class RiskEngine implements WriteBytesMarshallable {

    public static final ISerializationProcessor.SerializedModuleType MODULE_RE =
            ISerializationProcessor.SerializedModuleType.RISK_ENGINE;

    private final SymbolSpecificationProvider symbolSpecificationProvider;
    private final UserProfileService userProfileService;
    private final BinaryCommandsProcessor binaryCommandsProcessor;
    private final IntObjectHashMap<LastPriceCacheRecord> lastPriceCache;
    private final IntLongHashMap fees;
    private final IntLongHashMap adjustments;
    private final IntLongHashMap suspends;
    private final ObjectsPool objectsPool;

    // sharding by symbolId
    private final int shardId;
    private final long shardMask;

    private final String exchangeId; // TODO validate
    private final Path folder;

    private final boolean cfgIgnoreRiskProcessing;
    private final boolean cfgMarginTradingEnabled;

    private final ISerializationProcessor serializationProcessor;

    private final boolean logDebug;

    public RiskEngine(final int shardId,
                      final long numShards,
                      final ISerializationProcessor serializationProcessor,
                      final SharedPool sharedPool,
                      final ExchangeConfiguration exchangeConfiguration) {

        // 验证 numShards 是否是 2 的幂，因为分片数必须是 2 的幂
        if (Long.bitCount(numShards) != 1) {
            throw new IllegalArgumentException("Invalid number of shards " + numShards + " - must be power of 2");
        }

        // 获取交易所的初始化状态配置
        final InitialStateConfiguration initStateCfg = exchangeConfiguration.getInitStateCfg();

        // 获取交易所ID和默认文件夹路径
        this.exchangeId = initStateCfg.getExchangeId();
        this.folder = Paths.get(DiskSerializationProcessorConfiguration.DEFAULT_FOLDER);

        // 设置当前分片ID和分片掩码
        this.shardId = shardId;
        this.shardMask = numShards - 1; // 分片掩码，用于快速定位当前分片

        // 序列化处理器用于序列化和反序列化操作
        this.serializationProcessor = serializationProcessor;

        // 初始化对象池配置，这里配置了 SYM_RECORD 类型的对象池大小为 256K
        final HashMap<Integer, Integer> objectsPoolConfig = new HashMap<>();
        objectsPoolConfig.put(ObjectsPool.SYMBOL_POSITION_RECORD, 1024 * 256);
        this.objectsPool = new ObjectsPool(objectsPoolConfig);

        // 配置是否启用调试日志
        this.logDebug = exchangeConfiguration.getLoggingCfg().getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_RISK_DEBUG);

        // 尝试从快照加载历史状态数据
        if (ISerializationProcessor.canLoadFromSnapshot(serializationProcessor, initStateCfg, shardId, MODULE_RE)) {

            // 从快照中恢复状态，包含符号、用户信息、命令处理器、价格缓存等
            final State state = serializationProcessor.loadData(
                    initStateCfg.getSnapshotId(),
                    MODULE_RE,  // 模块标识
                    shardId,
                    bytesIn -> {
                        // 从快照中读取数据
                        if (shardId != bytesIn.readInt()) {
                            throw new IllegalStateException("wrong shardId");
                        }
                        if (shardMask != bytesIn.readLong()) {
                            throw new IllegalStateException("wrong shardMask");
                        }

                        // 读取符号规格、用户配置、命令处理器等
                        final SymbolSpecificationProvider symbolSpecificationProvider = new SymbolSpecificationProvider(bytesIn);
                        final UserProfileService userProfileService = new UserProfileService(bytesIn);
                        final BinaryCommandsProcessor binaryCommandsProcessor = new BinaryCommandsProcessor(
                                this::handleBinaryMessage, // 二进制消息处理
                                this::handleReportQuery,   // 查询报告处理
                                sharedPool,
                                exchangeConfiguration.getReportsQueriesCfg(),
                                bytesIn,
                                shardId);
                        // 读取价格缓存、费用、调整记录和暂停记录
                        final IntObjectHashMap<LastPriceCacheRecord> lastPriceCache = SerializationUtils.readIntHashMap(bytesIn, LastPriceCacheRecord::new);
                        final IntLongHashMap fees = SerializationUtils.readIntLongHashMap(bytesIn);
                        final IntLongHashMap adjustments = SerializationUtils.readIntLongHashMap(bytesIn);
                        final IntLongHashMap suspends = SerializationUtils.readIntLongHashMap(bytesIn);

                        // 返回读取到的状态
                        return new State(
                                symbolSpecificationProvider,
                                userProfileService,
                                binaryCommandsProcessor,
                                lastPriceCache,
                                fees,
                                adjustments,
                                suspends);
                    });

            // 从状态中恢复对象
            this.symbolSpecificationProvider = state.symbolSpecificationProvider;
            this.userProfileService = state.userProfileService;
            this.binaryCommandsProcessor = state.binaryCommandsProcessor;
            this.lastPriceCache = state.lastPriceCache;
            this.fees = state.fees;
            this.adjustments = state.adjustments;
            this.suspends = state.suspends;

        } else {
            // 如果无法从快照恢复状态，则初始化默认对象
            this.symbolSpecificationProvider = new SymbolSpecificationProvider();
            this.userProfileService = new UserProfileService();
            this.binaryCommandsProcessor = new BinaryCommandsProcessor(
                    this::handleBinaryMessage, // 二进制消息处理
                    this::handleReportQuery,   // 查询报告处理
                    sharedPool,
                    exchangeConfiguration.getReportsQueriesCfg(),
                    shardId);
            this.lastPriceCache = new IntObjectHashMap<>();
            this.fees = new IntLongHashMap();
            this.adjustments = new IntLongHashMap();
            this.suspends = new IntLongHashMap();
        }

        // 获取订单处理相关的配置
        final OrdersProcessingConfiguration ordersProcCfg = exchangeConfiguration.getOrdersProcessingCfg();

        // 是否忽略风险处理
        this.cfgIgnoreRiskProcessing = ordersProcCfg.getRiskProcessingMode() == OrdersProcessingConfiguration.RiskProcessingMode.NO_RISK_PROCESSING;

        // 是否启用保证金交易
        this.cfgMarginTradingEnabled = ordersProcCfg.getMarginTradingMode() == OrdersProcessingConfiguration.MarginTradingMode.MARGIN_TRADING_ENABLED;
    }

    @ToString
    public static class LastPriceCacheRecord implements BytesMarshallable, StateHash {

        // 卖价（默认为 Long.MAX_VALUE，表示未设置）
        public long askPrice = Long.MAX_VALUE;

        // 买价（默认为 0L，表示未设置）
        public long bidPrice = 0L;

        // 默认无参构造函数
        public LastPriceCacheRecord() {
        }

        // 带参数的构造函数，初始化买价和卖价
        public LastPriceCacheRecord(long askPrice, long bidPrice) {
            this.askPrice = askPrice;
            this.bidPrice = bidPrice;
        }

        // 从字节流中读取数据的构造函数（反序列化）
        public LastPriceCacheRecord(BytesIn bytes) {
            this.askPrice = bytes.readLong(); // 从字节流中读取卖价
            this.bidPrice = bytes.readLong(); // 从字节流中读取买价
        }

        // 将对象数据写入字节流（序列化）
        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeLong(askPrice); // 写入卖价到字节流
            bytes.writeLong(bidPrice); // 写入买价到字节流
        }

        // 生成一个平均价格的记录（买价和卖价取平均值）
        public LastPriceCacheRecord averagingRecord() {
            LastPriceCacheRecord average = new LastPriceCacheRecord();
            // 计算平均价（整数位移右移1位相当于除以2）
            average.askPrice = (this.askPrice + this.bidPrice) >> 1;
            average.bidPrice = average.askPrice; // 买价和卖价取相同值
            return average; // 返回平均记录
        }

        // 静态的 dummy 对象，用于占位或测试
        public static LastPriceCacheRecord dummy = new LastPriceCacheRecord(42, 42);

        // 计算对象的状态哈希值（用于状态校验或分布式一致性检查）
        @Override
        public int stateHash() {
            return Objects.hash(askPrice, bidPrice); // 基于买价和卖价生成哈希
        }
    }

    /**
     * 预处理命令处理器
     * 1. MOVE/CANCEL 命令会被忽略，并标记为该特定 uid 对于匹配引擎有效。
     * 2. PLACE ORDER 命令会检查该特定 uid 是否符合风险管理要求。
     * 3. ADD USER 和 BALANCE_ADJUSTMENT 命令会处理该特定 uid，不适用于匹配引擎。
     * 4. BINARY_DATA 命令会处理任何 uid 并标记为有效适用于匹配引擎 TODO: 哪个处理器标记？
     * 5. RESET 命令会处理任何 uid。
     *
     * @param cmd - 命令对象
     * @param seq - 命令序列号
     * @return 如果调用者应在批处理尚未处理完成时发布序列号，则返回 true。
     */
    public boolean preProcessCommand(final long seq, final OrderCommand cmd) {
        switch (cmd.command) {
            // 1. MOVE、CANCEL、REDUCE 和 ORDER_BOOK_REQUEST 命令被忽略
            case MOVE_ORDER:
            case CANCEL_ORDER:
            case REDUCE_ORDER:
            case ORDER_BOOK_REQUEST:
                return false;

            // 2. PLACE_ORDER 命令需要执行风险检查
            case PLACE_ORDER:
                if (uidForThisHandler(cmd.uid)) { // 如果该命令的 uid 对应于当前处理器
                    cmd.resultCode = placeOrderRiskCheck(cmd); // 进行下单风险检查
                }
                return false;

            // 3. ADD_USER 命令会为特定 uid 添加空的用户配置文件
            case ADD_USER:
                if (uidForThisHandler(cmd.uid)) { // 如果该命令的 uid 对应于当前处理器
                    cmd.resultCode = userProfileService.addEmptyUserProfile(cmd.uid)
                            ? CommandResultCode.SUCCESS
                            : CommandResultCode.USER_MGMT_USER_ALREADY_EXISTS; // 如果用户已经存在，返回错误代码
                }
                return false;

            // 4. BALANCE_ADJUSTMENT 命令会调整特定 uid 的余额
            case BALANCE_ADJUSTMENT:
                if (uidForThisHandler(cmd.uid)) { // 如果该命令的 uid 对应于当前处理器
                    cmd.resultCode = adjustBalance(
                            cmd.uid, cmd.symbol, cmd.price, cmd.orderId, BalanceAdjustmentType.of(cmd.orderType.getCode())); // 调整余额
                }
                return false;

            // 5. SUSPEND_USER 命令会暂停特定 uid 的用户配置文件
            case SUSPEND_USER:
                if (uidForThisHandler(cmd.uid)) { // 如果该命令的 uid 对应于当前处理器
                    cmd.resultCode = userProfileService.suspendUserProfile(cmd.uid); // 暂停用户配置文件
                }
                return false;

            // 6. RESUME_USER 命令会恢复特定 uid 的用户配置文件
            case RESUME_USER:
                if (uidForThisHandler(cmd.uid)) { // 如果该命令的 uid 对应于当前处理器
                    cmd.resultCode = userProfileService.resumeUserProfile(cmd.uid); // 恢复用户配置文件
                }
                return false;

            // 7. BINARY_DATA_COMMAND 和 BINARY_DATA_QUERY 命令处理任何 uid，并标记为匹配引擎有效
            case BINARY_DATA_COMMAND:
            case BINARY_DATA_QUERY:
                binaryCommandsProcessor.acceptBinaryFrame(cmd); // 处理二进制数据命令
                if (shardId == 0) { // 如果 shardId 是 0
                    cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE; // 标记为匹配引擎有效
                }
                return false;

            // 8. RESET 命令会重置状态，适用于任何 uid
            case RESET:
                reset(); // 执行重置操作
                if (shardId == 0) { // 如果 shardId 是 0
                    cmd.resultCode = CommandResultCode.SUCCESS; // 返回成功状态
                }
                return false;

            // 9. PERSIST_STATE_MATCHING 命令会标记为匹配引擎有效
            case PERSIST_STATE_MATCHING:
                if (shardId == 0) { // 如果 shardId 是 0
                    cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE; // 标记为匹配引擎有效
                }
                return true; // 返回 true，表示可以在批处理未完全处理时发布该序列号

            // 10. PERSIST_STATE_RISK 命令会持久化风险引擎的状态
            case PERSIST_STATE_RISK:
                final boolean isSuccess = serializationProcessor.storeData(
                        cmd.orderId,
                        seq,
                        cmd.timestamp,
                        MODULE_RE,
                        shardId,
                        this); // 持久化数据
                UnsafeUtils.setResultVolatile(cmd, isSuccess, CommandResultCode.SUCCESS, CommandResultCode.STATE_PERSIST_RISK_ENGINE_FAILED); // 设置结果
                return false;
        }
        return false; // 默认返回 false
    }

    private CommandResultCode adjustBalance(long uid, int currency, long amountDiff, long fundingTransactionId, BalanceAdjustmentType adjustmentType) {
        // 调用 userProfileService 进行余额调整操作
        final CommandResultCode res = userProfileService.balanceAdjustment(uid, currency, amountDiff, fundingTransactionId);

        // 如果余额调整成功（成功返回的是 SUCCESS）
        if (res == CommandResultCode.SUCCESS) {
            // 根据调整类型（ADJUSTMENT 或 SUSPEND）更新总调整记录
            switch (adjustmentType) {
                case ADJUSTMENT: // 如果是普通的余额调整（例如增加或减少金额）
                    adjustments.addToValue(currency, -amountDiff);  // 更新 adjustments（调整金额）
                    break;

                case SUSPEND: // 如果是暂停操作（例如冻结某个余额）
                    suspends.addToValue(currency, -amountDiff);  // 更新 suspends（暂停金额）
                    break;
            }
        }
        // 返回操作结果
        return res;
    }

    private void handleBinaryMessage(BinaryDataCommand message) {

        // 处理 BatchAddSymbolsCommand 类型的消息
        if (message instanceof BatchAddSymbolsCommand) {
            // 获取命令中包含的符号列表
            final IntObjectHashMap<CoreSymbolSpecification> symbols = ((BatchAddSymbolsCommand) message).getSymbols();
            // 遍历所有符号
            symbols.forEach(spec -> {
                // 如果符号是货币对（CURRENCY_EXCHANGE_PAIR）或者启用了保证金交易（cfgMarginTradingEnabled），则添加该符号
                if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR || cfgMarginTradingEnabled) {
                    symbolSpecificationProvider.addSymbol(spec);  // 将符号添加到符号提供者中
                } else {
                    // 否则，日志警告，不允许加入保证金符号
                    log.warn("Margin symbols are not allowed: {}", spec);
                }
            });

            // 处理 BatchAddAccountsCommand 类型的消息
        } else if (message instanceof BatchAddAccountsCommand) {
            // 获取命令中包含的用户账户列表
            ((BatchAddAccountsCommand) message).getUsers().forEachKeyValue((uid, accounts) -> {
                // 如果成功为用户创建了空的用户资料（用户首次创建）
                if (userProfileService.addEmptyUserProfile(uid)) {
                    // 遍历用户的账户信息，并调整余额
                    accounts.forEachKeyValue((cur, bal) ->
                            adjustBalance(uid, cur, bal, 1_000_000_000 + cur, BalanceAdjustmentType.ADJUSTMENT));
                } else {
                    // 如果用户已经存在，日志输出用户存在的调试信息
                    log.debug("User already exist: {}", uid);
                }
            });
        }
    }

    private <R extends ReportResult> Optional<R> handleReportQuery(ReportQuery<R> reportQuery) {
        return reportQuery.process(this);
    }

    public boolean uidForThisHandler(final long uid) {
        return (shardMask == 0) || ((uid & shardMask) == shardId);
    }

    private CommandResultCode placeOrderRiskCheck(final OrderCommand cmd) {

        // 根据用户UID获取用户资料
        final UserProfile userProfile = userProfileService.getUserProfile(cmd.uid);
        if (userProfile == null) {
            // 如果用户资料不存在，返回认证失败的结果码
            cmd.resultCode = CommandResultCode.AUTH_INVALID_USER;
            log.warn("User profile {} not found", cmd.uid);
            return CommandResultCode.AUTH_INVALID_USER;
        }

        // 根据订单的symbol获取交易对规格
        final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(cmd.symbol);
        if (spec == null) {
            // 如果交易对规格不存在，返回无效symbol的结果码
            log.warn("Symbol {} not found", cmd.symbol);
            return CommandResultCode.INVALID_SYMBOL;
        }

        // 如果配置中忽略风险检查，直接跳过处理
        if (cfgIgnoreRiskProcessing) {
            return CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        }

        // 检查用户账户是否有足够的资金
        final CommandResultCode resultCode = placeOrder(cmd, userProfile, spec);

        // 如果风险检查未通过（比如资金不足），返回相关错误结果码
        if (resultCode != CommandResultCode.VALID_FOR_MATCHING_ENGINE) {
            log.warn("{} risk result={} uid={}: Can not place {}", cmd.orderId, resultCode, userProfile.uid, cmd);
            log.warn("{} accounts:{}", cmd.orderId, userProfile.accounts);
            return CommandResultCode.RISK_NSF;  // 返回资金不足（NSF，No Sufficient Funds）错误
        }

        // 如果所有检查都通过，返回有效的结果码，表示订单可以进入匹配引擎
        return resultCode;
    }


    private CommandResultCode placeOrder(final OrderCommand cmd,
                                         final UserProfile userProfile,
                                         final CoreSymbolSpecification spec) {

        // 如果交易对类型是货币兑换对（CURRENCY_EXCHANGE_PAIR）
        if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {
            // 调用处理货币兑换订单的方法
            return placeExchangeOrder(cmd, userProfile, spec);

            // 如果交易对类型是期货合约（FUTURES_CONTRACT）
        } else if (spec.type == SymbolType.FUTURES_CONTRACT) {

            // 如果未启用保证金交易，直接返回错误码
            if (!cfgMarginTradingEnabled) {
                return CommandResultCode.RISK_MARGIN_TRADING_DISABLED;
            }

            // 获取用户在该交易对上的持仓记录
            SymbolPositionRecord position = userProfile.positions.get(spec.symbolId); // TODO: 是否使用 getIfAbsentPut？

            // 如果没有找到持仓记录，创建一个新的持仓记录
            if (position == null) {
                position = objectsPool.get(ObjectsPool.SYMBOL_POSITION_RECORD, SymbolPositionRecord::new);
                position.initialize(userProfile.uid, spec.symbolId, spec.quoteCurrency);
                userProfile.positions.put(spec.symbolId, position);
            }

            // 检查用户是否可以下保证金订单
            final boolean canPlaceOrder = canPlaceMarginOrder(cmd, userProfile, spec, position);
            if (canPlaceOrder) {
                // 如果可以下单，更新持仓记录的待持有数量
                position.pendingHold(cmd.action, cmd.size);
                return CommandResultCode.VALID_FOR_MATCHING_ENGINE;  // 返回可以进入匹配引擎的结果码
            } else {
                // 如果不能下单，尝试清理持仓记录（如果持仓为空）
                if (position.isEmpty()) {
                    removePositionRecord(position, userProfile);
                }
                return CommandResultCode.RISK_NSF;  // 返回资金不足（NSF）错误码
            }

            // 如果是其他类型的交易对，返回不支持的交易对类型
        } else {
            return CommandResultCode.UNSUPPORTED_SYMBOL_TYPE;
        }
    }

    // 该方法用于处理交易所订单的提交，检查订单是否符合风险控制条件，并计算所需的持有资金。
    // 如果资金充足，订单将被认为是有效的，可以继续提交给匹配引擎。否则，返回资金不足的错误。
    private CommandResultCode placeExchangeOrder(final OrderCommand cmd,
                                                 final UserProfile userProfile,
                                                 final CoreSymbolSpecification spec) {

        // 根据订单动作（买单或卖单）确定涉及的货币
        final int currency = (cmd.action == OrderAction.BID) ? spec.quoteCurrency : spec.baseCurrency;

        // 初始化一个变量，用于计算自由保证金（如果启用了保证金交易）
        long freeFuturesMargin = 0L;

        // 如果启用了保证金交易，检查用户的持仓并计算自由保证金
        if (cfgMarginTradingEnabled) {
            for (final SymbolPositionRecord position : userProfile.positions) {
                if (position.currency == currency) { // 如果持仓的货币与订单的货币相同
                    final int recSymbol = position.symbol;  // 获取持仓的符号ID
                    final CoreSymbolSpecification spec2 = symbolSpecificationProvider.getSymbolSpecification(recSymbol); // 获取该符号的规格

                    // 计算该持仓的自由保证金：持仓的预估利润减去所需的保证金
                    freeFuturesMargin +=
                            (position.estimateProfit(spec2, lastPriceCache.get(recSymbol)) - position.calculateRequiredMarginForFutures(spec2));
                }
            }
        }

        // 获取订单大小（即交易的数量）
        final long size = cmd.size;

        // 用于存储订单所需的持有金额
        final long orderHoldAmount;

        // 处理买单（BID）情况
        if (cmd.action == OrderAction.BID) {

            // 如果订单类型是预算型买单（FOK_BUDGET 或 IOC_BUDGET），需要确保预定的买入价格与实际价格一致
            if (cmd.orderType == OrderType.FOK_BUDGET || cmd.orderType == OrderType.IOC_BUDGET) {
                if (cmd.reserveBidPrice != cmd.price) { // 如果预定价格与实际价格不符
                    return CommandResultCode.RISK_INVALID_RESERVE_BID_PRICE; // 返回错误
                }

                // 计算买入所需的持有金额：买入数量 * 价格 + 手续费
                orderHoldAmount = CoreArithmeticUtils.calculateAmountBidTakerFeeForBudget(size, cmd.price, spec);
                if (logDebug) log.debug("hold amount budget buy {} = {} * {} + {} * {}", cmd.price, size, spec.quoteScaleK, size, spec.takerFee);

            } else {
                // 对于普通买单，确保预定价格大于等于实际价格
                if (cmd.reserveBidPrice < cmd.price) {
                    return CommandResultCode.RISK_INVALID_RESERVE_BID_PRICE; // 返回错误
                }

                // 计算普通买单所需的持有金额：买入数量 * 预定价格 + 手续费
                orderHoldAmount = CoreArithmeticUtils.calculateAmountBidTakerFee(size, cmd.reserveBidPrice, spec);
                if (logDebug) log.debug("hold amount buy {} = {} * ( {} * {} + {} )", orderHoldAmount, size, cmd.reserveBidPrice, spec.quoteScaleK, spec.takerFee);
            }

        } else { // 处理卖单（ASK）情况

            // 确保卖单价格乘以报价规模不小于手续费，否则返回错误
            if (cmd.price * spec.quoteScaleK < spec.takerFee) {
                return CommandResultCode.RISK_ASK_PRICE_LOWER_THAN_FEE; // 返回错误
            }

            // 计算卖出所需的持有金额：卖出数量 * 基础规模
            orderHoldAmount = CoreArithmeticUtils.calculateAmountAsk(size, spec);
            if (logDebug) log.debug("hold sell {} = {} * {} ", orderHoldAmount, size, spec.baseScaleK);
        }

        // 如果开启了调试日志，记录订单持有金额与账户余额及自由保证金的比较
        if (logDebug) {
            log.debug("R1 uid={} : orderHoldAmount={} vs serProfile.accounts.get({})={} + freeFuturesMargin={}",
                    userProfile.uid, orderHoldAmount, currency, userProfile.accounts.get(currency), freeFuturesMargin);
        }

        // 进行余额的临时修改，扣除订单所需的资金
        long newBalance = userProfile.accounts.addToValue(currency, -orderHoldAmount);

        // 检查修改后的余额加上自由保证金是否足够支付订单所需金额
        final boolean canPlace = newBalance + freeFuturesMargin >= 0;

        // 如果资金不足，回滚余额，并返回资金不足的错误
        if (!canPlace) {
            userProfile.accounts.addToValue(currency, orderHoldAmount); // 回滚余额
            return CommandResultCode.RISK_NSF; // 资金不足
        } else {
            // 如果资金充足，返回订单有效，允许提交给匹配引擎
            return CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        }
    }

    /**
     * 检查是否可以下保证金订单。
     * 1. 用户账户余额
     * 2. 保证金
     * 3. 当前限价订单
     * <p>
     * 注意：当前实现不考虑以不同货币报价的账户和头寸。
     *
     * @param cmd         - 订单命令
     * @param userProfile - 用户资料
     * @param spec        - 符号规格
     * @param position    - 用户的头寸记录
     * @return 如果允许下单，返回 true；否则返回 false
     */
    private boolean canPlaceMarginOrder(final OrderCommand cmd,
                                        final UserProfile userProfile,
                                        final CoreSymbolSpecification spec,
                                        final SymbolPositionRecord position) {

        // 计算订单的保证金要求：根据符号规格、订单动作（买/卖）和订单大小计算新订单所需的保证金
        final long newRequiredMarginForSymbol = position.calculateRequiredMarginForOrder(spec, cmd.action, cmd.size);

        // 如果计算出的所需保证金为-1，表示订单不会增加风险暴露，因此总是允许下单
        if (newRequiredMarginForSymbol == -1) {
            return true;
        }

        // 如果需要额外的保证金，进行以下计算

        final int symbol = cmd.symbol;  // 获取订单的符号ID
        long freeMargin = 0L;  // 初始化自由保证金（即用户账户的可用资金）

        // 遍历用户的所有头寸，计算自由保证金（只计算与当前订单同货币的头寸）
        for (final SymbolPositionRecord positionRecord : userProfile.positions) {
            final int recSymbol = positionRecord.symbol;
            // 如果头寸的符号与当前订单的符号不同
            if (recSymbol != symbol) {
                // 如果头寸的货币与订单符号的报价货币相同
                if (positionRecord.currency == spec.quoteCurrency) {
                    final CoreSymbolSpecification spec2 = symbolSpecificationProvider.getSymbolSpecification(recSymbol);
                    // 计算头寸的估算利润（即收益）减去所需保证金
                    freeMargin += positionRecord.estimateProfit(spec2, lastPriceCache.get(recSymbol));
                    freeMargin -= positionRecord.calculateRequiredMarginForFutures(spec2);
                }
            } else { // 当前订单的符号是用户已经持有的头寸的符号
                // 计算该头寸的估算利润
                freeMargin = position.estimateProfit(spec, lastPriceCache.get(spec.symbolId));
            }
        }

        // 注释掉的日志记录，原本用于调试：记录新保证金需求、账户余额和自由保证金的情况
//        log.debug("newMargin={} <= account({})={} + free {}",
//                newRequiredMarginForSymbol, position.currency, userProfile.accounts.get(position.currency), freeMargin);

        // 检查用户账户余额加上自由保证金是否能够覆盖新的保证金要求
        return newRequiredMarginForSymbol <= userProfile.accounts.get(position.currency) + freeMargin;
    }

    /**
     * 处理风险释放逻辑，根据订单命令和市场数据决定是否允许风险释放操作。
     * <p>
     * 该方法主要处理两种情况：
     * 1. 根据订单事件类型（如 REDUCE 或 REJECT）处理交易事件。
     * 2. 如果开启了保证金交易模式（`cfgMarginTradingEnabled`），则处理市场数据并更新最后价格缓存。
     *
     * @param seq - 序列号，用于标识该操作的唯一性（未使用，可能是预留字段）
     * @param cmd - 订单命令，包含订单信息、市场数据和匹配事件
     * @return true 如果操作成功处理，false 否则
     */
    public boolean handlerRiskRelease(final long seq, final OrderCommand cmd) {

        final int symbol = cmd.symbol;  // 获取订单符号

        final L2MarketData marketData = cmd.marketData;  // 获取市场数据（如买卖价格）
        MatcherTradeEvent mte = cmd.matcherEvent;  // 获取匹配事件（如订单状态变化）

        // 如果没有市场数据且事件为空或是BINARY_EVENT类型，跳过事件处理
        if (marketData == null && (mte == null || mte.eventType == MatcherEventType.BINARY_EVENT)) {
            return false;
        }

        final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(symbol);  // 获取符号规格
        if (spec == null) {
            throw new IllegalStateException("Symbol not found: " + symbol);  // 如果符号不存在，抛出异常
        }

        final boolean takerSell = cmd.action == OrderAction.ASK;  // 判断订单类型是否为卖单（ASK）

        // 如果有匹配事件，并且事件类型不是BINARY_EVENT，处理事件
        if (mte != null && mte.eventType != MatcherEventType.BINARY_EVENT) {
            // 如果是货币兑换对（CURRENCY_EXCHANGE_PAIR）符号类型，进行相应处理
            if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {

                // 根据用户ID决定是否获取用户资料
                final UserProfile takerUp = uidForThisHandler(cmd.uid)
                        ? userProfileService.getUserProfileOrAddSuspended(cmd.uid)
                        : null;

                // 如果事件类型为 REDUCE 或 REJECT，处理相应的拒绝或减少事件
                if (mte.eventType == MatcherEventType.REDUCE || mte.eventType == MatcherEventType.REJECT) {
                    if (takerUp != null) {
                        handleMatcherRejectReduceEventExchange(cmd, mte, spec, takerSell, takerUp);  // 处理拒绝或减少事件
                    }
                    mte = mte.nextEvent;  // 移动到下一个事件
                }

                // 处理其他事件类型（如买卖事件）
                if (mte != null) {
                    if (takerSell) {
                        handleMatcherEventsExchangeSell(mte, spec, takerUp);  // 处理卖单事件
                    } else {
                        handleMatcherEventsExchangeBuy(mte, spec, takerUp, cmd);  // 处理买单事件
                    }
                }
            } else {
                // 对于非货币兑换对符号类型（例如保证金模式符号），处理相应的匹配事件
                final UserProfile takerUp = uidForThisHandler(cmd.uid) ? userProfileService.getUserProfileOrAddSuspended(cmd.uid) : null;

                // 如果是保证金模式符号，获取用户的头寸记录
                final SymbolPositionRecord takerSpr = (takerUp != null) ? takerUp.getPositionRecordOrThrowEx(symbol) : null;
                do {
                    // 处理保证金模式下的匹配事件
                    handleMatcherEventMargin(mte, spec, cmd.action, takerUp, takerSpr);
                    mte = mte.nextEvent;  // 移动到下一个事件
                } while (mte != null);  // 继续处理直到没有更多事件
            }
        }

        // 如果市场数据不为空并且启用了保证金交易模式，更新最后价格缓存
        if (marketData != null && cfgMarginTradingEnabled) {
            final RiskEngine.LastPriceCacheRecord record = lastPriceCache.getIfAbsentPut(symbol, RiskEngine.LastPriceCacheRecord::new);
            // 更新买卖价格，使用市场数据中的第一条价格信息
            record.askPrice = (marketData.askSize != 0) ? marketData.askPrices[0] : Long.MAX_VALUE;
            record.bidPrice = (marketData.bidSize != 0) ? marketData.bidPrices[0] : 0;
        }

        return false;  // 该方法总是返回 false
    }

    /**
     * 处理保证金交易相关的撮合事件。
     *
     * @param ev          - 撮合交易事件 (例如，成交、拒绝、减少等)
     * @param spec        - 交易的符号规格（如，报价币种、交易手续费等）
     * @param takerAction - 接受方（即接单方）的操作类型 (BID 或 ASK)
     * @param takerUp     - 接受方的用户档案
     * @param takerSpr    - 接受方的持仓记录
     */
    private void handleMatcherEventMargin(final MatcherTradeEvent ev,
                                          final CoreSymbolSpecification spec,
                                          final OrderAction takerAction,
                                          final UserProfile takerUp,
                                          final SymbolPositionRecord takerSpr) {

        // 处理接单方 (Taker) 的交易逻辑
        if (takerUp != null) {
            if (ev.eventType == MatcherEventType.TRADE) {
                // 交易事件：更新接单方的持仓
                // `sizeOpen` 表示新开仓的数量
                final long sizeOpen = takerSpr.updatePositionForMarginTrade(takerAction, ev.size, ev.price);

                // 根据交易手续费计算接单方的手续费
                final long fee = spec.takerFee * sizeOpen;

                // 扣除接单方的手续费
                takerUp.accounts.addToValue(spec.quoteCurrency, -fee);

                // 将手续费添加到手续费总计
                fees.addToValue(spec.quoteCurrency, fee);
            } else if (ev.eventType == MatcherEventType.REJECT || ev.eventType == MatcherEventType.REDUCE) {
                // 拒绝或减少事件：仅处理接单方的操作（如取消订单或减少订单量）
                takerSpr.pendingRelease(takerAction, ev.size);
            }

            // 如果接单方的持仓为空，则移除该持仓记录
            if (takerSpr.isEmpty()) {
                removePositionRecord(takerSpr, takerUp);
            }
        }

        // 处理主单方 (Maker) 的交易逻辑，主要处理成交事件
        if (ev.eventType == MatcherEventType.TRADE && uidForThisHandler(ev.matchedOrderUid)) {
            // 获取主单方的用户档案
            final UserProfile maker = userProfileService.getUserProfileOrAddSuspended(ev.matchedOrderUid);
            // 获取主单方的持仓记录
            final SymbolPositionRecord makerSpr = maker.getPositionRecordOrThrowEx(spec.symbolId);

            // 更新主单方的持仓，`sizeOpen` 表示新开仓的数量
            long sizeOpen = makerSpr.updatePositionForMarginTrade(takerAction.opposite(), ev.size, ev.price);

            // 根据交易手续费计算主单方的手续费
            final long fee = spec.makerFee * sizeOpen;

            // 扣除主单方的手续费
            maker.accounts.addToValue(spec.quoteCurrency, -fee);

            // 将手续费添加到手续费总计
            fees.addToValue(spec.quoteCurrency, fee);

            // 如果主单方的持仓为空，则移除该持仓记录
            if (makerSpr.isEmpty()) {
                removePositionRecord(makerSpr, maker);
            }
        }
    }

    /**
     * 处理撮合引擎中的取消（REJECT）和减少（REDUCE）事件，更新接单方（Taker）账户余额。
     *
     * @param cmd         - 订单指令
     * @param ev          - 撮合交易事件
     * @param spec        - 交易的符号规格（如，报价币种、交易手续费等）
     * @param takerSell   - 判断接单方是否是卖单（即Taker是否为卖方）
     * @param taker       - 接单方的用户档案
     */
    private void handleMatcherRejectReduceEventExchange(final OrderCommand cmd,
                                                        final MatcherTradeEvent ev,
                                                        final CoreSymbolSpecification spec,
                                                        final boolean takerSell,
                                                        final UserProfile taker) {

        // 取消（REJECT）或减少（REDUCE）事件时，只有一方受到影响，因此只更新接单方的账户余额

        if (takerSell) {
            // 如果接单方是卖单（Taker Sell），则根据订单大小计算应释放的资金并返回给接单方
            taker.accounts.addToValue(spec.baseCurrency, CoreArithmeticUtils.calculateAmountAsk(ev.size, spec));
        } else {
            // 如果接单方是买单（Taker Buy）

            if (cmd.command == OrderCommandType.PLACE_ORDER && cmd.orderType == OrderType.FOK_BUDGET) {
                // 如果是 FOK（立即成交或取消）类型的订单，则按预设的价格计算并返回买单的资金
                taker.accounts.addToValue(spec.quoteCurrency, CoreArithmeticUtils.calculateAmountBidTakerFeeForBudget(ev.size, ev.price, spec));
            } else {
                // 如果不是 FOK 类型订单，则按出价和手续费计算买单的资金
                taker.accounts.addToValue(spec.quoteCurrency, CoreArithmeticUtils.calculateAmountBidTakerFee(ev.size, ev.bidderHoldPrice, spec));
            }
            // TODO: 处理 IOC（立即成交或取消）类型订单的 REJECT 事件，这时需要计算并返回剩余的预付款（如果有）
        }

    }

    /**
     * 处理撮合引擎中的卖单交易事件，更新接单方（Taker）和主单方（Maker）的账户余额。
     *
     * @param ev          - 撮合交易事件（包括订单大小、价格等信息）
     * @param spec        - 交易的符号规格（如，报价币种、交易手续费等）
     * @param taker       - 接单方的用户档案（Taker）
     */
    private void handleMatcherEventsExchangeSell(MatcherTradeEvent ev,
                                                 final CoreSymbolSpecification spec,
                                                 final UserProfile taker) {

        // 初始化接单方和主单方的数量和金额
        long takerSizeForThisHandler = 0L;
        long makerSizeForThisHandler = 0L;

        long takerSizePriceForThisHandler = 0L;

        final int quoteCurrency = spec.quoteCurrency;

        // 遍历所有事件
        while (ev != null) {
            assert ev.eventType == MatcherEventType.TRADE;

            // 聚合接单方的卖单数据（如果有接单方）
            if (taker != null) {
                takerSizePriceForThisHandler += ev.size * ev.price;  // 累积接单方的金额
                takerSizeForThisHandler += ev.size;  // 累积接单方的数量
            }

            // 处理主单方（Maker）的交易
            if (uidForThisHandler(ev.matchedOrderUid)) {
                final long size = ev.size;
                final UserProfile maker = userProfileService.getUserProfileOrAddSuspended(ev.matchedOrderUid);

                // 计算根据价格差来释放的资金
                final long priceDiff = ev.bidderHoldPrice - ev.price;
                final long amountDiffToReleaseInQuoteCurrency = CoreArithmeticUtils.calculateAmountBidReleaseCorrMaker(size, priceDiff, spec);
                maker.accounts.addToValue(quoteCurrency, amountDiffToReleaseInQuoteCurrency);

                // 计算主单方获得的基础币种金额
                final long gainedAmountInBaseCurrency = CoreArithmeticUtils.calculateAmountAsk(size, spec);
                maker.accounts.addToValue(spec.baseCurrency, gainedAmountInBaseCurrency);

                makerSizeForThisHandler += size;  // 累积主单方的数量
            }

            // 处理下一个事件
            ev = ev.nextEvent;
        }

        // 如果有接单方，更新接单方的账户余额
        if (taker != null) {
            taker.accounts.addToValue(quoteCurrency, takerSizePriceForThisHandler * spec.quoteScaleK - spec.takerFee * takerSizeForThisHandler);
        }

        // 如果有接单方或主单方参与交易，累积手续费
        if (takerSizeForThisHandler != 0 || makerSizeForThisHandler != 0) {
            fees.addToValue(quoteCurrency, spec.takerFee * takerSizeForThisHandler + spec.makerFee * makerSizeForThisHandler);
        }
    }

    /**
     * 处理撮合引擎中的买单交易事件，更新接单方（Taker）和主单方（Maker）的账户余额。
     *
     * @param ev          - 撮合交易事件（包括订单大小、价格等信息）
     * @param spec        - 交易的符号规格（如，报价币种、交易手续费等）
     * @param taker       - 接单方的用户档案（Taker）
     * @param cmd         - 订单命令
     */
    private void handleMatcherEventsExchangeBuy(MatcherTradeEvent ev,
                                                final CoreSymbolSpecification spec,
                                                final UserProfile taker,
                                                final OrderCommand cmd) {
        // 初始化接单方和主单方的数量和金额
        long takerSizeForThisHandler = 0L;
        long makerSizeForThisHandler = 0L;

        long takerSizePriceSum = 0L;
        long takerSizePriceHeldSum = 0L;

        final int quoteCurrency = spec.quoteCurrency;

        // 遍历所有事件
        while (ev != null) {
            assert ev.eventType == MatcherEventType.TRADE;

            // 为接单方（Taker）计算金额
            if (taker != null) {
                takerSizePriceSum += ev.size * ev.price; // 累积接单方的总金额
                takerSizePriceHeldSum += ev.size * ev.bidderHoldPrice; // 累积接单方被持有的金额
                takerSizeForThisHandler += ev.size; // 累积接单方的数量
            }

            // 处理主单方（Maker）的交易
            if (uidForThisHandler(ev.matchedOrderUid)) {
                final long size = ev.size;
                final UserProfile maker = userProfileService.getUserProfileOrAddSuspended(ev.matchedOrderUid);

                // 根据价格计算主单方的总金额，并扣除手续费
                final long gainedAmountInQuoteCurrency = CoreArithmeticUtils.calculateAmountBid(size, ev.price, spec);
                maker.accounts.addToValue(quoteCurrency, gainedAmountInQuoteCurrency - spec.makerFee * size);

                makerSizeForThisHandler += size; // 累积主单方的数量
            }

            ev = ev.nextEvent; // 处理下一个事件
        }

        // 如果有接单方，更新接单方的账户余额
        if (taker != null) {
            // 对于 FOK_BUDGET 订单，使用不同的金额计算方式
            if (cmd.command == OrderCommandType.PLACE_ORDER && cmd.orderType == OrderType.FOK_BUDGET) {
                takerSizePriceHeldSum = cmd.price;
            }
            // TODO IOC_BUDGET 订单可以被部分拒绝，需要调整持有的手续费

            // 更新接单方的账户余额，扣除相应的金额
            taker.accounts.addToValue(quoteCurrency, (takerSizePriceHeldSum - takerSizePriceSum) * spec.quoteScaleK);
            taker.accounts.addToValue(spec.baseCurrency, takerSizeForThisHandler * spec.baseScaleK);
        }

        // 如果有接单方或主单方参与交易，累积手续费
        if (takerSizeForThisHandler != 0 || makerSizeForThisHandler != 0) {
            fees.addToValue(quoteCurrency, spec.takerFee * takerSizeForThisHandler + spec.makerFee * makerSizeForThisHandler);
        }
    }

    private void removePositionRecord(SymbolPositionRecord record, UserProfile userProfile) {
        userProfile.accounts.addToValue(record.currency, record.profit);
        userProfile.positions.removeKey(record.symbol);
        objectsPool.put(ObjectsPool.SYMBOL_POSITION_RECORD, record);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(shardId).writeLong(shardMask);

        symbolSpecificationProvider.writeMarshallable(bytes);
        userProfileService.writeMarshallable(bytes);
        binaryCommandsProcessor.writeMarshallable(bytes);
        SerializationUtils.marshallIntHashMap(lastPriceCache, bytes);
        SerializationUtils.marshallIntLongHashMap(fees, bytes);
        SerializationUtils.marshallIntLongHashMap(adjustments, bytes);
        SerializationUtils.marshallIntLongHashMap(suspends, bytes);
    }

    public void reset() {
        userProfileService.reset();
        symbolSpecificationProvider.reset();
        binaryCommandsProcessor.reset();
        lastPriceCache.clear();
        fees.clear();
        adjustments.clear();
        suspends.clear();
    }

    @AllArgsConstructor
    @Getter
    private static class State {
        private final SymbolSpecificationProvider symbolSpecificationProvider;
        private final UserProfileService userProfileService;
        private final BinaryCommandsProcessor binaryCommandsProcessor;
        private final IntObjectHashMap<LastPriceCacheRecord> lastPriceCache;
        private final IntLongHashMap fees;
        private final IntLongHashMap adjustments;
        private final IntLongHashMap suspends;
    }
}
