package com.exchange.core.orderbook;

import com.exchange.core.common.*;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.config.LoggingConfiguration;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.OrderAction;
import com.exchange.core.common.constant.SymbolType;
import com.exchange.core.utils.SerializationUtils;
import exchange.core2.collections.objpool.ObjectsPool;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Slf4j
public final class OrderBookNaiveImpl implements IOrderBook {

    // 卖单（ASK）和买单（BID）价格桶的映射
    private final NavigableMap<Long, OrdersBucketNaive> askBuckets;
    private final NavigableMap<Long, OrdersBucketNaive> bidBuckets;

    // 交易对的市场规格，例如基础货币和报价货币
    private final CoreSymbolSpecification symbolSpec;

    // 订单ID到订单对象的映射
    private final LongObjectHashMap<Order> idMap = new LongObjectHashMap<>();

    // 订单簿事件帮助器
    private final OrderBookEventsHelper eventsHelper;

    // 是否启用调试日志
    private final boolean logDebug;

    // 构造函数：初始化订单簿，使用对象池和事件帮助器
    public OrderBookNaiveImpl(final CoreSymbolSpecification symbolSpec,
                              final ObjectsPool pool,
                              final OrderBookEventsHelper eventsHelper,
                              final LoggingConfiguration loggingCfg) {

        // 初始化市场规格
        this.symbolSpec = symbolSpec;

        // 初始化ASK和BID价格桶
        this.askBuckets = new TreeMap<>();
        this.bidBuckets = new TreeMap<>(Collections.reverseOrder());  // 买单按价格降序排列

        // 初始化事件帮助器
        this.eventsHelper = eventsHelper;

        // 是否启用调试日志
        this.logDebug = loggingCfg.getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_MATCHING_DEBUG);
    }

    // 构造函数：初始化订单簿，不使用对象池
    public OrderBookNaiveImpl(final CoreSymbolSpecification symbolSpec,
                              final LoggingConfiguration loggingCfg) {

        // 初始化市场规格
        this.symbolSpec = symbolSpec;

        // 初始化ASK和BID价格桶
        this.askBuckets = new TreeMap<>();
        this.bidBuckets = new TreeMap<>(Collections.reverseOrder());  // 买单按价格降序排列

        // 使用非池化事件帮助器
        this.eventsHelper = OrderBookEventsHelper.NON_POOLED_EVENTS_HELPER;

        // 是否启用调试日志
        this.logDebug = loggingCfg.getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_MATCHING_DEBUG);
    }

    // 构造函数：通过字节流反序列化订单簿，并恢复状态
    public OrderBookNaiveImpl(final BytesIn bytes, final LoggingConfiguration loggingCfg) {

        // 从字节流中恢复市场规格
        this.symbolSpec = new CoreSymbolSpecification(bytes);

        // 通过反序列化恢复ASK和BID价格桶
        this.askBuckets = SerializationUtils.readLongMap(bytes, TreeMap::new, OrdersBucketNaive::new);
        this.bidBuckets = SerializationUtils.readLongMap(bytes, () -> new TreeMap<>(Collections.reverseOrder()), OrdersBucketNaive::new);

        // 使用非池化事件帮助器
        this.eventsHelper = OrderBookEventsHelper.NON_POOLED_EVENTS_HELPER;

        // 重新构建订单ID到订单的映射缓存
        // TODO: 需要检查该操作对性能的影响
        askBuckets.values().forEach(bucket -> bucket.forEachOrder(order -> idMap.put(order.orderId, order)));
        bidBuckets.values().forEach(bucket -> bucket.forEachOrder(order -> idMap.put(order.orderId, order)));

        // 是否启用调试日志
        this.logDebug = loggingCfg.getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_MATCHING_DEBUG);

        // 校验内部状态（已注释掉）
        // validateInternalState();
    }

    @Override
    public void newOrder(final OrderCommand cmd) {

        switch (cmd.orderType) {
            case GTC:
                newOrderPlaceGtc(cmd);
                break;
            case IOC:
                newOrderMatchIoc(cmd);
                break;
            case FOK_BUDGET:
                newOrderMatchFokBudget(cmd);
                break;
            // TODO IOC_BUDGET and FOK support
            default:
                log.warn("Unsupported order type: {}", cmd);
                eventsHelper.attachRejectEvent(cmd, cmd.size);
        }
    }

    // GTC（Good-Til-Canceled）订单下单处理
    private void newOrderPlaceGtc(final OrderCommand cmd) {

        // 从命令中获取订单的操作类型、价格和数量
        final OrderAction action = cmd.action;
        final long price = cmd.price;
        final long size = cmd.size;

        // 检查订单是否可以立即匹配（是否存在相反方向的匹配订单）
        final long filledSize = tryMatchInstantly(cmd, subtreeForMatching(action, price), 0, cmd);

        // 如果订单完全匹配，则无需进一步处理，直接返回
        if (filledSize == size) {
            return;
        }

        // 获取订单ID并检查是否已经存在相同的订单ID（避免重复订单）
        long newOrderId = cmd.orderId;
        if (idMap.containsKey(newOrderId)) {
            // 订单ID重复，不能下单，但可以匹配已存在的订单
            eventsHelper.attachRejectEvent(cmd, cmd.size - filledSize); // 发送拒绝事件
            log.warn("duplicate order id: {}", cmd); // 打印日志
            return;
        }

        // 正常情况下，创建并放置一个常规的GTC限价订单
        final Order orderRecord = new Order(
                newOrderId,         // 订单ID
                price,               // 价格
                size,                // 数量
                filledSize,          // 已匹配数量
                cmd.reserveBidPrice, // 保留买入价格
                action,              // 操作类型（买或卖）
                cmd.uid,             // 用户ID
                cmd.timestamp);      // 时间戳

        // 根据订单操作类型选择合适的订单桶，并将新订单放入对应的价格桶中
        getBucketsByAction(action)
                .computeIfAbsent(price, OrdersBucketNaive::new)  // 如果价格桶不存在则创建新桶
                .put(orderRecord);   // 将订单放入价格桶中

        // 将订单记录放入订单ID映射表
        idMap.put(newOrderId, orderRecord);
    }

    // IoC（Immediate-Or-Cancel）订单匹配处理
    private void newOrderMatchIoc(final OrderCommand cmd) {

        // 尝试立即匹配订单
        final long filledSize = tryMatchInstantly(cmd, subtreeForMatching(cmd.action, cmd.price), 0, cmd);

        // 计算未匹配的数量
        final long rejectedSize = cmd.size - filledSize;

        // 如果订单未完全匹配，则发送拒绝事件
        if (rejectedSize != 0) {
            eventsHelper.attachRejectEvent(cmd, rejectedSize);
        }
    }

    // FOK（Fill-Or-Kill）订单预算匹配处理
    private void newOrderMatchFokBudget(final OrderCommand cmd) {

        final long size = cmd.size;

        // 根据订单操作类型选择合适的匹配子树（ASK为卖单，BID为买单）
        final SortedMap<Long, OrdersBucketNaive> subtreeForMatching =
                cmd.action == OrderAction.ASK ? bidBuckets : askBuckets;

        // 检查是否有足够的预算来填充该订单
        final Optional<Long> budget = checkBudgetToFill(size, subtreeForMatching);

        if (logDebug) log.debug("Budget calc: {} requested: {}", budget, cmd.price); // 打印调试日志

        // 如果有足够的预算并且预算限制满足条件，则进行立即匹配
        if (budget.isPresent() && isBudgetLimitSatisfied(cmd.action, budget.get(), cmd.price)) {
            tryMatchInstantly(cmd, subtreeForMatching, 0, cmd);
        } else {
            // 如果预算不足或条件不满足，发送拒绝事件
            eventsHelper.attachRejectEvent(cmd, size);
        }
    }

    // 判断预算限制是否满足
    private boolean isBudgetLimitSatisfied(final OrderAction orderAction, final long calculated, final long limit) {
        // 如果计算的预算等于限制，或者是买单且计算的预算大于限制，则满足条件
        return calculated == limit || (orderAction == OrderAction.BID ^ calculated > limit);
    }

    // 检查是否有足够的预算来填充订单
    private Optional<Long> checkBudgetToFill(
            long size,
            final SortedMap<Long, OrdersBucketNaive> matchingBuckets) {

        long budget = 0;  // 初始化预算

        // 遍历与订单匹配的订单桶
        for (final OrdersBucketNaive bucket : matchingBuckets.values()) {

            final long availableSize = bucket.getTotalVolume();  // 获取当前桶的可用数量
            final long price = bucket.getPrice();  // 获取当前桶的价格

            if (size > availableSize) {
                // 当前桶的可用数量不足以满足订单需求，减少订单大小并将该桶的总价加入预算
                size -= availableSize;
                budget += availableSize * price;
                if (logDebug) log.debug("add    {} * {} -> {}", price, availableSize, budget);  // 调试日志
            } else {
                // 当前桶的可用数量足够满足剩余的订单需求，直接计算结果并返回
                final long result = budget + size * price;
                if (logDebug) log.debug("return {} * {} -> {}", price, size, result);  // 调试日志
                return Optional.of(result);
            }
        }
        // 如果所有桶的可用数量都不足以满足订单，打印调试日志并返回空
        if (logDebug) log.debug("not enough liquidity to fill size={}", size);
        return Optional.empty();
    }

    // 获取用于匹配的订单桶子树
    private SortedMap<Long, OrdersBucketNaive> subtreeForMatching(final OrderAction action, final long price) {
        // 根据订单操作类型选择合适的订单桶（ASK为卖单，BID为买单），并返回该桶的子树
        return (action == OrderAction.ASK ? bidBuckets : askBuckets)
                .headMap(price, true);  // 获取价格小于等于指定价格的所有订单
    }

    /**
     * 尝试将订单立即与指定的排序桶进行匹配
     * 完全匹配的订单将从订单ID索引中移除
     * 如果发生任何交易，事件将发送到 tradesConsumer
     *
     * @param activeOrder     - GTC 或 IOC 类型的活动订单
     * @param matchingBuckets - 排序的订单桶映射
     * @param filled          - 当前订单已匹配的数量
     * @param triggerCmd      - 触发的命令（接单方）
     * @return 新的已匹配大小
     */
    private long tryMatchInstantly(
            final IOrder activeOrder,
            final SortedMap<Long, OrdersBucketNaive> matchingBuckets,
            long filled,
            final OrderCommand triggerCmd) {

//        log.info("matchInstantly: {} {}", order, matchingBuckets);

        // 如果匹配的桶为空，直接返回已匹配数量
        if (matchingBuckets.size() == 0) {
            return filled;
        }

        // 获取订单的大小
        final long orderSize = activeOrder.getSize();

        MatcherTradeEvent eventsTail = null;  // 用于存储交易事件链的尾部
        List<Long> emptyBuckets = new ArrayList<>();  // 用于存储被清空的桶的价格

        // 遍历每个桶进行匹配
        for (final OrdersBucketNaive bucket : matchingBuckets.values()) {

//            log.debug("Matching bucket: {} ...", bucket);
//            log.debug("... with order: {}", activeOrder);

            final long sizeLeft = orderSize - filled;  // 计算剩余的订单大小

            // 尝试在当前桶中匹配订单
            final OrdersBucketNaive.MatcherResult bucketMatchings = bucket.match(sizeLeft, activeOrder, eventsHelper);

            // 将匹配的订单从订单ID索引中移除
            bucketMatchings.ordersToRemove.forEach(idMap::remove);

            // 更新已匹配的数量
            filled += bucketMatchings.volume;

            // 将桶匹配的交易事件链接到当前命令的事件链
            if (eventsTail == null) {
                triggerCmd.matcherEvent = bucketMatchings.eventsChainHead;
            } else {
                eventsTail.nextEvent = bucketMatchings.eventsChainHead;
            }
            eventsTail = bucketMatchings.eventsChainTail;

//            log.debug("Matching orders: {}", matchingOrders);
//            log.debug("order.filled: {}", activeOrder.filled);

            long price = bucket.getPrice();  // 获取当前桶的价格

            // 如果桶已被清空，将其价格加入空桶列表
            if (bucket.getTotalVolume() == 0) {
                emptyBuckets.add(price);
            }

            // 如果订单已经完全匹配，跳出循环
            if (filled == orderSize) {
                break;
            }
        }

        // 移除已清空的桶
        emptyBuckets.forEach(matchingBuckets::remove);

//        log.debug("emptyBuckets: {}", emptyBuckets);
//        log.debug("matchingRecords: {}", matchingRecords);

        return filled;  // 返回新的已匹配数量
    }

    /**
     * 移除一个订单。<p>
     *
     * @param cmd 取消命令（orderId - 要移除的订单）
     * @return 如果订单移除成功，返回 true；如果未找到订单（可能已经被移除或匹配），返回 false
     */
    public CommandResultCode cancelOrder(OrderCommand cmd) {
        final long orderId = cmd.orderId;

        // 从 idMap 中获取订单
        final Order order = idMap.get(orderId);
        if (order == null || order.uid != cmd.uid) {
            // 订单已经被匹配并从订单簿中移除，或者UID不匹配
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        // 订单存在，移除订单
        idMap.remove(orderId);

        // 获取对应的订单簿
        final NavigableMap<Long, OrdersBucketNaive> buckets = getBucketsByAction(order.action);
        final long price = order.price;
        final OrdersBucketNaive ordersBucket = buckets.get(price);
        if (ordersBucket == null) {
            // 如果找不到对应的桶，抛出异常
            throw new IllegalStateException("Can not find bucket for order price=" + price + " for order " + order);
        }

        // 从桶中移除订单，如果桶为空，则移除桶
        ordersBucket.remove(orderId, cmd.uid);
        if (ordersBucket.getTotalVolume() == 0) {
            buckets.remove(price);  // 移除空的桶
        }

        // 发送减少事件
        cmd.matcherEvent = eventsHelper.sendReduceEvent(order, order.getSize() - order.getFilled(), true);

        // 填充命令中的动作字段（用于事件处理）
        cmd.action = order.getAction();

        return CommandResultCode.SUCCESS;
    }

    @Override
    public CommandResultCode reduceOrder(OrderCommand cmd) {
        final long orderId = cmd.orderId;
        final long requestedReduceSize = cmd.size;

        // 检查减少的订单数量是否有效
        if (requestedReduceSize <= 0) {
            return CommandResultCode.MATCHING_REDUCE_FAILED_WRONG_SIZE;
        }

        // 查找订单
        final Order order = idMap.get(orderId);
        if (order == null || order.uid != cmd.uid) {
            // 订单不存在，或者UID不匹配（订单可能已匹配、移动或取消）
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        // 计算剩余可减少的订单数量
        final long remainingSize = order.size - order.filled;
        final long reduceBy = Math.min(remainingSize, requestedReduceSize);

        // 获取当前订单所在的桶
        final NavigableMap<Long, OrdersBucketNaive> buckets = getBucketsByAction(order.action);
        final OrdersBucketNaive ordersBucket = buckets.get(order.price);
        if (ordersBucket == null) {
            // 不可能的状态，找不到订单所在的桶
            throw new IllegalStateException("Can not find bucket for order price=" + order.price + " for order " + order);
        }

        final boolean canRemove = (reduceBy == remainingSize);

        if (canRemove) {
            // 如果可以移除订单
            idMap.remove(orderId);

            // 如果桶为空，移除桶
            ordersBucket.remove(orderId, cmd.uid);
            if (ordersBucket.getTotalVolume() == 0) {
                buckets.remove(order.price);
            }

        } else {
            // 否则减少订单大小
            order.size -= reduceBy;
            ordersBucket.reduceSize(reduceBy);
        }

        // 发送减少事件
        cmd.matcherEvent = eventsHelper.sendReduceEvent(order, reduceBy, canRemove);

        // 更新命令中的动作字段（用于事件处理）
        cmd.action = order.getAction();

        return CommandResultCode.SUCCESS;
    }

    @Override
    public CommandResultCode moveOrder(OrderCommand cmd) {

        final long orderId = cmd.orderId;
        final long newPrice = cmd.price;

        // 查找订单
        final Order order = idMap.get(orderId);
        if (order == null || order.uid != cmd.uid) {
            // 订单不存在，或者UID不匹配（订单可能已匹配、移动或取消）
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        final long price = order.price;
        final NavigableMap<Long, OrdersBucketNaive> buckets = getBucketsByAction(order.action);
        final OrdersBucketNaive bucket = buckets.get(price);

        // 更新命令中的动作字段（用于事件处理）
        cmd.action = order.getAction();

        // 进行价格风险检查（特别是对于外汇交易对的买单）
        if (symbolSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR && order.action == OrderAction.BID && cmd.price > order.reserveBidPrice) {
            return CommandResultCode.MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT;
        }

        // 从原桶中移除订单，如果桶为空则移除桶
        bucket.remove(orderId, cmd.uid);
        if (bucket.getTotalVolume() == 0) {
            buckets.remove(price);
        }

        // 更新订单价格
        order.price = newPrice;

        // 尝试在新价格下匹配订单
        final SortedMap<Long, OrdersBucketNaive> matchingArea = subtreeForMatching(order.action, newPrice);
        final long filled = tryMatchInstantly(order, matchingArea, order.filled, cmd);

        if (filled == order.size) {
            // 如果订单完全匹配（100%可交易），从订单簿中移除订单
            idMap.remove(orderId);
            return CommandResultCode.SUCCESS;
        }

        order.filled = filled;

        // 如果未完全匹配，将订单放入对应的桶中
        final OrdersBucketNaive anotherBucket = buckets.computeIfAbsent(newPrice, p -> {
            OrdersBucketNaive b = new OrdersBucketNaive(p);
            return b;
        });
        anotherBucket.put(order);

        return CommandResultCode.SUCCESS;
    }

    /**
     * 根据订单动作获取相应的桶
     *
     * @param action - 订单动作（买单/卖单）
     * @return 返回相应的订单桶（买单桶或卖单桶）
     */
    private NavigableMap<Long, OrdersBucketNaive> getBucketsByAction(OrderAction action) {
        // 如果是卖单（ASK），返回卖单桶（askBuckets），否则返回买单桶（bidBuckets）
        return action == OrderAction.ASK ? askBuckets : bidBuckets;
    }

    /**
     * 根据订单ID从内部映射中获取订单
     *
     * @param orderId - 订单ID
     * @return 返回订单对象（如果存在），否则返回null
     */
    @Override
    public IOrder getOrderById(long orderId) {
        // 从idMap中获取订单，idMap为订单ID与订单对象的映射
        return idMap.get(orderId);
    }

    @Override
    public void fillAsks(final int size, L2MarketData data) {
        // 如果请求大小为0，直接返回
        if (size == 0) {
            data.askSize = 0;
            return;
        }

        // 遍历卖单桶（askBuckets），将数据填充到传入的 L2MarketData 对象
        int i = 0;
        for (OrdersBucketNaive bucket : askBuckets.values()) {
            data.askPrices[i] = bucket.getPrice();        // 设置卖单价格
            data.askVolumes[i] = bucket.getTotalVolume(); // 设置卖单总量
            data.askOrders[i] = bucket.getNumOrders();   // 设置卖单订单数
            if (++i == size) {
                break; // 填充到指定大小后跳出循环
            }
        }
        data.askSize = i; // 设置卖单的大小
    }

    @Override
    public void fillBids(final int size, L2MarketData data) {
        // 如果请求大小为0，直接返回
        if (size == 0) {
            data.bidSize = 0;
            return;
        }

        // 遍历买单桶（bidBuckets），将数据填充到传入的 L2MarketData 对象
        int i = 0;
        for (OrdersBucketNaive bucket : bidBuckets.values()) {
            data.bidPrices[i] = bucket.getPrice();        // 设置买单价格
            data.bidVolumes[i] = bucket.getTotalVolume(); // 设置买单总量
            data.bidOrders[i] = bucket.getNumOrders();   // 设置买单订单数
            if (++i == size) {
                break; // 填充到指定大小后跳出循环
            }
        }
        data.bidSize = i; // 设置买单的大小
    }

    @Override
    public int getTotalAskBuckets(final int limit) {
        return Math.min(limit, askBuckets.size());
    }

    @Override
    public int getTotalBidBuckets(final int limit) {
        return Math.min(limit, bidBuckets.size());
    }

    @Override
    public void validateInternalState() {
        askBuckets.values().forEach(OrdersBucketNaive::validate);
        bidBuckets.values().forEach(OrdersBucketNaive::validate);
    }

    @Override
    public OrderBookImplType getImplementationType() {
        return OrderBookImplType.NAIVE;
    }

    @Override
    public List<Order> findUserOrders(final long uid) {
        List<Order> list = new ArrayList<>();
        Consumer<OrdersBucketNaive> bucketConsumer = bucket -> bucket.forEachOrder(order -> {
            if (order.uid == uid) {
                list.add(order);
            }
        });
        askBuckets.values().forEach(bucketConsumer);
        bidBuckets.values().forEach(bucketConsumer);
        return list;
    }

    @Override
    public CoreSymbolSpecification getSymbolSpec() {
        return symbolSpec;
    }

    @Override
    public Stream<IOrder> askOrdersStream(final boolean sorted) {
        return askBuckets.values().stream().flatMap(bucket -> bucket.getAllOrders().stream());
    }

    @Override
    public Stream<IOrder> bidOrdersStream(final boolean sorted) {
        return bidBuckets.values().stream().flatMap(bucket -> bucket.getAllOrders().stream());
    }

    // for testing only
    @Override
    public int getOrdersNum(OrderAction action) {
        final NavigableMap<Long, OrdersBucketNaive> buckets = action == OrderAction.ASK ? askBuckets : bidBuckets;
        return buckets.values().stream().mapToInt(OrdersBucketNaive::getNumOrders).sum();
//        int askOrders = askBuckets.values().stream().mapToInt(OrdersBucketNaive::getNumOrders).sum();
//        int bidOrders = bidBuckets.values().stream().mapToInt(OrdersBucketNaive::getNumOrders).sum();
        //log.debug("idMap:{} askOrders:{} bidOrders:{}", idMap.size(), askOrders, bidOrders);
//        int knownOrders = idMap.size();
//        assert knownOrders == askOrders + bidOrders : "inconsistent known orders";
    }

    @Override
    public long getTotalOrdersVolume(OrderAction action) {
        final NavigableMap<Long, OrdersBucketNaive> buckets = action == OrderAction.ASK ? askBuckets : bidBuckets;
        return buckets.values().stream().mapToLong(OrdersBucketNaive::getTotalVolume).sum();
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeByte(getImplementationType().getCode());
        symbolSpec.writeMarshallable(bytes);
        SerializationUtils.marshallLongMap(askBuckets, bytes);
        SerializationUtils.marshallLongMap(bidBuckets, bytes);
    }
}
