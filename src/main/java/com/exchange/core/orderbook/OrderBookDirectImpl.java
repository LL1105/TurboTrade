package com.exchange.core.orderbook;

import com.exchange.core.common.*;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.config.LoggingConfiguration;
import com.exchange.core.common.constant.*;
import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.objpool.ObjectsPool;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public final class OrderBookDirectImpl implements IOrderBook {

    // 买卖价格的桶（OrderBook 中的买卖盘数据）
    private final LongAdaptiveRadixTreeMap<Bucket> askPriceBuckets;  // 卖出价格的桶（卖单）
    private final LongAdaptiveRadixTreeMap<Bucket> bidPriceBuckets;  // 买入价格的桶（买单）

    // 商品规格
    private final CoreSymbolSpecification symbolSpec;  // 商品的核心规格

    // 索引：订单ID -> 订单
    private final LongAdaptiveRadixTreeMap<DirectOrder> orderIdIndex;  // 订单ID到订单的映射表

    // 头部（可以为null）
    private DirectOrder bestAskOrder = null;  // 最佳卖单（当前最优卖出价格的订单）
    private DirectOrder bestBidOrder = null;  // 最佳买单（当前最优买入价格的订单）

    // 对象池
    private final ObjectsPool objectsPool;  // 用于管理内存池，避免频繁分配和回收内存

    private final OrderBookEventsHelper eventsHelper;  // 用于帮助处理订单簿中的事件

    private final boolean logDebug;  // 是否开启调试日志

    public OrderBookDirectImpl(final CoreSymbolSpecification symbolSpec,
                               final ObjectsPool objectsPool,
                               final OrderBookEventsHelper eventsHelper,
                               final LoggingConfiguration loggingCfg) {

        this.symbolSpec = symbolSpec;
        this.objectsPool = objectsPool;
        this.askPriceBuckets = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.bidPriceBuckets = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.eventsHelper = eventsHelper;
        this.orderIdIndex = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.logDebug = loggingCfg.getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_MATCHING_DEBUG);
    }

    public OrderBookDirectImpl(final BytesIn bytes,
                               final ObjectsPool objectsPool,
                               final OrderBookEventsHelper eventsHelper,
                               final LoggingConfiguration loggingCfg) {

        this.symbolSpec = new CoreSymbolSpecification(bytes);
        this.objectsPool = objectsPool;
        this.askPriceBuckets = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.bidPriceBuckets = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.eventsHelper = eventsHelper;
        this.orderIdIndex = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.logDebug = loggingCfg.getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_MATCHING_DEBUG);

        final int size = bytes.readInt();
        for (int i = 0; i < size; i++) {
            DirectOrder order = new DirectOrder(bytes);
            insertOrder(order, null);
            orderIdIndex.put(order.orderId, order);
        }
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
            // TODO IOC_BUDGET 和 FOK 支持
            default:
                // 对于不支持的订单类型，记录警告日志，并发送拒绝事件
                log.warn("不支持的订单类型: {}", cmd);
                eventsHelper.attachRejectEvent(cmd, cmd.size);
        }
    }

    // 处理GTC订单（Good Till Canceled）
    private void newOrderPlaceGtc(final OrderCommand cmd) {
        final long size = cmd.size;

        // 检查是否有可以匹配的市场订单
        final long filledSize = tryMatchInstantly(cmd, cmd);
        if (filledSize == size) {
            // 如果订单在提交之前已经完全成交，直接返回
            return;
        }

        final long orderId = cmd.orderId;
        // 检查订单ID是否重复
        if (orderIdIndex.get(orderId) != null) { // containsKey检查哈希表
            // 如果是重复的订单ID，不能提交订单，只能进行匹配
            eventsHelper.attachRejectEvent(cmd, size - filledSize);
            log.warn("重复的订单ID: {}", cmd);
            return;
        }

        final long price = cmd.price;

        // 正常的GTC订单处理
        final DirectOrder orderRecord = objectsPool.get(ObjectsPool.DIRECT_ORDER, (Supplier<DirectOrder>) DirectOrder::new);

        orderRecord.orderId = orderId;
        orderRecord.price = price;
        orderRecord.size = size;
        orderRecord.reserveBidPrice = cmd.reserveBidPrice;
        orderRecord.action = cmd.action;
        orderRecord.uid = cmd.uid;
        orderRecord.timestamp = cmd.timestamp;
        orderRecord.filled = filledSize;

        // 将订单记录添加到索引中
        orderIdIndex.put(orderId, orderRecord);
        // 将订单插入到订单簿
        insertOrder(orderRecord, null);
    }

    // 处理IOC订单（Immediate or Cancel）
    private void newOrderMatchIoc(final OrderCommand cmd) {
        final long filledSize = tryMatchInstantly(cmd, cmd);

        final long rejectedSize = cmd.size - filledSize;

        // 如果没有完全匹配，发送拒绝事件
        if (rejectedSize != 0) {
            eventsHelper.attachRejectEvent(cmd, rejectedSize);
        }
    }

    // 处理FOK_BUDGET订单（Fill Or Kill with Budget）
    private void newOrderMatchFokBudget(final OrderCommand cmd) {
        // 检查是否有足够的预算来完成订单
        final long budget = checkBudgetToFill(cmd.action, cmd.size);

        if (logDebug) log.debug("预算计算: {} 请求: {}", budget, cmd.price);

        // 如果预算足够，尝试立即匹配
        if (isBudgetLimitSatisfied(cmd.action, budget, cmd.price)) {
            tryMatchInstantly(cmd, cmd);
        } else {
            // 否则发送拒绝事件
            eventsHelper.attachRejectEvent(cmd, cmd.size);
        }
    }

    // 判断预算限制是否满足
    private boolean isBudgetLimitSatisfied(final OrderAction orderAction, final long calculated, final long limit) {
        return calculated != Long.MAX_VALUE
                && (calculated == limit || (orderAction == OrderAction.BID ^ calculated > limit));
    }

    // 检查是否有足够的预算来匹配订单
    private long checkBudgetToFill(final OrderAction action, long size) {
        DirectOrder makerOrder = (action == OrderAction.BID) ? bestAskOrder : bestBidOrder;

        long budget = 0L;

        // 遍历所有的订单
        while (makerOrder != null) {
            final Bucket bucket = makerOrder.parent;

            final long availableSize = bucket.volume;
            final long price = makerOrder.price;

            if (size > availableSize) {
                size -= availableSize;
                budget += availableSize * price;
                if (logDebug) log.debug("添加 {} * {} -> {}", price, availableSize, budget);
            } else {
                if (logDebug) log.debug("返回 {} * {} -> {}", price, size, budget + size * price);
                return budget + size * price;
            }

            // 切换到下一个订单（可以为null）
            makerOrder = bucket.tail.prev;
        }
        if (logDebug) log.debug("没有足够的流动性来填充订单，size={}", size);
        return Long.MAX_VALUE;
    }

    // 尝试立即匹配订单
    private long tryMatchInstantly(final IOrder takerOrder, final OrderCommand triggerCmd) {
        // 判断买入还是卖出
        final boolean isBidAction = takerOrder.getAction() == OrderAction.BID;
        // 判断是否需要限制价格
        // 如果订单类型是 FOK（Fill or Kill，即全部成交或撤单），并且是卖单（!isBidAction），则限制价格为 0L。否则，限制价格为 takerOrder 的价格
        final long limitPrice = (triggerCmd.command == OrderCommandType.PLACE_ORDER && triggerCmd.orderType == OrderType.FOK_BUDGET && !isBidAction)
                ? 0L
                : takerOrder.getPrice();

        // 选择最佳挂单
        // 如果没有合适的挂单，则返回已经成交的数量
        DirectOrder makerOrder;
        if (isBidAction) {
            makerOrder = bestAskOrder;
            if (makerOrder == null || makerOrder.price > limitPrice) {
                return takerOrder.getFilled();
            }
        } else {
            makerOrder = bestBidOrder;
            if (makerOrder == null || makerOrder.price < limitPrice) {
                return takerOrder.getFilled();
            }
        }

        // 找到合适的挂单

        // 计算 takerOrder 还有多少未成交的数量（remainingSize）。如果没有剩余的数量，直接返回已成交的数量
        long remainingSize = takerOrder.getSize() - takerOrder.getFilled();
        if (remainingSize == 0) {
            return takerOrder.getFilled();
        }

        // 遍历订单簿进行撮合

        // 获取当前价格桶的尾部订单，用于处理价格桶中的订单
        DirectOrder priceBucketTail = makerOrder.parent.tail;

        final long takerReserveBidPrice = takerOrder.getReserveBidPrice();

        MatcherTradeEvent eventsTail = null;

        // 遍历所有订单
        do {
            // 计算当前订单可以匹配的数量
            final long tradeSize = Math.min(remainingSize, makerOrder.size - makerOrder.filled);

            makerOrder.filled += tradeSize;
            makerOrder.parent.volume -= tradeSize;
            remainingSize -= tradeSize;

            // 从订单簿中移除已完成的订单
            final boolean makerCompleted = makerOrder.size == makerOrder.filled;
            if (makerCompleted) {
                makerOrder.parent.numOrders--;
            }

            final MatcherTradeEvent tradeEvent = eventsHelper.sendTradeEvent(makerOrder, makerCompleted, remainingSize == 0, tradeSize,
                    isBidAction ? takerReserveBidPrice : makerOrder.reserveBidPrice);

            if (eventsTail == null) {
                triggerCmd.matcherEvent = tradeEvent;
            } else {
                eventsTail.nextEvent = tradeEvent;
            }
            eventsTail = tradeEvent;

            if (!makerCompleted) {
                break;
            }

            // 如果订单已完成，从订单簿中移除
            orderIdIndex.remove(makerOrder.orderId);
            objectsPool.put(ObjectsPool.DIRECT_ORDER, makerOrder);

            if (makerOrder == priceBucketTail) {
                // 如果到达当前价格的尾部，移除价格桶引用
                final LongAdaptiveRadixTreeMap<Bucket> buckets = isBidAction ? askPriceBuckets : bidPriceBuckets;
                buckets.remove(makerOrder.price);
                objectsPool.put(ObjectsPool.DIRECT_BUCKET, makerOrder.parent);

                if (makerOrder.prev != null) {
                    priceBucketTail = makerOrder.prev.parent.tail;
                }
            }

            makerOrder = makerOrder.prev;

        } while (makerOrder != null
                && remainingSize > 0
                && (isBidAction ? makerOrder.price <= limitPrice : makerOrder.price >= limitPrice));

        // 更新最佳订单引用
        if (isBidAction) {
            bestAskOrder = makerOrder;
        } else {
            bestBidOrder = makerOrder;
        }

        return takerOrder.getSize() - remainingSize;
    }


    @Override
    public CommandResultCode cancelOrder(OrderCommand cmd) {

        // 查找订单，如果订单不存在或订单的UID不匹配，则返回未知订单ID错误
        final DirectOrder order = orderIdIndex.get(cmd.orderId);
        if (order == null || order.uid != cmd.uid) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        // 从订单索引中移除订单
        orderIdIndex.remove(cmd.orderId);
        // 将订单放回对象池中
        objectsPool.put(ObjectsPool.DIRECT_ORDER, order);

        // 删除订单并获取空闲的桶
        final Bucket freeBucket = removeOrder(order);
        if (freeBucket != null) {
            // 将空闲桶放回对象池
            objectsPool.put(ObjectsPool.DIRECT_BUCKET, freeBucket);
        }

        // 填充命令的action字段，以便后续事件处理
        cmd.action = order.getAction();

        // 发送减少事件
        cmd.matcherEvent = eventsHelper.sendReduceEvent(order, order.getSize() - order.getFilled(), true);

        return CommandResultCode.SUCCESS;
    }

    @Override
    public CommandResultCode reduceOrder(OrderCommand cmd) {

        final long orderId = cmd.orderId;
        final long requestedReduceSize = cmd.size;

        // 如果减少的尺寸无效，返回错误
        if (requestedReduceSize <= 0) {
            return CommandResultCode.MATCHING_REDUCE_FAILED_WRONG_SIZE;
        }

        // 查找订单
        final DirectOrder order = orderIdIndex.get(orderId);
        if (order == null || order.uid != cmd.uid) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        final long remainingSize = order.size - order.filled;
        // 计算最大可减少的尺寸
        final long reduceBy = Math.min(remainingSize, requestedReduceSize);
        final boolean canRemove = reduceBy == remainingSize;

        if (canRemove) {
            // 如果可以完全移除订单

            // 从订单索引中移除订单
            orderIdIndex.remove(orderId);
            // 将订单放回对象池
            objectsPool.put(ObjectsPool.DIRECT_ORDER, order);

            // 删除订单并获取空闲的桶
            final Bucket freeBucket = removeOrder(order);
            if (freeBucket != null) {
                // 将空闲桶放回对象池
                objectsPool.put(ObjectsPool.DIRECT_BUCKET, freeBucket);
            }

        } else {
            // 如果不能完全移除，减少订单的尺寸
            order.size -= reduceBy;
            order.parent.volume -= reduceBy;
        }

        // 发送减少事件
        cmd.matcherEvent = eventsHelper.sendReduceEvent(order, reduceBy, canRemove);

        // 填充命令的action字段
        cmd.action = order.getAction();

        return CommandResultCode.SUCCESS;
    }

    @Override
    public CommandResultCode moveOrder(OrderCommand cmd) {

        // 查找要移动的订单
        final DirectOrder orderToMove = orderIdIndex.get(cmd.orderId);
        if (orderToMove == null || orderToMove.uid != cmd.uid) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        // 对于交易对的买单进行风险检查
        if (symbolSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR && orderToMove.action == OrderAction.BID && cmd.price > orderToMove.reserveBidPrice) {
            return CommandResultCode.MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT;
        }

        // 移除订单并获取空闲桶
        final Bucket freeBucket = removeOrder(orderToMove);

        // 更新订单的价格
        orderToMove.price = cmd.price;

        // 填充命令的action字段
        cmd.action = orderToMove.getAction();

        // 尝试立即匹配订单
        final long filled = tryMatchInstantly(orderToMove, cmd);
        if (filled == orderToMove.size) {
            // 如果订单已完全匹配，移除订单
            orderIdIndex.remove(cmd.orderId);
            // 将订单放回对象池
            objectsPool.put(ObjectsPool.DIRECT_ORDER, orderToMove);
            return CommandResultCode.SUCCESS;
        }

        // 如果订单没有完全匹配，更新订单已成交量
        orderToMove.filled = filled;

        // 将订单插入到新的位置
        insertOrder(orderToMove, freeBucket);

        return CommandResultCode.SUCCESS;
    }

    // 移除订单并返回相应的桶
    private Bucket removeOrder(final DirectOrder order) {

        final Bucket bucket = order.parent;
        // 减少桶的成交量和订单数量
        bucket.volume -= order.size - order.filled;
        bucket.numOrders--;
        Bucket bucketRemoved = null;

        if (bucket.tail == order) {
            // 如果要移除的订单是桶的尾部订单
            if (order.next == null || order.next.parent != bucket) {
                // 如果没有下一个订单或者下一个订单的父桶不是当前桶，说明该桶已经空了，需要从订单簿中移除
                final LongAdaptiveRadixTreeMap<Bucket> buckets = order.action == OrderAction.ASK ? askPriceBuckets : bidPriceBuckets;
                buckets.remove(order.price);
                bucketRemoved = bucket;
            } else {
                // 如果桶中还有其他订单，更新尾部引用
                bucket.tail = order.next; // 始终不为空
            }
        }

        // 更新邻居订单的前后指针
        if (order.next != null) {
            order.next.prev = order.prev; // 可能为空
        }
        if (order.prev != null) {
            order.prev.next = order.next; // 可能为空
        }

        // 检查最佳卖单或买单是否引用了刚刚移除的订单
        if (order == bestAskOrder) {
            bestAskOrder = order.prev; // 可能为空
        } else if (order == bestBidOrder) {
            bestBidOrder = order.prev; // 可能为空
        }

        return bucketRemoved;
    }

    private void insertOrder(final DirectOrder order, final Bucket freeBucket) {

        // 判断当前订单是买单还是卖单
        final boolean isAsk = order.action == OrderAction.ASK;
        // 根据订单类型选择相应的订单簿（买单簿或卖单簿）
        final LongAdaptiveRadixTreeMap<Bucket> buckets = isAsk ? askPriceBuckets : bidPriceBuckets;
        // 获取对应价格的桶
        final Bucket toBucket = buckets.get(order.price);

        if (toBucket != null) {
            // 如果价格桶已存在

            // 如果有空闲桶，将其放回对象池
            if (freeBucket != null) {
                objectsPool.put(ObjectsPool.DIRECT_BUCKET, freeBucket);
            }

            // 更新该价格桶的成交量
            toBucket.volume += order.size - order.filled;
            // 更新该价格桶的订单数量
            toBucket.numOrders++;

            // 获取原来尾部订单
            final DirectOrder oldTail = toBucket.tail;
            // 获取原尾部订单的前一个订单（可能为空）
            final DirectOrder prevOrder = oldTail.prev;

            // 更新尾部订单
            toBucket.tail = order;
            oldTail.prev = order;
            if (prevOrder != null) {
                prevOrder.next = order;
            }

            // 更新当前订单的前后指针
            order.next = oldTail;
            order.prev = prevOrder;
            order.parent = toBucket; // 将当前订单的父桶指向该价格桶，

        } else {
            // 如果价格桶不存在，需要新建一个桶

            // 如果有空闲桶，复用空闲桶，否则从对象池中获取新桶
            final Bucket newBucket = freeBucket != null
                    ? freeBucket
                    : objectsPool.get(ObjectsPool.DIRECT_BUCKET, Bucket::new);

            // 将当前订单设为新桶的尾部
            newBucket.tail = order;
            newBucket.volume = order.size - order.filled; // 设置成交量
            newBucket.numOrders = 1; // 新桶中只有一个订单
            order.parent = newBucket; // 当前订单的父桶指向新桶
            buckets.put(order.price, newBucket); // 将新桶插入到订单簿中

            // 获取低价（对于卖单是低价，买单是高价）的桶
            final Bucket lowerBucket = isAsk ? buckets.getLowerValue(order.price) : buckets.getHigherValue(order.price);
            if (lowerBucket != null) {
                // 如果存在低价（或高价）桶，更新邻居指针
                DirectOrder lowerTail = lowerBucket.tail;
                final DirectOrder prevOrder = lowerTail.prev; // 获取前一个订单（可能为空）

                // 更新低价（或高价）桶尾部订单的前一个指针
                lowerTail.prev = order;
                if (prevOrder != null) {
                    prevOrder.next = order; // 更新前一个订单的下一个指针
                }

                // 更新当前订单的前后指针
                order.next = lowerTail;
                order.prev = prevOrder;
            } else {
                // 如果没有低价（或高价）桶，说明当前订单是最佳订单，更新最佳订单的引用

                // 获取当前的最佳订单（可能为空）
                final DirectOrder oldBestOrder = isAsk ? bestAskOrder : bestBidOrder;

                if (oldBestOrder != null) {
                    oldBestOrder.next = order; // 更新旧的最佳订单的下一个订单指针
                }

                // 更新最佳订单
                if (isAsk) {
                    bestAskOrder = order; // 更新卖单的最佳订单
                } else {
                    bestBidOrder = order; // 更新买单的最佳订单
                }

                // 当前订单没有下一个订单
                order.next = null;
                // 当前订单的前一个订单是旧的最佳订单
                order.prev = oldBestOrder;
            }
        }
    }


    @Override
    public int getOrdersNum(OrderAction action) {
        // 根据传入的 action 参数选择 ASK 或 BID 的价格桶集合
        final LongAdaptiveRadixTreeMap<Bucket> buckets = action == OrderAction.ASK ? askPriceBuckets : bidPriceBuckets;
        // 使用 MutableInteger 来累加订单数量
        final MutableInteger accum = new MutableInteger();
        buckets.forEach((p, b) -> accum.value += b.numOrders, Integer.MAX_VALUE);
        return accum.value;
    }

    @Override
    public long getTotalOrdersVolume(OrderAction action) {
        // 根据传入的 action 参数选择 ASK 或 BID 的价格桶集合
        final LongAdaptiveRadixTreeMap<Bucket> buckets = action == OrderAction.ASK ? askPriceBuckets : bidPriceBuckets;
        // 使用 MutableLong 来累加订单的成交量
        final MutableLong accum = new MutableLong();
        buckets.forEach((p, b) -> accum.value += b.volume, Integer.MAX_VALUE);
        return accum.value;
    }

    @Override
    public IOrder getOrderById(final long orderId) {
        // 从订单 ID 索引中获取订单
        return orderIdIndex.get(orderId);
    }

    @Override
    public void validateInternalState() {
        // 创建一个映射用于验证订单链中的订单
        final Long2ObjectHashMap<DirectOrder> ordersInChain = new Long2ObjectHashMap<>(orderIdIndex.size(Integer.MAX_VALUE), 0.8f);
        // 验证 ASK 链和 BID 链
        validateChain(true, ordersInChain);
        validateChain(false, ordersInChain);
        // 验证订单索引中的订单是否都存在于链中
        orderIdIndex.forEach((k, v) -> {
            if (ordersInChain.remove(k) != v) {
                thrw("chained orders does not contain orderId=" + k);
            }
        }, Integer.MAX_VALUE);

        // 确保链中没有遗漏的订单
        if (ordersInChain.size() != 0) {
            thrw("orderIdIndex does not contain each order from chains");
        }
    }

    private void validateChain(boolean asksChain, Long2ObjectHashMap<DirectOrder> ordersInChain) {
        // 根据传入的 asksChain 参数选择验证 ASK 链或 BID 链
        final LongAdaptiveRadixTreeMap<Bucket> buckets = asksChain ? askPriceBuckets : bidPriceBuckets;
        final LongObjectHashMap<Bucket> bucketsFoundInChain = new LongObjectHashMap<>();
        // 验证桶的内部状态
        buckets.validateInternalState();

        // 获取最优 ASK 或 BID 订单
        DirectOrder order = asksChain ? bestAskOrder : bestBidOrder;

        // 验证最优订单的 next 引用是否为 null
        if (order != null && order.next != null) {
            thrw("best order has not-null next reference");
        }
        long lastPrice = -1;
        long expectedBucketVolume = 0;
        int expectedBucketOrders = 0;
        DirectOrder lastOrder = null;

        while (order != null) {
            // 检查链中的订单是否有重复的 orderId
            if (ordersInChain.containsKey(order.orderId)) {
                thrw("duplicate orderid in the chain");
            }
            ordersInChain.put(order.orderId, order);

            // 累加订单的体积和订单数量
            expectedBucketVolume += order.size - order.filled;
            expectedBucketOrders++;

            // 验证订单的 next 引用是否正确
            if (lastOrder != null && order.next != lastOrder) {
                thrw("incorrect next reference");
            }

            // 验证父桶的尾部订单价格是否与当前订单价格一致
            if (order.parent.tail.price != order.price) {
                thrw("price of parent.tail differs");
            }

            // 验证价格变化的方向是否符合预期
            if (lastPrice != -1 && order.price != lastPrice) {
                if (asksChain ^ order.price > lastPrice) {
                    thrw("unexpected price change direction");
                }
                if (order.next.parent == order.parent) {
                    thrw("unexpected price change within same bucket");
                }
            }

            // 如果当前订单是父桶的尾部订单，验证桶的体积和订单数量
            if (order.parent.tail == order) {
                if (order.parent.volume != expectedBucketVolume) {
                    thrw("bucket volume does not match orders chain sizes");
                }
                if (order.parent.numOrders != expectedBucketOrders) {
                    thrw("bucket numOrders does not match orders chain length");
                }
                if (order.prev != null && order.prev.price == order.price) {
                    thrw("previous bucket has the same price");
                }
                expectedBucketVolume = 0;
                expectedBucketOrders = 0;
            }

            // 验证桶的价格是否唯一
            final Bucket knownBucket = bucketsFoundInChain.get(order.price);
            if (knownBucket == null) {
                bucketsFoundInChain.put(order.price, order.parent);
            } else if (knownBucket != order.parent) {
                thrw("found two different buckets having same price");
            }

            // 验证订单的 action 是否符合预期
            if (asksChain ^ order.action == OrderAction.ASK) {
                thrw("not expected order action");
            }

            lastPrice = order.price;
            lastOrder = order;
            order = order.prev;
        }

        // 验证最后一个订单是否是尾部订单
        if (lastOrder != null && lastOrder.parent.tail != lastOrder) {
            thrw("last order is not a tail");
        }

        // 遍历桶，验证是否有在价格树中找不到的桶
        buckets.forEach((price, bucket) -> {
            if (bucketsFoundInChain.remove(price) != bucket) thrw("bucket in the price-tree not found in the chain");
        }, Integer.MAX_VALUE);

        // 验证所有桶是否都能在链中找到
        if (!bucketsFoundInChain.isEmpty()) {
            thrw("found buckets in the chain that not discoverable from the price-tree");
        }
    }

    // 抛出非法状态异常
    private void thrw(final String msg) {
        throw new IllegalStateException(msg);
    }

    @Override
    public OrderBookImplType getImplementationType() {
        return OrderBookImplType.DIRECT;
    }

    @Override
    public List<Order> findUserOrders(long uid) {
        // 查找指定用户的所有订单
        final List<Order> list = new ArrayList<>();
        orderIdIndex.forEach((orderId, order) -> {
            if (order.uid == uid) {
                list.add(Order.builder()
                        .orderId(orderId)
                        .price(order.price)
                        .size(order.size)
                        .filled(order.filled)
                        .reserveBidPrice(order.reserveBidPrice)
                        .action(order.action)
                        .uid(order.uid)
                        .timestamp(order.timestamp)
                        .build());
            }
        }, Integer.MAX_VALUE);

        return list;
    }

    @Override
    public CoreSymbolSpecification getSymbolSpec() {
        return symbolSpec;
    }

    @Override
    public Stream<DirectOrder> askOrdersStream(boolean sortedIgnore) {
        // 返回ASK订单的流
        return StreamSupport.stream(new OrdersSpliterator(bestAskOrder), false);
    }

    @Override
    public Stream<DirectOrder> bidOrdersStream(boolean sortedIgnore) {
        // 返回BID订单的流
        return StreamSupport.stream(new OrdersSpliterator(bestBidOrder), false);
    }

    @Override
    public void fillAsks(final int size, L2MarketData data) {
        data.askSize = 0;
        askPriceBuckets.forEach((p, bucket) -> {
            final int i = data.askSize++;
            data.askPrices[i] = bucket.tail.price;
            data.askVolumes[i] = bucket.volume;
            data.askOrders[i] = bucket.numOrders;
        }, size);
    }

    @Override
    public void fillBids(final int size, L2MarketData data) {
        data.bidSize = 0;
        bidPriceBuckets.forEachDesc((p, bucket) -> {
            final int i = data.bidSize++;
            data.bidPrices[i] = bucket.tail.price;
            data.bidVolumes[i] = bucket.volume;
            data.bidOrders[i] = bucket.numOrders;
        }, size);
    }

    @Override
    public int getTotalAskBuckets(final int limit) {
        return askPriceBuckets.size(limit);
    }

    @Override
    public int getTotalBidBuckets(final int limit) {
        return bidPriceBuckets.size(limit);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        // 序列化 OrderBook 的实现类型、符号规范和订单数据
        bytes.writeByte(getImplementationType().getCode());
        symbolSpec.writeMarshallable(bytes);
        bytes.writeInt(orderIdIndex.size(Integer.MAX_VALUE));
        askOrdersStream(true).forEach(order -> order.writeMarshallable(bytes));
        bidOrdersStream(true).forEach(order -> order.writeMarshallable(bytes));
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static final class DirectOrder implements WriteBytesMarshallable, IOrder {

        @Getter
        public long orderId;  // 订单ID

        @Getter
        public long price;  // 价格

        @Getter
        public long size;  // 订单数量

        @Getter
        public long filled;  // 已成交数量

        // 新订单的保留买单价格，用于快速移动GTC买单
        @Getter
        public long reserveBidPrice;  // 保留价格

        // 仅在下单时使用
        @Getter
        public OrderAction action;  // 订单动作（买或卖）

        @Getter
        public long uid;  // 用户ID

        @Getter
        public long timestamp;  // 时间戳

        // 快速订单结构
        Bucket parent;  // 父级桶，用于组织订单

        // 下一个订单（根据匹配方向，卖单的价格递增）
        DirectOrder next;  // 下一个订单

        // 上一个订单（队列的尾部，优先级较低，价格较差，按匹配方向）
        DirectOrder prev;  // 上一个订单

        // 用户Cookie（暂时未使用）
        // public int userCookie;

        /**
         * 构造函数，通过读取字节数据初始化订单
         *
         * @param bytes 字节输入流
         */
        public DirectOrder(BytesIn bytes) {
            this.orderId = bytes.readLong(); // 订单ID
            this.price = bytes.readLong();  // 价格
            this.size = bytes.readLong(); // 数量
            this.filled = bytes.readLong(); // 已成交数量
            this.reserveBidPrice = bytes.readLong(); // 保留价格
            this.action = OrderAction.of(bytes.readByte());  // 订单动作
            this.uid = bytes.readLong(); // 用户ID
            this.timestamp = bytes.readLong(); // 时间戳
            // this.userCookie = bytes.readInt();  // 用户Cookie（暂时未使用）

            // TODO: 需要处理的部分
        }

        /**
         * 序列化方法，将订单数据写入字节流
         *
         * @param bytes 字节输出流
         */
        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeLong(orderId);  // 写入订单ID
            bytes.writeLong(price);  // 写入价格
            bytes.writeLong(size);  // 写入数量
            bytes.writeLong(filled);  // 写入已成交数量
            bytes.writeLong(reserveBidPrice);  // 写入保留价格
            bytes.writeByte(action.getCode());  // 写入订单动作
            bytes.writeLong(uid);  // 写入用户ID
            bytes.writeLong(timestamp);  // 写入时间戳
            // bytes.writeInt(userCookie);  // 写入用户Cookie（暂时未使用）
            // TODO: 需要处理的部分
        }

        /**
         * 转换成字符串形式，便于调试
         *
         * @return 订单的字符串表示
         */
        @Override
        public String toString() {
            return "[" + orderId + " " + (action == OrderAction.ASK ? 'A' : 'B')
                    + price + ":" + size + "F" + filled
                    // + " C" + userCookie  // 用户Cookie（暂时未使用）
                    + " U" + uid + "]";
        }

        /**
         * 计算对象的哈希值
         *
         * @return 订单对象的哈希值
         */
        @Override
        public int hashCode() {
            return Objects.hash(orderId, action, price, size, reserveBidPrice, filled,
                    //userCookie,  // 用户Cookie（暂时未使用）
                    uid);
        }

        /**
         * 比较两个订单对象是否相等（忽略 timestamp 和 userCookie）
         *
         * @param o 另一个订单对象
         * @return 如果两个订单相等，返回 true；否则返回 false
         */
        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null) return false;
            if (!(o instanceof DirectOrder)) return false;

            DirectOrder other = (DirectOrder) o;

            // 忽略 userCookie 和 timestamp
            return orderId == other.orderId
                    && action == other.action
                    && price == other.price
                    && size == other.size
                    && reserveBidPrice == other.reserveBidPrice
                    && filled == other.filled
                    && uid == other.uid;
        }

        /**
         * 根据订单的核心属性计算哈希值，忽略时间戳和用户Cookie
         *
         * @return 订单的状态哈希值
         */
        @Override
        public int stateHash() {
            return Objects.hash(orderId, action, price, size, reserveBidPrice, filled,
                    //userCookie,  // 用户Cookie（暂时未使用）
                    uid);
        }
    }


    @ToString
    private static class Bucket {

        // 总交易量
        long volume;

        // 订单数量
        int numOrders;

        // 链表尾部的订单对象，指向最后一个订单
        DirectOrder tail;

    }

}
