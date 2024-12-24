package com.exchange.core.orderbook;


import com.exchange.core.common.IOrder;
import com.exchange.core.common.MatcherTradeEvent;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.constant.MatcherEventType;
import com.exchange.core.utils.SerializationUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.wire.Wire;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.exchange.core.ExchangeCore.EVENTS_POOLING;


@Slf4j
@RequiredArgsConstructor
public final class OrderBookEventsHelper {

    // 定义一个非池化的事件帮助工具
    public static final OrderBookEventsHelper NON_POOLED_EVENTS_HELPER = new OrderBookEventsHelper(MatcherTradeEvent::new);

    // 事件链的供应器，用于创建新的 MatcherTradeEvent 对象
    private final Supplier<MatcherTradeEvent> eventChainsSupplier;

    // 事件链的头部
    private MatcherTradeEvent eventsChainHead;

    /**
     * 发送交易事件
     * 
     * @param matchingOrder 匹配的订单
     * @param makerCompleted 是否maker订单完成
     * @param takerCompleted 是否taker订单完成
     * @param size 交易的数量
     * @param bidderHoldPrice 持有的出价
     * @return 创建的交易事件
     */
    public MatcherTradeEvent sendTradeEvent(final IOrder matchingOrder,
                                            final boolean makerCompleted,
                                            final boolean takerCompleted,
                                            final long size,
                                            final long bidderHoldPrice) {

        // 创建新的交易事件
        final MatcherTradeEvent event = newMatcherEvent();

        // 设置事件类型为交易
        event.eventType = MatcherEventType.TRADE;
        event.section = 0;

        event.activeOrderCompleted = takerCompleted;

        // 设置匹配的订单信息
        event.matchedOrderId = matchingOrder.getOrderId();
        event.matchedOrderUid = matchingOrder.getUid();
        event.matchedOrderCompleted = makerCompleted;

        // 设置交易的价格和数量
        event.price = matchingOrder.getPrice();
        event.size = size;

        // 设置持有的出价
        event.bidderHoldPrice = bidderHoldPrice;

        return event;
    }

    /**
     * 发送减少事件
     * 
     * @param order 订单
     * @param reduceSize 减少的数量
     * @param completed 是否完成
     * @return 创建的减少事件
     */
    public MatcherTradeEvent sendReduceEvent(final IOrder order, final long reduceSize, final boolean completed) {
        // 创建新的减少事件
        final MatcherTradeEvent event = newMatcherEvent();
        event.eventType = MatcherEventType.REDUCE;
        event.section = 0;
        event.activeOrderCompleted = completed;

        // 设置减少的数量
        event.matchedOrderId = 0;
        event.matchedOrderCompleted = false;
        event.price = order.getPrice();
        event.size = reduceSize;

        // 设置持有的出价
        event.bidderHoldPrice = order.getReserveBidPrice();

        return event;
    }

    /**
     * 附加拒绝事件
     * 
     * @param cmd 订单命令
     * @param rejectedSize 被拒绝的数量
     */
    public void attachRejectEvent(final OrderCommand cmd, final long rejectedSize) {
        // 创建新的拒绝事件
        final MatcherTradeEvent event = newMatcherEvent();

        event.eventType = MatcherEventType.REJECT;
        event.section = 0;
        event.activeOrderCompleted = true;
        event.matchedOrderId = 0;
        event.matchedOrderCompleted = false;

        // 设置价格和拒绝的数量
        event.price = cmd.price;
        event.size = rejectedSize;

        // 设置持有的出价
        event.bidderHoldPrice = cmd.reserveBidPrice;

        // 将事件插入到命令的事件链中
        event.nextEvent = cmd.matcherEvent;
        cmd.matcherEvent = event;
    }

    /**
     * 创建二进制事件链
     * 
     * @param timestamp 时间戳
     * @param section 部分
     * @param bytes 二进制数据
     * @return 创建的事件链的头部
     */
    public MatcherTradeEvent createBinaryEventsChain(final long timestamp,
                                                     final int section,
                                                     final NativeBytes<Void> bytes) {

        long[] dataArray = SerializationUtils.bytesToLongArray(bytes, 5);

        MatcherTradeEvent firstEvent = null;
        MatcherTradeEvent lastEvent = null;
        for (int i = 0; i < dataArray.length; i += 5) {

            // 创建新的事件
            final MatcherTradeEvent event = newMatcherEvent();

            event.eventType = MatcherEventType.BINARY_EVENT;
            event.section = section;
            event.matchedOrderId = dataArray[i];
            event.matchedOrderUid = dataArray[i + 1];
            event.price = dataArray[i + 2];
            event.size = dataArray[i + 3];
            event.bidderHoldPrice = dataArray[i + 4];

            event.nextEvent = null;

            // 链接事件
            if (firstEvent == null) {
                firstEvent = event;
            } else {
                lastEvent.nextEvent = event;
            }
            lastEvent = event;
        }

        return firstEvent;
    }

    /**
     * 反序列化事件
     * 
     * @param cmd 订单命令
     * @return 反序列化后的事件
     */
    public static NavigableMap<Integer, Wire> deserializeEvents(final OrderCommand cmd) {

        // 存储每个部分的事件
        final Map<Integer, List<MatcherTradeEvent>> sections = new HashMap<>();
        cmd.processMatcherEvents(evt -> sections.computeIfAbsent(evt.section, k -> new ArrayList<>()).add(evt));

        NavigableMap<Integer, Wire> result = new TreeMap<>();

        sections.forEach((section, events) -> {
            // 将事件数据转换为长整型数组
            final long[] dataArray = events.stream()
                    .flatMap(evt -> Stream.of(
                            evt.matchedOrderId,
                            evt.matchedOrderUid,
                            evt.price,
                            evt.size,
                            evt.bidderHoldPrice))
                    .mapToLong(s -> s)
                    .toArray();

            // 序列化为 Wire 格式
            final Wire wire = SerializationUtils.longsToWire(dataArray);

            result.put(section, wire);
        });

        return result;
    }

    /**
     * 创建新的 MatcherTradeEvent 对象
     * 
     * @return 新的 MatcherTradeEvent 对象
     */
    private MatcherTradeEvent newMatcherEvent() {

        // 如果启用了事件池化
        if (EVENTS_POOLING) {
            if (eventsChainHead == null) {
                eventsChainHead = eventChainsSupplier.get();
            }
            final MatcherTradeEvent res = eventsChainHead;
            eventsChainHead = eventsChainHead.nextEvent;
            return res;
        } else {
            return new MatcherTradeEvent();
        }
    }
}
