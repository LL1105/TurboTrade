package com.exchange.core.orderbook;

import com.exchange.core.common.IOrder;
import com.exchange.core.common.MatcherTradeEvent;
import com.exchange.core.common.Order;
import com.exchange.core.common.constant.OrderAction;
import com.exchange.core.utils.SerializationUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@ToString
public final class OrdersBucketNaive implements Comparable<OrdersBucketNaive>, WriteBytesMarshallable {

    @Getter
    private final long price;  // 当前订单簿的价格

    private final LinkedHashMap<Long, Order> entries;  // 存储订单的映射，订单ID映射到 Order 对象

    @Getter
    private long totalVolume;  // 当前订单簿中的总交易量

    // 构造函数，初始化一个价格对应的订单簿
    public OrdersBucketNaive(final long price) {
        this.price = price;
        this.entries = new LinkedHashMap<>();
        this.totalVolume = 0;
    }

    // 构造函数，从字节流读取订单簿信息
    public OrdersBucketNaive(BytesIn bytes) {
        this.price = bytes.readLong();
        this.entries = SerializationUtils.readLongMap(bytes, LinkedHashMap::new, Order::new);
        this.totalVolume = bytes.readLong();
    }

    /**
     * 向订单簿中添加一个新订单
     *
     * @param order - 新订单
     */
    public void put(Order order) {
        entries.put(order.orderId, order);  // 将订单加入到订单簿
        totalVolume += order.size - order.filled;  // 更新总交易量
    }

    /**
     * 从订单簿中移除指定的订单
     *
     * @param orderId - 订单ID
     * @param uid     - 订单UID
     * @return - 被移除的订单，如果找不到则返回null
     */
    public Order remove(long orderId, long uid) {
        Order order = entries.get(orderId);
        if (order == null || order.uid != uid) {
            return null;  // 如果订单不存在或UID不匹配，返回null
        }

        entries.remove(orderId);  // 移除订单
        totalVolume -= order.size - order.filled;  // 更新总交易量
        return order;  // 返回被移除的订单
    }

    /**
     * 匹配订单簿中的订单
     * 完全匹配的订单将被移除，部分匹配的订单仍然保留在订单簿中
     *
     * @param volumeToCollect - 要收集的交易量
     * @param activeOrder     - 当前活动订单（用于获取保留买价）
     * @param helper          - 事件帮助工具，用于发送交易事件
     * @return - 匹配结果，包含匹配的事件链、匹配的交易量和需要移除的订单列表
     */
    public MatcherResult match(long volumeToCollect, IOrder activeOrder, OrderBookEventsHelper helper) {

        final Iterator<Map.Entry<Long, Order>> iterator = entries.entrySet().iterator();

        long totalMatchingVolume = 0;  // 累积的匹配交易量
        final List<Long> ordersToRemove = new ArrayList<>();  // 完全匹配的订单ID列表

        MatcherTradeEvent eventsHead = null;  // 交易事件链的头部
        MatcherTradeEvent eventsTail = null;  // 交易事件链的尾部

        // 遍历所有订单进行匹配
        while (iterator.hasNext() && volumeToCollect > 0) {
            final Map.Entry<Long, Order> next = iterator.next();
            final Order order = next.getValue();

            // 计算当前订单可以匹配的交易量
            final long v = Math.min(volumeToCollect, order.size - order.filled);
            totalMatchingVolume += v;

            order.filled += v;  // 更新订单已成交量
            volumeToCollect -= v;  // 更新剩余需要匹配的量
            totalVolume -= v;  // 更新总交易量

            // 如果订单完全匹配，则从订单簿中移除
            final boolean fullMatch = order.size == order.filled;

            final long bidderHoldPrice = order.action == OrderAction.ASK ? activeOrder.getReserveBidPrice() : order.reserveBidPrice;
            final MatcherTradeEvent tradeEvent = helper.sendTradeEvent(order, fullMatch, volumeToCollect == 0, v, bidderHoldPrice);

            // 连接交易事件链
            if (eventsTail == null) {
                eventsHead = tradeEvent;
            } else {
                eventsTail.nextEvent = tradeEvent;
            }
            eventsTail = tradeEvent;

            // 如果是完全匹配的订单，添加到移除列表并移除该订单
            if (fullMatch) {
                ordersToRemove.add(order.orderId);
                iterator.remove();
            }
        }

        // 返回匹配结果
        return new MatcherResult(eventsHead, eventsTail, totalMatchingVolume, ordersToRemove);
    }

    /**
     * 获取订单簿中的订单数量
     *
     * @return - 当前订单簿中的订单数量
     */
    public int getNumOrders() {
        return entries.size();
    }

    /**
     * 减少订单簿中的交易量
     *
     * @param reduceSize - 要减少的交易量
     */
    public void reduceSize(long reduceSize) {
        totalVolume -= reduceSize;  // 更新总交易量
    }

    /**
     * 校验订单簿的一致性
     * 检查所有订单的已成交量之和是否与 totalVolume 一致
     */
    public void validate() {
        long sum = entries.values().stream().mapToLong(c -> c.size - c.filled).sum();
        if (sum != totalVolume) {
            String msg = String.format("totalVolume=%d calculated=%d", totalVolume, sum);
            throw new IllegalStateException(msg);  // 如果不一致则抛出异常
        }
    }

    /**
     * 查找指定ID的订单
     *
     * @param orderId - 订单ID
     * @return - 找到的订单，若没有找到则返回null
     */
    public Order findOrder(long orderId) {
        return entries.get(orderId);
    }

    /**
     * 获取所有订单的列表（效率较低，仅用于测试）
     *
     * @return - 当前订单簿中的所有订单
     */
    public List<Order> getAllOrders() {
        return new ArrayList<>(entries.values());
    }

    /**
     * 对订单簿中的每个订单执行指定的操作
     *
     * @param consumer - 订单操作函数
     */
    public void forEachOrder(Consumer<Order> consumer) {
        entries.values().forEach(consumer);
    }

    /**
     * 将订单簿信息输出为单行字符串
     *
     * @return - 单行字符串表示的订单簿信息
     */
    public String dumpToSingleLine() {
        String orders = getAllOrders().stream()
                .map(o -> String.format("id%d_L%d_F%d", o.orderId, o.size, o.filled))
                .collect(Collectors.joining(", "));

        return String.format("%d : vol:%d num:%d : %s", getPrice(), getTotalVolume(), getNumOrders(), orders);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeLong(price);
        SerializationUtils.marshallLongMap(entries, bytes);
        bytes.writeLong(totalVolume);
    }

    @Override
    public int compareTo(OrdersBucketNaive other) {
        return Long.compare(this.getPrice(), other.getPrice());  // 根据价格比较订单簿
    }

    @Override
    public int hashCode() {
        return Objects.hash(price, Arrays.hashCode(entries.values().toArray(new Order[0])));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof OrdersBucketNaive)) return false;
        OrdersBucketNaive other = (OrdersBucketNaive) o;
        return price == other.getPrice()
                && getAllOrders().equals(other.getAllOrders());  // 根据价格和订单列表判断相等
    }

    // 匹配结果类，包含匹配的交易事件链、总匹配量和需要移除的订单列表
    @AllArgsConstructor
    public final class MatcherResult {
        public MatcherTradeEvent eventsChainHead;
        public MatcherTradeEvent eventsChainTail;
        public long volume;
        public List<Long> ordersToRemove;
    }

}

