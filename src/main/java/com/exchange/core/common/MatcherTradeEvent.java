package com.exchange.core.common;


import com.exchange.core.common.constant.MatcherEventType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// TODO move activeOrderCompleted, eventType, section into the order?
// TODO REDUCE needs remaining size (can write into size), bidderHoldPrice - can write into price
// TODO REJECT needs remaining size (can not write into size),

@AllArgsConstructor
@NoArgsConstructor
@Builder
public final class MatcherTradeEvent {

    // 事件类型，TRADE、REDUCE、REJECT 或 BINARY_EVENT
    public MatcherEventType eventType;

    // 事件所属的区段
    public int section;

    // 是否已完成活动订单
    // 默认情况下，除了当活动订单完全成交、被移除或被拒绝时为真；
    // 对于 REJECT 事件来说，一定为真；
    // 对于 REDUCE 事件，如果减仓是由 COMMAND 触发的，则为真
    public boolean activeOrderCompleted;

    // 对于 TRADE 类型的事件，这是 maker 订单的信息
    // maker 订单 ID
    public long matchedOrderId;

    // matchedOrderUid 为 0 时，表示该订单被拒绝
    public long matchedOrderUid;

    // matchedOrderCompleted 表示该匹配订单是否已经完成
    // 默认为 false，只有在匹配订单完全成交时才为真
    public boolean matchedOrderCompleted;

    // 交易的实际价格（来自 maker 订单），如果是拒绝事件，则价格为 0
    // 对于 REJECT 事件，可以从原始订单中取价格
    public long price;

    // 交易的大小（对于 TRADE 事件），
    // 对于 REDUCE 事件，表示 REDUCE 命令的有效减仓量，或 CANCEL 命令未成交的订单量
    // 对于 REJECT 事件，表示被拒绝订单的未成交部分
    public long size;

    // timestamp 时间戳，表示活动订单相关的事件时间（注释掉，未启用）
    // public long timestamp;

    // 投标人持有价格，依赖于活动订单的动作（买卖等）
    public long bidderHoldPrice;

    // 当前事件的下一个事件（链式结构）
    public MatcherTradeEvent nextEvent;

    // 仅用于测试，复制当前事件
    public MatcherTradeEvent copy() {
        MatcherTradeEvent evt = new MatcherTradeEvent();
        evt.eventType = this.eventType;
        evt.section = this.section;
        evt.activeOrderCompleted = this.activeOrderCompleted;
        evt.matchedOrderId = this.matchedOrderId;
        evt.matchedOrderUid = this.matchedOrderUid;
        evt.matchedOrderCompleted = this.matchedOrderCompleted;
        evt.price = this.price;
        evt.size = this.size;
        // evt.timestamp = this.timestamp;
        evt.bidderHoldPrice = this.bidderHoldPrice;
        return evt;
    }

    // 仅用于测试，查找链表末尾事件
    public MatcherTradeEvent findTail() {
        MatcherTradeEvent tail = this;
        while (tail.nextEvent != null) {
            tail = tail.nextEvent;
        }
        return tail;
    }

    // 获取链表的大小
    public int getChainSize() {
        MatcherTradeEvent tail = this;
        int c = 1;
        while (tail.nextEvent != null) {
            tail = tail.nextEvent;
            c++;
        }
        return c;
    }

    // 创建事件链，链长为 chainLength
    @NotNull
    public static MatcherTradeEvent createEventChain(int chainLength) {
        final MatcherTradeEvent head = new MatcherTradeEvent();
        MatcherTradeEvent prev = head;
        for (int j = 1; j < chainLength; j++) {
            MatcherTradeEvent nextEvent = new MatcherTradeEvent();
            prev.nextEvent = nextEvent;
            prev = nextEvent;
        }
        return head;
    }

    // 仅用于测试，将事件链转换为 List
    public static List<MatcherTradeEvent> asList(MatcherTradeEvent next) {
        List<MatcherTradeEvent> list = new ArrayList<>();
        while (next != null) {
            list.add(next);
            next = next.nextEvent;
        }
        return list;
    }

    /**
     * 比较两个事件链是否相等（包括链中的所有事件）
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof MatcherTradeEvent)) return false;
        MatcherTradeEvent other = (MatcherTradeEvent) o;

        // 忽略 timestamp（未启用）
        return section == other.section
                && activeOrderCompleted == other.activeOrderCompleted
                && matchedOrderId == other.matchedOrderId
                && matchedOrderUid == other.matchedOrderUid
                && matchedOrderCompleted == other.matchedOrderCompleted
                && price == other.price
                && size == other.size
                && bidderHoldPrice == other.bidderHoldPrice
                && ((nextEvent == null && other.nextEvent == null) || (nextEvent != null && nextEvent.equals(other.nextEvent)));
    }

    /**
     * 包括事件链的哈希计算
     */
    @Override
    public int hashCode() {
        return Objects.hash(
                section,
                activeOrderCompleted,
                matchedOrderId,
                matchedOrderUid,
                matchedOrderCompleted,
                price,
                size,
                bidderHoldPrice,
                nextEvent);
    }

    @Override
    public String toString() {
        return "MatcherTradeEvent{" +
                "eventType=" + eventType +
                ", section=" + section +
                ", activeOrderCompleted=" + activeOrderCompleted +
                ", matchedOrderId=" + matchedOrderId +
                ", matchedOrderUid=" + matchedOrderUid +
                ", matchedOrderCompleted=" + matchedOrderCompleted +
                ", price=" + price +
                ", size=" + size +
                // ", timestamp=" + timestamp +
                ", bidderHoldPrice=" + bidderHoldPrice +
                ", nextEvent=" + (nextEvent != null) +
                '}';
    }
}

