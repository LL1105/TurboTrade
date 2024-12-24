package com.exchange.core.common;

import com.exchange.core.common.constant.OrderAction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Objects;

/**
 * 订单类，继承自 OrderCommand，目的是避免在匹配订单时创建新对象。
 * 主要用于即时匹配的订单（例如市场订单或可以立即匹配的限价订单），
 * 以及在订单移动时使用相同的代码进行匹配。
 * <p>
 * 该对象不允许外部引用，订单对象只在订单簿中存在。
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
public final class Order implements WriteBytesMarshallable, IOrder {

    // 订单ID
    @Getter
    public long orderId;

    // 订单价格
    @Getter
    public long price;

    // 订单数量
    @Getter
    public long size;

    // 已成交数量
    @Getter
    public long filled;

    // 新订单 - 在交换模式下，为GTC买单的快速移动保留的价格
    @Getter
    public long reserveBidPrice;

    // 仅对于 PLACE_ORDER 操作需要
    @Getter
    public OrderAction action;

    // 用户ID
    @Getter
    public long uid;

    // 时间戳
    @Getter
    public long timestamp;

    // 使用 `BytesIn` 读取字节流并将其映射到字段
    public Order(BytesIn bytes) {
        this.orderId = bytes.readLong();  // 订单ID
        this.price = bytes.readLong();    // 价格
        this.size = bytes.readLong();     // 数量
        this.filled = bytes.readLong();   // 已成交数量
        this.reserveBidPrice = bytes.readLong();  // 保留价格
        this.action = OrderAction.of(bytes.readByte()); // 订单操作类型
        this.uid = bytes.readLong();      // 用户ID
        this.timestamp = bytes.readLong(); // 时间戳
    }

    // 将订单数据写入字节流
    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeLong(orderId);          // 订单ID
        bytes.writeLong(price);            // 价格
        bytes.writeLong(size);             // 数量
        bytes.writeLong(filled);           // 已成交数量
        bytes.writeLong(reserveBidPrice);  // 保留价格
        bytes.writeByte(action.getCode()); // 订单操作类型
        bytes.writeLong(uid);              // 用户ID
        bytes.writeLong(timestamp);        // 时间戳
    }

    // 重写toString方法，用于输出订单的简洁信息
    @Override
    public String toString() {
        return "[" + orderId + " " + (action == OrderAction.ASK ? 'A' : 'B')
                + price + ":" + size + "F" + filled
                // + " C" + userCookie
                + " U" + uid + "]";
    }

    // 重写hashCode方法，生成订单对象的哈希值
    @Override
    public int hashCode() {
        return Objects.hash(orderId, action, price, size, reserveBidPrice, filled,
                //userCookie, timestamp
                uid);
    }

    /**
     * 在计算 hashCode 和 equals 时，忽略时间戳，以便获得可重复的结果
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof Order)) return false;

        Order other = (Order) o;

        // 忽略时间戳和用户cookie
        return orderId == other.orderId
                && action == other.action
                && price == other.price
                && size == other.size
                && reserveBidPrice == other.reserveBidPrice
                && filled == other.filled
                && uid == other.uid;
    }

    // 用于状态哈希计算
    @Override
    public int stateHash() {
        return hashCode();
    }
}
