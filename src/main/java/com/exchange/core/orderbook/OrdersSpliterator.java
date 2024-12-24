package com.exchange.core.orderbook;

import lombok.AllArgsConstructor;

import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * 订单的 Spliterator 实现类，用于遍历订单簿中的订单
 */
@AllArgsConstructor
public final class OrdersSpliterator implements Spliterator<OrderBookDirectImpl.DirectOrder> {

    private OrderBookDirectImpl.DirectOrder pointer;  // 当前指向的订单

    /**
     * 尝试对下一个订单执行给定的操作
     *
     * @param action 要执行的操作，通常是一个消费函数
     * @return 如果存在下一个订单并成功执行操作，返回 true；如果没有更多订单，返回 false
     */
    @Override
    public boolean tryAdvance(Consumer<? super OrderBookDirectImpl.DirectOrder> action) {
        if (pointer == null) {
            return false;  // 如果指针为空，说明没有更多订单
        } else {
            action.accept(pointer);  // 执行操作（例如处理订单）
            pointer = pointer.prev;  // 将指针移动到前一个订单
            return true;
        }
    }

    /**
     * 尝试拆分当前 Spliterator 为两个独立的部分
     *
     * @return 当前不支持拆分，返回 null
     */
    @Override
    public Spliterator<OrderBookDirectImpl.DirectOrder> trySplit() {
        return null;  // 该实现不支持拆分
    }

    /**
     * 估计 Spliterator 的剩余元素数量
     *
     * @return 返回一个非常大的估计值，表示剩余订单数量无法确定
     */
    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;  // 由于没有长度信息，返回最大值
    }

    /**
     * 返回该 Spliterator 的特性
     *
     * @return 返回 ORDERED，表示此 Spliterator 是有序的
     */
    @Override
    public int characteristics() {
        return Spliterator.ORDERED;  // 标识此 Spliterator 是有序的
    }
}
