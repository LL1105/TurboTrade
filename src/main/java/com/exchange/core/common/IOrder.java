package com.exchange.core.common;

import com.exchange.core.common.constant.OrderAction;

public interface IOrder extends StateHash{

    long getPrice();

    long getSize();

    // 获取已经成交数量
    long getFilled();

    long getUid();

    long getOrderId();

    long getTimestamp();

    // 获取挂单价格
    long getReserveBidPrice();

    OrderAction getAction();
}
