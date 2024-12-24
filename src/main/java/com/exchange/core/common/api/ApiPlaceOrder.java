package com.exchange.core.common.api;

import com.exchange.core.common.constant.OrderAction;
import com.exchange.core.common.constant.OrderType;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * 交易所下单命令
 * ApiPlaceOrder 类表示一个下单请求命令，包含了订单的各种信息，如价格、数量、订单类型等。
 */
@Builder
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public final class ApiPlaceOrder extends ApiCommand {

    /*
     * 订单的价格
     */
    public final long price;

    /*
     * 订单的数量
     */
    public final long size;

    /*
     * 订单的 ID
     */
    public final long orderId;

    /*
     * 订单操作类型（如买入或卖出）
     */
    public final OrderAction action;

    /*
     * 订单类型（如限价单或市价单）
     */
    public final OrderType orderType;

    /*
     * 用户 ID，标识下单的用户
     */
    public final long uid;

    /*
     * 交易对符号 ID，标识所交易的资产对
     */
    public final int symbol;

    /*
     * 用户的cookie ID，可能用于跟踪用户会话
     */
    public final int userCookie;

    /*
     * 保留价格，可能用于挂单时的价格上下限（未使用时为 0）
     */
    public final long reservePrice;

    /**
     * 重写 `toString` 方法，用于输出订单的字符串表示
     * 
     * @return 订单的简洁字符串表示
     */
    @Override
    public String toString() {
        return "[ADD o" + orderId + " s" + symbol + " u" + uid + " " 
                + (action == OrderAction.ASK ? 'A' : 'B') // ASK 为卖单，B 为买单
                + ":" + (orderType == OrderType.IOC ? "IOC" : "GTC") // IOC 为立即成交，GTC 为有效直到取消
                + ":" + price + ":" + size + "]";
        // 若保留价格不为零时，可以显示它
        // (reservePrice != 0 ? ("(R" + reservePrice + ")") : "");
    }
}
