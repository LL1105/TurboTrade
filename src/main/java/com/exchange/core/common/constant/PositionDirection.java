package com.exchange.core.common.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum PositionDirection {

    // 多头持仓
    LONG(1),

    // 空头持仓
    SHORT(-1),

    // 空仓状态
    EMPTY(0);

    // 持仓方向的乘数
    @Getter
    private int multiplier;

    /**
     * 根据订单动作返回相应的持仓方向
     *
     * @param action 订单动作（买或卖）
     * @return 持仓方向
     */
    public static PositionDirection of(OrderAction action) {
        return action == OrderAction.BID ? LONG : SHORT;
    }

    /**
     * 根据字节码返回对应的持仓方向
     *
     * @param code 持仓方向的字节码
     * @return 持仓方向
     * @throws IllegalArgumentException 如果字节码无效，抛出异常
     */
    public static PositionDirection of(byte code) {
        switch (code) {
            case 1:
                return LONG;
            case -1:
                return SHORT;
            case 0:
                return EMPTY;
            default:
                throw new IllegalArgumentException("unknown PositionDirection:" + code);
        }
    }

    /**
     * 判断当前持仓方向是否与给定的订单动作相反
     *
     * @param action 订单动作（买或卖）
     * @return 如果持仓方向与订单动作相反，返回 true
     */
    public boolean isOppositeToAction(OrderAction action) {
        return (this == PositionDirection.LONG && action == OrderAction.ASK) ||
               (this == PositionDirection.SHORT && action == OrderAction.BID);
    }

    /**
     * 判断当前持仓方向是否与给定的订单动作相同
     *
     * @param action 订单动作（买或卖）
     * @return 如果持仓方向与订单动作相同，返回 true
     */
    public boolean isSameAsAction(OrderAction action) {
        return (this == PositionDirection.LONG && action == OrderAction.BID) ||
               (this == PositionDirection.SHORT && action == OrderAction.ASK);
    }

}
