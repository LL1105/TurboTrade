package com.exchange.core.common.constant;

public enum MatcherEventType {

    // 用于表示标准的交易事件，记录成功的交易或订单状态更新（例如修改订单）
    TRADE,

    // 反映市场流动性不足导致的订单拒绝情况，对于市场订单尤为重要，帮助交易系统处理无法执行的订单
    REJECT,

    // 当订单被取消或减少时，系统需要释放相关的资金或保证金，确保风险管理机制能正常工作
    REDUCE,

    // 提供了一个灵活的方式来附加自定义的数据，用于处理一些特殊的事件或需求，不限于标准的交易模式
    BINARY_EVENT
}
