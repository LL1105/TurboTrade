package com.exchange.core.common.constant;

import lombok.Getter;

@Getter
public enum ReportType {

    // 定义报告类型，分别对应不同的报告代码
    STATE_HASH(10001),            // 状态哈希报告
    SINGLE_USER_REPORT(10002),    // 单一用户报告
    TOTAL_CURRENCY_BALANCE(10003); // 总货币余额报告

    private final int code;  // 存储报告类型的代码

    // 构造函数，初始化报告类型的代码
    ReportType(int code) {
        this.code = code;
    }

    // 根据报告类型代码返回对应的报告类型
    public static ReportType of(int code) {

        switch (code) {
            case 10001:
                return STATE_HASH;           // 返回状态哈希报告类型
            case 10002:
                return SINGLE_USER_REPORT;   // 返回单一用户报告类型
            case 10003:
                return TOTAL_CURRENCY_BALANCE;  // 返回总货币余额报告类型
            default:
                throw new IllegalArgumentException("unknown ReportType:" + code);  // 如果代码未知，抛出异常
        }
    }
}
