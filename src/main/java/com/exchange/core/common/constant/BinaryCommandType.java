package com.exchange.core.common.constant;

import lombok.Getter;

/**
 * 定义了二进制命令类型的枚举
 * 这些命令类型用于标识不同的二进制命令
 */
@Getter
public enum BinaryCommandType {

    // 二进制命令类型
    ADD_ACCOUNTS(1002),  // 添加账户
    ADD_SYMBOLS(1003);   // 添加符号

    // 命令类型的代码
    private final int code;

    /**
     * 构造函数，用于根据命令类型代码初始化枚举值
     *
     * @param code 命令类型的代码
     */
    BinaryCommandType(int code) {
        this.code = code;
    }

    /**
     * 根据命令类型代码获取对应的命令类型枚举
     *
     * @param code 命令类型的代码
     * @return 对应的 BinaryCommandType 枚举值
     */
    public static BinaryCommandType of(int code) {
        switch (code) {
            case 1002:
                return ADD_ACCOUNTS;  // 返回添加账户命令
            case 1003:
                return ADD_SYMBOLS;   // 返回添加符号命令
            default:
                throw new IllegalArgumentException("unknown BinaryCommandType:" + code);  // 如果没有匹配的命令类型，抛出异常
        }
    }
}
