package com.exchange.core.common.constant;

import lombok.Getter;

/**
 * 余额调整类型枚举，表示不同的余额调整操作。
 */
@Getter
public enum BalanceAdjustmentType {
    /**
     * 余额调整操作
     */
    ADJUSTMENT(0),  // 代码为 0，表示余额调整

    /**
     * 余额暂停操作
     */
    SUSPEND(1);  // 代码为 1，表示余额暂停

    // 存储枚举的代码值
    private byte code;

    /**
     * 枚举构造函数，用于根据代码值初始化枚举类型
     *
     * @param code 枚举类型对应的代码值
     */
    BalanceAdjustmentType(int code) {
        this.code = (byte) code;  // 枚举的代码值被转为 byte 类型存储
    }

    /**
     * 根据代码值返回对应的枚举类型
     *
     * @param code 枚举类型的代码值
     * @return 返回对应的枚举类型
     * @throws IllegalArgumentException 如果代码值无效，则抛出异常
     */
    public static BalanceAdjustmentType of(byte code) {
        switch (code) {
            case 0:
                return ADJUSTMENT;  // 代码值为 0 时返回 ADJUSTMENT
            case 1:
                return SUSPEND;  // 代码值为 1 时返回 SUSPEND
            default:
                throw new IllegalArgumentException("unknown BalanceAdjustmentType:" + code);  // 无效代码值时抛出异常
        }
    }
}
