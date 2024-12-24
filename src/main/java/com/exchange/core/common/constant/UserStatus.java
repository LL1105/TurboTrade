package com.exchange.core.common.constant;

import lombok.Getter;

/**
 * 用户状态枚举类
 * <p>
 * 该枚举类定义了用户的不同状态，包括：
 * - `ACTIVE`（正常用户）
 * - `SUSPENDED`（挂起状态）
 */
@Getter
public enum UserStatus {

    // 用户状态：正常
    ACTIVE(0),

    // 用户状态：挂起
    SUSPENDED(1);

    // 状态的字节值表示
    private byte code;

    // 枚举构造函数，初始化状态的字节值
    UserStatus(int code) {
        this.code = (byte) code;
    }

    /**
     * 根据状态的字节值返回对应的 `UserStatus` 枚举项
     *
     * @param code 状态的字节值
     * @return 对应的 `UserStatus` 枚举项
     * @throws IllegalArgumentException 如果提供的字节值未知
     */
    public static UserStatus of(byte code) {
        switch (code) {
            case 0:
                return ACTIVE;
            case 1:
                return SUSPENDED;
            default:
                throw new IllegalArgumentException("未知的用户状态：" + code);
        }
    }

}
