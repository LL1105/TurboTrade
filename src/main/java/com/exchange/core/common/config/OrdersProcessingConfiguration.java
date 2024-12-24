package com.exchange.core.common.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * 订单处理配置类，用于定义交易所订单处理相关的配置。
 * 包含两项主要配置：风险处理模式和保证金交易模式。
 */
@AllArgsConstructor
@Getter
@Builder
@ToString
public final class OrdersProcessingConfiguration {

    // 默认的订单处理配置
    // 风险处理模式为 FULL_PER_CURRENCY，保证金交易模式为启用
    public static OrdersProcessingConfiguration DEFAULT = OrdersProcessingConfiguration.builder()
            .riskProcessingMode(RiskProcessingMode.FULL_PER_CURRENCY)  // 默认启用每个货币的独立风险处理
            .marginTradingMode(MarginTradingMode.MARGIN_TRADING_ENABLED)  // 默认启用保证金交易
            .build();

    // 风险处理模式：决定是否对每个货币/资产账户独立进行风险检查
    private final RiskProcessingMode riskProcessingMode;

    // 保证金交易模式：决定是否启用保证金交易
    private final MarginTradingMode marginTradingMode;

    /**
     * 风险处理模式枚举
     * 定义了风险检查的策略。
     */
    public enum RiskProcessingMode {
        /**
         * 启用风险处理，每个货币/资产账户独立检查风险
         */
        FULL_PER_CURRENCY,

        /**
         * 禁用风险处理，任何订单都被接受并被放置
         */
        NO_RISK_PROCESSING
    }

    /**
     * 保证金交易模式枚举
     * 定义了是否启用保证金交易的策略。
     */
    public enum MarginTradingMode {
        /**
         * 禁用保证金交易
         */
        MARGIN_TRADING_DISABLED,

        /**
         * 启用保证金交易
         */
        MARGIN_TRADING_ENABLED
    }
}
