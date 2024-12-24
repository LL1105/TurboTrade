package com.exchange.core.common.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Exchange configuration - 用于封装交易所的配置，包含各个子模块的配置。
 *
 * 该类包含：
 * - 订单处理配置
 * - 性能配置
 * - 初始化状态配置
 * - 报告查询配置
 * - 日志配置
 * - 序列化（快照和日志）配置
 */
@AllArgsConstructor
@Getter
@Builder
public final class ExchangeConfiguration {

    /*
     * 订单处理配置
     * 包含交易所的订单处理相关设置
     */
    private final OrdersProcessingConfiguration ordersProcessingCfg;

    /*
     * 性能配置
     * 包含交易所性能调优的相关设置
     */
    private final PerformanceConfiguration performanceCfg;

    /*
     * 交易所初始化状态配置
     * 包含交易所初始化时所需的配置信息
     */
    private final InitialStateConfiguration initStateCfg;

    /*
     * 报告查询配置
     * 包含交易所报表和查询的相关设置
     */
    private final ReportsQueriesConfiguration reportsQueriesCfg;

    /*
     * 日志配置
     * 包含交易所的日志设置
     */
    private final LoggingConfiguration loggingCfg;

    /*
     * 序列化（快照和日志）配置
     * 包含交易所的序列化（数据快照和日志）相关设置
     */
    private final SerializationConfiguration serializationCfg;

    /**
     * 重写 toString 方法，输出配置的详细信息
     *
     * @return 配置的字符串表示
     */
    @Override
    public String toString() {
        return "ExchangeConfiguration{" +
                "\n  ordersProcessingCfg=" + ordersProcessingCfg +
                "\n  performanceCfg=" + performanceCfg +
                "\n  initStateCfg=" + initStateCfg +
                "\n  reportsQueriesCfg=" + reportsQueriesCfg +
                "\n  loggingCfg=" + loggingCfg +
                "\n  serializationCfg=" + serializationCfg +
                '}';
    }

    /**
     * 创建一个具有预定义默认设置的配置构建器
     *
     * @return 配置构建器
     */
    public static ExchangeConfiguration.ExchangeConfigurationBuilder defaultBuilder() {
        return ExchangeConfiguration.builder()
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .initStateCfg(InitialStateConfiguration.DEFAULT)
                .performanceCfg(PerformanceConfiguration.DEFAULT)
                .reportsQueriesCfg(ReportsQueriesConfiguration.DEFAULT)
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(SerializationConfiguration.DEFAULT);
    }
}
