package com.exchange.core.common.config;

import com.exchange.core.common.api.binary.BatchAddAccountsCommand;
import com.exchange.core.common.api.binary.BatchAddSymbolsCommand;
import com.exchange.core.common.api.binary.BinaryDataCommand;
import com.exchange.core.common.api.reports.ReportQuery;
import com.exchange.core.common.api.reports.queryImpl.SingleUserReportQuery;
import com.exchange.core.common.api.reports.queryImpl.StateHashReportQuery;
import com.exchange.core.common.api.reports.queryImpl.TotalCurrencyBalanceReportQuery;
import com.exchange.core.common.constant.BinaryCommandType;
import com.exchange.core.common.constant.ReportType;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reports configuration - 用于管理报告查询和二进制命令的配置。
 *
 * 该类包含：
 * - 预定义的报告查询类型和自定义报告查询类型的构造器。
 * - 二进制命令类型和对应命令类的构造器。
 *
 * 使用此类时，可以加载默认配置或添加自定义配置来扩展报告查询类型和二进制命令类型。
 */
@Getter
public final class ReportsQueriesConfiguration {

    // 默认配置，使用预定义的报告查询类型和二进制命令类型
    public static final ReportsQueriesConfiguration DEFAULT = createStandardConfig();

    private final Map<Integer, Constructor<? extends ReportQuery<?>>> reportConstructors;
    private final Map<Integer, Constructor<? extends BinaryDataCommand>> binaryCommandConstructors;

    /**
     * 创建默认的报告配置
     *
     * @return 默认的报告配置
     */
    public static ReportsQueriesConfiguration createStandardConfig() {
        return createStandardConfig(Collections.emptyMap());
    }

    /**
     * 创建带有自定义报告的报告配置
     *
     * @param customReports 自定义报告的集合
     * @return 带有自定义报告的配置
     */
    public static ReportsQueriesConfiguration createStandardConfig(final Map<Integer, Class<? extends ReportQuery<?>>> customReports) {

        // 存储报告查询构造器的映射
        final Map<Integer, Constructor<? extends ReportQuery<?>>> reportConstructors = new HashMap<>();
        // 存储二进制命令构造器的映射
        final Map<Integer, Constructor<? extends BinaryDataCommand>> binaryCommandConstructors = new HashMap<>();

        // 添加二进制命令（不可扩展的）
        addBinaryCommandClass(binaryCommandConstructors, BinaryCommandType.ADD_ACCOUNTS, BatchAddAccountsCommand.class);
        addBinaryCommandClass(binaryCommandConstructors, BinaryCommandType.ADD_SYMBOLS, BatchAddSymbolsCommand.class);

        // 添加预定义的报告查询（可扩展的）
        addQueryClass(reportConstructors, ReportType.STATE_HASH.getCode(), StateHashReportQuery.class);
        addQueryClass(reportConstructors, ReportType.SINGLE_USER_REPORT.getCode(), SingleUserReportQuery.class);
        addQueryClass(reportConstructors, ReportType.TOTAL_CURRENCY_BALANCE.getCode(), TotalCurrencyBalanceReportQuery.class);

        // 添加自定义的报告查询
        customReports.forEach((code, customReport) -> addQueryClass(reportConstructors, code, customReport));

        // 返回构建的报告查询配置
        return new ReportsQueriesConfiguration(
                Collections.unmodifiableMap(reportConstructors),
                Collections.unmodifiableMap(binaryCommandConstructors));
    }

    /**
     * 向报告查询构造器映射中添加查询类型
     *
     * @param reportConstructors 报告构造器映射
     * @param reportTypeCode 报告类型编码
     * @param reportQueryClass 报告查询类
     */
    private static void addQueryClass(final Map<Integer, Constructor<? extends ReportQuery<?>>> reportConstructors,
                                      final int reportTypeCode,
                                      final Class<? extends ReportQuery<?>> reportQueryClass) {

        // 如果该报告类型已经存在，则抛出异常
        final Constructor<? extends ReportQuery<?>> existing = reportConstructors.get(reportTypeCode);
        if (existing != null) {
            throw new IllegalArgumentException("Configuration error: report type code " + reportTypeCode + " is already occupied by " + existing.getDeclaringClass().getName());
        }

        // 尝试获取报告类的构造函数
        try {
            // 添加构造函数到报告构造器映射
            reportConstructors.put(reportTypeCode, reportQueryClass.getConstructor(BytesIn.class));
        } catch (final NoSuchMethodException ex) {
            // 如果没有找到接受 BytesIn 构造函数，则抛出异常
            throw new IllegalArgumentException("Configuration error: report class " + reportQueryClass.getName() + " does not have a deserialization constructor accepting BytesIn");
        }
    }

    /**
     * 向二进制命令构造器映射中添加命令类型
     *
     * @param binaryCommandConstructors 二进制命令构造器映射
     * @param type 二进制命令类型
     * @param binaryCommandClass 二进制命令类
     */
    private static void addBinaryCommandClass(Map<Integer, Constructor<? extends BinaryDataCommand>> binaryCommandConstructors,
                                              BinaryCommandType type,
                                              Class<? extends BinaryDataCommand> binaryCommandClass) {
        try {
            // 添加构造函数到二进制命令构造器映射
            binaryCommandConstructors.put(type.getCode(), binaryCommandClass.getConstructor(BytesIn.class));
        } catch (final NoSuchMethodException ex) {
            // 如果没有找到接受 BytesIn 构造函数，则抛出异常
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * 构造函数，初始化报告查询和二进制命令构造器映射
     *
     * @param reportConstructors 报告查询构造器映射
     * @param binaryCommandConstructors 二进制命令构造器映射
     */
    private ReportsQueriesConfiguration(final Map<Integer, Constructor<? extends ReportQuery<?>>> reportConstructors,
                                        final Map<Integer, Constructor<? extends BinaryDataCommand>> binaryCommandConstructors) {
        this.reportConstructors = reportConstructors;
        this.binaryCommandConstructors = binaryCommandConstructors;
    }

    /**
     * 重写 toString 方法，用于输出配置的字符串表示
     *
     * @return 配置的字符串表示
     */
    @Override
    public String toString() {
        return "ReportsQueriesConfiguration{" +
                "reportConstructors=[" + reportToString(reportConstructors) +
                "], binaryCommandConstructors=[" + reportToString(binaryCommandConstructors) +
                "]}";
    }

    /**
     * 将构造器映射转换为字符串表示
     *
     * @param mapping 构造器映射
     * @return 字符串表示
     */
    private static String reportToString(final Map<Integer, ? extends Constructor<?>> mapping) {
        return mapping.entrySet().stream()
                .map(entry -> String.format("%d:%s", entry.getKey(), entry.getValue().getDeclaringClass().getSimpleName()))
                .collect(Collectors.joining(", "));
    }
}
