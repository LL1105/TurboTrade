package com.exchange.tests.util;

import org.HdrHistogram.Histogram;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public final class LatencyTools {

    // 定义需要计算的延迟百分位数
    private static final double[] PERCENTILES = new double[]{50, 90, 95, 99, 99.9, 99.99};

    /**
     * 根据 Histogram 生成一个包含延迟百分位数和最大延迟的报告
     *
     * @param histogram 通过 HdrHistogram 记录的延迟数据
     * @return 包含延迟报告的映射，键是百分位数或 "W"（最大延迟），值是对应的延迟时间字符串
     */
    public static Map<String, String> createLatencyReportFast(Histogram histogram) {
        final Map<String, String> fmt = new LinkedHashMap<>();

        // 计算并格式化各个百分位数的延迟
        Arrays.stream(PERCENTILES).forEach(p -> fmt.put(p + "%", formatNanos(histogram.getValueAtPercentile(p))));

        // 计算并格式化最大延迟（W）
        fmt.put("W", formatNanos(histogram.getMaxValue()));

        return fmt;
    }

    /**
     * 将延迟时间（以纳秒为单位）转换为适当的时间单位并格式化为字符串
     *
     * @param ns 延迟时间，单位为纳秒
     * @return 格式化后的延迟时间字符串，单位可能是微秒（µs）、毫秒（ms）或秒（s）
     */
    public static String formatNanos(long ns) {
        // 初始单位是微秒（µs）
        float value = ns / 1000f;
        String timeUnit = "µs";

        // 如果值大于 1000 µs，转换为毫秒（ms）
        if (value > 1000) {
            value /= 1000;
            timeUnit = "ms";
        }

        // 如果值大于 1000 ms，转换为秒（s）
        if (value > 1000) {
            value /= 1000;
            timeUnit = "s";
        }

        // 根据不同的值范围格式化结果
        if (value < 3) {
            // 如果小于 3，保留两位小数
            return Math.round(value * 100) / 100f + timeUnit;
        } else if (value < 30) {
            // 如果小于 30，保留一位小数
            return Math.round(value * 10) / 10f + timeUnit;
        } else {
            // 否则，取整
            return Math.round(value) + timeUnit;
        }
    }
}
