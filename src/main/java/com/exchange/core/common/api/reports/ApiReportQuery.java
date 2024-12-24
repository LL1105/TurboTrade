package com.exchange.core.common.api.reports;

import lombok.Builder;
import lombok.EqualsAndHashCode;

/**
 * ApiReportQuery 类用于表示API报告查询的结构。
 * <p>
 * 该类包含了查询的时间戳、传输ID以及查询对象本身。
 * 用于在API报告中传递报告查询的详细信息。
 * </p>
 */
@Builder
@EqualsAndHashCode
public final class ApiReportQuery {

    /**
     * 查询的时间戳，表示查询的发生时间。
     */
    public long timestamp;

    /**
     * 传输ID，用于标识唯一的报告查询请求。
     * <p>
     * 该ID可以保持常量，除非需要并发推送数据时才会变化。
     * </p>
     */
    public final int transferId;

    /**
     * 报告查询对象。代表具体的查询请求，可以是任何类型的可序列化对象。
     */
    public final ReportQuery<?> query;

    /**
     * 重写 toString 方法，返回 ApiReportQuery 的字符串表示。
     * 格式为：[REPORT_QUERY tid=transferId query=query]
     * 
     * @return ApiReportQuery 的字符串表示
     */
    @Override
    public String toString() {
        return "[REPORT_QUERY tid=" + transferId + " query=" + query + "]";
    }
}
