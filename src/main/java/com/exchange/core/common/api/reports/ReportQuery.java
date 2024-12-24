package com.exchange.core.common.api.reports;

import com.exchange.core.processors.MatchingEngineRouter;
import com.exchange.core.processors.RiskEngine;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Optional;
import java.util.stream.Stream;


public interface ReportQuery<T extends ReportResult> extends WriteBytesMarshallable {

    /**
     * 获取报告类型代码
     *
     * @return 报告类型代码（整数）
     */
    int getReportTypeCode();

    /**
     * 获取报告的 MapReduce 构造器，用于汇总多个数据部分。
     *
     * @param sections 报告数据的各个部分（`Stream<BytesIn>` 类型）
     * @return 报告结果
     */
    T createResult(Stream<BytesIn> sections);

    /**
     * 报告的主要逻辑，由匹配引擎线程执行。
     *
     * @param matchingEngine 匹配引擎实例
     * @return 自定义的报告结果（`Optional<T>` 类型）
     */
    Optional<T> process(MatchingEngineRouter matchingEngine);

    /**
     * 报告的主要逻辑，由风险引擎线程执行。
     *
     * @param riskEngine 风险引擎实例
     * @return 自定义的报告结果（`Optional<T>` 类型）
     */
    Optional<T> process(RiskEngine riskEngine);
}
