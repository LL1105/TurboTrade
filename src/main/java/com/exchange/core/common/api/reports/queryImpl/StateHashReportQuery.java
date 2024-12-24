package com.exchange.core.common.api.reports.queryImpl;

import com.exchange.core.common.api.reports.ReportQuery;
import com.exchange.core.common.api.reports.resultImpl.StateHashReportResult;
import com.exchange.core.common.constant.ReportType;
import com.exchange.core.processors.MatchingEngineRouter;
import com.exchange.core.processors.RiskEngine;
import com.exchange.core.utils.HashingUtils;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * 状态哈希报告查询类
 * 用于生成关于系统各模块和子模块状态的哈希报告
 */
@NoArgsConstructor
@EqualsAndHashCode
@ToString
@Slf4j
public final class StateHashReportQuery implements ReportQuery<StateHashReportResult> {

    // 构造函数，接收字节流，但目前未进行任何操作
    public StateHashReportQuery(BytesIn bytesIn) {
        // do nothing
    }

    /**
     * 获取报告的类型码
     * @return 状态哈希报告类型码
     */
    @Override
    public int getReportTypeCode() {
        return ReportType.STATE_HASH.getCode();
    }

    /**
     * 创建报告结果
     * 合并多个字节片段，生成一个完整的状态哈希报告
     * @param sections 多个字节流片段
     * @return 合并后的状态哈希报告
     */
    @Override
    public StateHashReportResult createResult(Stream<BytesIn> sections) {
        return StateHashReportResult.merge(sections);
    }

    /**
     * 处理匹配引擎，生成其状态哈希报告
     * @param matchingEngine 匹配引擎
     * @return 状态哈希报告结果
     */
    @Override
    public Optional<StateHashReportResult> process(MatchingEngineRouter matchingEngine) {

        final SortedMap<StateHashReportResult.SubmoduleKey, Integer> hashCodes = new TreeMap<>();

        final int moduleId = matchingEngine.getShardId();

        // 计算匹配引擎的各子模块的哈希值，并将其存入 map
        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.MATCHING_BINARY_CMD_PROCESSOR),
                matchingEngine.getBinaryCommandsProcessor().stateHash());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.MATCHING_ORDER_BOOKS),
                HashingUtils.stateHash(matchingEngine.getOrderBooks()));

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.MATCHING_SHARD_MASK),
                Long.hashCode(matchingEngine.getShardMask()));

        // 返回封装了哈希值的报告
        return Optional.of(
                new StateHashReportResult(hashCodes));
    }

    /**
     * 处理风险引擎，生成其状态哈希报告
     * @param riskEngine 风险引擎
     * @return 状态哈希报告结果
     */
    @Override
    public Optional<StateHashReportResult> process(RiskEngine riskEngine) {

        final SortedMap<StateHashReportResult.SubmoduleKey, Integer> hashCodes = new TreeMap<>();

        final int moduleId = riskEngine.getShardId();

        // 计算风险引擎的各子模块的哈希值，并将其存入 map
        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_SYMBOL_SPEC_PROVIDER),
                riskEngine.getBinaryCommandsProcessor().stateHash());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_USER_PROFILE_SERVICE),
                riskEngine.getUserProfileService().stateHash());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_BINARY_CMD_PROCESSOR),
                riskEngine.getBinaryCommandsProcessor().stateHash());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_LAST_PRICE_CACHE),
                HashingUtils.stateHash(riskEngine.getLastPriceCache()));

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_FEES),
                riskEngine.getFees().hashCode());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_ADJUSTMENTS),
                riskEngine.getAdjustments().hashCode());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_SUSPENDS),
                riskEngine.getSuspends().hashCode());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_SHARD_MASK),
                Long.hashCode(riskEngine.getShardMask()));

        // 返回封装了哈希值的报告
        return Optional.of(
                new StateHashReportResult(hashCodes));
    }

    /**
     * 序列化方法，但目前不进行任何操作
     * @param bytes 输出字节流
     */
    @Override
    public void writeMarshallable(BytesOut bytes) {
        // do nothing
    }
}
