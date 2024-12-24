package com.exchange.core.common.api.reports.queryImpl;

import com.exchange.core.common.CoreSymbolSpecification;
import com.exchange.core.common.api.reports.ReportQuery;
import com.exchange.core.common.api.reports.resultImpl.TotalCurrencyBalanceReportResult;
import com.exchange.core.common.constant.PositionDirection;
import com.exchange.core.common.constant.ReportType;
import com.exchange.core.common.constant.SymbolType;
import com.exchange.core.processors.MatchingEngineRouter;
import com.exchange.core.processors.RiskEngine;
import com.exchange.core.processors.SymbolSpecificationProvider;
import com.exchange.core.utils.CoreArithmeticUtils;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * `TotalCurrencyBalanceReportQuery` 类用于查询并生成总货币余额报告，支持与交易匹配引擎和风险引擎的交互。
 * 该类负责从交易引擎和风险引擎中获取必要的数据，处理并生成一个包含各种货币余额的报告。
 */
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public final class TotalCurrencyBalanceReportQuery implements ReportQuery<TotalCurrencyBalanceReportResult> {

    // 默认构造函数，当前没有具体实现，主要用于反序列化
    public TotalCurrencyBalanceReportQuery(BytesIn bytesIn) {
        // do nothing
    }

    /**
     * 获取报告的类型代码，返回对应报告类型的枚举值。
     * 
     * @return 报告类型代码
     */
    @Override
    public int getReportTypeCode() {
        return ReportType.TOTAL_CURRENCY_BALANCE.getCode();
    }

    /**
     * 通过字节流生成报告结果。合并多个报告部分，并返回合并后的 `TotalCurrencyBalanceReportResult` 实例。
     * 
     * @param sections 报告部分字节流
     * @return 合并后的报告结果
     */
    @Override
    public TotalCurrencyBalanceReportResult createResult(final Stream<BytesIn> sections) {
        return TotalCurrencyBalanceReportResult.merge(sections);
    }

    /**
     * 从 `MatchingEngineRouter`（匹配引擎）中获取货币余额数据并生成报告。
     * 对每个交易对的订单簿进行处理，计算相关的货币余额，并返回报告结果。
     * 
     * @param matchingEngine 匹配引擎路由
     * @return 生成的报告结果
     */
    @Override
    public Optional<TotalCurrencyBalanceReportResult> process(final MatchingEngineRouter matchingEngine) {

        // 存储货币余额的 Map，键为货币ID，值为余额
        final IntLongHashMap currencyBalance = new IntLongHashMap();

        // 处理匹配引擎中的订单簿
        matchingEngine.getOrderBooks().stream()
                // 只处理货币交易对类型的订单簿
                .filter(ob -> ob.getSymbolSpec().type == SymbolType.CURRENCY_EXCHANGE_PAIR)
                .forEach(ob -> {
                    // 获取符号的规格
                    final CoreSymbolSpecification spec = ob.getSymbolSpec();

                    // 计算卖方订单的货币余额（卖方 ask 订单）
                    currencyBalance.addToValue(
                            spec.getBaseCurrency(),
                            ob.askOrdersStream(false).mapToLong(ord -> CoreArithmeticUtils.calculateAmountAsk(ord.getSize() - ord.getFilled(), spec)).sum());

                    // 计算买方订单的货币余额（买方 bid 订单）
                    currencyBalance.addToValue(
                            spec.getQuoteCurrency(),
                            ob.bidOrdersStream(false).mapToLong(ord -> CoreArithmeticUtils.calculateAmountBidTakerFee(ord.getSize() - ord.getFilled(), ord.getReserveBidPrice(), spec)).sum());
                });

        // 返回根据订单余额生成的报告
        return Optional.of(TotalCurrencyBalanceReportResult.ofOrderBalances(currencyBalance));
    }

    /**
     * 从 `RiskEngine`（风险引擎）中获取货币余额、费用、调整等数据，并生成报告。
     * 通过用户的账户和头寸信息，计算余额、费用、调整，并分别为每个货币生成报告。
     * 
     * @param riskEngine 风险引擎
     * @return 生成的报告结果
     */
    @Override
    public Optional<TotalCurrencyBalanceReportResult> process(final RiskEngine riskEngine) {

        // 准备一个快速的价格缓存，用于利润估算
        final IntObjectHashMap<RiskEngine.LastPriceCacheRecord> dummyLastPriceCache = new IntObjectHashMap<>();
        riskEngine.getLastPriceCache().forEachKeyValue((s, r) -> dummyLastPriceCache.put(s, r.averagingRecord()));

        // 存储货币余额
        final IntLongHashMap currencyBalance = new IntLongHashMap();

        // 存储每个符号的多头持仓和空头持仓
        final IntLongHashMap symbolOpenInterestLong = new IntLongHashMap();
        final IntLongHashMap symbolOpenInterestShort = new IntLongHashMap();

        // 获取符号规格提供者
        final SymbolSpecificationProvider symbolSpecificationProvider = riskEngine.getSymbolSpecificationProvider();

        // 遍历用户资料，处理账户余额和头寸信息
        riskEngine.getUserProfileService().getUserProfiles().forEach(userProfile -> {
            // 添加账户余额
            userProfile.accounts.forEachKeyValue(currencyBalance::addToValue);
            // 处理用户的头寸
            userProfile.positions.forEachKeyValue((symbolId, positionRecord) -> {
                // 获取符号规格
                final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(symbolId);
                // 获取该符号的价格缓存（用于估算利润）
                final RiskEngine.LastPriceCacheRecord avgPrice = dummyLastPriceCache.getIfAbsentPut(symbolId, RiskEngine.LastPriceCacheRecord.dummy);
                // 估算头寸的利润并更新余额
                currencyBalance.addToValue(positionRecord.currency, positionRecord.estimateProfit(spec, avgPrice));

                // 处理头寸的方向，更新相应的多头或空头持仓
                if (positionRecord.direction == PositionDirection.LONG) {
                    symbolOpenInterestLong.addToValue(symbolId, positionRecord.openVolume);
                } else if (positionRecord.direction == PositionDirection.SHORT) {
                    symbolOpenInterestShort.addToValue(symbolId, positionRecord.openVolume);
                }
            });
        });

        // 返回包含余额、费用、调整、挂单余额等的报告
        return Optional.of(
                new TotalCurrencyBalanceReportResult(
                        currencyBalance,
                        new IntLongHashMap(riskEngine.getFees()),
                        new IntLongHashMap(riskEngine.getAdjustments()),
                        new IntLongHashMap(riskEngine.getSuspends()),
                        null,
                        symbolOpenInterestLong,
                        symbolOpenInterestShort));
    }

    /**
     * 实现 `writeMarshallable` 方法，这里没有具体实现，通常用于序列化对象。
     * 
     * @param bytes 输出的字节流
     */
    @Override
    public void writeMarshallable(BytesOut bytes) {
        // do nothing
    }
}
