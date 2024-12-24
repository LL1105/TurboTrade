package com.exchange.core.common.api.reports.resultImpl;

import com.exchange.core.common.api.reports.ReportResult;
import com.exchange.core.utils.SerializationUtils;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

import java.util.stream.Stream;

/**
 * `TotalCurrencyBalanceReportResult` 类用于表示货币余额报告的结果。
 * 它包含了账户余额、费用、调整、挂单余额、持仓等多种信息。
 */
@AllArgsConstructor
@EqualsAndHashCode
@Getter
@ToString
@Slf4j
public final class TotalCurrencyBalanceReportResult implements ReportResult {

    // 账户余额：货币 ID -> 余额
    final private IntLongHashMap accountBalances;
    
    // 费用：货币 ID -> 费用金额
    final private IntLongHashMap fees;
    
    // 调整：货币 ID -> 调整金额
    final private IntLongHashMap adjustments;
    
    // 暂停余额：货币 ID -> 暂停余额
    final private IntLongHashMap suspends;
    
    // 订单余额：货币 ID -> 订单余额
    final private IntLongHashMap ordersBalances;

    // 多头持仓：货币 ID -> 多头持仓量
    final private IntLongHashMap openInterestLong;
    
    // 空头持仓：货币 ID -> 空头持仓量
    final private IntLongHashMap openInterestShort;

    /**
     * 创建一个空的 `TotalCurrencyBalanceReportResult` 实例，所有字段初始化为 null。
     * 
     * @return 一个空的报告结果实例
     */
    public static TotalCurrencyBalanceReportResult createEmpty() {
        return new TotalCurrencyBalanceReportResult(
                null, null, null, null, null, null, null);
    }

    /**
     * 创建一个只包含订单余额的报告结果实例，其他字段初始化为 null。
     * 
     * @param currencyBalance 订单余额
     * @return 一个包含订单余额的报告结果实例
     */
    public static TotalCurrencyBalanceReportResult ofOrderBalances(final IntLongHashMap currencyBalance) {
        return new TotalCurrencyBalanceReportResult(
                null, null, null, null, currencyBalance, null, null);
    }

    /**
     * 通过字节流反序列化构造 `TotalCurrencyBalanceReportResult` 实例。
     * 
     * @param bytesIn 输入的字节流
     */
    private TotalCurrencyBalanceReportResult(final BytesIn bytesIn) {
        this.accountBalances = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.fees = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.adjustments = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.suspends = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.ordersBalances = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.openInterestLong = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.openInterestShort = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
    }

    /**
     * 将当前实例的所有字段数据序列化到字节流中。
     * 
     * @param bytes 输出的字节流
     */
    @Override
    public void writeMarshallable(final BytesOut bytes) {
        SerializationUtils.marshallNullable(accountBalances, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(fees, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(adjustments, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(suspends, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(ordersBalances, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(openInterestLong, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(openInterestShort, bytes, SerializationUtils::marshallIntLongHashMap);
    }

    /**
     * 合并账户余额、订单余额、费用、调整和暂停余额，返回全局的余额汇总。
     * 
     * @return 全局余额汇总
     */
    public IntLongHashMap getGlobalBalancesSum() {
        return SerializationUtils.mergeSum(accountBalances, ordersBalances, fees, adjustments, suspends);
    }

    /**
     * 合并账户余额、订单余额和暂停余额，返回客户的余额汇总。
     * 
     * @return 客户余额汇总
     */
    public IntLongHashMap getClientsBalancesSum() {
        return SerializationUtils.mergeSum(accountBalances, ordersBalances, suspends);
    }

    /**
     * 检查全局余额是否全部为零。
     * 
     * @return 如果全局余额都为零，返回 `true`，否则返回 `false`
     */
    public boolean isGlobalBalancesAllZero() {
        return getGlobalBalancesSum().allSatisfy(amount -> amount == 0L);
    }

    /**
     * 合并多个 `TotalCurrencyBalanceReportResult` 实例的字节流，返回一个新的合并结果。
     * 
     * @param pieces 字节流的流
     * @return 合并后的 `TotalCurrencyBalanceReportResult` 实例
     */
    public static TotalCurrencyBalanceReportResult merge(final Stream<BytesIn> pieces) {
        return pieces
                .map(TotalCurrencyBalanceReportResult::new)
                .reduce(
                        TotalCurrencyBalanceReportResult.createEmpty(),
                        (a, b) -> new TotalCurrencyBalanceReportResult(
                                SerializationUtils.mergeSum(a.accountBalances, b.accountBalances),
                                SerializationUtils.mergeSum(a.fees, b.fees),
                                SerializationUtils.mergeSum(a.adjustments, b.adjustments),
                                SerializationUtils.mergeSum(a.suspends, b.suspends),
                                SerializationUtils.mergeSum(a.ordersBalances, b.ordersBalances),
                                SerializationUtils.mergeSum(a.openInterestLong, b.openInterestLong),
                                SerializationUtils.mergeSum(a.openInterestShort, b.openInterestShort)));
    }

}
