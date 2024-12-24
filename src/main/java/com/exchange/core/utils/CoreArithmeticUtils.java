package com.exchange.core.utils;

import com.exchange.core.common.CoreSymbolSpecification;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class CoreArithmeticUtils {

    /**
     * 根据符号规格计算卖单的金额（Ask金额）。
     * 
     * @param size 交易的数量（大小）
     * @param spec 符号规格，包含了与该交易对相关的各种参数，如价格规模因子
     * @return 返回卖单金额
     */
    public static long calculateAmountAsk(long size, CoreSymbolSpecification spec) {
        return size * spec.baseScaleK;  // 使用基础规模因子计算金额
    }

    /**
     * 根据符号规格计算买单的金额（Bid金额）。
     * 
     * @param size 交易的数量（大小）
     * @param price 价格
     * @param spec 符号规格，包含了与该交易对相关的各种参数，如报价规模因子
     * @return 返回买单金额
     */
    public static long calculateAmountBid(long size, long price, CoreSymbolSpecification spec) {
        return size * (price * spec.quoteScaleK);  // 使用价格和报价规模因子计算金额
    }

    /**
     * 计算带有买单手续费的金额（Bid Taker Fee）。
     * 
     * @param size 交易的数量（大小）
     * @param price 价格
     * @param spec 符号规格，包含了与该交易对相关的各种参数，如报价规模因子和手续费
     * @return 返回带有手续费的买单金额
     */
    public static long calculateAmountBidTakerFee(long size, long price, CoreSymbolSpecification spec) {
        return size * (price * spec.quoteScaleK + spec.takerFee);  // 计算买单金额并加上手续费
    }

    /**
     * 计算带有买单手续费和价格差异调整的金额（Bid Taker Fee for Budget）。
     * 
     * @param size 交易的数量（大小）
     * @param priceDiff 价格差异
     * @param spec 符号规格，包含了与该交易对相关的各种参数，如报价规模因子、手续费和做市商费用
     * @return 返回带有价格差异和手续费调整的金额
     */
    public static long calculateAmountBidReleaseCorrMaker(long size, long priceDiff, CoreSymbolSpecification spec) {
        return size * (priceDiff * spec.quoteScaleK + (spec.takerFee - spec.makerFee));  // 加上价格差异和手续费差异
    }

    /**
     * 根据预算和买单手续费计算金额。
     * 
     * @param size 交易的数量（大小）
     * @param budgetInSteps 预算（以步骤为单位）
     * @param spec 符号规格，包含了与该交易对相关的各种参数，如报价规模因子和手续费
     * @return 返回根据预算和买单手续费计算的金额
     */
    public static long calculateAmountBidTakerFeeForBudget(long size, long budgetInSteps, CoreSymbolSpecification spec) {
        return budgetInSteps * spec.quoteScaleK + size * spec.takerFee;  // 使用预算和手续费计算金额
    }

}
