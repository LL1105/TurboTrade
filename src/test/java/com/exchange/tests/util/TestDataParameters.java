package com.exchange.tests.util;

import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Data
@Builder
public class TestDataParameters {
    // 总交易数量
    public final int totalTransactionsNumber;
    // 目标订单簿中的订单总数
    public final int targetOrderBookOrdersTotal;
    // 用户账户数量
    public final int numAccounts;
    // 允许的货币种类
    public final Set<Integer> currenciesAllowed;
    // 符号数量（交易对的数量）
    public final int numSymbols;
    // 允许的符号类型（如期货合约、货币交换对等）
    public final ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes;
    // 预填充模式（如填充订单数量等）
    public final TestOrdersGeneratorConfig.PreFillMode preFillMode;
    // 是否启用雪崩（Avalanche）IOC（立即取消）订单
    public final boolean avalancheIOC;

    // 单一交易对的期货合约测试数据构造器
    public static TestDataParameters.TestDataParametersBuilder singlePairMarginBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(3_000_000)  // 设置总交易数量为300万
                .targetOrderBookOrdersTotal(1000)    // 设置目标订单簿的订单总数为1000
                .numAccounts(2000)                   // 设置账户数量为2000
                .currenciesAllowed(TestConstants.CURRENCIES_FUTURES) // 设置允许的货币类型为期货货币
                .numSymbols(1)                       // 设置符号数量为1
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT) // 允许的符号类型为期货合约
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER); // 设置预填充模式为按订单数量填充
    }

    // 单一交易对的货币交换对测试数据构造器
    public static TestDataParameters.TestDataParametersBuilder singlePairExchangeBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(3_000_000)  // 设置总交易数量为300万
                .targetOrderBookOrdersTotal(1000)    // 设置目标订单簿的订单总数为1000
                .numAccounts(2000)                   // 设置账户数量为2000
                .currenciesAllowed(TestConstants.CURRENCIES_EXCHANGE) // 设置允许的货币类型为货币交换对
                .numSymbols(1)                       // 设置符号数量为1
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR) // 允许的符号类型为货币交换对
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER); // 设置预填充模式为按订单数量填充
    }

    /**
     * 中等规模的交易所测试数据配置：
     * - 1M 活跃用户（3M 货币账户）
     * - 1M 待处理限价订单
     * - 10K 符号
     *
     * @return 中等规模的测试数据配置
     */
    public static TestDataParameters.TestDataParametersBuilder mediumBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(3_000_000)  // 设置总交易数量为300万
                .targetOrderBookOrdersTotal(1_000_000) // 设置目标订单簿的订单总数为100万
                .numAccounts(3_300_000)                // 设置账户数量为330万
                .currenciesAllowed(TestConstants.ALL_CURRENCIES) // 设置允许的货币类型为所有货币
                .numSymbols(10_000)                    // 设置符号数量为10,000
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH) // 允许的符号类型为期货合约和货币交换对
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER); // 设置预填充模式为按订单数量填充
    }

    /**
     * 大规模的交易所测试数据配置：
     * - 3M 活跃用户（10M 货币账户）
     * - 3M 待处理限价订单
     * - 50K 符号
     *
     * @return 大规模的测试数据配置
     */
    public static TestDataParameters.TestDataParametersBuilder largeBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(3_000_000)  // 设置总交易数量为300万
                .targetOrderBookOrdersTotal(3_000_000) // 设置目标订单簿的订单总数为300万
                .numAccounts(10_000_000)              // 设置账户数量为1000万
                .currenciesAllowed(TestConstants.ALL_CURRENCIES) // 设置允许的货币类型为所有货币
                .numSymbols(50_000)                   // 设置符号数量为50,000
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH) // 允许的符号类型为期货合约和货币交换对
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER); // 设置预填充模式为按订单数量填充
    }

    /**
     * 超大规模的交易所测试数据配置：
     * - 10M 活跃用户（33M 货币账户）
     * - 30M 待处理限价订单
     * - 100K 符号
     *
     * @return 超大规模的测试数据配置
     */
    public static TestDataParameters.TestDataParametersBuilder hugeBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(10_000_000) // 设置总交易数量为1000万
                .targetOrderBookOrdersTotal(30_000_000) // 设置目标订单簿的订单总数为3000万
                .numAccounts(33_000_000)               // 设置账户数量为3300万
                .currenciesAllowed(TestConstants.ALL_CURRENCIES) // 设置允许的货币类型为所有货币
                .numSymbols(100_000)                   // 设置符号数量为100,000
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH) // 允许的符号类型为期货合约和货币交换对
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER); // 设置预填充模式为按订单数量填充
    }
}
