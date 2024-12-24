package com.exchange.tests.util;

import com.exchange.core.common.CoreSymbolSpecification;
import com.exchange.core.common.constant.SymbolType;
import com.google.common.collect.Sets;

import java.util.Set;

public final class TestConstants {

    // 各种符号常量，表示不同的交易对或交易类型
    public static final int SYMBOL_MARGIN = 5991; // 代表保证金交易符号
    public static final int SYMBOL_EXCHANGE = 9269; // 代表交易对符号
    public static final int SYMBOL_EXCHANGE_FEE = 9340; // 代表带有费用的交易对符号

    // 一些常见的用户ID常量，用于模拟不同的用户进行交易
    public static final long UID_1 = 1440001; 
    public static final long UID_2 = 1440002;
    public static final long UID_3 = 1440003;
    public static final long UID_4 = 1440004;

    // 自动生成符号的ID范围起始值
    public static final int SYMBOL_AUTOGENERATED_RANGE_START = 40000;

    // 各种法定货币的代码（根据ISO标准的货币代码）
    public static final int CURRENECY_AUD = 36; // 澳元
    public static final int CURRENECY_BRL = 986; // 巴西雷亚尔
    public static final int CURRENECY_CAD = 124; // 加元
    public static final int CURRENECY_CHF = 756; // 瑞士法郎
    public static final int CURRENECY_CNY = 156; // 人民币
    public static final int CURRENECY_CZK = 203; // 捷克克朗
    public static final int CURRENECY_DKK = 208; // 丹麦克朗
    public static final int CURRENECY_EUR = 978; // 欧元
    public static final int CURRENECY_GBP = 826; // 英镑
    public static final int CURRENECY_HKD = 344; // 港元
    public static final int CURRENECY_JPY = 392; // 日元
    public static final int CURRENECY_KRW = 410; // 韩元
    public static final int CURRENECY_MXN = 484; // 墨西哥比索
    public static final int CURRENECY_MYR = 458; // 马来西亚林吉特
    public static final int CURRENECY_NOK = 578; // 挪威克朗
    public static final int CURRENECY_NZD = 554; // 新西兰元
    public static final int CURRENECY_PLN = 985; // 波兰兹罗提
    public static final int CURRENECY_RUB = 643; // 俄罗斯卢布
    public static final int CURRENECY_SEK = 752; // 瑞典克朗
    public static final int CURRENECY_SGD = 702; // 新加坡元
    public static final int CURRENECY_THB = 764; // 泰铢
    public static final int CURRENECY_TRY = 949; // 土耳其里拉
    public static final int CURRENECY_UAH = 980; // 乌克兰赫夫纳
    public static final int CURRENECY_USD = 840; // 美元
    public static final int CURRENECY_VND = 704; // 越南盾
    public static final int CURRENECY_XAG = 961; // 白银
    public static final int CURRENECY_XAU = 959; // 黄金
    public static final int CURRENECY_ZAR = 710; // 南非兰特

    // 加密货币代码
    public static final int CURRENECY_XBT = 3762; // 比特币（Satoshi，1E-8）
    public static final int CURRENECY_ETH = 3928; // 以太坊（Szabo，1E-6）
    public static final int CURRENECY_LTC = 4141; // 莱特币（Litoshi，1E-8）
    public static final int CURRENECY_XDG = 4142; // Dogecoin
    public static final int CURRENECY_GRC = 4143; // GridCoin
    public static final int CURRENECY_XPM = 4144; // Primecoin
    public static final int CURRENECY_XRP = 4145; // 瑞波币
    public static final int CURRENECY_DASH = 4146; // Dash
    public static final int CURRENECY_XMR = 4147; // 门罗币
    public static final int CURRENECY_XLM = 4148; // Stellar
    public static final int CURRENECY_ETC = 4149; // 以太坊经典
    public static final int CURRENECY_ZEC = 4150; // ZCash

    // 定义不同的货币类型，用于期货交易和交换交易
    public static final Set<Integer> CURRENCIES_FUTURES = Sets.newHashSet(
            CURRENECY_USD,
            CURRENECY_EUR); // 期货交易的货币

    public static final Set<Integer> CURRENCIES_EXCHANGE = Sets.newHashSet(
            CURRENECY_ETH,
            CURRENECY_XBT); // 交换交易的货币

    // 定义所有支持的货币，包括法定货币和加密货币
    public static final Set<Integer> ALL_CURRENCIES = Sets.newHashSet(
            CURRENECY_AUD,
            CURRENECY_BRL,
            CURRENECY_CAD,
            CURRENECY_CHF,
            CURRENECY_CNY,
            CURRENECY_CZK,
            CURRENECY_DKK,
            CURRENECY_EUR,
            CURRENECY_GBP,
            CURRENECY_HKD,
            CURRENECY_JPY,
            CURRENECY_KRW,
            CURRENECY_MXN,
            CURRENECY_MYR,
            CURRENECY_NOK,
            CURRENECY_NZD,
            CURRENECY_PLN,
            CURRENECY_RUB,
            CURRENECY_SEK,
            CURRENECY_SGD,
            CURRENECY_THB,
            CURRENECY_TRY,
            CURRENECY_UAH,
            CURRENECY_USD,
            CURRENECY_VND,
            CURRENECY_XAG,
            CURRENECY_XAU,
            CURRENECY_ZAR,

            CURRENECY_XBT,
            CURRENECY_ETH,
            CURRENECY_LTC,
            CURRENECY_XDG,
            CURRENECY_GRC,
            CURRENECY_XPM,
            CURRENECY_XRP,
            CURRENECY_DASH,
            CURRENECY_XMR,
            CURRENECY_XLM,
            CURRENECY_ETC,
            CURRENECY_ZEC);

    // 示例：定义某些符号的规格
    // EUR/USD 期货合约的符号规格
    public static final CoreSymbolSpecification SYMBOLSPEC_EUR_USD = CoreSymbolSpecification.builder()
            .symbolId(SYMBOL_MARGIN)
            .type(SymbolType.FUTURES_CONTRACT)
            .baseCurrency(CURRENECY_EUR)  // 基础货币：欧元
            .quoteCurrency(CURRENECY_USD) // 报价货币：美元
            .baseScaleK(1) 
            .quoteScaleK(1)
            .marginBuy(2200)  // 买入保证金
            .marginSell(3210) // 卖出保证金
            .takerFee(0) // Taker费用
            .makerFee(0)  // Maker费用
            .build();

    // USD/JPY 期货合约，包含手续费
    public static final CoreSymbolSpecification SYMBOLSPECFEE_USD_JPY = CoreSymbolSpecification.builder()
            .symbolId(SYMBOL_MARGIN)
            .type(SymbolType.FUTURES_CONTRACT)
            .baseCurrency(CURRENECY_USD) // 基础货币：美元
            .quoteCurrency(CURRENECY_JPY) // 报价货币：日元
            .baseScaleK(1_000_00) // 1K USD "微" 交易单位
            .quoteScaleK(10)  // 每个步骤：10 JPY
            .marginBuy(5_000) // 买入保证金
            .marginSell(6_000) // 卖出保证金
            .takerFee(3) // Taker手续费
            .makerFee(2) // Maker手续费
            .build();

    // ETH/XBT 交换交易对的符号规格
    public static final CoreSymbolSpecification SYMBOLSPEC_ETH_XBT = CoreSymbolSpecification.builder()
            .symbolId(SYMBOL_EXCHANGE)
            .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
            .baseCurrency(CURRENECY_ETH)  // 基础货币：以太坊
            .quoteCurrency(CURRENECY_XBT) // 报价货币：比特币
            .baseScaleK(100_000)           // 每个交易单位：100K Szabo (0.1 ETH)
            .quoteScaleK(10)               // 每个步骤：10 satoshi
            .takerFee(0)                   // Taker费用
            .makerFee(0)                   // Maker费用
            .build();

    // XBT/LTC 带手续费的交换交易对的符号规格
    public static final CoreSymbolSpecification SYMBOLSPECFEE_XBT_LTC = CoreSymbolSpecification.builder()
            .symbolId(SYMBOL_EXCHANGE_FEE)
            .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
            .baseCurrency(CURRENECY_XBT)  // 基础货币：比特币（satoshi）
            .quoteCurrency(CURRENECY_LTC) // 报价货币：莱特币（litoshi）
            .baseScaleK(1_000_000)        // 1个交易单位：1M satoshi (0.01 BTC)
            .quoteScaleK(10_000)          // 每个步骤：10K litoshi
            .takerFee(1900)               // Taker费用：1900 litoshi
            .makerFee(700)                // Maker费用：700 litoshi
            .build();

    // 根据货币名称获取货币的对应代码
    public static int getCurrency(String currency) {
        switch (currency) {
            case "USD":
                return CURRENECY_USD;
            case "XBT":
                return CURRENECY_XBT;
            case "ETH":
                return CURRENECY_ETH;
        }
        throw new RuntimeException("Unknown currency [" + currency + "]");
    }
}
