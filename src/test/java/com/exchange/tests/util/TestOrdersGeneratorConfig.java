package com.exchange.tests.util;

import com.exchange.core.common.CoreSymbolSpecification;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.BitSet;
import java.util.List;
import java.util.function.Function;

@AllArgsConstructor
@Builder
@Getter
public class TestOrdersGeneratorConfig {

    // 核心符号的配置列表
    final List<CoreSymbolSpecification> coreSymbolSpecifications;

    // 总交易数量
    final int totalTransactionsNumber;

    // 用户账户的列表，每个账户的状态通过 BitSet 表示
    final List<BitSet> usersAccounts;

    // 目标订单簿的总订单数
    final int targetOrderBookOrdersTotal;

    // 随机数生成的种子值
    final int seed;

    // 是否开启雪崩式（Avalanche）IOC（Immediate-Or-Cancel）订单处理
    final boolean avalancheIOC;

    // 订单填充模式（预填充模式）
    final PreFillMode preFillMode;

    // 预填充模式枚举类
    @AllArgsConstructor  // 自动生成构造器，接受枚举常量的计算函数作为参数
    public enum PreFillMode {

        // 按照目标订单簿的订单总数填充
        ORDERS_NUMBER(TestOrdersGeneratorConfig::getTargetOrderBookOrdersTotal),

        // 按照目标订单簿的订单总数的 1.25 倍填充，用于快照测试，允许一些未完成的挂单
        ORDERS_NUMBER_PLUS_QUARTER(config -> config.targetOrderBookOrdersTotal * 5 / 4);

        // 计算填充顺序的函数，根据配置生成填充的订单数
        final Function<TestOrdersGeneratorConfig, Integer> calculateReadySeqFunc;
    }
}
