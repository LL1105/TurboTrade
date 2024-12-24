package com.exchange.tests.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exchange.core.common.config.InitialStateConfiguration;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.core.common.config.SerializationConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.IntStream;

@Slf4j
public class ThroughputTestsModule {

    /**
     * 执行吞吐量测试
     * 
     * @param performanceCfg 性能配置
     * @param testDataParameters 测试数据参数
     * @param initialStateCfg 初始状态配置
     * @param serializationCfg 序列化配置
     * @param iterations 测试迭代次数
     */
    public static void throughputTestImpl(final PerformanceConfiguration performanceCfg,
                                          final TestDataParameters testDataParameters,
                                          final InitialStateConfiguration initialStateCfg,
                                          final SerializationConfiguration serializationCfg,
                                          final int iterations) {

        // 异步准备测试数据
        final ExchangeTestContainer.TestDataFutures testDataFutures = ExchangeTestContainer.prepareTestDataAsync(testDataParameters, 1);

        // 创建测试容器并执行测试
        try (final ExchangeTestContainer container = ExchangeTestContainer.create(performanceCfg, initialStateCfg, serializationCfg)) {

            // 执行多次迭代并计算每次的吞吐量
            final float avgMt = container.executeTestingThread(
                    () -> (float) IntStream.range(0, iterations)
                            .mapToObj(j -> {
                                // 加载符号、用户和预填订单
                                container.loadSymbolsUsersAndPrefillOrdersNoLog(testDataFutures);

                                // 基准测试并计算每次的吞吐量 (MT/s)
                                final float perfMt = container.benchmarkMtps(testDataFutures.getGenResult().join().apiCommandsBenchmark.join());
                                log.info("{}. {} MT/s", j, String.format("%.3f", perfMt));

                                // 验证总余额报告是否正确
                                assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                                // 确保所有命令的最终订单簿状态一致
                                testDataFutures.coreSymbolSpecifications.join().forEach(
                                        symbol -> assertEquals(
                                                testDataFutures.getGenResult().join().getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(),
                                                container.requestCurrentOrderBook(symbol.symbolId)));

                                // TODO: 这里可以比较事件、余额、仓位等（未来可以实现）

                                // 重置交易核心并进行垃圾回收
                                container.resetExchangeCore();
                                System.gc();

                                return perfMt;
                            })
                            .mapToDouble(x -> x)
                            .average().orElse(0));

            // 输出平均吞吐量
            log.info("Average: {} MT/s", avgMt);
        }
    }
}
