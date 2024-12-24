package com.exchange.tests.util;

import com.exchange.core.common.api.ApiCommand;
import com.exchange.core.common.api.ApiPersistState;
import com.exchange.core.common.config.InitialStateConfiguration;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.core.common.config.SerializationConfiguration;
import com.exchange.core.common.constant.CommandResultCode;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.core.Is;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class PersistenceTestsModule {

    // TODO 当前持久化测试没有覆盖持仓序列化

    /**
     * 持久化测试实现
     * 
     * @param performanceConfiguration 性能配置
     * @param testDataParameters 测试数据参数
     * @param iterations 测试的迭代次数
     * @throws InterruptedException 
     * @throws ExecutionException 
     */
    public static void persistenceTestImpl(final PerformanceConfiguration performanceConfiguration,
                                           final TestDataParameters testDataParameters,
                                           final int iterations) throws InterruptedException, ExecutionException {

        for (int iteration = 0; iteration < iterations; iteration++) {

            log.debug(" ----------- 持久化测试 --- 第 {} 次迭代，共 {} 次 ----", iteration, iterations);

            final long stateId;

            // 异步准备测试数据
            final ExchangeTestContainer.TestDataFutures testDataFutures = ExchangeTestContainer.prepareTestDataAsync(testDataParameters, iteration);

            final String exchangeId = String.format("%012X", System.currentTimeMillis());
            final InitialStateConfiguration firstStartConfig = InitialStateConfiguration.cleanStart(exchangeId);

            final long originalPrefillStateHash;
            final float originalPerfMt;

            try (final ExchangeTestContainer container = ExchangeTestContainer.create(performanceConfiguration, firstStartConfig, SerializationConfiguration.DISK_SNAPSHOT_ONLY)) {

                // 加载符号、用户和预填充订单数据
                container.loadSymbolsUsersAndPrefillOrders(testDataFutures);

                log.info("创建快照...");
                stateId = System.currentTimeMillis() * 1000 + iteration;
                final ApiPersistState apiPersistState = ApiPersistState.builder().dumpId(stateId).build();
                try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("快照 {} 创建完成，用时 {}", stateId, t))) {
                    final CommandResultCode resultCode = container.getApi().submitCommandAsync(apiPersistState).get();
                    assertThat(resultCode, Is.is(CommandResultCode.SUCCESS));
                }

                log.info("请求状态哈希...");
                originalPrefillStateHash = container.requestStateHash();

                log.info("基准测试原始状态...");

                // 进行原始状态的性能基准测试
                originalPerfMt = container.executeTestingThreadPerfMtps(() -> {
                    final List<ApiCommand> apiCommandsBenchmark = testDataFutures.genResult.get().getApiCommandsBenchmark().join();
                    container.getApi().submitCommandsSync(apiCommandsBenchmark);
                    return apiCommandsBenchmark.size();
                });

                assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                log.info("{}. 原始吞吐量: {} MT/s", iteration, String.format("%.3f", originalPerfMt));
            }

            // 触发垃圾回收
            System.gc();
            Thread.sleep(200);

            // 从快照恢复配置
            final InitialStateConfiguration fromSnapshotConfig = InitialStateConfiguration.fromSnapshotOnly(exchangeId, stateId, 0);

            log.debug("从持久化状态创建新的交易所...");
            final long tLoad = System.currentTimeMillis();
            try (final ExchangeTestContainer recreatedContainer = ExchangeTestContainer.create(performanceConfiguration, fromSnapshotConfig, SerializationConfiguration.DISK_SNAPSHOT_ONLY)) {

                // 简单的同步查询，确保核心系统已经启动并可以响应
                recreatedContainer.totalBalanceReport();

                float loadTimeSec = (float) (System.currentTimeMillis() - tLoad) / 1000.0f;
                log.debug("加载+启动时间: {}s", String.format("%.3f", loadTimeSec));

                log.info("请求状态哈希...");
                final long restoredPrefillStateHash = recreatedContainer.requestStateHash();
                assertThat(restoredPrefillStateHash, is(originalPrefillStateHash));

                assertTrue(recreatedContainer.totalBalanceReport().isGlobalBalancesAllZero());
                log.info("恢复的快照有效，基准测试恢复后的状态...");

                // 进行恢复后的状态性能基准测试
                final float perfMt = recreatedContainer.executeTestingThreadPerfMtps(() -> {
                    final List<ApiCommand> apiCommandsBenchmark = testDataFutures.genResult.get().getApiCommandsBenchmark().join();
                    recreatedContainer.getApi().submitCommandsSync(apiCommandsBenchmark);
                    return apiCommandsBenchmark.size();
                });

                // 比较恢复后的吞吐量与原始吞吐量
                final float perfRatioPerc = perfMt / originalPerfMt * 100f;
                log.info("{}. 恢复后的吞吐量: {} MT/s ({}%)", iteration, String.format("%.3f", perfMt), String.format("%.1f", perfRatioPerc));
            }

            // 再次触发垃圾回收
            System.gc();
            Thread.sleep(200);
        }
    }

    // 忽略传入对象的消费者
    private static final Consumer<? super Object> IGNORING_CONSUMER = x -> {
    };

}
