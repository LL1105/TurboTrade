package com.exchange.tests.util;

import com.exchange.core.common.api.ApiPersistState;
import com.exchange.core.common.config.InitialStateConfiguration;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.core.common.config.SerializationConfiguration;
import com.exchange.core.common.constant.CommandResultCode;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.core.Is;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class JournalingTestsModule {

    // 定义 journaling 测试实现的方法
    public static void journalingTestImpl(final PerformanceConfiguration performanceConfiguration,
                                          final TestDataParameters testDataParameters,
                                          final int iterations) throws InterruptedException, ExecutionException {

        // 循环执行测试，迭代次数为 iterations
        for (int iteration = 0; iteration < iterations; iteration++) {

            // 打印当前迭代的日志
            log.debug(" ----------- journaling test --- iteration {} of {} ----", iteration, iterations);

            // 准备测试数据
            final ExchangeTestContainer.TestDataFutures testDataFutures = ExchangeTestContainer.prepareTestDataAsync(testDataParameters, iteration);

            final long stateId;
            final long originalFinalStateHash;

            final String exchangeId = ExchangeTestContainer.timeBasedExchangeId();  // 获取基于时间的交易所ID

            // 初始化配置，表示从一个干净的状态开始进行日志记录
            final InitialStateConfiguration firstStartConfig = InitialStateConfiguration.cleanStartJournaling(exchangeId);

            try (final ExchangeTestContainer container = ExchangeTestContainer.create(performanceConfiguration, firstStartConfig, SerializationConfiguration.DISK_JOURNALING)) {

                // 加载符号、用户和预填充订单
                container.loadSymbolsUsersAndPrefillOrders(testDataFutures);

                log.info("Creating snapshot...");  // 创建快照
                stateId = System.currentTimeMillis() * 1000 + iteration;  // 生成状态ID
                final ApiPersistState apiPersistState = ApiPersistState.builder().dumpId(stateId).build();  // 构建持久化状态对象

                // 记录创建快照的时间
                try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("Snapshot {} created in {}", stateId, t))) {
                    // 异步提交命令并获取结果
                    final CommandResultCode resultCode = container.getApi().submitCommandAsync(apiPersistState).get();
                    // 确保命令结果是成功的
                    assertThat(resultCode, Is.is(CommandResultCode.SUCCESS));
                }

                log.info("Running commands on original state...");  // 在原始状态上运行命令
                // 获取生成的测试命令并同步执行
                final TestOrdersGenerator.MultiSymbolGenResult genResult = testDataFutures.genResult.get();
                container.getApi().submitCommandsSync(genResult.getApiCommandsBenchmark().join());
                // 确保所有余额为零
                assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                // 获取原始状态的哈希值
                originalFinalStateHash = container.requestStateHash();
                log.info("Original state checks completed");
            }

            // TODO 使用 DiskSerializationProcessor 来发现快照和日志

            final long snapshotBaseSeq = 0L;

            // 从快照创建新的交易所配置
            final InitialStateConfiguration fromSnapshotConfig = InitialStateConfiguration.lastKnownStateFromJournal(exchangeId, stateId, snapshotBaseSeq);

            log.debug("Creating new exchange from persisted state...");  // 从持久化状态创建新交易所
            final long tLoad = System.currentTimeMillis();  // 记录加载时间
            try (final ExchangeTestContainer recreatedContainer = ExchangeTestContainer.create(performanceConfiguration, fromSnapshotConfig, SerializationConfiguration.DISK_JOURNALING)) {

                // 执行同步查询，确保核心已经启动并可以响应
                recreatedContainer.totalBalanceReport();

                // 计算加载和启动的总时间
                float loadTimeSec = (float) (System.currentTimeMillis() - tLoad) / 1000.0f;
                log.debug("Load+start+replay time: {}s", String.format("%.3f", loadTimeSec));

                // 获取恢复的状态哈希值，并确保与原始状态哈希匹配
                final long restoredStateHash = recreatedContainer.requestStateHash();
                assertThat(restoredStateHash, is(originalFinalStateHash));

                // 确保所有余额为零
                assertTrue(recreatedContainer.totalBalanceReport().isGlobalBalancesAllZero());
                log.info("Restored snapshot+journal is valid");
            }

        }

    }
}
