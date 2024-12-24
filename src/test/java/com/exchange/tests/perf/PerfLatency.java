package com.exchange.tests.perf;

import com.exchange.core.common.config.InitialStateConfiguration;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.core.common.config.SerializationConfiguration;
import com.exchange.tests.util.TestDataParameters;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static com.exchange.tests.util.LatencyTestsModule.latencyTestImpl;

@Slf4j
public final class PerfLatency {

    /**
     * This is latency test for simplified conditions
     * - one symbol (margin mode)
     * - ~1K active users (2K currency accounts)
     * - 1K pending limit-orders (in one order book)
     * 6-threads CPU can run this test
     */
    @Test
    public void testLatencyMargin() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairMarginBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                16);
    }

    /**
     * This is latency test for simplified conditions
     * - one symbol (exchange mode)
     * - ~1K active users (2K currency accounts)
     * - 1K pending limit-orders (in one order book)
     * 6-threads CPU can run this test
     */
    @Test
    public void testLatencyExchange() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                16);
    }

    /**
     * This is medium load latency test for verifying "triple million" capability:
     * - 1M active users (3M currency accounts)
     * - 1M pending limit-orders
     * - 1M+ messages per second throughput
     * - 10K symbols
     * - less than 1 millisecond 99.99% latency
     * 12-threads CPU and 32GiB RAM is required for running this test in 2+4 configuration.
     */
    @Test
    public void testLatencyMultiSymbolMedium() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.mediumBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                8);
    }

    /**
     * This is high load latency test for verifying exchange core scalability:
     * - 3M active users (10M currency accounts)
     * - 3M pending limit-orders
     * - 1M+ messages per second throughput
     * - 100K symbols
     * - less than 1 millisecond 99.99% latency
     * 12-threads CPU and 32GiB RAM is required for running this test in 2+4 configuration.
     */
    @Test
    public void testLatencyMultiSymbolLarge() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.largeBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                4);
    }

    /**
     * This is high load latency test for verifying exchange core scalability:
     * - 10M active users (33M currency accounts)
     * - 30M pending limit-orders
     * - 200K symbols
     * - 1M+ messages per second throughput
     * 12-threads CPU and 32GiB RAM is required for running this test in 2+4 configuration.
     */
    @Test
    public void testLatencyMultiSymbolHuge() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.hugeBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                2);
    }

}