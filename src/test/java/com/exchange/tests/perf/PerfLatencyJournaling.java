package com.exchange.tests.perf;

import com.exchange.core.common.config.InitialStateConfiguration;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.core.common.config.SerializationConfiguration;
import com.exchange.tests.util.ExchangeTestContainer;
import com.exchange.tests.util.TestDataParameters;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static com.exchange.tests.util.LatencyTestsModule.latencyTestImpl;

@Slf4j
public final class PerfLatencyJournaling {

    /*
     * -------------- Disk Journaling tests -----------------
     * Recommended tuned profile: latency-performance
     *
     */

    @Test
    public void testLatencyMarginJournaling() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairMarginBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                6);
    }

    @Test
    public void testLatencyExchangeJournaling() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                6);
    }

    @Test
    public void testLatencyMultiSymbolMediumJournaling() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.mediumBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                3);
    }

    @Test
    public void testLatencyMultiSymbolLargeJournaling() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.largeBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                3);
    }

    @Test
    public void testLatencyMultiSymbolHugeJournaling() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.hugeBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                2);
    }
}