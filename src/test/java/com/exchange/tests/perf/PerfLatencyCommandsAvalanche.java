package com.exchange.tests.perf;

import com.exchange.core.common.config.InitialStateConfiguration;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.tests.util.TestDataParameters;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static com.exchange.tests.util.LatencyTestsModule.individualLatencyTest;

@Slf4j
public final class PerfLatencyCommandsAvalanche {


    /**
     * Avalanche IoC orders test to verify matching performance for big-size taker orders.
     */
    @Test
    public void testLatencyMarginAvalancheIoc() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairMarginBuilder()
                        .avalancheIOC(true)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST);
    }


    /**
     * Avalanche IoC orders test to verify matching performance for big-size taker orders.
     */
    @Test
    public void testLatencyExchangeAvalancheIoc() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder()
                        .avalancheIOC(true)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST);
    }


    /**
     * Avalanche IoC orders test to verify matching performance for big-size taker orders. </br>
     * Why 4R1M configuration:</br>
     * Less matching engines provides better individual command latency - due to less interference. </br>
     * More risk engines provide better individual command latency because of parallel processing R2 stage (0.5 + 0.5/N) </br>
     */
    @Test
    public void testLatencyMultiSymbolMediumAvalancheIOC() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(4)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.mediumBuilder()
                        .numSymbols(2_000)
                        .avalancheIOC(true)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST);
    }


    @Test
    public void testLatencyMultiSymbolLargeAvalancheIOC() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(4)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.largeBuilder()
                        .numSymbols(5_000)
                        .avalancheIOC(true)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST);
    }

}