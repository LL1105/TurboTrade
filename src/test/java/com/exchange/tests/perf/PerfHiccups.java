package com.exchange.tests.perf;

import com.exchange.core.common.config.InitialStateConfiguration;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.tests.util.TestDataParameters;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static com.exchange.tests.util.LatencyTestsModule.hiccupTestImpl;


@Slf4j
public final class PerfHiccups {


    @Test
    public void testHiccupMargin() {
        hiccupTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairMarginBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                3);
    }


}