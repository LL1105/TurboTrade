package com.exchange.tests.perf;

import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.tests.util.ExchangeTestContainer;
import com.exchange.tests.util.PersistenceTestsModule;
import com.exchange.tests.util.TestDataParameters;
import com.exchange.tests.util.TestOrdersGeneratorConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public final class PerfPersistence {


    /**
     * This is serialization test for simplified conditions
     * - one symbol
     * - ~1K active users (2K currency accounts)
     * - 1K pending limit-orders (in one order book)
     * 6-threads CPU can run this test
     */
    @Test
    public void testPersistenceMargin() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(512)
                        .build(),
                TestDataParameters.singlePairMarginBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                10);
    }

    @Test
    public void testPersistenceExchange() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(512)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                10);
    }

    /**
     * This is serialization test for verifying "triple million" capability.
     * This test requires 10+ GiB free disk space, 16+ GiB of RAM and 12-threads CPU
     */
    @Test
    public void testPersistenceMultiSymbolMedium() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(1024)
                        .build(),
                TestDataParameters.mediumBuilder()
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                25);
    }

    @Test
    public void testPersistenceMultiSymbolLarge() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(4)
                        .msgsInGroupLimit(1024)
                        .build(),
                TestDataParameters.largeBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                25);
    }

    @Test
    public void testPersistenceMultiSymbolHuge() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(4)
                        .msgsInGroupLimit(1024)
                        .build(),
                TestDataParameters.hugeBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                25);
    }


}