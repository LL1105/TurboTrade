package com.exchange.tests.integration;

import com.exchange.core.common.config.InitialStateConfiguration;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.core.common.config.SerializationConfiguration;
import com.exchange.tests.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;


public final class ITMultiOperation {

    @Test
    @Timeout(60)
    public void shouldPerformMarginOperations() {
        ThroughputTestsModule.throughputTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .build(),
                TestDataParameters.builder()
                        .totalTransactionsNumber(1_000_000)
                        .targetOrderBookOrdersTotal(1000)
                        .numAccounts(2000)
                        .currenciesAllowed(TestConstants.CURRENCIES_FUTURES)
                        .numSymbols(1)
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT)
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                2
        );
    }

    @Test
    @Timeout(60)
    public void shouldPerformExchangeOperations() {
        ThroughputTestsModule.throughputTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .build(),
                TestDataParameters.builder()
                        .totalTransactionsNumber(1_000_000)
                        .targetOrderBookOrdersTotal(1000)
                        .numAccounts(2000)
                        .currenciesAllowed(TestConstants.CURRENCIES_EXCHANGE)
                        .numSymbols(1)
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR)
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                2);
    }

    @Test
    @Timeout(60)
    public void shouldPerformSharded() {
        ThroughputTestsModule.throughputTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .matchingEnginesNum(2)
                        .riskEnginesNum(2)
                        .build(),
                TestDataParameters.builder()
                        .totalTransactionsNumber(1_000_000)
                        .targetOrderBookOrdersTotal(1000)
                        .numAccounts(2000)
                        .currenciesAllowed(TestConstants.CURRENCIES_EXCHANGE)
                        .numSymbols(32)
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                2);
    }
}
