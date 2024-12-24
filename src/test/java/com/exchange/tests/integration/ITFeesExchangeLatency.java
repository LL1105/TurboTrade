package com.exchange.tests.integration;


import com.exchange.core.common.config.PerformanceConfiguration;

public class ITFeesExchangeLatency extends ITFeesExchange {
    @Override
    public PerformanceConfiguration getPerformanceConfiguration() {
        return PerformanceConfiguration.latencyPerformanceBuilder().build();
    }
}
