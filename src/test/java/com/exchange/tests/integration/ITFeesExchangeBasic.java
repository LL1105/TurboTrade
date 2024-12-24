package com.exchange.tests.integration;


import com.exchange.core.common.config.PerformanceConfiguration;

public class ITFeesExchangeBasic extends ITFeesExchange {
    @Override
    public PerformanceConfiguration getPerformanceConfiguration() {
        return PerformanceConfiguration.DEFAULT;
    }
}
