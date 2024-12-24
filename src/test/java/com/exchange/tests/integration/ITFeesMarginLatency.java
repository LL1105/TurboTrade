package com.exchange.tests.integration;


import com.exchange.core.common.config.PerformanceConfiguration;

public final class ITFeesMarginLatency extends ITFeesMargin {
    @Override
    public PerformanceConfiguration getPerformanceConfiguration() {
        return PerformanceConfiguration.latencyPerformanceBuilder().build();
    }
}
