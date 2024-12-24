package com.exchange.tests.integration;


import com.exchange.core.common.config.PerformanceConfiguration;

public class ITExchangeCoreIntegrationRejectionLatency extends ITExchangeCoreIntegrationRejection {

    @Override
    public PerformanceConfiguration getPerformanceConfiguration() {
        return PerformanceConfiguration.latencyPerformanceBuilder().build();
    }
}
