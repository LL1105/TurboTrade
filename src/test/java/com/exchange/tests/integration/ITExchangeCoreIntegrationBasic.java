package com.exchange.tests.integration;


import com.exchange.core.common.config.PerformanceConfiguration;

public final class ITExchangeCoreIntegrationBasic extends ITExchangeCoreIntegration {

    @Override
    public PerformanceConfiguration getPerformanceConfiguration() {
        return PerformanceConfiguration.DEFAULT;
    }
}
