package com.exchange.tests.integration;


import com.exchange.core.common.config.PerformanceConfiguration;

public final class ITFeesMarginBasic extends ITFeesMargin {
    @Override
    public PerformanceConfiguration getPerformanceConfiguration() {
        return PerformanceConfiguration.DEFAULT;
    }
}
