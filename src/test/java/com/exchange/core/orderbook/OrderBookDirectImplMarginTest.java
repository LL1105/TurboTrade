package com.exchange.core.orderbook;

import com.exchange.core.common.CoreSymbolSpecification;
import com.exchange.core.common.config.LoggingConfiguration;
import com.exchange.tests.util.TestConstants;
import exchange.core2.collections.objpool.ObjectsPool;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class OrderBookDirectImplMarginTest extends OrderBookDirectImplTest {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookDirectImpl(
                getCoreSymbolSpec(),
                ObjectsPool.createDefaultTestPool(),
                OrderBookEventsHelper.NON_POOLED_EVENTS_HELPER,
                LoggingConfiguration.DEFAULT);
    }

    @Override
    protected CoreSymbolSpecification getCoreSymbolSpec() {
        return TestConstants.SYMBOLSPEC_EUR_USD;
    }

}