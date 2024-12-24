package com.exchange.tests.perf.modules;

import com.exchange.core.common.config.LoggingConfiguration;
import com.exchange.core.orderbook.IOrderBook;
import com.exchange.core.orderbook.OrderBookDirectImpl;
import com.exchange.core.orderbook.OrderBookEventsHelper;
import com.exchange.tests.util.TestConstants;
import exchange.core2.collections.objpool.ObjectsPool;

public class ITOrderBookDirectImpl extends ITOrderBookBase {

    @Override
    protected IOrderBook createNewOrderBook() {

        return new OrderBookDirectImpl(
                TestConstants.SYMBOLSPEC_EUR_USD,
                ObjectsPool.createDefaultTestPool(),
                OrderBookEventsHelper.NON_POOLED_EVENTS_HELPER,
                LoggingConfiguration.DEFAULT);
    }
}
