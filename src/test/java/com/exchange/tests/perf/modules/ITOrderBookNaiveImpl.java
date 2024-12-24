package com.exchange.tests.perf.modules;


import com.exchange.core.common.config.LoggingConfiguration;
import com.exchange.core.orderbook.IOrderBook;
import com.exchange.core.orderbook.OrderBookNaiveImpl;
import com.exchange.tests.util.TestConstants;

public class ITOrderBookNaiveImpl extends ITOrderBookBase {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookNaiveImpl(TestConstants.SYMBOLSPEC_EUR_USD, LoggingConfiguration.DEFAULT);
    }
}
