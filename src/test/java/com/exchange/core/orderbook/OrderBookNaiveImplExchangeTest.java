package com.exchange.core.orderbook;


import com.exchange.core.common.CoreSymbolSpecification;
import com.exchange.core.common.config.LoggingConfiguration;
import com.exchange.tests.util.TestConstants;

public final class OrderBookNaiveImplExchangeTest extends OrderBookBaseTest {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookNaiveImpl(getCoreSymbolSpec(), LoggingConfiguration.DEFAULT);
    }

    @Override
    protected CoreSymbolSpecification getCoreSymbolSpec() {
        return TestConstants.SYMBOLSPEC_ETH_XBT;
    }


}