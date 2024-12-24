package com.exchange.tests.example;

import com.exchange.core.ExchangeApi;
import com.exchange.core.ExchangeCore;
import com.exchange.core.IEventsHandler;
import com.exchange.core.SimpleEventsProcessor;
import com.exchange.core.common.CoreSymbolSpecification;
import com.exchange.core.common.L2MarketData;
import com.exchange.core.common.api.*;
import com.exchange.core.common.api.binary.BatchAddSymbolsCommand;
import com.exchange.core.common.api.reports.queryImpl.SingleUserReportQuery;
import com.exchange.core.common.api.reports.queryImpl.TotalCurrencyBalanceReportQuery;
import com.exchange.core.common.api.reports.resultImpl.SingleUserReportResult;
import com.exchange.core.common.api.reports.resultImpl.TotalCurrencyBalanceReportResult;
import com.exchange.core.common.config.ExchangeConfiguration;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.OrderAction;
import com.exchange.core.common.constant.OrderType;
import com.exchange.core.common.constant.SymbolType;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

@Slf4j
public class ITCoreExample {

    @Test
    public void sampleTest() throws Exception {

        // 创建一个简单的异步事件处理器
        SimpleEventsProcessor eventsProcessor = new SimpleEventsProcessor(new IEventsHandler() {
            @Override
            public void tradeEvent(TradeEvent tradeEvent) {
                // 处理交易事件
                System.out.println("Trade event: " + tradeEvent);
            }

            @Override
            public void reduceEvent(ReduceEvent reduceEvent) {
                // 处理减少事件
                System.out.println("Reduce event: " + reduceEvent);
            }

            @Override
            public void rejectEvent(RejectEvent rejectEvent) {
                // 处理拒绝事件
                System.out.println("Reject event: " + rejectEvent);
            }

            @Override
            public void commandResult(ApiCommandResult commandResult) {
                // 处理命令结果事件
                System.out.println("Command result: " + commandResult);
            }

            @Override
            public void orderBook(OrderBook orderBook) {
                // 处理订单簿事件
                System.out.println("OrderBook event: " + orderBook);
            }
        });

        // 使用默认的交易所配置
        ExchangeConfiguration conf = ExchangeConfiguration.defaultBuilder().build();

        // 构建交易核心对象
        ExchangeCore exchangeCore = ExchangeCore.builder()
                .resultsConsumer(eventsProcessor)  // 设置事件处理器
                .exchangeConfiguration(conf)       // 设置交易所配置
                .build();

        // 启动 disruptor 线程
        exchangeCore.startup();

        // 获取交易所 API 用于发布命令
        ExchangeApi api = exchangeCore.getApi();

        // 定义货币代码常量
        final int currencyCodeXbt = 11;  // 比特币的货币代码
        final int currencyCodeLtc = 15;  // 莱特币的货币代码

        // 定义交易对常量
        final int symbolXbtLtc = 241;  // 比特币/莱特币的交易对ID

        Future<CommandResultCode> future;

        // 创建符号规范并发布它
        CoreSymbolSpecification symbolSpecXbtLtc = CoreSymbolSpecification.builder()
                .symbolId(symbolXbtLtc)         // 交易对ID
                .type(SymbolType.CURRENCY_EXCHANGE_PAIR)  // 交易对类型：货币兑换对
                .baseCurrency(currencyCodeXbt)    // 基础货币：比特币（1E-8）
                .quoteCurrency(currencyCodeLtc)   // 报价货币：莱特币（1E-8）
                .baseScaleK(1_000_000L) // 1手 = 1M satoshi (0.01 BTC)
                .quoteScaleK(10_000L)   // 1价格步长 = 10K litoshi
                .takerFee(1900L)        // Taker费用：每1手1900 litoshi
                .makerFee(700L)         // Maker费用：每1手700 litoshi
                .build();

        // 异步提交批量添加符号命令
        future = api.submitBinaryDataAsync(new BatchAddSymbolsCommand(symbolSpecXbtLtc));
        System.out.println("BatchAddSymbolsCommand result: " + future.get());


        // 创建第一个用户，uid=301
        future = api.submitCommandAsync(ApiAddUser.builder()
                .uid(301L)
                .build());

        System.out.println("ApiAddUser 1 result: " + future.get());


        // 创建第二个用户，uid=302
        future = api.submitCommandAsync(ApiAddUser.builder()
                .uid(302L)
                .build());

        System.out.println("ApiAddUser 2 result: " + future.get());

        // 第一个用户存入20 LTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(301L)
                .currency(currencyCodeLtc)
                .amount(2_000_000_000L)
                .transactionId(1L)
                .build());

        System.out.println("ApiAdjustUserBalance 1 result: " + future.get());


        // 第二个用户存入0.10 BTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(302L)
                .currency(currencyCodeXbt)
                .amount(10_000_000L)
                .transactionId(2L)
                .build());

        System.out.println("ApiAdjustUserBalance 2 result: " + future.get());


        // 第一个用户下达“Good-till-Cancel”买单
        // 假设BTCLTC的兑换率是1 BTC = 154 LTC
        // 以15400 litoshi的价格下单1手（0.01BTC），最多可调整至1.56 LTC的价格
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .price(15_400L)  // 设置价格
                .reservePrice(15_600L) // 设置保留价格
                .size(12L) // 设置订单大小
                .action(OrderAction.BID)  // 买单
                .orderType(OrderType.GTC) // Good-till-Cancel订单类型
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiPlaceOrder 1 result: " + future.get());


        // 第二个用户下达“Immediate-or-Cancel”卖单
        // 假设最差的卖出价格是1 BTC = 152.5 LTC
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(302L)
                .orderId(5002L)
                .price(15_250L)  // 设置价格
                .size(10L) // 设置订单大小
                .action(OrderAction.ASK)  // 卖单
                .orderType(OrderType.IOC) // Immediate-or-Cancel订单类型
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiPlaceOrder 2 result: " + future.get());


        // 请求订单簿
        CompletableFuture<L2MarketData> orderBookFuture = api.requestOrderBookAsync(symbolXbtLtc, 10);
        System.out.println("ApiOrderBookRequest result: " + orderBookFuture.get());


        // 第一个用户将剩余订单移至价格1.53 LTC
        future = api.submitCommandAsync(ApiMoveOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .newPrice(15_300L)  // 设置新的价格
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiMoveOrder 2 result: " + future.get());

        // 第一个用户取消剩余订单
        future = api.submitCommandAsync(ApiCancelOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiCancelOrder 2 result: " + future.get());

        // 查询第一个用户的账户报告
        Future<SingleUserReportResult> report1 = api.processReport(new SingleUserReportQuery(301), 0);
        System.out.println("SingleUserReportQuery 1 accounts: " + report1.get().getAccounts());

        // 查询第二个用户的账户报告
        Future<SingleUserReportResult> report2 = api.processReport(new SingleUserReportQuery(302), 0);
        System.out.println("SingleUserReportQuery 2 accounts: " + report2.get().getAccounts());

        // 第一个用户提取0.10 BTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(301L)
                .currency(currencyCodeXbt)
                .amount(-10_000_000L)
                .transactionId(3L)
                .build());

        System.out.println("ApiAdjustUserBalance 1 result: " + future.get());

        // 查询收取的费用
        Future<TotalCurrencyBalanceReportResult> totalsReport = api.processReport(new TotalCurrencyBalanceReportQuery(), 0);
        System.out.println("LTC fees collected: " + totalsReport.get().getFees().get(currencyCodeLtc));
    }
}
