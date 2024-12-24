package com.exchange.core;

import com.exchange.core.common.L2MarketData;
import com.exchange.core.common.MatcherTradeEvent;
import com.exchange.core.common.api.*;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.MatcherEventType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableLong;
import org.agrona.collections.MutableReference;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ObjLongConsumer;

// 该类负责处理并发送订单相关的事件，例如交易事件、市场数据和命令结果
@RequiredArgsConstructor
@Getter
@Slf4j
public class SimpleEventsProcessor implements ObjLongConsumer<OrderCommand> {

    private final IEventsHandler eventsHandler; // 事件处理器，用于发送事件到外部系统

    @Override
    public void accept(OrderCommand cmd, long seq) {
        try {
            // 发送命令结果、交易事件和市场数据
            sendCommandResult(cmd, seq);
            sendTradeEvents(cmd);
            sendMarketData(cmd);
        } catch (Exception ex) {
            log.error("处理命令结果数据时发生异常", ex); // 处理异常并记录日志
        }
    }

    // 发送交易事件
    private void sendTradeEvents(OrderCommand cmd) {
        final MatcherTradeEvent firstEvent = cmd.matcherEvent;
        if (firstEvent == null) {
            return; // 如果没有交易事件，则不处理
        }

        // 如果是“减少”事件，则发送减少事件
        if (firstEvent.eventType == MatcherEventType.REDUCE) {
            final IEventsHandler.ReduceEvent evt = new IEventsHandler.ReduceEvent(
                    cmd.symbol,
                    firstEvent.size,
                    firstEvent.activeOrderCompleted,
                    firstEvent.price,
                    cmd.orderId,
                    cmd.uid,
                    cmd.timestamp);

            eventsHandler.reduceEvent(evt); // 发送减少事件

            if (firstEvent.nextEvent != null) {
                throw new IllegalStateException("只期望一个 REDUCE 事件"); // 如果有后续事件，抛出异常
            }

            return; // 完成后返回
        }

        // 发送普通的交易事件
        sendTradeEvent(cmd);
    }

    // 发送交易事件
    private void sendTradeEvent(OrderCommand cmd) {
        final MutableBoolean takerOrderCompleted = new MutableBoolean(false); // 标记是否完成了买单
        final MutableLong mutableLong = new MutableLong(0L); // 累加交易的数量
        final List<IEventsHandler.Trade> trades = new ArrayList<>(); // 存储所有的交易事件

        final MutableReference<IEventsHandler.RejectEvent> rejectEvent = new MutableReference<>(null); // 存储拒绝事件

        // 遍历匹配器事件，处理每个事件
        cmd.processMatcherEvents(evt -> {

            if (evt.eventType == MatcherEventType.TRADE) { // 如果是交易事件
                final IEventsHandler.Trade trade = new IEventsHandler.Trade(
                        evt.matchedOrderId,
                        evt.matchedOrderUid,
                        evt.matchedOrderCompleted,
                        evt.price,
                        evt.size);

                trades.add(trade); // 将交易事件加入列表
                mutableLong.value += evt.size; // 累加交易数量

                if (evt.activeOrderCompleted) { // 如果活跃订单已完成
                    takerOrderCompleted.value = true; // 标记买单已完成
                }

            } else if (evt.eventType == MatcherEventType.REJECT) { // 如果是拒绝事件
                rejectEvent.set(new IEventsHandler.RejectEvent(
                        cmd.symbol,
                        evt.size,
                        evt.price,
                        cmd.orderId,
                        cmd.uid,
                        cmd.timestamp));
            }
        });

        // 如果有交易事件，则发送交易事件
        if (!trades.isEmpty()) {
            final IEventsHandler.TradeEvent evt = new IEventsHandler.TradeEvent(
                    cmd.symbol,
                    mutableLong.value,
                    cmd.orderId,
                    cmd.uid,
                    cmd.action,
                    takerOrderCompleted.value,
                    cmd.timestamp,
                    trades);

            eventsHandler.tradeEvent(evt); // 发送交易事件
        }

        // 如果有拒绝事件，则发送拒绝事件
        if (rejectEvent.ref != null) {
            eventsHandler.rejectEvent(rejectEvent.ref);
        }
    }

    // 发送市场数据事件
    private void sendMarketData(OrderCommand cmd) {
        final L2MarketData marketData = cmd.marketData;
        if (marketData != null) {
            final List<IEventsHandler.OrderBookRecord> asks = new ArrayList<>(marketData.askSize);
            for (int i = 0; i < marketData.askSize; i++) {
                asks.add(new IEventsHandler.OrderBookRecord(marketData.askPrices[i], marketData.askVolumes[i], (int) marketData.askOrders[i]));
            }

            final List<IEventsHandler.OrderBookRecord> bids = new ArrayList<>(marketData.bidSize);
            for (int i = 0; i < marketData.bidSize; i++) {
                bids.add(new IEventsHandler.OrderBookRecord(marketData.bidPrices[i], marketData.bidVolumes[i], (int) marketData.bidOrders[i]));
            }

            // 发送订单簿数据
            eventsHandler.orderBook(new IEventsHandler.OrderBook(cmd.symbol, asks, bids, cmd.timestamp));
        }
    }

    // 发送命令结果
    private void sendCommandResult(OrderCommand cmd, long seq) {
        switch (cmd.command) {
            case PLACE_ORDER:
                sendApiCommandResult(new ApiPlaceOrder(
                                cmd.price,
                                cmd.size,
                                cmd.orderId,
                                cmd.action,
                                cmd.orderType,
                                cmd.uid,
                                cmd.symbol,
                                cmd.userCookie,
                                cmd.reserveBidPrice),
                        cmd.resultCode,
                        cmd.timestamp,
                        seq);
                break;

            case MOVE_ORDER:
                sendApiCommandResult(new ApiMoveOrder(cmd.orderId, cmd.price, cmd.uid, cmd.symbol), cmd.resultCode, cmd.timestamp, seq);
                break;

            case CANCEL_ORDER:
                sendApiCommandResult(new ApiCancelOrder(cmd.orderId, cmd.uid, cmd.symbol), cmd.resultCode, cmd.timestamp, seq);
                break;

            case REDUCE_ORDER:
                sendApiCommandResult(new ApiReduceOrder(cmd.orderId, cmd.uid, cmd.symbol, cmd.size), cmd.resultCode, cmd.timestamp, seq);
                break;

            case ADD_USER:
                sendApiCommandResult(new ApiAddUser(cmd.uid), cmd.resultCode, cmd.timestamp, seq);
                break;

            case BALANCE_ADJUSTMENT:
                sendApiCommandResult(new ApiAdjustUserBalance(cmd.uid, cmd.symbol, cmd.price, cmd.orderId), cmd.resultCode, cmd.timestamp, seq);
                break;

            case BINARY_DATA_COMMAND:
                if (cmd.resultCode != CommandResultCode.ACCEPTED) {
                    sendApiCommandResult(new ApiBinaryDataCommand(cmd.userCookie, null), cmd.resultCode, cmd.timestamp, seq);
                }
                break;

            case ORDER_BOOK_REQUEST:
                sendApiCommandResult(new ApiOrderBookRequest(cmd.symbol, (int) cmd.size), cmd.resultCode, cmd.timestamp, seq);
                break;

            // TODO: 添加其他命令处理
        }
    }

    // 发送 API 命令结果
    private void sendApiCommandResult(ApiCommand cmd, CommandResultCode resultCode, long timestamp, long seq) {
        cmd.timestamp = timestamp;
        final IEventsHandler.ApiCommandResult commandResult = new IEventsHandler.ApiCommandResult(cmd, resultCode, seq);
        eventsHandler.commandResult(commandResult); // 发送命令结果事件
    }
}
