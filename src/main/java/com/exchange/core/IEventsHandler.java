package com.exchange.core;

import com.exchange.core.common.api.ApiCommand;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.OrderAction;
import lombok.Data;

import java.util.List;

/**
 * 用于非延迟敏感应用的方便事件处理器接口。<br>
 * 自定义的处理器实现应附加到 SimpleEventProcessor。<br>
 * 处理器方法按以下顺序从单线程调用：
 * <table summary="执行顺序">
 * <tr><td>1. </td><td> commandResult</td></tr>
 * <tr><td>2A. </td><td> 可选的 reduceEvent <td> 可选的 tradeEvent</td></tr>
 * <tr><td>2B. </td><td> <td>可选的 rejectEvent</td></tr>
 * <tr><td>3. </td><td> orderBook - 对 ApiOrderBookRequest 是强制的，其他命令为可选</td></tr>
 * </table>
 * 如果任何处理器抛出异常，事件处理将立即停止——如果必要，可以将逻辑包装在 try-catch 块中。
 */
public interface IEventsHandler {

    /**
     * 每个命令执行后会调用此方法。
     *
     * @param commandResult - 不可变对象，描述原始命令、结果代码和分配的序列号。
     */
    void commandResult(ApiCommandResult commandResult);

    /**
     * 如果订单执行导致一个或多个交易，会调用此方法。
     *
     * @param tradeEvent - 不可变对象，描述事件的详细信息
     */
    void tradeEvent(TradeEvent tradeEvent);

    /**
     * 如果无法按提供的价格限制匹配订单，调用此方法。
     *
     * @param rejectEvent - 不可变对象，描述事件的详细信息
     */
    void rejectEvent(RejectEvent rejectEvent);

    /**
     * 如果取消或减少命令成功执行，会调用此方法。
     *
     * @param reduceEvent - 不可变对象，描述事件的详细信息
     */
    void reduceEvent(ReduceEvent reduceEvent);

    /**
     * 当订单簿快照（L2MarketData）被附加到命令时，匹配引擎会调用此方法。
     * 对于 ApiOrderBookRequest 总是会发生，对于其他命令有时会发生。
     *
     * @param orderBook - 不可变对象，包含 L2 订单簿快照
     */
    void orderBook(OrderBook orderBook);

    @Data
    class ApiCommandResult {
        public final ApiCommand command; // 原始命令
        public final CommandResultCode resultCode; // 命令执行结果代码
        public final long seq; // 分配的序列号
    }

    @Data
    class TradeEvent {
        public final int symbol; // 交易的标的
        public final long totalVolume; // 总交易量
        public final long takerOrderId; // 买单订单 ID
        public final long takerUid; // 买单用户 ID
        public final OrderAction takerAction; // 买单操作类型
        public final boolean takeOrderCompleted; // 买单是否完成
        public final long timestamp; // 交易时间戳
        public final List<Trade> trades; // 交易列表
    }

    @Data
    class Trade {
        public final long makerOrderId; // 卖单订单 ID
        public final long makerUid; // 卖单用户 ID
        public final boolean makerOrderCompleted; // 卖单是否完成
        public final long price; // 交易价格
        public final long volume; // 交易量
    }

    @Data
    class ReduceEvent {
        public final int symbol; // 标的
        public final long reducedVolume; // 减少的交易量
        public final boolean orderCompleted; // 订单是否完成
        public final long price; // 价格
        public final long orderId; // 订单 ID
        public final long uid; // 用户 ID
        public final long timestamp; // 时间戳
    }

    @Data
    class RejectEvent {
        public final int symbol; // 标的
        public final long rejectedVolume; // 拒绝的交易量
        public final long price; // 拒绝时的价格
        public final long orderId; // 订单 ID
        public final long uid; // 用户 ID
        public final long timestamp; // 时间戳
    }

    @Data
    class CommandExecutionResult {
        public final int symbol; // 标的
        public final long volume; // 执行的交易量
        public final long price; // 执行的价格
        public final long orderId; // 订单 ID
        public final long uid; // 用户 ID
        public final long timestamp; // 时间戳
    }

    @Data
    class OrderBook {
        public final int symbol; // 标的
        public final List<OrderBookRecord> asks; // 卖单列表
        public final List<OrderBookRecord> bids; // 买单列表
        public final long timestamp; // 时间戳
    }

    @Data
    class OrderBookRecord {
        public final long price; // 价格
        public final long volume; // 数量
        public final int orders; // 订单数
    }
}
