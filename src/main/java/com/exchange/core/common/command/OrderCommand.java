package com.exchange.core.common.command;

import com.exchange.core.common.IOrder;
import com.exchange.core.common.L2MarketData;
import com.exchange.core.common.MatcherTradeEvent;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.OrderAction;
import com.exchange.core.common.constant.OrderCommandType;
import com.exchange.core.common.constant.OrderType;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderCommand implements IOrder {

    public OrderCommandType command;

    @Getter
    public long orderId;

    public int symbol;

    @Getter
    public long price;

    @Getter
    public long size;

    // 为 GTC 类型的买入订单在 快速变动场景 提供一个保留价格
    @Getter
    public long reserveBidPrice;

    //  确定订单原本的行为（买入或卖出）
    @Getter
    public OrderAction action;

    public OrderType orderType;

    @Getter
    public long uid;

    @Getter
    public long timestamp;

    public int userCookie;

    // 分组处理标识
    public long eventsGroup;
    public int serviceFlags;

    // 存储命令执行的结果代码，通常是表示成功或失败的标识。可以在执行过程中返回中间状态信息。
    public CommandResultCode resultCode;

    // 用于记录交易匹配的事件链，能够帮助追踪和分析每笔交易的过程。
    public MatcherTradeEvent matcherEvent;

    // 存储市场数据（如 Level 2 市场数据），为交易执行和市场分析提供支持。
    public L2MarketData marketData;

    /**
     * 创建一个新的订单命令对象。
     *
     * @param orderType       订单类型（LIMIT 或 MARKET）。
     * @param orderId         订单的唯一标识符。
     * @param uid             用户 ID，表示下单用户。
     * @param price           订单价格，仅对限价订单有效。
     * @param reserveBidPrice 保留的竞价价格，用于 GTC 买入订单的快速调整。
     * @param size            订单的数量或规模。
     * @param action          订单操作类型（ASK 或 BID）。
     * @return 初始化好的订单命令对象（OrderCommand）。
     */
    public static OrderCommand newOrder(OrderType orderType, long orderId, long uid, long price, long reserveBidPrice, long size, OrderAction action) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.PLACE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.price = price;
        cmd.reserveBidPrice = reserveBidPrice;
        cmd.size = size;
        cmd.action = action;
        cmd.orderType = orderType;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    /**
     * 创建一个取消订单的命令对象。
     *
     * @param orderId 订单的唯一标识符，指定要取消的订单。
     * @param uid     用户 ID，标识发出取消请求的用户。
     * @return        表示取消订单的命令对象。
     */
    public static OrderCommand cancel(long orderId, long uid) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    /**
     * 创建一个减少订单数量的命令对象。
     *
     * @param orderId    订单的唯一标识符，指定要减少数量的订单。
     * @param uid        用户 ID，标识发出减少请求的用户。
     * @param reduceSize 减少的数量。
     * @return           表示减少订单数量的命令对象。
     */
    public static OrderCommand reduce(long orderId, long uid, long reduceSize) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.REDUCE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.size = reduceSize;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    /**
     * 创建一个更新订单价格的命令对象。
     *
     * @param orderId 订单的唯一标识符，指定要更新的订单。
     * @param uid     用户 ID，标识发出更新请求的用户。
     * @param price   更新后的订单价格。
     * @return        表示更新订单价格的命令对象。
     */
    public static OrderCommand update(long orderId, long uid, long price) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.MOVE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.price = price;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    /**
     * 处理完整的 MatcherTradeEvent 链，链中的事件不会被移除或撤销。
     *
     * <p>此方法遍历命令对象中关联的 `MatcherTradeEvent` 链表，
     * 并对每个事件调用提供的处理器函数 `handler`。</p>
     *
     * @param handler 用于处理每个 `MatcherTradeEvent` 的消费函数（lambda 或方法引用）。
     */
    public void processMatcherEvents(Consumer<MatcherTradeEvent> handler) {
        MatcherTradeEvent mte = this.matcherEvent;
        while (mte != null) {
            handler.accept(mte);
            mte = mte.nextEvent;
        }
    }

    /**
     * 提取 MatcherTradeEvent 链中的所有事件并返回为一个列表。
     * <p>
     * <b>注意：</b>此方法会生成大量垃圾（临时对象），
     * 仅供测试使用，不适合生产环境。
     * </p>
     *
     * @return 包含所有 `MatcherTradeEvent` 的列表。
     */
    public List<MatcherTradeEvent> extractEvents() {
        List<MatcherTradeEvent> list = new ArrayList<>();
        processMatcherEvents(list::add);
        return list;
    }

    /**
     * 仅写入命令数据，不包含状态或事件。
     * <p>
     * 此方法将当前对象的命令数据复制到传入的 `cmd2` 对象中。
     * 请注意，`cmd2` 只是命令数据的容器，因此它不会接收事件数据或命令状态（例如 `resultCode` 或 `matcherEvent`）。
     * </p>
     *
     * @param cmd2 要覆盖的目标命令对象
     */
    public void writeTo(OrderCommand cmd2) {
        cmd2.command = this.command;
        cmd2.orderId = this.orderId;
        cmd2.symbol = this.symbol;
        cmd2.uid = this.uid;
        cmd2.timestamp = this.timestamp;

        cmd2.reserveBidPrice = this.reserveBidPrice;
        cmd2.price = this.price;
        cmd2.size = this.size;
        cmd2.action = this.action;
        cmd2.orderType = this.orderType;
    }

    /**
     * 慢速复制方法，仅用于测试。
     * <p>
     * 此方法将当前命令对象的所有数据（包括命令数据、事件链和市场数据）复制到一个新的 `OrderCommand` 对象中。
     * 注意：由于复制过程包括事件链和市场数据的深度复制，性能较慢，通常仅用于测试或调试。
     * </p>
     *
     * @return 复制后的新命令对象
     */
    public OrderCommand copy() {

        OrderCommand newCmd = new OrderCommand();
        writeTo(newCmd);
        newCmd.resultCode = this.resultCode;

        List<MatcherTradeEvent> events = extractEvents();

        for (MatcherTradeEvent event : events) {
            MatcherTradeEvent copy = event.copy();
            copy.nextEvent = newCmd.matcherEvent;
            newCmd.matcherEvent = copy;
        }

        if (marketData != null) {
            newCmd.marketData = marketData.copy();
        }

        return newCmd;
    }

    @Override
    public long getFilled() {
        return 0;
    }

    @Override
    public int stateHash() {
        throw new UnsupportedOperationException("Command does not represents state");
    }
}
