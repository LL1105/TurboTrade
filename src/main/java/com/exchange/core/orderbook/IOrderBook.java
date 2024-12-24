package com.exchange.core.orderbook;

import com.exchange.core.common.*;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.config.LoggingConfiguration;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.OrderAction;
import com.exchange.core.common.constant.OrderCommandType;
import com.exchange.core.utils.HashingUtils;
import exchange.core2.collections.objpool.ObjectsPool;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public interface IOrderBook extends WriteBytesMarshallable, StateHash {

    /**
     * 处理新订单。
     * 根据指定的价格（即订单是否可以立即匹配），
     * 订单将与订单簿中现有的相反方向的GTC订单进行匹配。
     * 如果有剩余的订单量（订单未完全匹配）：
     * IOC - 拒绝作为部分成交的订单。
     * GTC - 将其作为新限价订单放入订单簿中。
     * <p>
     * 如果出错，将附带拒绝链（简化风险处理）。
     *
     * @param cmd - 要匹配或放置的订单命令
     */
    void newOrder(OrderCommand cmd);

    /**
     * 完全取消订单。
     * <p>
     * 填充 cmd.action 以保存原始订单的操作类型。
     *
     * @param cmd - 订单命令
     * @return 如果订单未找到，返回 MATCHING_UNKNOWN_ORDER_ID，否则返回 SUCCESS。
     */
    CommandResultCode cancelOrder(OrderCommand cmd);

    /**
     * 减少订单的数量（按指定的数量减少）。
     * <p>
     * 填充 cmd.action 以保存原始订单的操作类型。
     *
     * @param cmd - 订单命令
     * @return 如果订单未找到，返回 MATCHING_UNKNOWN_ORDER_ID，否则返回 SUCCESS。
     */
    CommandResultCode reduceOrder(OrderCommand cmd);

    /**
     * 移动订单。
     * <p>
     * newPrice - 新价格（如果价格为0或与原价格相同，订单将不会移动）。
     * 填充 cmd.action 以保存原始订单的操作类型。
     *
     * @param cmd - 订单命令
     * @return 如果订单未找到，返回 MATCHING_UNKNOWN_ORDER_ID，否则返回 SUCCESS。
     */
    CommandResultCode moveOrder(OrderCommand cmd);

    // 仅供测试？
    int getOrdersNum(OrderAction action);

    // 仅供测试？
    long getTotalOrdersVolume(OrderAction action);

    // 仅供测试？
    IOrder getOrderById(long orderId);

    // 仅供测试 - 在不改变状态的情况下验证内部状态
    void validateInternalState();

    /**
     * 返回实际实现类型
     *
     * @return 当前的实现类型
     */
    OrderBookImplType getImplementationType();

    /**
     * 查找指定用户的所有订单。
     * <p>
     * 由于订单簿没有维护 UID 到订单的索引，因此这个操作比较慢。
     * <p>
     * 生产垃圾。
     * <p>
     * 在进行任何可变操作之前，必须处理这些订单。
     *
     * @param uid 用户ID
     * @return 该用户的订单列表
     */
    List<Order> findUserOrders(long uid);

    CoreSymbolSpecification getSymbolSpec();

    /**
     * 获取买单（ask）订单流。
     *
     * @param sorted 是否排序
     * @return 排序后的买单流
     */
    Stream<? extends IOrder> askOrdersStream(boolean sorted);

    /**
     * 获取卖单（bid）订单流。
     *
     * @param sorted 是否排序
     * @return 排序后的卖单流
     */
    Stream<? extends IOrder> bidOrdersStream(boolean sorted);

    /**
     * 订单簿的状态哈希是实现无关的。
     * 请参见 {@link IOrderBook#validateInternalState} 获取完整的反序列化对象的内部状态验证。
     *
     * @return 状态哈希值
     */
    @Override
    default int stateHash() {
        return Objects.hash(
                HashingUtils.stateHashStream(askOrdersStream(true)),
                HashingUtils.stateHashStream(bidOrdersStream(true)),
                getSymbolSpec().stateHash());
    }

    /**
     * 获取当前的 L2 市场数据快照
     *
     * @param size 每部分的最大大小（买单、卖单）
     * @return L2 市场数据快照
     */
    default L2MarketData getL2MarketDataSnapshot(final int size) {
        final int asksSize = getTotalAskBuckets(size);
        final int bidsSize = getTotalBidBuckets(size);
        final L2MarketData data = new L2MarketData(asksSize, bidsSize);
        fillAsks(asksSize, data);
        fillBids(bidsSize, data);
        return data;
    }

    default L2MarketData getL2MarketDataSnapshot() {
        return getL2MarketDataSnapshot(Integer.MAX_VALUE);
    }

    /**
     * 请求将 L2 市场数据发布到传出 disruptor 消息中
     *
     * @param data 预分配的环形缓冲区对象
     */
    default void publishL2MarketDataSnapshot(L2MarketData data) {
        int size = L2MarketData.L2_SIZE;
        fillAsks(size, data);
        fillBids(size, data);
    }

    void fillAsks(int size, L2MarketData data);

    void fillBids(int size, L2MarketData data);

    int getTotalAskBuckets(int limit);

    int getTotalBidBuckets(int limit);


    /**
     * 处理订单命令。
     * 根据命令类型，调用相应的订单处理方法。
     *
     * @param orderBook 当前的订单簿
     * @param cmd       订单命令
     * @return 处理结果码
     */
    static CommandResultCode processCommand(final IOrderBook orderBook, final OrderCommand cmd) {

        final OrderCommandType commandType = cmd.command;

        if (commandType == OrderCommandType.MOVE_ORDER) {
            return orderBook.moveOrder(cmd);

        } else if (commandType == OrderCommandType.CANCEL_ORDER) {
            return orderBook.cancelOrder(cmd);

        } else if (commandType == OrderCommandType.REDUCE_ORDER) {
            return orderBook.reduceOrder(cmd);

        } else if (commandType == OrderCommandType.PLACE_ORDER) {
            if (cmd.resultCode == CommandResultCode.VALID_FOR_MATCHING_ENGINE) {
                orderBook.newOrder(cmd);
                return CommandResultCode.SUCCESS;
            } else {
                return cmd.resultCode; // 不做任何更改
            }

        } else if (commandType == OrderCommandType.ORDER_BOOK_REQUEST) {
            int size = (int) cmd.size;
            cmd.marketData = orderBook.getL2MarketDataSnapshot(size >= 0 ? size : Integer.MAX_VALUE);
            return CommandResultCode.SUCCESS;

        } else {
            return CommandResultCode.MATCHING_UNSUPPORTED_COMMAND;
        }
    }

    /**
     * 根据字节流和对象池创建订单簿实现。
     *
     * @param bytes           输入字节流
     * @param objectsPool     对象池
     * @param eventsHelper    事件助手
     * @param loggingCfg      日志配置
     * @return 订单簿实现
     */
    static IOrderBook create(BytesIn bytes, ObjectsPool objectsPool, OrderBookEventsHelper eventsHelper, LoggingConfiguration loggingCfg) {
        switch (OrderBookImplType.of(bytes.readByte())) {
            case NAIVE:
                return new OrderBookNaiveImpl(bytes, loggingCfg);
            case DIRECT:
                return new OrderBookDirectImpl(bytes, objectsPool, eventsHelper, loggingCfg);
            default:
                throw new IllegalArgumentException();
        }
    }

    @FunctionalInterface
    interface OrderBookFactory {

        IOrderBook create(CoreSymbolSpecification spec, ObjectsPool pool, OrderBookEventsHelper eventsHelper, LoggingConfiguration loggingCfg);
    }

    @Getter
    enum OrderBookImplType {
        NAIVE(0), // 简单实现
        DIRECT(2); // 高效实现

        private byte code;

        OrderBookImplType(int code) {
            this.code = (byte) code;
        }

        public static OrderBookImplType of(byte code) {
            switch (code) {
                case 0:
                    return NAIVE;
                case 2:
                    return DIRECT;
                default:
                    throw new IllegalArgumentException("未知的 OrderBookImplType:" + code);
            }
        }
    }
}
