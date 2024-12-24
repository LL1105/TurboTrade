package com.exchange.core.common.api.reports.resultImpl;

import com.exchange.core.common.Order;
import com.exchange.core.common.api.reports.ReportResult;
import com.exchange.core.common.constant.PositionDirection;
import com.exchange.core.common.constant.UserStatus;
import com.exchange.core.utils.SerializationUtils;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 单一用户报告结果
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
@Getter
@Slf4j
public final class SingleUserReportResult implements ReportResult {

    // 该常量表示一个空的或未初始化的默认报告
    public static SingleUserReportResult IDENTITY = new SingleUserReportResult(0L, null, null, null, null, QueryExecutionStatus.OK);

    // 用户 ID
    private final long uid;

    // 风险引擎中的用户状态
    private final UserStatus userStatus;
    
    // 风险引擎中的账户信息，key 是货币的 symbolId，value 是账户的余额
    private final IntLongHashMap accounts;

    // 风险引擎中的持仓信息，key 是 symbolId，value 是对应的 Position 对象
    private final IntObjectHashMap<Position> positions;

    // 匹配引擎中的用户订单，key 是 symbolId，value 是该符号下的所有订单列表
    private final IntObjectHashMap<List<Order>> orders;

    // 查询执行状态
    private final QueryExecutionStatus queryExecutionStatus;

    /**
     * 根据匹配引擎的数据生成报告结果
     */
    public static SingleUserReportResult createFromMatchingEngine(long uid, IntObjectHashMap<List<Order>> orders) {
        return new SingleUserReportResult(uid, null, null, null, orders, QueryExecutionStatus.OK);
    }

    /**
     * 根据风险引擎中找到的用户数据生成报告结果
     */
    public static SingleUserReportResult createFromRiskEngineFound(long uid, UserStatus userStatus, IntLongHashMap accounts, IntObjectHashMap<Position> positions) {
        return new SingleUserReportResult(uid, userStatus, accounts, positions, null, QueryExecutionStatus.OK);
    }

    /**
     * 根据风险引擎中未找到用户的情况生成报告结果
     */
    public static SingleUserReportResult createFromRiskEngineNotFound(long uid) {
        return new SingleUserReportResult(uid, null, null, null, null, QueryExecutionStatus.USER_NOT_FOUND);
    }

    /**
     * 提取用户订单并按订单 ID 索引
     */
    public Map<Long, Order> fetchIndexedOrders() {
        return orders.stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Order::getOrderId, ord -> ord));
    }

    /**
     * 从字节输入流构造 SingleUserReportResult 对象
     */
    private SingleUserReportResult(final BytesIn bytesIn) {
        this.uid = bytesIn.readLong();
        this.userStatus = bytesIn.readBoolean() ? UserStatus.of(bytesIn.readByte()) : null;
        this.accounts = bytesIn.readBoolean() ? SerializationUtils.readIntLongHashMap(bytesIn) : null;
        this.positions = bytesIn.readBoolean() ? SerializationUtils.readIntHashMap(bytesIn, Position::new) : null;
        this.orders = bytesIn.readBoolean() ? SerializationUtils.readIntHashMap(bytesIn, b -> SerializationUtils.readList(b, Order::new)) : null;
        this.queryExecutionStatus = QueryExecutionStatus.of(bytesIn.readInt());
    }

    /**
     * 将该报告对象序列化为字节输出流
     */
    @Override
    public void writeMarshallable(final BytesOut bytes) {
        bytes.writeLong(uid);
        bytes.writeBoolean(userStatus != null);
        if (userStatus != null) {
            bytes.writeByte(userStatus.getCode());
        }
        bytes.writeBoolean(accounts != null);
        if (accounts != null) {
            SerializationUtils.marshallIntLongHashMap(accounts, bytes);
        }
        bytes.writeBoolean(positions != null);
        if (positions != null) {
            SerializationUtils.marshallIntHashMap(positions, bytes);
        }
        bytes.writeBoolean(orders != null);
        if (orders != null) {
            SerializationUtils.marshallIntHashMap(orders, bytes, symbolOrders -> SerializationUtils.marshallList(symbolOrders, bytes));
        }
        bytes.writeInt(queryExecutionStatus.code);
    }

    /**
     * 查询执行状态
     */
    public enum QueryExecutionStatus {
        OK(0),            // 执行成功
        USER_NOT_FOUND(1); // 用户未找到

        private final int code;

        QueryExecutionStatus(int code) {
            this.code = code;
        }

        public static QueryExecutionStatus of(int code) {
            switch (code) {
                case 0:
                    return OK;
                case 1:
                    return USER_NOT_FOUND;
                default:
                    throw new IllegalArgumentException("unknown ExecutionStatus:" + code);
            }
        }
    }

    /**
     * 合并多个报告片段为一个完整的报告
     */
    public static SingleUserReportResult merge(final Stream<BytesIn> pieces) {
        return pieces
                .map(SingleUserReportResult::new)
                .reduce(
                        IDENTITY,
                        (a, b) -> new SingleUserReportResult(
                                a.uid,
                                SerializationUtils.preferNotNull(a.userStatus, b.userStatus),
                                SerializationUtils.preferNotNull(a.accounts, b.accounts),
                                SerializationUtils.preferNotNull(a.positions, b.positions),
                                SerializationUtils.mergeOverride(a.orders, b.orders),
                                a.queryExecutionStatus != QueryExecutionStatus.OK ? a.queryExecutionStatus : b.queryExecutionStatus));
    }

    /**
     * 持仓信息类
     */
    @RequiredArgsConstructor
    @Getter
    public static class Position implements WriteBytesMarshallable {

        public final int quoteCurrency; // 报价货币
        public final PositionDirection direction; // 持仓方向
        public final long openVolume; // 开仓量
        public final long openPriceSum; // 开盘价格总和
        public final long profit; // 盈利

        public final long pendingSellSize; // 待处理的卖单总量
        public final long pendingBuySize; // 待处理的买单总量

        private Position(BytesIn bytes) {
            this.quoteCurrency = bytes.readInt();
            this.direction = PositionDirection.of(bytes.readByte());
            this.openVolume = bytes.readLong();
            this.openPriceSum = bytes.readLong();
            this.profit = bytes.readLong();
            this.pendingSellSize = bytes.readLong();
            this.pendingBuySize = bytes.readLong();
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeInt(quoteCurrency);
            bytes.writeByte((byte) direction.getMultiplier());
            bytes.writeLong(openVolume);
            bytes.writeLong(openPriceSum);
            bytes.writeLong(profit);
            bytes.writeLong(pendingSellSize);
            bytes.writeLong(pendingBuySize);
        }
    }

    /**
     * 输出报告结果的字符串表示
     */
    @Override
    public String toString() {
        return "SingleUserReportResult{" +
                "userProfile=" + userStatus +
                ", accounts=" + accounts +
                ", orders=" + orders +
                ", queryExecutionStatus=" + queryExecutionStatus +
                '}';
    }
}
