package com.exchange.core.common.api.reports.queryImpl;

import com.exchange.core.common.Order;
import com.exchange.core.common.UserProfile;
import com.exchange.core.common.api.reports.ReportQuery;
import com.exchange.core.common.api.reports.resultImpl.SingleUserReportResult;
import com.exchange.core.common.constant.ReportType;
import com.exchange.core.processors.MatchingEngineRouter;
import com.exchange.core.processors.RiskEngine;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@EqualsAndHashCode
@ToString
@Slf4j
public final class SingleUserReportQuery implements ReportQuery<SingleUserReportResult> {

    private final long uid;  // 用户ID

    // 构造函数，传入用户ID
    public SingleUserReportQuery(long uid) {
        this.uid = uid;
    }

    // 构造函数，从BytesIn流中读取数据（通常用于反序列化）
    public SingleUserReportQuery(final BytesIn bytesIn) {
        this.uid = bytesIn.readLong();
    }

    // 获取用户ID
    public long getUid() {
        return uid;
    }

    // 获取报告类型的代码，这里返回的是单一用户报告的类型代码
    @Override
    public int getReportTypeCode() {
        return ReportType.SINGLE_USER_REPORT.getCode();
    }

    // 从流中创建报告结果
    @Override
    public SingleUserReportResult createResult(final Stream<BytesIn> sections) {
        return SingleUserReportResult.merge(sections);  // 合并所有流部分生成结果
    }

    // 通过匹配引擎处理报告，返回包含用户所有订单的报告结果
    @Override
    public Optional<SingleUserReportResult> process(final MatchingEngineRouter matchingEngine) {
        final IntObjectHashMap<List<Order>> orders = new IntObjectHashMap<>();  // 用于存储每个交易对的订单列表

        // 遍历匹配引擎中的所有订单簿
        matchingEngine.getOrderBooks().forEach(ob -> {
            final List<Order> userOrders = ob.findUserOrders(this.uid);  // 查找用户的订单
            // 如果用户有订单，则将订单按交易对symbolId进行分类
            if (!userOrders.isEmpty()) {
                orders.put(ob.getSymbolSpec().symbolId, userOrders);
            }
        });

        // 返回一个Optional对象，包装生成的报告结果
        return Optional.of(SingleUserReportResult.createFromMatchingEngine(uid, orders));
    }

    // 通过风险引擎处理报告，返回包含用户账户和仓位的报告结果
    @Override
    public Optional<SingleUserReportResult> process(final RiskEngine riskEngine) {

        // 如果该风险引擎不处理该用户的ID，则直接返回空
        if (!riskEngine.uidForThisHandler(this.uid)) {
            return Optional.empty();
        }

        // 获取用户的个人资料
        final UserProfile userProfile = riskEngine.getUserProfileService().getUserProfile(this.uid);

        if (userProfile != null) {
            // 如果找到了用户的个人资料，则将其仓位信息封装为报告结果
            final IntObjectHashMap<SingleUserReportResult.Position> positions = new IntObjectHashMap<>(userProfile.positions.size());
            userProfile.positions.forEachKeyValue((symbol, pos) ->
                    positions.put(symbol, new SingleUserReportResult.Position(
                            pos.currency,       // 币种
                            pos.direction,      // 仓位方向（多头或空头）
                            pos.openVolume,     // 开仓量
                            pos.openPriceSum,   // 开仓价格总和
                            pos.profit,         // 盈利
                            pos.pendingSellSize, // 未成交卖单数量
                            pos.pendingBuySize))); // 未成交买单数量

            // 返回基于风险引擎数据的报告结果
            return Optional.of(SingleUserReportResult.createFromRiskEngineFound(
                    uid,
                    userProfile.userStatus,  // 用户状态
                    userProfile.accounts,    // 用户账户
                    positions));
        } else {
            // 如果没有找到用户资料，则返回“未找到”的报告结果
            return Optional.of(SingleUserReportResult.createFromRiskEngineNotFound(uid));
        }
    }

    // 将查询数据写入流中，通常用于序列化
    @Override
    public void writeMarshallable(final BytesOut bytes) {
        bytes.writeLong(uid);  // 将用户ID写入流
    }
}
