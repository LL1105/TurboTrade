package com.exchange.core.common;

import com.exchange.core.common.constant.OrderAction;
import com.exchange.core.common.constant.PositionDirection;
import com.exchange.core.processors.RiskEngine;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Objects;

@Slf4j
@NoArgsConstructor
public final class SymbolPositionRecord implements WriteBytesMarshallable, StateHash {

    // 用户ID
    public long uid;

    // 交易对符号
    public int symbol;
    
    // 货币类型
    public int currency;

    // 当前持仓状态（仅适用于保证金交易）
    public PositionDirection direction = PositionDirection.EMPTY; // 当前持仓方向（空仓、做多、做空）
    public long openVolume = 0;  // 开仓数量
    public long openPriceSum = 0;  // 开仓价格总和
    public long profit = 0;  // 当前持仓的盈亏

    // 待执行订单总量
    public long pendingSellSize = 0;  // 待卖单数量
    public long pendingBuySize = 0;   // 待买单数量

    // 初始化方法
    public void initialize(long uid, int symbol, int currency) {
        this.uid = uid;
        this.symbol = symbol;
        this.currency = currency;
        this.direction = PositionDirection.EMPTY;
        this.openVolume = 0;
        this.openPriceSum = 0;
        this.profit = 0;
        this.pendingSellSize = 0;
        this.pendingBuySize = 0;
    }

    // 从字节流读取数据的构造方法
    public SymbolPositionRecord(long uid, BytesIn bytes) {
        this.uid = uid;
        this.symbol = bytes.readInt();
        this.currency = bytes.readInt();
        this.direction = PositionDirection.of(bytes.readByte());
        this.openVolume = bytes.readLong();
        this.openPriceSum = bytes.readLong();
        this.profit = bytes.readLong();
        this.pendingSellSize = bytes.readLong();
        this.pendingBuySize = bytes.readLong();
    }

    /**
     * 检查持仓是否为空（无待执行订单，无开盘交易）- 可以从 HashMap 中删除
     *
     * @return 如果持仓为空（无待执行订单，无开盘交易），返回 true
     */
    public boolean isEmpty() {
        return direction == PositionDirection.EMPTY
                && pendingSellSize == 0
                && pendingBuySize == 0;
    }

    // 记录待持仓的订单数量
    public void pendingHold(OrderAction orderAction, long size) {
        if (orderAction == OrderAction.ASK) {
            pendingSellSize += size;
        } else {
            pendingBuySize += size;
        }
    }

    // 释放待持仓的订单数量
    public void pendingRelease(OrderAction orderAction, long size) {
        if (orderAction == OrderAction.ASK) {
            pendingSellSize -= size;
        } else {
            pendingBuySize -= size;
        }
    }

    /**
     * 估算当前持仓的利润
     *
     * @param spec 核心符号的规格
     * @param lastPriceCacheRecord 最新的价格记录
     * @return 估算的利润
     */
    public long estimateProfit(final CoreSymbolSpecification spec, final RiskEngine.LastPriceCacheRecord lastPriceCacheRecord) {
        switch (direction) {
            case EMPTY:
                return profit;
            case LONG:
                return profit + ((lastPriceCacheRecord != null && lastPriceCacheRecord.bidPrice != 0)
                        ? (openVolume * lastPriceCacheRecord.bidPrice - openPriceSum)
                        : spec.marginBuy * openVolume);
            case SHORT:
                return profit + ((lastPriceCacheRecord != null && lastPriceCacheRecord.askPrice != Long.MAX_VALUE)
                        ? (openPriceSum - openVolume * lastPriceCacheRecord.askPrice)
                        : spec.marginSell * openVolume);
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * 计算当前持仓或订单所需的保证金
     *
     * @param spec 符号的核心规格
     * @return 所需的保证金
     */
    public long calculateRequiredMarginForFutures(CoreSymbolSpecification spec) {
        final long specMarginBuy = spec.marginBuy;
        final long specMarginSell = spec.marginSell;

        final long signedPosition = openVolume * direction.getMultiplier();
        final long currentRiskBuySize = pendingBuySize + signedPosition;
        final long currentRiskSellSize = pendingSellSize - signedPosition;

        final long marginBuy = specMarginBuy * currentRiskBuySize;
        final long marginSell = specMarginSell * currentRiskSellSize;
        return Math.max(marginBuy, marginSell);
    }

    /**
     * 计算根据订单的执行大小而产生的保证金变化
     *
     * @param spec   符号的规格
     * @param action 订单动作（买或卖）
     * @param size   订单大小
     * @return 如果订单会减少当前风险敞口，返回 -1，否则返回新保证金
     */
    public long calculateRequiredMarginForOrder(final CoreSymbolSpecification spec, final OrderAction action, final long size) {
        final long specMarginBuy = spec.marginBuy;
        final long specMarginSell = spec.marginSell;

        final long signedPosition = openVolume * direction.getMultiplier();
        final long currentRiskBuySize = pendingBuySize + signedPosition;
        final long currentRiskSellSize = pendingSellSize - signedPosition;

        long marginBuy = specMarginBuy * currentRiskBuySize;
        long marginSell = specMarginSell * currentRiskSellSize;
        final long currentMargin = Math.max(marginBuy, marginSell);

        if (action == OrderAction.BID) {
            marginBuy += spec.marginBuy * size;
        } else {
            marginSell += spec.marginSell * size;
        }

        final long newMargin = Math.max(marginBuy, marginSell);
        return (newMargin <= currentMargin) ? -1 : newMargin;
    }

    /**
     * 更新保证金交易中的持仓
     * 1. 释放待持仓订单
     * 2. 相应地减少对冲持仓（如果存在）
     * 3. 增加新的持仓（如果有剩余交易量）
     *
     * @param action 订单动作（买或卖）
     * @param size   订单大小
     * @param price  订单价格
     * @return 打开的订单大小
     */
    public long updatePositionForMarginTrade(OrderAction action, long size, long price) {

        // 1. 释放待持仓订单
        pendingRelease(action, size);

        // 2. 减少对冲持仓
        final long sizeToOpen = closeCurrentPositionFutures(action, size, price);

        // 3. 增加新的持仓
        if (sizeToOpen > 0) {
            openPositionMargin(action, sizeToOpen, price);
        }
        return sizeToOpen;
    }

    private long closeCurrentPositionFutures(final OrderAction action, final long tradeSize, final long tradePrice) {

        if (direction == PositionDirection.EMPTY || direction == PositionDirection.of(action)) {
            return tradeSize;
        }

        if (openVolume > tradeSize) {
            openVolume -= tradeSize;
            openPriceSum -= tradeSize * tradePrice;
            return 0;
        }

        profit += (openVolume * tradePrice - openPriceSum) * direction.getMultiplier();
        openPriceSum = 0;
        direction = PositionDirection.EMPTY;
        final long sizeToOpen = tradeSize - openVolume;
        openVolume = 0;

        return sizeToOpen;
    }

    private void openPositionMargin(OrderAction action, long sizeToOpen, long tradePrice) {
        openVolume += sizeToOpen;
        openPriceSum += tradePrice * sizeToOpen;
        direction = PositionDirection.of(action);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(symbol);
        bytes.writeInt(currency);
        bytes.writeByte((byte) direction.getMultiplier());
        bytes.writeLong(openVolume);
        bytes.writeLong(openPriceSum);
        bytes.writeLong(profit);
        bytes.writeLong(pendingSellSize);
        bytes.writeLong(pendingBuySize);
    }

    // 重置所有持仓信息
    public void reset() {
        pendingBuySize = 0;
        pendingSellSize = 0;
        openVolume = 0;
        openPriceSum = 0;
        direction = PositionDirection.EMPTY;
    }

    // 验证持仓的内部状态是否合法
    public void validateInternalState() {
        if (direction == PositionDirection.EMPTY && (openVolume != 0 || openPriceSum != 0)) {
            log.error("uid {} : position:{} totalSize:{} openPriceSum:{}", uid, direction, openVolume, openPriceSum);
            throw new IllegalStateException();
        }
        if (direction != PositionDirection.EMPTY && (openVolume <= 0 || openPriceSum <= 0)) {
            log.error("uid {} : position:{} totalSize:{} openPriceSum:{}", uid, direction, openVolume, openPriceSum);
            throw new IllegalStateException();
        }

        if (pendingSellSize < 0 || pendingBuySize < 0) {
            log.error("uid {} : pendingSellSize:{} pendingBuySize:{}", uid, pendingSellSize, pendingBuySize);
            throw new IllegalStateException();
        }
    }

    @Override
    public int stateHash() {
        return Objects.hash(symbol, currency, direction.getMultiplier(), openVolume, openPriceSum, profit, pendingSellSize, pendingBuySize);
    }

    @Override
    public String toString() {
        return "SPR{" +
                "u" + uid +
                " sym" + symbol +
                " cur" + currency +
                " pos" + direction +
                " Σv=" + openVolume +
                " Σp=" + openPriceSum +
                " pnl=" + profit +
                " pendingS=" + pendingSellSize +
                " pendingB=" + pendingBuySize +
                '}';
    }
}
