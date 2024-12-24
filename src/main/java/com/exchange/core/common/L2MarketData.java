package com.exchange.core.common;

import lombok.ToString;

import java.util.Arrays;

/**
 * L2 市场数据承载对象
 * <p>
 * 注意：该对象可以包含脏数据，askSize 和 bidSize 很重要！
 */
@ToString
public final class L2MarketData {

    // L2市场数据的大小
    public static final int L2_SIZE = 32;

    // 当前的卖方订单量（ask）和买方订单量（bid）
    public int askSize;
    public int bidSize;

    // 卖方价格、卖方数量、卖方订单编号
    public long[] askPrices;
    public long[] askVolumes;
    public long[] askOrders;

    // 买方价格、买方数量、买方订单编号
    public long[] bidPrices;
    public long[] bidVolumes;
    public long[] bidOrders;

    // 数据发布时间戳和参考序列号
    public long timestamp;
    public long referenceSeq;

    // L2MarketData 构造函数，接收卖方和买方的价格、数量、订单编号数组
    public L2MarketData(long[] askPrices, long[] askVolumes, long[] askOrders, long[] bidPrices, long[] bidVolumes, long[] bidOrders) {
        this.askPrices = askPrices;
        this.askVolumes = askVolumes;
        this.askOrders = askOrders;
        this.bidPrices = bidPrices;
        this.bidVolumes = bidVolumes;
        this.bidOrders = bidOrders;

        // 初始化卖方和买方的订单数量
        this.askSize = askPrices != null ? askPrices.length : 0;
        this.bidSize = bidPrices != null ? bidPrices.length : 0;
    }

    // 另一个构造函数，接收卖方和买方的订单量
    public L2MarketData(int askSize, int bidSize) {
        this.askPrices = new long[askSize];
        this.bidPrices = new long[bidSize];
        this.askVolumes = new long[askSize];
        this.bidVolumes = new long[bidSize];
        this.askOrders = new long[askSize];
        this.bidOrders = new long[bidSize];
    }

    // 获取卖方价格的副本
    public long[] getAskPricesCopy() {
        return Arrays.copyOf(askPrices, askSize);
    }

    // 获取卖方数量的副本
    public long[] getAskVolumesCopy() {
        return Arrays.copyOf(askVolumes, askSize);
    }

    // 获取卖方订单编号的副本
    public long[] getAskOrdersCopy() {
        return Arrays.copyOf(askOrders, askSize);
    }

    // 获取买方价格的副本
    public long[] getBidPricesCopy() {
        return Arrays.copyOf(bidPrices, bidSize);
    }

    // 获取买方数量的副本
    public long[] getBidVolumesCopy() {
        return Arrays.copyOf(bidVolumes, bidSize);
    }

    // 获取买方订单编号的副本
    public long[] getBidOrdersCopy() {
        return Arrays.copyOf(bidOrders, bidSize);
    }

    // 计算卖方订单簿的总订单量
    public long totalOrderBookVolumeAsk() {
        long totalVolume = 0L;
        for (int i = 0; i < askSize; i++) {
            totalVolume += askVolumes[i];
        }
        return totalVolume;
    }

    // 计算买方订单簿的总订单量
    public long totalOrderBookVolumeBid() {
        long totalVolume = 0L;
        for (int i = 0; i < bidSize; i++) {
            totalVolume += bidVolumes[i];
        }
        return totalVolume;
    }

    // 创建一个 L2MarketData 对象的副本
    public L2MarketData copy() {
        return new L2MarketData(
                getAskPricesCopy(),
                getAskVolumesCopy(),
                getAskOrdersCopy(),
                getBidPricesCopy(),
                getBidVolumesCopy(),
                getBidOrdersCopy());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof L2MarketData)) {
            return false;
        }
        L2MarketData o = (L2MarketData) obj;

        // 比较卖方和买方的订单数量是否相等
        if (askSize != o.askSize || bidSize != o.bidSize) {
            return false;
        }

        // 比较卖方的价格、数量和订单编号
        for (int i = 0; i < askSize; i++) {
            if (askPrices[i] != o.askPrices[i] || askVolumes[i] != o.askVolumes[i] || askOrders[i] != o.askOrders[i]) {
                return false;
            }
        }
        // 比较买方的价格、数量和订单编号
        for (int i = 0; i < bidSize; i++) {
            if (bidPrices[i] != o.bidPrices[i] || bidVolumes[i] != o.bidVolumes[i] || bidOrders[i] != o.bidOrders[i]) {
                return false;
            }
        }
        return true;
    }

    // TODO: 需要实现 hashCode 方法
}
