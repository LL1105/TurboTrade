package com.exchange.tests.util;

import com.exchange.core.orderbook.IOrderBook;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

import java.util.*;
import java.util.function.UnaryOperator;

@Slf4j
public final class TestOrdersGeneratorSession {

    // 订单簿对象，表示交易所的订单簿
    public final IOrderBook orderBook;

    // 交易的总数
    public final int transactionsNumber;

    // 目标订单簿的订单数量的一半
    public final int targetOrderBookOrdersHalf;

    // 价格偏差，表示价格波动范围
    public final long priceDeviation;

    // 是否启用 IOC (立即或取消) 订单
    public final boolean avalancheIOC;

    // 用户数量
    public final int numUsers;

    // 用户ID映射函数
    public final UnaryOperator<Integer> uidMapper;

    // 交易符号
    public final int symbol;

    // 随机数生成器
    public final Random rand;

    // 存储订单价格的哈希映射
    public final IntIntHashMap orderPrices = new IntIntHashMap();

    // 存储订单大小的哈希映射
    public final IntIntHashMap orderSizes = new IntIntHashMap();

    // 存储订单用户ID的映射
    public final Map<Integer, Integer> orderUids = new LinkedHashMap<>();

    // 统计订单簿的不同维度
    public final List<Integer> orderBookSizeAskStat = new ArrayList<>();
    public final List<Integer> orderBookSizeBidStat = new ArrayList<>();
    public final List<Integer> orderBookNumOrdersAskStat = new ArrayList<>();
    public final List<Integer> orderBookNumOrdersBidStat = new ArrayList<>();

    // 最小价格和最大价格，价格波动的范围
    public final long minPrice;
    public final long maxPrice;

    // 快速填充阈值
    public final int lackOrOrdersFastFillThreshold;

    // 上一个成交的价格
    public long lastTradePrice;

    // 用于表示价格变动的方向，1表示价格变化，0表示固定价格
    public int priceDirection;

    // 是否已经放置了初始订单
    public boolean initialOrdersPlaced = false;

    // 完成的订单数、被拒绝的订单数、减少的订单数
    public long numCompleted = 0;
    public long numRejected = 0;
    public long numReduced = 0;

    // 订单操作计数器
    public long counterPlaceMarket = 0;
    public long counterPlaceLimit = 0;
    public long counterCancel = 0;
    public long counterMove = 0;
    public long counterReduce = 0;

    // 当前交易的序列号
    public int seq = 1;

    // 存储订单的填充序列
    public Integer filledAtSeq = null;

    // 订单簿统计（每256个订单更新一次）
    public int lastOrderBookOrdersSizeAsk = 0;
    public int lastOrderBookOrdersSizeBid = 0;
    public long lastTotalVolumeAsk = 0;
    public long lastTotalVolumeBid = 0;

    // 初始化交易会话
    public TestOrdersGeneratorSession(IOrderBook orderBook,
                                      int transactionsNumber,
                                      int targetOrderBookOrdersHalf,
                                      boolean avalancheIOC,
                                      int numUsers,
                                      UnaryOperator<Integer> uidMapper,
                                      int symbol,
                                      boolean enableSlidingPrice,
                                      int seed) {
        this.orderBook = orderBook;  // 初始化订单簿
        this.transactionsNumber = transactionsNumber;  // 初始化交易数量
        this.targetOrderBookOrdersHalf = targetOrderBookOrdersHalf;  // 初始化目标订单簿的一半订单数
        this.avalancheIOC = avalancheIOC;  // 是否启用 avalanche IOC
        this.numUsers = numUsers;  // 初始化用户数
        this.uidMapper = uidMapper;  // 初始化用户ID映射函数
        this.symbol = symbol;  // 初始化交易符号
        this.rand = new Random(Objects.hash(symbol * -177277, seed * 10037 + 198267));  // 创建随机数生成器

        // 根据随机生成的数值确定一个初始价格，并根据价格计算价格波动范围
        int price = (int) Math.pow(10, 3.3 + rand.nextDouble() * 1.5 + rand.nextDouble() * 1.5);
        this.lastTradePrice = price;  // 设置初始交易价格
        this.priceDeviation = Math.min((int) (price * 0.05), 10000);  // 设置价格偏差
        this.minPrice = price - priceDeviation * 5;  // 最小价格
        this.maxPrice = price + priceDeviation * 5;  // 最大价格

        this.priceDirection = enableSlidingPrice ? 1 : 0;  // 如果启用滑动价格，则允许价格上下波动

        // 设置快速填充阈值
        this.lackOrOrdersFastFillThreshold = Math.min(TestOrdersGenerator.CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND, targetOrderBookOrdersHalf * 3 / 4);
    }
}
