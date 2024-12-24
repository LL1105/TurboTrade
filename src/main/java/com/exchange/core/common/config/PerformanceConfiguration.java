package com.exchange.core.common.config;

import com.exchange.core.common.constant.CoreWaitStrategy;
import com.exchange.core.orderbook.IOrderBook;
import com.exchange.core.orderbook.OrderBookDirectImpl;
import com.exchange.core.orderbook.OrderBookNaiveImpl;
import com.exchange.core.utils.AffinityThreadFactory;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

/**
 * 交易所性能配置类
 * 该配置类用于调整交易所的性能相关参数，包括消息缓冲区大小、匹配引擎数量、风险引擎数量、
 * 线程工厂、L2更新频率等。根据不同的业务需求和性能要求，可以选择不同的配置组合。
 */
@AllArgsConstructor
@Getter
@Builder
public final class PerformanceConfiguration {

    // 默认的性能配置
    public static final PerformanceConfiguration DEFAULT = PerformanceConfiguration.baseBuilder().build();

    // 环形缓冲区的大小（命令的数量），必须是 2 的幂
    private final int ringBufferSize;

    // 匹配引擎的数量，每个实例需要额外的 CPU 核心
    private final int matchingEnginesNum;

    // 风险引擎的数量，每个实例需要额外的 CPU 核心
    private final int riskEnginesNum;

    // R2 阶段未处理的最大消息数，必须小于 ringBufferSize 的四分之一
    // 较小的值（如 100）提供较低的平均延迟
    // 较大的值（如 2000）提供更高的吞吐量和尾延迟
    private final int msgsInGroupLimit;

    // R2 阶段未处理的最大间隔，单位纳秒
    // 干扰 msgsInGroupLimit 参数
    // 较小的值（如 1000，1us）提供较低的平均延迟
    // 较大的值（如 2000）提供更高的吞吐量和尾延迟
    private final int maxGroupDurationNs;

    // 是否对每个成功执行的命令发送 L2 数据
    // 默认情况下（false），匹配引擎只有在分组处理器要求时才会每 10 毫秒发送一次 L2 更新。
    // 如果为 true，则会在每个成功执行的命令之后发送 L2 数据。
    // 启用此选项将影响性能。
    private final boolean sendL2ForEveryCmd;

    // L2 更新的深度
    // 默认值为 8（足够用于风险处理器，因为它不会检查订单簿深度）
    // 如果设置为 Integer.MAX_VALUE，则会发送完整的订单簿。
    private final int l2RefreshDepth;

    // Disruptor 线程工厂
    private final ThreadFactory threadFactory;

    // Disruptor 等待策略
    private final CoreWaitStrategy waitStrategy;

    // 订单簿工厂，用于创建订单簿实例
    private final IOrderBook.OrderBookFactory orderBookFactory;

    // 用于二进制命令和报告的 LZ4 压缩工厂
    private final Supplier<LZ4Compressor> binaryCommandsLz4CompressorFactory;

    @Override
    public String toString() {
        return "PerformanceConfiguration{" +
                "ringBufferSize=" + ringBufferSize +
                ", matchingEnginesNum=" + matchingEnginesNum +
                ", riskEnginesNum=" + riskEnginesNum +
                ", msgsInGroupLimit=" + msgsInGroupLimit +
                ", maxGroupDurationNs=" + maxGroupDurationNs +
                ", sendL2ForEveryCmd=" + sendL2ForEveryCmd +
                ", l2RefreshDepth=" + l2RefreshDepth +
                ", threadFactory=" + (threadFactory == null ? null : threadFactory.getClass().getSimpleName()) +
                ", waitStrategy=" + waitStrategy +
                ", orderBookFactory=" + (orderBookFactory == null ? null : orderBookFactory.getClass().getSimpleName()) +
                ", binaryCommandsLz4CompressorFactory=" + (binaryCommandsLz4CompressorFactory == null ? null : binaryCommandsLz4CompressorFactory.getClass().getSimpleName()) +
                '}';
    }

    // TODO 添加预计的用户数和符号数

    /**
     * 创建一个基础性能配置构建器，适用于默认配置。
     * 默认配置的 ringBufferSize 为 16 * 1024，匹配引擎和风险引擎各 1 个，消息组限制为 256，最大组时长为 10us。
     * @return PerformanceConfigurationBuilder
     */
    public static PerformanceConfiguration.PerformanceConfigurationBuilder baseBuilder() {
        return builder()
                .ringBufferSize(16 * 1024)
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .msgsInGroupLimit(256)
                .maxGroupDurationNs(10_000)
                .sendL2ForEveryCmd(false)
                .l2RefreshDepth(8)
                .threadFactory(Thread::new)
                .waitStrategy(CoreWaitStrategy.BLOCKING)
                .binaryCommandsLz4CompressorFactory(() -> LZ4Factory.fastestInstance().highCompressor())
                .orderBookFactory(OrderBookNaiveImpl::new);
    }

    /**
     * 创建一个低延迟性能配置构建器，适用于低延迟要求的场景。
     * 配置适合高吞吐量和低延迟的系统。
     * @return PerformanceConfigurationBuilder
     */
    public static PerformanceConfiguration.PerformanceConfigurationBuilder latencyPerformanceBuilder() {
        return builder()
                .ringBufferSize(2 * 1024)
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .msgsInGroupLimit(256)
                .maxGroupDurationNs(10_000)
                .sendL2ForEveryCmd(false)
                .l2RefreshDepth(8)
                .threadFactory(new AffinityThreadFactory(AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE))
                .waitStrategy(CoreWaitStrategy.BUSY_SPIN)
                .binaryCommandsLz4CompressorFactory(() -> LZ4Factory.fastestInstance().highCompressor())
                .orderBookFactory(OrderBookDirectImpl::new);
    }

    /**
     * 创建一个高吞吐量性能配置构建器，适用于高吞吐量要求的场景。
     * 配置适合需要处理大量订单的系统。
     * @return PerformanceConfigurationBuilder
     */
    public static PerformanceConfiguration.PerformanceConfigurationBuilder throughputPerformanceBuilder() {
        return builder()
                .ringBufferSize(64 * 1024)
                .matchingEnginesNum(4)
                .riskEnginesNum(2)
                .msgsInGroupLimit(4_096)
                .maxGroupDurationNs(4_000_000)
                .sendL2ForEveryCmd(false)
                .l2RefreshDepth(8)
                .threadFactory(new AffinityThreadFactory(AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE))
                .waitStrategy(CoreWaitStrategy.BUSY_SPIN)
                .binaryCommandsLz4CompressorFactory(() -> LZ4Factory.fastestInstance().highCompressor())
                .orderBookFactory(OrderBookDirectImpl::new);
    }
}
