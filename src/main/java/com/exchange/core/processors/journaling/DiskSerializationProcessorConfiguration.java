package com.exchange.core.processors.journaling;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.util.function.Supplier;

@AllArgsConstructor
@Getter
@Builder
public class DiskSerializationProcessorConfiguration {

    // 默认存储文件夹
    public static final String DEFAULT_FOLDER = "./dumps";

    // 每兆字节的字节数
    private static final long ONE_MEGABYTE = 1024 * 1024;

    // 定义两种 LZ4 压缩器的工厂：一种是快速压缩，另一种是高效压缩
    public static final Supplier<LZ4Compressor> LZ4_FAST = () -> LZ4Factory.fastestInstance().fastCompressor();
    public static final Supplier<LZ4Compressor> LZ4_HIGH = () -> LZ4Factory.fastestInstance().highCompressor();

    // 存储文件夹路径
    private final String storageFolder;

    // -------- 快照配置 ---------------

    // 快照压缩器的工厂，使用 LZ4 压缩
    // 注：使用 LZ4 高压缩模式会消耗大约两倍的时间
    private final Supplier<LZ4Compressor> snapshotLz4CompressorFactory;

    // -------- 日志配置 ---------------

    // 每个日志文件的最大大小
    private final long journalFileMaxSize;

    // 日志缓冲区大小
    private final int journalBufferSize;

    // 如果批处理数据大小（以字节为单位）超过此阈值，则使用 LZ4 压缩
    // 批处理的平均大小取决于流量和磁盘写入延迟，可能会达到 20-100KB（3M TPS 和 0.15ms 磁盘写入延迟）
    // 在适度负载下，对于单条消息，压缩通常不会启用
    private final int journalBatchCompressThreshold;

    // 日志压缩器的工厂，使用 LZ4 压缩
    // 注：由于高压缩模式对吞吐量影响非常大，不推荐使用 LZ4 高压缩模式
    private final Supplier<LZ4Compressor> journalLz4CompressorFactory;

    // 创建默认的配置实例
    public static DiskSerializationProcessorConfiguration createDefaultConfig() {

        return DiskSerializationProcessorConfiguration.builder()
                // 设置默认存储文件夹
                .storageFolder(DEFAULT_FOLDER)
                // 设置快照的 LZ4 压缩器（使用快速压缩）
                .snapshotLz4CompressorFactory(LZ4_FAST)
                // 设置日志文件的最大大小为 4000MB
                .journalFileMaxSize(4000 * ONE_MEGABYTE)
                // 设置日志缓冲区大小为 256KB
                .journalBufferSize(256 * 1024) // 256 KB - TODO: 基于环形缓冲区大小进行计算
                // 设置日志批处理压缩阈值为 2048 字节
                .journalBatchCompressThreshold(2048)
                // 设置日志的 LZ4 压缩器（使用快速压缩）
                .journalLz4CompressorFactory(LZ4_FAST)
                .build();
    }
}
