package com.exchange.core.common.config;

import com.exchange.core.processors.journaling.DiskSerializationProcessor;
import com.exchange.core.processors.journaling.DiskSerializationProcessorConfiguration;
import com.exchange.core.processors.journaling.DummySerializationProcessor;
import com.exchange.core.processors.journaling.ISerializationProcessor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.function.Function;

@AllArgsConstructor
@Getter
@Builder
@ToString
public class SerializationConfiguration {

    // 默认配置，不启用序列化，也不启用日志记录
    public static final SerializationConfiguration DEFAULT = SerializationConfiguration.builder()
            .enableJournaling(false) // 不启用日志记录
            .serializationProcessorFactory(cfg -> DummySerializationProcessor.INSTANCE) // 使用DummySerializationProcessor作为序列化处理器
            .build();

    // 仅使用磁盘快照，不启用日志记录
    public static final SerializationConfiguration DISK_SNAPSHOT_ONLY = SerializationConfiguration.builder()
            .enableJournaling(false) // 不启用日志记录
            .serializationProcessorFactory(exchangeCfg -> new DiskSerializationProcessor(exchangeCfg, DiskSerializationProcessorConfiguration.createDefaultConfig())) // 使用DiskSerializationProcessor处理快照
            .build();

    // 启用日志记录和磁盘快照
    public static final SerializationConfiguration DISK_JOURNALING = SerializationConfiguration.builder()
            .enableJournaling(true) // 启用日志记录
            .serializationProcessorFactory(exchangeCfg -> new DiskSerializationProcessor(exchangeCfg, DiskSerializationProcessorConfiguration.createDefaultConfig())) // 使用DiskSerializationProcessor处理日志和快照
            .build();

    /*
     * 控制是否启用日志记录（Journaling）。
     * 对于分析实例，应该设置为false。
     */
    private final boolean enableJournaling;

    /*
     * 序列化处理器的工厂方法，返回实现了ISerializationProcessor接口的实例。
     */
    private final Function<ExchangeConfiguration, ? extends ISerializationProcessor> serializationProcessorFactory;
}
