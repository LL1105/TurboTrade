package com.exchange.core.processors.journaling;

import com.exchange.core.ExchangeApi;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.config.InitialStateConfiguration;
import lombok.AllArgsConstructor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.function.Function;

/**
 * 负责管理交易所核心模块的序列化与反序列化，
 * 快照管理以及日志功能。它用于持久化系统状态并支持日志回放。
 */
public interface ISerializationProcessor {

    /**
     * 将模块状态序列化并存储到持久化存储（如磁盘或 NAS）。
     * 方法线程安全，由每个模块的线程调用。
     * 方法是同步的，只有数据安全存储后才会返回 true。
     *
     * @param snapshotId  - 唯一的快照 ID
     * @param seq         - 当前序列号
     * @param timestampNs - 时间戳（纳秒）
     * @param type        - 模块类型（如风险引擎或撮合引擎）
     * @param instanceId  - 模块实例号（每种模块类型从 0 开始）
     * @param obj         - 需要序列化的数据对象
     * @return true 如果序列化成功，否则返回 false
     */
    boolean storeData(long snapshotId,
                      long seq,
                      long timestampNs,
                      SerializedModuleType type,
                      int instanceId,
                      WriteBytesMarshallable obj);

    /**
     * 从存储中反序列化模块状态。
     * 方法线程安全，在每个模块创建时调用。
     *
     * @param snapshotId - 唯一的快照 ID
     * @param type       - 模块类型（如风险引擎或撮合引擎）
     * @param instanceId - 模块实例号（每种模块类型从 0 开始）
     * @param initFunc   - 初始化函数，用于创建模块实例
     * @param <T>        - 模块实现类的类型
     * @return 构建的模块对象，或抛出异常
     */
    <T> T loadData(long snapshotId,
                   SerializedModuleType type,
                   int instanceId,
                   Function<BytesIn, T> initFunc);

    /**
     * 将命令写入日志（journal）。
     *
     * @param cmd  - 需要写入的命令
     * @param dSeq - 日志序列号
     * @param eob  - 如果为 true，则同步提交所有之前的数据
     * @throws IOException - 如果写入失败，会抛出异常（系统会停止响应）
     */
    void writeToJournal(OrderCommand cmd, long dSeq, boolean eob) throws IOException;

    /**
     * 启用日志功能。
     *
     * @param afterSeq - 指定启用日志记录的起始序列号，对于较小的序列号不会写入日志
     * @param api      - 交易所 API 引用
     */
    void enableJournaling(long afterSeq, ExchangeApi api);

    /**
     * 获取所有可用的快照点。
     *
     * @return 包含快照点的有序映射
     */
    NavigableMap<Long, SnapshotDescriptor> findAllSnapshotPoints();

    /**
     * 部分回放日志。
     *
     * @param snapshotId - 快照 ID（对于树形历史非常重要）
     * @param seqFrom    - 起始命令序列号（不包括此序列号）
     * @param seqTo      - 结束命令序列号（包括此序列号）
     * @param api        - 交易所 API 引用
     */
    void replayJournalStep(long snapshotId, long seqFrom, long seqTo, ExchangeApi api);

    /**
     * 完全回放日志。
     *
     * @param initialStateConfiguration - 初始状态配置
     * @param api                       - 交易所 API 引用
     * @return 日志回放的最后序列号
     */
    long replayJournalFull(InitialStateConfiguration initialStateConfiguration, ExchangeApi api);

    /**
     * 完全回放日志并启用日志记录。
     *
     * @param initialStateConfiguration - 初始状态配置
     * @param exchangeApi               - 交易所 API 引用
     */
    void replayJournalFullAndThenEnableJouraling(InitialStateConfiguration initialStateConfiguration, ExchangeApi exchangeApi);

    /**
     * 检查指定的快照文件是否存在。
     *
     * @param snapshotId - 唯一的快照 ID
     * @param type       - 模块类型（如风险引擎或撮合引擎）
     * @param instanceId - 模块实例号（每种模块类型从 0 开始）
     * @return 如果快照文件存在，返回 true，否则返回 false
     */
    boolean checkSnapshotExists(long snapshotId, SerializedModuleType type, int instanceId);

    /**
     * 模块类型的枚举类，表示风险引擎和撮合引擎。
     */
    @AllArgsConstructor
    enum SerializedModuleType {
        RISK_ENGINE("RE"), // 风险引擎
        MATCHING_ENGINE_ROUTER("ME"); // 撮合引擎

        final String code;
    }

    /**
     * 判断是否可以从快照加载模块。
     *
     * @param serializationProcessor - 序列化处理器实例
     * @param initStateCfg           - 初始状态配置
     * @param shardId                - 分片 ID
     * @param module                 - 模块类型
     * @return true 如果快照存在并可以加载，否则返回 false
     */
    static boolean canLoadFromSnapshot(final ISerializationProcessor serializationProcessor,
                                       final InitialStateConfiguration initStateCfg,
                                       final int shardId,
                                       final ISerializationProcessor.SerializedModuleType module) {

        if (initStateCfg.fromSnapshot()) {

            final boolean snapshotExists = serializationProcessor.checkSnapshotExists(
                    initStateCfg.getSnapshotId(),
                    module,
                    shardId);

            if (snapshotExists) {
                // 如果快照存在，返回 true
                return true;
            } else {
                // 如果快照不存在，根据配置抛出异常
                if (initStateCfg.isThrowIfSnapshotNotFound()) {
                    throw new IllegalStateException("Snapshot " + initStateCfg.getSnapshotId() + " shardId=" + shardId + " not found for " + module);
                }
            }
        }

        return false;
    }
}

