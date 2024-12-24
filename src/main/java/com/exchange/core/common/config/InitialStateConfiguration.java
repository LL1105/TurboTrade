package com.exchange.core.common.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * 交易所初始化配置类。
 * <p>
 * TODO: 使用接口并为清理启动、快照、日志提供不同实现。
 */
@AllArgsConstructor
@Getter
@Builder
@ToString
public final class InitialStateConfiguration {

    // 默认的初始化配置，采用清理启动模式，交换机 ID 为 "MY_EXCHANGE"
    public static InitialStateConfiguration DEFAULT = InitialStateConfiguration.cleanStart("MY_EXCHANGE");

    // 用于测试的初始化配置，采用清理启动模式，交换机 ID 为 "EC0"
    public static InitialStateConfiguration CLEAN_TEST = InitialStateConfiguration.cleanStart("EC0");

    /*
     * 交换所使用的唯一标识符（ID）。
     * 该 ID 应该不包含特殊字符，因为它用于文件名。
     */
    private final String exchangeId;

    /*
     * 要加载的快照 ID。
     * 设置为 0 表示进行清理启动。
     */
    private final long snapshotId;

    /*
     * 快照的基础序列号，标识该快照的基准序列号。
     */
    private final long snapshotBaseSeq;

    /*
     * 从日志加载时，回放命令将停止在此时间戳。
     * 设置为 0 表示忽略日志，设置为 Long.MAX_VALUE 表示读取所有可用日志，直到遇到读取错误为止。
     */
    private final long journalTimestampNs;

    /*
     * 如果请求从快照加载并且未找到快照，是否抛出异常。
     * 如果为 true，则会抛出异常；否则，初始化为空的交易所。
     */
    private final boolean throwIfSnapshotNotFound;

    // TODO: 未来可以添加忽略日志的配置

    /**
     * 判断是否是从快照加载。
     * 如果快照 ID 非 0，则表示从快照加载。
     *
     * @return 如果是从快照加载，则返回 true；否则返回 false。
     */
    public boolean fromSnapshot() {
        return snapshotId != 0;
    }

    /**
     * 创建一个清理启动配置，不涉及日志。
     *
     * @param exchangeId 交换机 ID
     * @return 清理启动配置
     */
    public static InitialStateConfiguration cleanStart(String exchangeId) {
        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .snapshotId(0)  // 快照 ID 为 0，表示清理启动
                .build();
    }

    /**
     * 创建一个带日志的清理启动配置。
     * 启用日志，并要求如果找不到快照，则抛出异常。
     *
     * @param exchangeId 交换机 ID
     * @return 启用日志的清理启动配置
     */
    public static InitialStateConfiguration cleanStartJournaling(String exchangeId) {
        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .snapshotId(0)  // 快照 ID 为 0，表示清理启动
                .snapshotBaseSeq(0)  // 快照基础序列号为 0
                .throwIfSnapshotNotFound(true)  // 如果快照找不到，则抛出异常
                .build();
    }

    /**
     * 创建一个仅从快照加载的配置，不涉及日志回放。
     * 如果快照不存在，则抛出异常。
     *
     * @param exchangeId 交换机 ID
     * @param snapshotId 快照 ID
     * @param baseSeq    基础序列号
     * @return 从快照加载配置
     */
    public static InitialStateConfiguration fromSnapshotOnly(String exchangeId, long snapshotId, long baseSeq) {
        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .snapshotId(snapshotId)  // 设置要加载的快照 ID
                .snapshotBaseSeq(baseSeq)  // 设置基础序列号
                .throwIfSnapshotNotFound(true)  // 如果找不到快照，抛出异常
                .build();
    }

    /**
     * 创建一个从最后已知状态恢复的配置，包括日志回放，直到最后已知的开始序列。
     * 启用日志回放，直到时间戳最大值（Long.MAX_VALUE）。
     *
     * @param exchangeId 交换机 ID
     * @param snapshotId 快照 ID
     * @param baseSeq    基础序列号
     * @return 从最后已知状态恢复配置
     */
    public static InitialStateConfiguration lastKnownStateFromJournal(String exchangeId, long snapshotId, long baseSeq) {
        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .snapshotId(snapshotId)  // 设置要加载的快照 ID
                .snapshotBaseSeq(baseSeq)  // 设置基础序列号
                .throwIfSnapshotNotFound(true)  // 如果快照找不到，则抛出异常
                .journalTimestampNs(Long.MAX_VALUE)  // 启用日志回放，直到最大时间戳
                .build();
    }
}
