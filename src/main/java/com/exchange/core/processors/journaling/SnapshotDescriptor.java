package com.exchange.core.processors.journaling;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 描述快照信息的类。
 * 每个快照代表交易系统的一个状态，包含其元数据信息以及与相关日志的关联。
 */
@Data
public class SnapshotDescriptor implements Comparable<SnapshotDescriptor> {

    // 快照的唯一标识，0 表示初始快照（没有任何状态，表示全新启动）。
    private final long snapshotId;

    // 快照创建时的序列号，用于标识快照点。
    private final long seq;

    // 快照创建时的时间戳（纳秒）。
    private final long timestampNs;

    // 指向前一个快照的引用，用于构建链式快照结构。
    private final SnapshotDescriptor prev;

    // 指向下一个快照的引用，构建链式快照结构（TODO：支持多个分支时可能需要改为列表）。
    private SnapshotDescriptor next = null;

    // 当前快照包含的撮合引擎实例数量。
    private final int numMatchingEngines;

    // 当前快照包含的风险引擎实例数量。
    private final int numRiskEngines;

    // 基于当前快照创建的所有日志（从某个序列开始）。
    // 映射：起始序列号 -> 日志描述。
    private final NavigableMap<Long, JournalDescriptor> journals = new TreeMap<>();

    /**
     * 创建一个初始的空快照描述符。
     *
     * @param initialNumME - 初始撮合引擎实例数量。
     * @param initialNumRE - 初始风险引擎实例数量。
     * @return 新的空快照描述符实例。
     */
    public static SnapshotDescriptor createEmpty(int initialNumME, int initialNumRE) {
        return new SnapshotDescriptor(0, 0, 0, null, initialNumME, initialNumRE);
    }

    /**
     * 基于当前快照创建一个新的快照描述符。
     *
     * @param snapshotId  - 新快照的唯一标识。
     * @param seq         - 新快照的序列号。
     * @param timestampNs - 新快照的创建时间戳（纳秒）。
     * @return 新的快照描述符实例。
     */
    public SnapshotDescriptor createNext(long snapshotId, long seq, long timestampNs) {
        return new SnapshotDescriptor(snapshotId, seq, timestampNs, this, numMatchingEngines, numRiskEngines);
    }

    /**
     * 比较两个快照的序列号，用于排序。
     *
     * @param o - 要比较的另一个快照描述符。
     * @return 比较结果：负数表示当前快照的序列号小于参数快照，0 表示相等，正数表示大于。
     */
    @Override
    public int compareTo(@NotNull SnapshotDescriptor o) {
        return Long.compare(this.seq, o.seq);
    }
}
