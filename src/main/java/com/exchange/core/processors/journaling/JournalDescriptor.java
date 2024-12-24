package com.exchange.core.processors.journaling;

import lombok.Data;

/**
 * 日志描述符类，用于描述基于某快照的日志流信息。
 * 日志描述符包含了日志的元数据及与快照和前后日志的关系。
 */
@Data
public class JournalDescriptor {

    // 日志创建的时间戳（纳秒）。
    private final long timestampNs;

    // 日志的起始序列号，表示该日志包含的第一个命令的序列号。
    private final long seqFirst;

    // 日志的结束序列号，初始为 -1，表示日志尚未完成。
    private long seqLast = -1;

    // 此日志所基于的快照描述符。
    private final SnapshotDescriptor baseSnapshot;

    // 指向前一个日志描述符的引用（可为空）。
    private final JournalDescriptor prev;

    // 指向后一个日志描述符的引用（可为空）。
    private JournalDescriptor next = null;

    /**
     * @param timestampNs    日志的创建时间戳（纳秒）。
     * @param seqFirst       日志的起始序列号。
     * @param baseSnapshot   日志所基于的快照。
     * @param prev           前一个日志描述符（可以为空）。
     */
    public JournalDescriptor(long timestampNs, long seqFirst, SnapshotDescriptor baseSnapshot, JournalDescriptor prev) {
        this.timestampNs = timestampNs;
        this.seqFirst = seqFirst;
        this.baseSnapshot = baseSnapshot;
        this.prev = prev;
    }
}
