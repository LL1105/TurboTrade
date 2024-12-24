package com.exchange.core.common.api.reports.resultImpl;

import com.exchange.core.common.api.reports.ReportResult;
import com.exchange.core.utils.SerializationUtils;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Stream;

/**
 * 状态哈希报告结果
 */
@Getter
@Slf4j
@EqualsAndHashCode
@RequiredArgsConstructor
@ToString
public final class StateHashReportResult implements ReportResult {

    // 空的默认报告
    public static final StateHashReportResult EMPTY = new StateHashReportResult(new TreeMap<>());
    
    // 用于对子模块键进行排序的比较器
    private static final Comparator<SubmoduleKey> SUBMODULE_KEY_COMPARATOR =
            Comparator.<SubmoduleKey>comparingInt(k -> k.submodule.code)
                    .thenComparing(k -> k.moduleId);

    // 存储子模块的哈希值，按 SubmoduleKey 排序
    private final SortedMap<SubmoduleKey, Integer> hashCodes;

    // 从字节输入流构造 StateHashReportResult 对象
    private StateHashReportResult(final BytesIn bytesIn) {
        this.hashCodes = SerializationUtils.readGenericMap(bytesIn, TreeMap::new, SubmoduleKey::new, BytesIn::readInt);
    }

    /**
     * 合并多个报告片段为一个完整的报告
     */
    public static StateHashReportResult merge(final Stream<BytesIn> pieces) {
        return pieces.map(StateHashReportResult::new)
                .reduce(EMPTY, (r1, r2) -> {
                    SortedMap<SubmoduleKey, Integer> hashcodes = new TreeMap<>(r1.hashCodes);
                    hashcodes.putAll(r2.hashCodes);
                    return new StateHashReportResult(hashcodes);
                });
    }

    /**
     * 创建一个子模块键
     */
    public static SubmoduleKey createKey(int moduleId, SubmoduleType submoduleType) {
        return new SubmoduleKey(moduleId, submoduleType);
    }

    /**
     * 将该报告对象序列化为字节输出流
     */
    @Override
    public void writeMarshallable(final BytesOut bytes) {
        SerializationUtils.marshallGenericMap(hashCodes, bytes, (b, k) -> k.writeMarshallable(b), BytesOut::writeInt);
    }

    /**
     * 获取报告的状态哈希值
     * 计算方法是：将每个子模块的哈希值与它的子模块键组合，然后计算整体哈希值
     */
    public int getStateHash() {
        final int[] hashes = hashCodes.entrySet().stream()
                .mapToInt(e -> Objects.hash(e.getKey(), e.getValue())).toArray();

        return Arrays.hashCode(hashes);
    }

    /**
     * 模块类型枚举，包含风险引擎和匹配引擎
     */
    public enum ModuleType {
        RISK_ENGINE,
        MATCHING_ENGINE
    }

    /**
     * 子模块类型，定义了系统中各种子模块的类型
     */
    @AllArgsConstructor
    public enum SubmoduleType {
        RISK_SYMBOL_SPEC_PROVIDER(0, ModuleType.RISK_ENGINE),
        RISK_USER_PROFILE_SERVICE(1, ModuleType.RISK_ENGINE),
        RISK_BINARY_CMD_PROCESSOR(2, ModuleType.MATCHING_ENGINE),
        RISK_LAST_PRICE_CACHE(3, ModuleType.RISK_ENGINE),
        RISK_FEES(4, ModuleType.RISK_ENGINE),
        RISK_ADJUSTMENTS(5, ModuleType.RISK_ENGINE),
        RISK_SUSPENDS(6, ModuleType.RISK_ENGINE),
        RISK_SHARD_MASK(7, ModuleType.RISK_ENGINE),

        MATCHING_BINARY_CMD_PROCESSOR(64, ModuleType.MATCHING_ENGINE),
        MATCHING_ORDER_BOOKS(65, ModuleType.MATCHING_ENGINE),
        MATCHING_SHARD_MASK(66, ModuleType.MATCHING_ENGINE);

        public final int code;
        public final ModuleType moduleType;

        public static SubmoduleType fromCode(int code) {
            return Arrays.stream(values())
                    .filter(c -> c.code == code)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown SubmoduleType"));
        }
    }

    /**
     * 子模块键，包含模块 ID 和子模块类型，用于在哈希表中唯一标识子模块
     */
    @RequiredArgsConstructor
    @EqualsAndHashCode
    public static final class SubmoduleKey implements WriteBytesMarshallable, Comparable<SubmoduleKey> {

        public final int moduleId;    // 模块 ID
        public final SubmoduleType submodule; // 子模块类型

        private SubmoduleKey(final BytesIn bytesIn) {
            this.moduleId = bytesIn.readInt();
            this.submodule = SubmoduleType.fromCode(bytesIn.readInt());
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeInt(moduleId);
            bytes.writeInt(submodule.code);
        }

        /**
         * 比较当前子模块键与另一个子模块键
         */
        @Override
        public int compareTo(@NotNull SubmoduleKey o) {
            return SUBMODULE_KEY_COMPARATOR.compare(this, o);
        }
    }
}
