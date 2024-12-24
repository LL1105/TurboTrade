package com.exchange.core.utils;

import com.exchange.core.common.StateHash;
import lombok.extern.slf4j.Slf4j;
import org.agrona.collections.MutableLong;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
public final class HashingUtils {

    /**
     * 计算给定 BitSet 对象的状态哈希值
     *
     * @param bitSet 需要计算哈希值的 BitSet 对象
     * @return 计算得到的哈希值
     */
    public static int stateHash(final BitSet bitSet) {
        // 将 BitSet 转换为 long 数组并计算其哈希值
        return Arrays.hashCode(bitSet.toLongArray());
    }

    /**
     * 计算给定 LongObjectHashMap 对象的状态哈希值
     * 
     * @param hashMap 需要计算哈希值的 LongObjectHashMap 对象
     * @param <T> 泛型 T 必须实现 StateHash 接口
     * @return 计算得到的哈希值
     */
    public static <T extends StateHash> int stateHash(final LongObjectHashMap<T> hashMap) {
        final MutableLong mutableLong = new MutableLong();
        // 遍历哈希表的每个键值对，累加每个元素的哈希值
        hashMap.forEachKeyValue((k, v) -> mutableLong.addAndGet(Objects.hash(k, v.stateHash())));
        return Long.hashCode(mutableLong.value);  // 返回累加后的哈希值
    }

    /**
     * 计算给定 IntObjectHashMap 对象的状态哈希值
     * 
     * @param hashMap 需要计算哈希值的 IntObjectHashMap 对象
     * @param <T> 泛型 T 必须实现 StateHash 接口
     * @return 计算得到的哈希值
     */
    public static <T extends StateHash> int stateHash(final IntObjectHashMap<T> hashMap) {
        final MutableLong mutableLong = new MutableLong();
        // 遍历哈希表的每个键值对，累加每个元素的哈希值
        hashMap.forEachKeyValue((k, v) -> mutableLong.addAndGet(Objects.hash(k, v.stateHash())));
        return Long.hashCode(mutableLong.value);  // 返回累加后的哈希值
    }

    /**
     * 计算给定流中元素的状态哈希值
     * 
     * @param stream 需要计算哈希值的流
     * @return 计算得到的哈希值
     */
    public static int stateHashStream(final Stream<? extends StateHash> stream) {
        int h = 0;
        final Iterator<? extends StateHash> iterator = stream.iterator();
        // 遍历流中的每个元素，累加每个元素的哈希值
        while (iterator.hasNext()) {
            h = h * 31 + iterator.next().stateHash();  // 计算哈希值
        }
        return h;  // 返回累加后的哈希值
    }

    /**
     * 检查两个流中的元素是否相同，并且顺序相同
     *
     * @param s1 第一个流
     * @param s2 第二个流
     * @return 如果两个流包含相同的元素，并且顺序相同，则返回 true；否则返回 false
     */
    public static boolean checkStreamsEqual(final Stream<?> s1, final Stream<?> s2) {
        final Iterator<?> iter1 = s1.iterator(), iter2 = s2.iterator();
        // 遍历两个流，比较每个元素是否相等
        while (iter1.hasNext() && iter2.hasNext()) {
            if (!iter1.next().equals(iter2.next())) {
                return false;  // 一旦发现不相等的元素，返回 false
            }
        }
        // 如果两个流的元素数量相同且没有剩余元素，则返回 true
        return !iter1.hasNext() && !iter2.hasNext();
    }

}
