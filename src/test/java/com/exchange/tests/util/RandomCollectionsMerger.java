package com.exchange.tests.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;

@Slf4j
public class RandomCollectionsMerger {

    /**
     * 合并多个集合，并根据每个集合的大小分配权重，优先从较大的集合中抽取元素。
     *
     * @param chunks 要合并的集合列表
     * @param seed   用于随机数生成的种子
     * @param <T>    集合元素的类型
     * @return 合并后的单一集合
     */
    public static <T> ArrayList<T> mergeCollections(final Collection<? extends Collection<T>> chunks, final long seed) {

        // 使用提供的种子创建一个 JDK 随机数生成器
        final JDKRandomGenerator jdkRandomGenerator = new JDKRandomGenerator(Long.hashCode(seed));

        // 创建一个新的 ArrayList 来存储合并后的结果
        final ArrayList<T> mergedResult = new ArrayList<>();

        // 创建初始的权重对，每个权重对包含一个 Spliterator 和该集合的大小
        List<Pair<Spliterator<T>, Double>> weightPairs = chunks.stream()
                .map(chunk -> Pair.create(chunk.spliterator(), (double) chunk.size()))
                .collect(Collectors.toList());

        // 开始合并过程
        while (!weightPairs.isEmpty()) {

            // 使用 EnumeratedDistribution 创建一个加权的随机分布，基于每个集合的大小
            final EnumeratedDistribution<Spliterator<T>> ed = new EnumeratedDistribution<>(jdkRandomGenerator, weightPairs);

            // 从随机选中的 Spliterator 中取元素，直到从其中一个集合成功取出元素
            int missCounter = 0;
            while (missCounter++ < 3) {
                // 从加权分布中随机选择一个 Spliterator
                final Spliterator<T> sample = ed.sample();
                if (sample.tryAdvance(mergedResult::add)) {
                    // 如果成功取出元素，重置 missCounter
                    missCounter = 0;
                }
            }

            // 过滤掉那些已经为空的集合，重新构建权重对
            weightPairs = weightPairs.stream()
                    .filter(p -> p.getFirst().estimateSize() > 0) // 只保留不为空的 Spliterator
                    .map(p -> Pair.create(p.getFirst(), (double) p.getFirst().estimateSize())) // 重新计算大小权重
                    .collect(Collectors.toList());
        }

        // 返回合并后的结果
        return mergedResult;
    }
}
