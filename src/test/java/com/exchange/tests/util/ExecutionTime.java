package com.exchange.tests.util;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class ExecutionTime implements AutoCloseable {

    // 用于记录执行时间的消费者接口
    private final Consumer<String> executionTimeConsumer;

    // 记录开始时的纳秒时间
    private final long startNs = System.nanoTime();

    // 存储执行结束时的纳秒时间，使用 CompletableFuture 延迟计算
    @Getter
    private final CompletableFuture<Long> resultNs = new CompletableFuture<>();

    /**
     * 默认构造函数，默认执行时间消费者为空的 Consumer（不做任何操作）
     */
    public ExecutionTime() {
        this.executionTimeConsumer = s -> {
        };
    }

    /**
     * 在对象被关闭时（调用 `close()` 方法），记录并消费执行时间
     */
    @Override
    public void close() {
        executionTimeConsumer.accept(getTimeFormatted());
    }

    /**
     * 获取格式化后的执行时间
     *
     * @return 格式化后的执行时间字符串，单位为µs/ms/s等
     */
    public String getTimeFormatted() {
        // 如果执行时间还未计算完成，则先计算结束时间并完成 resultNs
        if (!resultNs.isDone()) {
            resultNs.complete(System.nanoTime() - startNs);
        }
        // 获取并格式化执行时间
        return LatencyTools.formatNanos(resultNs.join());
    }
}
