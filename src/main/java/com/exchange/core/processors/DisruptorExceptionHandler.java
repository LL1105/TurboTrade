package com.exchange.core.processors;

import com.lmax.disruptor.ExceptionHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;

@Slf4j
@RequiredArgsConstructor
public final class DisruptorExceptionHandler<T> implements ExceptionHandler<T> {

    // Disruptor 名称，用于标识和记录日志
    public final String name;

    // 异常处理器，接收异常和事件的序列号，并执行相应的处理
    public final BiConsumer<Throwable, Long> onException;

    /**
     * 处理事件处理过程中发生的异常
     *
     * @param ex      异常对象
     * @param sequence 异常发生的事件序列号
     * @param event   异常发生时正在处理的事件
     */
    @Override
    public void handleEventException(Throwable ex, long sequence, T event) {
        // 记录事件处理异常的日志
        log.debug("Disruptor '{}' seq={} caught exception: {}", name, sequence, event, ex);

        // 调用外部传入的异常处理逻辑
        onException.accept(ex, sequence);
    }

    /**
     * 处理启动过程中发生的异常
     *
     * @param ex 异常对象
     */
    @Override
    public void handleOnStartException(Throwable ex) {
        // 记录启动异常的日志
        log.debug("Disruptor '{}' startup exception: {}", name, ex);
    }

    /**
     * 处理关闭过程中发生的异常
     *
     * @param ex 异常对象
     */
    @Override
    public void handleOnShutdownException(Throwable ex) {
        // 记录关闭异常的日志
        log.debug("Disruptor '{}' shutdown exception: {}", name, ex);
    }
}
