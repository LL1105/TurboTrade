package com.exchange.core.utils;

import com.exchange.core.common.MatcherTradeEvent;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.constant.CommandResultCode;
import lombok.extern.slf4j.Slf4j;

import static net.openhft.chronicle.core.UnsafeMemory.UNSAFE;

/**
 * UnsafeUtils 类提供了对 OrderCommand 对象的原子性操作，
 * 包括使用 {@link sun.misc.Unsafe} 类进行低级别的内存操作，以提高性能。
 * 主要用途是设置命令结果和追加交易事件，避免锁的使用，确保操作是原子性的。
 */
@Slf4j
public final class UnsafeUtils {

    // 用于存取 OrderCommand 对象的 resultCode 和 matcherEvent 字段的内存偏移量
    private static final long OFFSET_RESULT_CODE;
    private static final long OFFSET_EVENT;

    static {
        try {
            // 获取 OrderCommand 类中 resultCode 字段的内存偏移量
            OFFSET_RESULT_CODE = UNSAFE.objectFieldOffset(OrderCommand.class.getDeclaredField("resultCode"));
            // 获取 OrderCommand 类中 matcherEvent 字段的内存偏移量
            OFFSET_EVENT = UNSAFE.objectFieldOffset(OrderCommand.class.getDeclaredField("matcherEvent"));
        } catch (NoSuchFieldException ex) {
            // 如果获取字段偏移量时出现异常，抛出 IllegalStateException
            throw new IllegalStateException(ex);
        }
    }

    /**
     * 使用 CAS（比较并交换）操作原子地设置订单命令的结果状态。
     * 
     * @param cmd 订单命令对象
     * @param result 命令执行结果，true 表示成功，false 表示失败
     * @param successCode 成功时的结果代码
     * @param failureCode 失败时的结果代码
     */
    public static void setResultVolatile(final OrderCommand cmd,
                                         final boolean result,
                                         final CommandResultCode successCode,
                                         final CommandResultCode failureCode) {

        // 根据 result 决定要设置的结果代码
        final CommandResultCode codeToSet = result ? successCode : failureCode;

        CommandResultCode currentCode;
        do {
            // 读取当前的 resultCode
            currentCode = (CommandResultCode) UNSAFE.getObjectVolatile(cmd, OFFSET_RESULT_CODE);

            // 如果当前结果代码已经是期望的代码，或者已经设置了失败的代码，则退出
            if (currentCode == codeToSet || currentCode == failureCode) {
                break;
            }

            // 否则，使用 CAS 操作尝试将 resultCode 更新为期望的代码
        } while (!UNSAFE.compareAndSwapObject(cmd, OFFSET_RESULT_CODE, currentCode, codeToSet));
    }

    /**
     * 使用 CAS（比较并交换）操作原子地将交易事件追加到订单命令的事件链中。
     * 
     * @param cmd 订单命令对象
     * @param eventHead 交易事件链的头部事件
     */
    public static void appendEventsVolatile(final OrderCommand cmd,
                                            final MatcherTradeEvent eventHead) {

        // 查找事件链的尾部
        final MatcherTradeEvent tail = eventHead.findTail();

        do {
            // 读取当前的事件头，并将新的事件附加到事件链的尾部
            tail.nextEvent = (MatcherTradeEvent) UNSAFE.getObjectVolatile(cmd, OFFSET_EVENT);

            // 使用 CAS 操作原子地将事件链更新为新的头部事件
        } while (!UNSAFE.compareAndSwapObject(cmd, OFFSET_EVENT, tail.nextEvent, eventHead));
    }
}
