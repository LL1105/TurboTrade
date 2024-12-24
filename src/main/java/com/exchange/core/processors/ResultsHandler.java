package com.exchange.core.processors;

import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.constant.OrderCommandType;
import com.lmax.disruptor.EventHandler;
import lombok.RequiredArgsConstructor;

import java.util.function.ObjLongConsumer;

@RequiredArgsConstructor
public final class ResultsHandler implements EventHandler<OrderCommand> {

    // 接受 OrderCommand 和 long 类型数据的消费函数
    private final ObjLongConsumer<OrderCommand> resultsConsumer;

    // 用于控制是否处理命令的标志
    private boolean processingEnabled = true;

    /**
     * 事件处理方法，用于处理每个 OrderCommand 事件
     *
     * @param cmd        当前的 OrderCommand 事件
     * @param sequence   事件的顺序号
     * @param endOfBatch 是否为批次的最后一个事件
     */
    @Override
    public void onEvent(OrderCommand cmd, long sequence, boolean endOfBatch) {

        // 如果当前命令是 GROUPING_CONTROL 类型，则更新处理标志
        if (cmd.command == OrderCommandType.GROUPING_CONTROL) {
            processingEnabled = cmd.orderId == 1; // 只有 orderId 为 1 时才启用处理
        }

        // 如果处理被启用，则消费该命令
        if (processingEnabled) {
            // 调用 resultsConsumer 来处理 OrderCommand 和 sequence
            resultsConsumer.accept(cmd, sequence);
        }
    }
}
