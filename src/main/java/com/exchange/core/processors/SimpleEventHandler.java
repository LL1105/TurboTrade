package com.exchange.core.processors;

import com.exchange.core.common.command.OrderCommand;

/**
 * SimpleEventHandler 接口定义了如何处理事件的通用方法。
 * 这个接口一般用于 Disruptor 或其他消息处理框架中的事件处理逻辑。
 * 实现该接口的类需要提供处理事件的具体实现。
 */
public interface SimpleEventHandler {

    /**
     * 处理事件命令并生成相应的数据。
     *
     * @param seq   - 事件的序列号，表示事件在处理链中的位置。
     * @param event - 事件对象，这里是 `OrderCommand` 类型，表示订单相关的命令数据。
     * @return 如果返回 true，则会强制发布当前序列号的事件。这通常用于批量操作时，指示框架处理完一定数量的事件后，强制将其发布出去。
     */
    boolean onEvent(long seq, OrderCommand event);

}
