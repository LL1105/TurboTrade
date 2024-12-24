package com.exchange.core.processors;

import com.exchange.core.common.MatcherTradeEvent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public final class SharedPool {

    // 使用阻塞队列来存储事件链
    private final LinkedBlockingQueue<MatcherTradeEvent> eventChainsBuffer;

    // 目标事件链的长度
    @Getter
    private final int chainLength;

    // 创建一个测试用的 SharedPool 实例
    public static SharedPool createTestSharedPool() {
        return new SharedPool(8, 4, 256); // 最大池大小为 8，初始池大小为 4，链条长度为 256
    }

    /**
     * 构造函数：创建一个新的共享池
     * 
     * @param poolMaxSize     - 池的最大大小。如果链条缓冲区已满，将跳过新链条的创建。
     * @param poolInitialSize - 初始预生成的链条数量。建议设置为大于模块数量的两倍，(RE+ME)*2。
     * @param chainLength     - 目标链条长度。链条越长，意味着更少的请求新的链条。然而，较长的链条可能会导致事件占位符的饥饿。
     */
    public SharedPool(final int poolMaxSize, final int poolInitialSize, final int chainLength) {

        // 校验初始池大小是否超过了最大池大小
        if (poolInitialSize > poolMaxSize) {
            throw new IllegalArgumentException("too big poolInitialSize");
        }

        // 初始化事件链缓冲区
        this.eventChainsBuffer = new LinkedBlockingQueue<>(poolMaxSize);
        this.chainLength = chainLength;

        // 预生成指定数量的链条并加入到缓冲区
        for (int i = 0; i < poolInitialSize; i++) {
            this.eventChainsBuffer.add(MatcherTradeEvent.createEventChain(chainLength));
        }
    }

    /**
     * 从共享池中请求下一个链条
     * 线程安全
     *
     * @return 返回链条的头部（MatcherTradeEvent），如果池为空则返回 null
     */
    public MatcherTradeEvent getChain() {
        // 尝试从池中取出链条
        MatcherTradeEvent poll = eventChainsBuffer.poll();

        // 如果池为空，则创建一个新的链条
        if (poll == null) {
            poll = MatcherTradeEvent.createEventChain(chainLength);
        }

        return poll;
    }

    /**
     * 提交一个链条到池中
     * 线程安全（单生产者安全性足够）
     *
     * @param head - 链条的头部指针（MatcherTradeEvent）
     */
    public void putChain(MatcherTradeEvent head) {
        // 将链条加入到缓冲区
        boolean offer = eventChainsBuffer.offer(head);
    }

}
