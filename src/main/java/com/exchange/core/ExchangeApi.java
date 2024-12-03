package com.exchange.core;

import com.exchange.core.common.command.OrderCommand;
import com.lmax.disruptor.RingBuffer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ExchangeApi {

    private final RingBuffer<OrderCommand> ringBuffer;
}
