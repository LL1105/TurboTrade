package com.exchange.core.common.api;


import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@Builder
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public final class ApiOrderBookRequest extends ApiCommand {

    final public int symbol;

    final public int size;

    @Override
    public String toString() {
        return "[OB " + symbol + " " + size + "]";
    }
}
