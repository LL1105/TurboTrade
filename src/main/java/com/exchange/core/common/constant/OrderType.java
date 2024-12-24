package com.exchange.core.common.constant;

import lombok.Getter;

@Getter
public enum OrderType {

    GTC(0),

    IOC(1),
    IOC_BUDGET(2),

    FOK(3),
    FOK_BUDGET(4);

    private final byte code;

    OrderType(final int code) {
        this.code = (byte) code;
    }

    public static OrderType of(final byte code) {
        switch (code) {
            case 0:
                return GTC;
            case 1:
                return IOC;
            case 2:
                return IOC_BUDGET;
            case 3:
                return FOK;
            case 4:
                return FOK_BUDGET;
            default:
                throw new IllegalArgumentException("unknown OrderType:" + code);
        }
    }

}
