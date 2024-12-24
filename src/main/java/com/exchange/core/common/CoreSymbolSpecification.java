package com.exchange.core.common;

import com.exchange.core.common.constant.SymbolType;
import lombok.*;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Objects;

@Builder
@AllArgsConstructor
@Getter
@ToString
public final class CoreSymbolSpecification implements WriteBytesMarshallable, StateHash {

    // 交易对的符号ID
    public final int symbolId;

    // 交易对类型（例如现货、期货）
    @NonNull
    public final SymbolType type;

    // 货币对的规格
    public final int baseCurrency;  // 基础货币（如 BTC、ETH）
    public final int quoteCurrency; // 报价货币（如 USD、EUR）
    public final long baseScaleK;   // 基础货币的数量乘数（如基础货币单位的合约规模）
    public final long quoteScaleK;  // 报价货币的数量乘数（如报价货币单位的步进值）

    // 每手交易的费用（以报价货币单位计）
    public final long takerFee; // TODO：检查这个不变式：taker费用不应低于maker费用
    public final long makerFee;

    // 保证金设置（仅适用于期货合约类型）
    public final long marginBuy;   // 买入保证金（以报价货币计）
    public final long marginSell;  // 卖出保证金（以报价货币计）

    // 使用字节流初始化 CoreSymbolSpecification 对象
    public CoreSymbolSpecification(BytesIn bytes) {
        this.symbolId = bytes.readInt();
        this.type = SymbolType.of(bytes.readByte());
        this.baseCurrency = bytes.readInt();
        this.quoteCurrency = bytes.readInt();
        this.baseScaleK = bytes.readLong();
        this.quoteScaleK = bytes.readLong();
        this.takerFee = bytes.readLong();
        this.makerFee = bytes.readLong();
        this.marginBuy = bytes.readLong();
        this.marginSell = bytes.readLong();
    }

    /* 尚未支持的功能：
    // 期货合约的订单簿限制（例如涨跌停板）
    // public final long highLimit;
    // public final long lowLimit;
    // 合约交换设置（如长期交换和短期交换）
    // public final long longSwap;
    // public final long shortSwap;
    // 活跃状态（如非活跃、活跃、已过期）
    */

    // 将对象数据写入字节输出流
    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(symbolId);
        bytes.writeByte(type.getCode());
        bytes.writeInt(baseCurrency);
        bytes.writeInt(quoteCurrency);
        bytes.writeLong(baseScaleK);
        bytes.writeLong(quoteScaleK);
        bytes.writeLong(takerFee);
        bytes.writeLong(makerFee);
        bytes.writeLong(marginBuy);
        bytes.writeLong(marginSell);
    }

    // 计算并返回该对象的状态哈希值
    @Override
    public int stateHash() {
        return Objects.hash(
                symbolId,
                type.getCode(),
                baseCurrency,
                quoteCurrency,
                baseScaleK,
                quoteScaleK,
                takerFee,
                makerFee,
                marginBuy,
                marginSell);
    }

    // 重写 equals 方法，比较两个 CoreSymbolSpecification 对象是否相等
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoreSymbolSpecification that = (CoreSymbolSpecification) o;
        return symbolId == that.symbolId &&
                baseCurrency == that.baseCurrency &&
                quoteCurrency == that.quoteCurrency &&
                baseScaleK == that.baseScaleK &&
                quoteScaleK == that.quoteScaleK &&
                takerFee == that.takerFee &&
                makerFee == that.makerFee &&
                marginBuy == that.marginBuy &&
                marginSell == that.marginSell &&
                type == that.type;
    }
}
