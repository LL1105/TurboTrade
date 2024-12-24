package com.exchange.core.common.api.binary;

import com.exchange.core.common.CoreSymbolSpecification;
import com.exchange.core.common.constant.BinaryCommandType;
import com.exchange.core.utils.SerializationUtils;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Collection;

/**
 * BatchAddSymbolsCommand 代表一个批量添加符号的命令。
 * 它用于将多个符号（或一个符号）添加到系统中。
 */
@AllArgsConstructor
@EqualsAndHashCode
@Getter
public final class BatchAddSymbolsCommand implements BinaryDataCommand {

    // 存储符号的映射，键为符号的 ID，值为符号的规格
    private final IntObjectHashMap<CoreSymbolSpecification> symbols;

    /**
     * 构造一个包含单个符号的批量添加命令
     *
     * @param symbol 单个符号规格
     */
    public BatchAddSymbolsCommand(final CoreSymbolSpecification symbol) {
        // 使用 IntObjectHashMap 来存储符号，符号的 ID 是键，符号本身是值
        symbols = IntObjectHashMap.newWithKeysValues(symbol.symbolId, symbol);
    }

    /**
     * 构造一个包含多个符号的批量添加命令
     *
     * @param collection 符号规格的集合
     */
    public BatchAddSymbolsCommand(final Collection<CoreSymbolSpecification> collection) {
        symbols = new IntObjectHashMap<>(collection.size());
        collection.forEach(s -> symbols.put(s.symbolId, s));  // 将每个符号的 ID 和符号规格添加到映射中
    }

    /**
     * 通过字节流反序列化构造命令
     *
     * @param bytes 反序列化的字节流
     */
    public BatchAddSymbolsCommand(final BytesIn bytes) {
        // 从字节流中读取符号的映射
        symbols = SerializationUtils.readIntHashMap(bytes, CoreSymbolSpecification::new);
    }

    /**
     * 将当前对象序列化成字节流
     *
     * @param bytes 输出字节流
     */
    @Override
    public void writeMarshallable(BytesOut bytes) {
        // 使用工具类将符号的映射序列化到字节流中
        SerializationUtils.marshallIntHashMap(symbols, bytes);
    }

    /**
     * 获取当前命令的二进制命令类型代码
     *
     * @return 返回命令类型代码
     */
    @Override
    public int getBinaryCommandTypeCode() {
        // 返回命令类型的代码，表示这是一个批量添加符号的命令
        return BinaryCommandType.ADD_SYMBOLS.getCode();
    }
}
