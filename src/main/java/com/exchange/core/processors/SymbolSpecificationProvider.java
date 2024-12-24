package com.exchange.core.processors;

import com.exchange.core.common.CoreSymbolSpecification;
import com.exchange.core.common.StateHash;
import com.exchange.core.utils.HashingUtils;
import com.exchange.core.utils.SerializationUtils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Objects;

@Slf4j
public final class SymbolSpecificationProvider implements WriteBytesMarshallable, StateHash {

    // symbol->specs: 存储符号 ID 与对应符号规格（CoreSymbolSpecification）的映射
    private final IntObjectHashMap<CoreSymbolSpecification> symbolSpecs;

    // 默认构造方法，初始化符号规格存储容器
    public SymbolSpecificationProvider() {
        this.symbolSpecs = new IntObjectHashMap<>();
    }

    // 反序列化构造方法，从字节流中读取符号规格信息
    public SymbolSpecificationProvider(BytesIn bytes) {
        this.symbolSpecs = SerializationUtils.readIntHashMap(bytes, CoreSymbolSpecification::new);
    }

    /**
     * 添加符号规格
     * 如果符号已经存在，返回 false；否则，注册符号并返回 true
     * 
     * @param symbolSpecification 符号规格
     * @return 如果添加成功返回 true，否则返回 false
     */
    public boolean addSymbol(final CoreSymbolSpecification symbolSpecification) {
        if (getSymbolSpecification(symbolSpecification.symbolId) != null) {
            return false; // 符号已存在，不能重复添加
        } else {
            registerSymbol(symbolSpecification.symbolId, symbolSpecification); // 注册新符号
            return true;
        }
    }

    /**
     * 根据符号 ID 获取符号规格
     * 
     * @param symbol 符号 ID
     * @return 返回符号规格，若不存在则返回 null
     */
    public CoreSymbolSpecification getSymbolSpecification(int symbol) {
        return symbolSpecs.get(symbol);
    }

    /**
     * 注册符号规格
     * 
     * @param symbol 符号 ID
     * @param spec   符号规格
     */
    public void registerSymbol(int symbol, CoreSymbolSpecification spec) {
        symbolSpecs.put(symbol, spec);
    }

    /**
     * 重置符号规格提供者，清空所有符号规格
     */
    public void reset() {
        symbolSpecs.clear(); // 清空所有符号规格
    }

    /**
     * 将当前对象的数据序列化到字节流
     * 
     * @param bytes 输出的字节流对象
     */
    @Override
    public void writeMarshallable(BytesOut bytes) {
        // 将 symbolSpecs 进行序列化
        SerializationUtils.marshallIntHashMap(symbolSpecs, bytes);
    }

    /**
     * 计算并返回当前对象的状态哈希值
     * 
     * @return 返回当前对象的哈希值
     */
    @Override
    public int stateHash() {
        return Objects.hash(HashingUtils.stateHash(symbolSpecs)); // 计算符号规格的哈希值
    }

}
