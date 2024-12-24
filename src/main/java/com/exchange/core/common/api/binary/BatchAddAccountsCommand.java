package com.exchange.core.common.api.binary;

import com.exchange.core.common.constant.BinaryCommandType;
import com.exchange.core.utils.SerializationUtils;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

/**
 * 批量添加账户命令类
 * 这个命令包含了一组用户及其账户信息
 */
@AllArgsConstructor
@EqualsAndHashCode
@Getter
public final class BatchAddAccountsCommand implements BinaryDataCommand {

    // 存储用户信息的映射，用户 ID -> (账户 ID -> 账户余额)
    private final LongObjectHashMap<IntLongHashMap> users;

    /**
     * 从二进制数据构造一个 BatchAddAccountsCommand 对象
     * 
     * @param bytes 输入的二进制数据
     */
    public BatchAddAccountsCommand(final BytesIn bytes) {
        // 反序列化二进制数据为 users 映射
        users = SerializationUtils.readLongHashMap(bytes, c -> SerializationUtils.readIntLongHashMap(bytes));
    }

    /**
     * 将 BatchAddAccountsCommand 对象写入二进制数据
     * 
     * @param bytes 输出的二进制数据
     */
    @Override
    public void writeMarshallable(BytesOut bytes) {
        // 将 users 映射序列化为二进制数据
        SerializationUtils.marshallLongHashMap(users, SerializationUtils::marshallIntLongHashMap, bytes);
    }

    /**
     * 获取该命令的二进制命令类型代码
     * 
     * @return 二进制命令类型代码
     */
    @Override
    public int getBinaryCommandTypeCode() {
        return BinaryCommandType.ADD_ACCOUNTS.getCode();  // 返回对应的命令类型代码 ADD_ACCOUNTS (1002)
    }
}
