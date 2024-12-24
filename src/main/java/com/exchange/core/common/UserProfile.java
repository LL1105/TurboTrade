package com.exchange.core.common;

import com.exchange.core.common.constant.UserStatus;
import com.exchange.core.utils.HashingUtils;
import com.exchange.core.utils.SerializationUtils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Objects;

/**
 * 用户资料类
 * <p>
 * 该类包含与用户相关的资料，包括其持仓记录、账户余额、调整计数器和当前状态。
 */
@Slf4j
public final class UserProfile implements WriteBytesMarshallable, StateHash {

    // 用户的唯一ID
    public final long uid;

    // symbol -> margin position records（保证金持仓记录）
    // TODO: 延迟初始化（仅在允许保证金交易时需要）
    public final IntObjectHashMap<SymbolPositionRecord> positions;

    // 防止重复调整的计数器
    public long adjustmentsCounter;

    // currency -> balance（账户余额，币种 -> 余额）
    public final IntLongHashMap accounts;

    // 用户当前的状态
    public UserStatus userStatus;

    // 构造函数：创建一个新的用户资料
    public UserProfile(long uid, UserStatus userStatus) {
        //log.debug("New {}", uid);
        this.uid = uid;
        this.positions = new IntObjectHashMap<>();
        this.adjustmentsCounter = 0L;
        this.accounts = new IntLongHashMap();
        this.userStatus = userStatus;
    }

    // 从字节流中构造 UserProfile
    public UserProfile(BytesIn bytesIn) {

        // 读取uid
        this.uid = bytesIn.readLong();

        // 读取持仓记录
        this.positions = SerializationUtils.readIntHashMap(bytesIn, b -> new SymbolPositionRecord(uid, b));

        // 读取调整计数器
        this.adjustmentsCounter = bytesIn.readLong();

        // 读取账户余额
        this.accounts = SerializationUtils.readIntLongHashMap(bytesIn);

        // 读取用户状态
        this.userStatus = UserStatus.of(bytesIn.readByte());
    }

    /**
     * 根据symbol获取持仓记录，如果没有找到则抛出异常
     *
     * @param symbol 标的物
     * @return 持仓记录
     * @throws IllegalStateException 如果未找到该symbol的持仓记录
     */
    public SymbolPositionRecord getPositionRecordOrThrowEx(int symbol) {
        final SymbolPositionRecord record = positions.get(symbol);
        if (record == null) {
            throw new IllegalStateException("未找到symbol " + symbol + " 的持仓记录");
        }
        return record;
    }

    /**
     * 将当前用户资料写入字节流
     *
     * @param bytes 字节输出流
     */
    @Override
    public void writeMarshallable(BytesOut bytes) {

        // 写入uid
        bytes.writeLong(uid);

        // 写入持仓记录
        SerializationUtils.marshallIntHashMap(positions, bytes);

        // 写入调整计数器
        bytes.writeLong(adjustmentsCounter);

        // 写入账户余额
        SerializationUtils.marshallIntLongHashMap(accounts, bytes);

        // 写入用户状态
        bytes.writeByte(userStatus.getCode());
    }

    /**
     * 返回用户资料的字符串表示
     *
     * @return 用户资料的字符串表示
     */
    @Override
    public String toString() {
        return "UserProfile{" +
                "uid=" + uid +
                ", positions=" + positions.size() +
                ", accounts=" + accounts +
                ", adjustmentsCounter=" + adjustmentsCounter +
                ", userStatus=" + userStatus +
                '}';
    }

    /**
     * 计算用户资料的状态哈希值，用于状态管理或持久化
     *
     * @return 用户资料的状态哈希值
     */
    @Override
    public int stateHash() {
        return Objects.hash(
                uid,
                HashingUtils.stateHash(positions),
                adjustmentsCounter,
                accounts.hashCode(),
                userStatus.hashCode());
    }
}
