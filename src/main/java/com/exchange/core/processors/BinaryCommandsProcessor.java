package com.exchange.core.processors;

import com.exchange.core.ExchangeApi;
import com.exchange.core.common.MatcherTradeEvent;
import com.exchange.core.common.StateHash;
import com.exchange.core.common.api.binary.BinaryDataCommand;
import com.exchange.core.common.api.reports.ReportQueriesHandler;
import com.exchange.core.common.api.reports.ReportQuery;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.config.ReportsQueriesConfiguration;
import com.exchange.core.common.constant.CommandResultCode;
import com.exchange.core.common.constant.OrderCommandType;
import com.exchange.core.orderbook.OrderBookEventsHelper;
import com.exchange.core.utils.HashingUtils;
import com.exchange.core.utils.SerializationUtils;
import com.exchange.core.utils.UnsafeUtils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.*;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * 二进制命令处理器，具有状态管理功能
 * <p>
 * 该类拥有接收数据的缓冲区，可以以任意顺序和重复接收事件，支持至少一次交付语义。
 */
@Slf4j
public final class BinaryCommandsProcessor implements WriteBytesMarshallable, StateHash {

    // TODO 连接对象池

    // 存储传输记录的映射，键为 transactionId，值为 TransferRecord（使用 long 数组和位集合存储数据）
    private final LongObjectHashMap<TransferRecord> incomingData;

    // 完整消息处理器，用于处理接收到的二进制数据命令
    private final Consumer<BinaryDataCommand> completeMessagesHandler;

    // 用于处理报告查询的处理器
    private final ReportQueriesHandler reportQueriesHandler;

    // 订单簿事件帮助器，用于生成与订单簿相关的事件
    private final OrderBookEventsHelper eventsHelper;

    // 报告查询配置
    private final ReportsQueriesConfiguration queriesConfiguration;

    // 分区标识符（可能用于分区或分类）
    private final int section;

    /**
     * 构造方法，初始化 BinaryCommandsProcessor。
     *
     * @param completeMessagesHandler  完整消息处理器，用于处理二进制数据命令
     * @param reportQueriesHandler    用于处理报告查询的处理器
     * @param sharedPool              共享池，用于创建事件链
     * @param queriesConfiguration    报告查询的配置
     * @param section                 分区标识符
     */
    public BinaryCommandsProcessor(final Consumer<BinaryDataCommand> completeMessagesHandler,
                                   final ReportQueriesHandler reportQueriesHandler,
                                   final SharedPool sharedPool,
                                   final ReportsQueriesConfiguration queriesConfiguration,
                                   final int section) {
        this.completeMessagesHandler = completeMessagesHandler;
        this.reportQueriesHandler = reportQueriesHandler;
        this.incomingData = new LongObjectHashMap<>();
        this.eventsHelper = new OrderBookEventsHelper(sharedPool::getChain);
        this.queriesConfiguration = queriesConfiguration;
        this.section = section;
    }

    /**
     * 另一个构造方法，允许使用 BytesIn 数据流初始化 BinaryCommandsProcessor。
     *
     * @param completeMessagesHandler  完整消息处理器
     * @param reportQueriesHandler    用于处理报告查询的处理器
     * @param sharedPool              共享池，用于创建事件链
     * @param queriesConfiguration    报告查询的配置
     * @param bytesIn                 输入数据流
     * @param section                 分区标识符
     */
    public BinaryCommandsProcessor(final Consumer<BinaryDataCommand> completeMessagesHandler,
                                   final ReportQueriesHandler reportQueriesHandler,
                                   final SharedPool sharedPool,
                                   final ReportsQueriesConfiguration queriesConfiguration,
                                   final BytesIn bytesIn,
                                   int section) {
        this.completeMessagesHandler = completeMessagesHandler;
        this.reportQueriesHandler = reportQueriesHandler;
        this.incomingData = SerializationUtils.readLongHashMap(bytesIn, b -> new TransferRecord(bytesIn));
        this.eventsHelper = new OrderBookEventsHelper(sharedPool::getChain);
        this.section = section;
        this.queriesConfiguration = queriesConfiguration;
    }

    /**
     * 接收一个二进制数据帧并处理。
     *
     * @param cmd 处理的命令
     * @return 命令的处理结果
     */
    public CommandResultCode acceptBinaryFrame(OrderCommand cmd) {

        final int transferId = cmd.userCookie;

        // 获取或创建一个 TransferRecord
        final TransferRecord record = incomingData.getIfAbsentPut(
                transferId,
                () -> {
                    final int bytesLength = (int) (cmd.orderId >> 32) & 0x7FFF_FFFF;
                    final int longArraySize = SerializationUtils.requiredLongArraySize(bytesLength, ExchangeApi.LONGS_PER_MESSAGE);
                    return new TransferRecord(longArraySize);
                });

        // 将命令的数据添加到 TransferRecord
        record.addWord(cmd.orderId);
        record.addWord(cmd.price);
        record.addWord(cmd.reserveBidPrice);
        record.addWord(cmd.size);
        record.addWord(cmd.uid);

        // 如果是最后一帧，表示所有数据已经接收完毕
        if (cmd.symbol == -1) {
            // 移除该 transferId 的记录，表示数据已处理完毕
            incomingData.removeKey(transferId);

            final BytesIn bytesIn = SerializationUtils.longsLz4ToWire(record.dataArray, record.wordsTransfered).bytes();

            if (cmd.command == OrderCommandType.BINARY_DATA_QUERY) {
                // 如果是查询命令，反序列化并处理报告查询
                deserializeQuery(bytesIn)
                        .flatMap(reportQueriesHandler::handleReport)
                        .ifPresent(res -> {
                            final NativeBytes<Void> bytes = Bytes.allocateElasticDirect(128);
                            res.writeMarshallable(bytes);
                            final MatcherTradeEvent binaryEventsChain = eventsHelper.createBinaryEventsChain(cmd.timestamp, section, bytes);
                            UnsafeUtils.appendEventsVolatile(cmd, binaryEventsChain);
                        });

            } else if (cmd.command == OrderCommandType.BINARY_DATA_COMMAND) {
                // 如果是数据命令，反序列化并调用处理器
                final BinaryDataCommand binaryDataCommand = deserializeBinaryCommand(bytesIn);
                completeMessagesHandler.accept(binaryDataCommand);

            } else {
                throw new IllegalStateException("Unexpected command type");
            }

            return CommandResultCode.SUCCESS;
        } else {
            // 如果不是最后一帧，表示数据仍在接收中
            return CommandResultCode.ACCEPTED;
        }
    }

    /**
     * 反序列化二进制数据命令。
     *
     * @param bytesIn 输入数据流
     * @return 反序列化后的 BinaryDataCommand 对象
     */
    private BinaryDataCommand deserializeBinaryCommand(BytesIn bytesIn) {
        final int classCode = bytesIn.readInt();

        final Constructor<? extends BinaryDataCommand> constructor = queriesConfiguration.getBinaryCommandConstructors().get(classCode);
        if (constructor == null) {
            throw new IllegalStateException("未知的二进制数据命令类代码: " + classCode);
        }

        try {
            return constructor.newInstance(bytesIn);

        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException ex) {
            throw new IllegalStateException("反序列化二进制数据命令失败: " + constructor.getDeclaringClass().getSimpleName(), ex);
        }
    }

    /**
     * 反序列化报告查询。
     *
     * @param bytesIn 输入数据流
     * @return 反序列化后的 ReportQuery 对象
     */
    private Optional<ReportQuery<?>> deserializeQuery(BytesIn bytesIn) {
        final int classCode = bytesIn.readInt();

        final Constructor<? extends ReportQuery<?>> constructor = queriesConfiguration.getReportConstructors().get(classCode);
        if (constructor == null) {
            log.error("未知的报告查询类代码: {}", classCode);
            return Optional.empty();
        }

        try {
            return Optional.of(constructor.newInstance(bytesIn));

        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException ex) {
            log.error("反序列化报告查询失败: {} 错误信息: {}", constructor.getDeclaringClass().getSimpleName(), ex.getMessage());
            return Optional.empty();
        }
    }

    /**
     * 序列化对象为二进制数据。
     *
     * @param data       要序列化的数据对象
     * @param objectType 对象类型
     * @return 序列化后的二进制数据
     */
    public static NativeBytes<Void> serializeObject(WriteBytesMarshallable data, int objectType) {
        final NativeBytes<Void> bytes = Bytes.allocateElasticDirect(128);
        bytes.writeInt(objectType);
        data.writeMarshallable(bytes);
        return bytes;
    }

    /**
     * 重置处理器状态，清空接收到的数据。
     */
    public void reset() {
        incomingData.clear();
    }

    /**
     * 序列化当前状态到字节流。
     *
     * @param bytes 输出字节流
     */
    @Override
    public void writeMarshallable(BytesOut bytes) {
        // 序列化 incomingData
        SerializationUtils.marshallLongHashMap(incomingData, bytes);
    }

    /**
     * 计算当前状态的哈希值。
     *
     * @return 当前状态的哈希值
     */
    @Override
    public int stateHash() {
        return HashingUtils.stateHash(incomingData);
    }

    /**
     * 内部类，表示一个传输记录。
     */
    private static class TransferRecord implements WriteBytesMarshallable, StateHash {

        // 存储传输数据的数组
        private long[] dataArray;
        // 已传输的数据单元数
        private int wordsTransfered;

        /**
         * 构造方法，初始化 TransferRecord。
         *
         * @param expectedLength 预期的数组长度
         */
        public TransferRecord(int expectedLength) {
            this.wordsTransfered = 0;
            this.dataArray = new long[expectedLength];
        }

        /**
         * 构造方法，通过 BytesIn 数据流反序列化 TransferRecord。
         *
         * @param bytes 输入数据流
         */
        public TransferRecord(BytesIn bytes) {
            wordsTransfered = bytes.readInt();
            this.dataArray = SerializationUtils.readLongArray(bytes);
        }

        /**
         * 向记录中添加一个数据单元。
         *
         * @param word 数据单元
         */
        public void addWord(long word) {
            if (wordsTransfered == dataArray.length) {
                // 如果数据数组已满，扩展数组
                log.warn("扩展传输缓冲区大小至 {}", dataArray.length * 2);
                long[] newArray = new long[dataArray.length * 2];
                System.arraycopy(dataArray, 0, newArray, 0, dataArray.length);
                dataArray = newArray;
            }

            dataArray[wordsTransfered++] = word;
        }

        /**
         * 序列化 TransferRecord 为字节流。
         *
         * @param bytes 输出字节流
         */
        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeInt(wordsTransfered);
            SerializationUtils.marshallLongArray(dataArray, bytes);
        }

        /**
         * 计算当前传输记录的哈希值。
         *
         * @return 传输记录的哈希值
         */
        @Override
        public int stateHash() {
            return Objects.hash(Arrays.hashCode(dataArray), wordsTransfered);
        }
    }

}
