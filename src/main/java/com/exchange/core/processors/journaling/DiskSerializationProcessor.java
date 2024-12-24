package com.exchange.core.processors.journaling;

import com.exchange.core.ExchangeApi;
import com.exchange.core.common.command.OrderCommand;
import com.exchange.core.common.config.ExchangeConfiguration;
import com.exchange.core.common.config.InitialStateConfiguration;
import com.exchange.core.common.config.PerformanceConfiguration;
import com.exchange.core.common.constant.BalanceAdjustmentType;
import com.exchange.core.common.constant.OrderAction;
import com.exchange.core.common.constant.OrderCommandType;
import com.exchange.core.common.constant.OrderType;
import lombok.extern.slf4j.Slf4j;
import net.jpountz.lz4.*;
import net.jpountz.xxhash.XXHashFactory;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.InputStreamToWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.agrona.collections.MutableLong;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


@Slf4j
public final class DiskSerializationProcessor implements ISerializationProcessor {

    private final int journalBufferFlushTrigger;
    private final long journalFileMaxSize;
    private final int journalBatchCompressThreshold;

    private final String exchangeId; // TODO validate
    private final Path folder;

    private final long baseSeq;

    private final ByteBuffer journalWriteBuffer;
    private final ByteBuffer lz4WriteBuffer;

    // TODO configurable
    private final LZ4Compressor lz4CompressorSnapshot;
    private final LZ4Compressor lz4CompressorJournal;
    private final LZ4SafeDecompressor lz4SafeDecompressor = LZ4Factory.fastestInstance().safeDecompressor();

    private ConcurrentSkipListMap<Long, SnapshotDescriptor> snapshotsIndex;

    private SnapshotDescriptor lastSnapshotDescriptor;
    private JournalDescriptor lastJournalDescriptor;


    private long baseSnapshotId;

    private long enableJournalAfterSeq = -1;

    private RandomAccessFile raf;
    private FileChannel channel;

    private int filesCounter = 0;

    private long writtenBytes = 0;

    private static final int MAX_COMMAND_SIZE_BYTES = 256;

//    private List<Integer> batchSizes = new ArrayList<>(100000);
//    final SingleWriterRecorder hdrRecorderRaw = new SingleWriterRecorder(Integer.MAX_VALUE, 2);
//    final SingleWriterRecorder hdrRecorderLz4 = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

    public DiskSerializationProcessor(ExchangeConfiguration exchangeConfig,
                                      DiskSerializationProcessorConfiguration diskConfig) {
        // 获取初始状态配置
        final InitialStateConfiguration initStateCfg = exchangeConfig.getInitStateCfg();

        // 从初始配置中获取交易所ID、存储文件夹路径、快照ID、基准序列等信息
        this.exchangeId = initStateCfg.getExchangeId();
        this.folder = Paths.get(diskConfig.getStorageFolder());  // 存储文件夹路径
        this.baseSnapshotId = initStateCfg.getSnapshotId();      // 基础快照ID
        this.baseSeq = initStateCfg.getSnapshotBaseSeq();         // 基础序列号

        // 获取性能配置
        final PerformanceConfiguration perfCfg = exchangeConfig.getPerformanceCfg();

        // 初始化日志和快照描述符
        this.lastJournalDescriptor = null; // 最后一条日志描述符（初始化时为null）
        this.lastSnapshotDescriptor = SnapshotDescriptor.createEmpty(perfCfg.getMatchingEnginesNum(), perfCfg.getRiskEnginesNum()); // 创建一个空的快照描述符

        // 从磁盘配置中获取日志缓冲区大小
        final int journalBufferSize = diskConfig.getJournalBufferSize();

        // 设置日志文件的最大大小（减去日志缓冲区大小）
        this.journalFileMaxSize = diskConfig.getJournalFileMaxSize() - journalBufferSize;

        // 日志缓冲区触发写入的大小（当缓冲区达到此大小时，触发写入操作）
        this.journalBufferFlushTrigger = journalBufferSize - MAX_COMMAND_SIZE_BYTES; // 保证缓冲区大小大于单个命令的最大字节数

        // 设置日志批处理压缩阈值（当达到此大小时，进行批量压缩）
        this.journalBatchCompressThreshold = diskConfig.getJournalBatchCompressThreshold();

        // 分配直接内存（直接内存是更高效的 I/O 操作内存）
        this.journalWriteBuffer = ByteBuffer.allocateDirect(journalBufferSize);

        // 初始化日志压缩器和快照压缩器
        this.lz4CompressorJournal = diskConfig.getJournalLz4CompressorFactory().get(); // 获取日志压缩器
        this.lz4CompressorSnapshot = diskConfig.getSnapshotLz4CompressorFactory().get(); // 获取快照压缩器

        // 计算日志压缩后的最大块长度
        final int maxCompressedBlockLength = lz4CompressorJournal.maxCompressedLength(journalBufferSize);
        this.lz4WriteBuffer = ByteBuffer.allocate(maxCompressedBlockLength); // 为压缩数据分配缓冲区
    }

    @Override
    public boolean storeData(long snapshotId,
                             long seq,
                             long timestampNs,
                             SerializedModuleType type,
                             int instanceId,
                             WriteBytesMarshallable obj) {
        // 1. 生成存储快照的文件路径
        final Path path = resolveSnapshotPath(snapshotId, type, instanceId);

        log.debug("Writing state into file {} ...", path);

        // 2. 使用输出流和压缩工具将对象序列化并写入快照文件
        try (final OutputStream os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW);  // 创建新文件输出流
             final OutputStream bos = new BufferedOutputStream(os);  // 使用缓冲流提高写入效率
             final LZ4FrameOutputStream lz4os = new LZ4FrameOutputStream(  // 使用LZ4压缩输出流进行数据压缩
                     bos,
                     LZ4FrameOutputStream.BLOCKSIZE.SIZE_4MB,  // 设置压缩块大小为4MB
                     -1,  // 最大压缩块数（不限制）
                     lz4CompressorSnapshot,  // 使用自定义的LZ4压缩器
                     XXHashFactory.fastestInstance().hash32(),  // 设置hash32用于校验
                     LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);  // 设置块间独立性
             final WireToOutputStream2 wireToOutputStream = new WireToOutputStream2(WireType.RAW, lz4os)) {  // 创建Wire到输出流的适配器

            final Wire wire = wireToOutputStream.getWire();  // 获取Wire实例，Wire是一个序列化工具

            // 3. 序列化对象并将其写入压缩流
            wire.writeBytes(obj);  // 将 WriteBytesMarshallable 对象序列化到 Wire 中

            log.debug("done serializing, flushing {} ...", path);

            // 4. 刷新数据，确保所有数据已写入文件
            wireToOutputStream.flush();

            log.debug("completed {}", path);  // 完成序列化并成功写入快照文件

        } catch (final IOException ex) {  // 捕获并处理可能的 I/O 异常
            log.error("Can not write snapshot file: ", ex);
            return false;  // 写入失败，返回 false
        }

        // 5. 同步写入主日志文件（记录元数据）
        synchronized (this) {  // 确保在多线程环境下对主日志文件的访问是线程安全的
            try (final OutputStream os = Files.newOutputStream(resolveMainLogPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {  // 打开主日志文件，创建并追加内容
                // 写入元数据到主日志文件，包括时间戳、序列号、快照ID、类型和实例ID
                os.write((System.currentTimeMillis() + " seq=" + seq + " timestampNs=" + timestampNs + " snapshotId=" + snapshotId + " type=" + type.code + " instance=" + instanceId + "\n").getBytes());
            } catch (final IOException ex) {  // 捕获写入主日志文件时的异常
                log.error("Can not write main log file: ", ex);
                return false;  // 写入主日志文件失败，返回 false
            }
        }

        return true;  // 成功存储数据并记录日志，返回 true
    }

    @Override
    public <T> T loadData(long snapshotId,
                          SerializedModuleType type,
                          int instanceId,
                          Function<BytesIn, T> initFunc) {

        // 1. 生成快照文件的路径
        final Path path = resolveSnapshotPath(snapshotId, type, instanceId);

        log.debug("Loading state from {}", path);  // 输出调试日志，标明正在加载的文件路径

        try (final InputStream is = Files.newInputStream(path, StandardOpenOption.READ);  // 打开快照文件的输入流进行读取
             final InputStream bis = new BufferedInputStream(is);  // 使用缓冲输入流以提高读取效率
             final LZ4FrameInputStream lz4is = new LZ4FrameInputStream(bis)) {  // 使用 LZ4 解压缩输入流来解压文件内容

            // TODO: 改进读取算法
            // 2. 将输入流转换为 Wire 对象进行数据反序列化
            final InputStreamToWire inputStreamToWire = new InputStreamToWire(WireType.RAW, lz4is);  // 使用 WireType.RAW 读取原始数据流
            final Wire wire = inputStreamToWire.readOne();  // 从输入流中读取数据，并将其解析为 Wire 对象

            log.debug("start de-serializing...");  // 输出日志，标明开始反序列化

            // 3. 使用 AtomicReference 存储反序列化后的数据
            AtomicReference<T> ref = new AtomicReference<>();  // 使用原子引用存储反序列化的数据
            wire.readBytes(bytes -> ref.set(initFunc.apply(bytes)));  // 读取 Wire 数据并通过 initFunc 函数进行转换

            return ref.get();  // 返回反序列化后的对象

        } catch (final IOException ex) {  // 处理文件读取或解压缩过程中可能出现的异常
            log.error("Can not read snapshot file: ", ex);  // 记录错误日志
            throw new IllegalStateException(ex);  // 抛出异常，确保方法调用者知道出错的情况
        }
    }

    public class WireToOutputStream2 implements AutoCloseable {
        private final Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer(128 * 1024 * 1024);
        private final Wire wire;
        private final DataOutputStream dos;

        public WireToOutputStream2(WireType wireType, OutputStream os) {
            wire = wireType.apply(bytes);
            dos = new DataOutputStream(os);
        }

        public Wire getWire() {
            wire.clear();
            return wire;
        }

        public void flush() throws IOException {
            int length = Math.toIntExact(bytes.readRemaining());
            dos.writeInt(length);

            final byte[] buf = new byte[1024 * 1024];

            while (bytes.readPosition() < bytes.readLimit()) {
                int read = bytes.read(buf);
                dos.write(buf, 0, read);
            }
        }

        @Override
        public void close() {
            bytes.release();
        }
    }


    // 单线程模式
    @Override
    public void writeToJournal(OrderCommand cmd, long dSeq, boolean eob) throws IOException {

        // TODO 改进检查逻辑
        // 如果 enableJournalAfterSeq 为 -1 或者当前序列号加上基础序列号小于等于 enableJournalAfterSeq，则跳过日志写入
        if (enableJournalAfterSeq == -1 || dSeq + baseSeq <= enableJournalAfterSeq) {
            return;
        }

        // 如果当前命令序列号等于 enableJournalAfterSeq + 1，记录日志表示启用日志功能
        if (dSeq + baseSeq == enableJournalAfterSeq + 1) {
            log.info("Enabled journaling at seq = {} ({}+{})", enableJournalAfterSeq + 1, baseSeq, dSeq);
        }

        // 是否启用调试模式
        boolean debug = false;

        // log.debug("Writing {}", cmd); // 可调试日志，已注释掉

        // 获取当前命令的类型
        final OrderCommandType cmdType = cmd.command;

        // 如果命令类型是关闭信号（SHUTDOWN_SIGNAL），则刷新缓冲区并返回
        if (cmdType == OrderCommandType.SHUTDOWN_SIGNAL) {
            flushBufferSync(false, cmd.timestamp);  // 同步刷新缓冲区
            log.debug("Shutdown signal received, flushed to disk");
            return;
        }

        // 如果命令类型不是“变更操作”（mutate），跳过（只记录修改命令，查询命令不记录）
        if (!cmdType.isMutate()) {
            return;
        }

        // 如果当前通道为空，则开始一个新的文件
        if (channel == null) {
            startNewFile(cmd.timestamp);
        }

        // 获取日志写入缓冲区
        final ByteBuffer buffer = journalWriteBuffer;

        // 写入命令的必要字段
        buffer.put(cmdType.getCode()); // 命令类型，1字节
        buffer.putLong(baseSeq + dSeq); // 序列号，8字节，可以作为增量进行压缩
        buffer.putLong(cmd.timestamp); // 时间戳，8字节，可以作为增量进行压缩
        buffer.putInt(cmd.serviceFlags); // 服务标志，4字节，可以用字典进行压缩
        buffer.putLong(cmd.eventsGroup); // 事件组，8字节，可以作为增量进行压缩

        // 如果启用调试，输出日志
        if (debug)
            log.debug("LOG {} eventsGroup={} serviceFlags={}", String.format("seq=%d t=%d cmd=%X (%s) ", baseSeq + dSeq, cmd.timestamp, cmdType.getCode(), cmdType), cmd.eventsGroup, cmd.serviceFlags);

        // 根据命令类型进行不同的写入处理

        // 如果是移动订单（MOVE_ORDER），写入相关字段
        if (cmdType == OrderCommandType.MOVE_ORDER) {
            buffer.putLong(cmd.uid); // 用户ID，8字节，可以进行字典压缩
            buffer.putInt(cmd.symbol); // 交易对，4字节，可以进行字典压缩
            buffer.putLong(cmd.orderId); // 订单ID，8字节，可以作为增量进行压缩
            buffer.putLong(cmd.price); // 价格，8字节，可以作为增量进行压缩

            if (debug) log.debug("move order seq={} t={} orderId={} symbol={} uid={} price={}", baseSeq + dSeq, cmd.timestamp, cmd.orderId, cmd.symbol, cmd.uid, cmd.price);
        }
        // 如果是取消订单（CANCEL_ORDER），写入相关字段
        else if (cmdType == OrderCommandType.CANCEL_ORDER) {
            buffer.putLong(cmd.uid); // 用户ID，8字节，可以进行字典压缩
            buffer.putInt(cmd.symbol); // 交易对，4字节，可以进行字典压缩
            buffer.putLong(cmd.orderId); // 订单ID，8字节，可以作为增量进行压缩

            if (debug) log.debug("cancel order seq={} t={} orderId={} symbol={} uid={}", baseSeq + dSeq, cmd.timestamp, cmd.orderId, cmd.symbol, cmd.uid);
        }
        // 如果是减少订单（REDUCE_ORDER），写入相关字段
        else if (cmdType == OrderCommandType.REDUCE_ORDER) {
            buffer.putLong(cmd.uid); // 用户ID，8字节，可以进行字典压缩
            buffer.putInt(cmd.symbol); // 交易对，4字节，可以进行字典压缩
            buffer.putLong(cmd.orderId); // 订单ID，8字节，可以作为增量进行压缩
            buffer.putLong(cmd.size); // 数量，8字节，可以进行低值压缩

            if (debug) log.debug("reduce order seq={} t={} orderId={} symbol={} uid={} size={}", baseSeq + dSeq, cmd.timestamp, cmd.orderId, cmd.symbol, cmd.uid, cmd.size);
        }
        // 如果是下单（PLACE_ORDER），写入相关字段
        else if (cmdType == OrderCommandType.PLACE_ORDER) {
            buffer.putLong(cmd.uid); // 用户ID，8字节，可以进行字典压缩
            buffer.putInt(cmd.symbol); // 交易对，4字节，可以进行字典压缩
            buffer.putLong(cmd.orderId); // 订单ID，8字节，可以作为增量进行压缩
            buffer.putLong(cmd.price); // 价格，8字节，可以作为增量进行压缩
            buffer.putLong(cmd.reserveBidPrice); // 保留买入价格，8字节，可以进行压缩（与价格的差值或0）
            buffer.putLong(cmd.size); // 数量，8字节
            buffer.putInt(cmd.userCookie); // 用户标识，4字节，可以进行压缩

            final int actionAndType = (cmd.orderType.getCode() << 1) | cmd.action.getCode();
            byte actionAndType1 = (byte) actionAndType;
            buffer.put(actionAndType1); // 1字节，表示订单类型和操作类型的合并

            if (debug) log.debug("place order seq={} t={} orderId={} symbol={} uid={} price={} reserveBidPrice={} size={} userCookie={} {}/{} actionAndType={}",
                    baseSeq + dSeq, cmd.timestamp, cmd.orderId, cmd.symbol, cmd.uid, cmd.price, cmd.reserveBidPrice, cmd.size, cmd.userCookie, cmd.action, cmd.orderType, actionAndType1);
        }
        // 如果是余额调整（BALANCE_ADJUSTMENT），写入相关字段
        else if (cmdType == OrderCommandType.BALANCE_ADJUSTMENT) {
            buffer.putLong(cmd.uid); // 用户ID，8字节，可以进行字典压缩
            buffer.putInt(cmd.symbol); // 交易对，4字节，可以进行字典压缩（货币类型）
            buffer.putLong(cmd.orderId); // 订单ID，8字节，可以作为增量进行压缩（交易ID）
            buffer.putLong(cmd.price); // 价格，8字节，可以进行低值压缩（金额）
            buffer.put(cmd.orderType.getCode()); // 1字节，表示调整或暂停操作
        }
        // 如果是用户相关命令（ADD_USER、SUSPEND_USER、RESUME_USER），写入用户ID
        else if (cmdType == OrderCommandType.ADD_USER ||
                cmdType == OrderCommandType.SUSPEND_USER ||
                cmdType == OrderCommandType.RESUME_USER) {
            buffer.putLong(cmd.uid); // 用户ID，8字节，可以作为增量进行压缩
        }
        // 如果是二进制数据命令（BINARY_DATA_COMMAND），写入相关数据
        else if (cmdType == OrderCommandType.BINARY_DATA_COMMAND) {
            buffer.put((byte) cmd.symbol); // 1字节（0 或 -1）
            buffer.putLong(cmd.orderId); // 8字节 word0
            buffer.putLong(cmd.price); // 8字节 word1
            buffer.putLong(cmd.reserveBidPrice); // 8字节 word2
            buffer.putLong(cmd.size); // 8字节 word3
            buffer.putLong(cmd.uid); // 8字节 word4
        }

        // 如果是风险状态持久化命令，注册快照变更并刷新缓冲区
        if (cmdType == OrderCommandType.PERSIST_STATE_RISK) {
            // 注册快照变更
            registerNextSnapshot(cmd.orderId, baseSeq + dSeq, cmd.timestamp);

            // 开始新的文件
            baseSnapshotId = cmd.orderId;
            filesCounter = 0;

            // 同步刷新缓冲区
            flushBufferSync(true, cmd.timestamp);
        }
        // 如果是重置命令，则强制开始新日志文件
        else if (cmdType == OrderCommandType.RESET) {
            flushBufferSync(true, cmd.timestamp); // 同步刷新缓冲区
        }
        // 如果是批处理结束或者缓冲区已满，刷新缓冲区
        else if (eob || buffer.position() >= journalBufferFlushTrigger) {
            flushBufferSync(false, cmd.timestamp); // 同步刷新缓冲区
        }
    }

    @Override
    public void enableJournaling(long afterSeq, ExchangeApi api) {
        enableJournalAfterSeq = afterSeq;
        api.groupingControl(0, 1);
    }

    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a)
            sb.append(String.format("%02x ", b));
        return sb.toString();
    }

    @Override
    public NavigableMap<Long, SnapshotDescriptor> findAllSnapshotPoints() {
        return snapshotsIndex;
    }

    @Override
    public void replayJournalStep(long snapshotId, long seqFrom, long seqTo, ExchangeApi exchangeApi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long replayJournalFull(InitialStateConfiguration initialCfg, ExchangeApi api) {
        // 如果日志的时间戳为0，说明不需要回放日志，直接返回当前序列号
        if (initialCfg.getJournalTimestampNs() == 0) {
            log.debug("No need to replay journal, returning baseSeq={}", baseSeq);
            return baseSeq;  // 返回基础序列号，表示当前已经是最新的状态
        }

        log.debug("Replaying journal...");

        // 控制分组的行为
        api.groupingControl(0, 0);

        // 使用 MutableLong 类型来保存最后读取到的序列号
        final MutableLong lastSeq = new MutableLong();

        // 分区计数器，从1开始，表示当前正在处理的日志分区编号
        int partitionCounter = 1;

        // 进入无限循环，处理多个日志文件分区
        while (true) {
            // 获取当前分区日志文件的路径
            final Path path = resolveJournalPath(partitionCounter, initialCfg.getSnapshotId());

            log.debug("Reading journal file: {}", path.toFile());
            try (
                    // 尝试打开文件流
                    final FileInputStream fis = new FileInputStream(path.toFile());
                    final BufferedInputStream bis = new BufferedInputStream(fis);
                    final DataInputStream dis = new DataInputStream(bis)
            ) {
                // 读取日志中的命令，并更新 lastSeq 序列号
                readCommands(dis, api, lastSeq, false);
                partitionCounter++;  // 处理下一个分区
                log.debug("File end reached, try next partition {}...", partitionCounter);

            } catch (FileNotFoundException ex) {
                // 如果文件未找到，表示日志回放到最后，返回最后的序列号
                log.debug("return lastSeq={}, file not found: {}", lastSeq, ex.getMessage());
                return lastSeq.value;  // 返回最后处理的序列号

            } catch (IOException ex) {
                // 如果发生其他 I/O 错误，表示文件读取异常，尝试下一个分区
                partitionCounter++;
                log.debug("File end reached through exception");
            }
        }
    }

    private void readCommands(final DataInputStream jr,
                              final ExchangeApi api,
                              final MutableLong lastSeq,
                              boolean insideCompressedBlock) throws IOException {

        // 循环读取所有命令，直到文件结束
        while (jr.available() != 0) {

            // 是否启用调试模式，调试模式下打印更多日志
            boolean debug = false; // 可以根据需要设置为 true

            // 读取一个命令字节（cmd），表示命令的类型
            final byte cmd = jr.readByte();

            // 如果是调试模式，打印当前序列号
            if (debug) log.debug("COMPR STEP lastSeq={} ", lastSeq);

            // 如果命令是压缩命令
            if (cmd == OrderCommandType.RESERVED_COMPRESSED.getCode()) {

                // 如果已经在压缩块中，则抛出异常，防止递归压缩
                if (insideCompressedBlock) {
                    throw new IllegalStateException("Recursive compression block (data corrupted)");
                }

                // 读取压缩块的大小和原始大小
                int size = jr.readInt();
                int origSize = jr.readInt();

                // 检查压缩块的大小是否合理
                if (size > 1000000) {
                    throw new IllegalStateException("Bad compressed block size = " + size + "(data corrupted)");
                }

                if (origSize > 1000000) {
                    throw new IllegalStateException("Bad original block size = " + size + "(data corrupted)");
                }

                // 读取压缩的数据
                byte[] compressedArray = new byte[size];
                int read = jr.read(compressedArray);
                if (read < size) {
                    throw new IOException("Can not read full block (only " + read + " bytes, not all " + size + " bytes) ");
                }

                // 解压缩数据
                byte[] originalArray = lz4SafeDecompressor.decompress(compressedArray, origSize);

                // 递归读取解压后的命令
                try (final ByteArrayInputStream bis = new ByteArrayInputStream(originalArray);
                     final DataInputStream dis = new DataInputStream(bis)) {

                    readCommands(dis, api, lastSeq, true);
                }

            } else {

                // 读取命令的基本数据
                final long seq = jr.readLong();           // 序列号
                final long timestampNs = jr.readLong();   // 时间戳
                final int serviceFlags = jr.readInt();    // 服务标志
                final long eventsGroup = jr.readLong();   // 事件组
                final OrderCommandType cmdType = OrderCommandType.fromCode(cmd); // 获取命令类型

                // 检查是否存在序列号的间隔
                if (seq != lastSeq.value + 1) {
                    log.warn("Sequence gap {}->{} ({})", lastSeq, seq, seq - lastSeq.value);
                }

                // 更新最后一个序列号
                lastSeq.value = seq;

                // 如果是调试模式，打印命令的详细信息
                if (debug) log.debug("eventsGroup={} serviceFlags={} cmdType={}", eventsGroup, serviceFlags, cmdType);

                // 根据不同的命令类型执行不同的操作
                if (cmdType == OrderCommandType.MOVE_ORDER) {

                    // 移动订单命令，读取相关数据并执行
                    final long uid = jr.readLong();
                    final int symbol = jr.readInt();
                    final long orderId = jr.readLong();
                    final long price = jr.readLong();

                    if (debug) log.debug("move order seq={} t={} orderId={} symbol={} uid={} price={}", lastSeq, timestampNs, orderId, symbol, uid, price);

                    api.moveOrder(serviceFlags, eventsGroup, timestampNs, price, orderId, symbol, uid);

                } else if (cmdType == OrderCommandType.CANCEL_ORDER) {

                    // 取消订单命令，读取相关数据并执行
                    final long uid = jr.readLong();
                    final int symbol = jr.readInt();
                    final long orderId = jr.readLong();

                    if (debug) log.debug("cancel order seq={} t={} orderId={} symbol={} uid={}", lastSeq, timestampNs, orderId, symbol, uid);

                    api.cancelOrder(serviceFlags, eventsGroup, timestampNs, orderId, symbol, uid);

                } else if (cmdType == OrderCommandType.REDUCE_ORDER) {

                    // 减少订单命令，读取相关数据并执行
                    final long uid = jr.readLong();
                    final int symbol = jr.readInt();
                    final long orderId = jr.readLong();
                    final long reduceSize = jr.readLong();

                    if (debug) log.debug("reduce order seq={} t={} orderId={} symbol={} uid={} reduceSize={}", lastSeq, timestampNs, orderId, symbol, uid, reduceSize);

                    api.reduceOrder(serviceFlags, eventsGroup, timestampNs, reduceSize, orderId, symbol, uid);

                } else if (cmdType == OrderCommandType.PLACE_ORDER) {

                    // 下订单命令，读取相关数据并执行
                    final long uid = jr.readLong();
                    final int symbol = jr.readInt();
                    final long orderId = jr.readLong();
                    final long price = jr.readLong();
                    final long reservedBidPrice = jr.readLong();
                    final long size = jr.readLong();
                    final int userCookie = jr.readInt();
                    final byte actionAndType = jr.readByte(); // 1 byte：包含订单操作和订单类型

                    final OrderAction orderAction = OrderAction.of((byte) (actionAndType & 0b1));
                    final OrderType orderType = OrderType.of((byte) ((actionAndType >> 1) & 0b1111));

                    if (debug)
                        log.debug("place order seq={} t={} orderId={} symbol={} uid={} price={} reserveBidPrice={} size={} userCookie={} {}/{} actionAndType={}", lastSeq, timestampNs, orderId, symbol, uid, price, reservedBidPrice, size, userCookie, orderAction, orderType, actionAndType);

                    api.placeNewOrder(serviceFlags, eventsGroup, timestampNs, orderId, userCookie, price, reservedBidPrice, size, orderAction, orderType, symbol, uid);

                } else if (cmdType == OrderCommandType.BALANCE_ADJUSTMENT) {

                    // 账户余额调整命令，读取相关数据并执行
                    final long uid = jr.readLong();
                    final int currency = jr.readInt();
                    final long transactionId = jr.readLong();
                    final long amount = jr.readLong();
                    final BalanceAdjustmentType adjustmentType = BalanceAdjustmentType.of(jr.readByte());

                    if (debug) log.debug("balanceAdjustment seq={}  {} uid:{} curre:{}", lastSeq, timestampNs, uid, currency);

                    api.balanceAdjustment(serviceFlags, eventsGroup, timestampNs, uid, transactionId, currency, amount, adjustmentType);

                } else if (cmdType == OrderCommandType.ADD_USER) {

                    // 添加用户命令，读取用户 ID 并执行
                    final long uid = jr.readLong();

                    if (debug) log.debug("add user  seq={}  {} uid:{} ", lastSeq, timestampNs, uid);

                    api.createUser(serviceFlags, eventsGroup, timestampNs, uid);

                } else if (cmdType == OrderCommandType.SUSPEND_USER) {

                    // 暂停用户命令，读取用户 ID 并执行
                    final long uid = jr.readLong();

                    if (debug) log.debug("suspend user seq={}  {} uid:{} ", lastSeq, timestampNs, uid);

                    api.suspendUser(serviceFlags, eventsGroup, timestampNs, uid);

                } else if (cmdType == OrderCommandType.RESUME_USER) {

                    // 恢复用户命令，读取用户 ID 并执行
                    final long uid = jr.readLong();

                    if (debug) log.debug("resume user seq={}  {} uid:{} ", lastSeq, timestampNs, uid);

                    api.resumeUser(serviceFlags, eventsGroup, timestampNs, uid);

                } else if (cmdType == OrderCommandType.BINARY_DATA_COMMAND) {

                    // 二进制数据命令，读取数据并执行
                    final byte lastFlag = jr.readByte();
                    final long word0 = jr.readLong();
                    final long word1 = jr.readLong();
                    final long word2 = jr.readLong();
                    final long word3 = jr.readLong();
                    final long word4 = jr.readLong();

                    if (debug)
                        log.debug("binary data seq={} t:{} {}", lastSeq, timestampNs, String.format("f=%d word0=%X word1=%X word2=%X word3=%X word4=%X", lastFlag, word0, word1, word2, word3, word4));

                    api.binaryData(serviceFlags, eventsGroup, timestampNs, lastFlag, word0, word1, word2, word3, word4);

                } else if (cmdType == OrderCommandType.RESET) {

                    // 重置命令，执行重置操作
                    api.reset(timestampNs);

                } else {

                    // 如果遇到未知命令类型，抛出异常
                    log.debug("eventsGroup={} serviceFlags={} cmdType={}", eventsGroup, serviceFlags, cmdType);
                    throw new IllegalStateException("unexpected command");
                }
            }
        }
    }

    @Override
    public void replayJournalFullAndThenEnableJouraling(InitialStateConfiguration initialStateConfiguration, ExchangeApi exchangeApi) {
        long seq = replayJournalFull(initialStateConfiguration, exchangeApi);
        enableJournaling(seq, exchangeApi);
    }

    @Override
    public boolean checkSnapshotExists(long snapshotId, SerializedModuleType type, int instanceId) {
        final Path path = resolveSnapshotPath(snapshotId, type, instanceId);
        final boolean exists = Files.exists(path);
        log.info("Checking snapshot file {} exists:{}", path, exists);
        return exists;
    }

    private void flushBufferSync(final boolean forceStartNextFile, final long timestampNs) throws IOException {

        // 调试日志：输出当前缓冲区的位置
//    log.debug("Flushing buffer position={}", buffer.position());

        // 跟踪批处理大小，计算批处理的平均大小，记录到 batchSizes 中
//    batchSizes.add(journalWriteBuffer.position());
//    if (batchSizes.size() == 1000) {
//        log.debug("Journal average batchSize = {} bytes", batchSizes.stream().mapToInt(c -> c).average());
//        batchSizes = new ArrayList<>();
//    }

        // 如果当前缓冲区的大小小于批处理压缩阈值，则进行未压缩写入（适用于单个消息或小批量）
        if (journalWriteBuffer.position() < journalBatchCompressThreshold) {
            writtenBytes += journalWriteBuffer.position(); // 累加已写入的字节数
            journalWriteBuffer.flip(); // 将缓冲区的写模式切换为读模式
//        long t = System.nanoTime();
            channel.write(journalWriteBuffer); // 写入缓冲区到通道
//        hdrRecorderRaw.recordValue(System.nanoTime() - t); // 记录写入时间
            journalWriteBuffer.clear(); // 清空缓冲区，准备下一次写入

        } else {
            // 如果缓冲区大小较大，则进行压缩写入
//        long t = System.nanoTime();
            int originalLength = journalWriteBuffer.position(); // 保存原始缓冲区长度
            journalWriteBuffer.flip(); // 将缓冲区切换为读模式
            lz4WriteBuffer.put(OrderCommandType.RESERVED_COMPRESSED.getCode()); // 写入压缩块标识
            lz4WriteBuffer.putInt(0); // 预留空间，稍后填充
            lz4WriteBuffer.putInt(0); // 预留空间，稍后填充
            lz4CompressorJournal.compress(journalWriteBuffer, lz4WriteBuffer); // 压缩缓冲区数据到 lz4WriteBuffer
            journalWriteBuffer.clear(); // 清空原始缓冲区
            writtenBytes += lz4WriteBuffer.position(); // 更新已写入的字节数
            int remainingCompressedLength = lz4WriteBuffer.position() - 9; // 计算压缩后的数据长度（去除已写入的头部信息）
            lz4WriteBuffer.putInt(1, remainingCompressedLength); // 填充压缩后的数据长度（1字节偏移量）
            lz4WriteBuffer.putInt(5, originalLength); // 填充原始数据长度（1 + 4字节偏移量）
            lz4WriteBuffer.flip(); // 切换到读模式
//        hdrRecorderLz4.recordValue(System.nanoTime() - t); // 记录压缩写入时间
            channel.write(lz4WriteBuffer); // 写入压缩后的数据到通道
            lz4WriteBuffer.clear(); // 清空压缩缓冲区，准备下一次写入
        }

        // 如果需要强制开始下一个文件，或者当前文件已写满，则开始新文件
        if (forceStartNextFile || writtenBytes >= journalFileMaxSize) {

//        log.info("RAW {}", LatencyTools.createLatencyReportFast(hdrRecorderRaw.getIntervalHistogram()));
//        log.info("LZ4-compression {}", LatencyTools.createLatencyReportFast(hdrRecorderLz4.getIntervalHistogram()));

            // 准备异步开始新文件，但只会执行一次
            startNewFile(timestampNs);
            writtenBytes = 0; // 重置已写入字节数
        }
    }

    /**
     * 启动一个新的文件，关闭当前的文件并创建新的日志文件。
     *
     * @param timestampNs 当前的时间戳（单位：纳秒）
     * @throws IOException 如果文件操作过程中发生 IO 异常
     */
    private void startNewFile(final long timestampNs) throws IOException {
        // 增加文件计数器
        filesCounter++;

        // 如果当前的文件通道不为空，关闭当前通道和随机访问文件
        if (channel != null) {
            channel.close();
            raf.close();
        }

        // 根据文件计数器和基本快照 ID 生成新的文件路径
        final Path fileName = resolveJournalPath(filesCounter, baseSnapshotId);

        // 如果文件已经存在，抛出异常
        if (Files.exists(fileName)) {
            throw new IllegalStateException("File already exists: " + fileName);
        }

        // 创建一个新的随机访问文件，并打开文件通道
        raf = new RandomAccessFile(fileName.toString(), "rwd");
        channel = raf.getChannel();

        // 注册下一个日志条目
        registerNextJournal(baseSnapshotId, timestampNs); // TODO 修正时间
    }

    /**
     * 注册下一个日志项，只能由 journal 线程调用。
     *
     * @param seq 当前日志项的序列号，表示日志项的顺序。
     * @param timestampNs 当前日志项的时间戳（单位：纳秒），表示日志项创建的时间。
     */
    private void registerNextJournal(long seq, long timestampNs) {
        // 创建并更新 lastJournalDescriptor，表示下一个日志项的状态
        lastJournalDescriptor = new JournalDescriptor(timestampNs, seq, lastSnapshotDescriptor, lastJournalDescriptor);
    }

    /**
     * 注册下一个快照，只能由 journal 线程调用。
     *
     * @param snapshotId 当前快照的唯一标识符。
     * @param seq 当前快照的序列号，可能用于标识快照的顺序。
     * @param timestampNs 当前快照的时间戳（单位：纳秒），表示快照创建的时间。
     */
    private void registerNextSnapshot(long snapshotId,
                                      long seq,
                                      long timestampNs) {
        // 更新 lastSnapshotDescriptor，创建并记录下一个快照的信息
        lastSnapshotDescriptor = lastSnapshotDescriptor.createNext(snapshotId, seq, timestampNs);
    }

    /**
     * 根据快照 ID、模块类型和实例 ID 生成并返回一个文件路径。
     *
     * @param snapshotId 快照的唯一标识符，用于生成文件名的一部分。
     * @param type 模块类型，枚举类型，用于在文件名中标识不同的模块类型。
     * @param instanceId 实例 ID，用于在文件名中区分同类型的多个实例。
     * @return 返回一个 Path 对象，指向由 snapshotId、type 和 instanceId 构成的快照文件路径。
     */
    private Path resolveSnapshotPath(long snapshotId, SerializedModuleType type, int instanceId) {
        // 使用指定的文件夹路径（folder），结合快照 ID、模块类型和实例 ID，生成文件路径
        // 文件名格式：exchangeId_snapshot_snapshotId_typeCode_instanceId.ecs
        return folder.resolve(String.format("%s_snapshot_%d_%s%d.ecs", exchangeId, snapshotId, type.code, instanceId));
    }

    /**
     * 生成主日志文件的路径。
     *
     * @return 主日志文件的路径
     */
    private Path resolveMainLogPath() {
        // 使用 exchangeId 生成主日志文件的路径，文件名为 exchangeId.eca
        return folder.resolve(String.format("%s.eca", exchangeId));
    }

    /**
     * 根据分区 ID 和快照 ID 生成并返回一个日志文件路径。
     *
     * @param partitionId 分区的唯一标识符，通常是一个数字。用于区分不同分区的日志文件。
     * @param snapshotId 快照的唯一标识符，确保每个快照的日志文件路径唯一。
     * @return 返回一个 Path 对象，指向由 partitionId 和 snapshotId 组成的日志文件路径。
     */
    private Path resolveJournalPath(int partitionId, long snapshotId) {
        // 使用指定的文件夹路径（folder），结合快照 ID 和分区 ID，生成文件路径
        // 文件名格式：exchangeId_journal_snapshotId_partitionId.ecj
        // partitionId 格式化为四位十六进制数
        return folder.resolve(String.format("%s_journal_%d_%04X.ecj", exchangeId, snapshotId, partitionId));
    }

}
