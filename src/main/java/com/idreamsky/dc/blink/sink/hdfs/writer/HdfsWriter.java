package com.idreamsky.dc.blink.sink.hdfs.writer;

import com.alibaba.blink.streaming.connectors.common.util.ByteUtils;
import com.idreamsky.dc.blink.sink.hdfs.pojo.HdfsMessage;
import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import com.idreamsky.dc.blink.common.utils.GlobalClock;
import lombok.Getter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.ipc.RemoteException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author colby.luo
 * @date 2020/3/26 17:38
 */
public class HdfsWriter {

    private static GLogger log = GLoggerFactory.getGLogger(HdfsWriter.class);
    /**
     * hdfs 文件系统
     */
    private final FileSystem hdfs;

    /**
     * 文件输出流
     */
    private FSDataOutputStream outputStream;

    /**
     * 写入文件
     */
    @Getter
    private String writePath;

    /**
     * 写入器状态
     */
    private AtomicInteger state;

    /**
     * 0: 空闲状态
     * 1: 正在写
     * -1: 已经关闭了
     * IDLE -> WRITING -> CLOSED
     * WRITING - IDLE
     */
    private static final int IDLE = 0;
    private static final int WRITING = 1;
    private static final int CLOSED = -1;

    @Getter
    private volatile Instant lastWriterTime;

    @Getter
    private volatile Instant lastFlushTime;

    public HdfsWriter(FileSystem hdfs, String writePath) throws IOException {
        this.hdfs = hdfs;

        initOutputStream(writePath);

        state = new AtomicInteger(IDLE);

        // 初始化写入时间为现在，避免刚创建就被关闭
        lastFlushTime = lastWriterTime = GlobalClock.currentSecond();

        log.info("小时级HDFS写入器初始化 writePath=[{}]", this.writePath);
    }

    /**
     * hdfs 对同一个文件只许一个客户端写入，也就是租约管理
     * 一般不会有多个 Client 同时写一个 hdfs 文件
     * 但是偶尔忘了关闭文件释放租约就会有这个问题
     */
    private void initOutputStream(String pathStr) throws IOException {
        Path path = new Path(pathStr);
        FSDataOutputStream outputStream = null;

        int tryCount = 0;
        int maxRetry = 10;
        RemoteException lastException = null;
        while (outputStream == null && tryCount <= maxRetry) {
            try {
                if (hdfs.exists(path)) {
                    outputStream = hdfs.append(path);
                } else {
                    outputStream = hdfs.create(path);
                }

            } catch (RemoteException e) {
                lastException = e;

                // 只重试租约有关的异常
                String className = e.getClassName();
                if (!className.equals(AlreadyBeingCreatedException.class.getName())
                    && !className.equals(RecoveryInProgressException.class.getName())) {
                    break;
                }

                log.error("path = [{}] 租约冲突", path, e);

                tryCount += 1;

                // 先尝试强制恢复租约
                if (tryRecoveryLease(path)) {
                    log.info("租约恢复成功 path: {}", path);
                    continue;
                }

                // 恢复租约失败，另起一个文件算了
                path = new Path(pathStr + "_" + tryCount);
                log.info("租约恢复失败尝试新path {}", path);
            }
        }

        if (outputStream == null) {
            throw lastException;
        }

        this.writePath = path.toString();
        this.outputStream = outputStream;
    }

    private boolean tryRecoveryLease(Path path) {
        DistributedFileSystem dfs = (DistributedFileSystem) this.hdfs;
        try {
            return dfs.recoverLease(path);
        } catch (Exception e) {
            log.error("租约恢复异常 path: {}", path, e);
            return false;
        }
    }

    public boolean write(HdfsMessage message) throws IOException {
        // 检查是否已经 CLOSED ，并置为写入中状态
        if (state.compareAndSet(IDLE, WRITING)) {
            try {
                outputStream.write(ByteUtils.toBytes(message.getMessage() + System.lineSeparator()));
                lastWriterTime = GlobalClock.currentSecond();
                return true;

                // } catch (IOException e) { 消息写入失败重试？ 目前还没有遇到，blink是否会重试？
            } finally {
                state.set(IDLE);
            }
        }
        return false;
    }

    /**
     * flush() 与 close()是互斥的
     */
    public synchronized void flush() throws IOException {
        if (state.get() > CLOSED) {
            try {
                DFSOutputStream dfsOutputStream = (DFSOutputStream) outputStream.getWrappedStream();
                EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags = EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH);
                dfsOutputStream.hsync(syncFlags);
                lastFlushTime = GlobalClock.currentSecond();
                log.info("刷hdfs数据到磁盘 path = [{}]", writePath);
            } catch (IOException e) {
                log.error("刷hdfs数据到磁盘异常 path = [{}]", writePath, e);
                throw e;
            }
        } else {
            log.info("文件已关闭，无法刷盘");
        }
    }

    /**
     * 这里有非常低的概率在判断需要关闭后，又有数据需要写入
     * 这个时候不管有没有数据写入过都会关闭，接下来的写入会重建 Writer
     * 有可能同一个文件存在两个 writer
     */
    public synchronized void close() throws IOException {
        // 已经关闭了
        if (state.get() == CLOSED) {
            return;
        }

        // 等待写完再关闭
        while (!state.compareAndSet(IDLE, CLOSED)) {
            Thread.yield();
        }

        try {
            outputStream.close();
            log.info("文件已关闭 {} ", writePath);
        } catch (IOException e) {
            log.error("文件关闭异常 {}", writePath, e);
            throw e;
        }
    }
}
