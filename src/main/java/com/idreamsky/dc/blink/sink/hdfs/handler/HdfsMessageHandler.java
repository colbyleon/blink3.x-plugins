package com.idreamsky.dc.blink.sink.hdfs.handler;

import com.idreamsky.dc.blink.sink.hdfs.pojo.HdfsMessage;
import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import com.idreamsky.dc.blink.common.utils.GlobalClock;
import com.idreamsky.dc.blink.sink.hdfs.vo.HiveSinkApplicationContext;
import com.idreamsky.dc.blink.sink.hdfs.writer.HdfsWriter;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 单个 topic 的消息处理
 * 按小时进行分区
 *
 * @author colby.luo
 * @date 2020/3/26 17:30
 */
public class HdfsMessageHandler {

    private static GLogger log = GLoggerFactory.getGLogger(HdfsMessageHandler.class);

    /**
     * 应用上下文
     */
    private final HiveSinkApplicationContext applicationContext;

    /**
     * path -> writer
     * hdfsWriter 对应一个文件
     */
    private Map<String, HdfsWriter> hdfsWriterMap = new ConcurrentHashMap<>();

    public HdfsMessageHandler(HiveSinkApplicationContext applicationContext) {
        this.applicationContext = applicationContext;

        // 定时刷数据, 定时关文件流
        applicationContext.getScheduler()
            .scheduleWithFixedDelay(new FlushCloseTask(), 10, 10, TimeUnit.SECONDS);

        log.info("消息处理器初始化完成 taskNumber:[{}]", applicationContext.getTaskNumber());
    }

    public void handle(HdfsMessage message) throws IOException {
        String path = message.getPath();
        HdfsWriter hdfsWriter = getHdfsWriter(path);

        try {
            while (!hdfsWriter.write(message)) {
                try {
                    // 刚好调用了 writer.close() 处于 closed 状态，等一会儿再新建一个
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (InterruptedException ignore) {
                }
                hdfsWriter = getHdfsWriter(path);
            }
        } catch (IOException e) {
            log.error("消息写入错误", e);
            throw e;
        }
    }

    private HdfsWriter getHdfsWriter(String path) throws IOException {
        HdfsWriter hdfsWriter = hdfsWriterMap.get(path);

        if (hdfsWriter == null) {
            FileSystem hdfs = applicationContext.getHdfs();

            hdfsWriter = new HdfsWriter(hdfs, path);

            hdfsWriterMap.put(path, hdfsWriter);
        }
        return hdfsWriter;
    }

    public void sync() throws IOException {
        Collection<HdfsWriter> values = hdfsWriterMap.values();

        IOException exception = null;

        for (HdfsWriter writer : values) {
            try {
                writer.flush();
            } catch (IOException e) {
                exception = e;
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    public void close() throws IOException {
        // 防止 ConcurrentModificationException
        Set<String> keys = new HashSet<>(hdfsWriterMap.keySet());

        // 异常向上传递
        IOException exception = null;

        for (String key : keys) {
            try {
                closeThenRemove(key);
            } catch (IOException e) {
                exception = e;
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * 保证先关闭后删除的语义
     * 避免两个相同的 writer 一个正在 close 但还没 closed，另一过来新建，就会产生租约冲突
     */
    private void closeThenRemove(String key) throws IOException {
        HdfsWriter writer = hdfsWriterMap.get(key);
        if (writer != null) {
            try {
                writer.close();
            } finally {
                hdfsWriterMap.remove(key);
            }
        }
    }

    public class FlushCloseTask implements Runnable {

        @Override
        public void run() {
            Duration flushInterval = applicationContext.getFlushInterval();
            Duration removeInterval = applicationContext.getRemoveInterval();

            Instant now = GlobalClock.currentSecond();

            Set<String> toRemoveWriter = new HashSet<>();
            Set<String> toFlushWriter = new HashSet<>();

            hdfsWriterMap.forEach((path, hdfsWriter) -> {
                Instant lastWriterTime = hdfsWriter.getLastWriterTime();
                Duration closeElapse = Duration.between(lastWriterTime, now);

                // 关闭间隔（比如 5 分钟）里没有写入则关闭文件
                if (closeElapse.compareTo(removeInterval) > 0) {
                    toRemoveWriter.add(path);
                }

                Instant lastFlushTime = hdfsWriter.getLastFlushTime();
                // 最后写入时间晚于最后刷新时间，没有写入则不需要刷呀
                if (lastWriterTime.compareTo(lastFlushTime) > 0) {
                    Duration flushElapse = Duration.between(lastFlushTime, now);
                    // 刷新时间超过刷新间隔如 30 秒
                    if (flushElapse.compareTo(flushInterval) > 0) {
                        toFlushWriter.add(path);
                    }
                }
            });

            // 理论上不会包含
            toFlushWriter.removeIf(toRemoveWriter::contains);

            if (!toFlushWriter.isEmpty() || !toRemoveWriter.isEmpty()) {
                log.info("定时任务检测到需要准备关闭文件 {} 个, 刷盘文件 {} 个",
                    toRemoveWriter.size(), toFlushWriter.size());
            }

            for (String key : toRemoveWriter) {
                try {
                    closeThenRemove(key);
                } catch (IOException ignore) {
                    // 日志在 writer 中有打
                }
            }

            for (String key : toFlushWriter) {
                HdfsWriter hdfsWriter = hdfsWriterMap.get(key);
                try {
                    hdfsWriter.flush();
                } catch (IOException ignore) {
                    // 日志在 writer 中有打
                }
            }
        }
    }
}
