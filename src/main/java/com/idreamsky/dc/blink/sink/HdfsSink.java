package com.idreamsky.dc.blink.sink;

import com.alibaba.blink.streaming.connector.custom.api.CustomSinkBase;
import com.idreamsky.dc.blink.sink.hdfs.handler.HdfsMessageHandler;
import com.idreamsky.dc.blink.sink.hdfs.pojo.HdfsMessage;
import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import com.idreamsky.dc.blink.sink.hdfs.vo.HiveSinkApplicationContext;
import com.idreamsky.dc.blink.sink.hdfs.vo.HdfsSinkRowReader;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * @author colby.luo
 * @date 2020/3/26 9:37
 */
@SuppressWarnings("JavaDoc")
public class HdfsSink extends CustomSinkBase {

    private static GLogger log = GLoggerFactory.getGLogger(HdfsSink.class);

    private HdfsMessageHandler messageHandler;

    private HiveSinkApplicationContext applicationContext;

    private HdfsSinkRowReader rowReader;


    /**
     * 初始化方法。每次初始建立和Failover的时候会调用一次。
     *
     * @param taskNumber 当前节点的编号。
     * @param numTasks   Sink节点的总数。
     * @throws IOException
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        log.info("HiveSink开始初始化 : 任务号: {}  总任务数: {}", taskNumber, numTasks);

        HiveSinkApplicationContext ctx = HiveSinkApplicationContext.create(taskNumber, numTasks, userParamsMap);
        log.info("应用配置信息 {}", ctx);

        HdfsSinkRowReader rowReader = HdfsSinkRowReader.create(rowTypeInfo, ctx);

        this.applicationContext = ctx;
        this.rowReader = rowReader;
        this.messageHandler = new HdfsMessageHandler(ctx);
    }


    /**
     * 处理插入单行数据。
     *
     * @param row
     * @throws IOException
     */
    @Override
    public void writeAddRecord(Row row) throws IOException {
        // 封装成一条 message
        HdfsMessage message = rowReader.read(row);

        messageHandler.handle(message);
    }

    /**
     * lose方法，释放资源。
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        messageHandler.close();
        applicationContext.close();
        log.info("HdfsSink 关闭成功");
    }


    /**
     * 如果进行批量插入，该方法需要把线程中缓存的数据全部刷入下游存储；若无，可不实现此方法。
     *
     * @throws IOException
     */
    @Override
    public void sync() throws IOException {
        messageHandler.sync();
        log.info("HiveSink 同步刷盘成功");
    }

    /**
     * 处理删除单行数据。
     *
     * @param row
     * @throws IOException
     */
    @Override
    public void writeDeleteRecord(Row row) throws IOException {
        log.error("writeDeleteRecord() 不支持的操作");
    }

    /**
     * 返回类名。
     */
    @Override
    public String getName() {
        return getClass().getSimpleName();
    }
}
