package com.idreamsky.dc.blink.sink;

import com.alibaba.blink.streaming.connector.custom.api.CustomSinkBase;
import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import com.idreamsky.dc.blink.sink.kafka.KafkaSender;
import com.idreamsky.dc.blink.sink.kafka.KafkaSinkRowReader;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;

/**
 * @author colby.luo
 * @date 2020/4/1 14:29
 */
public class KafkaSink extends CustomSinkBase {
    private static GLogger log = GLoggerFactory.getGLogger();

    private KafkaSender sender;

    private KafkaSinkRowReader rowReader;

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        sender = KafkaSender.create(taskNumber, numTasks, userParamsMap);

        rowReader = KafkaSinkRowReader.create(rowTypeInfo);

        log.info("KafkaSink 初始化完成 {}/{}", taskNumber, numTasks);
    }

    @Override
    public void writeAddRecord(Row row) throws IOException {
        sender.send(rowReader.read(row));
    }

    @Override
    public void sync() throws IOException {
        sender.sync();
        log.info("KafkaSink 同步消息完成");
    }

    @Override
    public void close() throws IOException {
        sender.close();
        log.info("KafkaSink 已关闭");
    }

    @Override
    public void writeDeleteRecord(Row row) throws IOException {
        log.error("不支持 writeDeleteRecord");
    }

    @Override
    public String getName() {
        return KafkaSink.class.getName();
    }
}
