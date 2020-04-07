package com.idreamsky.dc.blink.sink.kafka;

import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author colby.luo
 * @date 2020/4/1 16:25
 */
@Data
public class KafkaSinkRowReader {

    public static GLogger log = GLoggerFactory.getGLogger();

    public static final String MESSAGE_FIELD_NAME = "message";

    public static final String TIMESTAMP_FIELD_NAME = "ts";

    public static final String TOPIC_FIELD_NAME = "topic";

    private Integer messageIndex;

    private Integer timestampIndex;

    private Integer topicIndex;

    public static KafkaSinkRowReader create(RowTypeInfo rowTypeInfo) {
        KafkaSinkRowReader rowReader = new KafkaSinkRowReader();

        TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();

        int messageIndex = rowTypeInfo.getFieldIndex(MESSAGE_FIELD_NAME);
        if (messageIndex == -1) {
            log.error("缺少 message 字段");
            throw new IllegalArgumentException("缺少 message 字段");
        }
        if (!fieldTypes[messageIndex].getTypeClass().equals(String.class)) {
            log.error("message 字段类型必须为 varchar");
            throw new IllegalArgumentException("message 字段类型必须为 varchar");
        }

        int topicIndex = rowTypeInfo.getFieldIndex(TOPIC_FIELD_NAME);
        if (topicIndex == -1) {
            log.error("缺少 topic 字段");
            throw new IllegalArgumentException("缺少 topic 字段");
        }
        if (!fieldTypes[topicIndex].getTypeClass().equals(String.class)) {
            log.error("topic 字段类型必须为 varchar");
            throw new IllegalArgumentException("topic 字段类型必须为 varchar");
        }

        int timestampIndex = rowTypeInfo.getFieldIndex(TIMESTAMP_FIELD_NAME);
        if (timestampIndex != -1) {
            if (!fieldTypes[timestampIndex].getTypeClass().equals(Long.class)) {
                log.error("ts 字段类型必须为 bigint");
                throw new IllegalArgumentException("ts 字段类型必须为 bigint");
            }
        } else {
            log.info("缺少 ts 字段 将使用系统时间作为kafka的timestamp");
        }

        rowReader.setMessageIndex(messageIndex);
        rowReader.setTimestampIndex(timestampIndex);
        rowReader.setTopicIndex(topicIndex);

        return rowReader;
    }

    public ProducerRecord<String, String> read(Row row) {
        String message = String.valueOf(row.getField(messageIndex));
        String topic = String.valueOf(row.getField(topicIndex));
        Long timestamp = null;
        if (timestampIndex != -1) {
            timestamp = Long.parseLong(String.valueOf(row.getField(timestampIndex))) * 1000;
        }
        return new ProducerRecord<>(topic, null, timestamp, null, message);
    }
}
