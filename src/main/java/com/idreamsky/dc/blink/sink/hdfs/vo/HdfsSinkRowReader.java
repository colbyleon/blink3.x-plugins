package com.idreamsky.dc.blink.sink.hdfs.vo;

import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import com.idreamsky.dc.blink.sink.hdfs.pojo.HdfsMessage;
import lombok.Data;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.*;

/**
 * @author colby.luo
 * @date 2020/3/31 17:33
 */
@Data
public class HdfsSinkRowReader {
    private static final GLogger log = GLoggerFactory.getGLogger();

    private static final String MESSAGE_FIELD_NAME = "message";

    private HiveSinkApplicationContext applicationContext;

    private int messageIndex;

    private PathSchema pathSchema;

    private Map<String, Integer> placeholderIndexMap = new HashMap<>();


    public static HdfsSinkRowReader create(RowTypeInfo rowTypeInfo, HiveSinkApplicationContext ctx) {
        HdfsSinkRowReader rowReader = new HdfsSinkRowReader();

        List<String> fieldNames = Arrays.asList(rowTypeInfo.getFieldNames());

        int messageIndex = fieldNames.indexOf(MESSAGE_FIELD_NAME);
        if (messageIndex == -1) {
            throw new RuntimeException("必须有message字段, 并且为 varchar 类型");
        }

        rowReader.setMessageIndex(messageIndex);

        PathSchema hivePathSchema = ctx.getHivePathSchema();
        rowReader.setPathSchema(hivePathSchema);

        // 检查输入字段是否满足 pathSchema的要求
        Map<String, String> placeholderMap = hivePathSchema.getPlaceholderMap();
        for (String placeholder : placeholderMap.keySet()) {
            int index = fieldNames.indexOf(placeholder);
            if (index == -1) {
                log.error("缺少输入字段 需要的字段: {}, 现在的字段: {}",
                    placeholderMap.keySet(), fieldNames);
                throw new IllegalArgumentException("缺少输入字段: " + placeholder);
            }
            rowReader.addPlaceholderIndex(placeholder, index);
        }

        rowReader.setApplicationContext(ctx);

        return rowReader;
    }

    private void addPlaceholderIndex(String placeholder, int index) {
        placeholderIndexMap.put(placeholder, index);
    }


    public HdfsMessage read(Row row) {
        HdfsMessage hMsg = new HdfsMessage();
        hMsg.setMessage(String.valueOf(row.getField(messageIndex)));

        PathSchema.PathBuilder pathBuilder = pathSchema.builder();

        Set<Map.Entry<String, Integer>> entries = placeholderIndexMap.entrySet();

        for (Map.Entry<String, Integer> entry : entries) {
            String fieldName = entry.getKey();
            String value = String.valueOf(row.getField(entry.getValue()));
            pathBuilder = pathBuilder.fill(fieldName, value);
        }
        String path = pathBuilder.build();

        // 拼上文件名
        if (path.endsWith("/")) {
            path += "log_" + applicationContext.getTaskNumber();
        }else {
            path += "/log_" + applicationContext.getTaskNumber();
        }

        hMsg.setPath(path);

        return hMsg;
    }
}
