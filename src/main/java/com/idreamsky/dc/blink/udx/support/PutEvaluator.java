package com.idreamsky.dc.blink.udx.support;

import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author colby.luo
 * @date 2020/3/30 16:44
 */
public class PutEvaluator {
    private static GLogger log = GLoggerFactory.getGLogger();

    private final Connection connection;


    public static final byte[] DEFAULT_FAMILY = Bytes.toBytes("cf");


    public PutEvaluator(HBaseApplicationContext ctx) {
        connection = ctx.getHbaseConnection();
    }

    public Long eval(String rowKey, Object value, String tableName, String columnName) throws IOException {
        Table table = null;
        try {
            // hbase 不建议池化或缓存 table
            table = connection.getTable(TableName.valueOf(tableName));

            byte[] row = Bytes.toBytes(rowKey);
            byte[] valueBytes = serialValue(value);
            byte[] column = Bytes.toBytes(columnName);

            Put put = new Put(row).addColumn(DEFAULT_FAMILY, column, valueBytes);

            return table.checkAndMutate(row, DEFAULT_FAMILY)
                .qualifier(column)
                .ifNotExists()
                .thenPut(put) ? 1L : 0L;
        } catch (IOException e) {
            log.error("eval 错误 rowKey: {} value: {} tableName: {}", rowKey, value, tableName, e);
            throw e;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("table 关闭失败 rowKey: {} value: {} tableName: {}", rowKey, value, tableName, e);
                }
            }
        }
    }

    private byte[] serialValue(Object value) {
        if (value instanceof String) {
            return Bytes.toBytes((String) value);
        } else if (value instanceof Long) {
            return Bytes.toBytes((Long) value);
        } else if (value instanceof Integer) {
            return Bytes.toBytes((Integer) value);
        } else if (value instanceof Double) {
            return Bytes.toBytes((Double) value);
        } else if (value instanceof Float) {
            return Bytes.toBytes((Float) value);
        } else if (value instanceof Short) {
            return Bytes.toBytes((Short) value);
        } else if (value instanceof Boolean) {
            return Bytes.toBytes((Boolean) value);
        } else {
            return Bytes.toBytes(String.valueOf(value));
        }
    }
}
