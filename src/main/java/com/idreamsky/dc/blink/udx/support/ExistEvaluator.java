package com.idreamsky.dc.blink.udx.support;

import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author colby.luo
 * @date 2020/3/30 16:44
 */
public class ExistEvaluator {
    private static GLogger log = GLoggerFactory.getGLogger();

    private final Connection connection;


    public static final byte[] DEFAULT_FAMILY = Bytes.toBytes("cf");


    public ExistEvaluator(HBaseApplicationContext ctx) {
        connection = ctx.getHbaseConnection();
    }

    public Long eval(String rowKey, String tableName) throws IOException {
        Table table = null;
        try {
            // hbase 不建议池化或缓存 table
            table = connection.getTable(TableName.valueOf(tableName));

            byte[] row = Bytes.toBytes(rowKey);

            Get get = new Get(row);
            return table.exists(get) ? 1L : 0L;
        } catch (IOException e) {
            log.error("ExistEvaluator eval 错误 rowKey: {} value: {} tableName: {}", rowKey, tableName, e);
            throw e;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("ExistEvaluator table 关闭失败 rowKey: {} value: {} tableName: {}", rowKey, tableName, e);
                }
            }
        }
    }
}
