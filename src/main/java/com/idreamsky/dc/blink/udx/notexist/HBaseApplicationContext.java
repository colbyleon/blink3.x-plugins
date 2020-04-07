package com.idreamsky.dc.blink.udx.notexist;

import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.AliHBaseUEClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author colby.luo
 * @date 2020/3/30 16:12
 */
@Data
public class HBaseApplicationContext {

    private static GLogger log = GLoggerFactory.getGLogger();

    private String defaultTableName;

    private String defaultColumnName;

    private String zkAddress;

    private String username;

    private String password;

    private Connection hbaseConnection;

    private static int taskNumber;

    private static int numTasks;

    public static HBaseApplicationContext from(FunctionContext context) {
        HBaseApplicationContext ctx = new HBaseApplicationContext();

        String defaultTableName = context.getJobParameter("hbase.table.name", "new_user");
        String defaultColumnName = context.getJobParameter("hbase.column.name", "time");

        String zkAddress = context.getJobParameter("hbase.zookeeper.quorum", "这里是线上的默认地址");
        String username = context.getJobParameter("hbase.client.username", "线上默认");
        String password = context.getJobParameter("hbase.client.password", "线上默认");

        if (StringUtils.isNotBlank(defaultTableName)) {
            ctx.setDefaultTableName(defaultTableName);
        }

        ctx.setZkAddress(zkAddress);

        if (StringUtils.isNotBlank(username)) {
            ctx.setUsername(username);
        }

        if (StringUtils.isNotBlank(password)) {
            ctx.setPassword(password);
        }

        if (StringUtils.isNotBlank(defaultColumnName)) {
            ctx.setDefaultColumnName(defaultColumnName);
        }

        taskNumber = context.getIndexOfThisSubtask();
        numTasks = context.getNumberOfParallelSubtasks();
        return ctx;
    }

    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkAddress);
        if (username != null) {
            conf.set("hbase.client.username", username);
        }
        if (password != null) {
            conf.set("hbase.client.password", password);
        }

        // 测试时要把这行注释掉
        conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());

        try {
            hbaseConnection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            log.error("hbase 连接失败", e);
            throw e;
        }
    }

    public void close() throws IOException {
        try {
            hbaseConnection.close();
        } catch (IOException e) {
            log.error("HBase 连接关闭失败");
            throw e;
        }
    }
}
