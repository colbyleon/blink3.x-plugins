package com.idreamsky.dc.blink.sink.hdfs.vo;

import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author colby.luo
 * @date 2020/3/27 17:41
 */
@Data
public class HiveSinkApplicationContext {
    private static GLogger log = GLoggerFactory.getGLogger(HiveSinkApplicationContext.class);

    /**
     * 任务号
     */
    private Integer taskNumber;

    /**
     * 任务并发数
     */
    private Integer numTasks;

    /**
     * 文件刷新间隔
     */
    private Duration flushInterval;

    /**
     * 多久未写入则删除并关闭文件
     */
    private Duration removeInterval;

    /**
     * 文件路径的模板
     */
    private PathSchema hivePathSchema;

    /**
     * hdfs的配置
     */
    private String hdfsNameNode;

    /**
     * 单任务调度器
     */
    private ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(2, new ThreadFactory() {
        private AtomicInteger num = new AtomicInteger(0);

        @Override
        public Thread newThread(@Nonnull Runnable r) {
            String name = String.format("HiveSink-Scheduler(%d/%d)-%d", taskNumber, numTasks, num.incrementAndGet());
            return new Thread(r, name);
        }
    });

    /**
     * hdfs 文件系统
     */
    private FileSystem hdfs;

    public static HiveSinkApplicationContext create(int taskNumber, int numTasks, Map<String, String> userParamsMap) throws IOException {
        HiveSinkApplicationContext ctx = new HiveSinkApplicationContext();
        ctx.setTaskNumber(taskNumber);
        ctx.setNumTasks(numTasks);

        String schemaStr = userParamsMap.get("hive.path.schema");
        if (StringUtils.isNotBlank(schemaStr)) {
            ctx.setHivePathSchema(new PathSchema(schemaStr));
        }

        String flushInterval = userParamsMap.get("flush.interval");
        if (StringUtils.isNotBlank(flushInterval)) {
            ctx.setFlushInterval(Duration.ofSeconds(Integer.parseInt(flushInterval)));
        }

        String nameNode = userParamsMap.get("hdfs.namenode");
        if (StringUtils.isNotBlank(nameNode)) {
            ctx.setHdfsNameNode(nameNode);
        }

        ctx.init();

        return ctx;
    }

    public void init() throws IOException {

        if (hivePathSchema == null) {
            hivePathSchema = new PathSchema(
                "/user/hive/warehouse/test.db/{table}/log_date={date}/log_hour={hour}");
        }

        if (flushInterval == null) {
            flushInterval = Duration.ofSeconds(30);
        }

        if (removeInterval == null) {
            removeInterval = Duration.ofMinutes(20);
        }

        hdfs = initHdfs();

        log.info("HiveSink应用上下文初始化完成");
    }


    private FileSystem initHdfs() throws IOException {
        Configuration conf = getConfiguration();

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(conf);
            // 检查 hdfs 连通性
            hdfs.exists(new Path("/"));
        } catch (IOException e) {
            log.error("hdfs连接失败", e);
            throw e;
        }

        log.info("hdfs 连接成功");
        return hdfs;
    }

    public Configuration getConfiguration() throws IOException {

        String clusterName = "idsnameservice";
        String nameService = "namenode340,namenode90";
        String nameNode = hdfsNameNode;

        // nameNode 默认配置
        if (StringUtils.isBlank(nameNode)) {
            nameNode = "这里是线上的默认地址";
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", String.format("hdfs://%s", clusterName));
        conf.set("dfs.nameservices", clusterName);
        conf.set(String.format("dfs.ha.namenodes.%s", clusterName), nameService);

        String[] services = nameService.split(",");
        String[] nodes = nameNode.split(",");

        for (int i = 0; i < services.length; i++) {
            conf.set(String.format("dfs.namenode.rpc-address.%s.%s", clusterName, services[i]), nodes[i]);
        }

        conf.set("dfs.client.failover.proxy.provider." + clusterName,
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);

        return conf;
    }

    public void close() throws IOException {
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.shutdown();
        hdfs.close();
        log.info("HDFS以及应用上下文已关闭");
    }
}
