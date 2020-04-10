package com.idreamsky.dc.blink.udx;

import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import com.idreamsky.dc.blink.udx.support.HBaseApplicationContext;
import com.idreamsky.dc.blink.udx.support.PutEvaluator;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.IOException;

/**
 * @author colby.luo
 * @date 2020/3/30 15:09
 */
public class HBaseSetIfNotExistUdf extends ScalarFunction {
    private static GLogger log = GLoggerFactory.getGLogger();

    private HBaseApplicationContext applicationContext = null;

    private PutEvaluator putEvaluator;

    @Override
    public void open(FunctionContext context) throws Exception {

        HBaseApplicationContext ctx = HBaseApplicationContext.from(context);
        log.info("HBaseSetIfNotExistUdf开始初始化， 应用上下文配置 {}", ctx);

        ctx.init();
        log.info("HBaseApplicationContext初始化成功");

        applicationContext = ctx;

        putEvaluator = new PutEvaluator(ctx);

        log.info("HBaseSetIfNotExistUdf初始化完成");
    }

    public Long eval(String rowKey, Object value) throws IOException {
        String defaultTableName = applicationContext.getDefaultTableName();
        String defaultColumnName = applicationContext.getDefaultColumnName();
        return eval(rowKey, value, defaultTableName, defaultColumnName);
    }

    public Long eval(String rowKey, Object value, String tableName) throws IOException {
        String defaultColumnName = applicationContext.getDefaultColumnName();
        return eval(rowKey, value, tableName, defaultColumnName);
    }

    public Long eval(String rowKey, Object value, String tableName, String columnName) throws IOException {
        assertNotEmpty(rowKey, tableName, columnName);
        value = value == null ? "" : value;
        return putEvaluator.eval(rowKey, value, tableName, columnName);
    }

    private void assertNotEmpty(String rowKey, String columnName, String tableName) {
        if (StringUtils.isBlank(rowKey)) {
            log.error("rowKey 不能为空");
            throw new IllegalArgumentException("rowKey 不能为空");
        }
        if (StringUtils.isBlank(columnName)) {
            log.error("columnName 不能为空");
            throw new IllegalArgumentException("columnName 不能为空");
        }
        if (StringUtils.isBlank(tableName)) {
            log.error("hbase.table.name 未设置");
            throw new IllegalArgumentException("hbase.table.name 未设置");
        }
    }

    @Override
    public void close() throws Exception {
        applicationContext.close();
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
