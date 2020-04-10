package com.idreamsky.dc.blink.udx;

import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import com.idreamsky.dc.blink.udx.support.ExistEvaluator;
import com.idreamsky.dc.blink.udx.support.HBaseApplicationContext;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.IOException;

/**
 * rowKey 存不存在
 * 存在 1
 * 不存在 0
 *
 * @author colby.luo
 * @date 2020/4/9 18:22
 */
public class HBaseIsExistUdf extends ScalarFunction {
    private static GLogger log = GLoggerFactory.getGLogger();

    private HBaseApplicationContext applicationContext = null;

    private ExistEvaluator existEvaluator;

    @Override
    public void open(FunctionContext context) throws Exception {

        HBaseApplicationContext ctx = HBaseApplicationContext.from(context);
        log.info("HBaseIsExistUdf开始初始化， 应用上下文配置 {}", ctx);

        ctx.init();
        log.info("HBaseIsExistUdf初始化成功");

        applicationContext = ctx;

        existEvaluator = new ExistEvaluator(ctx);

        log.info("HBaseIsExistUdf初始化完成");
    }

    public Long eval(String rowKey) throws IOException {
        String defaultTableName = applicationContext.getDefaultTableName();
        return eval(rowKey, defaultTableName);
    }

    public Long eval(String rowKey, String tableName) throws IOException {
        assertNotEmpty(rowKey, tableName);
        return existEvaluator.eval(rowKey, tableName);
    }

    private void assertNotEmpty(String rowKey, String columnName) {
        if (StringUtils.isBlank(rowKey)) {
            log.error("rowKey 不能为空");
            throw new IllegalArgumentException("rowKey 不能为空");
        }
        if (StringUtils.isBlank(columnName)) {
            log.error("columnName 不能为空");
            throw new IllegalArgumentException("columnName 不能为空");
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
