import com.idreamsky.dc.blink.udx.HBaseIsExistUdf;
import com.idreamsky.dc.blink.udx.HBaseSetIfNotExistUdf;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author colby.luo
 * @date 2020/3/30 17:36
 */
public class HBaseIsExistUdfTest {

    /**
     * 本地测试先注释掉  conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
     */
    public static void main(String[] args) throws Exception {
        HBaseIsExistUdf udf = null;
        try {
            udf = new HBaseIsExistUdf();
            udf.open(getFunctionContext(0, 1));

        scanTest(udf);


        } finally {
            if (udf != null) {
                udf.close();
            }
        }
    }

    private static void scanTest(HBaseIsExistUdf udf) throws IOException {
        Scanner scanner = new Scanner(System.in);
        String playerId;
        while (!(playerId = scanner.nextLine()).equals("exit")) {
            Long result = udf.eval(playerId);
            System.out.println(result);
        }
    }

    private static FunctionContext getFunctionContext(int taskNum, int tasks) {
        return new FunctionContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return null;
            }

            @Override
            public File getCachedFile(String s) {
                return null;
            }

            @Override
            public int getNumberOfParallelSubtasks() {
                return tasks;
            }

            @Override
            public int getIndexOfThisSubtask() {
                return taskNum;
            }

            @Override
            public IntCounter getIntCounter(String s) {
                return null;
            }

            @Override
            public LongCounter getLongCounter(String s) {
                return null;
            }

            @Override
            public DoubleCounter getDoubleCounter(String s) {
                return null;
            }

            @Override
            public Histogram getHistogram(String s) {
                return null;
            }

            Map<String, String> map = new HashMap<>();

            {
                map.put("hbase.table.name", "new_user");
                map.put("hbase.zookeeper.quorum", "192.168.0.92:2181");
                map.put("hbase.client.username", "");
                map.put("hbase.client.password", "");
            }

            @Override
            public String getJobParameter(String s, String s1) {
                return map.getOrDefault(s, s1);
            }
        };
    }
}
