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
public class HBaseSetIfNotExistUdfTest {

    /**
     * todo
     * 本地测试先注释掉  conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
     */
    public static void main(String[] args) throws Exception {
        HBaseSetIfNotExistUdf udf = null;
        try {
            udf = new HBaseSetIfNotExistUdf();
            udf.open(getFunctionContext(0, 1));

        scanTest(udf);

//        batchTest(udf);

//        parallelTest();

//            tableNotExistTest(udf);

//            udf.close();
        } finally {
            if (udf != null) {
                udf.close();
            }
        }
    }

    private static void tableNotExistTest(HBaseSetIfNotExistUdf udf) throws IOException {
        System.out.println(udf.eval("123213", LocalDateTime.now().toString(), "notExistTable"));
    }

    private static void parallelTest() throws Exception {
        int parallel = 4;
        List<HBaseSetIfNotExistUdf> udfs = new ArrayList<>(parallel);
        AtomicIntegerArray counterArray = new AtomicIntegerArray(parallel);
        Set<String> discounter = new HashSet<>();

        for (int i = 0; i < parallel; i++) {
            HBaseSetIfNotExistUdf udf = new HBaseSetIfNotExistUdf();
            udf.open(getFunctionContext(i, parallel));
            udfs.add(udf);
        }

        Random random = new Random();
        AtomicReference<String> rowKeyRef = new AtomicReference<>(String.valueOf(random.nextInt(100000)));
        AtomicBoolean exit = new AtomicBoolean(false);

        // 每次消费同一份数据,而且都是同时开始
        CyclicBarrier barrier = new CyclicBarrier(parallel, new Runnable() {
            int n = 0;

            @Override
            public void run() {
                if (exit.get()) {
                    return;
                }
                // 由最后一个await()方法调用
                String newValue = String.valueOf(random.nextInt(100000));
                rowKeyRef.set(newValue);
                discounter.add(newValue);
                if (++n % 5000 == 0) {
                    System.out.println(n);
                }
            }
        });
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        for (int i = 0; i < udfs.size(); i++) {
            HBaseSetIfNotExistUdf udf = udfs.get(i);
            int finalI = i;
            executorService.execute(() -> {
                while (!exit.get()) {
                    try {
                        String rowKey = rowKeyRef.get();
                        barrier.await();
                        Long result = udf.eval(rowKey, finalI + "", "new_user");
                        counterArray.addAndGet(finalI, result.intValue());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                try {
                    barrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        Scanner scanner = new Scanner(System.in);
        String cmd = null;
        while (!(cmd = scanner.nextLine()).equals("exit")) {
            System.out.println(cmd + " 命令不支持，退出输入 exit");
        }

        // 退出
        exit.set(true);
        executorService.shutdown();
        System.out.println("正在退出...");
        executorService.awaitTermination(100, TimeUnit.MINUTES);

        System.out.println("实际： " + discounter.size());
        int sum = 0;
        for (int i = 0; i < parallel; i++) {
            System.out.println("线程" + i + ": " + counterArray.get(i));
            sum += counterArray.get(i);
        }
        System.out.println("线程总计：" + sum);

        for (HBaseSetIfNotExistUdf udf : udfs) {
            udf.close();
        }
    }

    /**
     * native: 43167  hbase: 43167  cost: 84513
     */
    private static void batchTest(HBaseSetIfNotExistUdf udf) throws IOException {
        Set<String> set = new HashSet<>(500000);
        Random random = new Random();

        int count = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            String rowKey = random.nextInt(50000) + "";
            set.add(rowKey);
            count += udf.eval(rowKey, "vvv");
            if (i != 0 && i % 1000 == 0) {
                System.out.println(i);
            }
        }

        System.out.printf("native: %d  hbase: %d  cost: %d%n", set.size(), count, System.currentTimeMillis() - start);

    }

    private static void scanTest(HBaseSetIfNotExistUdf udf) throws IOException {
        Scanner scanner = new Scanner(System.in);
        String playerId;
        while (!(playerId = scanner.nextLine()).equals("exit")) {
            Long result = udf.eval(playerId, LocalDateTime.now().toString());
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
