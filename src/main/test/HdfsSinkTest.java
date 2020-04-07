import com.idreamsky.dc.blink.sink.HdfsSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author colby.luo
 * @date 2020/3/26 21:23
 */
public class HdfsSinkTest {
    public static void main(String[] args) throws Exception {
        HdfsSink HDFSSink = new HdfsSink();

        setTypeInfo(HDFSSink);

        setUserParamMap(HDFSSink);

        HDFSSink.open(0, 1);

        registerListener(HDFSSink);

        doWrite(HDFSSink);

        HDFSSink.close();
    }

    private static void registerListener(HdfsSink HDFSSink) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    HDFSSink.sync();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 10000);

       /* timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    HDFSSink.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 10000,100);*/
    }

    private static void doWrite(HdfsSink HDFSSink) throws IOException, InterruptedException {
        String msg = "182.254.233.99|1585032575|AdData|1001||2a39556a863a5edc978d|com.indiesky.catcondo|3.0.0||0|iPhone|iPhone10,5|1242*2208|020000000000|Mozilla/5.0 (iPhone; CPU iPhone OS 13_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148|0|ios|13.3.1|1585032574|1.5.3|1EEE62DE-B741-4061-AA57-7182EF93F50C|0|1|||||126.0.63.233|1585032574|24DA5C01-202F-42A4-9238-E7466D3B4930||34635|bd8c591a882b54014897e0a011416bb3|1";
        Row row = Row.of("AdData1", msg, "20200101", "10");


        for (int i = 0; i < 100000; i++) {
            HDFSSink.writeAddRecord(row);
            Thread.sleep(10);
        }
    }

    private static void setUserParamMap(HdfsSink HDFSSink) {
        HashMap<String, String> userParamMap = new HashMap<>();
        userParamMap.put("hdfs.namenode", "192.168.0.92:8020,192.168.0.94:8020");
        HDFSSink.setUserParamsMap(userParamMap);
    }

    private static void setTypeInfo(HdfsSink HDFSSink) {
        TypeInformation<String> stringType = TypeInformation.of(String.class);
        TypeInformation<?>[] types = new TypeInformation[]{stringType, stringType, stringType, stringType};
        String[] fieldNames = {"table", "message", "date", "hour"};

        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
        HDFSSink.setRowTypeInfo(rowTypeInfo);
    }
}
