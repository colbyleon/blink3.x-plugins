import com.idreamsky.dc.blink.sink.KafkaSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author colby.luo
 * @date 2020/4/1 17:05
 */
public class KafkaSinkTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        KafkaSink kafkaSink = new KafkaSink();
        setUserParamMap(kafkaSink);
        setTypeInfo(kafkaSink);
        kafkaSink.open(0, 1);

        registerListener(kafkaSink);

        sendRecord(kafkaSink);

        TimeUnit.SECONDS.sleep(2);
        kafkaSink.close();
    }

    private static void registerListener(KafkaSink kafkaSink) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    kafkaSink.sync();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 1000);
    }

    private static void sendRecord(KafkaSink kafkaSink) throws IOException, InterruptedException {
        List<String> topics = Arrays.asList("demo5", "demo100");
        Random random = new Random();

        for (int i = 0; i < 1000; i++) {
            Row row = Row.of("hello", topics.get(random.nextInt(topics.size())), Instant.now().toEpochMilli());
            kafkaSink.writeAddRecord(row);
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    private static void setTypeInfo(KafkaSink kafkaSink) {
        TypeInformation<String> stringType = TypeInformation.of(String.class);
        TypeInformation<Long> longType = TypeInformation.of(Long.class);
        TypeInformation<?>[] types = new TypeInformation[]{stringType, stringType, longType};

        String[] fieldNames = {"message", "topic", "ts"};

        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
        kafkaSink.setRowTypeInfo(rowTypeInfo);
    }

    private static void setUserParamMap(KafkaSink kafkaSink) {
        Map<String, String> userParamMap = new HashMap<>();
        userParamMap.put("bootstrap.servers", "192.168.0.92:9292,192.168.0.94:9292,192.168.0.95:9292");

//        userParamMap.put("bootstrap.servers", "192.168.0.92:9293");

        kafkaSink.setUserParamsMap(userParamMap);
    }
}
