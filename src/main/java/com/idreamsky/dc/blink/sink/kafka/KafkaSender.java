package com.idreamsky.dc.blink.sink.kafka;

import com.idreamsky.dc.blink.common.utils.GLogger;
import com.idreamsky.dc.blink.common.utils.GLoggerFactory;
import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author colby.luo
 * @date 2020/4/1 15:16
 */
@Data
public class KafkaSender {

    private static GLogger log = GLoggerFactory.getGLogger();

    private Integer numTasks;

    private Integer taskNumber;

    private Map<String, Object> kafkaConfig = new HashMap<>();

    public static final String[] OPTIONAL_PRODUCER_KEYS = new String[]{
        "bootstrap.servers", "key.serializer", "value.serializer", "acks", "buffer.memory", "long", "compression.type",
        "retries", "ssl.key.password", "ssl.keystore.location", "ssl.keystore.password", "ssl.truststore.location",
        "ssl.truststore.password", "batch.size", "client.id", "connections.max.idle.ms", "linger.ms", "max.block.ms",
        "max.request.size", "partitioner.class", "receive.buffer.bytes", "request.timeout.ms", "sasl.jaas.config",
        "sasl.kerberos.service.name", "sasl.mechanism", "security.protocol", "send.buffer.bytes",
        "ssl.enabled.protocols", "ssl.keystore.type", "ssl.protocol", "ssl.provider", "ssl.truststore.type",
        "enable.idempotence", "interceptor.classes", "max.in.flight.requests.per.connection", "metadata.max.age.ms",
        "metric.reporters", "metrics.num.samples", "metrics.recording.level", "metrics.sample.window.ms",
        "reconnect.backoff.max.ms", "reconnect.backoff.ms", "retry.backoff.ms", "sasl.kerberos.kinit.cmd",
        "sasl.kerberos.min.time.before.relogin", "sasl.kerberos.ticket.renew.jitter",
        "sasl.kerberos.ticket.renew.window.factor", "ssl.cipher.suites", "ssl.endpoint.identification.algorithm",
        "ssl.keymanager.algorithm", "ssl.secure.random.implementation", "ssl.trustmanager.algorithm",
        "transaction.timeout.ms", "transactional.id"};


    public static final Map<String, Object> DEFAULT_CONFIG = new HashMap<>();

    static {
        DEFAULT_CONFIG.put("bootstrap.servers", "这里是线上的默认地址");
        DEFAULT_CONFIG.put("acks", "all");
        DEFAULT_CONFIG.put("retries", 10);
        DEFAULT_CONFIG.put("key.serializer", StringSerializer.class.getName());
        DEFAULT_CONFIG.put("value.serializer", StringSerializer.class.getName());
    }

    private KafkaProducer<String, String> producer;

    public static KafkaSender create(int taskNumber, int numTasks, Map<String, String> userParamsMap) {
        KafkaSender sender = new KafkaSender();
        sender.setNumTasks(numTasks);
        sender.setTaskNumber(taskNumber);

        // 从用户参数读取配置
        for (String key : OPTIONAL_PRODUCER_KEYS) {
            if (userParamsMap.containsKey(key)) {
                sender.addKafkaConfig(key, userParamsMap.get(key));
            }
        }

        sender.init();

        log.info("KafkaSender 初始化完成 {}/{}", taskNumber, numTasks);

        return sender;
    }

    private void init() {
        // 默认配置
        DEFAULT_CONFIG.forEach((k, v) -> kafkaConfig.computeIfAbsent(k, s -> v));

        log.info("kafka 配置 {}", kafkaConfig);

        try {
            // 检查 地址是否可通, 不能使用 kafkaAdmin 放到 Blink 里会报错
            this.producer = new KafkaProducer<>(kafkaConfig);
            log.info("KafkaProducer 创建成功");
        } catch (Exception e) {
            log.error("kafkaProducer 创建失败", e);
            throw new IllegalStateException(e);
        }
    }

    private void addKafkaConfig(String key, Object value) {
        kafkaConfig.put(key, value);
    }

    public void send(ProducerRecord<String, String> record) {
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("kafka 消息发送失败: {}", record, exception);
                }
            });
        } catch (Exception e) {
            log.error("kafka 消息发送失败 record: {}", record, e);
            throw e;
        }
    }

    public void sync() {
        try {
            producer.flush();
        } catch (Exception e) {
            log.error("kafka producer flush 失败", e);
            throw e;
        }
    }

    public void close() {
        try {
            producer.close(Duration.ofMinutes(1));
        } catch (Exception e) {
            log.error("kafkaProducer 关闭失败", e);
            throw e;
        }
    }
}
