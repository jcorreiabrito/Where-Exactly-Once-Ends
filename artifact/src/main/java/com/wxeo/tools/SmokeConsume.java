package com.wxeo.tools;

import com.wxeo.config.Args;
import com.wxeo.util.Json;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Java-client smoke consumer (acceptance test 3 and operator verification of the native stack).
 *
 * <pre>
 * java -cp artifact/target/wxeo-service.jar com.wxeo.tools.SmokeConsume \
 *   --bootstrap=127.0.0.1:9092 --topic=smoke --group=smoke-g --count=100 --timeout-ms=30000
 * </pre>
 *
 * <p>Prints one JSON line with the consumed count, per-partition offsets and a value digest; exit
 * code 0 iff the expected count was reached.</p>
 */
public final class SmokeConsume {

    public static void main(String[] argv) {
        Args a = Args.parse(argv);
        String bootstrap = a.get("bootstrap", "127.0.0.1:9092");
        String topic = a.get("topic", "smoke");
        String group = a.get("group", "wxeo-smoke-" + System.currentTimeMillis());
        int count = a.getInt("count", 100);
        long timeoutMs = a.getLong("timeout-ms", 30_000);
        boolean fromBeginning = a.getBool("from-beginning", true);
        boolean readCommitted = a.getBool("read-committed", false);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromBeginning ? "earliest" : "latest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, readCommitted ? "read_committed" : "read_uncommitted");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        long digest = 0;
        int consumed = 0;
        long firstOffset = -1;
        long lastOffset = -1;
        Map<Integer, Long> perPartition = new java.util.TreeMap<>();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("schema", "wxeo.smoke.consume.v1");
        out.put("topic", topic);
        out.put("bootstrap", bootstrap);
        out.put("group", group);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (consumed < count && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> r : records) {
                    if (firstOffset < 0) {
                        firstOffset = r.offset();
                    }
                    lastOffset = r.offset();
                    perPartition.merge(r.partition(), 1L, Long::sum);
                    digest = 31 * digest + (r.value() == null ? 0 : r.value().hashCode());
                    consumed++;
                }
            }
            var assignment = consumer.assignment();
            out.put("assigned_partitions", assignment.stream().map(TopicPartition::partition).sorted().toList());
        }
        out.put("consumed", consumed);
        out.put("expected", count);
        out.put("first_offset", firstOffset < 0 ? null : firstOffset);
        out.put("last_offset", lastOffset < 0 ? null : lastOffset);
        out.put("records_per_partition", perPartition);
        out.put("value_hash", Long.toHexString(digest));
        out.put("ok", consumed >= count);
        System.out.println(Json.write(out));
        System.exit(consumed >= count ? 0 : 1);
    }

    private SmokeConsume() {
    }
}
