package com.wxeo.it;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test against the native Kafka KRaft stack (artifact/harness/native-stack.ps1
 * start-kafka). Excluded from the default {@code mvn test}; run with
 * {@code mvn verify -Pintegration}. Skipped (assumption) when the brokers are not reachable.
 */
@Tag("integration")
class KafkaSmokeIT {

    private static final String BOOTSTRAP = System.getProperty("wxeo.it.kafka.bootstrap", "127.0.0.1:9091");

    private static Admin adminOrSkip() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("default.api.timeout.ms", "5000");
        props.put("request.timeout.ms", "5000");
        Admin admin = Admin.create(props);
        try {
            admin.describeCluster().clusterId().get(5, TimeUnit.SECONDS);
            return admin;
        } catch (Exception e) {
            try {
                admin.close();
            } catch (Exception ignored) {
                // ignore
            }
            assumeTrue(false, "native Kafka not reachable at " + BOOTSTRAP + ": " + e.getMessage());
            return null;
        }
    }

    @Test
    void createProduceConsumeDeleteTopicWithJavaClients() throws Exception {
        Admin admin = adminOrSkip();
        String topic = "wxeo-it-smoke-" + System.nanoTime();
        int partitions = 3;
        int records = 30;
        try {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 3))).all().get(30, TimeUnit.SECONDS);

            Properties pp = new Properties();
            pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
            pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            pp.put(ProducerConfig.ACKS_CONFIG, "all");
            pp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(pp)) {
                for (int i = 0; i < records; i++) {
                    producer.send(new ProducerRecord<>(topic, i % partitions, "k" + i, "v" + i));
                }
                producer.flush();
            }

            Properties cp = new Properties();
            cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
            cp.put(ConsumerConfig.GROUP_ID_CONFIG, "wxeo-it-" + topic);
            cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            int consumed = 0;
            Set<Integer> seenPartitions = new java.util.TreeSet<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp)) {
                consumer.subscribe(List.of(topic));
                long deadline = System.currentTimeMillis() + 30_000;
                while (consumed < records && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(300));
                    consumed += recs.count();
                    for (var r : recs) {
                        seenPartitions.add(r.partition());
                    }
                }
            }
            assertEquals(records, consumed, "all produced records consumed");
            assertEquals(partitions, seenPartitions.size(), "explicit partition assignment reached all partitions");
        } finally {
            try {
                admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // best effort cleanup
            }
            admin.close();
        }
    }
}
