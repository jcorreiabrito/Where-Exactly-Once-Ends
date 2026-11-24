package com.wxeo.publish;

import com.wxeo.config.EffectiveConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

/** Lazily-created Kafka producers shared by a mechanism instance (plain and transactional). */
public final class ProducerProvider implements AutoCloseable {

    private final EffectiveConfig cfg;
    private KafkaProducer<String, String> plain;
    private KafkaProducer<String, String> transactional;

    public ProducerProvider(EffectiveConfig cfg) {
        this.cfg = cfg;
    }

    /** Publication producer for DB mechanisms (acks=all, idempotent, no transaction). */
    public synchronized KafkaProducer<String, String> plain() {
        if (plain == null) {
            plain = new KafkaProducer<>(cfg.producerProps());
        }
        return plain;
    }

    /** Transactional producer for M1-kafka / M1x / M1x+M3 (one transaction per poll batch). */
    public synchronized KafkaProducer<String, String> transactional() {
        if (transactional == null) {
            var props = cfg.producerProps();
            props.put(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    "wxeo-" + cfg.runId() + "-" + cfg.instanceId());
            props.put(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);
            transactional = new KafkaProducer<>(props);
            transactional.initTransactions();
        }
        return transactional;
    }

    public synchronized KafkaProducer<String, String> currentTransactional() {
        return transactional;
    }

    @Override
    public synchronized void close() {
        if (transactional != null) {
            try {
                transactional.close(java.time.Duration.ofSeconds(10));
            } catch (RuntimeException ignored) {
                // closing a fenced/broken producer must not mask shutdown work
            }
            transactional = null;
        }
        if (plain != null) {
            try {
                plain.close(java.time.Duration.ofSeconds(10));
            } catch (RuntimeException ignored) {
                // ignore
            }
            plain = null;
        }
    }
}
