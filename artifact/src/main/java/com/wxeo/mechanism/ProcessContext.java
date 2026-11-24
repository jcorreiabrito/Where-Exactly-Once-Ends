package com.wxeo.mechanism;

import com.wxeo.config.EffectiveConfig;
import com.wxeo.ledger.Ledger;
import com.wxeo.metrics.RunMetrics;
import com.wxeo.publish.ProducerProvider;
import com.wxeo.publish.Publication;
import com.wxeo.sink.PostgresSink;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/** Shared collaborators handed to a mechanism instance. */
public final class ProcessContext {

    public final EffectiveConfig cfg;
    public final RunMetrics metrics;
    public final KafkaConsumer<String, String> consumer;
    public final ProducerProvider providers;
    public final Publication publication;
    public final PostgresSink sink; // null for M1-kafka (no external DB sink)

    public ProcessContext(EffectiveConfig cfg, RunMetrics metrics, KafkaConsumer<String, String> consumer,
                          ProducerProvider providers, Publication publication, PostgresSink sink) {
        this.cfg = cfg;
        this.metrics = metrics;
        this.consumer = consumer;
        this.providers = providers;
        this.publication = publication;
        this.sink = sink;
    }

    public boolean hasSink() {
        return sink != null;
    }

    public PostgresSink requireSink() {
        if (sink == null) {
            throw new IllegalStateException("mechanism " + cfg.mechanism().wireName + " has no DB sink");
        }
        return sink;
    }
}
