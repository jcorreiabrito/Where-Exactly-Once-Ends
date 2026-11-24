package com.wxeo.publish;

import com.wxeo.config.EffectiveConfig;
import com.wxeo.failpoint.FailpointNames;
import com.wxeo.failpoint.Failpoints;
import com.wxeo.ledger.Ledger;
import com.wxeo.metrics.RunMetrics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The publication stream {@code wxeo.sink.<run_id>} (docs/method.md §1.4.3, §3.4).
 *
 * <p>One derived entry record per processed journal entry, key = {@code event_id}, value = the
 * canonical {@code wxeo.entry.v1} JSON, produced to the <b>same partition index</b> as the source
 * record. DB mechanisms publish outside any transaction (at-least-once); M1x/ M1-kafka write inside
 * the Kafka transaction using {@link #recordFor} directly.</p>
 */
public final class Publication {

    private final EffectiveConfig cfg;
    private final ProducerProvider providers;
    private final RunMetrics metrics;

    public Publication(EffectiveConfig cfg, ProducerProvider providers, RunMetrics metrics) {
        this.cfg = cfg;
        this.providers = providers;
        this.metrics = metrics;
    }

    public ProducerRecord<String, String> recordFor(Ledger.ParsedEvent e) {
        return new ProducerRecord<>(cfg.sinkTopic(), e.partition(), e.event().eventId(),
                e.event().entryValueJson());
    }

    /** Inline non-transactional publication of a batch (M0, M3, M4, M4+M3). */
    public void publishInline(List<Ledger.ParsedEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        Failpoints.fire(FailpointNames.BEFORE_PUBLISH);
        List<Future<RecordMetadata>> futures = new ArrayList<>(events.size());
        for (Ledger.ParsedEvent e : events) {
            try {
                futures.add(providers.plain().send(recordFor(e)));
                metrics.inc("publication_sent");
            } catch (RuntimeException ex) {
                metrics.inc("publication_errors");
            }
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        for (Future<RecordMetadata> f : futures) {
            try {
                long remaining = Math.max(1, deadline - System.nanoTime());
                f.get(remaining, TimeUnit.NANOSECONDS);
            } catch (Exception ex) {
                metrics.inc("publication_errors");
            }
        }
    }

    /**
     * Relay publication of one outbox row (M2, M2+M3). Blocks until the broker acknowledges.
     * Fires {@code before_publish} before the send; the caller fires {@code before_relay_mark}
     * after the ack and before marking the row published.
     */
    public void sendRawBlocking(String eventId, int partition, String payload) throws Exception {
        Failpoints.fire(FailpointNames.BEFORE_PUBLISH);
        ProducerRecord<String, String> record = new ProducerRecord<>(cfg.sinkTopic(), partition,
                eventId, payload);
        providers.plain().send(record).get(60, TimeUnit.SECONDS);
        metrics.inc("publication_sent");
    }

    /** Blocking single-entry publication used by the M2 relay. */
    public void sendOneBlocking(Ledger.EventData event, int partition) throws Exception {
        sendRawBlocking(event.eventId(), partition, event.entryValueJson());
    }
}
