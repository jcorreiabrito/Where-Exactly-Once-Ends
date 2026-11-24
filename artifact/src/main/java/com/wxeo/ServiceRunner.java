package com.wxeo;

import com.wxeo.config.EffectiveConfig;
import com.wxeo.failpoint.FailpointNames;
import com.wxeo.failpoint.Failpoints;
import com.wxeo.ledger.Ledger;
import com.wxeo.mechanism.Batch;
import com.wxeo.mechanism.Mechanism;
import com.wxeo.mechanism.MechanismFactory;
import com.wxeo.mechanism.ProcessContext;
import com.wxeo.metrics.RunMetrics;
import com.wxeo.publish.ProducerProvider;
import com.wxeo.publish.Publication;
import com.wxeo.sink.PostgresSink;
import com.wxeo.sink.SqlRetry.FatalMechanismException;
import com.wxeo.sink.SqlRetry.UnknownOutcomeException;
import com.wxeo.util.IoUtil;
import com.wxeo.util.Json;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Headless consumer-service runtime: owns the Kafka consumer, the mechanism instance, the sink, the
 * metrics and the graceful-shutdown artifacts (docs/CONTRACTS.md §6, §7).
 *
 * <p>Exactly one JVM per consumer instance. Kill-safe by design: an abrupt kill produces no
 * artifacts (documented as a crash in the runner); a graceful stop (stop-flag file or SIGTERM) writes
 * {@code app_<run>_<instance>.json} and the histogram dumps.</p>
 */
public final class ServiceRunner {

    private static final Logger log = LoggerFactory.getLogger(ServiceRunner.class);

    private final EffectiveConfig cfg;
    private final RunMetrics metrics;
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final AtomicBoolean artifactsWritten = new AtomicBoolean(false);
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final Object terminationMonitor = new Object();

    private volatile String stopReason = "";
    private volatile String fatalError;
    private volatile List<Integer> assignedPartitions = List.of();
    private volatile boolean subscribed;
    private volatile PostgresSink.Attestation attestation;

    private KafkaConsumer<String, String> consumer;
    private ProducerProvider providers;
    private Publication publication;
    private PostgresSink sink;
    private Mechanism mechanism;
    private volatile long stopMs;

    public ServiceRunner(EffectiveConfig cfg) {
        this.cfg = cfg;
        this.metrics = new RunMetrics(cfg);
    }

    public EffectiveConfig config() {
        return cfg;
    }

    public RunMetrics metrics() {
        return metrics;
    }

    public boolean fatalError() {
        return fatalError != null;
    }

    public String fatalErrorMessage() {
        return fatalError;
    }

    // ---------------------------------------------------------------------------------------------

    /** Runs the service loop until a graceful stop is requested or a fatal condition occurs. */
    public void runBlocking() {
        try {
            writeConfigFile();
            Failpoints.init(cfg);
            createInfrastructure();
            mechanism = MechanismFactory.create(new ProcessContext(cfg, metrics, consumer, providers,
                    publication, sink));
            log.info("[wxeo] instance {} mechanism={} ordering={} group={} source={} sink={}",
                    cfg.instanceId(), cfg.mechanism().wireName,
                    cfg.orderEarly() ? "early" : "strict", cfg.group(), cfg.sourceTopic(), cfg.sinkTopic());
            mechanism.init();
            subscribe();
            writeReadyFile("subscribed");
            consumeLoop();
        } catch (FatalMechanismException e) {
            fatalError = e.getMessage();
            log.error("[wxeo] fatal mechanism error: {}", e.getMessage(), e);
        } catch (Throwable t) {
            fatalError = t.getClass().getSimpleName() + ": " + t.getMessage();
            log.error("[wxeo] service failure", t);
        } finally {
            shutdown();
        }
    }

    private void createInfrastructure() {
        if (cfg.mechanism().dbSink()) {
            sink = new PostgresSink(cfg);
            try {
                attestation = sink.attestation();
                log.info("[wxeo] append-only attestation: can_update={} can_delete={} triggers={}",
                        attestation.canUpdate(), attestation.canDelete(), attestation.triggerCount());
                if (cfg.failOnAttestationFailure() && !attestation.appendOnly()) {
                    throw new IllegalStateException("append-only attestation failed for role "
                            + cfg.dbUser() + ": " + attestation);
                }
            } catch (java.sql.SQLException e) {
                log.warn("[wxeo] attestation query failed: {}", e.getMessage());
            }
        } else {
            log.info("[wxeo] mechanism {} has no external DB sink (Kafka-sink positive control)",
                    cfg.mechanism().wireName);
        }
        providers = new ProducerProvider(cfg);
        publication = new Publication(cfg, providers, metrics);
        Map<String, Object> props = cfg.consumerProps();
        consumer = new KafkaConsumer<>(props);
    }

    private void subscribe() {
        consumer.subscribe(List.of(cfg.sourceTopic()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("[wxeo] rebalance revoke {}", partitions);
                try {
                    mechanism.onRevoke(partitions);
                } catch (RuntimeException e) {
                    log.warn("[wxeo] revoke hook failed: {}", e.getMessage());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                metrics.inc("rebalances");
                List<Integer> ps = partitions.stream().map(TopicPartition::partition).sorted().toList();
                assignedPartitions = ps;
                log.info("[wxeo] rebalance assign {} (instance {})", partitions, cfg.instanceId());
                try {
                    Failpoints.fire(FailpointNames.AFTER_FETCH); // no-op unless armed for this point
                    mechanism.onAssignment(partitions);
                } catch (Exception e) {
                    log.error("[wxeo] assignment hook failed", e);
                }
                writeReadyFile("assigned");
            }
        });
    }

    private void consumeLoop() {
        while (!stopRequested.get()) {
            if (stopFlagPresent()) {
                stopReason = "stop-file";
                break;
            }
            ConsumerRecords<String, String> records;
            try {
                records = consumer.poll(Duration.ofMillis(200));
            } catch (WakeupException e) {
                stopReason = stopReason.isEmpty() ? "wakeup" : stopReason;
                break;
            } catch (RuntimeException e) {
                metrics.inc("poll_errors");
                log.warn("[wxeo] poll failed: {}", e.toString());
                IoUtil.sleepQuietly(100);
                continue;
            }
            metrics.inc("batches_polled");
            metrics.inc("records_consumed", records.count());
            if (records.isEmpty()) {
                continue;
            }
            Failpoints.fire(FailpointNames.AFTER_FETCH);
            Batch batch = buildBatch(records);
            try {
                mechanism.process(batch);
            } catch (UnknownOutcomeException e) {
                metrics.inc("unknown_outcome_events");
                log.warn("[wxeo] unknown-outcome path: {} — not committing progress, replaying batch",
                        e.getMessage());
                seekBack(batch);
                IoUtil.sleepQuietly(50);
            } catch (com.wxeo.failpoint.InjectedFaultException e) {
                handleInjectedFault(batch, e.getClass().getSimpleName(), e.point());
            } catch (com.wxeo.failpoint.InjectedAbortException e) {
                handleInjectedFault(batch, e.getClass().getSimpleName(), e.point());
            } catch (FatalMechanismException e) {
                fatalError = e.getMessage();
                log.error("[wxeo] fatal mechanism condition, stopping instance", e);
                break;
            } catch (Exception e) {
                metrics.inc("process_errors");
                log.warn("[wxeo] batch processing failed ({}); replaying batch", e.toString());
                seekBack(batch);
                IoUtil.sleepQuietly(100);
            }
        }
    }

    private Batch buildBatch(ConsumerRecords<String, String> records) {        List<Ledger.ParsedEvent> events = new ArrayList<>(records.count());
        int poison = 0;
        var partitions = new ArrayList<>(records.partitions());
        partitions.sort(Comparator.comparingInt(TopicPartition::partition));
        Map<TopicPartition, OffsetAndMetadata> next = new LinkedHashMap<>();
        Map<TopicPartition, Long> end = new LinkedHashMap<>();
        Map<TopicPartition, Long> start = new LinkedHashMap<>();
        for (TopicPartition tp : partitions) {
            List<ConsumerRecord<String, String>> list = records.records(tp);
            if (list.isEmpty()) {
                continue;
            }
            long first = list.get(0).offset();
            long last = list.get(list.size() - 1).offset();
            start.put(tp, first);
            end.put(tp, last + 1);
            next.put(tp, new OffsetAndMetadata(last + 1));
            for (ConsumerRecord<String, String> rec : list) {
                try {
                    Ledger.EventData ev = Ledger.parse(rec.value());
                    events.add(new Ledger.ParsedEvent(ev, rec.partition(), rec.offset(), rec.value()));
                } catch (RuntimeException e) {
                    poison++;
                    metrics.inc("poison_records");
                    log.warn("[wxeo] poison record at {}-{}@{}: {} (skipped, progress still committed)",
                            rec.topic(), rec.partition(), rec.offset(), e.getMessage());
                }
            }
        }
        metrics.inc("events_processed", events.size());
        return new Batch(events, poison, next, end, start, consumer.groupMetadata(),
                System.currentTimeMillis());
    }

    private void handleInjectedFault(Batch batch, String kind, String point) {
        metrics.inc("injected_faults");
        Failpoints.restored(point);
        log.info("[wxeo] injected {} at {}; rolling back and replaying batch", kind, point);
        mechanism.recoverAfterInjectedFault();
        seekBack(batch);
    }

    private void seekBack(Batch batch) {
        try {
            for (Map.Entry<TopicPartition, Long> e : batch.startOffsets().entrySet()) {
                consumer.seek(e.getKey(), e.getValue());
            }
        } catch (RuntimeException e) {
            log.warn("[wxeo] seek-back failed: {}", e.getMessage());
        }
    }

    private boolean stopFlagPresent() {
        try {
            return Files.exists(cfg.stopFile());
        } catch (RuntimeException e) {
            return false;
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** Requests a graceful stop (stop-file polled in the loop; SIGTERM/SIGINT via the shutdown hook). */
    public void requestStop(String reason) {
        if (stopRequested.compareAndSet(false, true)) {
            stopReason = reason;
        }
        KafkaConsumer<String, String> c = consumer;
        if (c != null) {
            try {
                c.wakeup();
            } catch (RuntimeException ignored) {
                // consumer already closed
            }
        }
    }

    public void awaitTermination(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (terminationMonitor) {
            while (!terminated.get() && System.currentTimeMillis() < deadline) {
                try {
                    terminationMonitor.wait(Math.max(1, deadline - System.currentTimeMillis()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void shutdown() {
        stopMs = System.currentTimeMillis();
        try {
            if (mechanism != null) {
                mechanism.close();
            }
        } catch (Exception e) {
            log.warn("[wxeo] mechanism close failed: {}", e.getMessage());
        }
        for (Runnable r : new Runnable[]{
                () -> {
                    if (consumer != null) {
                        consumer.close(Duration.ofSeconds(10));
                    }
                },
                () -> {
                    if (providers != null) {
                        providers.close();
                    }
                },
                () -> {
                    if (sink != null) {
                        sink.close();
                    }
                }}) {
            try {
                r.run();
            } catch (RuntimeException e) {
                log.warn("[wxeo] shutdown step failed: {}", e.getMessage());
            }
        }
        writeApplicationArtifacts();
        terminated.set(true);
        synchronized (terminationMonitor) {
            terminationMonitor.notifyAll();
        }
        log.info("[wxeo] instance {} stopped (reason={}, fatal={})", cfg.instanceId(), stopReason, fatalError);
    }

    /** Idempotent; safe to call again from the JVM shutdown hook path. */
    public void writeApplicationArtifacts() {
        if (!artifactsWritten.compareAndSet(false, true)) {
            return;
        }
        metrics.markStopped();
        try {
            Map<String, Object> histPaths = metrics.writeHistograms();
            Map<String, Object> app = new LinkedHashMap<>();
            app.put("schema", "wxeo.app.v1");
            app.put("run_id", cfg.runId());
            app.put("instance_id", cfg.instanceId());
            app.put("mechanism", cfg.mechanism().wireName);
            app.put("ordering", cfg.orderEarly() ? "early" : "strict");
            app.put("start_utc", IoUtil.utcIso(metrics.startMs()));
            app.put("stop_utc", IoUtil.utcIso(metrics.stopMs()));
            app.put("stop_reason", stopReason);
            app.put("fatal_error", fatalError);
            app.put("jvm_args", java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments());
            app.put("java_version", System.getProperty("java.version"));
            app.put("counters", metrics.counterSnapshot());
            app.put("performance", metrics.performanceBlock());
            app.put("elapsed_seconds", metrics.elapsedSeconds());
            app.put("effective_config", Json.parse(cfg.fileJson()));
            app.put("config_sha256", cfg.fullSha256());
            app.put("shared_sha256", cfg.sharedSha256());
            app.put("mechanism_impl", mechanism == null ? null : mechanism.id());
            app.put("assigned_partitions", assignedPartitions);
            app.put("subscribed", subscribed);
            app.put("histograms", histPaths);
            if (attestation != null) {
                app.put("attestation", Map.of("can_update", attestation.canUpdate(),
                        "can_delete", attestation.canDelete(),
                        "trigger_count", attestation.triggerCount(),
                        "append_only", attestation.appendOnly()));
            }
            Path appPath = cfg.outputDir().resolve("app_" + cfg.runId() + "_" + cfg.instanceId() + ".json");
            IoUtil.writeAtomicFsync(appPath, Json.writePretty(app));
            log.info("[wxeo] wrote {}", appPath);
        } catch (RuntimeException e) {
            log.error("[wxeo] cannot write application artifacts: {}", e.getMessage(), e);
        }
    }

    private void writeConfigFile() {
        Path path = cfg.outputDir().resolve("config_" + cfg.runId() + "_" + cfg.instanceId() + ".json");
        IoUtil.writeAtomicFsync(path, Json.writePretty(Json.parse(cfg.fileJson())));
        log.info("[wxeo] effective config {} (shared_sha256={} config_sha256={})",
                path, cfg.sharedSha256(), cfg.fullSha256());
        System.out.println("WXEO_CONFIG_FILE=" + path);
        System.out.println("WXEO_SHARED_CONFIG_SHA256=" + cfg.sharedSha256());
        System.out.println("WXEO_CONFIG_SHA256=" + cfg.fullSha256());
    }

    private void writeReadyFile(String status) {
        try {
            subscribed = subscribed || status.equals("subscribed") || status.equals("assigned");
            Map<String, Object> ready = new LinkedHashMap<>();
            ready.put("schema", "wxeo.ready.v1");
            ready.put("run_id", cfg.runId());
            ready.put("instance_id", cfg.instanceId());
            ready.put("mechanism", cfg.mechanism().wireName);
            ready.put("group", cfg.group());
            ready.put("status", status);
            ready.put("partitions", assignedPartitions);
            ready.put("ts_utc", IoUtil.utcNowIso());
            Path path = cfg.outputDir().resolve("ready_" + cfg.runId() + "_" + cfg.instanceId() + ".json");
            IoUtil.writeAtomicFsync(path, Json.write(ready));
        } catch (RuntimeException e) {
            log.warn("[wxeo] cannot write readiness file: {}", e.getMessage());
        }
    }

    /** Utility for tests/monitoring: current histogram percentiles path. */
    public String percentilesPath() {
        return metrics.percentilesPath();
    }

    /** Test-only access to the underlying consumer once created. */
    KafkaConsumer<String, String> consumerForTest() {
        return consumer;
    }
}
