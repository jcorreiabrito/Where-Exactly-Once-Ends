package com.wxeo.metrics;

import com.wxeo.config.EffectiveConfig;
import com.wxeo.ledger.Ledger;
import com.wxeo.util.IoUtil;
import com.wxeo.util.Json;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-instance run metrics (docs/method.md §7.2, §7.7; docs/CONTRACTS.md §6).
 *
 * <p>Latency: end-to-end per event {@code l(e) = t_effect_commit_observed - scheduled_at_ms}, in
 * microseconds, recorded into an HdrHistogram with 3 significant digits over 1 µs .. 60 s. Per-batch
 * latency is the secondary diagnostic (batch completion relative to its newest event's schedule
 * time). On graceful shutdown the histograms are dumped as HdrHistogram {@code .hlog} files plus a
 * percentile JSON.</p>
 */
public final class RunMetrics {

    private final EffectiveConfig cfg;
    private final Histogram eventLatencyUs = new Histogram(1, 60_000_000L, 3);
    private final Histogram batchLatencyUs = new Histogram(1, 60_000_000L, 3);
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final long startMs = System.currentTimeMillis();
    private final long startNano = System.nanoTime();
    private volatile long stopMs = 0L;

    public RunMetrics(EffectiveConfig cfg) {
        this.cfg = cfg;
        for (String c : COUNTER_NAMES) {
            counters.put(c, new AtomicLong());
        }
    }

    public static final List<String> COUNTER_NAMES = List.of(
            "batches_polled", "records_consumed", "events_processed", "poison_records",
            "postings_inserted", "duplicate_detected", "outbox_rows_written", "outbox_published",
            "effect_commits", "progress_commits", "progress_commit_failures", "db_retries",
            "db_retry_exhausted", "unknown_outcome_events", "txn_aborts", "poll_errors",
            "process_errors", "injected_faults", "rebalances", "relay_errors", "relay_cycles",
            "progress_row_updates", "progress_seeks", "kafka_progress_commit_retries",
            "committed_batches", "committed_events", "publication_sent", "publication_errors",
            "commits_observed_lagging_zero");

    public void inc(String name) {
        counters.computeIfAbsent(name, k -> new AtomicLong()).incrementAndGet();
    }

    public void inc(String name, long delta) {
        counters.computeIfAbsent(name, k -> new AtomicLong()).addAndGet(delta);
    }

    public long get(String name) {
        AtomicLong v = counters.get(name);
        return v == null ? 0L : v.get();
    }

    public Map<String, Long> counterSnapshot() {
        Map<String, Long> out = new TreeMap<>();
        counters.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    /** Records a durable-effect commit observation for a whole batch of events. */
    public void recordEffectCommitMs(long commitMs, List<Ledger.ParsedEvent> events) {
        inc("effect_commits");
        inc("committed_batches");
        inc("committed_events", events.size());
        if (events.isEmpty()) {
            return;
        }
        long newestScheduled = Long.MIN_VALUE;
        for (Ledger.ParsedEvent e : events) {
            long scheduled = e.event().scheduledAtMs();
            newestScheduled = Math.max(newestScheduled, scheduled);
            long latUs = Math.max(1L, (commitMs - scheduled) * 1000L);
            eventLatencyUs.recordValue(Math.min(latUs, 60_000_000L));
        }
        long batchLatUs = Math.max(1L, (commitMs - newestScheduled) * 1000L);
        batchLatencyUs.recordValue(Math.min(batchLatUs, 60_000_000L));
    }

    public long startMs() {
        return startMs;
    }

    public void markStopped() {
        stopMs = System.currentTimeMillis();
    }

    public long stopMs() {
        return stopMs == 0 ? System.currentTimeMillis() : stopMs;
    }

    public double elapsedSeconds() {
        return (System.nanoTime() - startNano) / 1_000_000_000.0;
    }

    public String eventHistogramPath() {
        return cfg.outputDir().resolve("hist_" + cfg.runId() + "_" + cfg.instanceId() + "_event.hlog").toString();
    }

    public String batchHistogramPath() {
        return cfg.outputDir().resolve("hist_" + cfg.runId() + "_" + cfg.instanceId() + ".hlog").toString();
    }

    public String percentilesPath() {
        return cfg.outputDir().resolve("hist_" + cfg.runId() + "_" + cfg.instanceId() + ".json").toString();
    }

    /** Writes the .hlog dumps and the percentile JSON; called on graceful shutdown only. */
    public Map<String, Object> writeHistograms() {
        IoUtil.ensureDir(cfg.outputDir());
        Histogram ev = eventLatencyUs.copy();
        Histogram bt = batchLatencyUs.copy();
        writeHlog(batchHistogramPath(), bt, "batch");
        writeHlog(eventHistogramPath(), ev, "event");

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("schema", "wxeo.hist.v1");
        out.put("run_id", cfg.runId());
        out.put("instance_id", cfg.instanceId());
        out.put("mechanism", cfg.mechanism().wireName);
        out.put("unit", "us");
        out.put("event", percentiles(ev));
        out.put("batch", percentiles(bt));
        out.put("event_histogram_path", eventHistogramPath());
        out.put("batch_histogram_path", batchHistogramPath());
        out.put("written_utc", IoUtil.utcNowIso());
        IoUtil.writeAtomicFsync(Path.of(percentilesPath()), Json.write(out));

        Map<String, Object> paths = new LinkedHashMap<>();
        paths.put("event_histogram_path", eventHistogramPath());
        paths.put("batch_histogram_path", batchHistogramPath());
        paths.put("percentiles_path", percentilesPath());
        return paths;
    }

    private static Map<String, Object> percentiles(Histogram h) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (h.getTotalCount() == 0) {
            m.put("count", 0);
            m.put("p50_ms", null);
            m.put("p90_ms", null);
            m.put("p99_ms", null);
            m.put("p999_ms", null);
            m.put("max_ms", null);
            return m;
        }
        m.put("count", h.getTotalCount());
        m.put("p50_ms", usToMs(h.getValueAtPercentile(50.0)));
        m.put("p90_ms", usToMs(h.getValueAtPercentile(90.0)));
        m.put("p99_ms", usToMs(h.getValueAtPercentile(99.0)));
        m.put("p999_ms", usToMs(h.getValueAtPercentile(99.9)));
        m.put("max_ms", usToMs(h.getMaxValue()));
        return m;
    }

    private static double usToMs(double us) {
        return Math.round(us / 100.0) / 10.0;
    }

    private static void writeHlog(String path, Histogram h, String label) {
        try (OutputStream os = Files.newOutputStream(Path.of(path),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            HistogramLogWriter writer = new HistogramLogWriter(os);
            writer.outputLogFormatVersion();
            writer.outputComment("wxeo " + label + " e2e latency histogram, unit=us, 3 significant digits");
            writer.outputStartTime(java.time.Instant.now().toEpochMilli());
            writer.outputHistogram(h);
            os.flush();
        } catch (Exception e) {
            throw new IllegalStateException("cannot write histogram " + path, e);
        }
    }
}
