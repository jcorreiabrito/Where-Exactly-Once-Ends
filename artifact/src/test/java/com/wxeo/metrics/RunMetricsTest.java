package com.wxeo.metrics;

import com.wxeo.config.Args;
import com.wxeo.config.EffectiveConfig;
import com.wxeo.ledger.Ledger;
import com.wxeo.util.Json;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** HdrHistogram + counters contract tests (docs/method.md §7.2, §7.7; CONTRACTS §6). */
class RunMetricsTest {

    @TempDir
    Path dir;

    private EffectiveConfig config() {
        return EffectiveConfig.fromArgs(Args.parse(new String[]{
                "--wxeo.mechanism=M0", "--wxeo.run-id=hist-run", "--wxeo.instance-id=1",
                "--wxeo.bootstrap=127.0.0.1:19092",
                "--wxeo.db-url=jdbc:postgresql://127.0.0.1:15433/wxeo", "--wxeo.db-user=u",
                "--wxeo.output-dir=" + dir, "--wxeo.faults-log=" + dir.resolve("faults.log")}));
    }

    @Test
    void recordsPerEventLatencyAndWritesHistogramArtifacts() throws Exception {
        RunMetrics m = new RunMetrics(config());
        String seed = Ledger.canonicalSeedFromHex("0123456789abcdef");
        long scheduled = 1_764_000_000_000L;
        List<Ledger.ParsedEvent> events = List.of(
                new Ledger.ParsedEvent(Ledger.event(seed, 0, scheduled), 0, 0, "v"),
                new Ledger.ParsedEvent(Ledger.event(seed, 1, scheduled), 1, 0, "v"));
        m.inc("batches_polled");
        m.recordEffectCommitMs(scheduled + 5, events);
        m.recordEffectCommitMs(scheduled + 15, events);

        assertEquals(2, m.get("effect_commits"));
        assertEquals(2, m.get("committed_batches"));
        assertEquals(4, m.get("committed_events"));
        assertEquals(1, m.get("batches_polled"));

        Map<String, Object> paths = m.writeHistograms();
        Path hlog = Path.of(String.valueOf(paths.get("event_histogram_path")));
        Path batchHlog = Path.of(String.valueOf(paths.get("batch_histogram_path")));
        Path percentiles = Path.of(String.valueOf(paths.get("percentiles_path")));
        assertTrue(Files.exists(hlog), "event .hlog written");
        assertTrue(Files.exists(batchHlog), "batch .hlog written");
        assertTrue(Files.size(hlog) > 0);
        assertTrue(Files.readString(hlog).contains("Histogram log format version"),
                "HdrHistogram log format header");
        var p = Json.parse(Files.readString(percentiles));
        assertEquals(4, p.get("event").get("count").asLong());
        assertEquals(2, p.get("batch").get("count").asLong());
        double p50 = p.get("event").get("p50_ms").asDouble();
        assertTrue(p50 >= 5.0 && p50 <= 15.0, "p50 within recorded latency range, got " + p50);
        assertTrue(p.has("event_histogram_path"));
    }

    @Test
    void warmupEventsAreExcludedFromLatencyAndThroughputButCounted() throws Exception {
        EffectiveConfig cfg = EffectiveConfig.fromArgs(Args.parse(new String[]{
                "--wxeo.mechanism=M0", "--wxeo.run-id=hist-warmup", "--wxeo.instance-id=1",
                "--wxeo.bootstrap=127.0.0.1:19092",
                "--wxeo.db-url=jdbc:postgresql://127.0.0.1:15433/wxeo", "--wxeo.db-user=u",
                "--wxeo.warmup-events=2",
                "--wxeo.output-dir=" + dir, "--wxeo.faults-log=" + dir.resolve("faults.log")}));
        RunMetrics m = new RunMetrics(cfg);
        String seed = Ledger.canonicalSeedFromHex("0123456789abcdef");
        long scheduled = 1_764_000_000_000L;
        for (int seq = 0; seq < 4; seq++) {
            long commitMs = scheduled + (seq == 0 ? 5 : seq == 1 ? 10 : seq == 2 ? 20 : 30);
            m.recordEffectCommitMs(commitMs, java.util.List.of(
                    new Ledger.ParsedEvent(Ledger.event(seed, seq, scheduled), seq, 0, "v")));
        }
        var perf = m.performanceBlock();
        assertEquals(4L, perf.get("committed_events_total"));
        assertEquals(2L, ((Number) perf.get("warmup_events_excluded")).longValue());
        assertEquals(2L, perf.get("steady_state_events"));
        assertEquals(2L, m.get("warmup_events_excluded"));
        assertEquals(2L, m.get("latency_events_recorded"));
        assertTrue(((Number) perf.get("throughput_eps")).doubleValue() > 0.0);
        assertEquals(2L, ((Number) perf.get("throughput_numerator_events")).longValue());
        var paths = m.writeHistograms();
        var p = Json.parse(Files.readString(Path.of(String.valueOf(paths.get("percentiles_path")))));
        assertEquals(2, p.get("event").get("count").asLong(), "warm-up excluded from histogram");
        assertEquals(2, p.get("performance").get("warmup_events_excluded").asInt());
    }
    @Test
    void emptyHistogramsSerializeAsNulls() throws Exception {
        RunMetrics m = new RunMetrics(config());
        Map<String, Object> paths = m.writeHistograms();
        var p = Json.parse(Files.readString(Path.of(String.valueOf(paths.get("percentiles_path")))));
        assertEquals(0, p.get("event").get("count").asLong());
        assertTrue(p.get("event").get("p99_ms").isNull());
    }
}
