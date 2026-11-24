package com.wxeo.load;

import com.wxeo.config.Args;
import com.wxeo.ledger.GroundTruth;
import com.wxeo.ledger.Ledger;
import com.wxeo.util.IoUtil;
import com.wxeo.util.Json;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deterministic load generator: two phases, ground truth first.
 *
 * <p>Phase 1 (BEFORE any produce — A1 §4, D10): derive all events from
 * {@code (data_seed, seq)}, write {@code gt_<run_id>.ndjson} (canonical lines ordered by
 * {@code (event_id, leg_no)}), its {@code .sha256}, and the manifest; fsync all three.</p>
 *
 * <p>Phase 2: produce the same derivation (never read back the GT file), open-loop, explicit
 * partition assignment, {@code acks=all}, {@code enable.idempotence=true}; record per-event
 * {@code ack_ts_ms} and {@code scheduled_at_ms}; write {@code produced_<run_id>.ndjson} in seq order
 * and fsync it.</p>
 *
 * <pre>
 * java -cp artifact/target/wxeo-service.jar com.wxeo.load.LoadGenerator \
 *   --run-id=... --bootstrap=... --topic=... --events=N --partitions=P \
 *   --rate=&lt;events/s|max&gt; --data-seed-hex=&lt;16hex&gt; \
 *   --gt-out=&lt;path/gt_&lt;run_id&gt;.ndjson&gt; --manifest-out=&lt;path&gt; \
 *   [--produced-out=&lt;path&gt;] [--injection-seed-hex=&lt;16hex&gt;] [--ack-timeout-ms=...]
 * </pre>
 */
public final class LoadGenerator {

    public static void main(String[] argv) {
        Args a = Args.parse(argv);
        if (Args.hasHelpFlag(argv) || argv.length == 0) {
            System.out.println(usage());
            System.exit(argv.length == 0 ? 2 : 0);
            return;
        }
        int exit = 0;
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("schema", "wxeo.loader.result.v1");
        try {
            String runId = a.require("run-id");
            String bootstrap = a.require("bootstrap");
            String topic = a.require("topic");
            int events = resolveEvents(a);
            int partitions = a.getInt("partitions", 6);
            String rateRaw = a.get("rate", "max");
            double rate = parseRate(rateRaw);
            String seedRaw = a.get("data-seed-hex", null);
            String seedHex = null;
            String canonicalSeed;
            if (seedRaw != null && !seedRaw.isBlank()) {
                seedHex = normalizeSeedHex(seedRaw);
                canonicalSeed = hexToCanonicalDecimal(seedHex);
            } else {
                canonicalSeed = Ledger.canonicalSeed(a.require("master-seed"));
            }
            String masterSeedRaw = seedRaw != null && !seedRaw.isBlank()
                    ? seedRaw : a.get("master-seed", canonicalSeed);
            String injectionSeedHex = a.get("injection-seed-hex", null);
            Path gtOut = Path.of(a.require("gt-out"));
            Path manifestOut = Path.of(a.require("manifest-out"));
            Path producedOut = Path.of(a.get("produced-out",
                    gtOut.toAbsolutePath().getParent().resolve("produced_" + runId + ".ndjson").toString()));
            long ackTimeoutMs = a.getLong("ack-timeout-ms", 120_000);

            result.put("run_id", runId);
            result.put("events", events);
            result.put("partitions", partitions);
            result.put("topic", topic);
            result.put("rate", rateRaw);

            // ---------------- Phase 1: ground truth BEFORE any produce ----------------
            GroundTruth.GtResult gt = GroundTruth.write(gtOut, canonicalSeed, events);
            Map<String, Object> manifest = new LinkedHashMap<>();
            manifest.put("spec", "wxeo.gt-manifest.v1");
            manifest.put("run_id", runId);
            manifest.put("master_seed", canonicalSeed);
            manifest.put("master_seed_raw", masterSeedRaw);
            manifest.put("data_seed_hex", seedHex);
            manifest.put("injection_seed_hex", injectionSeedHex);
            manifest.put("events", events);
            manifest.put("partitions", partitions);
            manifest.put("gt_path", gtOut.toAbsolutePath().toString());
            manifest.put("gt_sha256", gt.gtSha256());
            manifest.put("gt_lines", gt.lines());
            manifest.put("produced_path", producedOut.toAbsolutePath().toString());
            manifest.put("topic", topic);
            manifest.put("rate", rateRaw);
            manifest.put("created_utc", IoUtil.utcNowIso());
            manifest.put("loader_java_version", System.getProperty("java.version"));
            IoUtil.writeStringFsync(manifestOut, Json.write(manifest));
            System.out.println("WXEO_GT_WRITTEN=" + gtOut + " sha256=" + gt.gtSha256() + " lines=" + gt.lines());

            // ---------------- Phase 2: produce from the same deterministic derivation ----------------
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
            props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(Math.max(120_000, ackTimeoutMs)));
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "wxeo-loader-" + runId);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

            long[] ackTs = new long[events];
            int[] ackPartition = new int[events];
            long[] ackOffset = new long[events];
            long[] schedMs = new long[events];
            java.util.Arrays.fill(ackTs, -1L);
            AtomicLong acked = new AtomicLong();
            AtomicLong failed = new AtomicLong();
            AtomicLong ackErrorFirst = new AtomicLong(Long.MIN_VALUE);

            long loadStartMs;
            long loadEndMs;
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                final long t0 = System.currentTimeMillis() + 200;
                for (int seq = 0; seq < events; seq++) {
                    long scheduledAt;
                    if (Double.isNaN(rate)) {
                        scheduledAt = System.currentTimeMillis();
                    } else {
                        scheduledAt = t0 + Math.round(seq * 1000.0 / rate);
                        paceUntil(scheduledAt);
                    }
                    schedMs[seq] = scheduledAt;
                    Ledger.EventData event = Ledger.event(canonicalSeed, seq, scheduledAt);
                    int partition = Ledger.partition(canonicalSeed, seq, partitions);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition,
                            event.eventId(), event.eventValueJson());
                    final int idx = seq;
                    try {
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                failed.incrementAndGet();
                                ackErrorFirst.compareAndSet(Long.MIN_VALUE, idx);
                                return;
                            }
                            ackTs[idx] = System.currentTimeMillis();
                            ackPartition[idx] = metadata.partition();
                            ackOffset[idx] = metadata.offset();
                            acked.incrementAndGet();
                        });
                    } catch (RuntimeException e) {
                        failed.incrementAndGet();
                        ackErrorFirst.compareAndSet(Long.MIN_VALUE, idx);
                    }
                }
                loadStartMs = t0;
                producer.flush();
                loadEndMs = System.currentTimeMillis();
            }

            // Written in seq order, after all acks, fsynced (explicit FileChannel.force; S5 F-M2).
            IoUtil.ensureDir(producedOut.toAbsolutePath().getParent());
            try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(producedOut,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                 var out = new java.io.BufferedOutputStream(
                         java.nio.channels.Channels.newOutputStream(channel), 1 << 20)) {
                StringBuilder sb = new StringBuilder(512);
                for (int seq = 0; seq < events; seq++) {
                    if (ackTs[seq] < 0) {
                        continue;
                    }
                    Ledger.Derivation d = Ledger.derive(canonicalSeed, seq);
                    sb.setLength(0);
                    sb.append("{\"event_id\":\"").append(d.eventId()).append("\"")
                            .append(",\"partition\":").append(ackPartition[seq])
                            .append(",\"offset\":").append(ackOffset[seq])
                            .append(",\"ack_ts_ms\":").append(ackTs[seq])
                            .append(",\"scheduled_at_ms\":").append(schedMs[seq])
                            .append('}').append('\n');
                    out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
                }
                out.flush();
                channel.force(true);
            }

            result.put("gt_sha256", gt.gtSha256());
            result.put("gt_lines", gt.lines());
            result.put("gt_path", gtOut.toAbsolutePath().toString());
            result.put("manifest_path", manifestOut.toAbsolutePath().toString());
            result.put("produced_path", producedOut.toAbsolutePath().toString());
            result.put("acked", acked.get());
            result.put("failed", failed.get());
            result.put("load_start_ms", loadStartMs);
            result.put("load_end_ms", loadEndMs);
            result.put("duration_ms", loadEndMs - loadStartMs);
            result.put("first_failed_seq", ackErrorFirst.get() == Long.MIN_VALUE ? null : ackErrorFirst.get());
            result.put("ok", acked.get() == events);
            if (acked.get() != events) {
                exit = 2;
            }
        } catch (Exception e) {
            result.put("ok", false);
            result.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
            e.printStackTrace(System.err);
            exit = 1;
        }
        System.out.println(Json.write(result));
        System.exit(exit);
    }

    /** Enforces the open-loop schedule without busy-waiting for the entire remainder. */
    private static void paceUntil(long targetMs) {
        while (true) {
            long remaining = targetMs - System.currentTimeMillis();
            if (remaining <= 0) {
                return;
            }
            if (remaining > 2) {
                IoUtil.sleepQuietly(remaining - 1);
            } else {
                Thread.onSpinWait();
            }
        }
    }

    private static int resolveEvents(Args a) {
        if (a.has("events")) {
            return a.getInt("events", 0);
        }
        String load = a.require("load");
        return switch (load.toLowerCase()) {
            case "1k" -> 1_000;
            case "10k" -> 10_000;
            case "100k" -> 100_000;
            case "1m" -> 1_000_000;
            default -> Integer.parseInt(load);
        };
    }

    /** {@code max} → NaN (unpaced); otherwise a positive events/s rate. */
    private static double parseRate(String raw) {
        if (raw == null || raw.isBlank() || raw.equalsIgnoreCase("max")
                || raw.equalsIgnoreCase("saturation") || raw.equalsIgnoreCase("unlimited")) {
            return Double.NaN;
        }
        double r = Double.parseDouble(raw);
        if (r <= 0) {
            throw new IllegalArgumentException("--rate must be positive events/s or 'max'");
        }
        return r;
    }

    private static String normalizeSeedHex(String raw) {
        String s = raw.trim().toLowerCase();
        if (s.startsWith("0x")) {
            s = s.substring(2);
        }
        if (!s.matches("[0-9a-f]{1,16}")) {
            throw new IllegalArgumentException("--data-seed-hex must be 1..16 hex chars: " + raw);
        }
        return s;
    }

    /** Interprets the 16-hex-char per-run data seed as a big-endian unsigned u64, rendered decimal. */
    static String hexToCanonicalDecimal(String hex) {
        return new java.math.BigInteger(hex, 16).toString(10);
    }

    public static String usage() {
        return String.join("\n",
                "com.wxeo.load.LoadGenerator — deterministic two-phase load generator",
                "",
                "  --run-id=<run_id>            (required)",
                "  --bootstrap=host:port        (required)",
                "  --topic=<source topic>       (required)",
                "  --events=<N>                 (required; or --load=1k|10k|100k|1m)",
                "  --partitions=<P>             (required)",
                "  --rate=<events/s|max>        (default max)",
                "  --data-seed-hex=<16hex>      per-run data seed (prereg §4.4); preferred over --master-seed",
                "  --master-seed=<u64 decimal>  alternative form (canonical decimal hash input)",
                "  --injection-seed-hex=<hex>   recorded in the manifest only",
                "  --gt-out=<path>              gt_<run_id>.ndjson path (required)",
                "  --manifest-out=<path>        manifest JSON path (required)",
                "  --produced-out=<path>        default: produced_<run_id>.ndjson next to the GT file",
                "  --ack-timeout-ms=<ms>        delivery timeout (default 120000)");
    }

    private LoadGenerator() {
    }
}
