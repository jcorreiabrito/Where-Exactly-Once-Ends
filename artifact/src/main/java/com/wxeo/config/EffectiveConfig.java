package com.wxeo.config;

import com.wxeo.util.IoUtil;
import com.wxeo.util.Json;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Effective configuration of one service instance, split for the G6 configuration-equality audit
 * (docs/method.md §4.11, §7.6):
 *
 * <ul>
 *   <li>{@code identity} — per-run/per-instance keys (run id, topics, group, schema, ports, paths);</li>
 *   <li>{@code shared} — tuning keys that must be identical across every mechanism (G6 hashes this);</li>
 *   <li>{@code intrinsic} — the declared mechanism-intrinsic components only (§4.11 audit list);</li>
 *   <li>{@code app} — JVM/build/OS facts of this process.</li>
 * </ul>
 *
 * <p>The file {@code config_<run_id>_<instance>.json} is written and fsynced before any record is
 * consumed. Its {@code shared_sha256} must be equal across mechanisms for the same cell.</p>
 */
public final class EffectiveConfig {

    public static final String SCHEMA = "wxeo.config.v1";

    /** Fixed mechanism set and derived capabilities (docs/method.md §4). */
    public enum MechanismKind {
        M0("M0", false, false, false, false, false, false),
        M1_KAFKA("M1-kafka", true, false, false, false, false, false),
        M1X("M1x", false, true, false, false, false, false),
        M2("M2", false, false, true, true, false, true),
        M3("M3", false, false, false, false, true, false),
        M4("M4", false, false, false, false, false, false),
        M2_M3("M2+M3", false, false, false, true, true, true),
        M4_M3("M4+M3", false, false, false, false, true, false),
        M1X_M3("M1x+M3", false, true, false, false, true, false);

        public final String wireName;
        public final boolean kafkaSinkOnly;
        public final boolean transactionalProducer;
        public final boolean inbox;
        public final boolean outboxRelay;
        public final boolean dedupIndex;
        public final boolean relayThread;

        MechanismKind(String wireName, boolean kafkaSinkOnly, boolean transactionalProducer,
                      boolean inbox, boolean outboxRelay, boolean dedupIndex, boolean relayThread) {
            this.wireName = wireName;
            this.kafkaSinkOnly = kafkaSinkOnly;
            this.transactionalProducer = transactionalProducer;
            this.inbox = inbox;
            this.outboxRelay = outboxRelay;
            this.dedupIndex = dedupIndex;
            this.relayThread = relayThread;
        }

        public boolean dbSink() {
            return !kafkaSinkOnly;
        }

        public boolean hasProgressTable() {
            return this == M4 || this == M4_M3;
        }

        public boolean hasTwoCommits() {
            return this != M1_KAFKA;
        }

        public static MechanismKind parse(String raw) {
            if (raw == null) {
                throw new IllegalArgumentException("missing --wxeo.mechanism");
            }
            String v = raw.trim();
            for (MechanismKind k : values()) {
                if (k.wireName.equals(v)) {
                    return k;
                }
            }
            throw new IllegalArgumentException("unknown mechanism '" + raw + "'; expected one of "
                    + List.of("M0", "M1-kafka", "M1x", "M2", "M3", "M4", "M2+M3", "M4+M3", "M1x+M3"));
        }
    }

    /** Failpoint configuration (docs/CONTRACTS.md §7). */
    public record FailpointConfig(boolean enabled, String name, String action, int after, long pauseMs,
                                  String faultsLog) {
    }

    private final Map<String, Object> identity;
    private final Map<String, Object> shared;
    private final Map<String, Object> intrinsic;
    private final Map<String, Object> app;
    private final Map<String, Object> failpoint;
    private final Map<String, Object> root;
    private final String json;
    private final String sharedSha256;
    private final String fullSha256;

    private final String runId;
    private final int instanceId;
    private final MechanismKind mechanism;
    private final boolean orderEarly;
    private final Path outputDir;
    private final Path stopFile;
    private final FailpointConfig fp;

    private EffectiveConfig(Map<String, Object> identity, Map<String, Object> shared,
                            Map<String, Object> intrinsic, Map<String, Object> app,
                            Map<String, Object> failpoint, String runId, int instanceId,
                            MechanismKind mechanism, boolean orderEarly, Path outputDir, Path stopFile) {
        this.identity = identity;
        this.shared = shared;
        this.intrinsic = intrinsic;
        this.app = app;
        this.failpoint = failpoint;
        this.runId = runId;
        this.instanceId = instanceId;
        this.mechanism = mechanism;
        this.orderEarly = orderEarly;
        this.outputDir = outputDir;
        this.stopFile = stopFile;

        this.sharedSha256 = IoUtil.sha256Hex(Json.write(shared).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Map<String, Object> r = new TreeMap<>();
        r.put("schema", SCHEMA);
        r.put("identity", identity);
        r.put("shared", shared);
        r.put("intrinsic", intrinsic);
        r.put("failpoint", failpoint);
        r.put("app", app);
        r.put("shared_sha256", sharedSha256);
        r.put("intrinsic_sha256", IoUtil.sha256Hex(
                Json.write(intrinsic).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        this.json = Json.write(r);
        this.fullSha256 = IoUtil.sha256Hex(this.json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        r.put("config_sha256", fullSha256);
        this.root = r;
        this.fp = new FailpointConfig(
                Boolean.TRUE.equals(failpoint.get("enabled")),
                String.valueOf(failpoint.get("name")),
                String.valueOf(failpoint.get("action")),
                ((Number) failpoint.get("after")).intValue(),
                ((Number) failpoint.get("pause_ms")).longValue(),
                String.valueOf(failpoint.get("faults_log")));
    }

    public static EffectiveConfig fromArgs(Args a) {
        String runId = a.require("wxeo.run-id");
        int instanceId = a.getInt("wxeo.instance-id", 1);
        MechanismKind mechanism = MechanismKind.parse(a.require("wxeo.mechanism"));
        String ordering = a.get("wxeo.ordering", "strict");
        if (!ordering.equals("strict") && !ordering.equals("early")) {
            throw new IllegalArgumentException("--wxeo.ordering must be strict|early, got " + ordering);
        }
        boolean orderEarly = ordering.equals("early");
        if (orderEarly && mechanism == MechanismKind.M1_KAFKA) {
            assertWarn("ordering=early requested for M1-kafka (single atomic commit); ordering is n/a and will be recorded as strict");
            orderEarly = false;
        }

        String bootstrap = a.require("wxeo.bootstrap");
        String dbUrl = a.get("wxeo.db-url", "");
        if (mechanism.dbSink() && dbUrl.isBlank()) {
            throw new IllegalArgumentException("--wxeo.db-url is required for mechanism " + mechanism.wireName);
        }
        String dbUser = a.get("wxeo.db-user", "");
        String dbSchema = a.get("wxeo.db-schema", "wxeo_run_" + runId);
        String sourceTopic = a.get("wxeo.source-topic", "wxeo.src." + runId);
        String sinkTopic = a.get("wxeo.sink-topic", "wxeo.sink." + runId);
        String group = a.get("wxeo.group", "wxeo.cg." + runId);
        Path outputDir = Path.of(a.require("wxeo.output-dir"));
        String faultsLog = a.get("wxeo.faults-log", outputDir.resolve("faults.log").toString());
        IoUtil.ensureDir(outputDir);

        Map<String, Object> identity = new TreeMap<>();
        identity.put("run_id", runId);
        identity.put("instance_id", instanceId);
        identity.put("mechanism", mechanism.wireName);
        identity.put("ordering", ordering);
        identity.put("bootstrap", bootstrap);
        identity.put("db_url", dbUrl);
        identity.put("db_user", dbUser);
        identity.put("db_schema", dbSchema);
        identity.put("source_topic", sourceTopic);
        identity.put("sink_topic", sinkTopic);
        identity.put("group", group);
        identity.put("output_dir", outputDir.toAbsolutePath().toString());
        identity.put("faults_log", faultsLog);
        identity.put("schema_init", a.get("wxeo.schema-init", "none"));

        Map<String, Object> consumer = new TreeMap<>();
        consumer.put("max.poll.records", 500);
        consumer.put("fetch.min.bytes", 1);
        consumer.put("fetch.max.wait.ms", 500);
        consumer.put("session.timeout.ms", 10_000);
        consumer.put("heartbeat.interval.ms", 3_000);
        consumer.put("max.poll.interval.ms", 300_000);
        consumer.put("isolation.level", "read_committed");
        consumer.put("enable.auto.commit", false);
        consumer.put("auto.offset.reset", "earliest");
        consumer.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        consumer.put("request.timeout.ms", 30_000);
        consumer.put("default.api.timeout.ms", 60_000);

        Map<String, Object> producer = new TreeMap<>();
        producer.put("acks", "all");
        producer.put("enable.idempotence", true);
        producer.put("max.in.flight.requests.per.connection", 5);
        producer.put("linger.ms", 0);
        producer.put("batch.size", 16_384);
        producer.put("compression.type", "none");
        producer.put("delivery.timeout.ms", 120_000);
        producer.put("request.timeout.ms", 30_000);

        Map<String, Object> jdbc = new TreeMap<>();
        jdbc.put("autocommit", false);
        jdbc.put("pool_size", 8);
        jdbc.put("batch_size", 500);
        jdbc.put("prepare_threshold", 5);
        jdbc.put("rewrite_batched_inserts", true);
        jdbc.put("connection_timeout_ms", 30_000);
        jdbc.put("synchronous_commit_asserted", true);

        Map<String, Object> histogram = new TreeMap<>();
        histogram.put("significant_digits", 3);
        histogram.put("lowest_us", 1);
        histogram.put("highest_us", 60_000_000);
        histogram.put("latency_unit", "us");

        Map<String, Object> batch = new TreeMap<>();
        batch.put("poll_timeout_ms", 200);
        batch.put("jdbc_batch_size", 500);

        Map<String, Object> retry = new TreeMap<>();
        retry.put("serialization_max_attempts", 5);
        retry.put("connection_max_attempts", 3);
        retry.put("backoff_ms", List.of(100, 300, 900));
        retry.put("kafka_progress_commit_max_attempts", 3);

        Map<String, Object> publication = new TreeMap<>();
        publication.put("partition_mode", "same-as-source");
        publication.put("ack_timeout_ms", 60_000);

        Map<String, Object> shared = new TreeMap<>();
        shared.put("consumer", consumer);
        shared.put("producer", producer);
        shared.put("jdbc", jdbc);
        shared.put("histogram", histogram);
        shared.put("batch", batch);
        shared.put("retry", retry);
        shared.put("publication", publication);

        Map<String, Object> intrinsic = new TreeMap<>();
        intrinsic.put("mechanism", mechanism.wireName);
        intrinsic.put("db_sink", mechanism.dbSink());
        intrinsic.put("transactional_producer", mechanism.transactionalProducer);
        intrinsic.put("dedup_index", mechanism.dedupIndex);
        intrinsic.put("inbox_table", mechanism.inbox);
        intrinsic.put("outbox_table", mechanism.outboxRelay);
        intrinsic.put("relay_thread", mechanism.relayThread);
        intrinsic.put("consumer_progress_table", mechanism.hasProgressTable());
        intrinsic.put("publication_path", mechanism.kafkaSinkOnly ? "kafka-transaction"
                : mechanism.transactionalProducer ? "kafka-transaction"
                : mechanism.relayThread ? "outbox-relay" : "inline");
        intrinsic.put("retry_unknown_outcome_mode", unknownOutcomeMode(mechanism));
        intrinsic.put("transactional_id_prefix", mechanism.transactionalProducer
                ? "wxeo-" + runId + "-" : null);
        intrinsic.put("transaction_timeout_ms", mechanism.transactionalProducer ? 60_000 : null);
        intrinsic.put("relay_batch_limit", mechanism.relayThread ? 100 : null);
        intrinsic.put("relay_poll_interval_ms", mechanism.relayThread ? 20 : null);

        Map<String, Object> app = new TreeMap<>();
        app.put("artifact", "wxeo-service");
        app.put("artifact_version", version());
        app.put("java_version", System.getProperty("java.version"));
        app.put("java_vendor", System.getProperty("java.vendor"));
        app.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        app.put("jvm_args", java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments());
        app.put("jvm_start_ms", java.lang.management.ManagementFactory.getRuntimeMXBean().getStartTime());

        Map<String, Object> failpoint = new TreeMap<>();
        boolean fpEnabled = a.getBool("wxeo.failpoint.enabled", false);
        String fpName = a.get("wxeo.failpoint.name", "");
        String actionRaw = a.get("wxeo.failpoint.action", "crash");
        long pauseMs = a.getLong("wxeo.failpoint.pause-ms", 0);
        if (actionRaw.startsWith("pause:")) {
            pauseMs = Long.parseLong(actionRaw.substring("pause:".length()));
            actionRaw = "pause";
        }
        if (fpEnabled && fpName.isBlank()) {
            fpEnabled = false;
        }
        failpoint.put("enabled", fpEnabled);
        failpoint.put("name", fpName);
        failpoint.put("action", actionRaw);
        failpoint.put("after", a.getInt("wxeo.failpoint.after", 1));
        failpoint.put("pause_ms", pauseMs);
        failpoint.put("faults_log", faultsLog);

        Path stopFile = outputDir.resolve("stop_" + runId + "_" + instanceId + ".flag");
        return new EffectiveConfig(identity, shared, intrinsic, app, failpoint, runId, instanceId,
                mechanism, orderEarly, outputDir, stopFile);
    }

    static String unknownOutcomeMode(MechanismKind m) {
        return switch (m) {
            case M0 -> "NO_WRITE_RETRY";
            case M1X -> "NO_WRITE_RETRY";
            case M4, M4_M3 -> "REREAD_PROGRESS";
            case M2, M3, M2_M3, M1X_M3 -> "RETRY_IDEMPOTENT";
            case M1_KAFKA -> "NONE";
        };
    }

    private static boolean warned;

    private static void assertWarn(String message) {
        if (!warned) {
            warned = true;
            System.err.println("[wxeo][warn] " + message);
        }
    }

    public static String version() {
        String v = EffectiveConfig.class.getPackage().getImplementationVersion();
        return v == null ? "1.0.0-dev" : v;
    }

    public String runId() {
        return runId;
    }

    public int instanceId() {
        return instanceId;
    }

    public MechanismKind mechanism() {
        return mechanism;
    }

    public boolean orderEarly() {
        return orderEarly;
    }

    public Path outputDir() {
        return outputDir;
    }

    public Path stopFile() {
        return stopFile;
    }

    public FailpointConfig failpoint() {
        return fp;
    }

    public String bootstrap() {
        return (String) identity.get("bootstrap");
    }

    public String dbUrl() {
        return (String) identity.get("db_url");
    }

    public String dbUser() {
        return (String) identity.get("db_user");
    }

    public String dbSchema() {
        return (String) identity.get("db_schema");
    }

    public String sourceTopic() {
        return (String) identity.get("source_topic");
    }

    public String sinkTopic() {
        return (String) identity.get("sink_topic");
    }

    public String group() {
        return (String) identity.get("group");
    }

    public String faultsLog() {
        return (String) identity.get("faults_log");
    }

    public String schemaInit() {
        return (String) identity.get("schema_init");
    }

    public boolean failOnAttestationFailure() {
        return Boolean.parseBoolean((String) identity.getOrDefault("fail_on_attestation_failure", "false"));
    }

    /** Canonical full config JSON (deterministic; map keys sorted at every level). */
    public String json() {
        return json;
    }

    /** JSON including a {@code config_sha256} field (what the config file contains). */
    public String fileJson() {
        return Json.write(root);
    }

    public String fullSha256() {
        return fullSha256;
    }

    /** SHA-256 over the shared tuning block only — must be equal across mechanisms (G6). */
    public String sharedSha256() {
        return sharedSha256;
    }

    public Map<String, Object> shared() {
        return shared;
    }

    public Map<String, Object> intrinsic() {
        return intrinsic;
    }

    public Map<String, Object> identity() {
        return identity;
    }

    public Map<String, Object> app() {
        return app;
    }

    /** Consumer property map for the native KafkaConsumer. */
    public Map<String, Object> consumerProps() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, group());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60_000);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.RangeAssignor");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG,
                "wxeo-" + runId + "-" + instanceId);
        return props;
    }

    /** Producer property map shared by every mechanism (publication and transactional variants). */
    public Map<String, Object> producerProps() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG, 16_384);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG,
                "wxeo-" + runId + "-" + instanceId + "-pub");
        return props;
    }

    public List<String> intrinsicKeys() {
        List<String> out = new ArrayList<>(intrinsic.keySet());
        out.sort(String::compareTo);
        return out;
    }
}
