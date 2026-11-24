package com.wxeo.config;

import com.wxeo.config.EffectiveConfig.MechanismKind;
import com.wxeo.util.Json;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Configuration contract tests (docs/method.md §4.11; G6 config-equality audit). */
class EffectiveConfigTest {

    @TempDir
    Path dir;

    private Args baseArgs(String mechanism, String ordering) {
        return Args.parse(new String[]{
                "--wxeo.mechanism=" + mechanism,
                "--wxeo.ordering=" + ordering,
                "--wxeo.run-id=a-m1x-k3-1k-p3-r07",
                "--wxeo.instance-id=2",
                "--wxeo.bootstrap=127.0.0.1:19092",
                "--wxeo.db-url=jdbc:postgresql://127.0.0.1:15433/wxeo",
                "--wxeo.db-user=wxeo_app",
                "--wxeo.source-topic=wxeo.src.a-m1x-k3-1k-p3-r07",
                "--wxeo.sink-topic=wxeo.sink.a-m1x-k3-1k-p3-r07",
                "--wxeo.group=wxeo.cg.a-m1x-k3-1k-p3-r07",
                "--wxeo.output-dir=" + dir,
                "--wxeo.faults-log=" + dir.resolve("faults.log")
        });
    }

    @Test
    void sharedConfigHashIsIdenticalAcrossMechanisms() {
        var m0 = EffectiveConfig.fromArgs(baseArgs("M0", "strict"));
        var m3 = EffectiveConfig.fromArgs(baseArgs("M3", "strict"));
        var m4 = EffectiveConfig.fromArgs(baseArgs("M4", "strict"));
        assertEquals(m0.sharedSha256(), m3.sharedSha256(), "G6: shared tuning block must be identical");
        assertEquals(m0.sharedSha256(), m4.sharedSha256());
        assertNotEquals(m0.fullSha256(), m3.fullSha256(), "intrinsic mechanism blocks differ");
        assertNotEquals(m0.intrinsic(), m3.intrinsic());
    }

    @Test
    void mechanismIntrinsicDeclarationsMatchTheSpec() {
        var m2 = EffectiveConfig.fromArgs(baseArgs("M2", "strict"));
        assertEquals(true, m2.intrinsic().get("inbox_table"));
        assertEquals(true, m2.intrinsic().get("outbox_table"));
        var m4 = EffectiveConfig.fromArgs(baseArgs("M4", "strict"));
        assertEquals(true, m4.intrinsic().get("consumer_progress_table"));
        assertEquals(true, m4.intrinsic().get("db_sink"));
        var mk = EffectiveConfig.fromArgs(baseArgs("M1-kafka", "strict"));
        assertEquals(false, mk.mechanism().dbSink());
        assertEquals("kafka-transaction", mk.intrinsic().get("publication_path"));
        var m3 = EffectiveConfig.fromArgs(baseArgs("M3", "strict"));
        assertEquals(true, m3.mechanism().dedupIndex);
        var m2m3 = EffectiveConfig.fromArgs(baseArgs("M2+M3", "strict"));
        assertEquals(false, m2m3.mechanism().inbox);
        assertEquals(true, m2m3.mechanism().dedupIndex);
        var m1xm3 = EffectiveConfig.fromArgs(baseArgs("M1x+M3", "strict"));
        assertEquals(true, m1xm3.mechanism().transactionalProducer);
        assertEquals(true, m1xm3.mechanism().dedupIndex);
    }

    @Test
    void orderingFlagIsRecordedAndEarlyOnlyWhereMeaningful() {
        var strict = EffectiveConfig.fromArgs(baseArgs("M0", "strict"));
        assertFalse(strict.orderEarly());
        var early = EffectiveConfig.fromArgs(baseArgs("M0", "early"));
        assertTrue(early.orderEarly());
        assertEquals("early", early.identity().get("ordering"));
        var kafkaEarly = EffectiveConfig.fromArgs(baseArgs("M1-kafka", "early"));
        assertFalse(kafkaEarly.orderEarly(), "single-commit M1-kafka records ordering as n/a (strict)");
    }

    @Test
    void invalidMechanismAndOrderingAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> MechanismKind.parse("M9"));
        assertThrows(IllegalArgumentException.class,
                () -> EffectiveConfig.fromArgs(baseArgs("M1x", "sideways")));
        assertThrows(IllegalArgumentException.class, () -> {
            Args missing = Args.parse(new String[]{
                    "--wxeo.mechanism=M3", "--wxeo.run-id=r", "--wxeo.bootstrap=b",
                    "--wxeo.output-dir=" + dir});
            EffectiveConfig.fromArgs(missing);
        }, "missing --wxeo.db-url for a DB mechanism must fail");
    }

    @Test
    void configFileJsonIsDeterministicAndSelfHashed() {
        var cfg = EffectiveConfig.fromArgs(baseArgs("M3", "strict"));
        String first = cfg.fileJson();
        var again = EffectiveConfig.fromArgs(baseArgs("M3", "strict"));
        assertEquals(first, again.fileJson(), "same inputs must produce byte-identical config JSON");
        var node = Json.parse(first);
        assertEquals(EffectiveConfig.SCHEMA, node.get("schema").asText());
        assertEquals(cfg.fullSha256(), node.get("config_sha256").asText());
        assertEquals(cfg.sharedSha256(), node.get("shared_sha256").asText());
        assertTrue(node.has("identity") && node.has("shared") && node.has("intrinsic") && node.has("app"));
        assertEquals("wxeo_run_a-m1x-k3-1k-p3-r07", node.get("identity").get("db_schema").asText());
    }

    @Test
    void failpointConfigurationIsParsed() {
        Args args = Args.parse(new String[]{
                "--wxeo.mechanism=M0", "--wxeo.run-id=r", "--wxeo.bootstrap=b",
                "--wxeo.db-url=jdbc:postgresql://127.0.0.1:5433/wxeo", "--wxeo.db-user=u",
                "--wxeo.output-dir=" + dir,
                "--wxeo.failpoint.enabled=true",
                "--wxeo.failpoint.name=before_effect_commit",
                "--wxeo.failpoint.action=pause:250",
                "--wxeo.failpoint.after=3"});
        var cfg = EffectiveConfig.fromArgs(args);
        assertTrue(cfg.failpoint().enabled());
        assertEquals("before_effect_commit", cfg.failpoint().name());
        assertEquals("pause", cfg.failpoint().action());
        assertEquals(250, cfg.failpoint().pauseMs());
        assertEquals(3, cfg.failpoint().after());
    }
}
