package com.wxeo.failpoint;

import com.wxeo.util.Json;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Failpoint engine tests: arming, after=N, receipts, throw/abort_txn/pause/none, disabled mode. */
class FailpointTest {

    @TempDir
    Path dir;

    private Path logPath() {
        return dir.resolve("faults.log");
    }

    private static List<String> lines(Path p) throws Exception {
        if (!Files.exists(p)) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String l : Files.readAllLines(p)) {
            if (!l.isBlank()) {
                out.add(l);
            }
        }
        return out;
    }

    @Test
    void disabledFailpointNeverFiresAndWritesNothing() throws Exception {
        Failpoints.initForTest(false, "before_effect_commit", "crash", 1, 0,
                logPath().toString(), "r", "M0", 1);
        Failpoints.fire("before_effect_commit");
        Failpoints.fire("after_fetch");
        assertEquals(0, Failpoints.hits());
        assertTrue(lines(logPath()).isEmpty());
    }

    @Test
    void firesOnTheNthHitOnly() throws Exception {
        Failpoints.initForTest(true, "before_effect_commit", "none", 3, 0,
                logPath().toString(), "r", "M0", 1);
        Failpoints.fire("before_effect_commit");
        Failpoints.fire("before_effect_commit");
        assertTrue(lines(logPath()).isEmpty(), "no receipt before the N-th hit");
        Failpoints.fire("before_effect_commit");
        List<String> receipts = lines(logPath());
        assertEquals(2, receipts.size(), "none action writes fired + restored");
        var first = Json.parse(receipts.get(0));
        assertEquals("fired", first.get("event").asText());
        assertEquals("before_effect_commit", first.get("fault").asText());
        assertEquals("r", first.get("run_id").asText());
        assertTrue(first.has("ts_utc") && first.has("detail"));
        assertEquals("restored", Json.parse(receipts.get(1)).get("event").asText());
        // fires at most once per process
        Failpoints.fire("before_effect_commit");
        assertEquals(2, lines(logPath()).size());
    }

    @Test
    void throwActionThrowsInjectedFaultAndWritesFiredReceipt() throws Exception {
        Failpoints.initForTest(true, "after_effect_commit", "throw", 1, 0,
                logPath().toString(), "r", "M3", 2);
        InjectedFaultException e = assertThrows(InjectedFaultException.class,
                () -> Failpoints.fire("after_effect_commit"));
        assertEquals("after_effect_commit", e.point());
        List<String> receipts = lines(logPath());
        assertEquals(1, receipts.size());
        assertEquals("fired", Json.parse(receipts.get(0)).get("event").asText());
        assertTrue(Json.parse(receipts.get(0)).get("detail").asText().contains("mechanism=M3"));
    }

    @Test
    void abortTxnActionThrowsInjectedAbort() throws Exception {
        Failpoints.initForTest(true, "before_progress_commit", "abort_txn", 1, 0,
                logPath().toString(), "r", "M1x", 1);
        InjectedAbortException e = assertThrows(InjectedAbortException.class,
                () -> Failpoints.fire("before_progress_commit"));
        assertEquals("before_progress_commit", e.point());
        assertEquals("fired", Json.parse(lines(logPath()).get(0)).get("event").asText());
    }

    @Test
    void pauseActionSleepsAndWritesFiredThenRestored() throws Exception {
        Failpoints.initForTest(true, "before_publish", "pause", 1, 120,
                logPath().toString(), "r", "M0", 1);
        long t0 = System.nanoTime();
        Failpoints.fire("before_publish");
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(elapsedMs >= 100, "pause should sleep ~120 ms, elapsed=" + elapsedMs);
        List<String> receipts = lines(logPath());
        assertEquals(2, receipts.size());
        assertEquals("restored", Json.parse(receipts.get(1)).get("event").asText());
    }

    @Test
    void wrongPointDoesNotFire() throws Exception {
        Failpoints.initForTest(true, "before_publish", "none", 1, 0,
                logPath().toString(), "r", "M0", 1);
        Failpoints.fire("after_fetch");
        assertTrue(lines(logPath()).isEmpty());
        assertEquals(0, Failpoints.hits());
    }

    @Test
    void productionInitRejectsUnknownFailpointName() {
        assertTrue(!FailpointNames.isKnown("not_a_real_point"));
        for (String known : FailpointNames.ALL) {
            assertTrue(FailpointNames.isKnown(known));
        }
        assertEquals(7, FailpointNames.ALL.size(), "seven fixed failpoint names (CONTRACTS §7)");
    }
}
