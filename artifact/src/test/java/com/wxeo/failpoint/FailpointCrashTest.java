package com.wxeo.failpoint;

import com.wxeo.util.Json;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kill-path test for the {@code crash} failpoint (docs/CONTRACTS.md §7; docs/method.md Appendix B):
 * the child JVM must die with exit code 99 and leave exactly one {@code fired} receipt and no
 * {@code restored} receipt.
 */
class FailpointCrashTest {

    @TempDir
    Path dir;

    @Test
    void crashFailpointHaltsWithExitCode99AndOneFiredReceipt() throws Exception {
        Path log = dir.resolve("faults.log");
        String java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java").toString();
        String classpath = System.getProperty("surefire.test.class.path",
                System.getProperty("java.class.path"));
        Process process = new ProcessBuilder(java, "-cp", classpath,
                FailpointCrashProbe.class.getName(), log.toString())
                .redirectErrorStream(true)
                .start();
        boolean exited = process.waitFor(60, TimeUnit.SECONDS);
        String output = new String(process.getInputStream().readAllBytes());
        assertTrue(exited, "child JVM must exit, output=" + output);
        assertEquals(99, process.exitValue(), "crash failpoint must exit with 99, output=" + output);
        assertTrue(output.contains("NOT_REACHED") == false, "process must halt before returning");

        List<String> receipts = Files.readAllLines(log);
        assertEquals(1, receipts.size(), "exactly one receipt for a terminal crash failpoint");
        var receipt = Json.parse(receipts.get(0));
        assertEquals("fired", receipt.get("event").asText());
        assertEquals("before_effect_commit", receipt.get("fault").asText());
        assertEquals("probe-run", receipt.get("run_id").asText());
        assertTrue(receipt.get("detail").asText().contains("action=crash"));
    }
}
