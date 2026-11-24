package com.wxeo.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Durability tests for S5 finding F-M2: the artifact's fsync path must be real.
 *
 * <p>{@code Files.newOutputStream} does not return a {@code FileOutputStream}, so the previous
 * {@code instanceof FileOutputStream -> getFD().sync()} check was a silent no-op. These tests (a)
 * verify forced writes survive a child process that halts immediately after the call, and (b) check
 * read-back equality; the same mechanism is used for GT, GT digest, manifest, produced manifest,
 * fault receipts and the app/config metrics artefacts.</p>
 */
class DurabilityTest {

    @TempDir
    Path dir;

    @Test
    void forcedWriteSurvivesAnImmediateProcessHalt() throws Exception {
        Path target = dir.resolve("durable.txt");
        String content = "wxeo-durability-" + "0123456789abcdef".repeat(1000);
        Path contentFile = dir.resolve("content.txt");
        IoUtil.writeStringFsync(contentFile, content);

        String java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java").toString();
        String classpath = System.getProperty("surefire.test.class.path",
                System.getProperty("java.class.path"));
        Process p = new ProcessBuilder(java, "-cp", classpath,
                DurabilityWriteProbe.class.getName(), target.toString(), contentFile.toString())
                .redirectErrorStream(true).start();
        assertTrue(p.waitFor(60, TimeUnit.SECONDS), "probe must exit");
        assertEquals(0, p.exitValue(), new String(p.getInputStream().readAllBytes()));

        // The child halted without closing anything after the fsync; the bytes must be readable.
        assertTrue(Files.exists(target), "fsynced file exists after abrupt halt");
        assertEquals(content.length(), Files.size(target));
        assertEquals(content, Files.readString(target, StandardCharsets.UTF_8));
    }

    @Test
    void appendLineFsyncAppendsAndReadsBack() throws Exception {
        Path log = dir.resolve("faults.log");
        for (int i = 0; i < 5; i++) {
            IoUtil.appendLineFsync(log, "{\"n\":" + i + "}");
        }
        List<String> lines = Files.readAllLines(log);
        assertEquals(5, lines.size());
        assertEquals("{\"n\":4}", lines.get(4));
        assertEquals(5L * ("{\"n\":4}\n".length()), Files.size(log));
    }

    @Test
    void atomicFsyncReplacesContentCompletely() throws Exception {
        Path target = dir.resolve("app.json");
        IoUtil.writeAtomicFsync(target, "{\"v\":1}");
        assertEquals("{\"v\":1}", Files.readString(target));
        IoUtil.writeAtomicFsync(target, "{\"v\":2}");
        assertEquals("{\"v\":2}", Files.readString(target));
        assertTrue(Files.exists(target));
    }
}
