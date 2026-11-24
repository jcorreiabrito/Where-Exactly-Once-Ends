package com.wxeo.ledger;

import com.wxeo.util.IoUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Contract tests of the ground-truth artefact: canonical lines, ordering, digest, fsynced files. */
class GroundTruthTest {

    @Test
    void writesCanonicalSortedLinesAndDigest(@TempDir Path dir) throws Exception {
        Path gt = dir.resolve("gt_test-run.ndjson");
        GroundTruth.GtResult r = GroundTruth.write(gt, Ledger.canonicalSeedFromHex("0123456789abcdef"), 3);

        List<String> lines = Files.readAllLines(gt, StandardCharsets.UTF_8);
        assertEquals(6, lines.size());
        assertEquals(6, r.lines());
        assertEquals(3, r.events());

        // Every line is canonical JSON with exactly the frozen key order and no spaces.
        int expectedLineNo = 0;
        String previousEventId = "";
        for (String line : lines) {
            assertTrue(line.startsWith("{\"line_no\":" + expectedLineNo + ",\"event_id\":\""), line);
            assertTrue(line.endsWith(",\"currency\":\"BRL\"}"), line);
            assertTrue(!line.contains(" "), "no spaces: " + line);
            var node = com.wxeo.util.Json.parse(line);
            String eventId = node.get("event_id").asText();
            assertTrue(eventId.compareTo(previousEventId) >= 0, "ordered by event_id");
            previousEventId = eventId;
            expectedLineNo++;
        }
        // leg_no alternates 1,2 for the first event, and equals 1 at even line numbers.
        for (int i = 0; i < lines.size(); i++) {
            var node = com.wxeo.util.Json.parse(lines.get(i));
            assertEquals((i % 2 == 0) ? 1 : 2, node.get("leg_no").asInt());
        }

        // The companion .sha256 file is hex+LF and equals the digest of the exact file bytes.
        Path shaFile = GroundTruth.shaPath(gt);
        String shaText = Files.readString(shaFile, StandardCharsets.US_ASCII);
        assertEquals(65, shaText.length(), "64 hex chars + LF");
        assertEquals(r.gtSha256() + "\n", shaText);
        assertEquals(r.gtSha256(), IoUtil.sha256Hex(gt));
        assertEquals("f4e127d98bddf1fba21f20b8b4cc9870995626acc8c152486118c8f822ea4f72",
                r.gtSha256(), "golden digest identical to the independent Python computation");
    }

    @Test
    void emptyGroundTruthIsJustAnEmptyFile(@TempDir Path dir) throws Exception {
        Path gt = dir.resolve("gt_empty.ndjson");
        GroundTruth.GtResult r = GroundTruth.write(gt, "0", 0);
        assertEquals(0, r.lines());
        assertEquals(0, Files.size(gt));
        assertEquals(IoUtil.sha256Hex(gt), r.gtSha256());
    }
}
