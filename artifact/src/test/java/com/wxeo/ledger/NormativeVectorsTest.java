package com.wxeo.ledger;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Gate G5: loads the normative cross-implementation vectors committed by the lead at
 * {@code docs/derive-vectors.csv} and asserts every row against this Java implementation. The
 * Python oracle asserts the same file independently; both must match byte-for-byte.
 */
class NormativeVectorsTest {

    private static Path repoRoot() {
        Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        for (int i = 0; i < 8 && dir != null; i++) {
            if (Files.isRegularFile(dir.resolve("docs/derive-vectors.csv"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        return null;
    }

    @Test
    void allNormativeVectorsMatch() throws IOException {
        Path root = repoRoot();
        assertNotNull(root, "docs/derive-vectors.csv not found from " + System.getProperty("user.dir"));
        List<String> lines = Files.readAllLines(root.resolve("docs/derive-vectors.csv"), StandardCharsets.UTF_8);
        assertTrue(lines.size() > 1, "normative vectors file must contain rows");
        assertEquals("seed_hex,seq,event_id,account_d,account_c,amount_minor,partition_p6,partition_p3",
                lines.get(0).trim(), "unexpected normative-vector header");
        int checked = 0;
        for (String line : lines.subList(1, lines.size())) {
            if (line.isBlank()) {
                continue;
            }
            String[] f = line.trim().split(",", -1);
            assertEquals(8, f.length, "row field count: " + line);
            String seedHex = f[0];
            int seq = Integer.parseInt(f[1]);
            String canonical = Ledger.canonicalSeedFromHex(seedHex);
            Ledger.Derivation d = Ledger.derive(canonical, seq);
            assertEquals(f[2], d.eventId(), "event_id row " + line);
            assertEquals(f[3], d.accountD(), "account_d row " + line);
            assertEquals(f[4], d.accountC(), "account_c row " + line);
            assertEquals(Long.parseLong(f[5]), d.amountMinor(), "amount_minor row " + line);
            assertEquals(Integer.parseInt(f[6]), Ledger.partition(canonical, seq, 6), "partition_p6 row " + line);
            assertEquals(Integer.parseInt(f[7]), Ledger.partition(canonical, seq, 3), "partition_p3 row " + line);
            checked++;
        }
        assertEquals(6, checked, "expected the six normative rows");
    }
}
