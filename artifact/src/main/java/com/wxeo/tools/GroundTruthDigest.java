package com.wxeo.tools;

import com.wxeo.config.Args;
import com.wxeo.ledger.GroundTruth;
import com.wxeo.ledger.Ledger;
import com.wxeo.util.IoUtil;
import com.wxeo.util.Json;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * G5 cross-check helper: prints the Java-side ground-truth digest and sample canonical lines for a
 * given per-run data seed and event count, so S4 can diff them against the independent Python
 * oracle's {@code experiments/oracle/g5-vectors.json}. Also usable in CI.
 *
 * <pre>
 * java -cp artifact/target/wxeo-service.jar com.wxeo.tools.GroundTruthDigest \
 *   --seed-hex=0000000000000001 --events=100 --partitions=6 [--out-dir=&lt;scratch&gt;] [--keep]
 * </pre>
 */
public final class GroundTruthDigest {

    public static void main(String[] argv) {
        Args a = Args.parse(argv);
        Map<String, Object> out = new LinkedHashMap<>();
        Path tmpDir = null;
        try {
            String seedHex = a.get("seed-hex", null);
            String canonical;
            if (seedHex != null) {
                canonical = new java.math.BigInteger(seedHex.replaceFirst("^0[xX]", ""), 16).toString(10);
            } else {
                canonical = Ledger.canonicalSeed(a.require("master-seed"));
            }
            int events = a.getInt("events", 100);
            int partitions = a.getInt("partitions", 6);
            String outDir = a.get("out-dir", null);
            tmpDir = outDir != null ? IoUtil.ensureDir(Path.of(outDir))
                    : Files.createTempDirectory("wxeo-gt-");
            Path gt = tmpDir.resolve("gt_g5.ndjson");
            GroundTruth.GtResult r = GroundTruth.write(gt, canonical, events);

            List<String> sample = new ArrayList<>();
            List<String> lines = Files.readAllLines(gt);
            if (!lines.isEmpty()) {
                sample.add(lines.get(0));
                sample.add(lines.get(1));
                sample.add(lines.get(lines.size() - 2));
                sample.add(lines.get(lines.size() - 1));
            }
            Ledger.Derivation first = Ledger.derive(canonical, 0);

            out.put("seed_hex", seedHex);
            out.put("master_seed", canonical);
            out.put("events", events);
            out.put("partitions", partitions);
            out.put("gt_lines", r.lines());
            out.put("gt_sha256", r.gtSha256());
            out.put("sample_lines", sample);
            out.put("first_event", Map.of(
                    "event_id", first.eventId(),
                    "account_d", first.accountD(),
                    "account_c", first.accountC(),
                    "amount_minor", first.amountMinor(),
                    "partition", Ledger.partition(canonical, 0, partitions)));
            out.put("gt_path", gt.toAbsolutePath().toString());
            if (!a.getBool("keep", false) && outDir == null) {
                try {
                    Files.deleteIfExists(gt);
                    Files.deleteIfExists(GroundTruth.shaPath(gt));
                    Files.deleteIfExists(tmpDir);
                } catch (Exception ignored) {
                    // temp cleanup is best effort
                }
            }
            out.put("ok", true);
        } catch (Exception e) {
            out.put("ok", false);
            out.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
            System.err.println(e);
        }
        System.out.println(Json.write(out));
        System.exit(Boolean.TRUE.equals(out.get("ok")) ? 0 : 1);
    }

    private GroundTruthDigest() {
    }
}
