package com.wxeo.ledger;

import com.wxeo.util.IoUtil;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;

/**
 * Ground-truth (GT) file writer — phase 1 of the deterministic load generator.
 *
 * <p>Contract (docs/CONTRACTS.md §4, docs/method.md §1.4.4, A1 §4/D10): the complete expected
 * posting set is written, hashed and fsynced <b>before any produce</b>. One posting leg per line,
 * canonical compact JSON, keys exactly {@code line_no,event_id,leg_no,account_id,side,
 * amount_minor,currency}, LF line endings, ordered by {@code (event_id, leg_no)} with byte-wise
 * event_id ordering; {@code line_no} is the 0-based index in the file. The companion
 * {@code .sha256} file is lowercase hex + LF.</p>
 */
public final class GroundTruth {

    public static final String SPEC = "wxeo.gt.v1";

    private GroundTruth() {
    }

    /** Result of writing the GT file: digest and line count. */
    public record GtResult(String gtSha256, long lines, long events, String path) {
    }

    /**
     * Generates, writes, hashes and fsyncs {@code gt_<runId>.ndjson} and
     * {@code gt_<runId>.sha256} for {@code events} deterministic events derived from the seed.
     * Phase 2 of the generator must re-derive (never read back this file).
     */
    public static GtResult write(Path gtPath, String canonicalSeed, int events) throws IOException {
        IoUtil.ensureDir(gtPath.toAbsolutePath().getParent());
        String[] eventIds = new String[events];
        for (int seq = 0; seq < events; seq++) {
            eventIds[seq] = Ledger.derive(canonicalSeed, seq).eventId();
        }
        Integer[] order = new Integer[events];
        for (int i = 0; i < events; i++) {
            order[i] = i;
        }
        Arrays.sort(order, Comparator.comparing((Integer i) -> eventIds[i])
                .thenComparingInt(i -> i)); // stable tie-break by seq; 128-bit collisions are practically impossible

        MessageDigest md = IoUtil.sha256();
        long lineNo = 0;
        // Real durability: explicit FileChannel + force(true) (S5 finding F-M2). The stream wrapper is
        // only for buffering; the force is called on the channel after the final flush.
        try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(gtPath,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
             BufferedOutputStream out = new BufferedOutputStream(
                     java.nio.channels.Channels.newOutputStream(channel), 1 << 20)) {
            for (Integer seq : order) {
                Ledger.EventData ev = Ledger.event(canonicalSeed, seq, 0L);
                for (Posting p : ev.postings()) {
                    Posting numbered = new Posting((int) lineNo, p.eventId(), p.legNo(),
                            p.accountId(), p.side(), p.amountMinor(), p.currency());
                    String line = numbered.gtLine();
                    byte[] bytes = line.getBytes(StandardCharsets.US_ASCII);
                    md.update(bytes);
                    md.update((byte) '\n');
                    out.write(bytes);
                    out.write('\n');
                    lineNo++;
                }
            }
            out.flush();
            channel.force(true);
        }
        String digest = HexFormat.of().formatHex(md.digest());
        IoUtil.writeStringFsync(shaPath(gtPath), digest + "\n");
        return new GtResult(digest, lineNo, events, gtPath.toString());
    }

    public static Path shaPath(Path gtPath) {
        return gtPath.resolveSibling(gtPath.getFileName().toString().replace(".ndjson", ".sha256"));
    }
}
