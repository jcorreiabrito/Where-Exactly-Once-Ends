package com.wxeo.ledger;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * G5 cross-implementation gate support: hard-coded golden vectors of the canonical derivation,
 * computed independently (Python, from docs/method.md §1.4.1) and embedded here. The Python oracle
 * must produce the same values; any disagreement invalidates the run.
 *
 * <p>Spec erratum recorded here and in artifact/README.md: §1.4.1 writes
 * {@code uint64_be(h1[28:36])} but h1 is a 32-byte SHA-256 digest, so only h1[28:32] exists. Both
 * implementations use the literal slice semantics (the available 4-byte suffix, big-endian).</p>
 */
class EventDerivationTest {

    private static final String SEED_0123 = "0123456789abcdef";
    private static final String SEED_FFFF = "ffffffffffffffff";
    private static final String SEED_ZERO = "0000000000000000";
    private static final String SEED_DEAD = "deadbeefcafebabe";
    private static final String SEED_A1B2 = "a1b2c3d4e5f60718";

    /** seed_hex, expected canonical decimal, seq, event_id, account_d, account_c, amount, p6, p3. */
    private record Vector(String seedHex, String seedDecimal, int seq, String eventId,
                          String accountD, String accountC, long amount, int p6, int p3) {
    }

    private static final List<Vector> GOLDEN = List.of(
            new Vector(SEED_0123, "81985529216486895", 0,
                    "545573c0b4fd567b7ea1070fa7118dd4", "acct:05a56c424930", "acct:35be9292dea0",
                    70652, 3, 0),
            new Vector(SEED_0123, "81985529216486895", 7,
                    "7fa20e79e24cf186a3256630d8629a68", "acct:0de9f4b074f7", "acct:12d743568954",
                    47039, 0, 0),
            new Vector(SEED_FFFF, "18446744073709551615", 42,
                    "40eb1d2c6ee7b8c35289d0677e1e5de9", "acct:d2ac7acbc1b8", "acct:4317d35fba65",
                    18334, 5, 2),
            new Vector(SEED_ZERO, "0", 1,
                    "b61a81d68cb064fca415b987c0c8e67f", "acct:2638a4ef1189", "acct:86fb1600bcc5",
                    72432, 1, 1),
            new Vector(SEED_DEAD, "16045690984503098046", 999999,
                    "7691fe4d4d9153e85bdce157a2105156", "acct:189927ff94da", "acct:10882cbd9a98",
                    80711, 2, 2),
            new Vector(SEED_A1B2, "11651590505119483672", 0,
                    "4580fc82fb0ee438f4e430cb34ec78dc", "acct:45b53d643489", "acct:91e0bd2f21cb",
                    93251, 1, 1));

    @Test
    void goldenVectorsMatchTheIndependentSpecification() {
        for (Vector v : GOLDEN) {
            String canonical = Ledger.canonicalSeedFromHex(v.seedHex());
            assertEquals(v.seedDecimal(), canonical, "canonical decimal seed for " + v.seedHex());
            Ledger.Derivation d = Ledger.derive(canonical, v.seq());
            assertEquals(v.eventId(), d.eventId(), "event_id for " + v.seedHex() + "/" + v.seq());
            assertEquals(v.accountD(), d.accountD(), "account_d");
            assertEquals(v.accountC(), d.accountC(), "account_c");
            assertEquals(v.amount(), d.amountMinor(), "amount_minor");
            assertEquals(v.p6(), Ledger.partition(canonical, v.seq(), 6), "partition P=6");
            assertEquals(v.p3(), Ledger.partition(canonical, v.seq(), 3), "partition P=3");
        }
    }

    @Test
    void canonicalSeedAcceptsDecimalAndHexForms() {
        assertEquals("18446744073709551615", Ledger.canonicalSeed("0xffffffffffffffff"));
        assertEquals("18446744073709551615", Ledger.canonicalSeed("0xffffffffffffffff"));
        assertEquals("18446744073709551615", Ledger.canonicalSeedFromHex("ffffffffffffffff"));
        assertEquals("1", Ledger.canonicalSeed("0000000000000001"));
        assertEquals("0", Ledger.canonicalSeed("0x0"));
        assertThrows(IllegalArgumentException.class, () -> Ledger.canonicalSeed("0x10000000000000000"));
        assertThrows(IllegalArgumentException.class, () -> Ledger.canonicalSeed("-1"));
    }

    @Test
    void eventValueJsonIsCompactAndKeyOrdered() {
        String canonical = Ledger.canonicalSeedFromHex(SEED_0123);
        String json = Ledger.event(canonical, 0, 1764000000000L).eventValueJson();
        assertEquals("{\"spec\":\"wxeo.ledger-event.v1\""
                + ",\"event_id\":\"545573c0b4fd567b7ea1070fa7118dd4\""
                + ",\"seq\":0,\"currency\":\"BRL\""
                + ",\"legs\":[{\"account_id\":\"acct:05a56c424930\",\"side\":\"D\",\"amount_minor\":70652}"
                + ",{\"account_id\":\"acct:35be9292dea0\",\"side\":\"C\",\"amount_minor\":70652}]"
                + ",\"scheduled_at_ms\":1764000000000}", json);
        assertTrue(!json.contains(" "), "canonical JSON contains no spaces");
        // Round-trip through the parser (this is what the consumer does).
        Ledger.EventData parsed = Ledger.parse(json);
        assertEquals(0, parsed.seq());
        assertEquals(70652, parsed.legs().get(0).amountMinor());
        assertEquals('D', parsed.legs().get(0).side());
    }

    @Test
    void entryValueJsonIsCompactAndKeyOrdered() {
        String canonical = Ledger.canonicalSeedFromHex(SEED_0123);
        String json = Ledger.event(canonical, 0, 0L).entryValueJson();
        assertEquals("{\"spec\":\"wxeo.entry.v1\""
                + ",\"event_id\":\"545573c0b4fd567b7ea1070fa7118dd4\""
                + ",\"seq\":0"
                + ",\"legs\":[{\"account_id\":\"acct:05a56c424930\",\"side\":\"D\",\"amount_minor\":70652}"
                + ",{\"account_id\":\"acct:35be9292dea0\",\"side\":\"C\",\"amount_minor\":70652}]}", json);
    }

    /** Golden ground-truth digest: seed 0123456789abcdef, N=3 events (6 posting lines). */
    @Test
    void goldenGroundTruthDigest() throws Exception {
        String canonical = Ledger.canonicalSeedFromHex(SEED_0123);
        StringBuilder sb = new StringBuilder();
        java.util.List<String[]> rows = new java.util.ArrayList<>();
        for (int seq = 0; seq < 3; seq++) {
            Ledger.EventData ev = Ledger.event(canonical, seq, 0L);
            rows.add(new String[]{ev.eventId(), String.valueOf(seq)});
            // keep derivation values for the expected lines below
        }
        rows.sort(java.util.Comparator.comparing(r -> r[0]));
        int lineNo = 0;
        for (String[] row : rows) {
            Ledger.EventData ev = null;
            for (int seq = 0; seq < 3; seq++) {
                Ledger.EventData candidate = Ledger.event(canonical, seq, 0L);
                if (candidate.eventId().equals(row[0])) {
                    ev = candidate;
                }
            }
            for (Posting p : ev.postings()) {
                sb.append(new Posting(lineNo++, p.eventId(), p.legNo(), p.accountId(), p.side(),
                        p.amountMinor(), p.currency()).gtLine()).append('\n');
            }
        }
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        String digest = java.util.HexFormat.of().formatHex(
                md.digest(sb.toString().getBytes(StandardCharsets.UTF_8)));
        assertEquals("f4e127d98bddf1fba21f20b8b4cc9870995626acc8c152486118c8f822ea4f72", digest,
                "GT digest for seed=0123456789abcdef N=3 must equal the independent Python computation");
    }

    @Test
    void invalidEventsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> Ledger.parse("{}"));
        assertThrows(IllegalArgumentException.class, () -> Ledger.parse(
                "{\"spec\":\"wxeo.ledger-event.v1\",\"event_id\":\"zz\",\"seq\":0,\"currency\":\"BRL\","
                        + "\"legs\":[],\"scheduled_at_ms\":1}"));
    }
}
