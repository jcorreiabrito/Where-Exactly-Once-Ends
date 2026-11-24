package com.wxeo.ledger;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * CANONICAL deterministic ledger-event derivation (docs/method.md §1.4.1) and the canonical JSON
 * byte formats of the source event and the publication-stream entry.
 *
 * <p>This class is part of the S3 artifact; S4 implements the same specification independently in
 * Python. The two implementations must agree byte-for-byte on every canonical posting line and its
 * digest (gate G5). The exact conventions implemented here are documented in
 * {@code artifact/README.md} § "Canonical formats" and were messaged to S4 at P2 start.</p>
 *
 * <pre>
 *   h1 = SHA-256("wxeo1|event|" + decimal(master_seed) + "|" + decimal(seq))
 *   event_id   = lowercase_hex(h1[0:16])      // 32 hex chars
 *   account_d  = "acct:" + lowercase_hex(h1[16:22])   // 12 hex chars
 *   account_c  = "acct:" + lowercase_hex(h1[22:28])
 *   amount     = 1 + (uint64_be(h1[28:36]) mod 100000)
 *   h2         = SHA-256("wxeo1|part|" + decimal(master_seed) + "|" + decimal(seq))
 *   partition  = uint64_be(h2[0:8]) mod P
 * </pre>
 */
public final class Ledger {

    public static final String EVENT_SPEC = "wxeo.ledger-event.v1";
    public static final String ENTRY_SPEC = "wxeo.entry.v1";
    public static final String CURRENCY = "BRL";
    public static final int AMOUNT_MODULUS = 100_000;

    private Ledger() {
    }

    /** One derived event (all values are a pure function of master_seed and seq). */
    public record Derivation(String eventId, String accountD, String accountC, long amountMinor) {
    }

    public record Leg(String accountId, char side, long amountMinor) {
    }

    /** A full ledger event (both legs of one balanced entry). */
    public record EventData(String eventId, int seq, String currency, List<Leg> legs, long scheduledAtMs) {

        public EventData {
            if (legs == null || legs.size() != 2) {
                throw new IllegalArgumentException("an event has exactly two legs");
            }
        }

        /** Canonical source-event JSON (Kafka record value) — compact, fixed key order, no spaces. */
        public String eventValueJson() {
            return "{\"spec\":\"" + EVENT_SPEC + "\""
                    + ",\"event_id\":\"" + eventId + '"'
                    + ",\"seq\":" + seq
                    + ",\"currency\":\"" + currency + '"'
                    + ",\"legs\":["
                    + legJson(legs.get(0)) + ',' + legJson(legs.get(1))
                    + "],\"scheduled_at_ms\":" + scheduledAtMs + '}';
        }

        /** Canonical publication-stream entry JSON (docs/method.md §1.4.3). */
        public String entryValueJson() {
            return "{\"spec\":\"" + ENTRY_SPEC + "\""
                    + ",\"event_id\":\"" + eventId + '"'
                    + ",\"seq\":" + seq
                    + ",\"legs\":["
                    + legJson(legs.get(0)) + ',' + legJson(legs.get(1))
                    + "]}";
        }

        /** Postings of this event, leg 1 = debit, leg 2 = credit; line numbers assigned later. */
        public List<Posting> postings() {
            Leg d = legs.get(0);
            Leg c = legs.get(1);
            List<Posting> out = new ArrayList<>(2);
            out.add(new Posting(0, eventId, 1, d.accountId(), d.side(), d.amountMinor(), currency));
            out.add(new Posting(0, eventId, 2, c.accountId(), c.side(), c.amountMinor(), currency));
            return out;
        }

        public String legSignature() {
            StringBuilder sb = new StringBuilder();
            for (Leg l : legs) {
                sb.append(l.accountId()).append(':').append(l.side()).append(':')
                        .append(l.amountMinor()).append(';');
            }
            return sb.toString();
        }

        private static String legJson(Leg leg) {
            return "{\"account_id\":\"" + leg.accountId() + "\""
                    + ",\"side\":\"" + leg.side() + '"'
                    + ",\"amount_minor\":" + leg.amountMinor() + '}';
        }
    }

    /** A parsed source record together with its Kafka coordinates. */
    public record ParsedEvent(EventData event, int partition, long offset, String rawValue) {
    }

    /**
     * Canonicalises the {@code --master-seed} argument. Accepted forms: decimal unsigned 64-bit
     * (CONTRACTS.md §7) or {@code 0x}-prefixed hexadecimal. The canonical form used inside every
     * hash input is the plain decimal representation of the value (no sign, no leading zeros).
     */
    public static String canonicalSeed(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("master_seed is empty");
        }
        String s = raw.trim();
        BigInteger v;
        if (s.startsWith("0x") || s.startsWith("0X")) {
            v = new BigInteger(s.substring(2), 16);
        } else {
            v = new BigInteger(s, 10);
        }
        if (v.signum() < 0) {
            throw new IllegalArgumentException("master_seed must be unsigned: " + raw);
        }
        if (v.bitLength() > 64) {
            throw new IllegalArgumentException("master_seed exceeds u64: " + raw);
        }
        return v.toString(10);
    }

    public static Derivation derive(String canonicalSeed, int seq) {
        byte[] h1 = sha256(("wxeo1|event|" + canonicalSeed + '|' + seq).getBytes(StandardCharsets.US_ASCII));
        String eventId = HexFormat.of().formatHex(h1, 0, 16);
        String accountD = "acct:" + HexFormat.of().formatHex(h1, 16, 22);
        String accountC = "acct:" + HexFormat.of().formatHex(h1, 22, 28);
        long amount = 1 + unsignedModBe(h1, 28, 8, AMOUNT_MODULUS);
        return new Derivation(eventId, accountD, accountC, amount);
    }

    public static int partition(String canonicalSeed, int seq, int partitions) {
        if (partitions <= 0) {
            throw new IllegalArgumentException("partitions must be positive");
        }
        byte[] h2 = sha256(("wxeo1|part|" + canonicalSeed + '|' + seq).getBytes(StandardCharsets.US_ASCII));
        return (int) unsignedModBe(h2, 0, 8, partitions);
    }

    /** Builds the full event for {@code (seed, seq)} with the given open-loop schedule time. */
    public static EventData event(String canonicalSeed, int seq, long scheduledAtMs) {
        Derivation d = derive(canonicalSeed, seq);
        return new EventData(
                d.eventId(),
                seq,
                CURRENCY,
                List.of(new Leg(d.accountD(), 'D', d.amountMinor()),
                        new Leg(d.accountC(), 'C', d.amountMinor())),
                scheduledAtMs);
    }

    /** Canonical source-event value bytes for {@code (seed, seq)}. */
    public static String eventValueJson(String canonicalSeed, int seq, long scheduledAtMs) {
        return event(canonicalSeed, seq, scheduledAtMs).eventValueJson();
    }

    /** Parses and validates a source-topic record value. */
    public static EventData parse(String rawValue) {
        com.fasterxml.jackson.databind.JsonNode n;
        try {
            n = com.wxeo.util.Json.parse(rawValue);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("malformed event JSON: " + e.getMessage(), e);
        }
        String spec = text(n, "spec");
        if (!EVENT_SPEC.equals(spec)) {
            throw new IllegalArgumentException("unsupported event spec: " + spec);
        }
        String eventId = text(n, "event_id");
        if (!eventId.matches("[0-9a-f]{32}")) {
            throw new IllegalArgumentException("event_id must be 32 lowercase hex chars: " + eventId);
        }
        int seq = n.path("seq").asInt(-1);
        if (seq < 0) {
            throw new IllegalArgumentException("seq missing/negative");
        }
        String currency = text(n, "currency");
        if (!currency.matches("[A-Z]{3}")) {
            throw new IllegalArgumentException("invalid currency: " + currency);
        }
        var legsNode = n.path("legs");
        if (!legsNode.isArray() || legsNode.size() != 2) {
            throw new IllegalArgumentException("event must carry exactly two legs");
        }
        List<Leg> legs = new ArrayList<>(2);
        for (var ln : legsNode) {
            String acct = text(ln, "account_id");
            String side = text(ln, "side");
            if (!side.equals("D") && !side.equals("C")) {
                throw new IllegalArgumentException("invalid side: " + side);
            }
            long amount = ln.path("amount_minor").asLong(Long.MIN_VALUE);
            if (amount <= 0) {
                throw new IllegalArgumentException("amount_minor must be positive");
            }
            legs.add(new Leg(acct, side.charAt(0), amount));
        }
        Leg d = legs.get(0);
        Leg c = legs.get(1);
        if (d.side() != 'D' || c.side() != 'C') {
            throw new IllegalArgumentException("leg 1 must be D and leg 2 must be C");
        }
        if (d.amountMinor() != c.amountMinor()) {
            throw new IllegalArgumentException("unbalanced event: legs differ in amount");
        }
        if (d.accountId().equals(c.accountId())) {
            throw new IllegalArgumentException("debit and credit accounts must differ");
        }
        long scheduled = n.path("scheduled_at_ms").asLong(Long.MIN_VALUE);
        if (scheduled == Long.MIN_VALUE) {
            throw new IllegalArgumentException("scheduled_at_ms missing");
        }
        return new EventData(eventId, seq, currency, legs, scheduled);
    }

    /** Byte-exact SHA-256 of a canonical source-event value. */
    public static String eventValueSha256(String rawValue) {
        return com.wxeo.util.IoUtil.sha256Hex(rawValue.getBytes(StandardCharsets.UTF_8));
    }

    private static String text(com.fasterxml.jackson.databind.JsonNode n, String field) {
        var v = n.get(field);
        if (v == null || !v.isTextual() || v.asText().isEmpty()) {
            throw new IllegalArgumentException("missing textual field: " + field);
        }
        return v.asText();
    }

    /** Unsigned big-endian integer in {@code bytes[off, off+len)} modulo {@code mod}, without overflow. */
    static long unsignedModBe(byte[] bytes, int off, int len, long mod) {
        long acc = 0;
        for (int i = 0; i < len; i++) {
            acc = ((acc << 8) | (bytes[off + i] & 0xFFL)) % mod;
        }
        return acc;
    }

    private static byte[] sha256(byte[] input) {
        MessageDigest md = com.wxeo.util.IoUtil.sha256();
        return md.digest(input);
    }
}
