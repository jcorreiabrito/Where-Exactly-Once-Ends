package com.wxeo.sink;

import com.wxeo.config.EffectiveConfig;
import com.wxeo.config.EffectiveConfig.MechanismKind;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Canonical PostgreSQL DDL (docs/method.md §1.4.2, §4; docs/CONTRACTS.md §2, §10).
 *
 * <p>The ledger table DDL is <b>identical for every DB-sink mechanism</b>. The only permitted
 * schema differences are the mechanism's auxiliary tables and the de-duplication index, exactly as
 * declared in §4.11's mechanism-intrinsic list. Schema naming: {@code run_<run_id>} (CONTRACTS §2;
 * note that method.md §8.3 writes {@code wxeo_run_<run_id>} — the frozen S3↔S4 contract wins).</p>
 */
public final class SchemaDdl {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_]{1,63}");

    private SchemaDdl() {
    }

    public static void requireIdentifier(String identifier, String what) {
        if (identifier == null || !IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException(what + " must match [A-Za-z0-9_]{1,63}: " + identifier);
        }
    }

    public static List<String> init(String schema, MechanismKind mechanism, String appRole, String readRole) {
        requireIdentifier(schema, "schema");
        List<String> out = new ArrayList<>();
        out.add("CREATE SCHEMA IF NOT EXISTS " + schema);
        out.add("CREATE TABLE IF NOT EXISTS " + schema + ".posting ("
                + "posting_id bigserial PRIMARY KEY,"
                + " event_id text NOT NULL,"
                + " leg_no smallint NOT NULL CHECK (leg_no BETWEEN 1 AND 2),"
                + " account_id text NOT NULL,"
                + " side char(1) NOT NULL CHECK (side IN ('D','C')),"
                + " amount_minor bigint NOT NULL CHECK (amount_minor > 0),"
                + " currency char(3) NOT NULL,"
                + " produced_at timestamptz NOT NULL,"
                + " inserted_at timestamptz NOT NULL DEFAULT clock_timestamp()"
                + ")");
        if (mechanism.dedupIndex) {
            out.add("CREATE UNIQUE INDEX IF NOT EXISTS posting_dedup_idx ON " + schema
                    + ".posting (event_id, leg_no)");
        }
        out.add("CREATE OR REPLACE FUNCTION " + schema + ".posting_deny_mutation() RETURNS trigger "
                + "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'posting is append-only'; END $$");
        out.add("DROP TRIGGER IF EXISTS posting_append_only ON " + schema + ".posting");
        out.add("CREATE TRIGGER posting_append_only BEFORE UPDATE OR DELETE ON " + schema
                + ".posting FOR EACH ROW EXECUTE FUNCTION " + schema + ".posting_deny_mutation()");
        if (mechanism.inbox || mechanism.outboxRelay) {
            out.add("CREATE TABLE IF NOT EXISTS " + schema
                    + ".inbox (event_id text PRIMARY KEY,"
                    + " processed_at timestamptz NOT NULL DEFAULT clock_timestamp())");
        }
        if (mechanism.outboxRelay) {
            out.add("CREATE TABLE IF NOT EXISTS " + schema + ".outbox ("
                    + "outbox_id bigserial PRIMARY KEY,"
                    + " event_id text NOT NULL UNIQUE,"
                    + " payload jsonb NOT NULL,"
                    + " created_at timestamptz NOT NULL DEFAULT clock_timestamp(),"
                    + " published_at timestamptz NULL)");
        }
        if (mechanism.hasProgressTable()) {
            out.add("CREATE TABLE IF NOT EXISTS " + schema + ".consumer_progress ("
                    + "partition int PRIMARY KEY,"
                    + " next_offset bigint NOT NULL,"
                    + " owner_epoch bigint NOT NULL,"
                    + " updated_at timestamptz NOT NULL DEFAULT clock_timestamp())");
        }
        // Append-only enforcement + least privilege (docs/method.md §1.4.2, §2.5 P8).
        out.add("REVOKE UPDATE, DELETE ON " + schema + ".posting FROM PUBLIC");
        if (appRole != null && !appRole.isBlank()) {
            requireIdentifier(appRole, "app role");
            out.add("GRANT USAGE ON SCHEMA " + schema + " TO " + appRole);
            out.add("GRANT INSERT, SELECT ON " + schema + ".posting TO " + appRole);
            if (mechanism.inbox || mechanism.outboxRelay) {
                out.add("GRANT INSERT, SELECT, UPDATE ON " + schema + ".inbox TO " + appRole);
            }
            if (mechanism.outboxRelay) {
                out.add("GRANT INSERT, SELECT, UPDATE ON " + schema + ".outbox TO " + appRole);
            }
            if (mechanism.hasProgressTable()) {
                out.add("GRANT INSERT, SELECT, UPDATE ON " + schema + ".consumer_progress TO " + appRole);
            }
            out.add("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA " + schema + " TO " + appRole);
        }
        if (readRole != null && !readRole.isBlank()) {
            requireIdentifier(readRole, "read role");
            out.add("GRANT USAGE ON SCHEMA " + schema + " TO " + readRole);
            out.add("GRANT SELECT ON ALL TABLES IN SCHEMA " + schema + " TO " + readRole);
        }
        return out;
    }

    public static String drop(String schema) {
        requireIdentifier(schema, "schema");
        return "DROP SCHEMA IF EXISTS " + schema + " CASCADE";
    }

    // ---- canonical runtime SQL (kept here so unit tests can assert on the exact statements) ----

    public static String insertPlain(String schema) {
        return "INSERT INTO " + schema + ".posting"
                + " (event_id, leg_no, account_id, side, amount_minor, currency, produced_at)"
                + " VALUES (?,?,?,?,?,?,?)";
    }

    public static String insertIdem(String schema) {
        return "INSERT INTO " + schema + ".posting"
                + " (event_id, leg_no, account_id, side, amount_minor, currency, produced_at)"
                + " VALUES (?,?,?,?,?,?,?) ON CONFLICT (event_id, leg_no) DO NOTHING";
    }

    public static String claimInbox(String schema) {
        return "INSERT INTO " + schema + ".inbox (event_id) VALUES (?)"
                + " ON CONFLICT (event_id) DO NOTHING";
    }

    public static String insertOutboxPlain(String schema) {
        return "INSERT INTO " + schema + ".outbox (event_id, payload) VALUES (?, ?::jsonb)";
    }

    public static String insertOutboxIdem(String schema) {
        return "INSERT INTO " + schema + ".outbox (event_id, payload) VALUES (?, ?::jsonb)"
                + " ON CONFLICT (event_id) DO NOTHING";
    }

    /** Monotone guard of docs/method.md §4.7 (upsert so the first batch creates the row). */
    public static String upsertProgress(String schema) {
        return "INSERT INTO " + schema + ".consumer_progress (partition, next_offset, owner_epoch)"
                + " VALUES (?,?,?)"
                + " ON CONFLICT (partition) DO UPDATE"
                + " SET next_offset = EXCLUDED.next_offset, owner_epoch = EXCLUDED.owner_epoch,"
                + " updated_at = clock_timestamp()"
                + " WHERE " + schema + ".consumer_progress.next_offset < EXCLUDED.next_offset";
    }

    public static String readProgress(String schema) {
        return "SELECT next_offset FROM " + schema + ".consumer_progress WHERE partition = ?";
    }

    public static String selectUnpublishedOutbox(String schema) {
        return "SELECT outbox_id, event_id, payload FROM " + schema + ".outbox"
                + " WHERE published_at IS NULL ORDER BY outbox_id LIMIT ?";
    }

    public static String markOutboxPublished(String schema) {
        return "UPDATE " + schema + ".outbox SET published_at = clock_timestamp()"
                + " WHERE outbox_id = ? AND published_at IS NULL";
    }

    public static String updateCount(String schema) {
        return "SELECT count(*) FROM " + schema + ".posting";
    }

    /** Default auxiliary table names, for SchemaTool reporting and the oracle's expectations. */
    public static List<String> tables(String schema, MechanismKind mechanism) {
        List<String> t = new ArrayList<>();
        t.add(schema + ".posting");
        if (mechanism.inbox || mechanism.outboxRelay) {
            t.add(schema + ".inbox");
        }
        if (mechanism.outboxRelay) {
            t.add(schema + ".outbox");
        }
        if (mechanism.hasProgressTable()) {
            t.add(schema + ".consumer_progress");
        }
        return t;
    }

    public static boolean hasDedupIndex(MechanismKind mechanism) {
        return mechanism.dedupIndex;
    }

    public static boolean hasAppendOnlyTrigger(MechanismKind mechanism) {
        return mechanism != null && mechanism.dbSink();
    }

    public static MechanismKind mechanismFor(String wire) {
        return EffectiveConfig.MechanismKind.parse(wire);
    }
}
