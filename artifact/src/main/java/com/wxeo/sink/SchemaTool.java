package com.wxeo.sink;

import com.wxeo.config.Args;
import com.wxeo.config.EffectiveConfig;
import com.wxeo.config.EffectiveConfig.MechanismKind;
import com.wxeo.util.Json;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Fresh-state schema lifecycle helper for S4's runner (docs/CONTRACTS.md §7, §10).
 *
 * <pre>
 *   java -cp artifact/target/wxeo-service.jar com.wxeo.sink.SchemaTool \
 *     --mode=init|drop|verify --db-url=jdbc:postgresql://127.0.0.1:5433/wxeo \
 *     --db-user=postgres [--db-password=...] --schema=wxeo_run_&lt;run_id&gt; \
 *     --mechanism=&lt;M0|M1-kafka|...&gt; [--app-role=wxeo_app] [--read-role=wxeo_read]
 * </pre>
 *
 * <p>Prints one compact JSON result line to stdout; exit code 0 on success, 1 on failure. GRANT
 * statements that reference a role which does not exist are reported as warnings, not failures, so
 * a minimal local stack (superuser only) still works.</p>
 */
public final class SchemaTool {

    public static void main(String[] argv) {
        Args a = Args.parse(argv);
        String mode = a.get("mode", "init");
        String dbUrl = a.get("db-url", null);
        String schema = a.get("schema", null);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("schema", "wxeo.schematool.v1");
        out.put("mode", mode);
        out.put("schema_name", schema);
        try {
            if (dbUrl == null || schema == null) {
                throw new IllegalArgumentException("--db-url and --schema are required");
            }
            String dbUser = a.get("db-user", "postgres");
            String dbPass = a.get("db-password", "");
            String mechanismWire = a.get("mechanism", "M0");
            MechanismKind mechanism = MechanismKind.parse(mechanismWire);
            out.put("mechanism", mechanism.wireName);
            try (Connection c = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
                switch (mode) {
                    case "init" -> init(c, schema, mechanism, a, out);
                    case "drop" -> drop(c, schema, out);
                    case "verify" -> verify(c, schema, mechanism, out);
                    default -> throw new IllegalArgumentException("unknown --mode: " + mode);
                }
            }
            out.put("ok", true);
            System.out.println(Json.write(out));
            System.exit(0);
        } catch (Exception e) {
            out.put("ok", false);
            out.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println(Json.write(out));
            System.exit(1);
        }
    }

    private static void init(Connection c, String schema, MechanismKind mechanism, Args a,
                             Map<String, Object> out) throws SQLException {
        String appRole = a.get("app-role", "wxeo_app");
        String readRole = a.get("read-role", "wxeo_read");
        String faultRole = a.get("fault-role", "wxeo_fault");
        List<String> statements = SchemaDdl.init(schema, mechanism, appRole, readRole, faultRole);        List<String> warnings = new ArrayList<>();
        int executed = 0;
        for (String sql : statements) {
            try (Statement st = c.createStatement()) {
                st.execute(sql);
                executed++;
            } catch (SQLException e) {
                if (sql.startsWith("GRANT") || sql.startsWith("REVOKE")) {
                    warnings.add("skipped privilege statement (" + e.getMessage() + "): " + sql);
                } else {
                    throw e;
                }
            }
        }
        out.put("statements_executed", executed);
        out.put("warnings", warnings);
        out.put("tables", SchemaDdl.tables(schema, mechanism));
        out.put("dedup_index", mechanism.dedupIndex);
        out.put("fault_role", faultRole);
        out.put("aux", Map.of(
                "inbox", mechanism.inbox || mechanism.outboxRelay,
                "outbox", mechanism.outboxRelay,
                "consumer_progress", mechanism.hasProgressTable()));
    }

    private static void drop(Connection c, String schema, Map<String, Object> out) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(SchemaDdl.drop(schema));
        }
        out.put("dropped", true);
    }

    private static void verify(Connection c, String schema, MechanismKind mechanism,
                               Map<String, Object> out) throws SQLException {
        Map<String, Object> checks = new LinkedHashMap<>();
        checks.put("posting_exists", tableExists(c, schema, "posting"));
        if (mechanism.inbox || mechanism.outboxRelay) {
            checks.put("inbox_exists", tableExists(c, schema, "inbox"));
        }
        if (mechanism.outboxRelay) {
            checks.put("outbox_exists", tableExists(c, schema, "outbox"));
        }
        if (mechanism.hasProgressTable()) {
            checks.put("consumer_progress_exists", tableExists(c, schema, "consumer_progress"));
        }
        if (tableExists(c, schema, "posting")) {
            String table = schema + ".posting";
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT has_table_privilege(current_user, ?, 'UPDATE'),"
                            + " has_table_privilege(current_user, ?, 'DELETE')")) {
                ps.setString(1, table);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    checks.put("can_update", rs.getBoolean(1));
                    checks.put("can_delete", rs.getBoolean(2));
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT count(*) FROM pg_trigger WHERE tgrelid = ?::regclass AND NOT tgisinternal")) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    checks.put("append_only_triggers", rs.getInt(1));
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT count(*) FROM pg_indexes WHERE schemaname = ? AND tablename = 'posting'"
                            + " AND indexname = 'posting_dedup_idx'")) {
                ps.setString(1, schema);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    checks.put("dedup_index_present", rs.getInt(1) > 0);
                }
            }
        }
        checks.put("expected_dedup_index", mechanism.dedupIndex);
        out.put("checks", checks);
    }

    private static boolean tableExists(Connection c, String schema, String table) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1) > 0;
            }
        }
    }

    private SchemaTool() {
    }
}
