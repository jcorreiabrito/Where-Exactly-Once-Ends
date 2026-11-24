package com.wxeo.it;

import com.wxeo.config.EffectiveConfig.MechanismKind;
import com.wxeo.sink.SchemaDdl;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test against the native PostgreSQL stack (artifact/harness/native-stack.ps1 start-pg).
 * Excluded from the default {@code mvn test}; run with {@code mvn verify -Pintegration}.
 *
 * <p>Skipped (assumption) when PostgreSQL on 127.0.0.1:5433 is not reachable.</p>
 */
@Tag("integration")
class PostgresSchemaIT {

    static final String URL = System.getProperty("wxeo.it.db.url", "jdbc:postgresql://127.0.0.1:5433/postgres");
    static final String USER = System.getProperty("wxeo.it.db.user", "postgres");

    private static Connection connect() {
        try {
            return DriverManager.getConnection(URL, USER, "");
        } catch (SQLException e) {
            assumeTrue(false, "native PostgreSQL not reachable at " + URL + ": " + e.getMessage());
            return null;
        }
    }

    private static void execute(Connection c, List<String> statements) throws SQLException {
        for (String sql : statements) {
            try (Statement st = c.createStatement()) {
                st.execute(sql);
            } catch (SQLException e) {
                // roles may not exist on a minimal local stack; privilege statements are optional here
                if (sql.startsWith("GRANT") || sql.startsWith("REVOKE")) {
                    continue;
                }
                throw e;
            }
        }
    }

    @Test
    void dedupMechanismGetsUniqueIndexAndAppendOnlyTriggerAndIdempotentInsert() throws SQLException {
        String schema = "wxeo_it_m3_" + System.nanoTime();
        try (Connection c = connect()) {
            execute(c, SchemaDdl.init(schema, MechanismKind.M3, null, null));
            assertEquals(1, count(c, "SELECT count(*) FROM pg_indexes WHERE schemaname='" + schema
                    + "' AND indexname='posting_dedup_idx'"));
            assertEquals(1, count(c, "SELECT count(*) FROM pg_trigger WHERE tgrelid='" + schema
                    + ".posting'::regclass AND NOT tgisinternal"));

            // plain INSERT twice -> 2 rows; idem INSERT twice -> still 2 rows (second is a no-op)
            execute(c, List.of("INSERT INTO " + schema + ".posting"
                    + " (event_id,leg_no,account_id,side,amount_minor,currency,produced_at)"
                    + " VALUES ('e1',1,'acct:a','D',100,'BRL',now())"));
            execute(c, List.of("INSERT INTO " + schema + ".posting"
                    + " (event_id,leg_no,account_id,side,amount_minor,currency,produced_at)"
                    + " VALUES ('e1',2,'acct:b','C',100,'BRL',now())"));
            for (int i = 0; i < 2; i++) {
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO " + schema + ".posting"
                                + " (event_id,leg_no,account_id,side,amount_minor,currency,produced_at)"
                                + " VALUES ('e1',1,'acct:a','D',100,'BRL',now())"
                                + " ON CONFLICT (event_id,leg_no) DO NOTHING")) {
                    ps.executeUpdate();
                }
            }
            assertEquals(2, count(c, "SELECT count(*) FROM " + schema + ".posting"));

            // append-only trigger blocks mutation
            boolean blocked = false;
            try (Statement st = c.createStatement()) {
                st.execute("UPDATE " + schema + ".posting SET amount_minor = 999 WHERE event_id = 'e1'");
            } catch (SQLException e) {
                blocked = true;
            }
            assertTrue(blocked, "append-only trigger must reject UPDATE");
        } finally {
            try (Connection c = DriverManager.getConnection(URL, USER, "")) {
                execute(c, List.of(SchemaDdl.drop(schema)));
            } catch (SQLException ignored) {
                // best effort
            }
        }
    }

    @Test
    void nonDedupMechanismHasNoUniquePostingIndex() throws SQLException {
        String schema = "wxeo_it_m0_" + System.nanoTime();
        try (Connection c = connect()) {
            execute(c, SchemaDdl.init(schema, MechanismKind.M0, null, null));
            assertEquals(0, count(c, "SELECT count(*) FROM pg_indexes WHERE schemaname='" + schema
                    + "' AND indexname='posting_dedup_idx'"));
        } finally {
            try (Connection c = DriverManager.getConnection(URL, USER, "")) {
                execute(c, List.of(SchemaDdl.drop(schema)));
            } catch (SQLException ignored) {
                // best effort
            }
        }
    }

    private static long count(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
