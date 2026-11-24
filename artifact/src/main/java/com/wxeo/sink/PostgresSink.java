package com.wxeo.sink;

import com.wxeo.config.EffectiveConfig;
import com.wxeo.ledger.Ledger;
import com.wxeo.ledger.Posting;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL ledger sink for every DB mechanism (docs/method.md §1.4.2, §4).
 *
 * <p>Transactions are explicit ({@code autocommit=false}); one poll batch's effect is one database
 * transaction (C12 unreachable by construction). Insert variants are mechanism-intrinsic:
 * {@link Tx#addPlainEvent} for M0/M1x/M2/M4 (plain INSERT) and {@link Tx#addIdemEvent} for
 * M3/M2+M3/M4+M3/M1x+M3 ({@code ON CONFLICT (event_id, leg_no) DO NOTHING} against the declared
 * unique index).</p>
 */
public final class PostgresSink implements AutoCloseable {

    private final HikariDataSource dataSource;
    private final EffectiveConfig cfg;

    public PostgresSink(EffectiveConfig cfg) {
        this.cfg = cfg;
        HikariConfig hc = new HikariConfig();
        hc.setPoolName("wxeo-" + cfg.instanceId());
        hc.setJdbcUrl(cfg.dbUrl());
        hc.setUsername(cfg.dbUser());
        hc.setPassword("");
        hc.setMaximumPoolSize(8);
        hc.setMinimumIdle(2);
        hc.setAutoCommit(false);
        hc.setConnectionTimeout(30_000);
        hc.setValidationTimeout(5_000);
        hc.setInitializationFailTimeout(30_000);
        hc.addDataSourceProperty("reWriteBatchedInserts", "true");
        hc.addDataSourceProperty("prepareThreshold", "5");
        hc.addDataSourceProperty("tcpKeepAlive", "true");
        this.dataSource = new HikariDataSource(hc);
    }

    public String schema() {
        return cfg.dbSchema();
    }

    public EffectiveConfig config() {
        return cfg;
    }

    /** Opens a new explicit transaction bounded by the returned {@link Tx}. */
    public Tx begin() throws SQLException {
        Connection c = dataSource.getConnection();
        c.setAutoCommit(false);
        return new Tx(c);
    }

    /** Reads the mechanism's own resume point for a partition; {@code -1} when absent (M4 family). */
    public long readProgress(int partition) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(SchemaDdl.readProgress(schema()))) {
            ps.setInt(1, partition);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1L;
            }
        }
    }

    public record OutboxRow(long outboxId, String eventId, int sourcePartition, String payload) {
    }

    /** Relay read path (M2, M2+M3): unpublished outbox rows in outbox_id order. */
    public List<OutboxRow> fetchUnpublished(int limit) throws SQLException {
        List<OutboxRow> rows = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(SchemaDdl.selectUnpublishedOutbox(schema()))) {
            c.setReadOnly(true);
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new OutboxRow(rs.getLong(1), rs.getString(2), rs.getInt(3), rs.getString(4)));
                }
            }
        }
        return rows;
    }

    /** Relay mark: only the first marker wins (C8/K7 safe under relay crash). */
    public boolean markPublished(long outboxId) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(SchemaDdl.markOutboxPublished(schema()))) {
            c.setAutoCommit(false);
            ps.setLong(1, outboxId);
            int n = ps.executeUpdate();
            c.commit();
            return n > 0;
        }
    }

    public record Attestation(boolean canUpdate, boolean canDelete, int triggerCount) {
        public boolean appendOnly() {
            return !canUpdate && !canDelete && triggerCount >= 1;
        }
    }

    /** P8 append-only attestation for the current (application) role. */
    public Attestation attestation() throws SQLException {
        String table = schema() + ".posting";
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT has_table_privilege(current_user, ?, 'UPDATE'),"
                             + " has_table_privilege(current_user, ?, 'DELETE')")) {
            ps.setString(1, table);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                boolean upd = rs.getBoolean(1);
                boolean del = rs.getBoolean(2);
                int triggers;
                try (PreparedStatement t = c.prepareStatement(
                        "SELECT count(*) FROM pg_trigger WHERE tgrelid = ?::regclass AND NOT tgisinternal")) {
                    t.setString(1, table);
                    try (ResultSet trs = t.executeQuery()) {
                        trs.next();
                        triggers = trs.getInt(1);
                    }
                }
                return new Attestation(upd, del, triggers);
            }
        }
    }

    @Override
    public void close() {
        dataSource.close();
    }

    // ---------------------------------------------------------------------------------------------

    /** One explicit database transaction; all effect duties of a poll batch share it. */
    public final class Tx implements AutoCloseable {

        private final Connection c;
        private PreparedStatement plainStmt;
        private PreparedStatement idemStmt;
        private PreparedStatement inboxStmt;
        private PreparedStatement outboxPlainStmt;
        private PreparedStatement outboxIdemStmt;
        private PreparedStatement progressStmt;
        private final int jdbcBatchSize;
        private int pendingPlain;
        private int pendingIdem;
        private int insertedRows;
        private int conflictRows;
        private boolean committed;
        private boolean rolledBack;

        private Tx(Connection c) {
            this.c = c;
            this.jdbcBatchSize = 500;
        }

        public int insertedRows() {
            return insertedRows;
        }

        public int conflictRows() {
            return conflictRows;
        }

        /** Adds the two postings of one event with a plain INSERT. */
        public void addPlainEvent(Ledger.EventData ev) throws SQLException {
            preparedPlain();
            long millis = ev.scheduledAtMs();
            for (Posting p : ev.postings()) {
                bindPosting(plainStmt, p, millis);
                plainStmt.addBatch();
                pendingPlain++;
                if (pendingPlain >= jdbcBatchSize) {
                    executePlain();
                }
            }
        }

        /** Adds the two postings of one event with {@code ON CONFLICT DO NOTHING}. */
        public void addIdemEvent(Ledger.EventData ev) throws SQLException {
            preparedIdem();
            long millis = ev.scheduledAtMs();
            for (Posting p : ev.postings()) {
                bindPosting(idemStmt, p, millis);
                idemStmt.addBatch();
                pendingIdem++;
                if (pendingIdem >= jdbcBatchSize) {
                    executeIdem();
                }
            }
        }

        /** M2 inbox claim: true iff this transaction created the claim. */
        public boolean claimInbox(String eventId) throws SQLException {
            if (inboxStmt == null) {
                inboxStmt = c.prepareStatement(SchemaDdl.claimInbox(schema()));
            }
            inboxStmt.setString(1, eventId);
            return inboxStmt.executeUpdate() == 1;
        }

        /**
         * M2 outbox insert (plain UNIQUE event_id) or M2+M3 outbox insert
         * ({@code ON CONFLICT DO NOTHING}). Returns true when a row was inserted.
         * {@code sourcePartition} preserves the "publish to the same partition index as the source
         * record" requirement (method.md §1.4.3) across the relay boundary.
         */
        public boolean insertOutbox(String eventId, int sourcePartition, String payloadJson,
                                    boolean idempotent) throws SQLException {
            PreparedStatement ps;
            if (idempotent) {
                if (outboxIdemStmt == null) {
                    outboxIdemStmt = c.prepareStatement(SchemaDdl.insertOutboxIdem(schema()));
                }
                ps = outboxIdemStmt;
            } else {
                if (outboxPlainStmt == null) {
                    outboxPlainStmt = c.prepareStatement(SchemaDdl.insertOutboxPlain(schema()));
                }
                ps = outboxPlainStmt;
            }
            ps.setString(1, eventId);
            ps.setInt(2, sourcePartition);
            ps.setString(3, payloadJson);
            return ps.executeUpdate() == 1;
        }

        /** M4 monotone progress upsert (same transaction as the postings). */
        public void upsertProgress(int partition, long nextOffset, long ownerEpoch) throws SQLException {
            if (progressStmt == null) {
                progressStmt = c.prepareStatement(SchemaDdl.upsertProgress(schema()));
            }
            progressStmt.setInt(1, partition);
            progressStmt.setLong(2, nextOffset);
            progressStmt.setLong(3, ownerEpoch);
            progressStmt.executeUpdate();
        }

        /** Executes any pending posting batch statements (called before the effect commit hook). */
        public void flush() throws SQLException {
            executePlain();
            executeIdem();
        }

        /** Commits and returns the wall-clock instant at which the commit acknowledgement was observed. */
        public long commit() throws SQLException {
            flush();
            c.commit();
            committed = true;
            return System.currentTimeMillis();
        }

        public void rollback() {
            if (rolledBack || committed) {
                return;
            }
            rolledBack = true;
            try {
                c.rollback();
            } catch (SQLException ignored) {
                // connection may be broken; the pool will discard it
            }
        }

        private void preparedPlain() throws SQLException {
            if (plainStmt == null) {
                plainStmt = c.prepareStatement(SchemaDdl.insertPlain(schema()));
            }
        }

        private void preparedIdem() throws SQLException {
            if (idemStmt == null) {
                idemStmt = c.prepareStatement(SchemaDdl.insertIdem(schema()));
            }
        }

        private void executePlain() throws SQLException {
            if (plainStmt == null || pendingPlain == 0) {
                return;
            }
            pendingPlain = 0;
            for (int n : plainStmt.executeBatch()) {
                if (n > 0 || n == PreparedStatement.SUCCESS_NO_INFO) {
                    insertedRows++;
                }
            }
        }

        private void executeIdem() throws SQLException {
            if (idemStmt == null || pendingIdem == 0) {
                return;
            }
            pendingIdem = 0;
            for (int n : idemStmt.executeBatch()) {
                if (n > 0) {
                    insertedRows++;
                } else if (n == 0) {
                    conflictRows++;
                }
            }
        }

        private void bindPosting(PreparedStatement ps, Posting p, long scheduledAtMs) throws SQLException {
            ps.setString(1, p.eventId());
            ps.setInt(2, p.legNo());
            ps.setString(3, p.accountId());
            ps.setString(4, p.side());
            ps.setLong(5, p.amountMinor());
            ps.setString(6, p.currency());
            ps.setTimestamp(7, new Timestamp(scheduledAtMs));
        }

        @Override
        public void close() {
            try {
                rollback();
            } finally {
                closeQuietly(plainStmt);
                closeQuietly(idemStmt);
                closeQuietly(inboxStmt);
                closeQuietly(outboxPlainStmt);
                closeQuietly(outboxIdemStmt);
                closeQuietly(progressStmt);
                try {
                    c.setAutoCommit(true);
                    c.close();
                } catch (SQLException ignored) {
                    // pool will discard broken connections
                }
            }
        }

        private void closeQuietly(PreparedStatement ps) {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ignored) {
                    // ignore
                }
            }
        }
    }

    /** Map of kafka partition -> end offset (exclusive), as recorded for the M4 progress guard. */
    public static Map<Integer, Long> endOffsetsAsMap(Map<org.apache.kafka.common.TopicPartition, Long> endOffsets) {
        Map<Integer, Long> out = new java.util.TreeMap<>();
        endOffsets.forEach((tp, off) -> out.put(tp.partition(), off));
        return out;
    }
}
