package com.wxeo.sink;

import java.sql.SQLException;

/**
 * PostgreSQL error classification for the mechanism-independent retry semantics of
 * docs/method.md §4.10.
 */
public final class SqlRetry {

    /** Outcome of a commit whose acknowledgement was lost — handled per mechanism policy. */
    public static final class UnknownOutcomeException extends RuntimeException {
        public UnknownOutcomeException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** A mechanism cannot make progress (e.g. offset-commit retries exhausted) → stop instance. */
    public static final class FatalMechanismException extends RuntimeException {
        public FatalMechanismException(String message, Throwable cause) {
            super(message, cause);
        }

        public FatalMechanismException(String message) {
            super(message);
        }
    }

    private SqlRetry() {
    }

    /** SQLSTATE class 40 — serialization failure / deadlock. */
    public static boolean isSerializationFailure(SQLException e) {
        String s = state(e);
        return s != null && s.startsWith("40");
    }

    /** SQLSTATE class 08 — connection exception (link loss / reset / timeout). */
    public static boolean isConnectionLoss(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException s) {
                String st = s.getSQLState();
                if (st != null && st.startsWith("08")) {
                    return true;
                }
                if (s.getErrorCode() == 0 && s.getMessage() != null
                        && (s.getMessage().contains("I/O error")
                        || s.getMessage().contains("Connection reset")
                        || s.getMessage().contains("Socket closed")
                        || s.getMessage().contains("connection attempt failed")
                        || s.getMessage().contains("Broken pipe"))) {
                    return true;
                }
            }
        }
        return false;
    }

    /** SQLSTATE 23505 — unique violation (only relevant for plain inserts / plain outbox inserts). */
    public static boolean isUniqueViolation(SQLException e) {
        String s = state(e);
        return "23505".equals(s);
    }

    public static String state(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException s && s.getSQLState() != null) {
                return s.getSQLState();
            }
        }
        return null;
    }

    public static String describe(Throwable e) {
        if (e instanceof SQLException s) {
            return "SQLState=" + s.getSQLState() + " code=" + s.getErrorCode() + " msg=" + s.getMessage();
        }
        return e.getClass().getSimpleName() + ": " + e.getMessage();
    }
}
