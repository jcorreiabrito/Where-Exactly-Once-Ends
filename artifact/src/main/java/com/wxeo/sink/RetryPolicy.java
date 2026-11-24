package com.wxeo.sink;

import java.sql.SQLException;

/**
 * The mechanism-independent retry semantics of docs/method.md §4.10, expressed as a pure
 * classification function so it can be unit-tested without a database.
 *
 * <pre>
 * SQLSTATE 40xxx (serialization/deadlock) -> retry txn immediately, bounded at 5 attempts
 * SQLSTATE 08xxx (connection loss)       -> retry whole txn, bounded at 3 attempts, backoff 100/300/900 ms
 * COMMIT sent, outcome unknown           -> mechanism policy:
 *      NO_WRITE_RETRY     M0, M1x        no write retry; do not commit progress (redelivery)
 *      RETRY_IDEMPOTENT   M2, M3, M2+M3, M1x+M3  retry txn (idempotent effect)
 *      REREAD_PROGRESS    M4, M4+M3      re-read consumer_progress; skip if advanced, else retry
 * </pre>
 */
public final class RetryPolicy {

    public static final int SERIALIZATION_MAX_ATTEMPTS = 5;
    public static final int CONNECTION_MAX_ATTEMPTS = 3;
    public static final long[] BACKOFF_MS = {100, 300, 900};

    public enum UnknownMode {
        NO_WRITE_RETRY,
        RETRY_IDEMPOTENT,
        REREAD_PROGRESS
    }

    public enum Decision {
        /** Retry the whole transaction immediately (serialization failure). */
        RETRY_IMMEDIATE,
        /** Back off, then retry the whole transaction (connection loss before commit was sent). */
        RETRY_WITH_BACKOFF,
        /** Commit acknowledgement lost: apply the mechanism's unknown-outcome policy. */
        UNKNOWN_OUTCOME,
        /** Bounded attempts exhausted or non-retriable error: propagate. */
        FAIL
    }

    private RetryPolicy() {
    }

    /**
     * @param inCommit        true when the exception was raised by the {@code COMMIT} call itself
     *                        (outcome may be unknown when the connection dropped)
     * @param mode            mechanism unknown-outcome policy
     * @param serializationAttempts attempts already used for class 40 retries (before this failure)
     * @param connectionAttempts     attempts already used for class 08 retries (before this failure)
     */
    public static Decision classify(SQLException e, boolean inCommit, UnknownMode mode,
                                    int serializationAttempts, int connectionAttempts) {
        if (isSerializationFailure(e)) {
            return serializationAttempts < SERIALIZATION_MAX_ATTEMPTS
                    ? Decision.RETRY_IMMEDIATE : Decision.FAIL;
        }
        if (isConnectionLoss(e)) {
            if (inCommit) {
                return mode == UnknownMode.NO_WRITE_RETRY ? Decision.UNKNOWN_OUTCOME
                        : connectionAttempts < CONNECTION_MAX_ATTEMPTS ? Decision.UNKNOWN_OUTCOME
                        : Decision.FAIL;
            }
            return connectionAttempts < CONNECTION_MAX_ATTEMPTS
                    ? Decision.RETRY_WITH_BACKOFF : Decision.FAIL;
        }
        return Decision.FAIL;
    }

    public static boolean isSerializationFailure(SQLException e) {
        return SqlRetry.isSerializationFailure(e);
    }

    public static boolean isConnectionLoss(SQLException e) {
        return SqlRetry.isConnectionLoss(e);
    }

    public static long backoffMs(int attemptNumberOneBased) {
        return BACKOFF_MS[Math.min(BACKOFF_MS.length - 1, Math.max(0, attemptNumberOneBased - 1))];
    }

    public static UnknownMode forMechanism(String wireName) {
        return switch (wireName) {
            case "M0", "M1x" -> UnknownMode.NO_WRITE_RETRY;
            case "M4", "M4+M3" -> UnknownMode.REREAD_PROGRESS;
            case "M1-kafka" -> UnknownMode.NO_WRITE_RETRY;
            default -> UnknownMode.RETRY_IDEMPOTENT;
        };
    }
}
