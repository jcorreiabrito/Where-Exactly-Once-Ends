package com.wxeo.sink;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for the frozen retry table of docs/method.md §4.10. */
class RetryPolicyTest {

    private static SQLException sql(String state) {
        return new SQLException("injected", state);
    }

    @Test
    void serializationFailuresRetryImmediatelyUpToFiveAttempts() {
        for (int attempts = 0; attempts < 5; attempts++) {
            assertEquals(RetryPolicy.Decision.RETRY_IMMEDIATE,
                    RetryPolicy.classify(sql("40001"), false, RetryPolicy.UnknownMode.NO_WRITE_RETRY,
                            attempts, 0));
        }
        assertEquals(RetryPolicy.Decision.FAIL,
                RetryPolicy.classify(sql("40P01"), false, RetryPolicy.UnknownMode.NO_WRITE_RETRY, 5, 0));
    }

    @Test
    void connectionLossBeforeCommitRetriesWithBackoffThreeTimes() {
        for (int attempts = 0; attempts < 3; attempts++) {
            assertEquals(RetryPolicy.Decision.RETRY_WITH_BACKOFF,
                    RetryPolicy.classify(sql("08006"), false, RetryPolicy.UnknownMode.RETRY_IDEMPOTENT,
                            attempts, attempts));
        }
        assertEquals(RetryPolicy.Decision.FAIL,
                RetryPolicy.classify(sql("08006"), false, RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, 0, 3));
        assertEquals(100L, RetryPolicy.backoffMs(1));
        assertEquals(300L, RetryPolicy.backoffMs(2));
        assertEquals(900L, RetryPolicy.backoffMs(3));
        assertEquals(900L, RetryPolicy.backoffMs(99));
    }

    @Test
    void unknownCommitOutcomeUsesTheMechanismPolicy() {
        // M0/M1x: documented naive policy, no write retry (redelivery may duplicate).
        assertEquals(RetryPolicy.Decision.UNKNOWN_OUTCOME,
                RetryPolicy.classify(sql("08006"), true, RetryPolicy.UnknownMode.NO_WRITE_RETRY, 0, 0));
        // M2/M3 family: retry is idempotent.
        assertEquals(RetryPolicy.Decision.UNKNOWN_OUTCOME,
                RetryPolicy.classify(sql("08006"), true, RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, 0, 0));
        assertEquals(RetryPolicy.Decision.UNKNOWN_OUTCOME,
                RetryPolicy.classify(sql("08006"), true, RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, 0, 2));
        assertEquals(RetryPolicy.Decision.FAIL,
                RetryPolicy.classify(sql("08006"), true, RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, 0, 3));
        // M4 family: re-read own progress.
        assertEquals(RetryPolicy.Decision.UNKNOWN_OUTCOME,
                RetryPolicy.classify(sql("08006"), true, RetryPolicy.UnknownMode.REREAD_PROGRESS, 0, 1));
    }

    @Test
    void nonRetriableErrorsFail() {
        assertEquals(RetryPolicy.Decision.FAIL,
                RetryPolicy.classify(sql("23505"), true, RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, 0, 0));
        assertEquals(RetryPolicy.Decision.FAIL,
                RetryPolicy.classify(sql("42703"), false, RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, 0, 0));
        assertEquals(RetryPolicy.Decision.FAIL,
                RetryPolicy.classify(new SQLException("plain"), false,
                        RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, 0, 0));
    }

    @Test
    void mechanismPoliciesMatchTheDeclaredRetryTable() {
        assertEquals(RetryPolicy.UnknownMode.NO_WRITE_RETRY, RetryPolicy.forMechanism("M0"));
        assertEquals(RetryPolicy.UnknownMode.NO_WRITE_RETRY, RetryPolicy.forMechanism("M1x"));
        assertEquals(RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, RetryPolicy.forMechanism("M2"));
        assertEquals(RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, RetryPolicy.forMechanism("M3"));
        assertEquals(RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, RetryPolicy.forMechanism("M2+M3"));
        assertEquals(RetryPolicy.UnknownMode.RETRY_IDEMPOTENT, RetryPolicy.forMechanism("M1x+M3"));
        assertEquals(RetryPolicy.UnknownMode.REREAD_PROGRESS, RetryPolicy.forMechanism("M4"));
        assertEquals(RetryPolicy.UnknownMode.REREAD_PROGRESS, RetryPolicy.forMechanism("M4+M3"));
    }

    @Test
    void realDriverStatesAreRecognised() {
        assertEquals(true, RetryPolicy.isSerializationFailure(sql("40P01")));
        assertEquals(true, RetryPolicy.isConnectionLoss(sql("08001")));
        assertEquals(false, RetryPolicy.isSerializationFailure(sql("08006")));
        assertEquals(false, RetryPolicy.isConnectionLoss(sql("23505")));
    }
}
