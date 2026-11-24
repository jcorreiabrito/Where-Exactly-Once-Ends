package com.wxeo.mechanism;

import com.wxeo.config.EffectiveConfig;
import com.wxeo.failpoint.FailpointNames;
import com.wxeo.failpoint.Failpoints;
import com.wxeo.ledger.Ledger;
import com.wxeo.metrics.RunMetrics;
import com.wxeo.publish.Publication;
import com.wxeo.sink.PostgresSink;
import com.wxeo.sink.RetryPolicy;
import com.wxeo.sink.RetryPolicy.UnknownMode;
import com.wxeo.sink.SqlRetry.FatalMechanismException;
import com.wxeo.sink.SqlRetry.UnknownOutcomeException;
import com.wxeo.util.IoUtil;
import org.apache.kafka.common.TopicPartition;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Shared protocol scaffolding for the mechanism implementations: the §4.10 retry table, the
 * offset-commit helper, the effect-transaction helper with the failpoint hooks, and inline
 * publication. Each concrete mechanism defines only its duty ordering and transaction contents.
 */
public abstract class AbstractMechanism implements Mechanism {

    protected final ProcessContext ctx;
    protected final EffectiveConfig cfg;
    protected final RunMetrics metrics;
    protected final PostgresSink sink;
    protected final Publication publication;

    protected AbstractMechanism(ProcessContext ctx) {
        this.ctx = ctx;
        this.cfg = ctx.cfg;
        this.metrics = ctx.metrics;
        this.sink = ctx.sink;
        this.publication = ctx.publication;
    }

    /** The three duties a mechanism may execute (docs/method.md §4.1). */
    protected enum Duty {
        EFFECT, PROGRESS, PUBLISH
    }

    @FunctionalInterface
    protected interface TxWork {
        void apply(PostgresSink.Tx tx) throws Exception;
    }

    @FunctionalInterface
    protected interface UnknownResolver {
        /** Returns the commit-observation instant if the transaction is known to have committed, else null. */
        Long resolve(Batch batch) throws Exception;
    }

    // ---- effect transaction ------------------------------------------------------------------

    protected long effectTransaction(Batch batch, boolean idempotent, UnknownMode mode,
                                     TxWork work) throws Exception {
        return effectTransaction(batch, idempotent, mode, work, null);
    }

    /**
     * Runs one effect transaction with the mechanism-independent retry policy:
     * SQLSTATE 40xxx → immediate retry (max 5); SQLSTATE 08xxx before COMMIT → retry with
     * 100/300/900 ms backoff (max 3); COMMIT acknowledgement lost → policy dependent (no write
     * retry for M0/M1x, idempotent retry for M2/M3 family, re-read own progress for M4 family).
     */
    protected long effectTransaction(Batch batch, boolean idempotent, UnknownMode mode,
                                     TxWork work, UnknownResolver resolver) throws Exception {
        int serializationAttempts = 0;
        int connectionAttempts = 0;
        while (true) {
            PostgresSink.Tx tx = null;
            boolean inCommit = false;
            try {
                tx = ctx.requireSink().begin();
                work.apply(tx);
                tx.flush();
                Failpoints.fire(FailpointNames.BEFORE_EFFECT_COMMIT);
                inCommit = true;
                long commitObservedMs = tx.commit();
                inCommit = false;
                metrics.inc("postings_inserted", tx.insertedRows());
                metrics.inc("duplicate_detected", tx.conflictRows());
                Failpoints.fire(FailpointNames.AFTER_EFFECT_COMMIT);
                return commitObservedMs;
            } catch (SQLException e) {
                RetryPolicy.Decision decision = RetryPolicy.classify(
                        e, inCommit, mode, serializationAttempts, connectionAttempts);
                if (tx != null) {
                    tx.rollback();
                }
                switch (decision) {
                    case RETRY_IMMEDIATE -> {
                        serializationAttempts++;
                        metrics.inc("db_retries");
                        continue;
                    }
                    case RETRY_WITH_BACKOFF -> {
                        connectionAttempts++;
                        metrics.inc("db_retries");
                        backoff(connectionAttempts);
                        continue;
                    }
                    case UNKNOWN_OUTCOME -> {
                        metrics.inc("unknown_outcome_events");
                        Long resolved = resolveUnknownOutcome(batch, mode, resolver);
                        if (resolved != null) {
                            metrics.inc("postings_inserted", tx == null ? 0 : tx.insertedRows());
                            metrics.inc("duplicate_detected", tx == null ? 0 : tx.conflictRows());
                            return resolved;
                        }
                        if (mode == UnknownMode.NO_WRITE_RETRY) {
                            metrics.inc("db_retry_exhausted");
                            throw new UnknownOutcomeException("effect COMMIT outcome unknown; documented policy"
                                    + " for " + cfg.mechanism().wireName
                                    + ": no write retry, do not commit progress (batch will be redelivered)", e);
                        }
                        connectionAttempts++;
                        if (connectionAttempts > RetryPolicy.CONNECTION_MAX_ATTEMPTS) {
                            metrics.inc("db_retry_exhausted");
                            throw new UnknownOutcomeException("effect COMMIT outcome unknown and unresolved", e);
                        }
                        metrics.inc("db_retries");
                        backoff(connectionAttempts);
                        continue;
                    }
                    case FAIL -> {
                        metrics.inc("db_retry_exhausted");
                        throw e;
                    }
                }
                throw e; // unreachable
            } catch (RuntimeException e) {
                if (tx != null) {
                    tx.rollback();
                }
                throw e;
            } finally {
                if (tx != null) {
                    tx.close();
                }
            }
        }
    }

    private Long resolveUnknownOutcome(Batch batch, UnknownMode mode, UnknownResolver resolver)
            throws Exception {
        if (mode != UnknownMode.REREAD_PROGRESS || resolver == null) {
            return null;
        }
        return resolver.resolve(batch);
    }

    protected void backoff(int attempt) {
        IoUtil.sleepQuietly(RetryPolicy.backoffMs(attempt));
    }

    // ---- progress -----------------------------------------------------------------------------

    /** Commits the batch's offsets with bounded retries; exhaustion stops the instance (§4.10). */
    protected void commitOffsets(Batch batch) {
        int attempts = 0;
        while (true) {
            try {
                ctx.consumer.commitSync(batch.nextOffsets());
                metrics.inc("progress_commits");
                return;
            } catch (RuntimeException e) {
                attempts++;
                metrics.inc("progress_commit_failures");
                if (attempts >= RetryPolicy.CONNECTION_MAX_ATTEMPTS) {
                    throw new FatalMechanismException("Kafka offset commit failed after " + attempts
                            + " attempts; stopping instance (progress stays behind)", e);
                }
                metrics.inc("kafka_progress_commit_retries");
                backoff(attempts);
            }
        }
    }

    /** Best-effort Kafka offset commit for observability only (M4 family); never used for resume. */
    protected void commitOffsetsBestEffort(Batch batch) {
        try {
            ctx.consumer.commitSync(batch.nextOffsets());
            metrics.inc("progress_commits");
            metrics.inc("commits_observed_lagging_zero");
        } catch (RuntimeException e) {
            metrics.inc("progress_commit_failures");
            metrics.inc("kafka_progress_commit_retries");
        }
    }

    // ---- publication ---------------------------------------------------------------------------

    protected void publishInline(Batch batch) {
        publication.publishInline(batch.events());
    }

    protected void publishTransactional(Ledger.ParsedEvent event,
                                        org.apache.kafka.clients.producer.KafkaProducer<String, String> txProducer) {
        txProducer.send(publication.recordFor(event));
        metrics.inc("publication_sent");
    }

    // ---- convenience ---------------------------------------------------------------------------

    protected List<Ledger.ParsedEvent> events(Batch batch) {
        return batch.events();
    }

    protected void addPostings(PostgresSink.Tx tx, Batch batch, boolean idempotent) throws Exception {
        for (Ledger.ParsedEvent e : batch.events()) {
            if (idempotent) {
                tx.addIdemEvent(e.event());
            } else {
                tx.addPlainEvent(e.event());
            }
        }
    }

    protected Map<TopicPartition, Long> endOffsets(Batch batch) {
        return batch.endOffsets();
    }

    /** Aborts the transactional producer if a transaction is open; called by the fault paths. */
    protected void abortKafkaTransactionQuietly() {
        var tx = ctx.providers.currentTransactional();
        if (tx != null) {
            try {
                tx.abortTransaction();
                metrics.inc("txn_aborts");
            } catch (RuntimeException ignored) {
                // producer may be fenced/broken; shutdown will close it
            }
        }
    }
}
