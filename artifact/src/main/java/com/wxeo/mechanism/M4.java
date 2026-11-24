package com.wxeo.mechanism;

import com.wxeo.failpoint.FailpointNames;
import com.wxeo.failpoint.Failpoints;
import com.wxeo.ledger.Ledger;
import com.wxeo.sink.PostgresSink;
import com.wxeo.sink.RetryPolicy;
import com.wxeo.sink.RetryPolicy.UnknownMode;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * M4 — offsets stored in the same PostgreSQL transaction as the postings (docs/method.md §4.7).
 *
 * <pre>
 * on assignment(p): stored = SELECT next_offset FROM consumer_progress WHERE partition = p
 *                   seek(p, coalesce(stored, committed_offset(p)))
 * strict process(B):
 *   db.begin
 *     insert_plain(postings)
 *     upsert consumer_progress(next_offset = end_offset(B,p))   (monotone guard)
 *   db.commit                                                   (D-effect ∧ D-progress atomic)
 *   publish_non_txn(B); kafka.commit_offsets(B)  (observability only, never used for resume)
 * early process(B): progress committed in its own transaction BEFORE the effect transaction
 *                   (degraded-variant probe for K4; W2 becomes reachable)
 * </pre>
 */
public class M4 extends AbstractMechanism {

    protected final boolean idempotent;

    public M4(ProcessContext ctx) {
        this(ctx, false);
    }

    protected M4(ProcessContext ctx, boolean idempotent) {
        super(ctx);
        this.idempotent = idempotent;
    }

    @Override
    public String id() {
        return "M4";
    }

    @Override
    public void onAssignment(Collection<TopicPartition> assigned) throws Exception {
        for (TopicPartition tp : assigned) {
            long stored = sink.readProgress(tp.partition());
            if (stored >= 0) {
                ctx.consumer.seek(tp, stored);
                metrics.inc("progress_seeks");
                System.out.println("[wxeo][M4] partition " + tp.partition() + " resume from stored progress "
                        + stored);
            }
        }
    }

    @Override
    public void process(Batch batch) throws Exception {
        if (cfg.orderEarly()) {
            commitProgressOnly(batch);
            long commitMs = effectTransaction(batch, idempotent, UnknownMode.NO_WRITE_RETRY,
                    tx -> addPostings(tx, batch, idempotent));
            batch.recordEffectCommit(metrics, commitMs);
        } else {
            long commitMs = effectTransaction(batch, idempotent, UnknownMode.REREAD_PROGRESS, tx -> {
                addPostings(tx, batch, idempotent);
                upsertProgress(tx, batch);
                // Effect and progress are one atomic commit; this hook is placed at the same
                // crash point as before_effect_commit (§4.7, §5.3 footnote 2).
                Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
            }, this::progressAheadOfBatch);
            batch.recordEffectCommit(metrics, commitMs);
            Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
        }
        publishInline(batch);
        commitOffsetsBestEffort(batch);
    }

    /** Reads consumer_progress; returns the commit instant when every batch partition already advanced. */
    private Long progressAheadOfBatch(Batch batch) throws Exception {
        for (Map.Entry<TopicPartition, Long> e : batch.endOffsets().entrySet()) {
            long stored = sink.readProgress(e.getKey().partition());
            if (stored < e.getValue()) {
                return null;
            }
        }
        return System.currentTimeMillis();
    }

    private void upsertProgress(PostgresSink.Tx tx, Batch batch) throws Exception {
        int n = 0;
        for (Map.Entry<TopicPartition, Long> e : batch.endOffsets().entrySet()) {
            tx.upsertProgress(e.getKey().partition(), e.getValue(), cfg.instanceId());
            n++;
        }
        metrics.inc("progress_row_updates", n);
    }

    /** Early-ordering probe: make progress durable in its own transaction before the effect. */
    private void commitProgressOnly(Batch batch) throws Exception {
        int attempts = 0;
        while (true) {
            PostgresSink.Tx tx = null;
            try {
                tx = sink.begin();
                upsertProgress(tx, batch);
                tx.flush();
                Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
                tx.commit();
                Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
                return;
            } catch (java.sql.SQLException e) {
                if (tx != null) {
                    tx.rollback();
                }
                boolean retriable = RetryPolicy.isSerializationFailure(e) || RetryPolicy.isConnectionLoss(e);
                attempts++;
                if (retriable && attempts <= RetryPolicy.CONNECTION_MAX_ATTEMPTS) {
                    metrics.inc("db_retries");
                    backoff(attempts);
                    continue;
                }
                throw e;
            } finally {
                if (tx != null) {
                    tx.close();
                }
            }
        }
    }

    public boolean idempotent() {
        return idempotent;
    }

    /** Unused helper retained for symmetry with M2's ledger write path. */
    protected void addPostingsFor(Batch batch, PostgresSink.Tx tx) throws Exception {
        for (Ledger.ParsedEvent e : batch.events()) {
            if (idempotent) {
                tx.addIdemEvent(e.event());
            } else {
                tx.addPlainEvent(e.event());
            }
        }
    }
}
