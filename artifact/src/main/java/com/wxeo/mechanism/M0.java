package com.wxeo.mechanism;

import com.wxeo.failpoint.FailpointNames;
import com.wxeo.failpoint.Failpoints;
import com.wxeo.sink.RetryPolicy.UnknownMode;

/**
 * M0 — naive at-least-once, plain INSERT (NEGATIVE CONTROL, docs/method.md §4.2).
 *
 * <pre>
 * strict: db.begin; insert_plain(postings); db.commit   (D-effect durable)
 *         publish_non_txn(B)                             (D-publish, at-least-once)
 *         kafka.commit_offsets(B)                        (D-progress durable)
 * early : progress ≺ effect (D2 probe; W2 reachable)
 * </pre>
 */
public final class M0 extends AbstractMechanism {

    public M0(ProcessContext ctx) {
        super(ctx);
    }

    @Override
    public String id() {
        return "M0";
    }

    @Override
    public void process(Batch batch) throws Exception {
        if (cfg.orderEarly()) {
            Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
            commitOffsets(batch);
            Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
            long commitMs = effectTransaction(batch, false, UnknownMode.NO_WRITE_RETRY,
                    tx -> addPostings(tx, batch, false));
            batch.recordEffectCommit(metrics, commitMs);
            publishInline(batch);
        } else {
            long commitMs = effectTransaction(batch, false, UnknownMode.NO_WRITE_RETRY,
                    tx -> addPostings(tx, batch, false));
            batch.recordEffectCommit(metrics, commitMs);
            publishInline(batch);
            Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
            commitOffsets(batch);
            Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
        }
    }
}
