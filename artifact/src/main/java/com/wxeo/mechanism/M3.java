package com.wxeo.mechanism;

import com.wxeo.failpoint.FailpointNames;
import com.wxeo.failpoint.Failpoints;
import com.wxeo.sink.RetryPolicy.UnknownMode;

/**
 * M3 — idempotency key with a unique index (docs/method.md §4.6).
 *
 * <pre>
 * strict: db.begin; INSERT ... ON CONFLICT (event_id,leg_no) DO NOTHING; db.commit
 *         publish_non_txn(B); kafka.commit_offsets(B)
 * early : offsets committed before the DB effect (D2 probe: dedup cannot restore a never-written row)
 * </pre>
 */
public final class M3 extends AbstractMechanism {

    public M3(ProcessContext ctx) {
        super(ctx);
    }

    @Override
    public String id() {
        return "M3";
    }

    @Override
    public void process(Batch batch) throws Exception {
        if (cfg.orderEarly()) {
            Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
            commitOffsets(batch);
            Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
            long commitMs = effectTransaction(batch, true, UnknownMode.RETRY_IDEMPOTENT,
                    tx -> addPostings(tx, batch, true));
            batch.recordEffectCommit(metrics, commitMs);
            publishInline(batch);
        } else {
            long commitMs = effectTransaction(batch, true, UnknownMode.RETRY_IDEMPOTENT,
                    tx -> addPostings(tx, batch, true));
            batch.recordEffectCommit(metrics, commitMs);
            publishInline(batch);
            Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
            commitOffsets(batch);
            Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
        }
    }
}
