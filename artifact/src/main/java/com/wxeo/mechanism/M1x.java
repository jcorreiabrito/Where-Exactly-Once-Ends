package com.wxeo.mechanism;

import com.wxeo.failpoint.FailpointNames;
import com.wxeo.failpoint.Failpoints;
import com.wxeo.ledger.Ledger;
import com.wxeo.sink.RetryPolicy;
import com.wxeo.sink.RetryPolicy.UnknownMode;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * M1x — Kafka EOS plus a non-atomic external DB write (the canonical EOS gap, docs/method.md §4.4).
 *
 * <pre>
 * txn.begin
 *   for r in B: txn.send(sink_topic, ...)            (D-publish/stream, inside T)
 *   db.begin; insert_plain(postings); db.commit     (D-effect, OUTSIDE T)
 *   txn.send_offsets_to_transaction(offsets(B))
 * txn.commit_transaction()                           (D-progress ∧ stream records atomic in Kafka)
 * </pre>
 *
 * <p>{@code ordering=early} (D2 probe) commits the Kafka transaction (progress + stream) before the
 * external DB transaction, making W2 (loss) reachable. Otherwise effect precedes progress.</p>
 */
public class M1x extends AbstractMechanism {

    private final boolean idempotent;
    private final UnknownMode unknownMode;

    public M1x(ProcessContext ctx) {
        this(ctx, false);
    }

    protected M1x(ProcessContext ctx, boolean idempotent) {
        super(ctx);
        this.idempotent = idempotent;
        this.unknownMode = RetryPolicy.forMechanism(cfg.mechanism().wireName);
    }

    @Override
    public String id() {
        return "M1x";
    }

    public boolean idempotent() {
        return idempotent;
    }

    @Override
    public void process(Batch batch) throws Exception {
        KafkaProducer<String, String> tx = ctx.providers.transactional();
        tx.beginTransaction();
        try {
            Failpoints.fire(FailpointNames.BEFORE_PUBLISH);
            for (Ledger.ParsedEvent e : batch.events()) {
                publishTransactional(e, tx);
            }
            if (cfg.orderEarly()) {
                tx.sendOffsetsToTransaction(batch.nextOffsets(), batch.groupMetadata());
                Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
                tx.commitTransaction();
                Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
                long commitMs = effectTransaction(batch, idempotent, unknownMode,
                        t -> addPostings(t, batch, idempotent));
                batch.recordEffectCommit(metrics, commitMs);
            } else {
                long commitMs = effectTransaction(batch, idempotent, unknownMode,
                        t -> addPostings(t, batch, idempotent));
                batch.recordEffectCommit(metrics, commitMs);
                tx.sendOffsetsToTransaction(batch.nextOffsets(), batch.groupMetadata());
                Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
                tx.commitTransaction();
                Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
            }
        } catch (org.apache.kafka.common.errors.ProducerFencedException
                 | org.apache.kafka.common.errors.InvalidProducerEpochException e) {
            abortKafkaTransactionQuietly();
            throw new com.wxeo.sink.SqlRetry.FatalMechanismException(
                    "transactional producer fenced; exiting instance", e);
        } catch (Exception e) {
            abortKafkaTransactionQuietly();
            throw e;
        }
    }
}
