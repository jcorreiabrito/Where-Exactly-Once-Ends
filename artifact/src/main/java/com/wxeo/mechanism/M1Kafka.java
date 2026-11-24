package com.wxeo.mechanism;

import com.wxeo.failpoint.FailpointNames;
import com.wxeo.failpoint.Failpoints;
import com.wxeo.ledger.Ledger;
import com.wxeo.sink.SqlRetry.FatalMechanismException;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * M1-kafka — Kafka EOS with a Kafka sink (POSITIVE CONTROL, docs/method.md §4.3).
 *
 * <pre>
 * txn.begin
 *   for r in B: txn.send(sink_topic, key=event_id, value=entry(r))   (D-effect, in Kafka)
 *   txn.send_offsets_to_transaction(offsets(B), group)               (D-progress, in Kafka)
 * txn.commit_transaction()                                           (atomic D-effect ∧ D-progress)
 * </pre>
 *
 * <p>Ordering flag n/a: one atomic Kafka commit, so K3/K4 are structurally unreachable (§5.3).</p>
 */
public final class M1Kafka extends AbstractMechanism {

    public M1Kafka(ProcessContext ctx) {
        super(ctx);
    }

    @Override
    public String id() {
        return "M1-kafka";
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
            tx.sendOffsetsToTransaction(batch.nextOffsets(), batch.groupMetadata());
            // The atomic Kafka commit is simultaneously the effect and the progress commit: both
            // crash-point hooks fire at the same location so S4 can realise K2/K5/K6 schedules on
            // M1-kafka (throw/abort_txn/crash/pause before the atomic commit).
            Failpoints.fire(FailpointNames.BEFORE_EFFECT_COMMIT);
            Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
            tx.commitTransaction();
            Failpoints.fire(FailpointNames.AFTER_EFFECT_COMMIT);
            Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
            batch.recordEffectCommit(metrics, System.currentTimeMillis());
        } catch (org.apache.kafka.common.errors.ProducerFencedException
                 | org.apache.kafka.common.errors.InvalidProducerEpochException e) {
            abortKafkaTransactionQuietly();
            throw new FatalMechanismException("transactional producer fenced; exiting instance", e);
        } catch (org.apache.kafka.common.errors.RetriableException e) {
            // A transaction commit whose outcome is unknown must never be retried blindly (§4.10):
            // abort and exit the instance; the coordinator resolves the outcome.
            abortKafkaTransactionQuietly();
            throw new FatalMechanismException("transaction commit outcome unknown; exiting instance", e);
        } catch (RuntimeException e) {
            abortKafkaTransactionQuietly();
            throw e;
        }
    }
}
