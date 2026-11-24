package com.wxeo.mechanism;

import com.wxeo.config.EffectiveConfig.MechanismKind;
import com.wxeo.failpoint.FailpointNames;
import com.wxeo.failpoint.Failpoints;
import com.wxeo.ledger.Ledger;
import com.wxeo.sink.PostgresSink;
import com.wxeo.sink.RetryPolicy.UnknownMode;
import com.wxeo.util.IoUtil;

import java.util.List;

/**
 * M2 — transactional outbox/inbox (docs/method.md §4.5, D3 realisation (a)).
 *
 * <pre>
 * db.begin
 *   for r in B: claim inbox(event_id) ON CONFLICT DO NOTHING
 *               if claimed: insert_plain(postings); INSERT outbox(event_id, payload)
 * db.commit                                  (D-effect ∧ dedup claim ∧ publish intent atomic)
 * publish deferred to the relay thread       (D-publish, at-least-once)
 * kafka.commit_offsets(B)                    (D-progress, after effect under strict ordering)
 * </pre>
 *
 * <p>The relay thread reads unpublished outbox rows and publishes them, marking {@code published_at}
 * after the broker acknowledgement (crash between send and mark duplicates the stream, never the
 * ledger — trace X7).</p>
 */
public class M2 extends AbstractMechanism {

    private final boolean inboxClaim;
    private final boolean idempotentPostings;
    private volatile boolean relayRunning;
    private Thread relayThread;
    private volatile String relayFailure;

    public M2(ProcessContext ctx) {
        this(ctx, true, false);
    }

    protected M2(ProcessContext ctx, boolean inboxClaim, boolean idempotentPostings) {
        super(ctx);
        this.inboxClaim = inboxClaim;
        this.idempotentPostings = idempotentPostings;
    }

    @Override
    public String id() {
        return "M2";
    }

    @Override
    public void init() {
        if (!cfg.mechanism().relayThread) {
            return;
        }
        relayRunning = true;
        relayThread = new Thread(this::relayLoop, "wxeo-relay-" + cfg.instanceId());
        relayThread.setDaemon(true);
        relayThread.start();
    }

    @Override
    public void process(Batch batch) throws Exception {
        if (cfg.orderEarly()) {
            Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
            commitOffsets(batch);
            Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
        }
        long commitMs = effectTransaction(batch, true, UnknownMode.RETRY_IDEMPOTENT, tx -> {
            for (Ledger.ParsedEvent e : batch.events()) {
                boolean apply = true;
                if (inboxClaim) {
                    apply = tx.claimInbox(e.event().eventId());
                    if (!apply) {
                        metrics.inc("duplicate_detected");
                    }
                }
                if (apply) {
                    if (idempotentPostings) {
                        tx.addIdemEvent(e.event());
                    } else {
                        tx.addPlainEvent(e.event());
                    }
                    boolean row = tx.insertOutbox(e.event().eventId(), e.partition(),
                            e.event().entryValueJson(), idempotentPostings);
                    if (row) {
                        metrics.inc("outbox_rows_written");
                    }
                }
            }
        });
        batch.recordEffectCommit(metrics, commitMs);
        if (!cfg.orderEarly()) {
            Failpoints.fire(FailpointNames.BEFORE_PROGRESS_COMMIT);
            commitOffsets(batch);
            Failpoints.fire(FailpointNames.AFTER_PROGRESS_COMMIT);
        }
    }

    private void relayLoop() {
        int batchLimit = 100;
        while (relayRunning) {
            try {
                metrics.inc("relay_cycles");
                List<PostgresSink.OutboxRow> rows = sink.fetchUnpublished(batchLimit);
                if (rows.isEmpty()) {
                    IoUtil.sleepQuietly(20);
                    continue;
                }
                for (PostgresSink.OutboxRow row : rows) {
                    if (!relayRunning) {
                        break;
                    }
                    publication.sendRawBlocking(row.eventId(), row.sourcePartition(), row.payload());
                    Failpoints.fire(FailpointNames.BEFORE_RELAY_MARK);
                    if (sink.markPublished(row.outboxId())) {
                        metrics.inc("outbox_published");
                    }
                }
            } catch (Exception e) {
                metrics.inc("relay_errors");
                relayFailure = e.getClass().getSimpleName() + ": " + e.getMessage();
                IoUtil.sleepQuietly(50);
            }
        }
    }

    public String relayFailure() {
        return relayFailure;
    }

    @Override
    public void close() {
        relayRunning = false;
        if (relayThread != null) {
            try {
                relayThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean inboxClaim() {
        return inboxClaim;
    }

    static MechanismKind kind(ProcessContext ctx) {
        return ctx.cfg.mechanism();
    }
}
