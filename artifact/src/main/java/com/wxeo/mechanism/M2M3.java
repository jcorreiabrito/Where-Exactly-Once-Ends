package com.wxeo.mechanism;

/**
 * M2+M3 — M2's DB transaction and relay, with the posting unique index as the dedup claim instead of
 * an inbox table (docs/method.md §4.8):
 * {@code {insert_idem postings, INSERT outbox ... ON CONFLICT (event_id) DO NOTHING}} then progress.
 */
public final class M2M3 extends M2 {

    public M2M3(ProcessContext ctx) {
        super(ctx, false, true);
    }

    @Override
    public String id() {
        return "M2+M3";
    }
}
