package com.wxeo.mechanism;

/**
 * M1x+M3 — M1x plus {@code insert_idem} for the external write: the posting index closes W1 while
 * the Kafka transaction keeps publication exactly-once (docs/method.md §4.8, P10).
 */
public final class M1xM3 extends M1x {

    public M1xM3(ProcessContext ctx) {
        super(ctx, true);
    }

    @Override
    public String id() {
        return "M1x+M3";
    }
}
