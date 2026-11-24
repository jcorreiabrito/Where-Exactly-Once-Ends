package com.wxeo.mechanism;

/**
 * M4+M3 — M4's atomic {@code {postings + progress}} transaction with {@code insert_idem} instead of
 * {@code insert_plain}: atomic progress closes W1/W2 and the posting index closes C7 zombies
 * (docs/method.md §4.8, P10).
 */
public final class M4M3 extends M4 {

    public M4M3(ProcessContext ctx) {
        super(ctx, true);
    }

    @Override
    public String id() {
        return "M4+M3";
    }
}
