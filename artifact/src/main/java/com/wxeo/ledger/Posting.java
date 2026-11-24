package com.wxeo.ledger;

import java.util.List;

/**
 * One posting leg as it is written to the ledger sink and to the ground-truth file.
 *
 * <p>Canonical ground-truth line (docs/CONTRACTS.md §4): compact JSON, keys in exactly this order,
 * no spaces, LF line endings, ordered by {@code (event_id, leg_no)}.</p>
 */
public record Posting(
        int lineNo,
        String eventId,
        int legNo,
        String accountId,
        String side,
        long amountMinor,
        String currency) {

    /** Canonical ground-truth JSON line WITHOUT the trailing newline. */
    public String gtLine() {
        return "{\"line_no\":" + lineNo
                + ",\"event_id\":\"" + eventId + '"'
                + ",\"leg_no\":" + legNo
                + ",\"account_id\":\"" + accountId + '"'
                + ",\"side\":\"" + side + '"'
                + ",\"amount_minor\":" + amountMinor
                + ",\"currency\":\"" + currency + "\"}";
    }

    /** Signed amount: debit positive, credit negative (docs/method.md §2.1). */
    public long signedAmount() {
        return side.equals("D") ? amountMinor : -amountMinor;
    }

    public static List<Posting> list(Posting a, Posting b) {
        return List.of(a, b);
    }
}
