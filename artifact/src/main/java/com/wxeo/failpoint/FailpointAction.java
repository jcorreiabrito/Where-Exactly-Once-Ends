package com.wxeo.failpoint;

/** Failpoint actions (docs/CONTRACTS.md §7): {@code crash|throw|abort_txn|pause}. */
public enum FailpointAction {
    /** Append the fired receipt and terminate the process immediately with exit code 99. */
    CRASH("crash"),
    /** Append the receipt and throw a generic injected fault (caller rolls back, no progress commit). */
    THROW("throw"),
    /** Append the receipt and throw an injected transaction-abort (caller aborts the open transaction). */
    ABORT_TXN("abort_txn"),
    /** Append the receipt, sleep for the configured pause, append the restored receipt, continue. */
    PAUSE("pause"),
    /** Append fired+restored receipts and continue with no effect (sham-injection mode for G4). */
    NONE("none");

    public final String wireName;

    FailpointAction(String wireName) {
        this.wireName = wireName;
    }

    public static FailpointAction parse(String raw) {
        if (raw == null) {
            return CRASH;
        }
        String v = raw.trim().toLowerCase();
        if (v.startsWith("pause:")) {
            return PAUSE;
        }
        for (FailpointAction a : values()) {
            if (a.wireName.equals(v)) {
                return a;
            }
        }
        throw new IllegalArgumentException("unknown failpoint action '" + raw
                + "'; expected crash|throw|abort_txn|pause[:ms]|none");
    }
}
