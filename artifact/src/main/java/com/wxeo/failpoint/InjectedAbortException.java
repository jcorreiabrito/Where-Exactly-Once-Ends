package com.wxeo.failpoint;

/**
 * Thrown by a failpoint armed with action {@code abort_txn}: the mechanism must roll back the open
 * transaction(s) and skip the progress commit, modelling a transactional abort at that crash point.
 */
public class InjectedAbortException extends RuntimeException {

    private final String point;

    public InjectedAbortException(String point) {
        super("injected transaction abort at failpoint " + point);
        this.point = point;
    }

    public String point() {
        return point;
    }
}
