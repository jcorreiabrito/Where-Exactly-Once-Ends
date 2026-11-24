package com.wxeo.failpoint;

/** Thrown by a failpoint armed with action {@code throw}. */
public class InjectedFaultException extends RuntimeException {

    private final String point;

    public InjectedFaultException(String point) {
        super("injected fault at failpoint " + point);
        this.point = point;
    }

    public String point() {
        return point;
    }
}
