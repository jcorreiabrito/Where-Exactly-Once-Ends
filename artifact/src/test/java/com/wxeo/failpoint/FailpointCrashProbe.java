package com.wxeo.failpoint;

/** Child-process probe: arms a crash failpoint, fires it, must never return (exit code 99). */
public final class FailpointCrashProbe {

    public static void main(String[] args) {
        String faultsLog = args[0];
        Failpoints.initForTest(true, "before_effect_commit", "crash", 1, 0,
                faultsLog, "probe-run", "M0", 1);
        Failpoints.fire("before_effect_commit");
        System.out.println("NOT_REACHED_CRASH_FAILPOINT_DID_NOT_HALT");
        System.exit(42);
    }

    private FailpointCrashProbe() {
    }
}
