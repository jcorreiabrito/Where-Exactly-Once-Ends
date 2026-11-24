package com.wxeo.failpoint;

import com.wxeo.config.EffectiveConfig;
import com.wxeo.util.IoUtil;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Level-1 deterministic failpoints (docs/method.md §5.5, Appendix B; docs/CONTRACTS.md §7).
 *
 * <p>Armed by {@code -Dwxeo.failpoints.enabled=true} or {@code --wxeo.failpoint.enabled=true}
 * plus a fixed failpoint name, an action and {@code after=N} (fire on the N-th hit). At most one
 * firing per process. Every firing appends a receipt to the S4-provided {@code faults.log}; a
 * non-terminal action additionally appends a {@code restored} receipt after the caller has rolled
 * back / performed the action, and a {@code crash} action appends only {@code fired} before
 * {@link Runtime#halt(int) halt(99)} (terminal by design — the process is dead and cannot restore).</p>
 */
public final class Failpoints {

    private static volatile EffectiveConfig.FailpointConfig config;
    private static volatile FaultsLog faultsLog;
    private static volatile String runId = "unknown";
    private static volatile String mechanism = "unknown";
    private static volatile int instanceId = 0;

    private static final AtomicInteger HITS = new AtomicInteger();
    private static final AtomicBoolean FIRED = new AtomicBoolean(false);

    private Failpoints() {
    }

    public static synchronized void init(EffectiveConfig cfg) {
        config = cfg.failpoint();
        runId = cfg.runId();
        mechanism = cfg.mechanism().wireName;
        instanceId = cfg.instanceId();
        faultsLog = new FaultsLog(java.nio.file.Path.of(cfg.faultsLog()), cfg.runId());
        HITS.set(0);
        FIRED.set(false);
        if (enabled() && !FailpointNames.isKnown(config.name())) {
            throw new IllegalArgumentException("unknown failpoint name '" + config.name()
                    + "'; expected one of " + FailpointNames.ALL);
        }
        if (enabled()) {
            // Validate the action eagerly so a bad action cannot surface mid-run.
            FailpointAction a = FailpointAction.parse(config.action());
            if (a == FailpointAction.PAUSE && config.pauseMs() < 0) {
                throw new IllegalArgumentException("pause action needs a non-negative pause duration");
            }
        }
    }

    /** Test hook: arms the failpoints directly without an EffectiveConfig. */
    public static void initForTest(boolean enabled, String name, String action, int after,
                                   long pauseMs, String faultsLogPath, String runId,
                                   String mechanism, int instanceId) {
        config = new EffectiveConfig.FailpointConfig(enabled, name, action, after, pauseMs, faultsLogPath);
        faultsLog = new FaultsLog(java.nio.file.Path.of(faultsLogPath), runId);
        Failpoints.runId = runId;
        Failpoints.mechanism = mechanism;
        Failpoints.instanceId = instanceId;
        HITS.set(0);
        FIRED.set(false);
    }

    public static boolean enabled() {
        EffectiveConfig.FailpointConfig c = config;
        return c != null && c.enabled();
    }

    public static String configuredName() {
        EffectiveConfig.FailpointConfig c = config;
        return c == null ? "" : c.name();
    }

    public static int hits() {
        return HITS.get();
    }

    /**
     * Fires the failpoint if (a) it is enabled, (b) it matches {@code point}, and (c) this is the
     * N-th hit. Never returns on {@code crash} (halts the JVM with exit code 99).
     */
    public static void fire(String point) {
        EffectiveConfig.FailpointConfig c = config;
        if (c == null || !c.enabled() || !point.equals(c.name())) {
            return;
        }
        if (FIRED.get()) {
            return;
        }
        int n = HITS.incrementAndGet();
        int after = Math.max(1, c.after());
        if (n < after) {
            return;
        }
        if (!FIRED.compareAndSet(false, true)) {
            return;
        }
        FailpointAction action = FailpointAction.parse(c.action());
        String detail = detail(action, point, n, after);
        switch (action) {
            case CRASH -> {
                writeReceipt(point, "fired", detail);
                // Flush stdout/stderr before the halt so the evidence is not lost.
                System.out.flush();
                System.err.flush();
                Runtime.getRuntime().halt(99);
            }
            case THROW -> {
                writeReceipt(point, "fired", detail);
                throw new InjectedFaultException(point);
            }
            case ABORT_TXN -> {
                writeReceipt(point, "fired", detail);
                throw new InjectedAbortException(point);
            }
            case PAUSE -> {
                writeReceipt(point, "fired", detail);
                IoUtil.sleepQuietly(c.pauseMs());
                writeReceipt(point, "restored", detail);
            }
            case NONE -> {
                writeReceipt(point, "fired", detail);
                writeReceipt(point, "restored", detail);
            }
        }
    }

    /** Writes the {@code restored} receipt for a non-terminal action after the caller recovered. */
    public static void restored(String point) {
        EffectiveConfig.FailpointConfig c = config;
        if (c == null || !c.enabled() || !point.equals(c.name())) {
            return;
        }
        writeReceipt(point, "restored", detail(FailpointAction.parse(c.action()), point,
                Math.max(1, HITS.get()), Math.max(1, c.after())));
    }

    private static String detail(FailpointAction action, String point, int hit, int after) {
        return "action=" + action.wireName + ";point=" + point + ";hit=" + hit + ";after=" + after
                + ";mechanism=" + mechanism + ";instance=" + instanceId + ";run_id=" + runId;
    }

    private static void writeReceipt(String fault, String event, String detail) {
        FaultsLog log = faultsLog;
        if (log != null) {
            log.receipt(fault, event, detail);
        }
    }

    public static String faultsLogPath() {
        EffectiveConfig.FailpointConfig c = config;
        return c == null ? "" : c.faultsLog();
    }
}
