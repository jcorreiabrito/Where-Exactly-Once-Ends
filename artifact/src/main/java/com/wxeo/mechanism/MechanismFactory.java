package com.wxeo.mechanism;

import com.wxeo.config.EffectiveConfig.MechanismKind;

/** Constructs the mechanism configuration for a service instance (docs/method.md §4). */
public final class MechanismFactory {

    private MechanismFactory() {
    }

    public static Mechanism create(ProcessContext ctx) {
        MechanismKind kind = ctx.cfg.mechanism();
        return switch (kind) {
            case M0 -> new M0(ctx);
            case M1_KAFKA -> new M1Kafka(ctx);
            case M1X -> new M1x(ctx);
            case M2 -> new M2(ctx);
            case M3 -> new M3(ctx);
            case M4 -> new M4(ctx);
            case M2_M3 -> new M2M3(ctx);
            case M4_M3 -> new M4M3(ctx);
            case M1X_M3 -> new M1xM3(ctx);
        };
    }

    /** Returns the declared commit plan for a mechanism/ordering (see {@link CommitPlan}). */
    public static CommitPlan plan(MechanismKind kind, boolean early) {
        return CommitPlan.of(kind, early);
    }

    public static boolean requiresDb(MechanismKind kind) {
        return kind.dbSink();
    }
}
