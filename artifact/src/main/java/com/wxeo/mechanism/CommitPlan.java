package com.wxeo.mechanism;

import com.wxeo.config.EffectiveConfig.MechanismKind;

import java.util.ArrayList;
import java.util.List;

/**
 * Declarative commit plan of a mechanism: the order of the three duties D-effect, D-progress and
 * D-publish (docs/method.md §4.1) under the {@code strict}/{@code early} ordering flag (D2).
 *
 * <p>Used by the implementation to document/assert its own protocol shape and by the unit tests
 * (including the crash-injection state-machine simulation) to verify that the nine mechanisms and
 * the ordering probe match the frozen specification.</p>
 */
public final class CommitPlan {

    public enum Duty {
        EFFECT,
        PROGRESS,
        PUBLISH,
        /** M1-kafka / M4 canonical: one atomic durable operation covering effect and progress. */
        ATOMIC_EFFECT_PROGRESS
    }

    public enum Extra {
        /** Publication is deferred to the M2 outbox relay thread (at-least-once). */
        RELAY_PUBLICATION,
        /** Publication records are written inside the Kafka transaction. */
        PUBLICATION_IN_TRANSACTION,
        /** Kafka offset commit after the DB progress for observability only (M4 family). */
        KAFKA_OFFSETS_OBSERVABILITY_ONLY
    }

    private final MechanismKind mechanism;
    private final boolean early;
    private final List<Duty> duties;
    private final List<Extra> extras;

    private CommitPlan(MechanismKind mechanism, boolean early, List<Duty> duties, List<Extra> extras) {
        this.mechanism = mechanism;
        this.early = early;
        this.duties = List.copyOf(duties);
        this.extras = List.copyOf(extras);
    }

    public MechanismKind mechanism() {
        return mechanism;
    }

    public boolean early() {
        return early;
    }

    public List<Duty> duties() {
        return duties;
    }

    public List<Extra> extras() {
        return extras;
    }

    public String describe() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < duties.size(); i++) {
            if (i > 0) {
                sb.append("->");
            }
            sb.append(duties.get(i));
        }
        if (!extras.isEmpty()) {
            sb.append(" ex=").append(extras);
        }
        return sb.toString();
    }

    /** Canonical plan for a mechanism configuration and ordering variant (method.md §4). */
    public static CommitPlan of(MechanismKind m, boolean early) {
        List<Duty> d = new ArrayList<>();
        List<Extra> x = new ArrayList<>();
        switch (m) {
            case M0, M3 -> {
                if (early) {
                    d.add(Duty.PROGRESS);
                    d.add(Duty.EFFECT);
                    d.add(Duty.PUBLISH);
                } else {
                    d.add(Duty.EFFECT);
                    d.add(Duty.PUBLISH);
                    d.add(Duty.PROGRESS);
                }
            }
            case M1_KAFKA -> {
                d.add(Duty.ATOMIC_EFFECT_PROGRESS);
                x.add(Extra.PUBLICATION_IN_TRANSACTION);
            }
            case M1X, M1X_M3 -> {
                x.add(Extra.PUBLICATION_IN_TRANSACTION);
                if (early) {
                    d.add(Duty.PUBLISH);
                    d.add(Duty.PROGRESS);
                    d.add(Duty.EFFECT);
                } else {
                    d.add(Duty.PUBLISH);
                    d.add(Duty.EFFECT);
                    d.add(Duty.PROGRESS);
                }
            }
            case M2, M2_M3 -> {
                x.add(Extra.RELAY_PUBLICATION);
                if (early) {
                    d.add(Duty.PROGRESS);
                    d.add(Duty.EFFECT);
                } else {
                    d.add(Duty.EFFECT);
                    d.add(Duty.PROGRESS);
                }
            }
            case M4, M4_M3 -> {
                if (early) {
                    // Degraded-variant probe: progress committed in a separate transaction first.
                    d.add(Duty.PROGRESS);
                    d.add(Duty.EFFECT);
                    d.add(Duty.PUBLISH);
                } else {
                    d.add(Duty.ATOMIC_EFFECT_PROGRESS);
                    d.add(Duty.PUBLISH);
                }
                x.add(Extra.KAFKA_OFFSETS_OBSERVABILITY_ONLY);
            }
            default -> throw new IllegalArgumentException("no plan for " + m);
        }
        return new CommitPlan(m, early && m.hasTwoCommits(), d, x);
    }

    /** True when the plan has at least two distinct durable commits (ordering flag meaningful). */
    public boolean hasTwoCommits() {
        return mechanism.hasTwoCommits();
    }
}
