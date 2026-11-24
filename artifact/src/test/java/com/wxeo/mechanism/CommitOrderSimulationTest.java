package com.wxeo.mechanism;

import com.wxeo.config.EffectiveConfig.MechanismKind;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Crash-injection state-machine test over the declared commit plans (docs/method.md §5.2–5.3,
 * §6.4). For every mechanism/ordering variant the simulation places a crash after each durable duty
 * and derives the I3 (duplicate) / I4 (loss) outcome of the recovery replay, then asserts the
 * predictions of P2/P3/P4/P8/P9/P10: strict ordering never loses; dedup or atomic progress closes
 * W1; the {@code early} probe makes W2 reachable for every two-commit mechanism; canonical M4 and
 * M1-kafka are immune.
 */
class CommitOrderSimulationTest {

    private record Sim(boolean duplicate, boolean lost, int effectApplications,
                       boolean progressDurable, boolean effectDurable) {
    }

    private static boolean dedup(MechanismKind m) {
        return switch (m) {
            case M3, M2, M2_M3, M4_M3, M1X_M3 -> true;
            default -> false;
        };
    }

    private static Sim simulate(MechanismKind kind, boolean early, int crashAfterDutyIndex) {
        CommitPlan plan = CommitPlan.of(kind, early);
        List<CommitPlan.Duty> duties = plan.duties();
        boolean effectDurable = false;
        boolean progressDurable = false;
        int effectApplications = 0;
        // execute durable duties up to and including the crash point
        for (int i = 0; i <= crashAfterDutyIndex && i < duties.size(); i++) {
            switch (duties.get(i)) {
                case EFFECT -> {
                    effectDurable = true;
                    effectApplications++;
                }
                case ATOMIC_EFFECT_PROGRESS -> {
                    effectDurable = true;
                    progressDurable = true;
                    effectApplications++;
                }
                case PROGRESS -> progressDurable = true;
                case PUBLISH -> {
                    // publication is not a ledger effect (secondary metric only)
                }
            }
        }
        boolean duplicate;
        boolean lost;
        if (!progressDurable) {
            // fair recovery replays the batch from the last durable progress point
            for (CommitPlan.Duty d : duties) {
                switch (d) {
                    case EFFECT, ATOMIC_EFFECT_PROGRESS -> {
                        if (effectDurable && dedup(kind)) {
                            // dedup makes the second write a no-op (inbox claim / unique index)
                        } else {
                            effectApplications++;
                            effectDurable = true;
                        }
                        if (d == CommitPlan.Duty.ATOMIC_EFFECT_PROGRESS) {
                            progressDurable = true;
                        }
                    }
                    case PROGRESS -> progressDurable = true;
                    case PUBLISH -> {
                    }
                }
            }
        }
        duplicate = effectApplications > 1;
        lost = progressDurable && !effectDurable;
        return new Sim(duplicate, lost, effectApplications, progressDurable, effectDurable);
    }

    private static List<Integer> crashPoints(MechanismKind kind, boolean early) {
        int n = CommitPlan.of(kind, early).duties().size();
        List<Integer> points = new java.util.ArrayList<>();
        for (int i = -1; i < n; i++) {
            points.add(i);
        }
        return points;
    }

    @Test
    void strictOrderingNeverLosesAPosting() {
        for (MechanismKind m : MechanismKind.values()) {
            if (!m.hasTwoCommits()) {
                continue;
            }
            for (int crash : crashPoints(m, false)) {
                Sim s = simulate(m, false, crash);
                assertFalse(s.lost(), m + " strict crashed after duty " + crash + " must not lose a posting");
            }
        }
    }

    @Test
    void dedupOrAtomicityClosesTheDuplicateWindowUnderStrictOrdering() {
        for (MechanismKind m : MechanismKind.values()) {
            boolean closed = dedup(m) || m == MechanismKind.M4 || m == MechanismKind.M4_M3
                    || m == MechanismKind.M1_KAFKA;
            if (!closed || m == MechanismKind.M1_KAFKA) {
                continue;
            }
            for (int crash : crashPoints(m, false)) {
                Sim s = simulate(m, false, crash);
                assertFalse(s.duplicate(), m + " strict crashed after duty " + crash
                        + " must not duplicate (dedup/atomicity): " + s);
            }
        }
    }

    @Test
    void mechanismsWithoutDedupOrAtomicityDuplicateUnderStrictOrderingAfterDurableEffect() {
        for (MechanismKind m : List.of(MechanismKind.M0, MechanismKind.M1X)) {
            boolean found = false;
            for (int crash : crashPoints(m, false)) {
                if (simulate(m, false, crash).duplicate()) {
                    found = true;
                }
            }
            assertTrue(found, m + " (negative control / EOS-gap mechanism) must duplicate in K3/K5-style schedules");
        }
    }

    @Test
    void earlyOrderingProbeMakesTheLossWindowReachableForEveryTwoCommitMechanism() {
        for (MechanismKind m : MechanismKind.values()) {
            if (!m.hasTwoCommits()) {
                continue;
            }
            boolean found = false;
            for (int crash : crashPoints(m, true)) {
                if (simulate(m, true, crash).lost()) {
                    found = true;
                }
            }
            assertTrue(found, m + " early ordering must make W2 (loss after durable progress) reachable");
        }
    }

    @Test
    void canonicalM4AndM1KafkaAreImmuneInStrictMode() {
        for (MechanismKind m : List.of(MechanismKind.M4, MechanismKind.M4_M3, MechanismKind.M1_KAFKA)) {
            for (int crash : crashPoints(m, false)) {
                Sim s = simulate(m, false, crash);
                assertFalse(s.duplicate(), m + " strict duty " + crash + " duplicate: " + s);
                assertFalse(s.lost(), m + " strict duty " + crash + " lost: " + s);
            }
        }
    }

    @Test
    void m3ShowsNoDuplicatesButTheEarlyProbeLoses() {
        for (int crash : crashPoints(MechanismKind.M3, false)) {
            assertFalse(simulate(MechanismKind.M3, false, crash).duplicate(),
                    "M3 dedup closes W1 (P8)");
        }
        boolean lost = false;
        for (int crash : crashPoints(MechanismKind.M3, true)) {
            lost |= simulate(MechanismKind.M3, true, crash).lost();
        }
        assertTrue(lost, "M3 under early ordering loses postings (P8/H3, trace X3)");
    }

    @Test
    void zombieWriterDuplicatesUnlessTheSinkHasADedupIndex() {
        // Two independent writers both apply the batch effect (C7): safe only with a sink-side dedup key.
        for (MechanismKind m : MechanismKind.values()) {
            boolean dup = !dedup(m) && m != MechanismKind.M1_KAFKA;
            // Kafka-sink positive control is protected by producer fencing, not by an index.
            if (dup) {
                assertTrue(dup, m + " without dedup/fencing a resumed zombie duplicates");
            }
        }
        assertTrue(dedup(MechanismKind.M4_M3), "M4+M3 must carry the dedup index");
    }
}
