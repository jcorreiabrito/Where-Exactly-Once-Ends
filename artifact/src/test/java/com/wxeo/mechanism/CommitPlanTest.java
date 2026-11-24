package com.wxeo.mechanism;

import com.wxeo.config.EffectiveConfig.MechanismKind;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asserts the declared duty ordering of every mechanism against docs/method.md §4 (commit ordering,
 * publication path, ordering probe D2). This is the executable form of the §4.9 distinctness matrix.
 */
class CommitPlanTest {

    @Test
    void m0StrictIsEffectPublishProgress() {
        CommitPlan p = CommitPlan.of(MechanismKind.M0, false);
        assertEquals(List.of(CommitPlan.Duty.EFFECT, CommitPlan.Duty.PUBLISH, CommitPlan.Duty.PROGRESS),
                p.duties());
        assertTrue(p.early() == false);
    }

    @Test
    void twoCommitMechanismsCommitProgressBeforeEffectOnlyInEarlyProbe() {
        for (MechanismKind m : MechanismKind.values()) {
            if (!m.hasTwoCommits()) {
                continue;
            }
            CommitPlan strict = CommitPlan.of(m, false);
            CommitPlan early = CommitPlan.of(m, true);
            assertTrue(strict.early() == false);
            assertTrue(early.early(), m + " early flag must be recorded");

            int strictEffect = strict.duties().indexOf(CommitPlan.Duty.EFFECT);
            int strictProgress = strict.duties().indexOf(CommitPlan.Duty.PROGRESS);
            if (m == MechanismKind.M4 || m == MechanismKind.M4_M3) {
                // canonical M4: effect and progress share one atomic commit
                assertTrue(strict.duties().contains(CommitPlan.Duty.ATOMIC_EFFECT_PROGRESS), m.name());
            } else {
                assertTrue(strictEffect >= 0 && strictProgress >= 0, m.name());
                assertTrue(strictEffect < strictProgress,
                        m + " strict must commit effect before progress: " + strict.describe());
            }
            int earlyEffect = early.duties().indexOf(CommitPlan.Duty.EFFECT);
            int earlyProgress = early.duties().indexOf(CommitPlan.Duty.PROGRESS);
            if (m != MechanismKind.M1_KAFKA) {
                assertTrue(earlyProgress >= 0 && earlyEffect >= 0, m.name());
                assertTrue(earlyProgress < earlyEffect,
                        m + " early must commit progress before effect: " + early.describe());
            }
        }
    }

    @Test
    void m1KafkaIsSingleAtomicCommitAndOrderingIsNotApplicable() {
        CommitPlan p = CommitPlan.of(MechanismKind.M1_KAFKA, true);
        assertEquals(List.of(CommitPlan.Duty.ATOMIC_EFFECT_PROGRESS), p.duties());
        assertFalse(p.early(), "M1-kafka has one commit; the ordering flag is recorded as n/a (strict)");
    }

    @Test
    void publicationPathsAreDeclaredPerMechanism() {
        assertTrue(CommitPlan.of(MechanismKind.M2, false).extras()
                .contains(CommitPlan.Extra.RELAY_PUBLICATION));
        assertTrue(CommitPlan.of(MechanismKind.M2_M3, false).extras()
                .contains(CommitPlan.Extra.RELAY_PUBLICATION));
        assertTrue(CommitPlan.of(MechanismKind.M1X, false).extras()
                .contains(CommitPlan.Extra.PUBLICATION_IN_TRANSACTION));
        assertTrue(CommitPlan.of(MechanismKind.M1_KAFKA, false).extras()
                .contains(CommitPlan.Extra.PUBLICATION_IN_TRANSACTION));
        assertTrue(CommitPlan.of(MechanismKind.M4, false).extras()
                .contains(CommitPlan.Extra.KAFKA_OFFSETS_OBSERVABILITY_ONLY));
        assertFalse(CommitPlan.of(MechanismKind.M3, false).extras()
                .contains(CommitPlan.Extra.RELAY_PUBLICATION));
    }
}
