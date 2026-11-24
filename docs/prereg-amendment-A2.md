# Pre-registration Amendment A2 — decision rules, censoring, verdicts, matrix arithmetic, seeds, and formal-model scope

| Field | Value |
|---|---|
| Document | `docs/prereg-amendment-A2.md` — surgical amendment to `docs/prereg.md` and to the clauses of `docs/method.md` listed in §0.2 |
| Status | **DRAFT FOR AUTHOR APPROVAL at the P2 gate.** Not binding until the author approves it item by item. On approval it supersedes exactly the frozen clauses listed in §0.2; everything else in `docs/prereg.md` and `docs/method.md` remains in force. |
| Prepared by | S2 (methodologist), for the lead to present to the author. |
| Trigger | S5 pre-Stage-A red-team review (`docs/redteam-oracle.md`, findings **F-P1, F-P2, F-P3, F-P5, F-O3, F-O4, F-F1**, plus the §14 must-fix list items 9–11). |
| Approved sources | `docs/P1-APPROVAL.md` (all D1–D15 accepted; D2 ordering probe; D8 I1a/I1b; D12 θ; D13 M1-kafka comparability; D15 paired seeds), `docs/BRIEF.md` §2/§5/§6/§9, `docs/AMENDMENT-A1.md`. |
| Does this change the study? | **No matrix, N, factor, cell, gate, metric, or cut-ladder change.** This amendment changes *how the frozen evidence is classified, scored and tested* and corrects documentation, schema and scope statements that the frozen plan or the implementation already contradict. Every run already scheduled in `experiments/run_order.csv` remains scheduled. |
| Required after approval | (i) the listed text edits in `docs/prereg.md`/`docs/method.md`; (ii) schema/validator and analyzer updates (checklist in §6, no code in this document); (iii) one re-freeze after *all* P2 fixes per F-P6; (iv) S5 re-review before Stage A. |
| Evidence snapshot | `docs/prereg-freeze.json` frozen 2026-11-24T13:09:53Z, git `1873ab12…`; `experiments/run_order.csv` 7,871 rows; `formal/tlc-results/tlc-summary.json` checked 2026-11-24T12:51:13Z; recomputations in §9. |

---

## 0. Scope, classification, and what does not change

### 0.1 Item map and classification

Legend — **[C]** correction of a pre-registered defect: the frozen text (or the frozen implementation) is internally inconsistent or unsatisfiable and must be changed; the author must ratify because the pre-registration is frozen. **[CL]** clarification: encoding/accounting wording only; no scientific content changes. **[A]** author decision: more than one defensible repair exists; the draft recommends one and states the trade-off.

| Item | Findings addressed | Class | Supersedes (exact clauses) |
|---|---|---|---|
| **A2-1** Corrected hypothesis decision rules H1–H5 | F-P1 (a)–(d), F-P5 items 5–6 | **C** (+ **A** sub-choices AC-1, AC-3, AC-4) | `prereg.md` §3.1–§3.6; `method.md` §9.5 |
| **A2-2** Censoring rule | F-P5 item 1, F-G2 | **C** (+ **A** sub-choice AC-8: fragility threshold) — implementation alignment to already-normative `method.md` §7.3 | `prereg.md` §3.*, §7.1–§7.4; no method change |
| **A2-3** I1a/I1b and spurious verdict rule | F-O3, F-O4 (+F-O7 partially) | **C** + **CL** (+ **A** sub-choices AC-5 spurious verdict, AC-6 P11 wording) | `prereg.md` §5 (counts/violations), §6.1, §8.1 G3; `method.md` §2.4–§2.8 (one table row), P11 |
| **A2-4** Matrix arithmetic correction | F-P2 | **C** (transcription error; totals unchanged) | `prereg.md` §2.2 (two row cells), §2.8 note |
| **A2-5** Seed-formula encoding | F-P3 | **CL** (+ one **C** sub-fix: `master_seed` length) (+ **A** sub-choice AC-2: no regeneration) | `prereg.md` §4.4; `matrix.yaml` metadata already matches |
| **A2-6** Analyzer changes implied by A2-1…A2-3 | F-P5 items 1–7 | **C** (implementation checklist; no code here) | `tools/analyze.py`, `experiments/validate_record.py` (via freeze) |
| **A2-7** Formal-model scope statement | F-F1 | **C** (claim narrower than the FO-6 promise) + **A** (AC-7: narrow now vs extend model) | `method.md` §6.7 FO-6, §12 checklist; `formal/README.md`; paper TLC paragraph |
| **A2-8** Impact table | — | — | — |

### 0.2 What this amendment does NOT change (explicit)

- **Matrix:** the 9 mechanisms, 8 fault conditions, 7 crash classes, 3 load levels, 3 partition counts, ordering variants, sensitivity blocks and control cells are unchanged; no cell is added, removed or re-labelled as an executed run. `experiments/matrix.yaml` and `experiments/run_order.csv` must **not** be regenerated for A2. (The separate F-P4 must-fix — Stage-A `fault` token case — is a code/matrix defect, not part of A2; it is a precondition for the final freeze.)
- **N:** 30 core / 10 sensitivity / 3 pilot / 20 floor unchanged.
- **Totals:** 7,866 planned slots, 7,851 executable, 20 validation runs, 7,871 scheduled rows unchanged (§4 proves the arithmetic).
- **Gates G1–G8:** semantics unchanged except the two additions in §2.5 (censoring counts against fault-free positive controls) and §3.6 (balanced-spurious-pair mutation class).
- **Metrics, instruments, load modes, cut ladder, exclusion policy (prereg §8.2), rerun policy (§8.3), thresholds (α, Cliff bands, CP CI)** unchanged.
- **Interpretation of results:** no result exists yet at the time of this amendment. No live (non-dry-run) run has been recorded: `results/RUNLOG.md` contains only dry-run entries over synthetic fixtures, and no pilot/Stage A/B/C measurement records exist. This amendment is therefore not data-dependent.

---

## 1. A2-1 — Corrected hypothesis decision rules (F-P1)

### 1.1 Shared definitions (apply to all hypotheses)

**Run outcome for "clean" claims.** For every hypothesis clause that asserts a mechanism produced *no violation*, the run outcome is

```
outcome_clean_fail(r) = r.violations.violating_run  OR  r.quiescence.censored
```

(`censored ⇒ treated as violating`, per A2-2). For existence claims ("a violation occurred") only an **observed** violation counts (`violating_run`), and the evidence run must be non-censored; censoring cannot manufacture evidence.

**Cell sets.** A *cell* is (stage, mechanism, condition, load, partitions, ordering_variant). Three evidence classes are frozen in §1.2:

- **canonical** — the mechanism's declared (strict-ordering, non-probe) configuration; used for every "clean" claim.
- **probe** — the `ordering=early` cells (Stage A `K4`, Stage B `F4b`); a separate evidence class used only for H3's loss arm and reported descriptively elsewhere. Probe cells are *never* counted as mechanism failures for H2/H4/H5; they are the fault model's demonstration that a broken ordering obligation defeats de-duplication.
- **control** — fault-free cells (`A ctrl`, Stage B `F0`, Stage C `F0`); canonical by definition.
- **n/a** — structurally unreachable (cell has no scheduled run); excluded from every denominator and every claim, listed in the reachability tables, never counted as a pass. This is the only mechanism by which n/a cells are handled; the matrix is unchanged.
- **sensitivity / pilot / validation** — reported descriptively; **excluded from confirmatory hypothesis tests** (N=10 sensitivity, N=3 pilot and the 20 validation runs are calibration/robustness evidence, per prereg §2.3–§2.5; retaining them in confirmatory tables was an analyzer defect, F-P5 item 3).

**Confirmatory evidence base for H1–H5:** core blocks only — Stage A (1k events, P=3, N=30) and Stage B core block (100k events, P=6, N=30). Sensitivity blocks (S1: 10k/P6; S2: 100k/P3 and P12, N=10) are reported as sensitivity analyses with pre-registered direction only; they cannot change a hypothesis verdict.

**Denominators (all tables).** For each cell: `N_planned` (frozen slots incl. n/a), `N_executable` (planned − n/a), `N_scheduled` (rows in run_order), `N_valid` (valid and not excluded; includes censored), `N_excluded` (invalid; reasons), `N_missing` (executable − valid − excluded), `N_censored` (⊂ N_valid). No table may report only `N_valid`. A cell with `N_valid < 20` is descriptive only (existing prereg §7.2 rule).

**Tests and pairing.**
- Paired test (only where both mechanisms have a run in the same `(stage, condition, load, partitions, rep)` cell): **McNemar exact sign test** on discordant pairs, **one-sided** in the pre-registered direction (two-sided also reported). Pairing is by the shared repetition seed (D15) and the cell key; runs in different cells are never paired.
- Unpaired: **Fisher exact** (two-sided, or one-sided where the direction is pre-registered).
- Proportions: Clopper–Pearson 95 % CI; zero-event cells report the exact CP upper bound and the rule-of-three bound (prereg §7.5 template unchanged).
- Multiplicity: Holm within the families of §1.8.

### 1.2 Frozen evidence-class cell sets

**Stage A — crash classes (executed sets verified against `run_order.csv`; per-cell N=30).**

| Mechanism | Canonical crash classes | Probe (`ordering=early`) | n/a (no run) |
|---|---|---|---|
| M0 | K1, K2, K3, K5, K6 | K4 | K7 |
| M1-kafka | K1, K2, K5, K6 | — (no second commit) | K3, K4, K7 |
| M1x | K1, K2, K3, K5, K6 | K4 | K7 |
| M2 | K1, K2, K3, K5, K6, K7 | K4 | — |
| M3 | K1, K2, K3, K5, K6 | K4 | K7 |
| M4 | K1, K2, K5, K6 | K4 (degraded-variant probe) | K3, K7 |
| M2+M3 | K1, K2, K3, K5, K6, K7 | K4 | — |
| M4+M3 | K1, K2, K5, K6 | K4 (degraded-variant probe) | K3, K7 |
| M1x+M3 | K1, K2, K3, K5, K6 | K4 | K7 |

Control cells: `A ctrl`, one per mechanism (N=30), canonical.

**Stage B — scripted faults (core block: 100k events, P=6, N=30).**

| Mechanism | Canonical fault cells | Probe (F4b, `early`) | n/a |
|---|---|---|---|
| M0 | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |
| M1-kafka | F0, F1, F2, F3, F6 | — | F4a, F4b, F5 |
| M1x | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |
| M2 | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |
| M3 | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |
| M4 | F0, F1, F2, F3, F4a, F5, F6 | — | F4b |
| M2+M3 | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |
| M4+M3 | F0, F1, F2, F3, F4a, F5, F6 | — | F4b |
| M1x+M3 | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |

Stage C: all fault-free (`F0`) cells are canonical; the 6 core blocks are (10k, 100k) × (P3, P6, P12), N=30; the 1M and saturation blocks are confirmatory only for the pre-registered performance contrasts of prereg §7.3 and are not used in H2/H4 latency tests.

**Executed-cell counts (from `run_order.csv`, reproducible — §9):** 52 crash cells (+9 control cells) and 67 fault cells in the core blocks; Stage A total 1,830 runs = (52+9)×30; Stage B core 67×30 = 2,010. The 8 K4-probe cells and the 6 F4b-probe cells are already inside those executed counts; the 11 n/a crash cells and 5 n/a fault cells have no runs. **No cell is moved between classes by this amendment; the classification is a frozen label on cells that already exist.**

### 1.3 H1 — M1x violates where the DB/offset commits can diverge; M1-kafka does not

**Confirmed** iff all three hold:
1. **Exposure exists.** M1x has ≥ 1 run with `violating_run = true` (observed, not merely censored) in the pre-registered divergence set
   `D_H1 = {A-K2, A-K3} ∪ {B-F1, B-F2, B-F3, B-F4a, B-F6}`,
   and the injection in every cell used for this clause is attested (receiver rule §1.1; a cell is unusable if its attested-fire rate is < 80 % or its exclusion rate > 20 %).
2. **M1-kafka is clean.** Zero `outcome_clean_fail` runs in the **entire canonical executed set of M1-kafka**: Stage A {K1, K2, K5, K6} + `A ctrl`; Stage B {F0, F1, F2, F3, F6}. Report exact CP upper bounds per cell and pooled. (M1-kafka's n/a cells K3, K4, K7, F4a, F4b, F5 are excluded with the reachability reason; they are not passes.)
3. **Paired contrast is significant.** At least one of the five pre-registered McNemar tests is significant after Holm across C1, with the pair present in enough runs:
   `C1 = {A-K2, B-F1, B-F2, B-F3, B-F6}`, comparison **M1x vs M1-kafka**, paired by `(stage, condition, load, partitions, rep)`, one-sided in the direction "M1x has more violating runs", Holm-adjusted p < 0.05.
   Rationale for the set: these are exactly the cells where (i) both mechanisms execute and (ii) the model predicts divergence (M1x W1 exposure vs M1-kafka single-site atomicity). Stage A-K3 is used only in clause 1 because M1-kafka K3 is structurally n/a — the original §3.1(iii) demanded a McNemar test there and was therefore unsatisfiable.

**Refuted** iff clause 2 fails (any M1-kafka canonical `outcome_clean_fail` run — this also fails gate G2 and halts the campaign), or M1x is clean in every cell of `D_H1` with attestation ≥ 80 % in each cell used and `N_valid ≥ 20`.
**Inconclusive** iff the evidence cells fail the attestation/exclusion/censoring quality bars (`N_valid < 20`, attested-fire < 80 %, exclusion > 20 %, or > 20 % censored in a cell required by clause 1), or clause 1 holds but no C1 test is analysable (e.g. fewer than the required paired runs) — then the paper reports the observed violations without the paired significance claim.
**Deleted:** old §3.1(iii) (McNemar on K3). **Not used:** pilot, validation, sensitivity blocks.

### 1.4 H2 — M2 sufficient, with a measured cost

**Correctness component confirmed** iff M2 has zero `outcome_clean_fail` runs in **all M2 canonical cells**:
Stage A {K1, K2, K3, K5, K6, K7} + `A ctrl`; Stage B {F0, F1, F2, F3, F4a, F5, F6} (core N=30). Report CP upper bounds per cell and pooled.
**Probe cells are excluded from this clause and reported separately** (M2's K4/F4b early-ordering rows are the H3 evidence class; the model *predicts* M2 to lose there by design, so including them made H2 refuted by construction).

**Cost component confirmed** iff, in the pre-registered direction,
M2 is worse than M3 **or** M4 on at least one primary performance outcome:
- throughput: `Cliff δ_better(M2, other) ≤ −0.33` (M2 worse) with Holm-adjusted one-sided p < 0.05 in family P1, on the 6 Stage C core blocks (block medians); or
- p99 latency: `δ_better(M2, other) ≤ −0.33` (M2 worse, i.e. higher p99) with Holm-adjusted one-sided p < 0.05 in family P2.
A metric/pair is only assessable with ≥ 4 of the 6 blocks present. `δ_better(x,y) > 0` means x is better (higher throughput / lower p99) — the sign convention is frozen here and replaces the previous `abs(δ)` check, which confirmed a direction that was not tested.

**Confirmed** iff both components; **partially confirmed** iff exactly one; **refuted** iff the correctness component fails with attested injections; **inconclusive** if the correctness cells are underpowered (N_valid < 20 or censoring/exclusion bars) or the cost component is unassessable.

### 1.5 H3 — M3: zero duplicates, losses only under the ordering fault

**Confirmed** iff both arms hold:
- **Duplicate arm.** M3 has zero runs with `v3 = true` across **all** M3 executed cells in the core blocks (Stage A all executed crash classes including the K4 probe; Stage B all executed fault conditions including F4b); per-cell CP upper bounds reported. (An observed duplicate refutes the arm: H3's duplicate claim is zero.)
- **Loss arm.** In the **probe** class — Stage A `K4` with `ordering=early` (N=30) and Stage B `F4b` with `early` (N=30) — M3 has ≥ 1 **non-censored, valid** run with `v4 = true`, the injection attested, and `ordering_variant = early` recorded in the run record. Attested-fire rate ≥ 80 % and exclusion rate ≤ 20 % in the cell where the loss is observed. Existence is the claim; **no p-value is possible or required** (a single observed loss is decisive for "losses occur"; zero losses are bounded statistically).
**Refuted** iff the duplicate arm fails (any v3), or both probe cells meet the quality bars (N_valid ≥ 20 each; attested ≥ 80 %) and **zero** non-censored v4 observations are found across them (then the design could not demonstrate the predicted loss at this resolution).
**Inconclusive** iff probe cells miss the quality bars, the probe was not realised (`ordering=early` flag absent from all runs), or all probe runs are censored.
**Deleted:** old §3.3(iii) and family C2. There is **no** M3-strict K4 control in the frozen matrix (K4 is only run with `early`; the `ctrl` cell is a different condition with a different data seed, so a McNemar pairing by rep would be invalid — the old rule was unsatisfiable and the analyzer self-paired M3 with M3). Adding a new `k4-strict` control cell would change the matrix, which this amendment does not do; the existence claim above does not need it.

### 1.6 H4 — M4 (or M4+M3) sufficient under scripted faults; M4 faster than M2

H4's A1 wording is scoped to the scripted faults F1–F6. The frozen rule added "every reachable Stage A/B cell", which includes M4's pre-declared K6 exposure (model P9: M4 has no sink fencing against a zombie writer) and the early-ordering probes; that made H4 refuted by construction. Corrected scope:

**Correctness component confirmed** iff:
- **M4:** zero `outcome_clean_fail` runs in Stage A {K1, K2, K5} + `A ctrl` and Stage B {F0, F1, F2, F3, F4a, F5, F6} (core N=30). **K6 is excluded from this clause as a pre-declared model-predicted exposure of M4** (method §6.4 J30 / P9): the K6-M4 result is reported separately as a model corroboration test, and its outcome neither confirms nor refutes H4.
- **M4+M3:** zero `outcome_clean_fail` runs in Stage A {K1, K2, K5, K6} + `A ctrl` and Stage B {F0, F1, F2, F3, F4a, F5, F6}. The K6 cell is inside the claim for M4+M3 because the unique index is the mechanism that closes it.
Probe cells excluded (as in H2). K3/K7 are n/a for M4/M4+M3 (reachability).

**Latency component confirmed** iff, on the 6 Stage C core blocks (block medians), `δ_better(M4, M2) ≥ +0.33` on **p99 latency** (primary) with Holm-adjusted one-sided p < 0.05 in family P2, and M4's median p99 is lower than M2's in ≥ 4 of 6 blocks. p50 is a secondary corroboration with the same sign rule and is reported but cannot substitute for p99. (The original "at least one of the two loads at P=6" is not testable as a paired contrast — n=2 blocks cannot reach α=0.05; the 6-block formulation is the pre-registered one from §7.3 of the frozen plan.)

**Confirmed** iff both components; **partially confirmed** iff one; **refuted** iff the correctness component fails; **inconclusive** if performance blocks are insufficient (cut ladder) or correctness cells are underpowered.

### 1.7 H5 — no single mechanism dominates (repaired dominance definition)

The frozen rule was vacuous: clause (i) required zero violations in *every* executed cell including the probe class, which the model predicts no two-commit mechanism can satisfy, so "no mechanism dominates" was confirmed by construction. Two repairs are possible; **AC-1** asks the author to choose.

**Recommended (H5-A — "residual-closure dominance").** A mechanism `x` dominates iff all three hold:
1. **Canonical-correct:** `x` has zero `outcome_clean_fail` runs in all of its canonical correctness cells (§1.2 sets; the same sets used by H2/H4), with CP upper bounds reported.
2. **Performance non-inferior:** `x` is non-inferior to the best other mechanism in ≥ 4 of the 6 Stage C core blocks on **both** throughput and p99, where non-inferior means `|δ_better| < 0.147` (negligible band, as frozen) **and** the 95 % bootstrap CI of the median ratio contains 1 (existing mechanics retained).
3. **No declared residual exposure:** `residual(x) = ∅`, where the residual set is frozen from the P1-approved model (method §6.6/§11) as:

| Mechanism | Closed exposure classes | `residual(x)` |
|---|---|---|
| M0 | — | {E1, E3, E4, E5} |
| M1x | E2 (canonical strict ordering¹), E5 (Kafka transaction) | {E1, E3, E4} |
| M2 | E1, E2 (atomic DB progress), E3 (inbox), E4 | {E5} (relay publication is at-least-once, not exactly-once) |
| M3 | E1 (index), E2 (strict ordering¹), E3 (index), E4 (index) | {E5} |
| M4 | E1, E2 (atomic progress), E4 | {E3 (C7 has no sink fencing), E5} |
| M2+M3 | E1–E4 | {E5} |
| M4+M3 | E1–E4 | {E5} |
| M1x+M3 | E1–E5 (index + Kafka transaction + canonical ordering) | **∅** |

¹ The strict-ordering obligation is discharged by the mechanism's canonical configuration (D2); H3's probe measures the consequence of breaking it. This closure rule is part of the frozen model argument, not a post-hoc excuse.

`residual(x) = ∅` only for **M1x+M3**. Hence H5-A is the falsifiable prediction: *the only complete-coverage mechanism is not also performance-non-inferior*; if M1x+M3 is canonical-correct and non-inferior on both metrics in ≥ 4 blocks, it dominates and **H5 is refuted**. M1-kafka is excluded from the dominance domain (different sink; D13).

**Verdicts:** **Confirmed** iff no mechanism satisfies (1)–(3) **and** at least one mechanism satisfies (1) with adequate power (non-vacuity guard; otherwise inconclusive — this guard is the fix for the vacuity defect). **Refuted** iff some mechanism satisfies all three. **Inconclusive** iff no mechanism is canonical-clean with adequate power, or the required Stage C blocks are missing.
The full residual table is reported descriptively regardless of the verdict, and the paper presents the measured trade-off frontier.

**Alternative (H5-B — "canonical frontier", for AC-1).** Drop clause (3); H5 is then about canonical correctness + performance only: confirmed iff no canonical-clean mechanism is performance-non-inferior; refuted iff one is; inconclusive iff none is canonical-clean. Trade-off: simpler and purely empirical, but it drops the "across all failure classes" part of H5's original statement and is likely to be refuted by M3 if M3 is fastest while still being blind to W2 in the probe class.

### 1.8 Multiplicity families (final)

| Family | Tests | Correction | Used by |
|---|---|---|---|
| **C1** | 5 one-sided McNemar tests: A-K2, B-F1, B-F2, B-F3, B-F6 (M1x vs M1-kafka) | Holm (m = 5) | H1(iii) |
| **C2** | **deleted** (no valid H3 control exists) | — | — |
| **C3** | **deleted** (H4 correctness is a zero-claim, not a comparison) | — | — |
| **C4** | no tests: exact CP bounds for all zero-violation claims (M2, M3, M4, M4+M3, combinations; H3 duplicate arm) | — | H2/H3/H4 |
| **P1** | throughput: Friedman + Nemenyi family-wise across the 8 DB mechanisms (M1-kafka excluded by D13), then pairwise Wilcoxon on 6 block medians with Holm (28 pairs) | Nemenyi gate + Holm | H2/H4/H5 cost/latency |
| **P2** | p99 latency: same structure as P1 | Nemenyi gate + Holm | H2/H4/H5 |
| **P3** | time-to-consistent-state: 3 pre-registered contrasts (M2 vs M3, M4 vs M2, M4+M3 vs M2), pooled over the canonical fault cells of Stage B core (F1,F2,F3,F4a,F5,F6), Fisher exact two-sided, Holm (m = 3); per-cell rates reported descriptively | Holm | H2/H4 cost corroboration |
| **T1** | exploratory: everything else (divergence, publication integrity, 1M and capacity contrasts, sensitivity blocks, pilot/validation evidence) | none; labelled exploratory | — |

α = 0.05. Both unadjusted and adjusted p reported; effect sizes reported regardless. H2/H4 performance contrasts are pre-declared members of P1/P2 and use their Holm-adjusted p; no additional correction. H5 uses the frozen non-inferiority mechanics. The Fisher sensitivity analysis (unpaired) is reported alongside McNemar as in the frozen §7.2.

### 1.9 Satisfiability check (why the corrected rules can fire on the frozen matrix)

- H1: M1x and M1-kafka both execute A-K2 and B-F1/F2/F3/F6 (verified from `run_order.csv`; §9), so five McNemar tests are possible, each with up to 30 paired runs (a pair requires both mechanisms' runs for the same `rep`); K3 is used only as exposure evidence (M1x executes it; M1-kafka does not).
- H2: M2's canonical sets contain only cells the model predicts clean; probe cells are a separate class.
- H3: the K4/F4b probe cells exist for M3 with N=30 each; existence needs no comparator.
- H4: M4/M4+M3 canonical sets exclude only the pre-declared K6-M4 exposure and the probe class; all other canonical cells exist.
- H5: `residual = ∅` for M1x+M3, which executes Stage C core blocks; the verdict depends on measured performance (and can be refuted).
- Deleted families C2/C3 remove two tests that could never be significant; total multiplicity shrinks, so the amendment is conservative (no inflation).

---

## 2. A2-2 — Censoring rule (F-P5 item 1)

### 2.1 Normative basis

`docs/method.md` §7.3 already states: *"censored runs are included in the correctness primary outcome (they are violating runs)"*, and `prereg.md` §8.3 states censored runs are **not** re-run (they are outcomes). The implementation and analysis contradicted this (analyzer copied `violating_run` only; `extract()` at `analyze.py:407`). This item is therefore a **correction**: no new methodology, only the already-approved one made operative.

### 2.2 Rule

1. A run is **censored** iff `quiescence.censored = true` (quiescence not reached within `T_max`, as defined in method §7.3). Censored runs are `valid` unless a §8.2 exclusion cause applies; they are never excluded for being censored and never re-run.
2. For every "clean" claim (all H1(ii), H2, H3 duplicate arm, H4, H5 clause 1, and the C4 zero-violation confirmations): `outcome_clean_fail(r) = violating_run(r) OR censored(r)`. Censoring therefore *counts against* a mechanism's clean claim — the conservative direction.
3. For existence claims (H1(i), H3 loss arm): only an observed violation in a **non-censored** valid run is evidence; censoring never substitutes for an observation.
4. Time-to-consistency (family P3): censoring is the outcome itself — proportion consistent within `T_max`, CP CI; no change.
5. **Reporting requirement (no silent denominators):** every correctness table reports per cell `N_planned`, `N_executable`, `N_valid`, `N_excluded` (by reason), `N_censored`, `N_missing`, and both `violating_rate` and `violating_or_censored_rate`. The primary correctness statistic for clean claims is the violating-or-censored rate; the uncorrected rate is reported alongside for transparency but never used as the primary.
6. **Fragility:** a cell with `N_censored / N_valid > 20 %` is flagged; any hypothesis whose verdict depends on a flagged cell is reported as fragile (the verdict stands only if its direction is conservative — e.g. a clean claim that already fails on uncorrected violations). No additional threshold is introduced (AC-8).
7. Gates: for the fault-free positive controls (G2: M1-kafka, M4+M3 control runs), a censored run counts as a gate failure (a fault-free control that cannot quiesce indicates a harness/mechanism problem); this is the conservative reading of "zero violations in control runs". No other gate changes.
8. The run record already carries `quiescence.censored`; the analyzer must derive `outcome_clean_fail` from it (checklist §6, item 2). The oracle and the runner are **not** changed by this item.

---

## 3. A2-3 — I1a/I1b and spurious-posting verdict rule (F-O3, F-O4)

### 3.1 The contradiction and the resolution

The frozen normative I1 is D8's **I1 = I1a ∧ I1b**: I1a structural well-formedness (per event, exactly one D and one C posting, equal amount, one currency — `method.md` §2.3), I1b signed sum zero. Under that definition a whole-entry duplicate (`count = 4`) **does** violate I1a — so the explanatory table in `method.md` §2.8 ("whole entry duplicated → I1 passes") and proposition P11's operational test (`v3=1 ∧ v1=0`) were wrong: under the approved I1, `v3=1 ⇒ v1=1` for every posting-level duplicate, and the test could never fire. The oracle already computes the two components separately (`structurally_malformed_entries`, `imbalanced_sum_entries`, `imbalanced_entries`; `oracle.py:584–585, 700–726`) and reports `I1a`/`I1b` attribution (`oracle.py:844–847`).

**Resolution (minimal; keeps D8 unchanged):** split the *reporting* of I1 into its two approved components, and restate P11 in terms of I1b (sum-to-zero) only:
- `v1a` (derived, analysis-level) := `counts.structurally_malformed_entries > 0`;
- `v1b` (derived, analysis-level) := `counts.imbalanced_sum_entries > 0`;
- `v1` stays the stored, normative I1 flag: `v1 = v1a ∨ v1b` (by construction of the oracle counter; validator enforces the boolean identity).
- **P11 (restated, author-owned text):** *there exist runs in which I3 is violated while I1b holds — a duplicated balanced entry still sums to zero; therefore sum-to-zero reconciliation is blind to duplication, while the structural clause I1a detects it.* The pre-registered P11 test becomes `v3 = 1 ∧ v1b = 0` (derived from stored counts, no post-hoc choice). The `method.md` §2.8 table row is corrected to: whole-entry duplication → **I1a fails, I1b passes, I2 passes, I3 fails, I4 passes**. The paper's practitioner message is corrected to "sum-to-zero checks are blind; ledger-shape checks are not" (AC-6 offers the alternative of redefining I1 as pairability, which would change D8 and is not recommended).

### 3.2 Schema ratification (spurious and the I1 split)

The oracle already emits `v5 = spurious_postings > 0` and sets `violating_run = v1 ∨ v2 ∨ v3 ∨ v4 ∨ v5` (`oracle.py:835–866`), and already exports `structurally_malformed_entries` and `imbalanced_sum_entries` in `counts` (`oracle.py:1006–1007`). The frozen run-record schema lists neither, and the validator enforces **exact key sets** (`validate_record.py:167–180`), so the live path cannot currently emit a schema-valid record (S5 F-RN1). The amendment ratifies the implemented semantics and extends the schema exactly as follows.

**`violations` object (exact key set):** `{v1, v2, v3, v4, v5, violating_run}` — all booleans. Frozen equation:

```
violating_run  ==  v1 ∨ v2 ∨ v3 ∨ v4 ∨ v5
v1             ==  (v1a ∨ v1b)          [derived from counts; not stored as separate booleans]
v1a            :=  counts.structurally_malformed_entries > 0
v1b            :=  counts.imbalanced_sum_entries > 0
v2 = mutated_postings>0 ; v3 = dup_postings>0 ; v4 = lost_postings>0 ; v5 = spurious_postings>0
```

**`counts` object (add exactly two keys):** `structurally_malformed_entries` (I1a: events whose posting multiset is not exactly one balanced pair) and `imbalanced_sum_entries` (I1b: events whose signed sum is non-zero, per currency). `imbalanced_entries` remains the I1-total count (events with structural **or** sum defect). `spurious_postings` already exists and now has an official verdict class.

**Verdict semantics (no post-hoc freedom):**
- Any of the five violation classes makes `violating_run = true` **as stored by the oracle**; the analysis reads `violations.violating_run` and never recomputes it (validator cross-checks the equation above and fails a record whose stored flags disagree).
- A **spurious** posting is a fabricated sink row (`π ∉ Ω`). It is a correctness violation (`v5`), **not** an invalid-run signal: a pipeline that fabricates balanced entries must be reported, not excluded. The previous `method.md` §2.4 wording ("harness-invalid if >0 unexpectedly") is withdrawn as the verdict rule and replaced by: spurious ⇒ `v5` ⇒ `violating_run`; attribution is reported as `SPURIOUS`, and it is never folded into `dup_postings`.
- Malformed/unparseable observed rows (F-O7) are counted as spurious postings (and set `v5`); they are never silently dropped.
- G3 adds one mutation class — **balanced spurious pair** (both legs of a new balanced event id, sum zero) — with ≥ 100 mutations, and requires the detector to attribute it to `SPURIOUS`/`v5`. The existing five classes stay (drop, duplicate, flip sign, update value, single spurious row); 100 % detection remains required.
- I2's append-only *privilege* attestation (F-O2) is not part of this amendment; its separation is an implementation defect to be fixed under the same re-freeze.

---

## 4. A2-4 — Matrix arithmetic correction (F-P2)

### 4.1 Corrected rows (§2.2 "Executed fault cells" column)

The frozen table lists `M4 | 8 (F4b n/a)` and `M4+M3 | 8 (F4b n/a)`: a row cannot be 8 while naming one of its eight cells n/a. The corrected values are **M4 = 7, M4+M3 = 7**. The stated total (**67**) was already correct; the two row cells were the transcription error.

### 4.2 Arithmetic proof (unchanged totals)

| Quantity | Recomposition |
|---|---|
| Executed fault cells | 72 planned slots − 5 n/a = **67**. n/a = M1-kafka {F4a, F4b, F5} (3) + M4 {F4b} (1) + M4+M3 {F4b} (1). Per-mechanism: 8+5+8+8+8+**7**+8+**7**+8 = **67** ✔ |
| Executed crash cells | 63 planned − 11 n/a = **52**. n/a = M1-kafka {K3,K4,K7} (3) + M0 {K7} + M1x {K7} + M3 {K7} + M4 {K3,K7} (2) + M4+M3 {K3,K7} (2) + M1x+M3 {K7} = 11. Per-mechanism: 6+4+6+7+6+5+7+5+6 = **52** ✔ |
| Pilot | 9×8×3 = 216 planned slots; the 5 n/a fault cells contribute 5×3 = **15** n/a slots; 216 − 15 = **201** executable runs ✔ |
| Stage A | 52×30 + 9×30 = 1,560 + 270 = **1,830** ✔ |
| Stage B | 67×30 + 67×10 + 67×10×2 = 2,010 + 670 + 1,340 = **4,020** ✔ |
| Stage C | 9×2×3×30 + 9×10 + 9×10 = 1,620 + 90 + 90 = **1,800** ✔ |
| Nominal planned slots | 216 + 1,830 + 4,020 + 1,800 = **7,866** ✔ |
| Executable matrix runs | 201 + 1,830 + 4,020 + 1,800 = **7,851** ✔ (+20 validation = 7,871 scheduled rows) ✔ |

### 4.3 Clerical clarification in §2.8

The sentence "The 15-slot difference is exactly the mechanism × condition reachability n/a set" refers **only to the pilot slot accounting** (5 n/a fault cells × 3 reps). It must not be read as 15 n/a slots among Stages A–C: Stage A/B/C matrices contain only executed cells, and the Stage A/B totals (1,830 / 4,020) are unaffected by the pilot's 15 n/a slots. Text edit only.

---

## 5. A2-5 — Seed-formula encoding (F-P3)

### 5.1 Frozen formula (implemented; now stated unambiguously)

The frozen prose writes `SHA-256(master_seed | "data" | stage | condition | rep)[0:16]`, leaving the separator, the token encodings and the slice unit ambiguous. The implementation (`matrix.py:74–82`) and `matrix.yaml` metadata (`data_seed_formula`, `injection_seed_formula`) use **direct ASCII concatenation without separators**. This amendment freezes that reading:

```
master_seed_text = "wxeo-campaign-v1|master|2026-11-24"
master_seed      = lower_hex( SHA-256( UTF-8(master_seed_text) ) )[0:32]        # 32 hex chars
data_seed        = lower_hex( SHA-256( UTF-8( master_seed + "data" + stage_token
                                              + condition_token + decimal(rep) ) ) )[0:16]
injection_seed   = lower_hex( SHA-256( UTF-8( master_seed + "inj"  + stage_token
                                              + condition_token + decimal(rep) ) ) )[0:16]
```

- `+` = direct concatenation of the ASCII byte strings; **no separator, no delimiter, no whitespace**.
- `master_seed` is 32 lowercase hex chars (the frozen §4.4 clarification wrongly said 16; that sentence is corrected). `data_seed`/`injection_seed` are 16 lowercase hex chars (64 bits).
- `stage_token` ∈ {`pilot`, `a`, `b`, `c`} (single lowercase letter for the stages; the word `pilot` for the pilot).
- `condition_token` = the lowercase condition token used in the run id and cell id: `f0`…`f6`, `k1`…`k7`, `ctrl` for matrix runs; `valid-<kind>-<fault token>` for the 20 validation runs (`matrix.py:197–201`), which are evidence-only and excluded from every confirmatory analysis.
- `decimal(rep)` = canonical decimal integer `0..N−1`, no sign, no leading zeros.
- `order_seed` and `bootstrap_seed` are generated by the same phrase rule (`order`: `wxeo-campaign-v1|order|2026-11-24`; bootstrap: `wxeo-campaign-v1|bootstrap|2026-11-24|B=10000`), as recorded in `docs/prereg-freeze.json`.
- The `|` characters inside `master_seed_text` (and in the order/bootstrap phrase constants) are part of those fixed phrase strings, which are hashed **once** to derive the seeds; they are **not** separators in the per-run `data_seed`/`injection_seed` formula, which is a bare concatenation.
- **Uniqueness (no collisions):** `master_seed` is fixed-length 32; `kind` ∈ {`data`,`inj`} (two distinct formula families); `stage_token` is from a finite set with distinct prefixes; `condition_token` is from a finite set and is followed by a decimal rep, so the decomposition is unique for all scheduled combinations. No two scheduled runs can share a seed.

### 5.2 Regeneration decision

**No already-frozen seed is regenerated.** The committed `experiments/run_order.csv` contains 7,871 rows (7,851 matrix runs + 20 validation runs) whose `data_seed`/`injection_seed` values reproduce exactly under the formula above; §9 shows the full recomputation and one worked example. `matrix.yaml` and `run_order.csv` therefore keep their hashes. The only changes are: the `docs/prereg.md` §4.4 text (this section), the `master_seed` length clarification, and a re-freeze of the documentation hashes (`docs/prereg-freeze.json`) after the edit.

**Alternative (not recommended):** implement literal `|` separators. This changes every seed, `matrix.yaml`, `run_order.csv` and all freeze hashes, for zero scientific benefit, and is only defensible if the author prefers the literal frozen notation over the implemented one. AC-2 records the decision; the draft recommendation is to freeze the implemented concatenation (no regeneration).

---

## 6. A2-6 — Analyzer changes implied by A2-1…A2-3 (implementation checklist for S4)

No code in this amendment. The frozen analyzer must implement exactly the following; the re-freeze records the new analyzer hash, and S5 re-verifies against a synthetic fixture before Stage A.

1. **Outcome derivation.** In `extract()`: add `v5`, `censored`, and the derived `outcome_clean_fail = violating_run ∨ censored`; add derived `v1a`/`v1b` from `counts.structurally_malformed_entries` / `counts.imbalanced_sum_entries`; **read** `violations.violating_run` — never recompute it.
2. **Denominators.** `correctness_by_cell`: replace `n_planned = len(group)` with the frozen slot count from `matrix.yaml`/`prereg-freeze.json`; emit `N_planned`, `N_executable`, `N_scheduled`, `N_valid`, `N_excluded` (by reason), `N_censored`, `N_missing`; k for clean claims uses `outcome_clean_fail`; report both uncorrected and correctness-corrected rates; per-invariant columns for `v1a`, `v1b`, `v3`, `v4`, `v5` (not only `v1`).
3. **Evidence-class classifier.** Add a frozen `evidence_class ∈ {canonical, probe, control, validation, pilot, sensitivity, n/a}` computed from (§1.2) cell tables keyed by (stage, condition, load, partitions, ordering_variant); **no hypothesis test may run over a set that mixes classes**. Drop pilot, validation (`cell_id` prefix `V.`), sensitivity and n/a runs from confirmatory tables; keep separate datasets for them.
4. **H1.** Delete the Stage A-K3 McNemar test. Build family **C1** = 5 one-sided McNemar tests, named `C1.stageA_K2`, `C1.stageB_F1`, `C1.stageB_F2`, `C1.stageB_F3`, `C1.stageB_F6` (M1x vs M1-kafka), paired by the seed key; Holm over 5; implement the three-clause rule of §1.3 including the attestation/exclusion/censoring bars. Restrict the M1-kafka clean arm to its canonical executed set (not "every M1-kafka valid run", which currently includes pilot/validation runs and every fault).
5. **H2.** Correctness over M2 canonical sets only; remove probe cells from `m2_cells`; drop the `fault not in (F0,none,ctrl)` catch-all (it pulled pilot fault runs in). Cost component: direction-aware one-sided tests with the frozen `δ_better` sign convention; require ≥ 4 blocks.
6. **H3.** Remove family **C2** and the `c2_sig` requirement. Duplicate arm: `v3` over all M3 executed core cells. Loss arm: `v4` over M3 probe cells (K4/F4b with `ordering_variant=early`), non-censored valid runs, attested; implement the §1.5 quality bars.
7. **H4.** Correctness over the §1.6 sets (M4 excludes K6; M4+M3 includes K6); report K6-M4 as a separate model-corroboration row; latency component uses the 6-block p99 test with the sign rule (replace the current `abs(δ)` helper).
8. **H5.** Implement §1.7 H5-A (or H5-B if the author chooses it): canonical-clean + non-inferiority + frozen residual table; add the non-vacuity guard; report the residual table and frontier descriptively.
9. **Delete family C3**; restrict **P3** to the 3 pre-registered contrasts with per-cell descriptive reporting (do not pool fault and fault-free runs; current code pools all 100k/P6 runs).
10. **Direction-aware cost helper.** Replace `_cost_confirmed`'s `abs(delta) ≥ 0.33 and p < 0.05` with a sign-aware, one-sided test in the pre-registered direction; report two-sided p as well; fix the mislabelled "better/worse" note.
11. **Validator.** Extend `VIOLATION_KEYS` to `{v1,v2,v3,v4,v5,violating_run}` and `COUNTS_KEYS` with `structurally_malformed_entries`, `imbalanced_sum_entries`; enforce `violating_run == v1∨v2∨v3∨v4∨v5` and `v1 == (counts.structurally_malformed_entries>0 ∨ counts.imbalanced_sum_entries>0)`; keep the exact-key-set rule (so the schema and oracle cannot drift).
12. **Reporting tables.** Every hypothesis table in the paper reports: evidence class, `N_planned/N_executable/N_valid/N_excluded/N_censored/N_missing`, exclusion and censoring rates, CP bounds, unadjusted and Holm-adjusted p, effect sizes; probe results in a separate table; n/a cells in a reachability table.
13. **Acceptance check before Stage A (S5 re-review):** analyzer reproduces the amended verdicts on a synthetic fixture containing (i) an M1x-vs-M1-kafka divergence case, (ii) a probe-only M3 loss case, (iii) a canonical M2 clean case with censored runs, (iv) a spurious-only case, (v) a duplicate case with `v1a=1, v1b=0`; all five verdicts must match the amended rules; no data-dependent code paths.

---

## 7. A2-7 — Formal-model scope statement (F-F1)

### 7.1 The claim the paper may make (frozen wording)

> "We encoded the crash-point semantics of the mechanism family in a bounded TLA+ model (`formal/ledger.tla`) and checked it with TLC 2.19 (tla2tools v1.7.4, SHA-256 `936A2620…0E88`) on Java 21 at the bounds N = 3 events, batch size 2 and one partition, for eight configurations of four mechanism booleans (de-duplication, atomic effect+progress, Kafka transaction, early-ordering probe). TLC checked the state invariants `TypeOK`, `I1Balance`, `I3Unique` and `I4CompleteWhenDone`; all eight configurations matched the expectations recorded before checking. The model is a **bounded witness of the protocol abstractions** — it shows that the two windows are reachable and that the closure conditions hold at those bounds — and **not an unbounded proof** of the propositions. The following are not modelled and are therefore not machine-checked: rebalancing and the concurrent (zombie) writer (C6/C7), append-only mutation (I2), liveness under fair recovery (I4 is checked as a state invariant at termination), publication-stream invariants, partial-batch durability (C11/C12) and multi-partition scheduling. The machine-checked claim therefore covers only the W1/W2 closure argument for the abstract family; the C7-based claims (P9, exposure E3, the M4+M3 rationale) rest on the pen-and-paper argument and on the Stage A K6 experiments. `StrictProgressBound` is a tautology of the encoding and is not counted as evidence; `StreamCompleteWhenDone` is defined but not listed in any TLC configuration and is not a checked result."

### 7.2 Evidence and reporting corrections

- Current committed results: `formal/tlc-results/tlc-summary.json`, checked 2026-11-24T12:51:13Z, distinct states **20, 18, 8, 8, 8, 39, 22, 21** (configs 01–08); violating configurations stop at the first counterexample, so their counts vary slightly with worker scheduling. Outcomes by invariant: configs 01 (naive) and 06 (txn-only) violate `I3Unique`; 07 (early probe) and 08 (early+dedup) violate `I4CompleteWhenDone`; 02–05 violate nothing.
- **The paper/prereg must cite the committed summary (version, bounds, outcome by invariant name, timestamp) and must not hard-code a distinct-state sequence as a result.** The stale sequences in `formal/README.md` ("20, 18, 8, 8, 8, 8, 21, 21") and in the drafted paper paragraph (which quoted 36/21) are documentation defects to be corrected on approval; the model outcomes are unchanged.
- method §6.7 FO-6's promise that the model covers "C3, C4, C5, **C7**" is **narrowed** by the paragraph above: C3/C4/C5 (the windows) are modelled; C7 is not. Extending the model with a two-writer zombie fragment is deferred (AC-7); if the author prefers extension to narrowing, the wording above changes to list the extended bounds and C7 becomes formally covered only after a new TLC run.
- The model↔mechanism mapping remains: M1-kafka = atomic+txn, M1x = txn only, M3/M2 = dedup (M2's atomic-ledger half; its relay is not modelled), M4 = atomic; the paper must state this abstraction explicitly (as the paragraph does for the booleans).

---

## 8. A2-8 — Impact table

| Item | Why needed | What changes in results / statistics | Risk if not approved |
|---|---|---|---|
| **A2-1** corrected H1–H5 | Current rules are unsatisfiable (H1 iii, H3 iii) or refuted/vacuous by construction (H2/H4 probe cells, H5) | Hypothesis verdicts become computable; family C2/C3 deleted; C1 redefined (5 McNemar tests); P3 restricted; H5 definition replaced (AC-1) | The five hypotheses cannot be reported as designed; paper would report "inconclusive"/"confirmed by construction" while claiming to test them — the review's top-severity finding |
| **A2-2** censoring | Method §7.3 already requires censored = violating; analyzer currently scores censored runs clean (false-safety direction) | Correctness rates become conservative; denominators fully reported; gates treat censored controls as failures | Up to 100 % censoring could masquerade as clean data; every "zero violations" claim would be uninterpretable |
| **A2-3** I1a/I1b + spurious | Normative I1 contradicts §2.8/P11, making P11 unfalsifiable; spurious verdict is an unratified extension and balanced fabrication is invisible to the published v1–v4 rule | `violations.v5` and two `counts` fields become official (already emitted by the oracle); P11 test becomes `v3=1 ∧ v1b=0`; balanced-spurious mutation class added to G3 | P11 reported as "falsified by construction"; a fabricating pipeline can be scored clean; live records fail the validator (no valid data) |
| **A2-4** matrix arithmetic | Two §2.2 row cells are internally inconsistent (8 with a named n/a cell) though totals are right | Documentation only; totals and run counts unchanged (proved in §4; verified against `run_order.csv`) | Reviewers recompute and find a contradiction in the pre-registered matrix; erodes trust in all other counts |
| **A2-5** seed encoding | Frozen prose formula is ambiguous (separators, slice unit, master length); implementation is bare concatenation | No seed changes, no regeneration; doc text + one length correction; re-freeze of documentation hashes | Pre-registration-vs-implementation mismatch; reproducibility audit fails even though all 7,871 seeds are reproducible |
| **A2-6** analyzer checklist | The frozen analyzer implements the defective rules literally | New test names, families, denominators and filters (no change to raw data) | Even after A2-1…A2-3 approval, the frozen analysis would still produce the old, invalid verdicts |
| **A2-7** TLC scope | Paper claim promised C7 coverage that the model does not have; README/paper quote stale counts | Claim narrowed to a bounded witness; C7/K6 evidence attributed to argument + Stage A; counts cited from the summary | Overclaiming machine-checked coverage of the zombie exposure; falsifiable-adversarial review would find it |
| **A2-8** this table | The author must see the blast radius of each item | — | Uninformed approval of a package that changes five hypothesis verdicts |

---

## 9. Provenance, recomputations, and cross-check of S5 findings

### 9.1 Independent recomputations (all read-only, reproducible)

| Check | Result |
|---|---|
| `experiments/run_order.csv` row count | 7,871 = pilot 221 (201 matrix + 20 validation) + A 1,830 + B 4,020 + C 1,800 ✔ |
| Stage A executed crash classes per mechanism | M0 6, M1-kafka 4, M1x 6, M2 7, M3 6, M4 5, M2+M3 7, M4+M3 5, M1x+M3 6 = **52**; +9 control cells ✔ |
| Stage B executed fault cells per mechanism | M0 8, M1-kafka 5, M1x 8, M2 8, M3 8, M4 **7**, M2+M3 8, M4+M3 **7**, M1x+M3 8 = **67** ✔ (F-P2 confirmed) |
| Probe cells | K4 executed for 8 mechanisms (240 rows = 8×30); F4b for 6 mechanisms (180 rows in the core block) ✔ |
| Stage B blocks | 100k/P6 2,010; 10k/P6 670; 100k/P3 670; 100k/P12 670 ✔ |
| Stage C blocks | six fixed-rate blocks × 270 = 1,620; 1M/P6 90; 100k/P6 saturation 90 = 1,800 ✔ |
| Seed formula | `master_seed = 90650742b511adfb662272117444cf43`; worked example (first scheduled row, pilot M4+M3 F4a rep 2): `data_seed = facff75d0b52e055`, `injection_seed = 059307dd98035b92` — both reproduce under §5.1 ✔ |
| TLC summary | checked 2026-11-24T12:51:13Z; distinct states 20,18,8,8,8,39,22,21; outcomes as in §7.2 ✔ |
| Oracle/validator state | oracle already emits `v5` and the two I1 split counts; validator enforces exact key sets and therefore currently rejects such records (schema gap closed by A2-3) ✔ |

### 9.2 Cross-check of the S5 memo

Confirmed substantively: F-P1(a)–(d), F-P2, F-P3, F-P5 items 1–7, F-O3, F-O4, F-F1 (including the stale README sequence and `StrictProgressBound` tautology). Two specifics are inaccurate and should not be copied into the paper:

1. **F-P1(a)'s suggested feasible C1 family includes `F4a`** ("Stage-B F1/F2/F4a/F6"). F4a is **n/a for M1-kafka** (prereg §2.2; method §6.3 J10; `run_order.csv`: F4a is executed for 8 mechanisms but not M1-kafka), so it cannot be a paired M1x-vs-M1-kafka contrast. The memo's own citation of the frozen §3.6 list (F0,F1,F2,F3,F6) is correct; the suggested replacement should read F1,F2,F3,F6. This amendment uses those four plus A-K2.
2. **F-F1's quoted distinct-state sequence** ("20, 18, 8, 8, 8, 8, 22, 21 (07 = 22, 06 = 39)") mis-transcribes the committed summary. The actual sequence is 20, 18, 8, 8, 8, **39**, **22**, 21 (06 = 39, 07 = 22). The substantive finding (README hard-codes a stale list, counts vary for violating configs) stands; use the summary, not either hard-coded list.

---

## 10. Requested author decision

The item-by-item approval sheet and the signature block follow; the requested decision is stated in one paragraph at the end of this section.


| Approval | Item | Recommended option |
|---|---|---|
| ☐ | A2-1 corrected H1–H5 | approve as drafted (H5-A = AC-1 recommended) |
| ☐ | A2-2 censoring rule | approve |
| ☐ | A2-3 I1a/I1b + spurious verdict | approve (AC-6 split reporting; AC-5 v5 = violation) |
| ☐ | A2-4 matrix arithmetic | approve |
| ☐ | A2-5 seed encoding | approve (AC-2 no regeneration) |
| ☐ | A2-6 analyzer checklist | approve |
| ☐ | A2-7 TLC scope | approve (AC-7 narrow claim) |
| ☐ | AC-1 H5 definition | H5-A ☐ / H5-B ☐ |
| ☐ | AC-3 H1 significance | require C1 ☐ / existence-only ☐ |
| ☐ | AC-4 H2 cost semantics | at least one of M3/M4 ☐ / both ☐ |
| ☐ | AC-8 censoring fragility | > 20 % flag ☐ / other: ______ |

| Role | Name | Decision | Date (UTC) |
|---|---|---|---|
| Author (owner of the formal model and proofs) | João Brito | approve / revise | |
| Lead (integration, freeze) | | acknowledge and apply | |
| Methodologist (S2) | | submitted | 2026-11-24 |

**Requested decision (one paragraph).** The author is asked to **approve all eight items of Amendment A2 as drafted** at the P2 gate. A2-1, A2-2, A2-3, A2-4, A2-6 and A2-7 (the scope narrowing) are corrections of pre-registered defects — frozen rules that the frozen matrix cannot satisfy, an analysis that contradicts the already-normative censoring rule, an invariant/proposition contradiction, a two-cell arithmetic transcription error, an analyzer that implements the defective rules, and a machine-checked claim broader than the model — and none of them changes the study's matrix, N, factors, cells, gates, metrics or thresholds. A2-5 is a clarification: the seed formula is frozen exactly as already implemented, and **no seed is regenerated** (all 7,871 committed seed values reproduce exactly). The only genuine methodological choices in the package are bounded and separately listed for item-by-item approval: **AC-1** H5 repair (recommended H5-A residual-closure dominance; alternative H5-B canonical frontier), **AC-2** seed encoding (recommended: freeze the implemented concatenation; alternative: literal separators and regenerate all seeds — not recommended), **AC-3** H1 significance clause (recommended: require one of the five C1 McNemar tests; alternative: existence-plus-clean-control only, weaker), **AC-4** H2 cost semantics (recommended: at least one of M3/M4 on at least one metric, matching the frozen "or"; alternative: require both), **AC-5** spurious verdict (recommended: `v5` is a correctness violation and ratifies the oracle's current behaviour; alternative: spurious ⇒ invalid run, which risks excluding real fabrication bugs), **AC-6** P11 restatement (recommended: split I1a/I1b reporting and restate P11 in terms of I1b, preserving D8; alternative: redefine I1 as pairability, which changes the approved formal invariant), **AC-7** TLC scope (recommended: narrow the claim now as worded in §7; alternative: extend the model with a two-writer zombie fragment, which needs new TLC work and delays the freeze), and **AC-8** censoring fragility threshold (recommended: report censoring rates and flag cells with > 20 % censoring; no hard cap). Approving A2-1…A2-8 with the recommended options for AC-1…AC-8 makes all five hypotheses decidable on the frozen matrix, restores the pre-registered censoring semantics, makes the verdict rule explicit, fixes the two documentation defects, and aligns the machine-checked claim with the actual TLC run — after which the lead can regenerate the freeze and Stage A can start. If any item is rejected, the affected hypotheses/claims must be reported as "inconclusive" (H1–H5) or "untested" (P11, machine-checked claim) rather than silently reverting to the defective rule.