# Pre-registration Amendment A2 — decision rules, censoring, verdicts, matrix amendment, seeds, and formal-model scope

| Field | Value |
|---|---|
| Document | `docs/prereg-amendment-A2.md` — surgical amendment to `docs/prereg.md` and to the clauses of `docs/method.md` listed in §0.1 |
| Status | **DRAFT FOR AUTHOR APPROVAL at the P2 gate.** Not binding until the author approves it item by item. On approval it supersedes exactly the frozen clauses listed in §0.1; everything else in `docs/prereg.md` and `docs/method.md` remains in force. |
| Prepared by | S2 (methodologist), for the lead to present to the author. |
| Trigger | S5 pre-Stage-A red-team review (`docs/redteam-oracle.md`: **F-P1, F-P2, F-P3, F-P5, F-O3, F-O4, F-F1**, §14 must-fix items 9–11) and the realisability fixes it forced on the matrix (`results/DEVIATIONS.md` items **S5-1/S5-4**, §0.3 below). |
| Approved sources | `docs/P1-APPROVAL.md` (D1–D15; D2 ordering probe; D8 I1a/I1b; D12 θ; D13 M1-kafka comparability; D15 paired seeds), `docs/BRIEF.md` §2/§5/§6/§9, `docs/AMENDMENT-A1.md`. |
| Does this change the study? | **It ratifies the P2 matrix amendment already forced by S5's realisability fixes (§0.2) and changes how the frozen evidence is classified, scored and tested; it prescribes no further matrix change.** Mechanisms, fault classes, load levels, partition counts, repetitions, Stage C, metrics, instruments, cut ladder and gates are unchanged except the K6/M1-kafka-K5/M1-kafka-F6 reachability declarations (already executed in the matrix) and the two gate clarifications in §2.5/§3.6. |
| Evidence snapshot (pinned for this revision) | `experiments/matrix.yaml` sha256 `4c1051f739f53ee1bf154782eff1cb72ea08f1f14fd0611ae8cfba2341e29599`; `experiments/run_order.csv` sha256 `4698eace6c08861bfafec292a3ecc43b1a5545499821f456153afa1d75fac166`; **7,514 scheduled rows = 7,488 executable + 26 validation**; matrix accounting block (matrix.yaml `accounting:`) reports pilot 216/198, A 1,530, B 3,960, C 1,800, totals 7,506 planned / 7,488 executable / 26 validation. `docs/prereg-freeze.json` at this revision: 2026-11-24T13:32:43Z, git `28d02aa…`. **Provisional: all cell sets and counts below are derived from these two matrix hashes; they MUST be re-validated at the final freeze (hash Z) after S5's re-review.** No live (non-dry-run) run has been recorded; only dry-run fixtures and synthetic fixtures exist, so this amendment is not data-dependent. |
| Required after approval | (i) the listed text edits in `docs/prereg.md`/`docs/method.md`; (ii) schema/validator/analyzer ratification and the acceptance checks of §6 (no code in this document); (iii) one re-freeze at the final matrix hashes per F-P6 and re-validation of the derivation in §1.2/§9; (iv) S5 re-review before Stage A. |

---

## 0. Scope, classification, matrix amendment, and what does not change

### 0.1 Item map and classification

Legend — **[C]** correction of a pre-registered defect: the frozen text (or the frozen implementation) is internally inconsistent or unsatisfiable and must be changed; the author must ratify because the pre-registration is frozen. **[CL]** clarification: encoding/accounting wording only; no scientific content changes. **[A]** author decision: more than one defensible repair exists; the draft recommends one and states the trade-off.

| Item | Findings addressed | Class | Supersedes (exact clauses) |
|---|---|---|---|
| **A2-1** Corrected hypothesis decision rules H1–H5 | F-P1 (a)–(d), F-P5 items 5–6; matrix change §0.2 | **C** (+ **A** sub-choices AC-1, AC-3a/b, AC-4) | `prereg.md` §3.1–§3.6; `method.md` §9.5 |
| **A2-2** Censoring rule | F-P5 item 1, F-G2 | **C** (+ **A** sub-choice AC-8) — implementation alignment to already-normative `method.md` §7.3 | `prereg.md` §3.*, §7.1–§7.4; no method change |
| **A2-3** I1a/I1b and spurious verdict rule | F-O3, F-O4 (+F-O7 partially) | **C** + **CL** (+ **A** sub-choices AC-5, AC-6) — now phrased as **ratification** of the implemented schema/verdict semantics plus acceptance checks | `prereg.md` §5 (counts/violations), §6.1, §8.1 G3; `method.md` §2.4–§2.8 (one table row), P11 |
| **A2-4** Matrix arithmetic and per-stage accounting | F-P2; matrix change §0.2 | **C** (transcription defect + required old→new accounting) | `prereg.md` §2.2 (row cells), §2.3, §2.5, §2.8 |
| **A2-5** Seed-formula encoding | F-P3; matrix change §0.2 | **CL** (+ one **C** sub-fix: `master_seed` length) (+ **A** sub-choice AC-2) | `prereg.md` §4.4; matrix.yaml metadata already matches |
| **A2-6** Analyzer ratification and remaining acceptance checks | F-P5 items 1–7; matrix change §0.2 | **C** — phrased as ratify current implementation + complete listed checks | `tools/analyze.py`, `experiments/validate_record.py` (via freeze) |
| **A2-7** Formal-model scope statement + forced C7 limitation | F-F1; K6 realisability (§0.2) | **C** + **A** (AC-7a/b) | `method.md` §6.7 FO-6, §12 checklist; `formal/README.md`; paper TLC paragraph |
| **A2-8** Impact table | — | — | — |

### 0.2 Matrix amendment at P2 — requires author approval

S5's realisability findings forced a change to the frozen matrix. The change is **already executed** (`results/DEVIATIONS.md` S5-1/S5-4; `experiments/matrix.yaml` + `run_order.csv` regenerated and re-hashed); this amendment brings it to the author for approval and makes every rule and denominator consistent with it. The K6 declaration and the six G1 gate-control runs are final; no further regeneration is expected (if S5's re-review forces one, the lead sends new hashes and the mechanical derivation of §1.2/§4 is re-run).

| Stage | Planned (old → new) | Executable (old → new) | Δ runs | Cause |
|---|---|---|---|---|
| P pilot | 216 → 216 | 201 → **198** | −3 | M1-kafka F6 becomes n/a → one pilot cell × 3 repetitions; pilot n/a slots 15 → 18 |
| A | 1,830 → **1,530** | 1,830 → **1,530** | −300 | K6 (zombie) declared n/a for **all nine** mechanisms (not realisable within `T_max`, see §7.2): −9 cells × 30 = −270; M1-kafka K5 n/a (no external rewind without breaking D5): −1 cell × 30 = −30. Crash cells 52 → 42; controls unchanged at 9 (`fault=none`) |
| B | 4,020 → **3,960** | 4,020 → **3,960** | −60 | M1-kafka F6 n/a (rewinding its committed offset would duplicate the positive control's ledger): 1 fault cell × (30 core + 10 S1 + 20 S2) = −60; fault cells 67 → 66 |
| C | 1,800 → 1,800 | 1,800 → 1,800 | 0 | unchanged (6 fixed-rate blocks × 270 + 1M 90 + saturation 90) |
| **Total** | **7,866 → 7,506** | **7,851 → 7,488** | **−360 / −363** | — |
| Validation | 20 → **26** | — | +6 | six pre-Stage-A G1 gate-control runs added (g1k3 ×3, g1k5 ×3); other validation unchanged (sham 6, selftest 6, c1 2, quiesce 2, oracle 4) |
| Scheduled rows | 7,871 → **7,514** | — | −357 | = executable 7,488 + validation 26 |

**Author decision required:** approve the matrix amendment in this table (it changes N counts and two reachability declarations; it does not change mechanisms, factors, loads, partitions, the number of repetitions, Stage C, the metrics or the cut ladder). Approval is listed in §10.

### 0.3 What this amendment does NOT change (explicit)

- **No further matrix regeneration.** All cell sets in §1.2 are derived from the pinned `matrix.yaml`/`run_order.csv` hashes in the header and are re-validated at the final freeze. If a future regeneration is forced, the sets/counts are re-derived mechanically (the derivation rules, not the numbers, are the frozen object).
- **Mechanisms, factors, levels, repetitions, Stage C, metrics, instruments, load modes, cut ladder, exclusion policy (prereg §8.2), rerun policy (§8.3), thresholds (α, Cliff bands, CP CI)** unchanged.
- **Gates G1–G8:** semantics unchanged except the two additions in §2.5 (censoring counts against fault-free positive controls) and §3.6 (balanced-spurious-pair mutation class).
- **No result exists yet.** No live run has been recorded (`results/RUNLOG.md` contains dry-run entries only), so no verdict can be data-dependent.

### 0.4 Author-choice register (AC-1…AC-8)

All choices are bounded; the draft recommendation is first in each bullet.

- **AC-1 — H5 definition.** **(a) H5-A residual-closure dominance (recommended):** canonical-correct ∧ performance-non-inferior ∧ no declared residual exposure; with K6 n/a the residual table is model-declared and reported as such. **(b) H5-B canonical frontier:** drop the residual clause; simpler and purely empirical, but with K6 n/a M4/M3 may become canonical-clean and performance-non-inferior, so H5-B is materially more likely to be refuted for the wrong reason (no failure-class coverage).
- **AC-2 — Seed encoding.** **(a) Freeze the implemented concatenation, no seed regeneration (recommended).** **(b) Literal `|` separators:** regenerates all seeds, matrix/run_order and freeze hashes for no scientific benefit.
- **AC-3 — H1 confirmatory test.** **(a) AC-3b (recommended):** one pre-registered pooled stratified exact sign test over `C1 = {A-K2, B-F1, B-F2, B-F3}` (aggregated discordant pairs b, c; exact one-sided binomial at p = 0.5; direction "M1x violates more"; cells unweighted; ≥ 2 cells with pairs required; the four per-cell Holm tests reported descriptively) — needs `b ≥ 5` with `c = 0` at α = 0.05. **(b) AC-3a:** require ≥ 1 of the four per-cell one-sided McNemar tests after Holm (m = 4; smallest threshold 0.0125 → a cell needs `b ≥ 7` with `c = 0`). AC-3b is the higher-power choice and is recommended because M1-kafka F6 is n/a (the old highest-frequency replay cell is no longer pairable).
- **AC-4 — H2 cost semantics.** **(a) At least one of M3/M4 on at least one metric (recommended; matches the frozen "M3 or M4").** **(b) Require both.**
- **AC-5 — Spurious verdict.** **(a) `v5` is a correctness violation (recommended; ratifies current oracle behaviour).** **(b) Spurious ⇒ invalid run:** risks excluding real fabrication bugs.
- **AC-6 — P11 restatement.** **(a) Split I1a/I1b reporting and restate P11 in terms of I1b (recommended; preserves D8).** **(b) Redefine I1 as pairability:** changes the approved formal invariant.
- **AC-7 — TLC scope and the forced C7 limitation.** **(a) Narrow the claim (recommended):** bounded witness only, as worded in §7.1. **Forced limitation bullet:** C7/E3/P9/K6 have no experimental or model-checked evidence — an owner cannot be paused past `max.poll.interval.ms` = 300 s within `T_max` = 300 s, so K6 is n/a for every mechanism, and the model itself does not include C7; these claims are model-and-argument-only and are carried to the paper's threats section (§7.2). **(b) Extend the model with a two-writer C7 fragment:** formalises the argument but still produces no K6 data and delays the freeze.
- **AC-8 — Censoring fragility.** **(a) Flag cells with > 20 % censored (recommended), no hard cap.** **(b) A different threshold/rule (author-specified).**


---

## 1. A2-1 — Corrected hypothesis decision rules (F-P1), reconciled to the P2 matrix

### 1.1 Shared definitions (apply to all hypotheses)

**Run outcome for "clean" claims.** For every hypothesis clause that asserts a mechanism produced *no violation*, the run outcome is

```
outcome_clean_fail(r) = r.violations.violating_run  OR  r.quiescence.censored
```

(`censored ⇒ treated as violating`, per A2-2). For existence claims ("a violation occurred") only an **observed** violation counts (`violating_run`), and the evidence run must be non-censored; censoring cannot manufacture evidence.

**Evidence classes** (frozen labels; the classifier derives membership from the pinned matrix, not from run-id strings):
- **canonical** — strict-ordering, non-probe cells (all controls are canonical).
- **probe** — `ordering=early` cells: Stage A `K4` (8 mechanisms), Stage B `F4b` (6 mechanisms). A separate evidence class used only for H3's loss arm and reported descriptively elsewhere; never counted as a mechanism failure for H2/H4/H5.
- **control** — fault-free cells: Stage A controls now carry `fault=none` (run-id/cell-id token `ctrl`, e.g. `A.M0.ctrl.1k.p3`); Stage B `F0`; Stage C `F0`.
- **n/a** — no scheduled run (structurally unreachable or not realisable); excluded from every denominator and claim, listed in the reachability tables, never counted as a pass.
- **pilot / validation / sensitivity** — reported descriptively and **excluded from confirmatory hypothesis tests**. The 26 validation runs (including the 6 pre-Stage-A G1 gate-control runs) are evidence-only; the N=10 sensitivity blocks cannot change a hypothesis verdict.

**Confirmatory evidence base for H1–H5:** core blocks only — Stage A (1k events, P=3, N=30) and Stage B core block (100k events, P=6, N=30). Stage C (fault-free performance) feeds only the performance components.

**Denominators (all tables).** For each cell, report `N_planned`, `N_executable`, `N_scheduled`, `N_valid` (valid and not excluded; includes censored), `N_excluded` (by reason), `N_missing`, `N_censored` (⊂ N_valid). Source of the planned/executable numbers: the frozen `matrix.yaml` accounting block (pilot 216/198, A 1,530/1,530, B 3,960/3,960, C 1,800/1,800; totals 7,506 planned / 7,488 executable / 26 validation / 7,514 scheduled). A cell with `N_valid < 20` is descriptive only.

**Tests and pairing.**
- Paired tests only where both mechanisms have a run in the same `(stage, condition, load, partitions, rep)` cell: **McNemar exact sign test** on discordant pairs, one-sided in the pre-registered direction (two-sided also reported). Pairing is by the shared repetition seed (D15) and the cell key; runs in different cells are never paired.
- Unpaired: Fisher exact.
- Proportions: Clopper–Pearson 95 % CI; zero-event cells report the exact CP upper bound and the rule-of-three bound (prereg §7.5 template unchanged).
- **Pooled stratified sign test** (used by AC-3b for H1): for a frozen set of cells `S`, let `b_c` be the number of pairs in cell `c` where the treatment mechanism fails and the control mechanism is clean, and `c_c` the reverse; aggregate `b = Σ_c b_c`, `c = Σ_c c_c`; the statistic is the exact one-sided binomial sign test of `b` successes in `n = b + c` trials at p = 0.5. Each discordant pair has weight 1; cells are unweighted; a cell with no pairs contributes zero; the test is analysable only if ≥ 2 cells contribute pairs, otherwise the hypothesis is inconclusive. The cells, weights and sign convention are frozen here, so there is no post-hoc pooling choice.
- Multiplicity: Holm within the families of §1.8.

### 1.2 Frozen evidence-class cell sets (derived from the pinned matrix)

Derived from `matrix.yaml` sha256 `4c1051f7…` / `run_order.csv` sha256 `4698eace…` and re-validated at the final freeze. Counts are quoted, not hard-coded into the rules: the classifier must fail if the matrix hash differs from the freeze.

**Stage A — crash classes (core N=30; control cells N=30).**

| Mechanism | Canonical crash classes | Probe (`ordering=early`) | n/a (no run) |
|---|---|---|---|
| M0 | K1, K2, K3, K5 | K4 | K6, K7 |
| M1-kafka | K1, K2 | — | K3, K4, K5, K6, K7 |
| M1x | K1, K2, K3, K5 | K4 | K6, K7 |
| M2 | K1, K2, K3, K5, K7 | K4 | K6 |
| M3 | K1, K2, K3, K5 | K4 | K6, K7 |
| M4 | K1, K2, K5 | K4 (degraded-variant probe) | K3, K6, K7 |
| M2+M3 | K1, K2, K3, K5, K7 | K4 | K6 |
| M4+M3 | K1, K2, K5 | K4 (degraded-variant probe) | K3, K6, K7 |
| M1x+M3 | K1, K2, K3, K5 | K4 | K6, K7 |

Controls: nine Stage A cells, one per mechanism, `fault=none` (canonical).

**Stage B — scripted faults (core block: 100k events, P=6, N=30).**

| Mechanism | Canonical fault cells | Probe (F4b, `early`) | n/a |
|---|---|---|---|
| M0 | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |
| M1-kafka | F0, F1, F2, F3 | — | F4a, F4b, F5, F6 |
| M1x | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |
| M2 | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |
| M3 | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |
| M4 | F0, F1, F2, F3, F4a, F5, F6 | — | F4b |
| M2+M3 | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |
| M4+M3 | F0, F1, F2, F3, F4a, F5, F6 | — | F4b |
| M1x+M3 | F0, F1, F2, F3, F4a, F5, F6 | F4b | — |

Stage C: all fault-free (`F0`) cells canonical; the six core blocks are (10k, 100k) × (P3, P6, P12), N=30; the 1M and saturation blocks are confirmatory only for the pre-registered performance contrasts and are not used in H2/H4 latency tests.

**Counts implied by the tables (quoted from the pinned accounting):** Stage A = 51 cells (34 canonical crash + 8 K4-probe + 9 controls) × 30 = 1,530 runs; n/a crash cells = 21 of 63. Stage B = 66 executed fault cells (60 canonical + 6 F4b-probe) with n/a cells = 6 of 72. Pilot = 66 executable cells of 72 planned (6 n/a cells = 18 n/a slots of 216). The matrix amendment and its delta are in §0.2; the arithmetic proof is in §4.

### 1.3 H1 — M1x violates where the DB/offset commits can diverge; M1-kafka does not

**Confirmed** iff all three hold:
1. **Exposure exists.** M1x has ≥ 1 run with `violating_run = true` (observed, non-censored) in the pre-registered divergence set
   `D_H1 = {A-K2, A-K3} ∪ {B-F1, B-F2, B-F3, B-F4a, B-F6}`
   (all executed by M1x in the pinned matrix), with the injection attested. A cell is unusable if its attested-fire rate is < 80 % or its exclusion rate > 20 %.
2. **M1-kafka is clean.** Zero `outcome_clean_fail` runs in the **entire canonical executed set of M1-kafka**: Stage A {K1, K2} + control; Stage B {F0, F1, F2, F3}. Report exact CP upper bounds per cell and pooled. M1-kafka's n/a cells (Stage A K3–K7; Stage B F4a, F4b, F5, F6) are excluded with the reachability reasons and are not passes. **This arm is narrower than before the matrix amendment** (it previously included K5 and F6).
3. **Significance — one of the two pre-registered options, chosen by AC-3:**
   - **AC-3b (recommended):** the single confirmatory test is the **pooled stratified exact sign test** over `C1 = {A-K2, B-F1, B-F2, B-F3}`, i.e. aggregate the discordant pairs of the four cells, `b = Σ b_c` (M1x fails, M1-kafka clean), `c = Σ c_c` (reverse), and require `b > c` and the one-sided exact binomial p < 0.05. The four per-cell one-sided McNemar tests are reported descriptively (Holm-adjusted within the four cells). The cells, unweighted pairs and the direction "M1x violates more" are frozen in this paragraph.
   - **AC-3a:** require at least one of the four per-cell one-sided McNemar tests to be significant after Holm (m = 4).
   **Power arithmetic (plain):** under AC-3a Holm over four tests makes the smallest threshold α/4 = 0.0125; with zero reverse discordance a cell needs `b ≥ 7` one-direction discordant pairs (2⁻⁶ = 0.0156 > 0.0125 ≥ 2⁻⁷ = 0.0078). Under AC-3b the pooled test needs only `b ≥ 5` with `c = 0` at α = 0.05 (2⁻⁵ = 0.031 < 0.05), and it accumulates evidence across the four cells; it is the higher-power choice and is recommended because M1-kafka F6 is now n/a (the old C1's highest-frequency replay cell is no longer pairable).

**Deleted:** the old Stage A-K3 McNemar test (K3 is exposure evidence only; M1-kafka K3 is n/a).
**Refuted** iff clause 2 fails (any M1-kafka canonical `outcome_clean_fail` — this also fails gate G2 and halts the campaign) or M1x is clean in every cell of `D_H1` with attestation ≥ 80 % in each cell used and `N_valid ≥ 20`.
**Inconclusive** iff the evidence cells fail the quality bars (N_valid < 20; attested-fire < 80 %; exclusion > 20 %; > 20 % censored in a required cell) or no significance test is analysable (fewer than two cells with pairs under AC-3b; zero discordant pairs under AC-3a) — then the paper reports the observed violations without the paired significance claim.

### 1.4 H2 — M2 sufficient, with a measured cost

**Correctness component confirmed** iff M2 has zero `outcome_clean_fail` runs in **all M2 canonical cells**:
Stage A {K1, K2, K3, K5, K7} + control; Stage B {F0, F1, F2, F3, F4a, F5, F6} (core N=30). Report CP upper bounds per cell and pooled.
**Probe cells are excluded from this clause and reported separately** (M2's K4/F4b `early` rows are the H3 evidence class; the model predicts M2 to lose there by design). K6 is n/a for every mechanism (§0.2/§7.2), so it is neither evidence for nor against M2.

**Cost component confirmed** iff, in the pre-registered direction, M2 is worse than M3 **or** M4 on at least one primary performance outcome:
- throughput: `Cliff δ_better(M2, other) ≤ −0.33` (M2 worse) with Holm-adjusted one-sided p < 0.05 in family P1, on the 6 Stage C core blocks (block medians); or
- p99 latency: `δ_better(M2, other) ≤ −0.33` (M2 worse, higher p99) with Holm-adjusted one-sided p < 0.05 in family P2.
A metric/pair is assessable only with ≥ 4 of the 6 blocks present. `δ_better(x,y) > 0` means x is better (higher throughput / lower p99) — frozen sign convention, replacing the previous `abs(δ)` check. Stage C is unchanged by the matrix amendment, so H2's cost component is unaffected.

**Confirmed** iff both components; **partially confirmed** iff exactly one; **refuted** iff the correctness component fails with attested injections; **inconclusive** if the correctness cells are underpowered or the cost component is unassessable.

### 1.5 H3 — M3: zero duplicates, losses only under the ordering fault

**Confirmed** iff both arms hold:
- **Duplicate arm.** M3 has zero runs with `v3 = true` across **all** M3 executed cells in the core blocks (Stage A executed crash classes including the K4 probe; Stage B all eight executed fault conditions including F4b); per-cell CP upper bounds reported. An observed duplicate refutes the arm.
- **Loss arm.** In the **probe** class — Stage A `K4` with `ordering=early` (N=30) and Stage B `F4b` with `early` (N=30) — M3 has ≥ 1 **non-censored, valid** run with `v4 = true`, the injection attested, and `ordering_variant = early` recorded. Attested-fire ≥ 80 % and exclusion ≤ 20 % in the cell where the loss is observed. Existence is the claim; no p-value is possible or required.
**Refuted** iff the duplicate arm fails, or both probe cells meet the quality bars (N_valid ≥ 20 each; attested ≥ 80 %) and **zero** non-censored v4 observations are found across them.
**Inconclusive** iff probe cells miss the quality bars, the probe was not realised, or all probe runs are censored.
**Deleted:** old §3.3(iii) and family C2 (no M3-strict K4 control exists in the frozen matrix; adding one would be a matrix change and is not done). The K4 realisation in the fixed implementation is the `ordering=early` probe plus a `before_effect_commit` crash (DEVIATIONS S5-1), which preserves the intended probe semantics; F4b is the Level-2 early-ordering variant. Both M3 probe cells exist in the pinned matrix (N=30 each).

### 1.6 H4 — M4 (or M4+M3) sufficient under scripted faults; M4 faster than M2

H4's A1 wording is scoped to the scripted faults F1–F6. Corrected under the P2 matrix:

**Correctness component confirmed** iff:
- **M4:** zero `outcome_clean_fail` runs in Stage A {K1, K2, K5} + control and Stage B {F0, F1, F2, F3, F4a, F5, F6} (core N=30).
- **M4+M3:** zero `outcome_clean_fail` runs in the same sets (Stage A {K1, K2, K5} + control; Stage B the same seven canonical cells).
**The old K6 clauses are removed, not restated:** K6 is n/a for every mechanism because a zombie writer cannot be paused past `max.poll.interval.ms` = 300 s within `T_max` = 300 s (§7.2). M4's model-predicted zombie exposure (P9) is therefore a model-and-argument claim with no Stage A cell; the same holds for M4+M3's closure of that exposure. H4 neither confirms nor refutes those claims, and the paper must say so. Probe cells excluded as in H2; K3/K7 n/a for M4/M4+M3 (reachability).

**Latency component confirmed** iff, on the 6 Stage C core blocks (block medians), `δ_better(M4, M2) ≥ +0.33` on **p99 latency** (primary) with Holm-adjusted one-sided p < 0.05 in family P2, and M4's median p99 is lower than M2's in ≥ 4 of 6 blocks. p50 is secondary corroboration with the same sign rule. Stage C is unchanged by the matrix amendment.

**Confirmed** iff both components; **partially confirmed** iff one; **refuted** iff the correctness component fails; **inconclusive** if performance blocks are insufficient (cut ladder) or correctness cells are underpowered.

### 1.7 H5 — no single mechanism dominates (repaired dominance definition)

The frozen rule was vacuous: clause (i) required zero violations in *every* executed cell including probe cells, which the model predicts no two-commit mechanism can satisfy, so "no mechanism dominates" was confirmed by construction. Two repairs are possible; **AC-1** asks the author to choose.

**Recommended (H5-A — "residual-closure dominance").** A mechanism `x` dominates iff all three hold:
1. **Canonical-correct:** zero `outcome_clean_fail` runs in all of `x`'s canonical correctness cells (§1.2; the same sets used by H2/H4), with CP upper bounds.
2. **Performance non-inferior:** non-inferior to the best other mechanism in ≥ 4 of the 6 Stage C core blocks on **both** throughput and p99, where non-inferior means `|δ_better| < 0.147` (negligible band, as frozen) **and** the 95 % bootstrap CI of the median ratio contains 1.
3. **No declared residual exposure:** `residual(x) = ∅`, from the frozen P1-approved model (method §6.6/§11):

| Mechanism | Closed exposure classes | `residual(x)` |
|---|---|---|
| M0 | — | {E1, E3, E4, E5} |
| M1x | E2 (canonical strict ordering¹), E5 (Kafka transaction) | {E1, E3, E4} |
| M2 | E1, E2 (atomic DB progress), E3 (inbox), E4 | {E5} (relay publication is at-least-once) |
| M3 | E1 (index), E2 (strict ordering¹), E3 (index), E4 (index) | {E5} |
| M4 | E1, E2 (atomic progress), E4 | {E3 (C7, now model-only), E5} |
| M2+M3 | E1–E4 | {E5} |
| M4+M3 | E1–E4 | {E5} |
| M1x+M3 | E1–E5 | **∅** |

¹ The strict-ordering obligation is discharged by the mechanism's canonical configuration (D2); H3's probe measures the consequence of breaking it. **E3/C7 is now model-and-argument-only:** with K6 n/a for every mechanism, no experiment corroborates the zombie closure; the model itself does not include C7 (§7.2). The residual table is therefore a *declared model residual*, not a measured one, and the paper must say so. M1-kafka is excluded from the dominance domain (different sink; D13).

`residual(x) = ∅` only for **M1x+M3**. Hence H5-A is the falsifiable prediction: *the only complete-coverage mechanism is not also performance-non-inferior*; if M1x+M3 is canonical-correct and non-inferior on both metrics in ≥ 4 blocks, it dominates and **H5 is refuted**.

**Verdicts:** **Confirmed** iff no mechanism satisfies (1)–(3) **and** at least one mechanism satisfies (1) with adequate power (non-vacuity guard; otherwise inconclusive). **Refuted** iff some mechanism satisfies all three. **Inconclusive** iff no mechanism is canonical-clean with adequate power, or the required Stage C blocks are missing. The residual table is reported descriptively regardless of the verdict.

**Alternative (H5-B — "canonical frontier", for AC-1).** Drop clause (3); H5 is then about canonical correctness + performance only: confirmed iff no canonical-clean mechanism is performance-non-inferior; refuted iff one is; inconclusive iff none is canonical-clean. **With K6 now n/a, H5-B becomes materially more likely to be REFUTED:** M4 and M3 both lose their only pre-declared canonical correctness exposure (M4's K6) and may become canonical-clean, so if either is also fast, H5-B is refuted for a reason (no failure-class coverage) that clause (3) was designed to prevent. H5-A is therefore recommended.

### 1.8 Multiplicity families (final)

| Family | Tests | Correction | Used by |
|---|---|---|---|
| **C1** | under AC-3a: 4 one-sided per-cell McNemar tests (A-K2, B-F1, B-F2, B-F3) with Holm (m = 4); under AC-3b: **one** pooled stratified exact sign test over the same four cells (α = 0.05) plus the four per-cell tests reported descriptively (Holm) | as specified by AC-3 | H1(iii) |
| **C2** | **deleted** | — | — |
| **C3** | **deleted** | — | — |
| **C4** | no tests: exact CP bounds for all zero-violation claims (M2, M3, M4, M4+M3, combinations; H3 duplicate arm) | — | H2/H3/H4 |
| **P1** | throughput: Friedman + Nemenyi family-wise across the 8 DB mechanisms (M1-kafka excluded by D13), then pairwise Wilcoxon on 6 block medians with Holm (28 pairs) | Nemenyi gate + Holm | H2/H4/H5 cost |
| **P2** | p99 latency: same structure as P1 | Nemenyi gate + Holm | H2/H4/H5 cost |
| **P3** | time-to-consistent-state: 3 pre-registered contrasts (M2 vs M3, M4 vs M2, M4+M3 vs M2), pooled over the canonical fault cells of Stage B core (F1, F2, F3, F4a, F5, F6), Fisher exact two-sided, Holm (m = 3); per-cell rates descriptive | Holm | H2/H4 cost corroboration |
| **T1** | exploratory: everything else (divergence, publication integrity, 1M and capacity contrasts, sensitivity blocks, pilot/validation evidence, K6 non-realisability) | none; labelled exploratory | — |

α = 0.05. Both unadjusted and adjusted p reported; effect sizes reported regardless. H2/H4 performance contrasts are pre-declared members of P1/P2 and use their Holm-adjusted p; H5 uses the frozen non-inferiority mechanics.

### 1.9 Satisfiability on the P2 matrix (why each rule can fire)

- H1: M1x and M1-kafka both execute A-K2 and B-F1/F2/F3 in the pinned matrix, so the four C1 cells have up to 30 paired runs each; K3 is exposure evidence only (M1-kafka K3 n/a). Under AC-3b the pooled test over four cells can reach α with ≥ 5 one-direction discordant pairs (c = 0); under AC-3a a single cell needs ≥ 7.
- H2: M2's canonical sets are executed and contain no probe cells; K6 is n/a for all.
- H3: M3's K4/F4b probe cells exist with N=30 each; existence needs no comparator.
- H4: M4/M4+M3 canonical sets are exactly the executed strict cells; the K6 clauses are removed because K6 is n/a for every mechanism.
- H5: `residual(·) = ∅` only for M1x+M3, which executes the Stage C core blocks; the verdict depends on measured performance and can be refuted. H5-B's refutation risk is noted in §1.7.
- Deleted C2/C3 remove two tests that could never be significant (multiplicity shrinks; conservative).

---

## 2. A2-2 — Censoring rule (F-P5 item 1)

### 2.1 Normative basis

`docs/method.md` §7.3 already states: *"censored runs are included in the correctness primary outcome (they are violating runs)"*, and `prereg.md` §8.3 states censored runs are **not** re-run (they are outcomes). The fixed implementation now folds censoring into the correctness outcome in the analyzer; this item ratifies and completes that alignment. No new methodology.

### 2.2 Rule

1. A run is **censored** iff `quiescence.censored = true`. Censored runs are `valid` unless a §8.2 exclusion cause applies; never excluded for being censored, never re-run.
2. For every "clean" claim (H1(ii), H2, H3 duplicate arm, H4, H5 clause 1, C4 zero claims): `outcome_clean_fail(r) = violating_run(r) OR censored(r)`. Censoring counts against a clean claim (conservative direction).
3. For existence claims (H1(i), H3 loss arm): only an observed violation in a **non-censored** valid run is evidence.
4. Time-to-consistency (P3): censoring is the outcome itself (proportion consistent within `T_max`, CP CI); no change.
5. **No silent denominators.** Every correctness table reports per cell `N_planned`, `N_executable`, `N_scheduled`, `N_valid`, `N_excluded` (by reason), `N_censored`, `N_missing`, and both `violating_rate` and `violating_or_censored_rate`. Planned/executable numbers come from the matrix accounting block of §1.1 (pilot 216/198, A 1,530/1,530, B 3,960/3,960, C 1,800/1,800; 7,506/7,488/26/7,514) and the frozen reserved-run list; `N_planned` is never redefined as the number of surviving records.
6. **Fragility (AC-8):** a cell with `N_censored / N_valid > 20 %` is flagged; a hypothesis whose verdict depends on a flagged cell is reported as fragile (the verdict survives only if its direction is already conservative).
7. **Gates:** for fault-free positive controls (G2: M1-kafka and M4+M3 control runs), a censored run counts as a gate failure. The six new G1 gate-control runs are **validation** evidence and are excluded from confirmatory tables (they exist to prove the gates can fire).
8. The run record already carries `quiescence.censored`; the oracle and runner are unchanged by this item.

---

## 3. A2-3 — I1a/I1b and spurious verdict: ratification of the implementation + acceptance checks (F-O3, F-O4)

### 3.1 The contradiction and the resolution

The frozen normative I1 is D8's **I1 = I1a ∧ I1b**: I1a structural well-formedness (per event, exactly one D and one C posting, equal amount, one currency — `method.md` §2.3), I1b signed sum zero. Under that definition a whole-entry duplicate (`count = 4`) **does** violate I1a, so the explanatory table in `method.md` §2.8 ("whole entry duplicated → I1 passes") and P11's operational test (`v3=1 ∧ v1=0`) were wrong: under the approved I1, `v3=1 ⇒ v1=1` for every posting-level duplicate, and the test could never fire. The resolution keeps D8 unchanged and splits only the *reporting*:

- `v1a` (derived in analysis from stored counts) := `counts.structurally_malformed_entries > 0`;
- `v1b` (derived) := `counts.imbalanced_sum_entries > 0`;
- `v1` (stored, normative I1 flag) := `v1a ∨ v1b`.
- **P11 (restated, author-owned text):** *there exist runs in which I3 is violated while I1b holds — a duplicated balanced entry still sums to zero; therefore sum-to-zero reconciliation is blind to duplication, while the structural clause I1a detects it.* The pre-registered P11 test becomes `v3 = 1 ∧ v1b = 0`. The `method.md` §2.8 table row is corrected to: whole-entry duplication → I1a fails, I1b passes, I2 passes, I3 fails, I4 passes. The practitioner message is corrected to "sum-to-zero checks are blind; ledger-shape checks are not" (AC-6 offers the alternative of redefining I1 as pairability; not recommended).

### 3.2 Ratified schema and verdict semantics (implemented; to be verified, not added)

The fixed implementation now emits and the validator now enforces the following exact key sets (`experiments/oracle/oracle.py`, `experiments/validate_record.py`); this amendment ratifies them as the frozen schema and requires the acceptance checks of §3.4.

**`violations` (exact keys):** `{v1, v2, v3, v4, v5, violating_run}`, all booleans. Frozen equations:

```
violating_run  ==  v1 ∨ v2 ∨ v3 ∨ v4 ∨ v5
v1             ==  (v1a ∨ v1b)          [v1a/v1b derived from counts; not stored as separate booleans]
v1a            :=  counts.structurally_malformed_entries > 0
v1b            :=  counts.imbalanced_sum_entries > 0
v2 = mutated_postings>0 ; v3 = dup_postings>0 ; v4 = lost_postings>0 ; v5 = spurious_postings>0
```

**`counts` (exact keys):** `produced, consumed_estimate, expected_postings, observed_postings, dup_postings, dup_entries, lost_postings, lost_entries, imbalanced_entries, structurally_malformed_entries, imbalanced_sum_entries, ledger_entries_total, mutated_postings, spurious_postings`. `imbalanced_entries` is the I1-total count (events with structural or sum defect); the two split counters are the I1a/I1b components; `ledger_entries_total` is the duplicate-inclusive entry count used by the divergence metric.

### 3.3 Verdict semantics (no post-hoc freedom)

- Any of the five violation classes makes `violating_run = true` **as stored by the oracle**; the analysis reads `violations.violating_run` and never recomputes it.
- A **spurious** posting is a fabricated sink row (`π ∉ Ω`). It is a correctness violation (`v5`), **not** an invalid-run signal: a pipeline that fabricates balanced entries must be reported, not excluded. The previous `method.md` §2.4 wording ("harness-invalid if >0 unexpectedly") is withdrawn as the verdict rule and replaced by: spurious ⇒ `v5` ⇒ `violating_run`; attribution is `SPURIOUS`, never folded into `dup_postings`.
- Malformed/unparseable observed rows (F-O7) are counted as spurious postings (and set `v5`); they are never silently dropped.
- The G3 corpus now includes the duplicate-whole-entry class (the balanced X1 signature) and the **balanced fabricated entry** class (S5 F-O6 remediation), in addition to drop/delete/duplicate/sign-flip/update/currency-mix/negative-amount classes; 100 % detection is required, with attribution to the right class.

### 3.4 Acceptance checks (S4/S5, before Stage A)

1. Verify the validator's exact key sets and add/verify the **cross-field equations** `violating_run == v1∨v2∨v3∨v4∨v5` and `v1 == (structurally_malformed_entries>0 ∨ imbalanced_sum_entries>0)` (the key sets are already enforced; the equations must be enforced too, so a record cannot contain internally inconsistent flags).
2. Verify the oracle's counters `structurally_malformed_entries` and `imbalanced_sum_entries` satisfy their definitions on the G3 fixtures (whole-entry duplication: v1a=1, v1b=0; single-leg duplicate: v1a=1, v1b=1).
3. Verify the analysis derives `v1a`/`v1b` from counts and applies the restated P11 test `v3=1 ∧ v1b=0`.
4. Verify G3 reports 100 % detection for the balanced-fabricated-entry class with `SPURIOUS` attribution.
5. Correct `method.md` §2.8 and P11 wording at approval (author-owned text; AC-6 preserves D8).

---

## 4. A2-4 — Matrix arithmetic and per-stage accounting (F-P2 + P2 matrix amendment)

### 4.1 Corrected rows and the new matrix

The frozen §2.2 "executed fault cells" column listed `M4 | 8 (F4b n/a)` and `M4+M3 | 8 (F4b n/a)`; corrected values **7** and **7** (the stated total 67 was already correct for the pre-amendment matrix). Under the P2 matrix amendment (§0.2) the fault-cell total is **66** and the crash-cell total is **42**; §4.2 proves the new totals and §4.3 states the old→new delta.

### 4.2 Arithmetic proof of the new totals (pinned `matrix.yaml` accounting + run_order)

| Quantity | Recomposition |
|---|---|
| Stage A cells | 42 crash cells + 9 control cells (`fault=none`) = **51** cells; n/a crash cells = 63 − 42 = **21** (K6 n/a for all 9 mechanisms = 9; K3 n/a for M1-kafka/M4/M4+M3 = 3; K4 n/a for M1-kafka = 1; K5 n/a for M1-kafka = 1; K7 n/a for M0/M1-kafka/M1x/M3/M4/M4+M3/M1x+M3 = 7) |
| Stage A runs | 51 × 30 = **1,530** = (42 × 30 = 1,260 crash) + (9 × 30 = 270 control) |
| Pilot cells/slots | 72 planned cells − 6 n/a cells = 66 executable cells; n/a slots = 6 cells × 3 = **18**; planned 216, executable **198** |
| Stage B cells | 72 planned fault cells − 6 n/a = **66** (M1-kafka F4a, F4b, F5, F6; M4 F4b; M4+M3 F4b); 60 canonical + 6 F4b-probe |
| Stage B runs | 66 × 30 (core) + 66 × 10 (S1, 10k/P6) + 66 × 10 × 2 (S2, 100k/P3 and P12) = 1,980 + 660 + 1,320 = **3,960** |
| Stage C runs | 9 mechanisms × 2 loads × 3 partition counts × 30 = **1,620**; 1M at P6: 9 × 10 = **90**; saturation at 100k/P6: 9 × 10 = **90**; total **1,800** (unchanged) |
| Nominal totals | planned 216 + 1,530 + 3,960 + 1,800 = **7,506**; executable 198 + 1,530 + 3,960 + 1,800 = **7,488**; plus 26 validation = **7,514 scheduled rows** (pinned `run_order.csv`) |

### 4.3 Old→new delta (the matrix amendment)

| Stage | Planned old→new | Executable old→new | Δ | Reason |
|---|---|---|---|---|
| P | 216 → 216 | 201 → 198 | −3 | M1-kafka F6 n/a (1 cell × 3 reps) |
| A | 1,830 → 1,530 | 1,830 → 1,530 | −300 | K6 n/a × 9 mechanisms (−270); M1-kafka K5 n/a (−30) |
| B | 4,020 → 3,960 | 4,020 → 3,960 | −60 | M1-kafka F6 n/a (1 cell × 60 runs across core/S1/S2) |
| C | 1,800 → 1,800 | 1,800 → 1,800 | 0 | unchanged |
| Total | 7,866 → 7,506 | 7,851 → 7,488 | −360 / −363 | — |
| Validation | 20 → 26 | — | +6 | six G1 gate-control runs (g1k3 ×3, g1k5 ×3) |
| Scheduled rows | 7,871 → 7,514 | — | −357 | executable + validation |

Cell deltas: crash cells 52 → 42; fault cells 67 → 66; n/a crash cells 11 → 21; n/a fault cells 5 → 6; pilot n/a cells 5 → 6. **Stage C is untouched, so all performance components (H2 cost, H4 latency, H5 performance) are unaffected by the matrix amendment.**

### 4.4 Clerical clarifications

- "15-slot difference" in the frozen §2.8 was the *pilot* n/a accounting (5 cells × 3 reps); it is now **18 slots** (6 cells × 3 reps) because M1-kafka F6 became n/a. The sentence must not be read as n/a slots among Stages A–C.
- Stage A control cells now carry `fault=none` (run-id/cell-id token `ctrl`); the prereg §5 fault enum already admits `none`.

---

## 5. A2-5 — Seed-formula encoding (F-P3; unchanged by the matrix amendment)

### 5.1 Frozen formula (implemented; stated unambiguously)

The frozen prose writes `SHA-256(master_seed | "data" | stage | condition | rep)[0:16]`, leaving the separator, token encodings and slice unit ambiguous. The implementation (`matrix.py`) and `matrix.yaml` metadata use **direct ASCII concatenation without separators**. This amendment freezes that reading:

```
master_seed_text = "wxeo-campaign-v1|master|2026-11-24"
master_seed      = lower_hex( SHA-256( UTF-8(master_seed_text) ) )[0:32]        # 32 hex chars
data_seed        = lower_hex( SHA-256( UTF-8( master_seed + "data" + stage_token
                                              + condition_token + decimal(rep) ) ) )[0:16]
injection_seed   = lower_hex( SHA-256( UTF-8( master_seed + "inj"  + stage_token
                                              + condition_token + decimal(rep) ) ) )[0:16]
```

- `+` = direct concatenation of the ASCII byte strings; **no separator, no delimiter, no whitespace**.
- `master_seed` is 32 lowercase hex chars (the old §4.4 clarification wrongly said 16; corrected). `data_seed`/`injection_seed` are 16 lowercase hex chars (64 bits).
- `stage_token` ∈ {`pilot`, `a`, `b`, `c`}; `condition_token` = the lowercase condition token used in the run id/cell id (`f0`…`f6`, `k1`…`k7`, `ctrl` for matrix runs; `valid-<kind>-<fault token>` for the validation runs, e.g. `valid-g1k3-k3`, `valid-sham-f4a`); `decimal(rep)` = canonical decimal `0..N−1`, no sign, no leading zeros.
- `order_seed` and `bootstrap_seed` follow the same phrase rule (`wxeo-campaign-v1|order|2026-11-24`; `wxeo-campaign-v1|bootstrap|2026-11-24|B=10000`), as recorded in `docs/prereg-freeze.json`.
- The `|` characters inside the phrase constants are part of those fixed strings hashed once; they are **not** separators in the per-run formula.
- **Uniqueness:** `master_seed` is fixed-length 32; `kind` ∈ {`data`,`inj`}; `stage_token` and `condition_token` come from finite sets with distinct prefixes and are followed by a canonical decimal rep; the decomposition is unique for every scheduled run.

### 5.2 Regeneration decision (updated for the P2 matrix)

**No seed value is regenerated.** The P2 matrix amendment replaced the schedule (`run_order.csv` 7,871 → 7,514 rows; new hashes in the header) but **not the seed formula and not the seed constants**: `master_seed = 90650742b511adfb662272117444cf43`, `order_seed = 08e2a8d7…`, `bootstrap_seed = a6a7bdd0…` are unchanged, and every retained cell keeps the same `data_seed`/`injection_seed` (verified: pilot `m4m3-f4a-r02` retains `facff75d0b52e055`/`059307dd98035b92`). New validation runs (g1k3/g1k5) reproduce under the same formula (verified: `V.g1k3.M0.K3` → `d9e7e7082587cbb2` from condition token `valid-g1k3-k3`, rep 23). Cells removed by the matrix amendment retire their seeds; no science is affected. The amendment therefore requires only the corrected §4.4 text and a re-freeze of documentation/matrix hashes at the final freeze Z.

**Alternative (not recommended, AC-2):** implement literal `|` separators, which would regenerate every seed, `matrix.yaml`, `run_order.csv` and all freeze hashes for zero scientific benefit.

---

## 6. A2-6 — Analyzer ratification and remaining acceptance checks (F-P5 items 1–7)

No code in this amendment. The fixed implementation already contains part of the required behaviour (validation-run exclusion via `cell_id V.*`; frozen `N_planned` from the reserved-run list; `v5` in the per-invariant loop; censoring folded into `violating_run`; schema key sets in the validator). This section ratifies the target semantics and lists the **remaining acceptance checks**; the re-freeze records the final analyzer hash and S5 re-verifies against a synthetic fixture before Stage A.

1. **Outcome derivation.** `outcome_clean_fail = violating_run ∨ censored` (verify present); derive `v1a`/`v1b` from `counts.structurally_malformed_entries`/`imbalanced_sum_entries`; read `violations.violating_run` — never recompute it.
2. **Denominators.** Per cell emit `N_planned` (frozen reserved-run list), `N_executable`, `N_scheduled`, `N_valid`, `N_excluded` (by reason), `N_censored`, `N_missing`; k for clean claims uses `outcome_clean_fail`; report the uncorrected and corrected rates; per-invariant columns for `v1a`, `v1b`, `v3`, `v4`, `v5`. Planned/executable source: matrix accounting block (§1.1).
3. **Evidence-class classifier.** Frozen labels from the pinned matrix (§1.2), keyed by (stage, condition, load, partitions, ordering_variant); fail the analysis if the matrix hash differs from the freeze. Exclude pilot, validation (`cell_id V.*`, 26 runs including the six g1k3/g1k5 gate controls), sensitivity and n/a rows from confirmatory tables; keep them as separate datasets. Control cells carry `fault=none`.
4. **H1.** Remove the old Stage A-K3 McNemar test. Build `C1 = {A-K2, B-F1, B-F2, B-F3}` (omit B-F6: M1-kafka F6 is n/a): under AC-3b the confirmatory test is the pooled stratified exact sign test over the four cells plus descriptive per-cell Holm tests; under AC-3a the four per-cell one-sided McNemar tests with Holm (m = 4). Restrict the M1-kafka clean arm to A {K1,K2}+control and B {F0–F3}.
5. **H2.** Correctness over M2 canonical sets only (Stage A {K1,K2,K3,K5,K7}+control; Stage B 7 canonical); remove probe cells and the old fault catch-all. Cost: direction-aware one-sided tests with the frozen `δ_better` sign convention; require ≥ 4 blocks.
6. **H3.** Remove family C2 and `c2_sig`. Duplicate arm: `v3` over all M3 executed core cells. Loss arm: `v4` over M3 probe cells (K4/F4b with `ordering_variant=early`), non-censored valid runs, attested; implement the §1.5 quality bars.
7. **H4.** Correctness over M4/M4+M3 canonical sets (Stage A {K1,K2,K5}+control; Stage B 7 canonical); **remove all K6 references**; latency component uses the 6-block p99 test with the sign rule.
8. **H5.** Implement §1.7 H5-A (or H5-B if AC-1 chooses it): canonical sets per §1.2, non-inferiority mechanics, frozen residual table with the E3 model-only footnote, non-vacuity guard; report the frontier descriptively.
9. **Delete family C3; restrict P3** to the 3 pre-registered contrasts with per-cell descriptive reporting (no pooling of fault and fault-free runs).
10. **Direction-aware cost helper.** Replace `abs(δ) ≥ 0.33 and p < 0.05` with a sign-aware one-sided test in the pre-registered direction; report two-sided p as well.
11. **Validator equations.** Key sets are already enforced; add/verify the cross-field equations of §3.2 and the flag/count consistency rules; keep the exact-key-set rule.
12. **G3 acceptance.** Verify 100 % detection for the balanced-fabricated-entry class (already in the corpus) with `SPURIOUS` attribution; verify the duplicate-whole-entry class is attributed to I3 with v1a=1, v1b=0.
13. **Reporting tables.** Every hypothesis table reports evidence class, the seven N fields, exclusion and censoring rates, CP bounds, unadjusted and Holm-adjusted p, effect sizes; probe results in a separate table; n/a cells in a reachability table; the matrix hash and freeze hash in the caption.
14. **Acceptance fixture (S5 re-review before Stage A).** The analyzer reproduces the amended verdicts on a synthetic fixture containing: (i) an M1x-vs-M1-kafka divergence case under both AC-3 paths; (ii) a probe-only M3 loss case; (iii) a canonical M2 clean case with censored runs; (iv) a spurious-only case; (v) a duplicate case with v1a=1, v1b=0; (vi) a K6-absent matrix (no K6 rows) classified n/a, not missing. All must match the amended rules; no data-dependent code paths.

---

## 7. A2-7 — Formal-model scope statement and the forced C7 limitation (F-F1 + K6 non-realisability)

### 7.1 The claim the paper may make (frozen wording)

> "We encoded the crash-point semantics of the mechanism family in a bounded TLA+ model (`formal/ledger.tla`) and checked it with TLC 2.19 (tla2tools v1.7.4, SHA-256 `936A2620…0E88`) on Java 21 at the bounds N = 3 events, batch size 2 and one partition, for eight configurations of four mechanism booleans (de-duplication, atomic effect+progress, Kafka transaction, early-ordering probe). TLC checked the state invariants `TypeOK`, `I1Balance`, `I3Unique` and `I4CompleteWhenDone`; all eight configurations matched the expectations recorded before checking. The model is a **bounded witness of the protocol abstractions** — it shows that the two windows are reachable and that the closure conditions hold at those bounds — and **not an unbounded proof** of the propositions. The following are not modelled and are therefore not machine-checked: rebalancing and the concurrent (zombie) writer (C6/C7), append-only mutation (I2), liveness under fair recovery (I4 is checked as a state invariant at termination), publication-stream invariants, partial-batch durability (C11/C12) and multi-partition scheduling. The machine-checked claim covers only the W1/W2 closure argument for the abstract family. `StrictProgressBound` is a tautology of the encoding and is not counted as evidence; `StreamCompleteWhenDone` is defined but not listed in any TLC configuration and is not a checked result."

### 7.2 Forced new limitation: E3/C7 is not experimentally realisable

**Threat statement (paper threats-to-validity section, binding):** *The concurrent-writer (zombie) exposure E3/C7 is not experimentally realisable on the reported substrate. A consumer owner cannot be paused beyond `max.poll.interval.ms` = 300 s while remaining within the pre-registered quiescence bound `T_max` = 300 s, so a second writer of the same partition cannot be forced during a run; K6 is therefore n/a for every mechanism (matrix amendment §0.2), and the TLC model does not include C7 either. Consequently P9, exposure E3 and the M4+M3 rationale for E3 rest on the pen-and-paper argument and model reasoning only; the paper reports them as argued, not measured, and the decision table marks E3 as model-declared.*

Evidence sentence: "In the frozen matrix, K6 has no scheduled run for any mechanism (`matrix.yaml` accounting / `results/DEVIATIONS.md` S5-1: zombie writer not realisable), and the bounded model does not model C7; therefore no K6-based claim is empirical."

### 7.3 Evidence and reporting corrections

- Committed TLC results: `formal/tlc-results/tlc-summary.json`, checked 2026-11-24T12:51:13Z; distinct states **20, 18, 8, 8, 8, 39, 22, 21** (configs 01–08); violating configurations stop at the first counterexample, so their counts vary slightly with worker scheduling. Outcomes by invariant: 01 (naive) and 06 (txn-only) violate `I3Unique`; 07 (early probe) and 08 (early+dedup) violate `I4CompleteWhenDone`; 02–05 violate nothing. The paper/prereg must **cite the committed summary** (version, bounds, outcome by invariant name, timestamp) and must not hard-code a distinct-state sequence as a result.
- The stale sequences in `formal/README.md` ("20, 18, 8, 8, 8, 8, 21, 21") and in the drafted paper paragraph (36/21) are documentation defects to be corrected at approval; the model outcomes are unchanged.
- method §6.7 FO-6's promise that the model covers C7 is withdrawn: C3/C4/C5 (the windows) are modelled; C6/C7 are not (model or experiment). Extending the model with a two-writer zombie fragment (AC-7b) would formalise the argument but still cannot produce K6 evidence.
- Model↔mechanism mapping unchanged: M1-kafka = atomic+txn, M1x = txn only, M3/M2 = dedup (M2's atomic-ledger half; relay not modelled), M4 = atomic; the paper states this abstraction explicitly.

---

## 8. A2-8 — Impact table

| Item | Why needed | What changes in results / statistics | Risk if not approved |
|---|---|---|---|
| **Matrix amendment (§0.2)** | S5 realisability fixes changed what can run (K6 impossible; M1-kafka K5/F6 n/a); the matrix was regenerated | Per-stage N and totals: 7,866/7,851 → 7,506/7,488 (+26 validation, 7,514 scheduled) | The paper would report counts that do not match `results/DEVIATIONS.md`/`matrix.yaml`; hypotheses' denominators would be unreproducible |
| **A2-1** corrected H1–H5 | Rules unsatisfiable (H1 iii, H3 iii) or refuted/vacuous by construction (H2/H4 probes, H5) and now inconsistent with the amended cell sets | Verdicts become computable; C1 = 4 cells (AC-3a/b); C2/C3 deleted; P3 restricted; H5 replaced (AC-1); H4 K6 clauses removed | Hypotheses cannot be reported as designed; "confirmed/inconclusive" outcomes would be artifacts |
| **A2-2** censoring | Method §7.3 already requires censored = violating; implementation now folds it | Correctness rates conservative; denominators fully reported; censored controls fail G2 | Censoring could masquerade as clean data |
| **A2-3** I1a/I1b + spurious (ratification) | P11 was unfalsifiable under normative I1; the implemented `v5`/split counters were unratified | Schema ratified (`v5`, split counts, `ledger_entries_total`); P11 test `v3=1 ∧ v1b=0`; validator equations | P11 "falsified by construction"; fabrication can be scored clean; live records fail validation |
| **A2-4** matrix arithmetic | Two row cells wrong; P2 amendment needs accounting | Documentation + old→new per-stage accounting; totals proven from pinned accounting/run_order | Reviewers cannot reconcile the matrix |
| **A2-5** seed encoding | Frozen prose ambiguous; schedule regenerated | Formula frozen; no seed values change; matrix/run_order hashes re-pinned | Pre-registration-vs-implementation mismatch; reproducibility audit fails |
| **A2-6** analyzer ratification | Frozen analyzer still contains defective rules (C1-K3, C2/C3, H4 K6, H5, P3 pooling) though partly updated | Test names/families/denominators/filters finalized | Approved rules would not be executed by the frozen analysis |
| **A2-7** TLC scope + C7 limitation | Claim promised C7 coverage that neither model nor experiment has | Claim narrowed; E3/P9/K6 evidence attributed to argument/model only; stale counts corrected | Overclaiming measured zombie evidence; adversarial review would find it |
| **A2-8** this table | Shows blast radius per item | — | Uninformed approval of a package that changes five hypothesis verdicts and the reported N |

---

## 9. Provenance, recomputations, and cross-check of S5 findings

### 9.1 Independent recomputations at this revision (all read-only, reproducible)

| Check | Result |
|---|---|
| Pinned hashes | `matrix.yaml` `4c1051f7…`, `run_order.csv` `4698eace…` (matrix.sha256); freeze snapshot 2026-11-24T13:32:43Z, git `28d02aa…` |
| Scheduled rows | **7,514** = pilot 224 (198 matrix + 26 validation) + A 1,530 + B 3,960 + C 1,800 ✔ |
| matrix.yaml accounting | pilot cells 72/66/6 n/a, runs 216/198; A cells 51/51/0, runs 1,530; B cells 264/264/0, runs 3,960; C cells 72/72/0, runs 1,800; totals 7,506 planned / 7,488 executable / 26 validation / 7,514 ✔ |
| Stage A sets | executed crash cells per mechanism: M0 5, M1-kafka 2, M1x 5, M2 6, M3 5, M4 4, M2+M3 6, M4+M3 4, M1x+M3 5 = **42**; probe K4 = 8 mechanisms; controls = 9 (`fault=none`) ✔ |
| Stage B sets | executed fault cells: 8, 4, 8, 8, 8, 7, 8, 7, 8 = **66**; F4b probe = 6 mechanisms; blocks 1,980 + 660 + 660 + 660 ✔ |
| Validation composition | 26 = sham 6 + selftest 6 + c1 2 + quiesce 2 + oracle 4 + g1k3 3 + g1k5 3 ✔ |
| Seed stability | master/order/bootstrap constants unchanged; retained cell `pilot-m4m3-f4a-r02` retains `facff75d0b52e055`/`059307dd98035b92`; `V.g1k3.M0.K3` reproduces `d9e7e7082587cbb2` with condition token `valid-g1k3-k3`, rep 23 ✔ |
| TLC summary | checked 2026-11-24T12:51:13Z; distinct states 20,18,8,8,8,39,22,21; outcomes as in §7.3 ✔ |
| Validator/oracle state | validator enforces exact `violations` = {v1,v2,v3,v4,v5,violating_run} and the 14-key `counts`; oracle emits v5 and the I1 split counters; G3 corpus includes whole-entry duplication and balanced fabricated entry ✔ |

*The pre-amendment recomputations in the previous A2 revision (7,871 rows, 1,830/4,020, 20 validation, 52/67 cells) are superseded by the table above; they remain in the DEVIATIONS register as the old accounting.*

### 9.2 Cross-check of the S5 memo

Confirmed substantively: F-P1(a)–(d), F-P2, F-P3, F-P5 items 1–7, F-O3, F-O4, F-F1 (including the stale README sequence and the `StrictProgressBound` tautology). Two specifics are inaccurate and should not be copied into the paper:

1. **F-P1(a)'s suggested feasible C1 family includes `F4a`** ("Stage-B F1/F2/F4a/F6"). F4a is **n/a for M1-kafka** (prereg §2.2; method §6.3 J10; `run_order.csv`: F4a is executed for 8 mechanisms, not M1-kafka), so it cannot be a paired M1x-vs-M1-kafka contrast. The memo's own quote of the frozen §3.6 list (F0,F1,F2,F3,F6) is correct; the post-amendment set is F1,F2,F3,F6 minus F6 (now n/a) = F1,F2,F3, plus A-K2.
2. **F-F1's quoted distinct-state sequence** ("20, 18, 8, 8, 8, 8, 22, 21 (07 = 22, 06 = 39)") mis-transcribes the committed summary. The actual sequence is 20, 18, 8, 8, 8, **39**, **22**, 21 (06 = 39, 07 = 22). The substantive finding (stale hard-coded lists; counts vary for violating configs) stands; use the summary, not either hard-coded list.

---

## 10. Requested author decision

**Approval sheet** (the requested decision is stated in one paragraph at the end of this section).

| Approval | Item | Recommended option |
|---|---|---|
| ☐ | **Matrix amendment at P2 (§0.2)** | approve: P 216/201→216/198; A 1,830→1,530; B 4,020→3,960; C 1,800; totals 7,866/7,851→7,506/7,488; validation 20→26; rows 7,871→7,514 (K6 non-realisability, M1-kafka K5/F6 n/a, six G1 gate-control runs) |
| ☐ | A2-1 corrected H1–H5 | approve as drafted (H5-A = AC-1 recommended) |
| ☐ | A2-2 censoring rule | approve |
| ☐ | A2-3 I1a/I1b + spurious (ratification) | approve (AC-6 split reporting; AC-5 v5 = violation) |
| ☐ | A2-4 matrix arithmetic + accounting | approve |
| ☐ | A2-5 seed encoding | approve (AC-2 no regeneration) |
| ☐ | A2-6 analyzer ratification + checks | approve |
| ☐ | A2-7 TLC scope + C7 limitation | approve (AC-7a narrow claim + forced limitation) |
| ☐ | AC-1 H5 definition | H5-A ☐ / H5-B ☐ |
| ☐ | AC-3 H1 significance | AC-3b pooled stratified sign test (recommended) ☐ / AC-3a four per-cell Holm tests ☐ |
| ☐ | AC-4 H2 cost semantics | at least one of M3/M4 ☐ / both ☐ |
| ☐ | AC-8 censoring fragility | > 20 % flag ☐ / other: ______ |

| Role | Name | Decision | Date (UTC) |
|---|---|---|---|
| Author (owner of the formal model and proofs) | João Brito | approve / revise | |
| Lead (integration, freeze) | | acknowledge and apply | |
| Methodologist (S2) | | submitted | 2026-11-24 |

**Requested decision (one paragraph).** The author is asked to **approve all eight items of Amendment A2 as drafted, the P2 matrix amendment in §0.2, and the recommended options for AC-1…AC-8** at the P2 gate (AC-3b pooled stratified sign test, AC-1 H5-A residual-closure dominance, AC-2 no seed regeneration, AC-4 at least one of M3/M4, AC-5 `v5` = violation, AC-6 split I1a/I1b reporting, AC-7a narrowed TLC claim plus the forced C7 limitation, AC-8 >20 % censoring flag). A2-1, A2-2, A2-3, A2-4, A2-6 and A2-7 are corrections/ratifications of pre-registered defects and implemented-but-unratified semantics — frozen rules the frozen matrix cannot satisfy, an analysis that contradicted the already-normative censoring rule, an invariant/proposition contradiction that made P11 unfalsifiable, a stale arithmetic transcription, an analyzer that still implements defective rules in places, and a machine-checked claim broader than the model. The matrix amendment in §0.2 is the already-executed re-scoping forced by S5's realisability findings (K6 cannot be realised within `T_max`, M1-kafka K5/F6 are n/a, six G1 gate-control validation runs were added); it changes counts and two reachability declarations but not the mechanisms, factors, loads, partitions, repetitions, Stage C, metrics or cut ladder, and it makes the forced new limitation explicit — **E3/C7 is model-and-argument-only**, because an owner cannot be paused past `max.poll.interval.ms` = 300 s within `T_max` = 300 s, so P9/E3/K6 have no experimental evidence (carried to the paper's threats section). No seed values change and no further matrix regeneration is implied; on approval the lead applies the listed text edits, completes the acceptance checks of §3.4/§6, re-freezes at the final matrix hashes and re-validates the derivation, and S5 re-reviews before Stage A. If any item is rejected, the affected hypotheses are reported "inconclusive", the affected claims "untested", and the matrix amendment must be reported as a deviation rather than silently reversed.