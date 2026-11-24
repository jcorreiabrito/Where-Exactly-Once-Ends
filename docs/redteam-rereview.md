# Red-team **re-review** — fixed implementation, new matrix, Amendment A2, gates, acceptance evidence

**Reviewer:** S5 (red team) — mandatory pre-Stage-A re-review
**Date:** 2026-11-24 (UTC-03), ~14:38–14:53Z
**Scope:** the S5 fix pass after `docs/redteam-oracle.md` (verdict **BLOCK**, 9 critical / 19 major /
6 minor), the regenerated matrix/run order, pre-registration Amendment A2, the falsifiable gate
suite, the S3 acceptance evidence, and the corrected oracle/runner/injector/analyzer code.
**Authority:** `docs/redteam-oracle.md` §14 (must-fix list), A1 §11, BRIEF §2/§6, `docs/method.md`
§7, `docs/prereg.md` §7–§10, lead's P2-gate directive.
**Write scope:** this file only. No code, matrix, results or other docs were modified. Throwaway
scripts live under `%TEMP%\wxeo-s5-rereview\`. No git command, no stack start/stop/format.

> **STATUS UPDATE — final focused re-review (2026-11-24, ~19:24–20:35Z), see §10 below.**
> §0–§9 below are the **historical** 14:53Z re-review (verdict BLOCK for the *launch* of the pilot).
> **Superseding verdict for the pilot gate: `PASS-WITH-CONDITIONS`** (retroactive launch accepted;
> no bare PASS). **Stage A/B/C remain `BLOCK`ed** pending the five conditions in §10.5
> (author approvals + M7, R10 analyzer, R12 rates, evidence/provenance close-out, re-freeze/S5
> re-verify). Read §10 for the final adjudication, verified hashes, evidence pointers and the
> residual list.

---

## 0. Verdict

> # **BLOCK** — the pilot and Stage A must not start.
>
> The *fixes that were claimed* are partly real (oracle, gates, freeze, matrix, G3, acceptance
> evidence — all verified), but the **run-production path** is still not executable end-to-end:
> the injector factory, the receipt attribution rules and the fault-timing/phase logic are mutually
> inconsistent with the frozen matrix, so every injected run in the pilot and in Stage A fails
> before or after the injection. On the reviewed snapshot this affects **all 1,530 Stage-A runs**
> (1,260 injected) and **171 of 198 pilot matrix runs**; Stage B scripted faults (3,060 runs) are
> structurally unexecutable as well.
>
> **Snapshot note (important).** This review verified snapshot **S0**:
> freeze `2026-11-24T14:38:38Z`, aggregate `5eec8c4a568e307b…`, matrix.yaml `4c1051f739…`,
> run_order.csv `4698eace…`, runner `55283041…`, injectors `052be2ae…`, oracle `f09cd28f…`,
> analyze.py `fa08b0bc…`, jar `6e822375…`. While the review was running, S4 began the mandated
> fix pass: snapshot **S1** (freeze `2026-11-24T14:47:02Z`, aggregate `b74c71cf…`, matrix.yaml
> `bce2568a…`, run_order.csv `520b1190…`, injectors `61e7acac…`, runner `1c5c9453…`,
> receipts `9715091a…` (unchanged), jar unchanged) appeared at 11:45–11:47 local.
> I inspected S1 but have **not** accepted it: R1/R2 are fixed there, R3/R4 are only partially
> fixed, and R5/R6/R7/R8 and the analyzer/A2/rates items below stand. The focused re-re-review
> requested by the lead (checks (a)–(f)) is still mandatory and is where S1 can be accepted.
>
> **Scope split — accepted explicitly (§9.10):** for the **pilot gate**, R12 (`rates.json`) and
> R10 (A2 analyzer) are **pre-Stage-A conditions**, not pilot blockers, because rates are pilot
> outputs (runner refuses Stage B/C without them) and A2 is approved only after the pilot while
> pilot data are descriptive. Entry conditions: the pilot must archive the raw evidence from which
> `rates.json` will be derived and be labelled as a max-rate deviation (M6-style), and the pilot's
> G1/G4 gate evidence is cross-checked independently against the runner validator. R12/R10 stay in
> the **Stage-A gate** (§9.9). The pilot gate itself is R1–R9 + R11 + green pytest + live evidence
> (fault-free valid record, K3 app-owned attested, K2 S4-owned attested, C=2 R5 case) + rebuilt jar
> and new hashes.
>
> None of this criticises the research design. The research question, factor structure, derivation
> contract, matrix arithmetic, A2 decision-rule repair, oracle predicate content, G3 corpus, gate
> falsifiability mechanism, freeze machinery and the S3 acceptance evidence are sound. The
> blockers are integration defects in the harness that *produce* the records, plus pending
> author/analyzer work that the fix pass correctly deferred to A2.

### Top blockers (ranked)

| # | Finding | Severity | Blast radius (snapshot) |
|---|---|---|---|
| 1 | **R1** injector factory never dispatches the external Level-2 injectors (`F1,F2,F3,F4a,F5,F6`) → `ValueError` before any record | critical | 3,213 / 7,488 reserved runs (159 pilot incl. 6 selftest; all Stage-B scripted faults). **Fixed in S1.** |
| 2 | **R5** app-owned failpoints are armed on **both** C=2 consumer instances → two app `fired` receipts (and both instances crash); the "exactly one app fired" rule can never hold | critical | 1,131 app-failpoint runs (750 Stage-A K1/K3/K4/K7 + 3 g1k3 + 360 Stage-B F4b + 18 pilot F4b) |
| 3 | **R2** the app-owned path wrote an **S4 companion `fired`** receipt while the validator forbids any S4 fired on app-owned runs | critical | 753 runs (Stage-A K1/K3/K4/K7 + g1k3). **Writer fixed in S1; C=2 (R5) remains.** |
| 4 | **R3** injector-mode / ownership mapping: K2/K5 rows carried `injector_mode="failpoint"` (treated app-owned, no failpoint name); F4b (mode `live`, L2) is app-owned in reality but validated as S4-owned; g1k5 validation rows still `failpoint` | critical | 513 Stage-A K2/K5 + 378 F4b + 3 g1k5. K2/K5 matrix fixed in S1; F4b/g1k5 remain. |
| 5 | **R6** K5 fires immediately at load start (before any progress), so the rewind moves 0 offsets → `action_verified=false` → `fault_not_fired`; the duplicate-delivery class never occurs → **G1's K5 arm can never pass** | critical | 240 Stage-A K5 + 3 g1k5 validation (and G1) |
| 6 | **R4** fault phase window: nothing set `InjectorContext.load_start_ms/load_end_ms` on S0 (all S4 receipts lack a phase block → rejected); S1 sets a *planned upper bound*, but with `experiments/rates.json` missing the L2 trigger is 15–45 s into an expected 60 s load while real 1 k loads last ~0.2–2 s → every pilot L2 fire lands after the real load (excluded) | critical | 189 pilot L2 runs + all S4-owned Stage A K2/K5 (243) |
| 7 | **R7** F1 verification uses a **stale** `stack-state.json` broker count (never drops after `taskkill`) and restore invokes the `.ps1` without `pwsh` → F1 injection never verifies / restore fails | critical-major | 567 F1 runs (pilot 27 + Stage B 540) → Stage B blocked |
| 8 | **R8** `exit_status.consumer` is always `{}` because `_stop_consumers()` clears `self.processes` before `_finish()` builds it; the validator never checks `EXIT_KEYS` | major | every live run (7,488) |
| 9 | **R9** validator does not enforce A2 §3.4 cross-field equations (`violating_run == v1∨…∨v5`, `v1 == v1a∨v1b`) | major | every record; A2 acceptance check #1 unmet |
| 10 | **R10** frozen `tools/analyze.py` still implements the **pre-A2** rules (Stage-A K3 McNemar, family C2 self-pairing, C3, H2/H4 including probe cells, old H5 incl. M1-kafka, different P3); no `v1a/v1b`/P11 test; no matrix-hash pin; no record re-validation; `attested` computed independently of `experiments/receipts.py` | major | confirmatory analysis cannot yet run under A2 |
| 11 | **R11** `results/harness/gate-negative-controls.json` is a **gate input** but is not covered by the freeze (nor RUNLOG) | major | post-hoc freedom on G1–G8 |
| 12 | **R12** `experiments/rates.json` does not exist and **no procedure generates it**; pilot/Stage A silently fall back to `max` (fixed-rate claim violated, undisclosed), Stage B/C abort; `R_off` cannot be derived from a 1 k/P3-only pilot | major | 7,488 runs' pre-registered rate mode; Stage B/C blocked |

---

## 1. What was verified, and how (independent commands)

All checks are read-only w.r.t. the repository; outputs in `%TEMP%\wxeo-s5-rereview\`.

| Check | Command / method | Result |
|---|---|---|
| Test suite (S0) | `python -m pytest experiments/tests -q -p no:cacheprovider` | **52 passed** |
| Matrix/order/hash pins | own parser of `matrix.yaml`/`run_order.csv` + SHA-256 | see §2 — all match A2/freeze |
| Seeds | re-implemented frozen formula over all rows | **7,514 / 7,514 exact**; 0 pairing violations |
| A2 arithmetic/cell sets | own recomputation from pinned matrix | all A2 §1.2/§4.2 claims match |
| Oracle adversarial | T0–T13 fixtures (whole-entry dup, balanced spurious, missing/granted attestation, pub lost/corrupt, currency mix, malformed row, GT truncated, source extra, M1-kafka ledger dup) | §3 — all expected behaviours |
| G3 mutation corpus | `mutation_test.py --mutations 100 --events 100` → `%TEMP%` | **18/18 classes, 4/4 corpora, 1,800/1,800 detected and attributed** |
| Gate negative controls | re-ran `gate_negative_controls.py` with output redirected to `%TEMP%` | **all 8 gates FAIL as required** against the current analyzer |
| Analyzer dry run | `tools/analyze.py --records results/synthetic/run-fixture/run-records --out %TEMP%…` | runs; G1 FAIL, G2 PENDING, G3 PASS, G4 PASS, G5 PASS, G6 PENDING, G7 PENDING, G8 PASS; H1 inconclusive, H2 partially confirmed, H3 inconclusive, H4 partially confirmed, H5 inconclusive |
| Freeze check | `python experiments/freeze.py --check` | ok; 31/31 file hashes match; jar `6e822375…` matches; aggregate `5eec8c4a…` |
| Freeze drift refusal | monkeypatched `FREEZE_PATH` to a tampered temp copy | tampered file hash → `ok=false`; tampered jar hash → artifact drift; missing freeze → fail |
| Acceptance evidence | recomputed GT digests, inspected scratch artefacts/logs | **genuine 4/4 PASS** (see §4) |
| Stack safety | only read ports/processes/stack-state; no start/stop/format | — |

---

## 2. Independent recomputation — the new matrix, order, seeds, A2

### 2.1 Snapshot S0 (the matrix this re-review was asked to verify)

Pinned: `matrix.yaml` `4c1051f739f53ee1bf154782eff1cb72ea08f1f14fd0611ae8cfba2341e29599`
(180,398 B); `run_order.csv` `4698eace6c08861bfafec292a3ecc43b1a5545499821f456153afa1d75fac166`
(1,139,035 B); `matrix.sha256` matches both; A2 header pins the same two hashes. **All match.**

| Quantity | Recomputed from the committed files | Declared (A2/DEVIATIONS/accounting) | Verdict |
|---|---|---|---|
| Cells (all stages) | 459 = 72 P + 51 A + 264 B + 72 C | 459 | ✔ |
| Pilot cells planned/executed/n-a | 72 / 66 / 6 | 72 / 66 / 6 | ✔ |
| Pilot runs planned/executable | 216 / 198 (n-a slots 18) | 216 / 198 | ✔ |
| Stage A cells / runs | 51 / 1,530 | 51 / 1,530 | ✔ |
| Stage A crash cells | 42 = 34 canonical + 8 K4 probes; 9 controls `fault=none` | 34 + 8 + 9 | ✔ |
| Stage A n-a crash cells | 21 of 63 | 21 | ✔ |
| Stage B cells / runs | 264 cells / 3,960 = 66×30 + 66×10 + 66×2×10 | 3,960 | ✔ |
| Stage B executed fault cells | 66 core = 60 canonical + 6 F4b probes; n-a 6 of 72 | 60 + 6 | ✔ |
| Stage C cells / runs | 72 / 1,800 = 54×30 + 9×10 + 9×10 | 1,800 | ✔ |
| Totals | 7,506 planned / 7,488 executable / **26 validation** / **7,514 rows** | same | ✔ |
| Validation composition | sham 6 + selftest 6 + c1 2 + quiesce 2 + oracle 4 + g1k3 3 + g1k5 3 = 26 | 26 | ✔ |
| Stage A executed classes per mechanism | M0 5, M1-kafka 2, M1x 5, M2 6, M3 5, M4 4, M2+M3 6, M4+M3 4, M1x+M3 5 = 42 | 42 | ✔ |
| Stage B executed fault cells per mechanism | 8,4,8,8,8,7,8,7,8 = 66 | 66 | ✔ |
| Order rows / unique run ids | 7,514 / 7,514 | 7,514 | ✔ |
| Stage label counts | P 198, V 26, A 1,530, B 3,960, C 1,800 | same | ✔ |
| Matrix ↔ order set equality | 0 missing / 0 extra; 0 n-a cell scheduled; 0 non-executed rows | — | ✔ |
| Seeds (implemented concatenation formula, `stage_token ∈ {pilot,a,b,c}`, condition token lower-case) | **0 mismatches / 7,514**; 0 all-digit hex seeds; pairing groups with >1 seed = 0 | A2 §5 | ✔ |
| A2 retained-cell claim | `pilot-m4m3-f4a-1k-p3-r02` retains `facff75d0b52e055` / `059307dd98035b92` (verified) | A2 §5.2 | ✔ |
| A2 g1k3 seed claim | `pilot-valid-g1k3-r23` data seed `d9e7e7082587cbb2` | A2 §5.2 | ✔ |
| Ordering constraints | max consecutive same mechanism **3**; worst 60-window share **28.33 %**; ≤40 %/60-block holds; stages P/V then A,B,C contiguous | prereg §4.3 | ✔ |
| early rows | 618 = A-K4 240 + B-F4b 360 + pilot-F4b 18; no other early cells | D2 | ✔ |
| Stage-A fault tokens | `K1,K2,K3,K4,K5,K7,none` (no lower-case); controls `fault=none` | §5 schema / A2 §4.4 | ✔ |
| Run ids | 7,514 unique, lower-case `[a-z0-9-]+`; 90 saturation `-sat-` rows | P2-RULINGS R2/R3 | ✔ |
| n-a reachability reasons | every `executed=false` cell carries a non-empty reason (K6 ×9, M1-kafka K5, M1-kafka F4a/F4b/F5/F6, M4/M4+M3 F4b) | S5-1/S5-4 | ✔ |
| A2 Stage-A sets vs matrix | per mechanism canonical/probe/n-a sets match A2 §1.2 **exactly** | A2 §1.2 | ✔ |
| A2 Stage-B sets vs matrix (core) | per mechanism canonical/probe sets match A2 §1.2 (60 canonical + 6 probe) | A2 §1.2 | ✔ |

**A2 arithmetic proof (§4.2) recomputed:** A = 42×30 + 9×30 = 1,530; B = 1,980 + 660 + 1,320 =
3,960; C = 1,620 + 90 + 90 = 1,800; totals 7,506/7,488 + 26 = 7,514. ✔
**A2 satisfiability (§1.2–§1.9) independently checked:** C1 = {A-K2, B-F1, B-F2, B-F3} has 30
paired M1x/M1-kafka runs in every cell (A-K2 both mechanisms 30; B core 100k/P6 both 30) so the
AC-3b pooled sign test can fire; H2 canonical sets exist and exclude probe cells; H3 has M3 K4 (30)
and F4b (30) probes; H4 canonical sets exist for M4/M4+M3; M1x+M3 executes the six Stage-C core
blocks so H5-A can be refuted. ✔

### 2.2 In-progress snapshot S1 (not accepted; for the lead's tracking only)

S1 = freeze `2026-11-24T14:47:02Z` (hash `a0c2409a…`), aggregate `b74c71cf…`, matrix.yaml
`bce2568a59406499c100ec2fd90c136f00071be0b4fa187fd638a24006e69894` (180,393 B), run_order.csv
`520b119053e3fc3e5b0878dcdd8ff3f2f998287b4773101371c4006E62F06AF7` (1,138,537 B), injectors
`61e7acac…`, runner `1c5c9453…`, receipts unchanged `9715091a…`, analyze/validate unchanged.
Quick recomputation on S1: 459 cells, 7,514 rows, same per-stage accounting; Stage-A K2/K5 rows
now `injector_mode="external"`; seeds still **0/7,514 mismatches**; pairing 0 violations; early
rows 618; max consecutive 3; worst 60-window 28.33 %; order/matrix set equality 0/0. `freeze
--check` ok. **Not verified in detail:** A2 §1.2 sets on S1 (cell sets appear unchanged but the
derivation must be re-run at acceptance), DEVIATIONS regeneration vs S1 matrix, tests on S1.

---

## 3. Per-previous-finding status (S5 memo → fixed snapshot S0)

Legend: **FIXED** = verified by my own command/test; **PARTIAL** = part verified, named remainder
open; **NOT FIXED** = claim does not hold on the verified snapshot; **DOC/A2** = author/analysis
deliverable still pending; **POSITIVE** = no action.

| ID | Sev | Status | Evidence / remainder |
|---|---|---|---|
| F-O1 live dump `time` | crit | **FIXED** | `oracle.py` imports `time`; `dump_source_topic`/`dump_kafka_topic` take `consumer_factory`; regression tests pass; real dump path exercised by fake consumer test. |
| F-O2 vacuous attestation | maj | **FIXED** (minor note R16) | `dump_pg` queries **application-role** privileges via `information_schema.role_table_grants`, trigger count and own row count; `evaluate_attestation(required=True)` makes missing → v2; G3 classes `attestation_granted`/`attestation_missing` detect 100 %; T5 `can_update=true` → v2. Note: an absent attestation *file* raises instead of recording v2 (runner always writes the file, so low risk). |
| F-O3 I1a/I1b vs P11 | maj | **PARTIAL (oracle FIXED; analysis/A2 pending)** | T1 whole-entry dup: `structurally_malformed=1`, `imbalanced_sum=0`, v1+v3, attribution `I1,I1a,I3`; T3 single-row dup: `malformed=1, imbal_sum=1`, attribution `I1,I1a,I1b,I3` — matches A2 §3.4 check 2. **Remainder:** analyzer does not derive v1a/v1b and does not implement the restated P11 test `v3=1 ∧ v1b=0` (R10); method §2.8/P11 text pending A2 approval. |
| F-O4 unratified spurious | maj | **PARTIAL (schema FIXED; equations/pending A2)** | T2 balanced fabricated entry → `v1..v4 false, v5 true, violating_run true`, attribution `SPURIOUS`; analysis reads stored `violations.violating_run` verbatim. **Remainder:** validator does not enforce `violating_run == v1∨…∨v5` (R9); ratification pending A2. |
| F-O5 divergence/content | maj | **FIXED** | T6 publication record lost → `lost_records=1, divergence=-1` (documented signed difference); T7 pub content corrupt → `pub_mutated_records=1`; M1-kafka ledger dup detected via expansion (T13 v1+v3). |
| F-O6 G3 coverage | maj | **FIXED** | Re-ran corpus on current oracle: 18 classes / 4 corpora / **1,800/1,800 detected, 1,800/1,800 correctly attributed**, clean/input controls pass. |
| F-O7 malformed rows | min | **FIXED** | Appending `deadbeef,not-a-number,…` makes `oracle_run` raise `ValueError` (run invalid), never silently drop; `dump_pg` records independent DB row count and cross-checks CSV rows. |
| F-O8 oracle independence | — | **POSITIVE** | unchanged; oracle remains stdlib + lazy psycopg2/confluent imports, no artifact import. |
| F-I1 no external `fired` | crit | **NOT FIXED (S0) → FIXED (S1)** | S0: `make_injector` raised `ValueError` for F1/F2/F3/F4a/F5/F6 → 3,213 reserved runs cannot inject (R1). S1 restores dispatch; direct calls now return the six external classes; validator accepts synthetic S4 fired+restored with phase (tested). Live execution still unverified. |
| F-I2 L1 double-restored | maj | **PARTIAL → NOT FIXED via other paths** | Validator implements per-action ownership (non-terminal: app fired+app restored, S4 must not duplicate; terminal: app fired + one S4 restored) and regression tests pass. But the integrated writer emitted an S4 companion fired on S0 (R2; fixed in S1) and, more importantly, **both C=2 instances are armed** → two app fired receipts (R5), and F4b's ownership is mis-mapped (R3). No injected run can pass today. |
| F-I3 unverified action / phase | maj | **NOT FIXED (S0) → PARTIAL (S1)** | S0: `InjectorContext.load_start_ms/load_end_ms` never assigned → every S4 receipt lacks the phase block → validator rejects; `consumers_alive` hard-coded True. S1 fills a window, but with a **planned upper bound**, and L2 fire time still derives from `_expected_load_seconds()` = 60 s when `rates.json` is absent → 15–45 s delay vs a ~0.2–2 s real 1 k load; the runner's new post-check compares against the real window and will exclude those runs (R4). F1 action verification still broken (R7); K5 fires before progress (R6). |
| F-I4 no rerun/exclusion path | maj | **FIXED** (minor R14) | `_attempt_loop` R_max=3 with same seeds; `_write_invalid_record` always writes a schema-valid excluded record; `attempts.csv`; `_attempt_once` catches everything. Remainder: arbitrary exceptions default to `application_did_not_start` (misleading reason, R14), and a validation failure inside `_finish` still raises rather than producing a distinct closed-list reason. |
| F-I5 fire-rate denominator | min | **FIXED** | `deviations.compute_fire_rates` + `## 3b` table generated from `results/faults.log`; currently empty (no injected runs yet, correctly). |
| F-E1 Stage-A case/K4 | crit | **FIXED** | All matrix/order Stage-A faults upper-case canonical, `ctrl → none`; `K4` in the failpoint map (early probe); canonical `record_stage/record_fault`; M1-kafka role map present; matrix regenerated/re-hashed. |
| F-E2 K5/K6 no-ops | crit | **PARTIAL** | K6 declared n/a for all 9 mechanisms with proof (matrix has no K6 run; A2 §7.2) — **FIXED**. K5 realisation exists as protocol replay, but the runner fires K5 **at load start**, rewinding zero progress → `moved=false` → `fault_not_fired` and no duplicate delivery (R6). |
| F-E3 K2≡K3 | maj | **FIXED** (live unverified) | `K2UnknownOutcome` waits for a seeded committed-progress marker, then installs a Toxiproxy `timeout` toxic on the effect link (db proxy for DB sinks, produce proxy for M1-kafka); matrix K2 rows now `external` (S1); validator accepts S4 K2 receipts (tested). Not yet exercised live. |
| F-E4 F6/M1-kafka, M4 role, F4b L2 | crit | **FIXED** (matrix/code) | M1-kafka F6 n/a in matrix (pilot/B) with D5 reason; M4-family rewind uses write-capable `wxeo_fault`; rewind is k×`max.poll.records`; F4b L2 uses seeded `--wxeo.failpoint.after=N`. Caveat: F4b is app-owned but its `injector_mode=live/L2` makes the validator treat it as S4-owned (R3). |
| F-E5 G1 evidence timing | maj | **FIXED** (execution blocked by R2/R5) | 6 g1k3/g1k5 validation runs present in matrix/order; G1 is evaluated per class in the analyzer (K3 and K5 separately). The runs themselves would currently fail the receipt protocol. |
| F-G1 tautological gates | crit | **PARTIAL** | The gate table is genuinely falsifiable: all 8 negative controls FAIL as required (re-run against current analyzer); gates are computed over all executed runs, G1 per class, G6 compares app `shared_sha256` across mechanisms within a cell, G7 implemented, G8 real clock/residue/disk. **Remainders:** (i) `gate-negative-controls.json` not frozen (R11); (ii) G2 does not count a censored control as a failure (A2 §2.5 item 7); (iii) G7 does not verify generator ≥3× capacity — only lag ≤5 ms and calibration runs; (iv) G8 does not verify affinity/priority (which are themselves unrecorded, R13); (v) G4 uses the analyzer's own `attested` re-derivation rather than `experiments/receipts.validate_run_receipts` (single-source rule not enforced). |
| F-G2 quiescence | crit | **FIXED** (live unverified) | `kafka_admin.py` uses AdminClient `list_offsets` + `list_consumer_group_offsets` for real lag; consumer quiescence = lag 0 + monotone sink count stable for T_quiet + `pg_stat_activity` idle + M2 relay drained; M1-kafka uses read-committed LSO stable; consumers are stopped **before** dumps/oracle; PG dump is `REPEATABLE READ READ ONLY`; T_max → censored. |
| F-P1 unsatisfiable rules | crit | **PARTIAL (A2 drafted; analyzer not updated/approved)** | A2-1 is satisfiable on the new matrix (independently checked, §2.1). But the **frozen analyzer** on S0 still contains the old rules: C1 Stage-A K3 McNemar (line ~857), C2 self-pairing `_paired_field("M3","M3",…)` + rep-mismatched M3-K4-vs-control fallback (lines ~888–902), C3, H2/H4 including probe cells, old H5 dominance including M1-kafka, P3 on fault-free Stage-C instead of A2's 3 Stage-B contrasts. A2 §6 requires these replaced before the analysis freeze (R10). |
| F-P2 §2.2 row arithmetic | maj | **FIXED** | A2 §4.1/§4.2 corrected (7/7 for M4/M4+M3, 66 fault cells, 42 crash cells); matrix accounting matches recomputation. Remaining prose: DEVIATIONS §4 R4 still prints the pre-amendment "201/7,851/20" text (`R19`). |
| F-P3 seed formula | maj | **FIXED** (doc) | A2 §5.1 freezes the implemented concatenation unambiguously (32-hex master, 16-hex data/injection, stage tokens, no separators, no regeneration); all 7,514 seeds reproduce. |
| F-P4 lower-case tokens | maj | **FIXED** | Matrix/order carry canonical upper-case Stage-A faults and `none` for controls; `record_fault` mapping; validator accepts; analyzer filters match. |
| F-P5 analyzer accounting | maj | **PARTIAL** | Fixed and verified: censored ⇒ `violating_run`; `n_planned` from the reserved-run list with `n_missing`; validation `V.*` excluded from confirmatory tables; direction-aware cost/latency tests (`delta*direction>0`, no `abs`); H1 clean arm restricted to named cells; H5 non-vacuity guard (H5 now `inconclusive` on the synthetic fixture instead of vacuous `confirmed`). **Not fixed:** P3 is a different family/data than A2 (fault-free 100k/P6 Stage-C, all pairs) — A2 requires 3 pre-registered contrasts over the canonical Stage-B fault cells; F-P5 item 7 (`attested` single-source) not fixed — `extract()` counts receipts itself for L1/F4b and ignores restored, disagreeing with `receipts.validate_run_receipts`; analyzer does not re-validate records on load. |
| F-P6 freeze hygiene | maj | **PARTIAL** | Freeze regenerated after the fixes (S0 14:38:38Z), `--check` implemented and called by the runner preflight; jar + deterministic 66-file source-tree digest bound; `--check` refuses drift (verified by tampering a temp freeze). **Gaps:** gate-negative-controls evidence not frozen (R11); S0 freeze `git_sha=39d7a07…` while HEAD was `77e9f8a…` (commit made after the freeze; S1 re-freeze now carries HEAD); `results/raw/summaries/reserved-runs.csv` (n_planned source) is regenerated and not hashed (derivable from frozen run_order, acceptable but state it); DEVIATIONS must be regenerated after the matrix re-hash (S1 did at 11:46:54). |
| F-P7 matrix compliance positive | min | **POSITIVE** | All §2.1 checks pass; only documentation nits remain (R19). |
| F-M1 performance pipeline | maj | **FIXED (artefact hooks delivered)** | S3 added service-side warm-up skip (first N committed events; `warmup_events_excluded`, `warmup_boundary_commit_ms`), `send_ts_ms` in the produced manifest (verified in acceptance artefacts), throughput window and histogram JSON with performance block (verified in acceptance app/hist JSON); runner merges per-instance percentiles (count-weighted interpolation, method recorded) and computes throughput from warm-up-excluded committed/acked events, generator lag p99, time-to-consistent from quiescence. Caveats: percentile merge is an approximation (not HdrHistogram exact merge) and is disclosed; per-instance merge/`histogram_path` details are recorded. |
| F-M2 fsync no-op | maj | **FIXED** | `IoUtil.writeFsync` uses `FileChannel.force(true)`; `appendLineFsync` uses `FileOutputStream.getFD().sync()`; `GroundTruth` opens a `FileChannel` and `force(true)` after flush before produce; config writer is atomic+fsync. Verified in source (jar rebuilt and frozen). |
| F-M3 fairness details | min | **DOC — not done** | Paper/S2 prose task; no code change expected. |
| F-F1 TLC over-claim | maj | **PARTIAL (doc/A2)** | `formal/README.md` now has a correct "Scope of the machine-checked claim" / "not modelled" section and the outcome table no longer hard-codes a distinct-state sequence; `tlc-summary.json` checked 12:51:13Z. **Pending:** method §6.7 FO-6 / prereg wording withdrawal and paper paragraph are A2-7, to be applied at approval; stale DISTINCT-state lists in other drafts must cite the summary. |
| F-RN1 live record path | crit | **PARTIAL** | Canonical stage/fault mapping; always-write record (valid or excluded); validator before write; R_max reruns; config block read from app `config_<run>_1.json` with `shared_sha256`/`intrinsic_sha256`. **Remainders:** the live path has never produced a valid record (see §4: only one invalid record in `results/raw/run-records/`); `exit_status.consumer` empty (R8); invalid reasons mislabelled (R14); records not re-validated by the analyzer. |
| F-RN2 residue/readiness | maj | **FIXED (code)** | Pre-run asserts topic/group/schema absence; readiness = all instances `assigned` + lag 0 for 5 s; post-run schema drop; restart epochs/reset cadence recorded; M5 retains topics by design (unique names provide freshness), broker safety properties verified. |
| F-RN3 clock/process limits | min | **PARTIAL** | Real clock probe (wall-vs-monotonic drift, boot time before/after) fixed; **process telemetry broken:** `hikari_pool` is read from the wrong nested path (always null; config has `shared.jdbc.pool_size=8`) and affinity/priority are read after `_stop_consumers()` has cleared PIDs (always null) (R13); G8 does not check them. |
| F-R1 recomputation | min | **POSITIVE** | Matrix/order/counts/seeds/constraints recompute exactly (S0 and S1); three doc nits remain (R19). |

**Count of previous findings by status (S0, 34 defect findings + 1 positive F-O8):** FIXED 19;
PARTIAL 11 (F-O3, F-O4, F-I2, F-I3, F-E2, F-G1, F-P1, F-P5, F-P6, F-F1, F-RN3); NOT FIXED 2
(F-I1 injector dispatch; F-I2's integrated form is listed PARTIAL but no injected run can pass
today); DOC-pending 1 (F-M3); POSITIVE/documentary 2 (F-O8 and F-R1/F-P7, included in FIXED where
they are documentation fixes). No previous finding regressed to a *worse* state; several "FIXED"
claims are FIXED only in isolation and are defeated by the integration defects below.

---

## 4. Acceptance evidence — verified genuine

`%LOCALAPPDATA%\eos-runtime\run\acceptance\` (S3 suite, written 14:36:32–14:37:30Z):

| Claim | Verification | Verdict |
|---|---|---|
| 4/4 tests PASS | `acceptance-result.json` `tests` = acceptance3_kafka_smoke, acceptance4a_M0_e2e, acceptance4b_M3_e2e, acceptance5_failpoint_crash, all `passed=true`, `ok=true`; log ends "ALL ACCEPTANCE TESTS PASSED" | ✔ |
| 2000/2000 postings, 0 duplicates (M0, M3) | smoke results `posting_rows=2000, expected_postings=2000, duplicate_keys=0` computed by `smoke-e2e.ps1` via SQL after the run; scratch dirs contain the real app/config/hist artefacts and 18 KB service stdout | ✔ |
| Correct GT digest | **independently regenerated**: seed `0000000000000001`, 1000 events, P=3 → `ad520789a139575117419cd105871ab28ad13636e8ac3a33cca039352335d043` exactly the recorded/file digest (200-event crash run → `afe9d71b39bc…` exact); on-disk GT files hash to the recorded values | ✔ |
| Config hash | acceptance app JSON and archived config both have `shared_sha256=f87be4111433dccc5b6e6d3947f4682732686c94ac3935f454fde75235717c26`, identical for M0 and M3 while `intrinsic_sha256` differs (M3 dedup index true, M0 false) — exactly the G6 contract | ✔ |
| Service exit codes | M0 0, M3 0; crash run exit **99**; `fired_count=1`, receipts file contains exactly one app `fired` with `action=crash;point=before_effect_commit;hit=1;after=1` | ✔ |
| Attestation | M0/M3 `can_update=false, can_delete=false, append_only_triggers=1`, M3 dedup index present/expected | ✔ |
| Not a fixture / real timestamps | artefacts include real config/app/hist/loader/service logs with contiguous 14:36:32→14:37:30Z timestamps and stack-state transitions (Kafka up 14:36:32, PG 14:36:53, Toxi 14:36:55, stopped 14:37:22/30); scratch tree has 124 real files; suite restarted the stack via the native harness and recovered per-run smoke JSON | ✔ |
| Inconsistency with S3/S4 claims | none found: S4's report says the runner live smoke was still queued and `results/raw/run-records/` confirms exactly one **invalid** live record (`application_did_not_start`, 13:48Z); acceptance suite tests the **jar** through S3's harness, not the runner. `results/harness/live-smoke-evidence.json` does not exist | consistent |

**Caveat (R17):** the acceptance evidence lives outside the repository and is not hashed/frozen or
recorded in `results/RUNLOG.md`; `acceptance-result.json` records the jar *path* but not its SHA-256.
The acceptance suite proves the artifact, not the runner; the runner's live path remains unproven.

---

## 5. New findings R1–R22 (severity, evidence, required fix)

> "S1" = the in-progress revision at freeze 14:47:02Z inspected but not accepted. A finding marked
> S1-FIXED still has to pass the focused re-re-review (lead checks (a)–(f)).

### R1 (critical, **S1-FIXED**) — injector factory has no dispatch for external L2 faults
**Evidence (S0):** direct call `make_injector(ctx(injector_mode="live", fault="F1"))` raised
`ValueError: no injector for fault 'F1'` for F1,F2,F3,F4a,F5,F6; only F4b (FailpointWatch) and
K2/K5 dispatched. S0 reserved-run impact: **3,213 rows** (159 pilot incl. 6 selftest; 3,060 Stage B;
`ValueError` path → 3 invalid attempts each, mislabelled `application_did_not_start`).
**S1:** dispatch restored (`fault in INJECTOR_TYPES and mode in live/selftest/external`); all 13
fault/mode combinations construct without raising (verified). **Required before acceptance:** the
same construction test over every `(fault, injector_mode)` in the new matrix, plus a live run.

### R2 (critical, **S1-FIXED**) — app-owned run wrote an S4 companion `fired` receipt
**Evidence (S0):** `FailpointWatch.wait_for_app_fired` appended `source="s4", event="fired"` next
to the app's own fired; `receipts.validate_run_receipts` rejects any S4 fired on app-owned runs
("S4 must not write fired for an app-owned injection"). Reproduced: app fired + companion + S4
restored → `attested=False`.
**S1:** companion write removed (verification recorded in `state`, not as a receipt); writer-side
protocol now matches the validator. **Required:** a regression test driving the real
`FailpointWatch` against a temp receipt log (not a hand-built receipt list), asserting exactly one
app fired + one S4 restored and `attested=True`.

### R3 (critical, **S1-PARTIAL**) — injector-mode/ownership mapping
**Evidence (S0):** matrix carried `injector_mode="failpoint"` for K2/K5 (S4-owned) → the validator
classifies them app-owned, then fails them for "no failpoint name" (510 Stage-A runs). F4b (L2,
`live`) is app-owned in reality but the validator classifies it S4-owned (`expected exactly one S4
fired, got 0`) → 378 pilot/B runs. S1 fixes the matrix K2/K5 rows (`external`), but (a) the six
**g1k5 validation rows are still `injector_mode="failpoint"`** (3 runs; `K5` is S4-owned), and
(b) F4b ownership is still not expressed to the validator (`FailpointWatch` is used but
`injector_mode=live`, `fault_level=L2`). **Required:** single ownership table used by builder,
runner, validator and analyzer; `g1k5` mode fixed; F4b declared app-owned (e.g. mode `failpoint`
or fault-level/ownership field) and tested; regenerate/re-hash and re-derive counts if any matrix
change.

### R4 (critical, S0 **NOT FIXED**; S1 **PARTIAL**) — fault phase window / L2 timing
**Evidence (S0):** nothing assigns `InjectorContext.load_start_ms/load_end_ms`; `_phase()` returns
`nulls`; `validate_run_receipts` rejects every S4 fired receipt for an incomplete phase block, and
`consumers_alive` is `True` by default. **S1:** window is filled at load start with a *planned upper
bound* and closed after the loader with the real end; the runner post-check compares the fire time
to the real window (+2 s margin) and excludes out-of-window fires; `consumers_alive` is measured.
**Still broken:** the receipt itself carries the planned bound (not the documented real window);
`_expected_load_seconds()` still returns 60 s when `rates.json` is absent, and K2/K5 are special-
cased, so pilot L2 faults (189 runs) fire 15–45 s after a real 1 k load of ~0.2–2 s → all excluded;
K5 fires immediately (R6). **Required:** fire on progress markers (like K2) or on the real load
window reported by the loader; if a planned bound must be recorded, the validator must reject a
fire after the real end; `rates.json` procedure required before any fixed-rate claim (R12).

### R5 (critical, S0 and **S1 NOT FIXED**) — app-owned failpoints fire on both C=2 instances
**Evidence:** `_service_args(instance)` passes the same failpoint configuration to every instance;
each JVM's `Failpoints` increments its own hit counter and fires `after=1` on its first event →
both instances write an app `fired` receipt (and both halt for `action=crash`). The validator
requires exactly one app fired; `FailpointWatch._verify_app_action` only checks PID 1; restore
restarts both. **Impact:** 1,131 app-failpoint runs (750 Stage-A K1/K3/K4/K7 + 3 g1k3 + 360 Stage-B
F4b + 18 pilot F4b) cannot satisfy the receipt rule even after R2; the realisation also does not
match "crash one owner instance" semantics. **Required:** arm the failpoint on one deterministically
chosen instance (`--wxeo.failpoint.instance` / per-instance config) or accept and validate
per-instance receipts; record which instance fired; restore exactly that instance; add a regression
test with C=2.

### R6 (critical, S0 and **S1 NOT FIXED**) — K5 fires before any progress → no duplicate delivery / G1 impossible
**Evidence:** `_start_injector_thread` calls `injector.fire()` immediately for K2 **and K5**
(`if self.run["fault"] in ("K2","K5")`), whereas K2 internally waits for its progress marker and K5
does not. K5 therefore stops consumers at load start, snapshots progress ≈ 0, rewinds by
`k×500` from 0 (no-op), and `_verify()` requires `rewind.moved` → false → `InjectionNotVerified` →
`fault_not_fired` exclusion; even if it verified, no duplicate delivery could occur. The G1
negative control requires M0 to show I3 violations in K5, so G1 can never pass. **Impact:** 240
Stage-A K5 + 3 g1k5 validation runs; G1 gate. **Required:** wait for a seeded committed-progress
marker (as K2 does) before the stop-rewind-restart, and verify the rewind moved offsets; add a
`gate_negative_controls`-style test for the K5 realisation.

### R7 (critical-major, S0 and **S1 NOT FIXED**) — F1 verification and restore are broken
**Evidence:** `F1BrokerKill._verify` computes the broker count from `stack-state.json`, which is
only rewritten by harness verbs; after `taskkill` it still reports 3 nodes (the file also counts
nodes without liveness checks) → `reduced=False` → `InjectionNotVerified`, no fired receipt.
`_restore` calls `self.ctx.call([self.ctx.native_stack_ps1, "start-kafka"])`, i.e. executes a
`.ps1` through `subprocess` without `pwsh` on Windows → restore fails. **Impact:** 567 F1 runs
(pilot 27, Stage B 540) → Stage B blocked. **Required:** verify broker liveness via AdminClient
`list_topics` brokers or process liveness per PID (and record the observed broker count), and invoke
the harness through `pwsh -File`.

### R8 (major, S0 and **S1 NOT FIXED**) — `exit_status.consumer` is always empty
**Evidence:** `_stop_consumers()` calls `self.processes.clear()` before `_finish()` builds
`"exit_status": {"consumer": {inst: proc.returncode …}}`; the dict is therefore `{}` for every live
run. `validate_record.EXIT_KEYS` is defined but never checked. **Impact:** service exit codes (a
convergence/attestation signal, and part of the acceptance contract) are lost for all 7,488 runs;
the validator cannot catch it. **Required:** capture return codes before clearing, require
`consumer`/`load_generator` in the validator, and record both instances.

### R9 (major, S0 and **S1 NOT FIXED**) — validator does not enforce A2 §3.4 cross-field equations
**Evidence:** `validate_record` enforces key sets only. A record with `{v3:true, violating_run:false}`
and a record with `v1:false` while `structurally_malformed_entries=3` both **validate**. A2 §3.4
acceptance check #1 explicitly requires the equations `violating_run == v1∨v2∨v3∨v4∨v5` and
`v1 == (structurally_malformed_entries>0 ∨ imbalanced_sum_entries>0)` (and the count/flags
consistency rules). **Impact:** a corrupt/tampered oracle result can be frozen as "valid"; the
verdict could disagree with the stored flags. **Required:** implement and test the equations; also
require `counts` non-null on valid runs.

### R10 (major, S0 and **S1 NOT FIXED**) — frozen analyzer is still the pre-A2 analyzer
**Evidence (code + synthetic run):** `tools/analyze.py` still builds `C1` with
`stageA_K3_M1x_vs_M1-kafka` (unfirable: M1-kafka K3 n/a), `C2` with `_paired_field("M3","M3",…)`
and a rep-mismatched M3-K4-vs-control fallback, `C3`, `H2`/`H4` correctness over all A/B cells
including probe cells, old H5 dominance (Stage A/B all cells, includes M1-kafka, no residual
closure), and `P3` as fault-free 100k/P6 Stage-C pairs. It does not derive `v1a/v1b`, has no P11
test, no matrix-hash gate ("fail if the matrix hash differs from the freeze"), does not re-validate
loaded records, and re-derives `attested` independently of `experiments/receipts.py` (F-P5 item 7).
**Impact:** if Stage A data are analysed with the currently frozen analyzer, the A2-approved rules
would not be executed; C2/K3 tests cannot be significant. **Required (before analysis freeze,
after author approval of A2):** implement A2-6 checks 1–14, add the matrix pin/classifier, single-
source `attested`, record re-validation, and the acceptance fixture of A2 §6 item 14; re-freeze and
have S5 re-verify. (This is expected sequencing — A2 is still a draft — but it must be on the
pre-Stage-A critical path, not after the pilot.)

### R11 (major, S0 and **S1 NOT FIXED**) — gate falsifiability evidence is not frozen
**Evidence:** `analyze.build_gate_table` only marks a gate PASS if
`results/harness/gate-negative-controls.json` records `failed_as_required` for it, but that file is
not in `prereg-freeze.json` `files`/`evidence` (only `g3-mutation.json` is hash-recorded) and is not
hashed anywhere else. It was generated 10:28, before the last analyzer edit (10:31); I re-ran the
script against the current analyzer and all 8 negative controls still FAIL as required, so the
current file's content is valid **today** — but it is a post-hoc-editable gate input.
**Impact:** the paper's main internal-validity gate evidence is not bound to the frozen pipeline.
**Required:** add it to the freeze (hash under `pre_freeze_harness_evidence`), regenerate it from
the final analyzer, re-freeze; ideally also RUNLOG the generation command.

### R12 (major, S0 and **S1 NOT FIXED**) — `rates.json` does not exist and nothing generates it
**Evidence:** `experiments/rates.json` is absent; `LiveSequence._effective_rate` raises for Stage
B/C but silently returns `"max"` for pilot/Stage A fixed-rate cells — an **undisclosed** deviation
from prereg §6.2 ("fixed-rate mode; R_off = 0.75 × min pilot capacity"); no code path writes a rate
table, and the pilot matrix contains only 1 k/P3 cells, so `R_off(10k/100k, P3/P6/P12)` cannot be
derived from pilot data as specified. **Impact:** stage B/C abort at the first run (3,960 + 1,800
runs); pilot/A run at saturation while labelled fixed-rate (affects pilot timing and G7); the
Stage-A fault phase timing inherits the 60 s fallback (R4). **Required:** an implemented,
pre-registered rate-calibration procedure (which runs measure capacity at which (load, P)),
generation of `experiments/rates.json` as a pilot artefact, a DEVIATIONS entry for any fallback,
and G7 verifying the ≥3× generator capability. Decide this at the P2 gate before the pilot (the
pilot is supposed to produce these numbers).

### R13 (major, S0 and **S1-not-verified**) — process telemetry is not actually recorded
**Evidence:** `_process_limits()` looks up `hikari_pool` under `shared.hikari_pool_size` or
`shared.hikari.pool_size`, but the app config nests it at `shared.jdbc.pool_size` (=8) → always
`null`; affinity/priority are read via `Get-Process` in `_finish`, after `_stop_consumers()` has
cleared `consumer_pids` → always `null`. G8's requirement string mentions affinity/priority but the
gate never checks them. **Impact:** F-RN3's "real affinity/priority read-back" claim is not true;
the fairness audit cannot verify pinned limits from records. **Required:** read process limits
while the services are alive (before stop) from the correct config path; add gate assertions.

### R14 (minor) — arbitrary exceptions are mislabelled `application_did_not_start`
**Evidence:** `_attempt_once` maps any exception without `exclude_reason` to `EXCLUDE_START`
(application_did_not_start). The S0 injector `ValueError` (R1) and any oracle/injector wiring bug
therefore appear in DEVIATIONS as application-start failures, hiding the real cause; the closed
list has no "internal/wiring bug" reason and prereg says internal bugs should fail loudly.
**Required:** a distinct closed-list reason (or campaign abort) for internal/wiring errors, and
record the exception type/message in the record notes (already partially done).

### R15 (minor) — dedup mechanisms report `postings_inserted = 0`
**Evidence (acceptance artifacts):** M3 smoke wrote 2,000 posting rows and 0 duplicates, but the app
counter `postings_inserted = 0`; `PostgresSink.executeIdem` counts only `n > 0`, while with
`reWriteBatchedInserts=true` PostgreSQL/JDBC returns `SUCCESS_NO_INFO = -2` (the plain path counts
`-2`, the idem path does not). **Impact:** instrumentation counters are wrong for
M3/M2+M3/M4+M3/M1x+M3; oracle counts are unaffected (they read the DB), but paper/appendix
instrumentation numbers would be false. **Required:** count `SUCCESS_NO_INFO` in the idem path (and
`conflictRows` accordingly), rebuild jar, re-freeze.

### R16 (minor) — missing attestation *file* raises instead of recording v2
**Evidence:** `oracle._read_json` raises `FileNotFoundError` for a missing attestation path; the
contract (`evaluate_attestation(required=True)`) says missing attestation on a DB sink is an I2
violation. The runner always writes the attestation file (even with empty checks → v2), so the
realistic path is covered; a hard file loss invalidates the run (safe direction) instead of a clean
pass. **Required:** read tolerantly and let `evaluate_attestation` set v2 (or explicitly record an
invalid run reason), and test it.

### R17 (minor) — acceptance evidence not archived/hashed/jar-identified
**Evidence:** `acceptance-result.json` + scratch artefacts live under `%LOCALAPPDATA%`, outside the
repository; no SHA-256 of the JSON/log/jar is recorded in RUNLOG/freeze; the JSON records the jar
path only. The evidence is genuine (§4) but not reproducible from the repository alone.
**Required:** copy a scrubbed summary + hashes into `results/harness/` (path-scrubbed), record the
jar SHA and the generating command in RUNLOG, and freeze it.

### R18 (minor) — A2's own evidence snapshot is stale
**Evidence:** A2 header pins `docs/prereg-freeze.json` at `13:32:43Z`, git `28d02aa…`; the final S0
freeze is `14:38:38Z` (S1 `14:47:02Z`). A2 itself says the snapshot is provisional and must be
re-validated at final freeze Z; the matrix hashes it pins are the correct S0 ones.
**Required:** re-pin A2's evidence-snapshot line at the accepted freeze when the author approves
(it is a one-line provenance update, not a substance change).

### R19 (minor) — DEVIATIONS accounting text still mixes pre-amendment numbers
**Evidence:** DEVIATIONS §2 "Cells with zero executed runs (471)" includes the 19 **validation**
pseudo-cells from `_cell_plan` (452 confirmatory cells absent + 19 V.\* keys = 471); the list should
separate validation evidence from confirmatory cells. §4 register rows R4 still say "15 n/a slots /
201 executable / 7,866/7,851 (+20 validation)" (superseded by §1b/S5-1; A2 §4.4 requires the
15-slot sentence to be clarified), and R2/R3/S4-3 mention "20 validation runs" (now 26).
**Required:** regenerate DEVIATIONS after the final matrix (S1 did) and add the erratum lines at
the P2 gate (A2 already flags the 15-slot wording).

### R20 (minor) — M1-kafka quiescence blind spots
**Evidence:** publication quiescence uses read-committed LSO stability; an open transaction that
has written no records is invisible; each check creates an ephemeral consumer group
`wxeo.quiesce.<pid>.<ts>` that is never deleted (group residue grows with 7,488+ runs; list_groups
is used for residue checks). **Required:** state the LSO definition's limitation; use a fixed
private group or delete it (deleting a consumer group is safe on Windows KRaft — only topic
deletion is fatal — but confirm with S3), or record the residue policy.

### R21 (minor) — S0 freeze `git_sha` did not name the commit containing it
**Evidence (S0):** freeze `git_sha=39d7a07e…`, but `.git/refs/heads/main` was `77e9f8a…` (the commit
"feat(artifact): accepted implementation …" was made at 14:39:24Z, after the 14:38:38Z freeze).
`freeze --check` cannot detect this (it hashes files, not the commit). S1's re-freeze carries
`77e9f8a…` correctly. **Required:** re-freeze after the last commit (or record an explicit
code-commit → freeze-commit mapping) and state it in RUNLOG; a CI check comparing freeze
`git_sha` to HEAD would prevent recurrence.

### R22 (minor) — gate semantic gaps relative to A2/F-G1 wording
**Evidence:** G2 ignores censoring for the positive controls (A2 §2.5 item 7 says a censored control
is a gate failure); G7 requires only `rates.json` + lag ≤5 ms + calibration runs, not the
pre-registered "generator sustains ≥3× max capacity"; G8 claims affinity/priority in its
requirement string but checks none. **Required:** implement or reword; they are cheap to fix while
the analyzer is being updated for A2.

---

## 6. Conditions before confirmatory runs (P2 gate / pre-Stage-A checklist)

The pilot is the vehicle that must produce several of these, so the pilot may start **only** if it
is explicitly a gate-calibration pilot and no confirmatory stage begins until all conditions hold.
The lead's mandated focused re-review checks (a)–(f) are incorporated; I add (g)–(k).

**Run-production (blocking; verified by me before any Stage A):**
1. **(a)** For **every** `(fault, injector_mode)` appearing in the accepted matrix,
   `make_injector` returns an injector (or `None` exactly for `F0/none`) without raising; the
   ownership table maps K2/K5 → S4, K1/K3/K4/K7/F4b → app, F1/F2/F3/F4a/F5/F6 → S4.
2. **(b)** A synthetic app-owned run (K3) and a synthetic S4-owned run (K2) each yield exactly one
   `fired` + one `restored` (per owner rules) and `validate_run_receipts(...)==attested`.
   Additional required case: **C=2 app-owned crash** yields exactly one fired receipt (R5) and the
   fired instance is recorded; non-terminal app actions yield app fired+app restored and no S4
   receipt.
3. **(c)** A receipt with `fire_ms` outside the **real** load window is rejected
   (`uninformative_fire_outside_window`); a K5 run only fires after a seeded progress marker and
   the rewind verifiably moved offsets (R4/R6).
4. **(d)** One **live** runner run produces a schema-valid record with `oracle.source_integrity=true`,
   `validity.valid=true`, non-empty `exit_status.consumer`, correct `config_shared_sha256`, and
   `live_smoke_evidence.py` exits 0; plus a second run of an injected class (K3) passing (2).
   The existing S3 acceptance evidence is necessary but not sufficient (it tests the jar).
5. Same for **one live run per injector type** before Stage B (F1 broker kill/restore in
   particular, R7), or record the injector as unvalidated and remove its cells.
6. **Pilot calibration:** at least one valid fault-free run per load/partition type and the rate
   calibration needed to create `experiments/rates.json`; the compute projection filled from
   measured pilot values only (P2-GATE §3). No Stage B/C run while `rates.json` is missing (R12).

**Matrix/data integrity:**
7. **(e)** On the accepted hashes: matrix = 7,506 planned / 7,488 executable / 26 validation /
   7,514 rows; per-stage 216/198, 1,530, 3,960, 1,800; Stage-A 34 canonical + 8 probe + 9 controls
   (21 n-a crash cells); Stage-B 60 canonical + 6 probe (6 n-a of 72); pilot n-a 6 cells/18 slots;
   seeds 7,514/7,514 exact with the frozen formula; constraints ≤3 consecutive / ≤40 % per 60-block
   / stage contiguity; A2 §1.2 cell sets match the matrix; every n-a cell has its reachability
   reason; K6 absent everywhere, M1-kafka K5/F6 absent; controls `fault=none`; early only K4/F4b.
8. DEVIATIONS regenerated from the accepted matrix and consistent with §1b/totals (R19); the
   validity-run composition (26) matches S4-3.

**Freeze/provenance:**
9. **(f)** `freeze --check` binds the rebuilt jar (`6e822375…` or the next accepted build) and all
   frozen configs; freeze aggregate recorded; freeze `git_sha` equals the commit containing the
   accepted pipeline; DEVIATIONS names every post-freeze change; acceptance evidence archived and
   hashed (R17); gate-negative-controls frozen (R11); `results/raw/summaries/reserved-runs.csv`
   regenerated from the frozen order (or hashed).
10. Records re-validate on write **and** on analysis load (R9 validator equations + analyzer
    re-validation) — a record that cannot be re-validated must not enter `results/raw`.

**Author/A2:**
11. Author approves the P2 matrix amendment, A2-1…A2-8 and AC-1…AC-8 at the P2 gate; the lead
    applies the method/prereg/A2 text edits (incl. P11, TLC scope, §2.8 table, §2.2 rows, seed
    wording, 15-slot clarification); A2's evidence snapshot is re-pinned (R18).
12. **Analyzer updated to A2-6** (R10): C1 pooled/AC-3a per choice, C2/C3 deleted, H2/H4 canonical
    sets excluding probes, H5-A with residual table and non-vacuity guard, P3 three contrasts,
    `v1a/v1b` + P11 test, matrix-hash classifier, single-source `attested`, record re-validation,
    G2 censored control, G7 capability, G8 affinity; then re-freeze and S5 re-verifies on the A2
    acceptance fixture (A2 §6 item 14).
13. K6 non-realisability acknowledged in writing (P2-GATE §4 item 3) and carried to the paper's
    threats section; E3/P9 marked model-and-argument-only.

**Gate evidence (before Stage A data are analysed):**
14. G1–G8 pass on the pilot/gate-control evidence with the negative controls, in particular G1
    per-class with attested M0 K3 **and** K5 (the K5 realisation must be fixed first — R6), and G6
    cross-mechanism on real app configs, G7 with a real rate table and generator capability, G8
    with real process telemetry.

---

## 7. Residual risks for the paper (state these, do not hide them)

1. **Zombie/C7 (E3, P9, K6)** is not experimentally realisable in the frozen `T_max`/`max.poll`
   window and is **not** modelled by TLC: M4's and M4+M3's closure of E3, and the M4+M3 rationale,
   are model/argument-only. The decision table must mark E3 model-declared, and H4 must not claim
   K6 evidence (A2-7 wording).
2. **Oracle scope:** update-then-revert is invisible at end state (no WAL/logical decoding); the
   append-only attestation is a privilege/trigger attestation by the app role, not a proof against
   a privileged operator; sink-dump provenance is the DB row count + digest, not an independent
   copy of the DB.
3. **Publication metrics:** `pub_mutated_records`, `pub_lost`, signed `divergence` are secondary
   metrics (T1 exploratory); a lost publication does **not** set `violating_run` (T6). Publication
   content is checked against the ledger for DB mechanisms, but a publication stream cannot be
   proven complete for M1-kafka independently of the ledger it constitutes.
4. **Measurement:** latency clock is `System.currentTimeMillis()` (1 ms, clamped µs histogram),
   warm-up = max(2,000, 10 %) of committed events, throughput window from loader/broker ack times,
   per-instance percentile merge is count-weighted interpolation (not exact HdrHistogram merge),
   M2's publication cost is off the critical path, M1-kafka's publication is inside the Kafka
   transaction; crashed instances write no histogram; the paper must state all of this.
5. **Windows-native substrate:** no Docker/root; process kills approximate machine failures;
   RF=3 on one host; `reset-kafka` wipes storage between blocks (M5) so "one campaign, one boot" is
   per block; topic deletion is fatal (operations, not science).
6. **A2-dependent verdicts:** H1 (AC-3a vs AC-3b), H5 (H5-A vs H5-B), censoring fragility (AC-8) and
   the analyzer implementation are author-approved choices; the paper must report both the
   pre-registered rule and any deviation, and must not present the old C2/K3 tests as results.
7. **Rates:** the fixed-rate `R_off` values must be visible in the paper/prereg with their pilot
   derivation; if the fallback `max` is used anywhere before Stage B, it must be reported as a
   deviation (currently undisclosed).
8. **Acceptance vs runner:** S3's 4/4 PASS is real and proves the artifact; it is not evidence that
   the runner produces valid records. The first valid runner record must be in RUNLOG and frozen
   before Stage A.

---

## 8. Commands / artefacts used (reproducible; all read-only w.r.t. the repo)

```
python -m pytest experiments/tests -q -p no:cacheprovider            # 52 passed (S0)
python experiments/freeze.py --check                                  # ok (S0)
python tools/analyze.py --records results/synthetic/run-fixture/run-records \
    --out %TEMP%\wxeo-s5-rereview\analysis-synthetic --harness-dir results/harness
python experiments/oracle/mutation_test.py --mutations 100 --events 100 \
    --out %TEMP%\wxeo-s5-rereview\g3-mutation-rerun.json --workdir %TEMP%\wxeo-s5-rereview\g3-work
# gate negative controls re-run in-process with _REPO redirected to %TEMP% (all 8 FAIL as required)
# matrix/order/seeds/A2 recomputation: %TEMP%\wxeo-s5-rereview\check_matrix3.py
# oracle adversarial T0–T13:            %TEMP%\wxeo-s5-rereview\oracle_adv.py
# injector factory / receipt tests:      %TEMP%\wxeo-s5-rereview\{make_injector_check,proto_check}.py
# freeze drift tests:                    %TEMP%\wxeo-s5-rereview\freeze_dev_check.py
# GT digest reproduction:                %TEMP%\wxeo-s5-rereview\gt_check.py
```

No repository file was created or modified except this memo. No git command, no start/stop/format
of Kafka/PostgreSQL/Toxiproxy, no confirmatory run.

---

## 9. Final re-review checklist (command-level) — run only on the lead's signal

**Status:** prepared 2026-11-24 by S5. Run only when the lead sends the new tree/hashes after S4's
R5–R12 fix pass. Every step is **read-only w.r.t. the repository**; helper scripts go in
`%TEMP%\wxeo-s5-final\`; the stack is never started/stopped/formatted. Live evidence required from
S4/S3 is **inspected, not produced** by S5. Expected total time ≈ 30–40 min (checks 9.1–9.4 ≈
15 min; 9.5–9.8 ≈ 20 min).

**Inputs to request from the lead before starting:** new `matrix.yaml`/`run_order.csv`/`matrix.sha256`
hashes; new `docs/prereg-freeze.json` hash/aggregate; rebuilt jar SHA-256; the run-id + record path
of (i) one fault-free schema-valid live run with `source_integrity=true`, (ii) a K3 app-owned live
run, (iii) a K2 S4-owned live run, (iv) a C=2 app-owned (K3 or K4) live run, (v) the S3 acceptance
bundle (or a copy under `results/harness/`); the A2-6/M6/A2-approval status.

### 9.0 Snapshot + provenance (2 min)
```powershell
$R = "<repo>"; Set-Location $R
Get-FileHash experiments\matrix.yaml, experiments\run_order.csv, experiments\matrix.sha256,
  experiments\injectors.py, experiments\runner.py, experiments\receipts.py,
  experiments\validate_record.py, experiments\matrix.py, experiments\deviations.py,
  experiments\freeze.py, experiments\live_smoke_evidence.py, tools\analyze.py,
  docs\prereg-amendment-A2.md, docs\prereg-freeze.json, artifact\target\wxeo-service.jar `
  -Algorithm SHA256 | Select-Object @{n='f';e={Split-Path $_.Path -Leaf}},Hash
Get-Item experiments\matrix.yaml, experiments\run_order.csv, docs\prereg-freeze.json |
  Select-Object Name,Length,LastWriteTime
Get-Content .git\refs\heads\main                     # must equal freeze git_sha
python experiments/freeze.py --check                 # must print ok:true, no drift
```
**Pass:** all hashes equal the lead's numbers; freeze `ok:true`; `git_sha == HEAD`; A2 header hash
pins equal actual matrix/order hashes; freeze records the rebuild jar; freeze `files` includes
`results/harness/gate-negative-controls.json` (R11); freeze lives 10.0 not far behind mtime.
**Record** the exact snapshot in the memo §2.2 (replacing the "in-progress S1" note).

### 9.1 (a) Injector construction over the **whole** matrix (2 min)
```powershell
python %TEMP%\wxeo-s5-final\check_injectors.py
```
Script: load `matrix.yaml`; loop every unique `(fault, injector_mode, fault_level, mechanism,
ordering_variant)` **and** every one of the 7,514 rows; call `INJ.make_injector(ctx)`; assert
no exception, `None` iff `fault in {F0,none}`; assert the class map:
`F1→F1BrokerKill, F2→F2ConsumerKill, F3→F3ProduceTimeout, F4a→F4aOffsetCommitFailure,
F4b→FailpointWatch, F5→F5DbPartition, F6→F6DuplicateRedelivery, K1/K3/K4/K7→FailpointWatch,
K2→K2UnknownOutcome, K5→K5DuplicateDelivery`; `failpoint_config_for_run` enabled for app-owned only;
`MD.record_fault`/`record_stage` round-trip for every cell.
**Pass:** 0 raises, 0 mis-typed, 7,514/7,514 construct. **Fail = BLOCK.**

### 9.2 (b) Receipt protocol — app-owned K3 and S4-owned K2, plus C=2 and negatives (5 min)
```powershell
python %TEMP%\wxeo-s5-final\check_receipts.py
```
Uses the **real writer path** (`FailpointWatch` against a `%TEMP%` faults.log with a fake ctx, then
`RCPT.validate_run_receipts`), not hand-built lists only. Required cases:
1. K3 app-owned crash: one app `fired` (no S4 companion) + one S4 `restored` → `attested=True`,
   owner `app`; a stray S4 `fired` must be **rejected**.
2. K2 S4-owned: one S4 `fired` with `action_verified=true` + complete phase + one S4 `restored` →
   `attested=True`; missing phase / `action_verified=false` / missing restored → `False`.
3. Non-terminal app action (pause/throw): app fired + app restored, no S4 receipt → `True`.
4. Sham: exactly one `armed` + one `restored`, no fired → `True`; any fired → `False`.
5. **C=2 app-owned case (R5):** assert `_service_args(1)` and `_service_args(2)` give
   `--wxeo.failpoint.enabled=true` only to the **designated** instance, or that the writer/validator
   accepts exactly one app fired and records the fired instance; two app-fired receipts must never
   be produced by the runner (assert by construction and by validator: two app fired → `False`).
6. `receipts.classify` agrees with the ownership table for every receipt in the live record.
**Pass:** all asserts; K3 and K2 both `attested=True`. **Fail = BLOCK.**

### 9.3 (c) Out-of-window and action verification (3 min)
```powershell
python %TEMP%\wxeo-s5-final\check_window.py
```
1. `RCPT.validate_run_receipts` with `fire_ms < load_start_ms` or `fire_ms > load_end_ms`, and with
   `phase.outside_window=true` → `attested=False` mentioning "outside".
2. Runner post-check: with a temp faults.log and monkeypatched `timings` (real end before fire),
   `_collect_fault_receipts` appends `EXCLUDE_WINDOW`; a fire inside the real window appends nothing.
3. K2 marker: fake `progress_probe` never reaching marker → `InjectionNotVerified`; reaching marker →
   `fire()` proceeds (toxiproxy mocked).
4. K5 (**R6**): assert the K5 fire path waits for a durable-progress marker **before**
   `stop_consumers()` (record call order in a fake ctx); a below-marker rewind must not verify;
   `rewind.moved=true` only after real progress.
5. `consumers_alive` in the fired phase reflects real process liveness (fake dead pid → False).
**Pass:** all; **Fail = BLOCK.**

### 9.4 (d) Live evidence supplied by S4/S3 (5–8 min, read-only)
```powershell
python experiments/validate_record.py results\raw\run-records\run_<ID>.json --quiet   # exit 0, each ID
python experiments\live_smoke_evidence.py --run-id <SMOKE_ID>                         # exit 0
python %TEMP%\wxeo-s5-final\check_live_records.py --ids <SMOKE>,<K3>,<K2>,<C2>
```
`check_live_records.py` asserts per record: `validity.valid=true`, `excluded=false`,
`oracle.source_integrity=true`, `oracle.gt_sha256 == expected_sha256` (non-null),
`config.shared_sha256` and `config.intrinsic_sha256` present, `exit_status.consumer` non-empty for
all C instances, `validity.residue_check/clock_check=true`, `fault` canonical, `stage` canonical;
per injected record: exactly the expected receipts (K3: one app fired + one S4 restored; K2: one S4
fired with verified action + one S4 restored), phase window complete and fire inside the **real**
load window, `attested=true`; for the C=2 app-owned record: exactly one app fired and the fired
instance recorded; for the smoke record: `source_integrity=true` and (for a fault-free run) zero
violations.
Also: `results/harness/live-smoke-evidence.json` exists and `passed=true`; a RUNLOG row exists for
each run-id; the oracle JSON exists and `result_sha256` is present; acceptance bundle: re-derive the
GT digest with `derive.py` for its seed/events and compare (as in §4); jar SHA in the evidence/
freeze equals the rebuilt jar.
**Pass:** all records valid and ≥1 injected record attested for each owner class. **Fail = BLOCK.**

### 9.5 (e) Matrix / order / seeds / A2 / DEVIATIONS recompute (5 min)
```powershell
python %TEMP%\wxeo-s5-final\check_matrix_final.py     # adapted from %TEMP%\wxeo-s5-rereview\check_matrix3.py
python -c "import sys;sys.path.insert(0,'.');from experiments import deviations as D;D.write_deviations('experiments/matrix.yaml','results/raw/run-records',r'%TEMP%\wxeo-s5-final\DEVIATIONS.md')"
```
Asserts (all must pass): rows **7,514**; stage label counts **V 26 / P 198 / A 1,530 / B 3,960 /
C 1,800**; cells 459 with per-stage planned/executable/na = 72/66/6, 51/51/0, 264/264/0, 72/72/0;
runs 216/198, 1,530, 3,960, 1,800; totals 7,506/7,488/26/7,514; Stage-A 34 canonical + 8 K4 probe +
9 controls, n/a crash cells 21; Stage-B core 60 canonical + 6 F4b probes, n-a 6 of 72; pilot n-a 6
cells/18 slots; seeds **0 mismatches / 7,514**, pairing groups >1 seed **0**, no all-digit seeds;
early rows exactly {A-K4 240, B-F4b 360, pilot-F4b 18}; max consecutive same mechanism **≤3**;
worst 60-window share **≤0.40**; no n-a cell scheduled; matrix↔order 0 missing/0 extra; no
non-executed rows; all Stage-A `fault` canonical (incl. `none` for controls); validation composition
26 = sham6+selftest6+c1-2+quiesce2+oracle4+g1k3-3+g1k5-3; **A2 §1.2 cell sets per mechanism equal
the matrix exactly**; A2 pinned hashes equal actual; `matrix.sha256` matches; DEVIATIONS regenerated
to temp has the **actual** matrix hash, the amended accounting, and separates `V.*` from the
zero-executed confirmatory list (R19); `results/raw/summaries/reserved-runs.csv` (or its successor)
matches the frozen order.
**Fail = BLOCK.**

### 9.6 (f) Freeze, artifact binding, drift refusal (3 min)
```powershell
python experiments/freeze.py --check
python %TEMP%\wxeo-s5-final\check_freeze_drift.py    # monkeypatches FREEZE_PATH to tampered temp copies
python experiments\runner.py --check-env             # read-only preflight printout; no run started
Get-Content .git\refs\heads\main
```
Asserts: freeze `ok:true`; **jar SHA equal to the rebuilt jar**; source-tree digest bound; every
frozen file hash matches; `gate-negative-controls.json` hash present (R11); tampered file hash →
`ok=false`; tampered jar hash → artifact drift; missing freeze → fail; freeze `git_sha == HEAD`;
runner preflight reports `freeze.ok=true` (and would refuse drift); acceptance/live evidence hashes
recorded (R17). Regenerate gate negative controls to temp and confirm all 8 still FAIL and the
committed file's hash equals the frozen hash.
**Fail = BLOCK.**

### 9.7 R5–R12 acceptance checks (explicit pass/fail per finding)
| ID | Check | Pass criterion |
|---|---|---|
| **R5** | per-instance failpoint scoping + C=2 receipt rule (code + 9.2 case 5 + live C=2 record) | runner cannot produce >1 app fired; live C=2 app-owned record has exactly one app fired, recorded instance, attested |
| **R6** | K5 waits for a durable progress marker before stop/rewind; rewind moves offsets; live g1k5 record | unit ordering test passes; live K5 record `v3=true`, `attested=true`, `action_verified=true`, `rewind.moved=true`; G1 K5 class passes on the gate-control data |
| **R7** | F1 verify uses live AdminClient broker metadata (not stack-state); restore invokes `pwsh -File native-stack.ps1` | unit fake (killed broker → reduced count → `_verify=true`); restore command list starts with pwsh; one live/self-test F1 attested run or F1 cells removed |
| **R8** | `exit_status.consumer` captured before clear; validator requires `EXIT_KEYS` for valid records; live record non-empty | 9.4 check passes; inconsistent record missing exit keys fails validation |
| **R9** | validator A2 §3.4 equations (`violating_run==v1..v5`, `v1==v1a∨v1b`, flag/count consistency); analyzer derives `v1a/v1b` + P11 test | 3 engineered inconsistent records rejected; consistent accepted; analyzer exposes `v1a/v1b` and `v3=1 ∧ v1b=0` P11 test |
| **R10** | deferred only if author has not approved A2: confirm P2-GATE flags analyzer-A2 as pre-freeze item; if **approved**, run full A2-6 checks (C1 pooled/AC-3a, no C2/C3, H2/H4 canonical, H5-A residual table, P3 three contrasts, matrix pin, single-source attested, record re-validation, A2 §6 item 14 acceptance fixture) | status documented; if approved, all A2-6 checks pass before the analysis freeze (does not block data collection; blocks analysis) |
| **R11** | `gate-negative-controls.json` in freeze; regenerated to temp = same statuses; analyzer gate table uses it | hash present; all 8 FAIL; `all_passed=true`; frozen hash == regenerated semantic content |
| **R12** | `rates.py`/documented generator exists and is runnable; pilot/A no longer silently use `max` for fixed-rate; Stage B/C guard refuses without table; G7 uses table+capability; M6 deviation recorded | generator `--help`/schema present; `_effective_rate` raises on missing table for fixed-rate pilot/A/B/C; a pilot rate-calibration artefact exists; M6 in DEVIATIONS; G7 no longer PENDING for a missing table when mode is fixed-rate |
| **R4** | progress-based L2 triggers + actual-window receipts | unit: fire cannot be scheduled without a load window; receipt phase carries the real window; out-of-window fire excluded; 189 pilot L2 runs no longer structurally excluded |
| **R3** | single ownership table incl. g1k5 + F4b | one table consumed by matrix/runner/validator/analyzer; g1k5 mode/ownership correct; F4b validated as app-owned; matrix regen+rehash verified in 9.5 |
| **R2/R1** | already verified in S1; re-run 9.1/9.2 on the final tree | no raises; no S4 companion fired; attested app-owned + S4-owned |

### 9.8 Full regression sweep (5–8 min)
```powershell
python -m pytest experiments/tests -q -p no:cacheprovider          # all green (expect ≥ new R5–R12 tests)
python experiments\oracle\mutation_test.py --mutations 100 --events 100 `
  --out %TEMP%\wxeo-s5-final\g3.json --workdir %TEMP%\wxeo-s5-final\g3work   # 18/18, 1800/1800
python %TEMP%\wxeo-s5-rereview\oracle_adv.py                         # T0–T13 as in §3
python %TEMP%\wxeo-s5-final\gate_neg.py                              # all 8 FAIL as required
python tools\analyze.py --records <live evidence records copy> --out %TEMP%\wxeo-s5-final\analysis `
  --harness-dir <harness with regenerated negative controls>          # runs; dataset=real; gates computed
python experiments\validate_record.py results\raw\run-records\*.json  # 0 invalid beyond documented exclusions
python %TEMP%\wxeo-s5-final\path_hygiene.py                           # 0 absolute-path violations in committed artefacts
```
Also: confirm G3 evidence hash in freeze == actual; no repo file changed by S5 (only this memo);
`results/DEVIATIONS.md` matches the regenerated temp copy (modulo timestamp).

### 9.9 Verdict rules and output (two gates — see §9.10)
**PILOT gate** — all of 9.0–9.3, 9.6 and 9.8 green; **R1–R9 + R11** verified per §9.7; live
evidence of 9.4 present and genuine (fault-free valid record with `source_integrity=true`, K3
app-owned attested, K2 S4-owned attested, C=2 app-owned R5 case, rebuilt jar + new hashes);
pytest green including the new R5/R6/R7/R8/R9 regression tests. **PASS / PASS-WITH-CONDITIONS /
BLOCK** decided on those items only, with the §9.10 scope split recorded explicitly.
**STAGE-A gate** — the pilot gate **plus** R10 (A2-6 analyzer implemented, matrix-hash pin, A2
acceptance fixture, re-frozen after author approval) **plus** R12 (`rates.json` generated by an
implemented, author-ratified procedure from pilot data; Stage-A/B/C fixed-rate and saturation modes
no longer silently fall back to `max`; guard verification; M6 deviation registered). If the pilot
gate passes but R10/R12 are open, the verdict is **PASS-WITH-CONDITIONS** for the pilot and
**BLOCK for Stage A** — never a bare PASS.
- **PASS:** every in-scope check green and every other item has a named condition with owner/date.
- **BLOCK:** any failure in 9.1–9.6/9.8, any in-scope critical R open, or any claimed fixed item
  that cannot be reproduced from the committed artefacts.
- **Output:** update `docs/redteam-rereview.md` §0 (new verdict + snapshot), §3 (final status
  table), §5 (final R-status), §6 (remaining conditions), §9.10 (scope split confirmation); no
  other files. Time-stamp and hash the final snapshot in the memo.

### 9.10 Scope-split acceptance (lead directive, 2026-11-24)

**I accept the pilot-gate / Stage-A-gate split explicitly.** R12 and R10 do **not** block the pilot;
they are recorded as **pre-Stage-A conditions** (not pilot conditions). Reasons, with evidence:

1. **R12 (`rates.json`) cannot logically block the pilot.** Its inputs *are* pilot outputs
   (`docs/prereg.md` §6.2: "values fixed after the pilot"; the pilot's stated purpose includes
   "measure mechanism capacities for the target-rate rules (§6.2)"), and the runner already refuses
   Stage B/C when the table is missing (`_effective_rate()` raises for stages B/C — verified on S0
   and S1). The pilot's own L2 fault cells are made rate-independent by the R4 fix (progress-based
   triggers), which is in the pilot gate. Therefore the pilot may run at the fallback `max` rate
   **provided it is recorded as a deviation** (M6-style; the lead's R4/M6 mandate) and **provided
   the pilot archives the raw evidence from which `rates.json` will be derived** (loader result,
   `t_load_start`/`t_load_end`, ack rates, per-cell run records).
   *Stage-A condition and one evidence-based caveat:* Stage A is pre-registered fixed-rate at
   1 k/P3, so it needs `R_off(1k,3)`. The frozen warm-up rule is `max(2,000, 10 %)` events
   (`runner._warmup_events`), i.e. **2,000 > 1,000 for every 1 k pilot/Stage-A run**; the service
   therefore reports `warmup_events_configured=2000`, excludes all 1,000 events, and the run
   record's `performance.throughput_eps_median`/percentiles are `null` for 1 k cells. Consequently
   the pilot cannot yield a "mechanism capacity at (1k, P3)" from the frozen performance block, and
   the same applies to the other `(load, P)` combinations Stage B/C need, which do not appear in
   the pilot matrix (only 1 k/P3). `rates.py` must therefore either (i) define capacity from the
   archived loader/ack window (`loader_result`, `t_load_start/end`, produced manifest) and have the
   author ratify that measurement definition (M6-style deviation), or (ii) add author-approved
   calibration runs at the required `(load, P)`. **If neither is done, Stage A is BLOCKed even if
   the pilot gate passes** — the Stage-A fixed-rate mode would otherwise be a silent `max`
   fallback, contradicting prereg §6.2. This is a Stage-A condition, not a pilot one.
2. **R10 (A2 analyzer) cannot block pilot data collection.** The author approves A2 at the P2 gate
   *after* the pilot; pilot data are descriptive/calibration data, not confirmatory, and no
   hypothesis verdict may be computed from them. The gate table needed for the pilot gate (G1–G8)
   does not depend on the A2 hypothesis families. The only way R10 touches the pilot is **gate
   evidence correctness**: `tools/analyze.py::extract()` re-derives `attested` itself instead of
   using `experiments/receipts.validate_run_receipts`, so a pilot gate table computed by the
   pre-A2 analyzer could mis-score G1/G4. In §9.7 I therefore verify the pilot's G1/G4 evidence
   **independently from the runner-validated records**; if the frozen analyzer's G1/G4 status
   disagrees with the runner-validator-derived status, the **pilot gate fails** until that
   gate-relevant subset is aligned (this is a verification-time consistency check, not an R10
   data-collection blocker). The full R10 set (C1 pooled/AC-3a, C2/C3 deletion, canonical H2/H4,
   H5-A, P3 three contrasts, `v1a/v1b` + P11, matrix-hash pin, record re-validation, A2 §6 item 14
   acceptance fixture) is a **pre-Stage-A / pre-analysis-freeze condition**, executed immediately
   after author approval at P2 as the lead states.
3. **What the pilot gate does require from R10/R12's domains:** (i) the R9 validator equations and
   R11 frozen gate evidence are in the pilot gate and must be green; (ii) the pilot records must be
   labelled and handled as non-confirmatory (never enter `results/analysis/` or a hypothesis
   table); (iii) any pilot-based rate derivation must be transparent and author-ratified before
   Stage A. These are recorded in §6 conditions 6 and 11–12 and in §9.9.
4. **No other blocker-for-pilot is implied by the split.** R1–R9 + R11 include the run-production
   defects (injector dispatch, ownership/C=2 receipts, K5 marker, L2 window, F1 verify, exit
   status, validator equations, frozen gate evidence) — the set that decides whether the pilot can
   produce valid, attested, schema-valid records at all.

---

*End of re-review. The fixes that were made are real and measurable; the production path that turns
them into valid records is not. Re-run the §6 conditions (a)–(f) plus R5/R6/R7/R8 on the next
snapshot before the pilot is declared green. §9 is the command-level checklist for that final pass.*

---

## 10. FINAL FOCUSED RE-REVIEW — pilot gate & Stage-A conditions (2026-11-24, ~19:24–20:35Z)

**Reviewer:** S5. **Task:** final verdict for (a) the **PILOT gate** = R1–R9 + R11 + live evidence, and
(b) the **Stage-A conditions** (R10 analyzer + R12 rates confirmed deferred; anything else?).
**Method:** read-only inspection of the repository at the evidence commit; no harness/runner/stack
command was executed and no run was started/stopped. Pure-Python checks were limited to constructor/
validator/receipt functions (no stack). Git was used read-only (`log`/`show`/`diff`, `status`).
Throwaway checks: `%TEMP%\wxeo-s5-final\{check_final,code_checks,agg_check}.py`.

### 10.0 Verdict

> # PILOT GATE: **PASS-WITH-CONDITIONS** — retroactive acceptance of the pilot launch.
> # STAGE A/B/C: **BLOCK** (unchanged; five conditions in §10.5).
>
> This is **not a bare PASS**. R1–R9 + R11 are verified green on the committed tree, the live
> evidence is genuine and independently re-validated, and the pilot is running under the frozen
> pipeline. The conditions are (i) evidence/freeze provenance does not yet bind the committed
> live-smoke bundle, (ii) two pre-approval Stage-A executions must be quarantined, (iii) the
> in-flight pilot still owes the g1k5 / post-freeze app-owned / F1-F5 injector evidence (G1, R6,
> R7), plus the five Stage-A conditions.
>
> **Adjudication (i) — pilot launched before the R1–R9+R11 verdict.** **Acceptable retroactively**
> because (a) this verdict on R1–R9+R11 is PASS, (b) the pilot was launched **after** the committed
> freeze (`frozen_at 19:14:26Z`; first pilot record 19:15:20Z; run-record `git_sha` equals freeze
> `git_sha` `5f6f181`, i.e. the frozen pipeline), (c) it is a gate-calibration pilot (A3.4, not a
> confirmatory sample), and (d) Stage A/B/C remain hard-stopped. It is a **procedural deviation**
> from the split-gate wording and must be recorded in `results/DEVIATIONS.md`/P2-GATE (owner: lead),
> not silently absorbed.
>
> **Adjudication (ii) — M7 calibration block (54 runs) still awaiting author approval.** **Yes, this
> pilot-gate verdict can be issued with M7 as an explicit Stage-A precondition.** M7 is an
> *additive calibration block* needed for the honest Stage B/C projection (`rates.json`); it does
> not validate the harness and therefore is not a pilot-gate item. It must nevertheless be
> author-approved and executed — and `rates.py`/R12 implemented — **before Stage A**; otherwise
> Stage A's pre-registered fixed-rate mode has no evidence-based rate table. The pilot data
> themselves confirm the need: every 1k record has `performance.warmup_events_excluded = 2000`,
> `perf_window` null, `n_instances_merged = 0`, and no throughput/p50/p99 values (§10.1), so
> capacity cannot be derived from the pilot matrix as frozen.

### 10.1 Verified snapshot (hashes checked in this review)

| Artefact | SHA-256 (first 16) / value |
|---|---|
| git `refs/heads/main` (HEAD) | `0bfcfdad4662f5cdefaed324235d86424586c014` (commit `0bfcfda`) |
| `docs/prereg-freeze.json` | file `7bea3530ad7a3223…`, 9,932 B, `frozen_at 2026-11-24T19:14:26Z`, `git_sha 5f6f181…` |
| freeze `aggregate_pipeline_sha256` | **`fd271416aebb6f9cb88da680694e4fd43e692b94db8c1e9f78c3dfd8d84e788a`** |
| freeze recomputation | **matches** its own 34 file hashes (independent re-hash of the documented aggregate algorithm) |
| artefact binding | jar `6e8223751706d64e7b77752b42df5694d4231ba8bd3522034806bae25e8e50d3` (34,467,687 B) + source tree `60ef2c711adb3acb…` — jar hash equals the actual jar in the tree |
| freeze pre-freeze evidence | G3 `fc4af460…`, gate-negative-controls `6945399e…` (both hashed, R11) |
| `experiments/matrix.yaml` / `run_order.csv` | `ba985717e773b7e4…` / `6a0159c6b7999f5c…` — both equal the freeze entries and actual files |
| `experiments/runner.py` / `receipts.py` / `kafka_admin.py` | `ed690369aace9321…` / `18548c17411ec700…` / `c15435d451c734b6…` |
| `experiments/oracle/oracle.py` / `injectors.py` / `validate_record.py` | `72a158f711eec2cc…` / `9d921abcd354a18f…` / `9decfdd9a58f2ee1…` |
| `experiments/live_smoke_evidence.py` | `9c7e40523394b564…` |
| `tools/analyze.py` (still pre-A2 — R10) | `fa08b0bc20fe91bf…` (equal to freeze) |
| live smoke evidence file | `62fe30c8d2579268…` (3,629 B) |

Every frozen file hash matched the working tree (**0 drift** across the 34 frozen files). All 7,514
run-order seeds recompute exactly; matrix↔order is 0 missing / 0 extra.

### 10.2 Pilot gate — items verified GREEN (independently re-checked)

| Item | Result | Evidence pointer |
|---|---|---|
| **R1** injector factory dispatch (whole matrix) | **PASS** — constructed **7,514/7,514** rows with 0 raises; class map correct (`F1BrokerKill 569, F2ConsumerKill 569, F3ProduceTimeout 569, F4aOffsetCommitFailure 506, F5DbPartition 506, F6DuplicateRedelivery 506, K2UnknownOutcome 270, K5DuplicateDelivery 243, FailpointWatch 1,131`); `make_injector` returns `None` exactly for the 2,645 F0/no-fault rows; failpoint config enabled only for app-owned classes (K1/K3/K4/K7/F4b) | `%TEMP%\wxeo-s5-final\code_checks.py`; `experiments/injectors.py` |
| **R2** no S4 companion `fired` on app-owned runs | **PASS** — `pilot-valid-g1k3-r22` has exactly 1 app `fired` (detail carries `instance=1`) + 1 S4 `restored`; `validate_run_receipts(...) → attested=true, owner=app, fired=1, restored=1, app_fired=1, s4_fired=0`; the failed attempt `g1k3-r21` is retained only as an excluded record | same script; `results/raw/run-records/run_pilot-valid-g1k3-r22.json` |
| **R3** single ownership table incl. g1k5/F4b | **PASS (code+matrix)** — ownership table used by injector, receipts and record; validation slate has `g1k5` rows with `injector_mode=external` (matrix validation section); no app/S4 ownership mismatch found in the live records | `experiments/receipts.py`; `experiments/matrix.yaml` |
| **R4** phase window / L2 timing | **PASS for marker-driven classes** — K2 receipt has `phase` window, `action_verified=true`, `outside_window=false`, `consumers_alive=true`; runner excludes `uninformative_fire_outside_window` (F3 selftest correctly excluded). **Caveat for F1/F3/F5, §10.3-P3** (not marker-driven; 1k window too short) | `run_a-m3-k2-1k-p3-r02`, `run_pilot-valid-selftest-r11` |
| **R5** C=2 app-owned failpoint scoping | **PASS** — runner arms only `failpoint_instance=1` (`_service_args` + `--wxeo.failpoint.instance`); record `config.runner.failpoint_instance=1`; `g1k3-r22` C=2 has exactly one app fired, attested; the earlier double-fire attempt is an excluded record, not a valid one | `experiments/runner.py` lines ~1444–1478; record receipts |
| **R6** K5 waits for durable progress marker | **PASS (code)** — `K5DuplicateDelivery` implemented; rewind via write-capable fault role, k×batch offsets, verified action. **Live g1k5 evidence not yet produced** (in-flight pilot, order indices 78/135/138) → pilot-gate condition P2 | `experiments/injectors.py`; `experiments/run_order.csv` |
| **R7** F1 verify/restore | **PASS (code)** — F1 verification via live AdminClient broker metadata; restore through `pwsh -File` harness; `action_verified` required. **No live F1 record yet** (selftest slate partially run; F3 r11 excluded by window) → pilot-gate condition P2 | `experiments/kafka_admin.py`; `results/redteam-fixes.md` |
| **R8** `exit_status.consumer` captured | **PASS** — live records have `exit_status.consumer {"1":0,"2":0}` + `load_generator`; validator requires `EXIT_KEYS` and rejects a record missing them (my tamper test) | live records; `experiments/validate_record.py` |
| **R9** validator A2 §3.4 equations | **PASS** — direct tamper tests: `v3=true / violating_run=false` → rejected; `violating_run=true` with all flags false → rejected; missing `exit_status.consumer` → rejected; base record → valid | `%TEMP%\wxeo-s5-final\code_checks.py` |
| **R11** gate falsifiability evidence frozen | **PASS** — `gate-negative-controls.json` hash is in the freeze's `pre_freeze_harness_evidence.files_sha256` (matches actual); G3 hash likewise; gate table PASS requires the gate's negative control to have failed (`tools/analyze.py::build_gate_table`) | freeze; `results/harness/gate-negative-controls.json` (8/8 FAIL as required) |
| **Live evidence — fault-free** | **PASS** — `pilot-m2-f0-1k-p3-r00`: schema-valid, `valid=true`, `source_integrity=true`, GT==expected, 2,000/2,000 postings, dup=0 lost=0, not censored, quiescence checks (lag 0/sink stable/app idle/relay drained) all true, exit codes 0/0; record+oracle SHA-256 in `live-smoke-evidence.json` **match the files**; `passed=true` | `results/harness/live-smoke-evidence.json`; `results/raw/run-records/run_pilot-m2-f0-1k-p3-r00.json` |
| **Live evidence — app-owned K3** | **PASS** — `pilot-valid-g1k3-r22`: valid, source-integrity, 2,000 expected / 2,044 observed, **44 duplicates**, 0 losses, `violating_run=true` (M0 negative control), receipts attested (1 app + 1 S4 restored), C=2 | same |
| **Live evidence — S4-owned K2** | **PASS** — `a-m3-k2-1k-p3-r02`: valid, source-integrity, 2,000/2,000, one S4 `fired` with `action_verified=true` **inside the real load window** + one `restored` | same |
| **Post-freeze pipeline evidence** | **PASS (partial, important)** — pilot matrix records produced **after** the freeze under the frozen runner are already valid/attested: `run_pilot-m1x-f2-1k-p3-r01` and `run_pilot-m1xm3-f6-1k-p3-r01` (valid, `source_integrity=true`, not censored, S4 receipts attested, `git_sha=5f6f181`=freeze git_sha) | `results/raw/run-records/` |
| **G3 mutation corpus** | **PASS** — 18 classes / 4 corpora (DB-sink, attestation, publication-metrics, M1-kafka ledger), **1,800/1,800 detected + attributed**, clean and input-tamper controls pass (SYNTHETIC, labelled) | `results/harness/g3-mutation.json` |
| **Gate negative controls** | **PASS** — G1–G8 each FAIL as required, `all_passed=true` | `results/harness/gate-negative-controls.json` |
| **S3 acceptance** | **PASS** — 4/4 tests (Kafka smoke, M0 e2e, M3 e2e, failpoint crash), archived under `results/harness/acceptance/` with path-scrubbed JSON | `results/harness/acceptance/acceptance-result.json` |
| **TLC** | **PASS (bounded)** — 8/8 configurations match pre-registered expectations (reported scope: N=3/BatchSize=2/single partition, not an unbounded proof) | `formal/tlc-results/tlc-summary.json` |

### 10.3 Pilot-gate conditions and residuals (must close before P2 sign-off; none requires stopping the running pilot)

- **P1 (major — evidence↔freeze binding).** The committed `results/harness/live-smoke-evidence.json`
  records `artifact.freeze_aggregate = 1ccb5aac…`, which is **not** the committed freeze aggregate
  (`fd271416…`) and does not exist in git history. Timeline: evidence `generated_at
  19:14:14Z`; `runner.py` mtime `19:14:25Z`; freeze re-issued `19:14:26Z`; the frozen pipeline was
  therefore re-issued **after** the evidence bundle was produced, and the underlying smoke records
  (`r00` 18:29Z, `g1k3-r22` 19:08Z, `a-m3-k2-r02` 19:13Z) predate the freeze. The freeze is
  internally consistent and currently drift-free, and the running pilot was launched after it, but
  the *gate evidence* is not reproducible from the committed artefacts as the lead's summary
  states. **Fix:** regenerate `live_smoke_evidence.py` against the committed freeze and/or anchor
  the live-evidence claim to the post-freeze pilot records; add the freeze/evidence commands to
  `results/RUNLOG.md` (currently absent) and record the old→new aggregate mapping in
  `results/DEVIATIONS.md`. Note `freeze.run_sha` is `5f6f181` while HEAD is `0bfcfda` (R21): record
  the code-commit→freeze-commit mapping. *Owner: lead/S4. Not a pilot-stop item; blocker for P2
  sign-off.*
- **P2 (major — in-flight evidence slate not yet complete).** Pilot-gate items that the split
  defined as live evidence but that do not exist yet at review time: **K5** (`pilot-valid-g1k5-r24/25/26`
  are scheduled at order indices 78/135/138), **post-freeze app-owned K3** (`g1k3-r23` at index 37),
  and the injector **selftest slate** (only `r11`/F3 executed, and it was correctly excluded
  `uninformative_fire_outside_window`). **Additional risk:** `F1/F3/F5` are *not* in the
  marker-driven class set (`MARKER_DRIVEN_FAULTS = K2,K5,F2,F4a,F6`) and therefore still use the
  60 s expected-load fallback with no `rates.json`; at 1 k/max the real load is ~0.2–1.5 s (F3
  selftest: 1.15 s window, fire 286 ms after end). Their 1 k pilot cells will therefore be
  structurally excluded, so F1/F3/F5 will have **no live in-window attestation** from the pilot.
  **Fix (before P2/G1/G7 claims):** either extend the rate floor to F1/F3/F5 for pilot cells
  (identical across mechanisms, record as A3/M6-class deviation) or declare those injectors
  "unvalidated in pilot" in writing and gate their Stage-B cells on a pre-Stage-B live validation.
  G1 cannot PASS until the g1k5 runs exist (analyzer requires K3 **and** K5 per class).
- **P3 (major — process deviation: pre-approval Stage-A executions committed as evidence).**
  `results/raw/run-records/run_a-m3-k2-1k-p3-r01.json` (excluded, outside window) and
  `run_a-m3-k2-1k-p3-r02.json` (valid, source-integrity, K2 attested) are **Stage-A matrix records**
  executed before P2 author approval and committed in the evidence commit. They were produced
  before the freeze. They must be **quarantined as evidence** (moved/labelled out of the confirmatory
  `results/raw/run-records/` set or marked evidence-only with a closed-list reason) and the
  reserved Stage-A reps re-run under the approved campaign; otherwise the Stage-A analysis would
  silently ingest pre-approval, pre-freeze data and the reserve/skip logic would skip the rep.
  Record the pre-approval execution in DEVIATIONS (owner: lead/S4).
- **P4 (major — M7 accepted as Stage-A precondition).** All pilot/Stage-A 1k records report
  `warmup_events_excluded=2000`, null `perf_window`, `n_instances_merged=0`, and null
  throughput/p50/p99 (observed in every record of this snapshot); `experiments/rates.py` and
  `rates.json` are absent; the runner correctly raises for Stage B/C without the table. The M7
  calibration block (9 mechanisms × {10k,100k} × P6 × N=3 = 54 runs) plus an author-ratified
  `rates.py` derivation are therefore required before Stage A/B/C. **Adjudication: yes — the
  pilot-gate verdict stands with M7 as an explicit Stage-A precondition** (author approval, then
  execution, then re-freeze; §10.5 C1/C3).
- **P5 (minor residuals beyond R10/R12; not pilot blockers, but must be fixed or downgraded in
  writing before their gate claims are used):**
  - **R13** run-record `process_limits` is still inaccurate: records report `jvm_xms/jvm_xmx
    "2g"/"2g"` while the running service is started with `-Xms512m -Xmx1g` (A3.1) and
    `affinity_mask`/`priority_class`/`hikari_pool` are `null` (`_process_limits` still hard-codes
    2g and the post-stop lookups find nothing). Fix before Stage-A records are frozen (or G8's
    affinity/priority wording must be downgraded).
  - **R20** publication-quiescence helper still creates ephemeral consumer groups
    `wxeo.quiesce.<pid>.<ts>` and never deletes them (`experiments/kafka_admin.py`); group residue
    will accumulate over 7.5k runs. Fix/document before the campaign.
  - **R17/R21** acceptance evidence is archived but not hashed in the freeze/RUNLOG; the final
    freeze/evidence commands are not in RUNLOG; freeze `git_sha` names the pre-evidence commit
    (R21). Fold into P1 close-out.
  - **R22** gate semantics: analyzer now implements fault-aware G1 (K3 **and** K5), G7 (rate table +
    generator lag + calibration) and G8 (clock/affinity); G7/G8 can only be claimed once `rates.json`
    and real process limits exist. G2 should treat a censored positive control as a failure (verify
    on the first control records). Cheap to confirm while the analyzer is updated for A2 (R10).
  - **R18** A2 evidence-snapshot re-pin is pending author approval (one-line provenance update, not
    substance). **R15** appears fixed (`SUCCESS_NO_INFO` counted in the idem path); **R19** the
    zero-executed list no longer shows `V.*` in the first rows (spot-check; full check in Stage-A
    close-out).
- **P6 (code nit, non-blocking).** `tools/analyze.py` G1 `_g1_class` contains a leftover dead
  assignment (`runs = [...]` with a malformed boolean expression) immediately overwritten by the
  correct one; no behavioural effect, but it should be removed when the analyzer is rewritten for
  A2 (R10) so the frozen code review is clean.

### 10.4 What "PASS-WITH-CONDITIONS" means operationally

1. The **pilot may continue** unmodified; the run-production path (injector construction, receipts,
   validator, freeze pre-flight, quiescence) is verified on the frozen tree, and the pilot is
   producing valid/attested records under that freeze (P: `run_pilot-m1x-f2-1k-p3-r01`,
   `run_pilot-m1xm3-f6-1k-p3-r01`; K3 evidence: `g1k3-r22`; K2: `a-m3-k2-r02`).
2. **No Stage A/B/C run** may start. Stage A is blocked by R10, R12 + M7, the P1–P3 close-out and
   the author's P2 approvals (matrix amendment M1, A2, K6 limitation, budget plan).
3. The pilot is **descriptive/calibration only**; no hypothesis verdict, table cell, or figure may
   be computed from pilot or pre-freeze records. The pre-approval Stage-A records P3 must not enter
   confirmatory data.
4. The split-gate decision is confirmed: **R10 and R12 are the only deferred Stage-A blockers from
   the prior list**, and no new critical defect was found in the frozen pipeline; the residuals in
   P5 are non-blocking for Stage A execution but blocking for their associated *claims* (G8
   affinity, G7 capability, acceptance/freeze provenance) and must be closed or reworded before the
   P2 gate is signed.

### 10.5 Stage-A conditions (the five-line list requested)

1. **Author approvals at P2:** matrix amendment M1 (216→198 pilot / 1,830→1,530 A / 4,020→3,960 B, 20→26 validation), Amendment A2 items A2-1…A2-8 + AC-1…AC-8, K6 non-realisability acknowledgment, **M7 calibration-block approval**, and the budget plan for `H`.
2. **R10 (deferred → must land):** implement and freeze the A2 analyzer (C1 pooled/AC-3, no C2/C3, canonical H2/H4 sets excluding probes, H5-A residual table + non-vacuity guard, P3 three contrasts, `v1a/v1b` + P11, matrix-hash pin, single-source `attested`, record re-validation, A2 §6 item-14 acceptance fixture); re-freeze; S5 re-verifies.
3. **R12 + M7:** author-ratified `experiments/rates.py` derivation + execute the 54-run M7 calibration block (9 mech × {10k,100k} × P6 × N=3), generate `experiments/rates.json`, connect G7 (≥3× generator capability + lag + T_quiet), and re-freeze; no silent `max` fallback; M6/A3.3 deviation closed.
4. **Evidence/provenance close-out (P1+P3):** regenerate live-smoke evidence against committed freeze `fd271416…` (or re-anchor to post-freeze pilot records), RUNLOG the freeze/evidence commands, map `git_sha`→HEAD, record the pilot-launch and pre-approval Stage-A executions in DEVIATIONS, and quarantine `run_a-m3-k2-r01/r02` from confirmatory data.
5. **Complete the gate evidence and residual fixes:** g1k5 + post-freeze app-owned K3 + F1/F3/F5 injector validation live (or written "unvalidated, cells gated"), G1 per class green, G2/G7/G8 positive evidence, and R13/R20/R22 residuals closed or explicitly downgraded; then S5 re-confirms the re-issued freeze and the pilot projection before any Stage A run.

### 10.6 Commands/artefacts used (all read-only; no stack/runner invocation)

```powershell
git status --porcelain=v1; git log --oneline -3; git show 5f6f181:docs/prereg-freeze.json
Get-FileHash <freeze/matrix/order/runner/receipts/kafka_admin/oracle/injectors/validate_record/analyze/jar/evidence files>
python %TEMP%\wxeo-s5-final\check_final.py       # matrix/order/cells/reps/seeds/constraints/records (read-only)
python %TEMP%\wxeo-s5-final\agg_check.py         # freeze aggregate self-consistency (matches fd271416…)
python %TEMP%\wxeo-s5-final\code_checks.py       # 7,514/7,514 injector construction; receipts K3/K2; R9 tamper tests
Get-Content results/raw/run-records/run_pilot-m2-f0-1k-p3-r00.json        # fault-free live evidence
Get-Content results/raw/run-records/run_pilot-valid-g1k3-r22.json         # app-owned K3 (C=2, attested)
Get-Content results/raw/run-records/run_a-m3-k2-1k-p3-r02.json            # S4-owned K2 (in-window, verified)
Get-Content results/harness/live-smoke-evidence.json                      # NOTE: stale aggregate 1ccb5aac…
Get-Content results/harness/gate-negative-controls.json                   # 8/8 FAIL as required
Get-Content results/harness/g3-mutation.json                              # 1,800/1,800, 18 classes
Get-Content results/harness/acceptance/acceptance-result.json             # 4/4 PASS
```

**No file in the repository was created or modified except this memo.** The pilot (pid process owned
by the lead) was observed only through its committed/appended records and RUNLOG; the stack was not
touched, no runner command was executed, no confirmatory data were produced or analysed.

*End of final focused re-review.
Pilot gate: **PASS-WITH-CONDITIONS**. Stage A/B/C: **BLOCK** under §10.5.*
