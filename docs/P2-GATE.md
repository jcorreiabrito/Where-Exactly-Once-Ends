# P2 GATE — pilot evidence, compute projection, and author decisions

Status: **DRAFT — awaiting pilot completion.** Hard stop per A1 §12: no Stage A/B/C run before
author approval of (i) the matrix amendment, (ii) pre-registration Amendment A2, and (iii) the
projected compute budget.

## 1. What changed since P1 approval (all disclosed, nothing silent)

| # | Change | Authority | Impact |
|---|---|---|---|
| M1 | Matrix amendment: pilot 216 planned / **198 executable** (was 201); Stage A **1,530** (was 1,830); Stage B **3,960** (was 4,020); Stage C 1,800 (unchanged); total **7,506 planned / 7,488 executable** (was 7,866/7,851); validation **26** (was 20). | S5 early red-team required truthful K-class realisability | Removed cells: K6 (zombie) for all 9 mechanisms (−270 A-runs) and M1-kafka K5 (−30); M1-kafka F6 n/a in pilot/B (−63). Added: 6 G1 gate-control validation runs. `experiments/matrix.yaml` and `run_order.csv` regenerated and re-hashed. |
| M2 | K6 (zombie writer) is **not experimentally realisable** within the pre-registered `T_max = 300 s`: the recipe requires pausing an owner beyond `max.poll.interval.ms = 300 s`. E3/C7/P9 and the M4+M3 zombie rationale therefore rest on the formal/pen-and-paper argument, not on Stage A data. | S5 + S2 amendment A2-7 | H4 loses its K6 clauses; H5 residual table footnotes E3 as model-only; paper threat statement added. |
| M3 | Pre-registration **Amendment A2** (8 items): corrected satisfiable H1–H5 decision rules, censored⇒violating, I1a/I1b + spurious `v5` verdict, matrix arithmetic, seed-encoding clarification, analyzer checklist, TLC scope narrowing. | S5 findings F-P1/F-O3/F-O4/F-P2/F-P3/F-P5/F-F1 | Hypothesis verdicts become computable; no matrix/N/statistic changes beyond M1. |
| M4 | Harness fixes from S5's BLOCK memo: oracle live-dump `time` bug, receipt protocol v2 (one fired+restored, phase-verified), Stage-A fault wiring/tokens, AdminClient quiescence, falsifiable gates G1–G8 with negative controls, performance pipeline (warm-up exclusion, committed-event throughput, `send_ts_ms`), real fsync, path hygiene. | S5 memo §14 must-fix list | S5 re-review required before freeze (see §4). |
| M5 | **Operational deviation — Kafka topic lifecycle.** Topic deletion is fatal on this Windows KRaft deployment (partition-dir rename fails: `Error while renaming dir for <topic>` → all log dirs offline → brokers shut down). The campaign therefore **never deletes topics per run**: fresh state comes from unique topic names/group/schema per run, and storage is reclaimed by periodic full-stack `reset-kafka` with load-dependent cadence (1k→50 runs, 10k→25, 100k→20, 1M→every run); brokers also set `delete.topic.enable=false` as a safety net. | Platform constraint discovered in S3's live acceptance run | No change to cells/N; affects reset cadence and disk accounting only. Recorded in `results/DEVIATIONS.md` (M5) and the paper's threats/environment paragraph. |
| M6 | **Fault-trigger implementation.** Level-2 stochastic faults fire on a seeded progress marker (25–75 % of the intended event count, recorded) and the receipt records the actual observed load window, instead of a wall-clock delay. This is required because `rates.json` is a pilot output; the pre-registered wording says "random recorded times". | Implementation constraint (S5 R4) | No change to cells/N; trigger semantics are recorded per receipt; the paper must state deterministic progress-based triggers (stochastic across repetitions by seed). |
| M7 | **Warm-up vs calibration (S5 §9.10 condition).** The frozen warm-up rule `max(2000, 10 % events)` excludes *all* events of a 1k run, so pilot/Stage-A runs have null performance fields by construction, and mechanism capacity cannot be derived from them. Proposal: (i) pilot adds a **calibration block** of 9 mechanisms × loads {10k, 100k} × P6 × N=3 = **54 runs** to measure capacity from committed events outside warm-up and loader/ack windows; (ii) `rates.py` derives fixed target rates from that block by a frozen rule (e.g. 50 % of the minimum mechanism capacity at the stage's load/partitions), author-ratified; (iii) Stage A 1k runs are executed at `rate=max` and analysed **for correctness only** (performance claims come from Stage C), recorded as a deviation. | S5 re-review §9.10 | Adds 54 pilot runs (≈ +25–35 min at 100k); makes fixed-rate Stage B/C rates evidence-based; Stage A performance claims are explicitly out of scope. **Author decision requested.** |

## 2. Harness validation evidence (gates, pre-pilot)

All pre-Stage-A harness conditions are now satisfied on the live stack; none of this is mechanism
data. Evidence files are committed under `results/harness/`.

| Condition | Result | Evidence |
|---|---|---|
| S3 artifact acceptance (live end-to-end) | **4/4 passed** | `results/harness/acceptance/acceptance-result.json`, `acceptance-suite.log` |
| Oracle checker mutation testing (G3) | **1,800/1,800 detected (100 %), 1,800/1,800 correctly attributed, 18/18 classes pass**; clean controls and input-integrity controls pass; corpus is SYNTHETIC (harness validation, not a mechanism measurement) | `results/harness/g3-mutation.json` (`overview.all_classes_passed=true`) |
| Gate negative controls (G1–G8 can fail) | **8/8 fail as required** | `results/harness/gate-negative-controls.json` (`all_passed=true`) |
| Live end-to-end smoke (fault-free) | **passed=true**; record schema-valid, source_integrity=true, GT=expected, artifact matches freeze, receipts attested | `results/harness/live-smoke-evidence.json` (run `pilot-m2-f0-1k-p3-r00`) |
| Live fault attestation — app-owned class | **attested** (K3): one app `fired` + one S4 `restored`, 2,000 expected / 2,044 observed / 44 duplicates / 0 losses; M0 negative control produces the predicted I3 violations | `results/raw/run-records/run_pilot-valid-g1k3-r22.json` |
| Live fault attestation — S4-owned class | **attested** (K2): one S4 `fired` inside the real load window (`action_verified=true`) + one `restored`; 2,000/2,000, no duplicates/losses | `results/raw/run-records/run_a-m3-k2-1k-p3-r02.json` |
| Model checking (bounded witnesses) | **8/8 TLC configurations match expectations** (bounded scope N=3/BatchSize=2; not an unbounded proof) | `formal/tlc-results/tlc-summary.json` |
| Oracle independence | oracle and derivator hashes recorded; oracle reads only frozen vectors + raw dumps | `g3-mutation.json` → `oracle` block; `docs/redteam-oracle.md` |

Note: G3's mutation corpus is synthetic by design (docs/method.md §7.6 requires seeded mutations of a
clean fixture, not re-labelled real runs), so it validates the checker, not the mechanisms.

## 3. Pilot results and compute projection

Status: pilot RUNNING (launched 16:14 local; 198 executable + 26 validation rows, pace ≈41 s/run at
1k events). The table below is filled ONLY from measured pilot values (`docs/prereg.md` §9.2) and is
regenerated by `tools/projection.py` when the pilot finishes. Preliminary values from the first valid
runs are shown for the feasibility discussion and are explicitly marked preliminary.

| Run type | n | measured n | median (s) | p90 (s) | projected h (p90) |
|---|---|---|---|---|---|
| pilot matrix (1k/P3/C=2) | 198 | *pilot totals below* | | | |
| pilot validation | 26 | 3 | 36.0 | 43.2 | 0.31 |
| A crash-class + controls | 1,530 | 0 | – | 45.4 (preliminary) | 19.29 (+15 % = 22.18 h) |
| B core (100k/P6) | 1,980 | 0 | – | **not measured** | requires M7 calibration |
| B S1 (10k/P6) | 660 | 0 | – | **not measured** | requires M7 calibration |
| B S2 (100k/P3,P12) | 1,320 | 0 | – | **not measured** | requires M7 calibration |
| C core (fixed-rate) | 1,620 | 0 | – | **not measured** | requires M7 calibration |
| C 1M | 90 | 0 | – | **not measured** | requires M7 calibration |
| C capacity (saturation) | 90 | 0 | – | **not measured** | requires M7 calibration |

**Measured feasibility warning (P2 decision item).** The 1k/P3/C=2 run takes ≈36–45 s end-to-end on
this host. With the frozen counts, Stage A alone projects to ≈19 h (p90) / ≈17 h (median), i.e. it
already exceeds the 8–24 h budget once any contingency or Stage B/C is added. The largest frozen
cut-ladder step (correctness N 30→20) still leaves Stage A at ≈1,020 runs ≈13 h, and B/C cannot be
projected at all until the M7 calibration block is executed (the pilot is 1k-only and §9.2 forbids
estimates). **The pre-registered 7,506-run programme is therefore not executable within H = 8–24 h on
this single host.** The author must choose at this gate between: (a) a reduced, explicitly
pre-registered design (e.g. Stage A correctness only at reduced N, with B/C declared out of scope),
(b) the M7 calibration block followed by a narrow Stage B/C subset sized from measured 100k times, or
(c) extending the budget/host. No number in the paper may claim B/C evidence unless it is run.

Cut ladder (fixed, A1 §7 / prereg §2.8): step 1 drop C 1M (−90); step 2 drop B S2 (−1,320);
step 3 reduce correctness N to 20 (A 1,530→1,020; B core 1,980→1,320); step 4 escalate to author.
Measured values make step 4 unavoidable on this host; the exact reduction is the author's decision.

**S5 verdict (2026-11-24):** PILOT gate = PASS-WITH-CONDITIONS (retroactive launch accepted);
Stage A/B/C remain BLOCKED under 5 conditions — author P2 approvals incl. M7; A2 analyzer (R10);
`rates.py` + M7 calibration + re-freeze (R12); evidence/provenance close-out (regenerated evidence
against the committed freeze, RUNLOG rows, quarantine of pre-approval Stage-A records); complete gate
evidence (K5, post-freeze app-owned K3, live validation or gated declaration for F1/F3/F5) before S5
re-confirms the re-freeze. See `docs/redteam-rereview.md` §10.

## 4. Author decisions requested at this gate
1. **Matrix amendment M1** (counts above) — approve / reject.
2. **Amendment A2** items A2-1 … A2-8 + bounded choices AC-1…AC-8
   (`docs/prereg-amendment-A2.md` §10 approval sheet). Recommended: approve all with AC-1 = H5-A,
   AC-2 = no regeneration, AC-3 = pooled stratified exact sign test (AC-3b), AC-4 = at least one of
   M3/M4, AC-5 = `v5` violation, AC-6 = split I1a/I1b, AC-7 = narrow TLC claim, AC-8 = >20 %
   censoring flag.
3. **K6 non-realisability acknowledgment** (M2): accept that E3/C7/P9 evidence is
   model-and-argument only; no K6 data will be claimed.
4. **Budget plan** for `H` per §3 — including the measured feasibility warning: the full programme
   does not fit; choose the reduced design or extend budget/host.
5. **M7 calibration block** (54 runs: 9 mechanisms × {10k,100k} × P6 × N=3) — approve so Stage B/C
   can be projected from measured 100k times and so `rates.py` (R12) can derive the fixed rates.
6. **S5 re-review verdict** is a precondition: current status PASS-WITH-CONDITIONS for the pilot gate
   and BLOCK for Stage A/B/C until the five §10.5 conditions are closed.

## 5. Sign-off
- [ ] Matrix amendment approved
- [ ] Amendment A2 approved (item-by-item)
- [ ] K6 limitation acknowledged
- [ ] Budget plan approved
- [ ] Freeze hash recorded: `docs/prereg-freeze.json` → `<pending>`
