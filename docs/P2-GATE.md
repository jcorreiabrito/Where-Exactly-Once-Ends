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

## 2. Harness validation evidence (gates, pre-pilot)
_Pending final evidence runs. Required before Stage A: G1–G8 all pass with their negative controls
(`results/harness/gate-negative-controls.json`), checker mutation testing 100 %
(`results/harness/g3-mutation.json`), live end-to-end smoke with `source_integrity=true`, and the
pilot's own gate controls._

## 3. Pilot results and compute projection
_Pending. Table to be completed ONLY from measured pilot values (`docs/prereg.md` §9.2 worksheet):
per-run-type median/p90 reset+load+fault+drain+verify+archive times, projected hours per stage,
`T_total` incl. 15 % contingency, and the cut-ladder decision applied against the author's
8–24 h budget. No estimated number may appear here._

| Run type | n | median (s) | p90 (s) | projected h (p90) |
|---|---|---|---|---|
| Pilot 198 + 26 validation | 224 | — | — | — |
| A crash-class + controls | 1,530 | — | — | — |
| B core (100k/P6) | 1,980 | — | — | — |
| B S1 (10k/P6) | 660 | — | — | — |
| B S2 (100k/P3,P12) | 1,320 | — | — | — |
| C fixed-rate core | 1,620 | — | — | — |
| C 1M | 90 | — | — | — |
| C saturation | 90 | — | — | — |
| **Total** | **7,506** | | | — |

Cut ladder (fixed, A1 §7 / prereg §2.8): step 1 drop C 1M (−90); step 2 drop B S2 (−1,320);
step 3 reduce correctness N to 20 (A 1,530→1,020; B core 1,980→1,320); step 4 escalate to author.
Applied steps and resulting totals will be recorded in `results/DEVIATIONS.md` and reported here.

**Author decision requested:** approve the executed budget plan (which steps of the ladder are
applied) for the available wall-clock `H = 8–24 h`. _Options and measured consequences to be
filled from the pilot._

## 4. Author decisions requested at this gate
1. **Matrix amendment M1** (counts above) — approve / reject.
2. **Amendment A2** items A2-1 … A2-8 + bounded choices AC-1…AC-8
   (`docs/prereg-amendment-A2.md` §10 approval sheet). Recommended: approve all with AC-1 = H5-A,
   AC-2 = no regeneration, AC-3 = pooled stratified exact sign test (AC-3b), AC-4 = at least one of
   M3/M4, AC-5 = `v5` violation, AC-6 = split I1a/I1b, AC-7 = narrow TLC claim, AC-8 = >20 %
   censoring flag.
3. **K6 non-realisability acknowledgment** (M2): accept that E3/C7/P9 evidence is
   model-and-argument only; no K6 data will be claimed.
4. **Budget plan** for `H` per §3.
5. **S5 re-review verdict** is a precondition: the gate is not offered until S5 confirms the fixed
   implementation and the re-issued freeze.

## 5. Sign-off
- [ ] Matrix amendment approved
- [ ] Amendment A2 approved (item-by-item)
- [ ] K6 limitation acknowledged
- [ ] Budget plan approved
- [ ] Freeze hash recorded: `docs/prereg-freeze.json` → `<pending>`
