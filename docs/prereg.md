# Pre-registration — Where Exactly-Once Ends

**Frozen experimental design, matrices, measurement contract, and statistical analysis plan**

| Field | Value |
|---|---|
| Document | `docs/prereg.md` — pre-registered design and analysis plan |
| Version | 1.0-draft, P1 deliverable |
| Status | **DRAFT FOR AUTHOR APPROVAL. NOT IN FORCE.** Binding from the moment the author signs the P1 gate (A1 §12); all measurement must follow it verbatim (A1 §11). |
| Companion | `docs/method.md` (model, propositions P1–P12, prediction tables, mechanisms, crash points) |
| Rule | Anything not written here before the first analysed run is **exploratory** and must be labelled as such in the paper. Deviations are recorded in `results/DEVIATIONS.md`; nothing is silently changed. |
| Contains results | **No.** No measurement exists yet. Run counts are arithmetic design facts; time projections are blank until the pilot (P2). |
| Integrity | Every number in the paper will trace to `results/raw/` through `results/RUNLOG.md` (BRIEF §2, §8). |

**Normative relationship.** `docs/method.md` says what is asserted and why (model, propositions, prediction tables). This document says exactly what will be executed and how it will be analysed. In a conflict, the model governs claims, this document governs execution; the author resolves and records the resolution.

---

## 1. Scope

This pre-registration covers: (a) the exact experiment matrices for the pilot and Stages A–D; (b) the run protocol (reset, randomisation, interleaving, seeds, warm-up, quiescence); (c) the measurement instruments and the per-run record; (d) the validity gates and exclusion rules; (e) the frozen statistical analysis plan, including hypothesis decision rules, multiple-comparison families, effect-size thresholds, and zero-event reporting language; (f) the analysis-code freeze procedure; (g) the compute-projection method and the mechanical cut ladder; (h) the deviations policy; (i) the environment deviations (native processes instead of containers); (j) decisions requiring author approval.

Out of scope: implementations (S3), execution (S4), red-team objections (S5), manuscript and venue compliance (S6/lead).

---

## 2. Matrices — exact specification

### 2.1 Factors and levels

| Factor | Levels | Notes |
|---|---|---|
| Mechanism | 9 configurations: `M0`, `M1-kafka`, `M1x`, `M2`, `M3`, `M4`, `M2+M3`, `M4+M3`, `M1x+M3` | `docs/method.md` §4 |
| Fault condition (Level 2) | `F0` (control), `F1`, `F2`, `F3`, `F4a`, `F4b`, `F5`, `F6` | 8 levels; F4 split by ordering direction (`docs/method.md` §5.4) |
| Crash class (Level 1) | `K1..K7` | 7 classes; reachability per mechanism in `docs/method.md` §5.3 |
| Load (events) | `1k` (Stages A, pilot), `10k`, `100k`, `1M` (Stage C only) | exact `N` per event count |
| Partitions `P` | `3`, `6`, `12` | P=6 is canonical; P=3/12 sensitivity |
| Consumer instances `C` | `2` at canonical; `1` as pilot robustness check | rebalances must be real |
| Ordering variant | `strict` (canonical), `early` (probe; D2) | only for K4/F4b cells |
| Repetitions `N` | 30 (core correctness and performance), 10 (sensitivity and 1M), 3 (pilot), 20 (floor after cuts) | see §2.5 |
| Target rate mode | `fixed-rate` (latency runs), `saturation` (capacity runs) | §6.2 |

**Not varied (held identical for all mechanisms):** shared Kafka/PG/JVM configuration (`docs/method.md` §4.11, Appendix C), event distribution and seeds, C=2, batch sizes, topic/partition layout, host and OS state. Any mechanism-intrinsic difference is declared in `docs/method.md` §4.11 and audited by gate G6.

### 2.2 Mechanism set and reachability (executed cells)

| Configuration | Executed fault cells (of 8) | Executed crash classes (of 7) | Notes |
|---|---|---|---|
| M0 | 8 | 6 (K7 n/a) | negative control |
| M1-kafka | 5 (F4a, F4b, F5 are n/a) | 4 (K3, K4, K7 n/a) | positive control; no external store |
| M1x | 8 | 6 (K7 n/a) | the EOS gap |
| M2 | 8 | 7 | |
| M3 | 8 | 6 (K7 n/a) | |
| M4 | 8 (F4b n/a) | 5 (K3, K7 n/a) | |
| M2+M3 | 8 | 7 | |
| M4+M3 | 8 (F4b n/a) | 5 (K3, K7 n/a) | |
| M1x+M3 | 8 | 6 (K7 n/a) | |
| **Executed cells** | **67** (72 − 5 n/a) | **52** (63 − 11 n/a) | n/a cells are recorded as n/a with the reachability proof, never as passes (`docs/method.md` §5.3, §6.4) |

### 2.3 Stage P — pilot

| Property | Value |
|---|---|
| Purpose | calibrate per-run timings for the compute projection; validate the oracle, the injectors, the source-integrity gate, and the read path; measure mechanism capacities for the target-rate rules (§6.2); verify C=1 robustness |
| Design | 9 mechanisms × 8 fault conditions × N=3 × 1k events × P=3 × C=2 |
| Runs | **201 executed** of **216 planned slots** (15 mechanism × condition slots are structurally `n/a` by reachability, see §2.2; they are recorded with `executed=false` and the reachability reason, never counted as passes). Planned/executable totals are reported throughout. |
| Additional validation runs | 20 dedicated: sham-injection runs (injector armed, no action), C=1 robustness runs, injector self-tests, oracle mutation-test fixtures; recorded separately and not part of the matrix |
| Output | `pilot_*.json` run records + the completed projection worksheet (§9.4) written into this document at P2 |

### 2.4 Stage A — structural correctness under deterministic crash points (Level 1)

| Property | Value |
|---|---|
| Design | 9 mechanisms × executed crash classes (52 cells) × N=30 × 1k events × P=3; each run injects exactly one crash-point fault at the cell's class, deterministically, via failpoints/process kills/Toxiproxy; K4 cells use the `early` ordering variant (D2) |
| Crash-class runs | 52 × 30 = **1,560** |
| Control runs | 9 mechanisms × 30 = **270** (`A-ctrl-*`, no injection; required for baselines and for G2) |
| Total Stage A | **1,830 runs** |
| Analysis status | **confirmatory** for P2/P3/P5/P8/P9/P10 structural claims; the control cells feed G1/G2 |
| Notes | A run whose armed injection did not fire is invalid, excluded, counted; the runner repeats it (up to the rerun cap) to reach N_valid = 30, else the cell is reported at N_valid < 30 as a deviation |

### 2.5 Stage B — correctness under stochastic faults (Level 2) and realistic load

| Property | Value |
|---|---|
| Core design | 9 mechanisms × 8 fault conditions, executed cells = 67 × N=30 × 100k events × P=6 × C=2; faults injected at random recorded times (Level 2) |
| Core runs | 67 × 30 = **2,010** |
| Sensitivity block S1 (load) | 67 cells × N=10 × 10k events × P=6 → **670 runs** |
| Sensitivity block S2 (partitions) | 67 cells × N=10 × 100k events × P∈{3,12} → **1,340 runs** (literal A1 reading); D7(b) alternative: both sensitivity blocks at 10k → **1,340 runs at 10k** (cheaper; author decision D7) |
| Total Stage B (nominal, literal) | **4,020 runs** (2,010 + 670 + 1,340) |
| Analysis status | confirmatory for the *frequency* component of H1/H2/H3/H4 and for H5; the deterministic structural claims are Stage A's |

### 2.6 Stage C — performance, fault-free

| Property | Value |
|---|---|
| Core design | 9 mechanisms × loads {10k, 100k} × P {3,6,12} × N=30 × fixed-rate mode |
| Core runs | 9 × 2 × 3 × 30 = **1,620** |
| 1M block | 9 mechanisms × 100k→1M at P=6 × N=10 → **90 runs** |
| Capacity block | 9 mechanisms × 100k at P=6 × saturation mode × N=10 → **90 runs** |
| Total Stage C | **1,800 runs** |
| Analysis status | confirmatory for the performance component of H2/H4/H5; the 1M and capacity blocks are confirmatory for their pre-registered contrasts but are the first items on the cut ladder |

### 2.7 Stage D — performance under faults and time-to-consistent-state

| Property | Value |
|---|---|
| Design | no new runs; reuses Stage B records |
| Outcomes | throughput under faults, p50/p99/p99.9 under faults, time-to-consistent-state (censoring rate), all by cell |
| Analysis status | confirmatory for H2's cost component under faults and for the time-to-consistency contrasts |

### 2.8 Totals and the cut ladder

| Package | Planned slots | Executable runs |
|---|---|---|
| Pilot | 216 | 201 (+20 validation) |
| Stage A | 1,830 | 1,830 |
| Stage B nominal | 4,020 | 4,020 |
| Stage C | 1,800 | 1,800 |
| Stage D | 0 | 0 |
| **Nominal total** | **7,866** | **7,851 (+20 validation)** |

The 15-slot difference is exactly the mechanism × condition reachability `n/a` set of §2.2; no
confirmatory cell is removed by this accounting. Every table in the paper reports planned slots,
executable runs, executed runs and excluded runs separately.

**Cut ladder (A1 §7, mechanical).** Given the author's available wall-clock `H` and the P2 projection `T_total`:
1. If `T_total > H`: remove the Stage C 1M block (−90 runs). Recompute.
2. If still `> H`: remove the partition-sensitivity block S2 (−1,340 runs). Recompute.
3. If still `> H`: reduce correctness-cell repetitions to N = 20 for Stages A and B and the control cells (Stage A 1,830 → 1,220; Stage B 2,010 → 1,340; sensitivity blocks if retained at N=10 unaffected). Recompute; N_valid floor = 20.
4. If still `> H`: **STOP.** The shortfall and three concrete reduction options (drop M1-kafka from Stage B/C; drop the combinations from Stage B; reduce Stage B load to 10k) are escalated to the author under D1. No further cut may be applied by the team.
**Never cut:** the control cells, the validity gates, the oracle, or any cell needed by the H1/H3 structural tests. Every applied step and every removed cell appears in `results/DEVIATIONS.md` and in the paper.

---

## 3. Hypothesis decision rules (pre-registered)

The paper reports each hypothesis as **confirmed**, **refuted**, or **inconclusive**, using exactly these rules. "Inconclusive" is a legitimate, reportable outcome (e.g. wide CI, injector not realisable).

### 3.1 H1 — M1x violates, M1-kafka does not
- **Confirmed** iff: (i) in Stage A, M1x has at least one violating run in K2 or K3 cells with an attested injection; (ii) M1-kafka has zero violating runs in every executed cell (Stage A and B), with exact upper bounds reported; (iii) the Stage A comparison M1x vs M1-kafka in K3 is significant by McNemar's exact test at α = 0.05 (Holm family C1).
- **Refuted** iff: M1x is clean (zero violating runs) in all K2/K3 cells *and* all injections are attested as fired; or M1-kafka shows a violation (which also fails gate G2 and halts the campaign).
- **Inconclusive** iff: the K2/K3 injectors are not reliably realisable (attestation failure rate > 20 % in the cell), or CIs overlap so that no directional claim can be made.

### 3.2 H2 — M2 sufficient, with a cost
- **Correctness component confirmed** iff M2 has zero violating runs in all executed Stage A and Stage B cells, with exact upper bounds reported.
- **Cost component confirmed** iff, in Stage C/D, M2 is worse than M3 *or* M4 in the pre-registered direction on at least one primary performance outcome (median throughput lower, or p99 latency higher) with Cliff's |δ| ≥ 0.33 (small) and Holm-adjusted p < 0.05 in family P1 or P2.
- **Confirmed** iff both components confirmed; **partially confirmed** iff exactly one; **refuted** iff the correctness component fails (a violation with attested injection); **inconclusive** if the correctness CIs are too wide or the performance comparison is underpowered (e.g. N_valid < 20).

### 3.3 H3 — M3: zero duplicates, but losses under the ordering fault
- **Confirmed** iff: (i) M3 has zero I3 violations in all executed Stage A/B cells (exact upper bounds reported); (ii) M3 has at least one I4 violation in the K4 (`early` probe) cells and/or the Stage B F4b cells, with attested injection and the ordering flag recorded; (iii) the Stage A K4 comparison M3-`early` vs M3-`strict` (control) is significant by McNemar's exact test at α = 0.05 (family C2).
- **Refuted** iff the K4/F4b injections are attested as fired and M3 shows no I4 violation at N = 30 (then the design could not demonstrate the predicted loss; H3's loss component is refuted at this resolution).
- **Inconclusive** iff D2 is not approved and the ordering probe is not run (the loss arm is then untested; the paper says so explicitly), or the probe is unreliable.

### 3.4 H4 — M4 (or M4+M3) sufficient with lower latency than M2
- **Correctness component confirmed** iff M4 and M4+M3 have zero violating runs in every *reachable* Stage A/B cell (n/a cells are excluded with the reachability proof and are not counted as passes), with exact upper bounds.
- **Latency component confirmed** iff M4's median p99 (and median p50 as secondary) is lower than M2's in at least one of the two Stage C loads at P=6, with Cliff's |δ| ≥ 0.33 and Holm-adjusted p < 0.05 (family P2).
- **Confirmed** iff both; **partially confirmed** iff one; **refuted** iff the correctness component fails; **inconclusive** if underpowered.

### 3.5 H5 — no single mechanism dominates
- **Dominance definition (pre-registered).** Mechanism `x` dominates iff: (i) `x` has zero violating runs in **every** executed correctness cell (Stage A and B) with upper bounds reported; **and** (ii) `x` is non-inferior to the best of the other mechanisms on both median throughput and median p99 latency in at least 4 of the 6 Stage C (load × partition) blocks, where non-inferior means Cliff's |δ| < 0.147 (negligible band) and the 95 % bootstrap CI of the median ratio contains 1.
- **Confirmed** iff no mechanism satisfies both (i) and (ii). **Refuted** iff at least one does (then the paper must name it and revise the RQ3 recommendation). **Inconclusive** if any mechanism's correctness cells are too underpowered to assess (i).

### 3.6 Multiplicity and families

| Family | Tests | Correction |
|---|---|---|
| **C1** | H1: M1x vs M1-kafka per fault condition where both are executed (F0,F1,F2,F3,F6) | Holm |
| **C2** | H3: M3-`early` vs M3-`strict` on I4 (K4, F4b) | Holm |
| **C3** | H4: M4/M4+M3 correctness comparisons vs M1x (poison control) | Holm |
| **C4** | Zero-violation confirmation of M2, M3, M4, M4+M3, combinations: no test, only exact upper bounds (no p-values) | — |
| **P1** | Throughput: all pairwise mechanism comparisons within Stage C/D (the 9-mechanism Friedman + Nemenyi family) | Nemenyi (Friedman family-wise) + Holm within pairwise tests |
| **P2** | p99 latency: same family as P1 | as P1 |
| **P3** | Time-to-consistent-state: proportion-consistent comparisons at 100k/P=6 | Holm |
| **T1** | Exploratory: everything else (divergence, publication integrity, 1M contrasts, capacity) | no correction; labelled exploratory |

α = 0.05 (two-sided) everywhere. Both unadjusted and adjusted p-values are reported. Effect sizes are reported regardless of significance.

---

## 4. Run protocol

### 4.1 Per-run sequence (exact order)

1. **Reserve** the run in `experiments/run_order.csv` (already fixed by the pre-generated schedule; no on-the-fly choice).
2. **Reset:** stop consumer instances; create fresh topics `wxeo.src.<run_id>` and `wxeo.sink.<run_id>` (P partitions, RF=3, `min.insync.replicas=2`); create a fresh consumer group `wxeo.cg.<run_id>`; create a fresh PostgreSQL schema `wxeo_run_<run_id>` with the mechanism's tables; assert no residue from the previous run (topic list, schema list, group list, PG sessions).
3. **Start** the consumer instance(s) with the pinned JVM command; wait for readiness (health probe: group stable, lag = 0, no errors for 5 s).
4. **Record** the pre-run environment block (versions, digests, config hashes, free disk, process list, clock check).
5. **Ground truth:** generate `gt_<run_id>.ndjson` and `gt_<run_id>.sha256`, fsync both **before** the first produce (A1 §4).
6. **Arm** the fault injector per cell (Level 1: the crash-point hook config; Level 2: the seeded fault schedule). Sham mode for control cells: armed but no action.
7. **Produce** the event stream in open-loop fixed-rate mode; record per-event `scheduled_at_ms` and `ack_ts_ms` (produced manifest).
8. **Fire** the injection at its scheduled trigger (Level 1: phase marker; Level 2: seeded time); record receipt; restore the system; for process kills, restart the instance and wait for group stability.
9. **Quiesce:** wait per the quiescence definition (`docs/method.md` §7.3): lag = 0, no new sink rows for `T_quiet`, no active app transaction, retry drain elapsed; or `T_max` timeout (censored).
10. **Dump:** source dump, sink dump, publication-stream dump where applicable, progress-state snapshot, `pg_stat_activity` snapshot.
11. **Oracle:** verify GT digest, source-integrity gate, compute §7.1 metrics, write `oracle_<run_id>.json`.
12. **Stop** consumers; collect process telemetry; run residue checks; archive the run record and the retained artefacts.
13. **Drop** the schema and topics; the run is complete.

A run that fails steps 2–5 or the source-integrity gate is **invalid** and is redone; a run that fails quiescence is censored (not invalid).

### 4.2 Fresh state and periodic restarts

Kafka broker and PostgreSQL **processes are long-lived** and restarted every 50 runs (recorded in `fault_restart.log`), because per-run restart of 3 broker JVMs + PG would dominate wall-clock and is not required for logical freshness. This is a deviation from A1 §7's "restarted components" wording, recorded in §11 and in `results/DEVIATIONS.md` (D6).

### 4.3 Randomisation and interleaving

- The full list of runs is generated deterministically from this document's matrices: `experiments/matrix.yaml` (generated, committed, hashed before execution).
- Run order: seeded Fisher–Yates shuffle of the run list with seed `order_seed` (recorded), subject to: (i) no more than 3 consecutive runs of the same mechanism; (ii) no more than 40 % of a day block (60 runs) from one mechanism; (iii) stage order is Pilot → A → B → C (stages are not interleaved because they use different loads and would bias warm-up/memory state); within a stage, mechanisms are fully interleaved.
- Realised order committed as `experiments/run_order.csv` **before** the first run of each stage.

### 4.4 Seeds

- `master_seed` recorded once (hex string).
- Data seed per repetition: `data_seed(stage, condition, rep) = SHA-256(master_seed | "data" | stage | condition | rep)[0:16]`.
- Injection seed per repetition: `inj_seed(stage, condition, rep) = SHA-256(master_seed | "inj" | stage | condition | rep)[0:16]`.
- **Pairing:** all mechanisms in the same `(stage, condition, rep)` use the same data seed, which makes correctness runs paired by experimental material and enables McNemar's test (§7.2). Injection seeds are also shared, but the *realised fault time* differs per mechanism because phase progress differs; this is recorded and is not a pairing claim.
- A run is reproducible from `(matrix cell, rep)` alone.
- **Seed text encoding (P2 clarification, 2026-11-24).** `master_seed`, `data_seed` and
  `injection_seed` are recorded as 16-hex-char strings. The event-derivation seed argument is
  `data_seed` interpreted as a big-endian unsigned 64-bit integer and rendered as canonical decimal
  ASCII (no sign, no leading zeros); the Java generator and the Python oracle use exactly that text
  in their domain-separated SHA-256 derivations (`wxeo1|event|`, `wxeo1|part|`). GT `line_no` is
  0-based; posting `leg_no` is 1-based. This changes no N, cell, factor or statistic.

### 4.5 Warm-up and steady state

- Warm-up: the first `max(2 000, 10 % of the run's events)` events are excluded from **performance** metrics; they are included in correctness.
- Steady state: from the end of warm-up to the last committed intended event. Throughput = committed events in the window / window duration.
- For fault runs, the performance window excludes the fault episode and the restoration interval; the duration and exclusion are recorded in the run record (`perf_window` block).

### 4.6 Quiescence and observation

Exactly as `docs/method.md` §7.3: `T_quiet = 10 s`, `T_max = 300 s`, polling at 1 Hz, final observation in a `REPEATABLE READ READ ONLY` transaction after the application is stopped. A pilot calibration doubles `T_quiet` and verifies count stability (G7).

---

## 5. Per-run record (JSON) — exact schema

One JSON object per run, path `results/raw/<stage>/<run_id>.json`, committed (compressed if large). No field may be omitted; unknown values are `null` with an accompanying `notes` entry.

| Field | Type | Unit/Source | Description |
|---|---|---|---|
| `schema` | string | const | `"wxeo.run.v1"` |
| `run_id` | string | runner | `<stage>-<mech>-<condition>-r<rep>`; exact encoding (Kafka-legal, lower-case) fixed in `docs/CONTRACTS.md` §2, e.g. `b-m2m3-f1-100k-p6-r12` |
| `stage` | string | runner | `pilot`/`A`/`B`/`C`/`D` |
| `cell_id` | string | matrix | unique cell identifier |
| `mechanism` | string | matrix | one of the 9 |
| `ordering_variant` | string | matrix | `strict`/`early` |
| `fault` | string | matrix | `F0..F6`, `K1..K7`, or `none` |
| `fault_level` | string | matrix | `L1`/`L2`/`none` |
| `load_events` | int | matrix | intended events |
| `partitions` | int | matrix | source partitions |
| `consumer_instances` | int | matrix | C |
| `rep` | int | matrix | 0..N−1 |
| `data_seed` | string | §4.4 | hex |
| `injection_seed` | string | §4.4 | hex |
| `master_seed` | string | §4.4 | hex |
| `git_sha` | string | runner | repository commit |
| `host` | object | runner | OS build, CPU model, RAM, disk free at start |
| `versions` | object | runner | Java, Kafka, PostgreSQL, Toxiproxy, PowerShell, Python; SHA-256 of vendored binaries |
| `config` | object | runner | full shared+intrinsic configuration, plus `config_sha256` |
| `process_limits` | object | runner | affinity mask, priority class, `-Xmx`, pool sizes, broker/PG limits |
| `timings` | object | runner | `t_start`, `t_load_start`, `t_load_end`, `t_fault_fire`, `t_restore`, `t_quiesce`, `t_observation`, `t_end` (UTC ms) |
| `quiescence` | object | runner | `lag_per_partition`, `quiet_period_ms`, `censored` (bool), `t_consistent_ms` (int or null if censored) |
| `performance` | object | app+HdrHistogram | `throughput_eps_median` (per run), `p50_ms`, `p90_ms`, `p99_ms`, `p999_ms`, `max_ms`, `histogram_path`, `warmup_events_excluded`, `perf_window` |
| `counts` | object | oracle | `produced`, `consumed_estimate`, `expected_postings`, `observed_postings`, `dup_postings`, `dup_entries`, `lost_postings`, `lost_entries`, `imbalanced_entries`, `mutated_postings`, `spurious_postings` |
| `violations` | object | oracle | `v1`, `v2`, `v3`, `v4` (bool), `violating_run` (bool) |
| `publication` | object | oracle | `pub_records`, `dup_records`, `lost_records`, `divergence` |
| `oracle` | object | oracle | `gt_sha256`, `expected_sha256`, `source_integrity` (bool), `oracle_script_sha256`, `result_sha256` |
| `fault_receipts` | array | injector | receipts from `faults.log` for this run (0, 1, or more) |
| `validity` | object | runner | `valid` (bool), `excluded` (bool), `exclude_reason`, `residue_check`, `clock_check`, `disk_free_start_gb`, `notes` |
| `artifacts` | object | runner | paths (relative) of gt/dumps/logs/raw histogram, plus SHA-256 of each retained artefact |
| `stdout_path`, `stderr_path` | string | runner | application logs |
| `exit_status` | object | runner | per-process exit codes |

**Retention policy.** Full dumps are retained for: all violating runs, all control runs, and the pre-registered systematic sample (rep indices 0, 10, 20 when N=30; 0 and 5 when N=10). For all other runs the dumps are deleted after the oracle result and digest are written (the counts are recomputable from run records; the dumps are re-generatable deterministically). The disk budget is checked against the free-space floor in §9.3; a run whose retained artefact copy fails its digest is invalid.

---

## 6. Measurement contract

### 6.1 Correctness

The oracle (`docs/method.md` §7.4) computes the metrics of `docs/method.md` §7.1 from `(Ω, D)`. The primary outcome is `violating_run`; the per-invariant flags `v1..v4` are secondary. The oracle is built before the mechanisms and validated by the gates in §8.

### 6.2 Load modes and target-rate rules (values fixed after the pilot)

To avoid coordinated omission and to avoid unfair per-mechanism tuning, two modes are pre-registered:

- **Fixed-rate (latency) mode.** The generator emits events at a fixed offered rate `R_off`, independent of responses. Rule: `R_off(load, P) = 0.75 × min over mechanisms of the pilot-measured capacity at (load, P)`. Values are written into this section at P2 from pilot measurements. If a mechanism cannot sustain 0.75× the *minimum* capacity (i.e. it is slower), its queueing latency grows and is reported honestly as the consequence of an open-loop offered load.
- **Saturation (capacity) mode.** Offered rate = `2.5 × max over mechanisms of the pilot capacity`; the measured outcome is achieved throughput and its stability over the steady-state window. Used only for the Stage C capacity block (N=10 per mechanism at 100k/P=6). The generator is validated to sustain ≥ 3 × max capacity (gate G7).

Both rules are deterministic functions of pilot measurements, so no post-hoc choice is possible.

### 6.3 Latency, throughput, time-to-consistent-state

Exactly per `docs/method.md` §7.2–7.3 and the run-record fields in §5. HdrHistogram configuration is fixed (3 significant digits, 1 µs..60 s). Latency percentiles are per run; the analysis uses run-level values as the resampling unit.

### 6.4 Instruments and versions

Pinned in `docs/method.md` Appendix C; every run records the full config object, the config SHA-256, and binary digests. The per-run process telemetry (CPU time, peak working set, context switches) is recorded but is not a primary outcome.

---

## 7. Frozen statistical analysis plan

All tests below are implemented in the frozen analysis scripts (§10) and are reported in full (unadjusted and adjusted p, effect sizes, CIs, N per cell, exclusion rates). Nothing here may be changed after the analysis freeze without a `DEVIATIONS.md` entry and an explicit exploratory label.

### 7.1 Estimands

- Correctness: `π_c` = probability that a run in cell `c` is a violating run. Report: `k/N` with **Clopper–Pearson exact 95 % CI**.
- Per-invariant: same for `v1..v4`, plus counts (median/IQR) of `dup_postings`, `lost_postings` etc.
- Performance: per cell, the distribution of per-run median throughput and per-run latency percentiles (p50, p99, p99.9). Report median + IQR and a **bootstrap 95 % CI** for the median, B = 10,000 resamples, seed recorded (`bootstrap_seed`), percentile method primary, BCa as sensitivity.
- Time-to-consistency: proportion of runs consistent within `T_max` (CP 95 % CI) and, among consistent runs, median `t_consistent` with bootstrap CI. Censored runs are reported as a separate proportion; they are never dropped.

### 7.2 Correctness tests

- **Primary (paired):** **McNemar's exact test** on paired runs (same `(stage, condition, rep)` across two mechanisms). Reported: discordant-pair counts `(b, c)`, exact two-sided p.
- **Sensitivity (unpaired):** **Fisher's exact test** on `k/N` tables.
- **Multiple comparisons:** Holm–Bonferroni within the pre-declared families of §3.6; both unadjusted and adjusted p reported.
- **Effect size for proportions:** risk difference with CP-style CI and odds ratio with exact CI; reported with the test.
- **No test is run on a cell with `N_valid < 20`;** such cells are reported descriptively and flagged (pre-registered rule).
- For binary outcomes the run is the unit; per-posting counts are not treated as independent observations (clustering warning).

### 7.3 Performance tests

- **Friedman test across mechanisms**, with **blocks = (load × partitions) within each stage, aggregated to block medians** (A1 §9): Stage C gives 6 blocks (2 loads × 3 partitions) × 9 mechanisms. Report the statistic, the 6 block medians per mechanism, average ranks, and exact/permutation p where feasible.
- **Nemenyi post-hoc** with a critical-difference diagram, α = 0.05.
- **Cliff's delta** with bootstrap 95 % CI for every reported pair (B = 10,000, seed recorded). Pre-registered thresholds: |δ| < 0.147 negligible; < 0.33 small; < 0.474 medium; otherwise large.
- Pairwise tests within the P1/P2 families: Wilcoxon–Mann–Whitney exact/permutation on run-level values, Holm-adjusted (the Nemenyi result is the family-wise gate; pairwise Holm tests are reported for direction and magnitude).
- The 1M block and the capacity block are pre-registered confirmatory contrasts but are analysed with their own (smaller) N and reported as such; they are the first cut-ladder items.

### 7.4 Time-to-consistent-state tests

- Primary: proportion of runs reaching consistency within `T_max` per cell (CP 95 % CI), compared across mechanisms with Fisher/McNemar as appropriate, Holm within family P3.
- Secondary: `t_consistent` median + bootstrap CI among non-censored runs; censoring rates reported per cell.
- Model note: a mechanism that never quiesces is a correctness failure, not "not applicable".

### 7.5 Zero-variance, all-zero cells, and reporting language

Pre-registered sentence template (used verbatim in the paper; no stronger wording is allowed):

> *"Mechanism M produced zero violations in N = ⟨N_valid⟩ valid runs under ⟨condition⟩ (⟨N_excluded⟩ excluded: ⟨reasons⟩). The exact Clopper–Pearson 95 % upper bound on the per-run violation probability is ⟨U⟩ (rule-of-three bound 3/N = ⟨3/N⟩). This bounds the violation probability; it does not prove absence."*

Rules: (i) never write "proved", "eliminates", "never fails", "exactly-once" without a scope qualifier; (ii) a 0/N result with N < 20 is not reported as evidence; (iii) any cell where all mechanisms are zero is reported as a table row, not omitted (publication-bias rule); (iv) power statements: for N=30, p=0.05 → P(observe ≥1) ≈ 0.785; p=0.10 → ≈ 0.958; quoted wherever a zero result is interpreted.

### 7.6 Perfect separation and degenerate outcomes

If a test is undefined (e.g. McNemar with zero discordant pairs, or Fisher with a zero margin), the p-value is reported as "not applicable" with the discordant counts; the conclusion then rests on the CIs and the pre-registered decision rules, and the cell is flagged. No continuity correction is applied to exact tests.

### 7.7 Cell-to-exposure map for the decision table (RQ3)

Frozen mapping used by `docs/method.md` §11 D-R2: E1 → {K2, K3, K5, F2, F4a, F6}; E2 → {K4, F4b}; E3 → {K6}; E4 → {K2/C9-class, F5}; E5 → publication metrics in M1x/M2 cells; the combined sets per §6.6. "Observed consistent with sufficiency" for an exposure = zero violating runs in all its cells with attested injections and exclusion rate ≤ 20 %.

### 7.8 Independence, autocorrelation, and order effects

- Diagnostics: lag-1 autocorrelation of throughput residuals within each stage; Durbin–Watson; Spearman ρ between the metric and the chronological run index; day/block means.
- Pre-registered action: if |ρ| is significant at α = 0.05, an **exploratory** sensitivity analysis adds run order as a covariate; the primary analysis is unchanged and the diagnostic is reported in the paper.
- No data-driven transformation, outlier removal, or model selection is permitted.

### 7.9 Exploratory analyses

Pre-declared exploratory (reported, labelled, no inferential claims beyond description): publication-integrity rates and divergence; 1M and capacity contrasts if cut; per-fault fire-rate as an explanatory variable; performance under faults with N < 30; cross-sink comparisons involving M1-kafka (if D13(b)); and any analysis not in §7.1–7.8.

---

## 8. Validity gates and exclusions

### 8.1 Gates (must pass before Stage A data are analysed)

G1–G8 exactly as in `docs/method.md` §7.6: M0 negative control shows predicted violations; M1-kafka and M4+M3 fault-free controls show zero; oracle mutation testing 100 % detection for drop/duplicate/flip-sign/update/spurious defects (≥ 100 seeded mutations per class); injector attestation complete; ground-truth digest equality (Java ↔ Python); configuration-hash equality across mechanisms; generator capability ≥ 3 × max capacity and quiescence calibration; clock/host sanity.

### 8.2 Exclusion rules (exhaustive; nothing else may exclude a run)

A run is invalid and excluded **only** if, before outcome inspection:
1. an armed injection produced no `fired`/`restored` receipt, or the receipt is malformed;
2. the source-integrity gate fails (gaps/duplicates in the source dump, GT digest mismatch);
3. the residue check fails (state from a previous run) or the environment block is incomplete;
4. the clock check fails (clock step during the run) or free disk fell below the floor;
5. the application did not start / the run did not execute its intended cell (wrong config hash, wrong mechanism, wrong ordering variant).

Exclusions are decided by the runner from provenance data only, without looking at violations or performance. Every exclusion is logged with its reason; exclusion rate is reported per cell; a cell with exclusion rate > 20 % is flagged and its inference labelled fragile (A1 §5).

### 8.3 Reruns

Invalid runs are rerun (same cell, same rep index, same seeds) up to `R_max = 3` attempts; a run missing after R_max is a deviation with `N_valid < N_planned` recorded. Censored (quiescence-timeout) runs are **not** rerun: they are outcomes.

---

## 9. Compute-projection method (no numbers before the pilot)

### 9.1 Run-count table (design facts, not projections)

| Run type | Stage | Events | P | N | Runs |
|---|---|---|---|---|---|
| pilot matrix | P | 1k | 3 | 3 | 216 |
| pilot validation | P | 1k | 3 | — | 20 |
| crash-class cells | A | 1k | 3 | 30 | 1,560 |
| controls | A | 1k | 3 | 30 | 270 |
| fault cells | B | 100k | 6 | 30 | 2,010 |
| sensitivity S1 | B | 10k | 6 | 10 | 670 |
| sensitivity S2 | B | 100k | 3,12 | 10 | 1,340 |
| performance core | C | 10k,100k | 3,6,12 | 30 | 1,620 |
| 1M block | C | 1M | 6 | 10 | 90 |
| capacity block | C | 100k | 6 | 10 | 90 |

### 9.2 Projection formula (to be filled at P2)

For every run type `τ` the pilot measures median and p90 of:

`t_τ = t_reset + t_start + t_load(R_off, L) + t_fault(τ) + t_restore(τ) + t_drain + t_verify(L) + t_archive`

where `t_load = L / R_off` (plus generator lag), `t_fault` is 0 for fault-free types, `t_restore` is measured per fault class (broker restart, consumer restart, rebalance wait, link restore), `t_drain` covers retry backoff, and `t_verify` scales with the posting count. The projection is

`T_total = Σ_τ n_τ · t_τ^{(p90)} + 0.15 · Σ_τ n_τ · t_τ^{(p90)}`  (15 % contingency for reruns/exclusions)

and is reported as a table with per-type `n`, measured `t_τ`, and projected hours; the worksheet template is fixed below and committed with the P2 report.

| Run type | n | measured median (s) | measured p90 (s) | projected h (p90) | notes |
|---|---|---|---|---|---|
| pilot matrix | 216 | — | — | — | |
| pilot validation | 20 | — | — | — | |
| A crash-class | 1,560 | — | — | — | per-class restore times recorded separately |
| A controls | 270 | — | — | — | |
| B core | 2,010 | — | — | — | includes fault restore |
| B S1 | 670 | — | — | — | |
| B S2 | 1,340 | — | — | — | |
| C core | 1,620 | — | — | — | |
| C 1M | 90 | — | — | — | |
| C capacity | 90 | — | — | — | |
| **total** | **7,886** | | | — | = 7,866 matrix runs + 20 validation; +15 % contingency = **—** |

The blanks are filled **only** from pilot measurements at P2 (A1 §12); no time value may be estimated in advance and presented as a result.

### 9.3 Disk budget and retention

Facts: free disk on this host is ≈ 14 GB at the time of the design (BRIEF §9). Drivers: ground-truth files (written then deleted per D10), PostgreSQL data + WAL for the largest runs (1M events ≈ 2M posting rows plus indexes), Kafka topics (source + sink, RF=3), dumps, and retained raw records. Rules: (i) scratch on a local path, curated copies into the repository (D11); (ii) a free-space floor of 3 GB is checked before every run (G8); (iii) the retention policy of §5 bounds retained dumps; (iv) the 1M block is executed with `wal_level`, checkpoint and autovacuum settings as pinned; (v) the measured disk high-water mark per run type is recorded in the pilot and becomes part of the projection report. If the floor cannot be maintained, the 1M block is the first cut (cut ladder step 1).

### 9.4 Cut-ladder application procedure

Applied at P2 by the lead with the author: (1) compute `T_total` from measured pilot constants; (2) compare to `H`; (3) apply cut steps in the fixed order (§2.8), recomputing after each step; (4) record every applied step in `results/DEVIATIONS.md` with the measured constants and the new totals; (5) if step 4 is reached, stop and escalate to the author. The prereg is *not* edited after P2; the executed plan is recorded in `results/DEVIATIONS.md` and reported in the paper.

---

## 10. Analysis-code freeze

Before the first Stage A run:

1. The oracle/checker, the metric computation, all statistical tests, the table/figure generators, and the run-record validator are committed.
2. `docs/prereg-freeze.json` records: file list, SHA-256 of each file, git SHA, UTC timestamp, and the `bootstrap_seed`/`order_seed`/`master_seed` generation procedure.
3. The freeze is announced to the lead; any post-freeze change is committed **with** a `results/DEVIATIONS.md` entry naming the file, the reason, and the affected analyses, and the affected results are labelled "post-freeze analysis".
4. The analysis is a pure function of `results/raw/` (plus the frozen scripts): every table and figure in the paper is regenerated by `tools/analyze.py` and `tools/make-figures.py` in CI from committed raw data.
5. No analysis may read anything outside `results/raw/`, `docs/prereg-freeze.json`, and the environment/version block.

Deliverables of the freeze: `results/analysis/*.csv` (per-cell summaries), `results/analysis/tests.json` (all tests, raw and adjusted), `results/analysis/decision-table.csv`, and the scripts' hashes.

---

## 11. Environment deviations (native processes instead of containers)

| Aspect | A1 wording / container assumption | This host's execution | Recorded where |
|---|---|---|---|
| Isolation unit | containers | native Windows processes started by the runner; no container image | run record `process_limits`; `results/DEVIATIONS.md` |
| Fault injection | restarted containers | real process kills (`taskkill /F`), real TCP cuts (Toxiproxy), JVM failpoints for Level 1 | `faults.log`; `docs/method.md` §5.5 |
| Resource limits | cgroup limits | CPU affinity mask, priority class, `-Xmx`, Hikari pool, broker/PG config | run record `process_limits`, §5 |
| Version pinning | image digests | SHA-256 of vendored archives/binaries + full config hashes | run record `versions`, `config` |
| Restart cadence | per-run fresh containers | fresh topics/group/schema and fresh consumer JVMs per run; broker/PG processes restarted every 50 runs | §4.2; D6 |
| Reproducibility claim | bit-identical container | deterministic from `(cell, rep)` given the pinned binaries; not bit-identical across hosts | threats T-R1/T-R3 |

These deviations are reported in the paper's threats section and in `results/DEVIATIONS.md` from the first run.

---

## 12. Deviations policy

1. `results/DEVIATIONS.md` is **generated** by the runner: a diff between the planned matrix (this document + `experiments/matrix.yaml`) and the executed runs, plus a hand-written entry for every methodological change (reason, date, effect on the analysis, section of the paper affected).
2. No cell may be silently dropped: a missing cell becomes a deviations row with `N_planned`, `N_valid`, `N_excluded`, and the reason.
3. Every deviation is reported in the paper (table or limitation paragraph).
4. The paper reports the truthful N per cell, the exclusion rate per cell, and the non-fired-fault count (A1 §11).
5. Changes to this document after the freeze are not allowed; they become deviations entries plus, if needed, a prereg amendment signed by the author.

---

## 13. Decisions requiring author approval (cross-reference)

This prereg is executable only after the author resolves the decisions registered in `docs/method.md` §13: **D1** budget/coverage, **D2** ordering probe (H3), **D3** M2 realisation, **D4** injector targeting, **D5** F6 semantics, **D6** reset protocol, **D7** sensitivity interpretation, **D8** I1 formalisation, **D9** I4 formalisation, **D10** ground-truth storage, **D11** scratch path, **D12** θ/certification language, **D13** M1-kafka comparability, **D14** configuration/fairness sign-off, **D15** paired seeds. This document assumes the draft recommendations stated there; changing a recommendation changes the matrices above and requires an amendment before the freeze.

## 14. Freeze checklist (signature block)

- [ ] Author approved D1–D15 (P1 gate).
- [ ] `docs/method.md` frozen with the author's formal-section edits.
- [ ] This prereg frozen; `docs/prereg-freeze.json` committed with script hashes.
- [ ] `experiments/matrix.yaml` generated and hash-committed; `experiments/run_order.csv` generated and committed before each stage.
- [ ] Pilot executed (P2); projection worksheet (§9.2) filled from measured values; cut ladder decision recorded; author re-approves the projected budget (P2 gate).
- [ ] Gates G1–G8 passed on pilot data; oracle mutation testing at 100 %.
- [ ] Environment deviations (§11) recorded in `results/DEVIATIONS.md`.
- [ ] `results/RUNLOG.md` initialised with the pinned commands and UTC timestamps.

| Role | Name | Decision | Date (UTC) |
|---|---|---|---|
| Author (owner of formal model and proofs) | João Brito | approve / revise | |
| Lead (integration, venue) | | acknowledge | |
| Methodologist (S2) | | submitted | 2026-11-24 |
