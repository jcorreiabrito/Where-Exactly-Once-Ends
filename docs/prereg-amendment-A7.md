# Pre-registration Amendment A7 — reduced confirmatory programme on the available native host

Status: normative deviation issued under the author's instruction of 2026-11-24 ("do not stop until
you have confirmatory data; stop only when the paper is complete"). The approved P2 plan
(`docs/P2-APPROVAL.md`) required a Linux/container host. No such host is reachable from this machine
and the environment cannot create one (no administrator rights, WSL2/Hyper-V unavailable — the
substrate constraint documented in A3/A4). Rather than fabricate or omit evidence, the campaign is
executed as a **reduced confirmatory programme** on the available native Windows stack (the A3
substrate), with the reframed analysis below. Every number still comes from executed runs under the
frozen pipeline; the paper must state the reduced design and its power limits.

## A7.1 Executed design (all rows from the frozen run order)

| Block | Runs | Purpose |
|---|---|---|
| Validation slate | 26 | G1/G3/G4/G5 harness gates, sham/self-test, quiescence, oracle fixtures |
| Performance block (M7 core only) | 54 | 9 mechanisms × {10k, 100k} × P6 × N=3, measured with mechanism-committed events |
| Stage A correctness | 153 | 51 Stage-A cells × N=3 (mechanisms × {ctrl,K1,K2,K3,K4,K5,K7 minus declared n/a}) |
| **Total** | **233** | |

Not executed (declared out of scope in the paper): pilot 216, Stage A remaining N (27), M7spot/M7sat
(calibration for B/C rates), Stage B (3,960), Stage C (1,800). Consequently: no fixed-rate Stage B/C
claims, no 1M/saturation claims, no confirmatory H1–H5 hypothesis tests.

## A7.2 Reframed analysis (descriptive/exploratory, no significance claims)

Because Stage-A N is 3 (not the pre-registered 30), the A2 inference (pooled stratified sign test,
H5 non-inferiority) is not identified and is **not** reported as confirmatory. The paper reports:

1. per-cell violation counts and proportions of the pre-registered consistency predicate (I1–I4),
   with two-sided 95 % Clopper–Pearson intervals (descriptive, not hypothesis tests);
2. the negative-control contrast (M0 vs mechanisms with idempotency/atomicity) as an exploratory
   comparison, with exact confidence intervals and explicit underpowering disclosure;
3. mechanism-committed throughput at 10k and 100k (N=3 per cell): median and range per mechanism,
   with the warm-up-excluded streaming window; no significance ranking;
4. harness-validation evidence (gates, mutation, negative controls, live smoke/fault efficacy where
   executed) unchanged and reported as validation, not mechanism data.

## A7.3 Substrate and provenance

Executed on the native Windows process substrate (A3): three KRaft brokers, PostgreSQL 16.11,
Toxiproxy 2.9, JDK 21. The pre-campaign freeze was re-issued in the same session
(`docs/prereg-freeze.json`, git_sha of tag `host-ready-v1`) so each record carries the frozen
pipeline's git_sha; all rows are resumable, logged in `results/RUNLOG.md`, and validated by the frozen
record validator. Mechanical deviations already disclosed (M5–M22, A3/A4) apply; the K6 zombie is n/a;
K5 evidence depends on the live protocol-replay injector and is reported only if executed validly.

## A7.4 Claims permitted in the paper

Permitted: a real, executed, mechanically-checked measurement of ledger-invariant preservation (and
its failure modes) for nine mechanism configurations under six crash classes at 1k events on the
A3 substrate, plus committed-event throughput at 10k/100k for the same nine mechanisms; plus the
formal model, the artifact, and the harness-validation programme.

Not permitted: any claim of the full pre-registered campaign, any confirmatory hypothesis verdict,
any Stage B/C fixed-rate or 1M/saturation performance claim, any claim that the reduced design has
the pre-registered statistical power, or any number not traceable to `results/` + `results/RUNLOG.md`.

## A7.5 What was actually executed (final accounting)

The campaign produced **154 records: 152 valid, 2 excluded**. Correctness: **40 cells / 119 valid
Stage-A runs** ($N=3$ where complete, $N=2$ for one M3-K1 cell after a readiness retry). Performance:
**14 mechanism/load cells** (10k: $N=3$ for seven mechanisms; 100k: $N=1$ for seven mechanisms;
M2/M2+M3 censored by the outbox relay). The following were **not executable** on the native
substrate and are excluded, counted and disclosed: (i) K5 protocol replay (external S4 replay not
realisable); (ii) K1 for M1x and M1x+M3 (transactional-producer fencing after the failpoint crash
prevents restart convergence); (iii) K2 for M1-kafka (marker-driven injection never reaches the
seeded progress marker before the load ends). Two injector-realisation fixes were applied during
the campaign and the tree was re-frozen once (K1 trigger offset on the assignment-hook fire;
marker-window fixed rate for Stage-A K2). Records from affected cells before the fix are archived
under `results/raw/invalid-attempts/` and none appear in the analysis.

Analysis output: `results/analysis-reduced/` (CSVs, LaTeX tables, macros, `summary.json`), generated
by `tools/analyze_reduced.py`; the paper's Tables 2–4 and the abstract numbers come from it. Key
observations: the four benign classes (fault-free, K1, K2, K7) produced no violation in the executed
runs; K3 duplicated only in the unguarded M0 (302 duplicate postings) and M1x (462); the K4
early-ordering probe violated in **24/24 runs across all eight exposed mechanisms** with 272–494
lost postings per cell, including M4's in-transaction offsets. These are descriptive results, not
confirmatory hypothesis verdicts.

