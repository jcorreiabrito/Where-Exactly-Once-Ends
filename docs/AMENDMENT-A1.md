# AMENDMENT A1 — binding methodology update (received 2026-11-24, from the author)

> Status: BINDING. Supersedes the original brief where they conflict. Everything else in
> `docs/BRIEF.md` (INTEGRITY, TOOLING, GITHUB, LATEX, VENUE, DEFINITION OF DONE) stays unchanged.
> Rule: do not stop, restart, or discard work already done. Finish the current step, then apply
> this incrementally. If S2 already wrote `docs/method.md`, REVISE in place (git diff visible).
> Hold S3/S4 code work depending on the old design until P1 is approved by the author. Anything
> independent (repo scaffold, CI skeleton, guard-secrets.ps1, build.ps1) may continue.
> Every methodological decision in `docs/method.md` and `docs/prereg.md` needs the author's
> explicit approval before it takes effect. S2 DRAFTS options and trade-offs; the author decides
> and owns the formal model and proofs. Log this amendment in TEAM.md.

## 1. Mechanisms (equal tuning effort; identical Kafka/PG settings for all)
- **M0** naive at-least-once, plain INSERT, offset committed after DB write.
  NEGATIVE CONTROL: must show violations, otherwise the harness is broken.
- **M1** Kafka EOS only (idempotent producer + transactions + read_committed), two variants:
  - **M1-kafka**: Kafka sink (positive control; zero violations expected in-scope).
  - **M1x**: same pipeline with a non-atomic external DB write (what practitioners deploy).
- **M2** transactional outbox.
- **M3** idempotency key with unique index `(event_id, leg_no)`, `INSERT ... ON CONFLICT DO NOTHING`.
- **M4** offsets stored in the same DB transaction as the postings (auto-commit off; seek to the
  stored offset on assignment/rebalance).
- **Combinations for RQ3**: M2+M3, M4+M3, M1x+M3. Justify any omitted combination.

## 2. Hypotheses (falsifiable, pre-registered; report confirmed / refuted / inconclusive)
- **H1** M1x produces >=1 invariant violation where DB commit and offset commit can diverge;
  M1-kafka produces none. (Known in principle; contribution is the mapping, measured frequency, cost.)
- **H2** M2 yields zero violations across modelled failure classes, with measurable throughput and
  latency penalty vs. M3 and M4.
- **H3** M3 alone yields zero duplicates but yields LOST postings when the offset is committed
  before the DB write is durable (commit-ordering failure). Dedup cannot restore a posting never
  written. (Replaces old H3, which was not falsifiable.)
- **H4** M4 (or M4+M3) yields zero violations across F1–F6 with lower latency than M2.
- **H5** No single mechanism dominates on both correctness and performance across all failure classes.

## 3. Formal part (author owns it; team drafts)
- Invariants: **I1** every entry sums to zero per currency; **I2** postings append-only;
  **I3** at most one entry per business event id; **I4** every consumed event eventually has exactly
  one entry (liveness under fair recovery).
- Define the EOS boundary formally as the transitions covered by Kafka's atomic commit; state
  assumptions (crash-recovery faults, no Byzantine, DB isolation level, clocks).
- Crash-point model **C1..Cn**: enumerate every point where a process may crash or a message may be
  lost/duplicated/reordered. Map each fault F1–F6 to the crash points it exercises.
- Every (mechanism x crash point) cell gets a proof sketch or a concrete counterexample trace.
- RQ3 and the decision table are answered by this model; experiments are evidence, not proof.
- Optional: TLA+/Alloy model checked with TLC at small bounds; report the bounds.

## 4. Oracle (independent; built BEFORE the mechanisms)
- Load generator writes ground-truth expected postings to file BEFORE sending to Kafka.
- Separate checker (no shared code with the pipeline) reads DB + ground truth and computes
  duplicates (I3), losses (I4), imbalance (I1), mutated/deleted rows (I2).
- Quiescence defined and timed (no commits for T seconds and lag = 0).

## 5. Harness validation (gate before any measurement)
- Negative control (M0 shows predicted violations) + positive control (M1-kafka and M4+M3 under
  no faults show zero).
- Mutation testing of the checker (drop row, duplicate row, flip leg sign, update row):
  **100% detection required**.
- Every injector logs a timestamped event to `results/faults.log` when it fires. Runs where the
  fault did not fire are excluded and counted; report exclusion rate per cell.

## 6. Fault injection, two levels
- **Level 1 deterministic**: crash-point injection at each C1..Cn (code hooks behind a flag,
  JVM agent/failpoints, Toxiproxy).
- **Level 2 stochastic**: F1–F6 at random times, recorded seed.

## 7. Experiment design — stages (replace full-factorial matrix)
- **Pilot first** (N=3, small load): measure time per run and write projected total compute time
  in `docs/prereg.md`. If it exceeds available time, cut in this order: 1M load, then partition
  sensitivity, then repetitions down to N>=20 for correctness cells. Never cut controls.
- **Stage A** correctness, deterministic: all mechanisms x all crash points, 1k events, N=30.
- **Stage B** correctness, stochastic: all mechanisms x F1–F6, 100k events, 6 partitions, N=30;
  sensitivity at 10k events and 3/12 partitions, N=10.
- **Stage C** performance, fault-free: mechanisms x {10k,100k} x partitions {3,6,12}, N=30;
  1M events at 6 partitions only, N=10.
- **Stage D** performance under faults + time to consistent state: reuse Stage B runs.
- Randomize and interleave run order across mechanisms. Fresh state per run (new topic, empty
  schema, restarted components).
- Note (lead): this host has no Docker/WSL; "containers" becomes native Kafka KRaft broker
  processes + PostgreSQL + Toxiproxy. Deviation must be explicit in prereg and threats.

## 8. Measurement
- Open-loop load at fixed target rate (avoid coordinated omission), **HdrHistogram**, warm-up
  discarded, steady-state window defined in advance.
- Pin and record container/process limits, JVM flags, Kafka/PG configs (acks=all,
  min.insync.replicas, idempotence, read_committed, fsync), Docker/OS versions (or native
  process/OS versions on this host).
- Metrics: duplicates, losses, violations by I1–I4, throughput, p50/p99/p99.9 latency, time to
  consistent state.

## 9. Statistics (in `docs/prereg.md`, committed BEFORE any experiment; analysis code frozen)
- Correctness outcomes are mostly-zero counts: report proportion of violating runs with
  **Clopper–Pearson 95% CI**; for zero events report the upper bound (rule of three, 3/N).
  Compare with exact tests (Fisher/McNemar) + **Holm** correction. "Zero observed" is never "proved".
- Performance: median + IQR, bootstrap 95% CI (10,000 resamples, recorded seed). **Friedman**
  across mechanisms with blocks = (load x partitions) within each stage, aggregated to block
  medians; **Nemenyi** post-hoc; alpha=0.05; **Cliff's delta** with CI and thresholds fixed in
  advance.
- Report unadjusted and adjusted p-values; label anything not in the prereg as exploratory.
- Check run independence and report any autocorrelation.

## 10. Threats to validity
Internal (harness defects, faults that did not fire, config bias, order drift), construct (single
currency, no reversals, oracle vs. accounting practice), external (single-node vs. multi-AZ
cluster, synthetic load, version specificity), conclusion (multiple comparisons, zero-event
bounds), reproducibility (seeds, pinned image/process digests, raw CSVs, scripts regenerating
every figure).

## 11. Red team additions
S5 additionally attacks: can the oracle miss violations; did faults actually fire; is the
mechanism comparison fair (same configs, hardware, tuning effort); any post-hoc analysis not in
the prereg. S4 executes ONLY what `docs/prereg.md` specifies; deviations go to
`results/DEVIATIONS.md`.

## 12. Phases
- **P1** now also produces `docs/prereg.md` (+ optional `formal/ledger.tla`) and **STOPS for author
  approval**.
- **P2** adds oracle + harness validation + pilot and **STOPS again** for the author to review the
  pilot and the projected compute budget before P3.
