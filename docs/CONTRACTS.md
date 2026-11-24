# INTERFACE CONTRACTS — S3 (artifact) ↔ S4 (oracle/runner)

Frozen at P1 approval (2026-11-24). Both agents MUST implement against these names; changes go
through the lead and are recorded in `results/DEVIATIONS.md`. Normative sources:
`docs/method.md` (§1.3, §1.4, §4, §5, §7, Appendices B–C), `docs/prereg.md` (all), A1.

## 1. Ownership (no cross-writes)
| Owner | Tree | Must not touch |
|---|---|---|
| S3 | `artifact/**`, `artifact/harness/**` | `experiments/**`, `results/**`, `docs/**`, `main.tex` |
| S4 | `experiments/**`, `results/**`, `tools/analyze.py`, `tools/make-figures.py` | `artifact/**`, `docs/**`, `main.tex` |
| Lead | `docs/**`, paper, CI, git, TEAM | — |

## 2. Naming (per run)
- Source topic `wxeo.src.<run_id>`; publication stream `wxeo.sink.<run_id>`; consumer group `wxeo.cg.<run_id>`.
- Sink for DB mechanisms: PostgreSQL schema `run_<run_id>` (one schema per run; dropped after archive).
- `run_id` = `<stage>-<mechanism>-<fault>-<load>-<partitions>-<rep>` with an optional `-sat-` token
  for the Stage C saturation block. **Ruling 2026-11-24 (S4 proposal adopted):** lower-case, only
  `[a-z0-9-]` (Kafka-legal topic names, no `+`);
  mechanism tokens `m0, m1-kafka, m1x, m2, m3, m4, m2m3, m4m3, m1xm3`;
  stage `pilot|a|b|c`; condition `f0..f6, k1..k7, ctrl`; load `1k|10k|100k|1m`;
  partitions `p3|p6|p12`; repetition `rNN`.
  Examples: `a-m1x-k3-1k-p3-r07`, `b-m2m3-f1-100k-p6-r12`, `c-m2-f0-100k-p6-sat-r05`,
  `c-m2-f0-1m-p6-r03`; validation runs `pilot-valid-<kind>-rNN`.
  The pre-registered schema field stays `<stage>-<mech>-<condition>-r<rep>` semantically; this is
  the exact encoding, and `cell_id` remains the unique matrix-cell identifier.

## 3. Event spec and deterministic derivation
Exactly `docs/method.md` §1.4.1 (JSON schema `wxeo.ledger-event.v1`; SHA-256 derivation with domain
prefixes `wxeo1|event|` and `wxeo1|part|`), including the **pre-freeze erratum** of 2026-11-24:
the amount word is the literal in-bounds slice `uint32_be(h1[28:32]) mod 100000` (the original
`h1[28:36]` window exceeds the 32-byte digest; the corrected semantics is what both implementations
already evaluate). Normative reference vectors: **`docs/derive-vectors.csv`** — Java and Python
must reproduce every row byte-for-byte (gate G5). Java load generator and Python oracle implement
the spec independently.

## 4. Ground-truth file and canonical digest
- Phase 1 (before ANY produce): generate all events, write `gt_<run_id>.ndjson` — one posting leg per
  line, canonical JSON with keys exactly `line_no,event_id,leg_no,account_id,side,amount_minor,currency`,
  no spaces, LF line endings, ordered by `(event_id, leg_no)`; then `gt_<run_id>.sha256` (SHA-256 of
  the file bytes), both fsynced; record the digest in the run record.
- Phase 2: produce from the same deterministic derivation (not by re-reading GT).
- Oracle regenerates independently, verifies `gt` digest == regenerated digest == source-dump digest,
  then the runner may delete the bulk GT file (keeping digest + manifest; D10).
- **Canonical text encoding (ruling 2026-11-24, S4 proposal adopted):** GT `line_no` is **0-based**;
  posting `leg_no` is **1-based** (1..2). The derivation seed argument is the per-repetition
  `data_seed` of `docs/prereg.md` §4.4, interpreted as a big-endian unsigned 64-bit integer and
  rendered as canonical decimal ASCII (no sign, no leading zeros); Java and Python must use exactly
  that text in the domain-separated hashes. Seeds are stored/recorded as 16-hex-char strings; the
  loader receives the hex value (e.g. `--data-seed-hex=<16hex>`) and derives the decimal text
  internally; the oracle does the same independently. Partition derivation uses the same seed text
  with domain prefix `wxeo1|part|`. This is an encoding clarification: it changes no N, cell, factor
  or statistic.

## 5. Run-record schema
Exactly `docs/prereg.md` §5 JSON. S4 owns the validator; S3's processes write only the subordinate
files listed in §6. One run record per run: `results/raw/run-records/run_<run_id>.json`.

## 6. Subordinate artefact files (S3 writes; S4 consumes/archives)
| File | Written by | Content |
|---|---|---|
| `app_<run_id>_<instance>.json` | service at shutdown | mechanism, instance id, counters (polled batches, retries, duplicate-detected, conflicts), effective config JSON, JVM args, start/stop UTC, histogram path |
| `config_<run_id>_<instance>.json` | service at startup | full effective config (for G6 hash); printed before processing |
| `hist_<run_id>_<instance>.hlog` | service | HdrHistogram compressed dump of committed-batch e2e latencies (µs), plus `hist_<...>.json` percentiles p50/p99/p99.9 |
| `produced_<run_id>.ndjson` | load generator | `{event_id,partition,offset,ack_ts_ms,scheduled_at_ms}` |
| `faults.log` | S4 injectors + S3 failpoints | append-only NDJSON `{ts_utc,run_id,fault,event:fired|restored,detail}` |
| `stdout/stderr` | S4 | per-process logs |

## 7. CLI contracts
Service (Spring Boot jar, one JVM per consumer instance):
```
java [jvm-flags] -jar artifact/target/wxeo-service.jar \
  --wxeo.mechanism=M0|M1-kafka|M1x|M2|M3|M4|M2+M3|M4+M3|M1x+M3 \
  --wxeo.ordering=strict|early --wxeo.run-id=... --wxeo.instance-id=1 \
  --wxeo.bootstrap=host:port --wxeo.db-url=jdbc:postgresql://host:port/db \
  --wxeo.db-user=... --wxeo.db-schema=run_<run_id> \
  --wxeo.source-topic=... --wxeo.sink-topic=... --wxeo.group=... \
  --wxeo.output-dir=<scratch> --wxeo.faults-log=<path> \
  --wxeo.failpoint.enabled=true|false --wxeo.failpoint.name=<name> --wxeo.failpoint.action=crash|throw|abort_txn|pause \
  --wxeo.failpoint.after=N   # fire on the N-th hit
```
Load generator (main class in the same artifact):
```
java -cp artifact/target/wxeo-service.jar com.wxeo.load.LoadGenerator \
  --run-id=... --bootstrap=... --topic=... --events=N --partitions=P --rate=<events/s|max> \
  --master-seed=<u64> --gt-out=<path> --manifest-out=<path> --ack-timeout-ms=...
```
Failpoints names (fixed): `after_fetch, before_effect_commit, after_effect_commit, before_progress_commit,
after_progress_commit, before_publish, before_relay_mark`. Actions: `crash` (Runtime.halt(99)),
`throw`, `abort_txn`, `pause(ms)`. Failpoint receipts go to `faults.log`.

## 8. Native stack library (S3 owns `artifact/harness/`)
`native-stack.ps1` verbs: `start-kafka`, `stop-kafka`, `start-pg`, `stop-pg`, `start-toxiproxy`,
`stop-toxiproxy`, `status`, `restart-all`. Uses `EOS_RUNTIME_ROOT` (default `%LOCALAPPDATA%\eos-runtime`),
vendor paths from `tools/TOOLCHAIN-VERSIONS.txt`; IDs/ports from `artifact/harness/stack.json`
(3 brokers 9091–9093 + controllers 9192–9194, PG 5433, Toxiproxy 8474 / listeners 19092→9092 and
15433→5433). Every start writes `stack-state.json` with pids and command lines.
S4's runner calls these verbs; it does not start raw java itself.

## 9. Fault injection ownership
- S4 implements all injectors (process kill via pid from `stack-state.json`, Toxiproxy HTTP API,
  Kafka AdminClient offset reset via a small `java -cp ...` helper if needed).
- S3 implements only in-process failpoints (deterministic Level-1) and the `faults.log` writer API.
- Receipt protocol: exactly one `fired` and one `restored` per armed injection; S4 enforces, G4 checks.

## 11. Manuscript integration contract (S4 → paper)
`tools/analyze.py` (frozen before Stage A) MUST emit, from `results/raw/` only:
- `results/analysis/macros.tex` — `\newcommand` definitions for at least the macros already
  `\providecommand`-defaulted in `main.tex` (hypothesis verdicts, per-mechanism violating-run counts,
  early-probe loss runs, valid/excluded run totals). Values formatted for prose, e.g. `0.0\%`;
- `results/analysis/tables/t1-validation.tex` … `t6-decision.tex` — complete `table`/`tabular`
  environments (booktabs, `\small`), captions and labels:
  T1 harness gates G1–G8, T2 correctness by mechanism × fault class (violating runs / N + one-sided
  95 % upper bound), T3 duplicate/loss/invariant counts by mechanism, T4 performance by mechanism ×
  load × partitions (median/IQR, p99, p99.9, time-to-consistent), T5 TLC check summary (mirror of
  `formal/tlc-results/tlc-summary.json`), T6 practitioner decision table (S4 generates from
  `results/analysis/decision-table.csv`; the derivation rules are frozen in prereg §7.7);
- `figures/*.pdf` — vector figures generated by `tools/make-figures.py` from committed CSVs only:
  `fig-correctness.pdf` (mechanism × fault-class violating-run proportion with CIs),
  `fig-latency-ecdf.pdf` (fault-free latency ECDFs per mechanism), `fig-cost-coverage.pdf`
  (violations closed vs p99 overhead), `fig-throughput.pdf` (throughput by load/partitions).
- `results/analysis/tests.json` — every test, unadjusted and Holm-adjusted p, effect sizes with CIs.
`main.tex` inputs these conditionally (`\inputifexists` / `\IfFileExists`), so a missing artefact
renders a clearly marked placeholder, never a fabricated number. No prose number in the paper may
come from anywhere else.

## 12. Repository hygiene for these artefacts
Committed: `results/raw/run-records/`, `results/raw/summaries/`, `results/analysis/*.csv|json|tex`,
`figures/*.pdf`, `results/RUNLOG.md`, `results/DEVIATIONS.md`, `results/faults.log` (rotated if
large). Not committed: per-run dumps, GT bulk files, broker/PG data dirs, HdrHistogram `.hlog` bulk
(keep digests + summary percentiles). Large commits require the 3 GB free-space floor to hold.
