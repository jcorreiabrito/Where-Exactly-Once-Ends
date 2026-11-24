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
- `run_id` = `<stage>-<mechanism>-<fault>-<load>-<partitions>-<rep>` lower-case, no spaces.

## 3. Event spec and deterministic derivation
Exactly `docs/method.md` §1.4.1 (JSON schema `wxeo.ledger-event.v1`; SHA-256 derivation with domain
prefixes `wxeo1|event|` and `wxeo1|part|`). Java load generator and Python oracle implement it
independently and must agree byte-for-byte on the canonical posting lines and their digest (gate G5).

## 4. Ground-truth file and canonical digest
- Phase 1 (before ANY produce): generate all events, write `gt_<run_id>.ndjson` — one posting leg per
  line, canonical JSON with keys exactly `line_no,event_id,leg_no,account_id,side,amount_minor,currency`,
  no spaces, LF line endings, ordered by `(event_id, leg_no)`; then `gt_<run_id>.sha256` (SHA-256 of
  the file bytes), both fsynced; record the digest in the run record.
- Phase 2: produce from the same deterministic derivation (not by re-reading GT).
- Oracle regenerates independently, verifies `gt` digest == regenerated digest == source-dump digest,
  then the runner may delete the bulk GT file (keeping digest + manifest; D10).

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

## 10. Fresh-state protocol (S4)
Per run: create topics, create PG schema + tables (DDL identical for all DB mechanisms; dedup index
only in dedup mechanisms), fresh consumer group, fresh JVMs; drop schema/topics after archive.
Full-stack restart every 50 runs (D6) with `restart-all`.
