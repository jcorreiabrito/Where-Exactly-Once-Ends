# artifact — reference implementation ("Where Exactly-Once Ends")

Java 21 / Spring Boot 3.3 / spring-kafka reference implementation of the nine ledger mechanisms
(M0, M1-kafka, M1x, M2, M3, M4, M2+M3, M4+M3, M1x+M3) against a PostgreSQL 16 sink and a Kafka
publication stream, plus the deterministic load generator and the native-process harness.

Implemented against the frozen interface in `docs/CONTRACTS.md` and the method in
`docs/method.md` (§1.3–1.4, §4, §5, §7.4–7.7, Appendices B–C), with the P1 approval
(`docs/P1-APPROVAL.md`) and the P2 rulings (`docs/P2-RULINGS.md`). This file is the operator
manual; the authoritative design text remains the method/prereg documents.

---

## 1. Build

The host has no Maven on `PATH` and no Docker. Use the vendored toolchain (JDK 21 + Maven 3.9.9) via
the helper:

```powershell
# unit tests only; no Docker, no Kafka, no PostgreSQL required (CI-safe)
powershell -ExecutionPolicy Bypass -File artifact/build.ps1 test

# runnable flat jar -> artifact/target/wxeo-service.jar
powershell -ExecutionPolicy Bypass -File artifact/build.ps1 package

# integration tests against the native stack (see §3), tagged @Tag("integration")
powershell -ExecutionPolicy Bypass -File artifact/build.ps1 verify -Pintegration
```

`artifact/build.ps1` sets `JAVA_HOME` to `%EOS_RUNTIME_ROOT%\vendor\jdk21` (default
`%LOCALAPPDATA%\eos-runtime`) and runs `%EOS_RUNTIME_ROOT%\vendor\maven\bin\mvn.cmd`.

Equivalent direct commands (with `JAVA_HOME` pointing at the vendored JDK 21):

```powershell
& "$env:EOS_RUNTIME_ROOT\vendor\maven\bin\mvn.cmd" -B -ntp -f artifact/pom.xml test
& "$env:EOS_RUNTIME_ROOT\vendor\maven\bin\mvn.cmd" -B -ntp -f artifact/pom.xml package
```

The shaded jar is **flat** (all dependency classes at the root) on purpose: both contract CLI forms
work from the single path `artifact/target/wxeo-service.jar`:

```powershell
java -jar artifact/target/wxeo-service.jar --wxeo.mechanism=M3 ...          # service
java -cp  artifact/target/wxeo-service.jar com.wxeo.load.LoadGenerator ...   # load generator
```

(A Spring-Boot-repackaged jar stores classes under `BOOT-INF/classes`, which would break the
`-cp ... com.wxeo.load.LoadGenerator` form required by `docs/CONTRACTS.md` §7.)

### Tests

* Unit tests (default `mvn test`, 45 tests): canonical derivation / normative G5 vectors,
  ground-truth file + digest, commit plans and the crash-injection state-machine simulation of
  P2/P3/P8/P9, retry-table classification, failpoint engine incl. the real crash path
  (child JVM exits 99 with one `fired` receipt), effective-config/G6 hashing, HdrHistogram
  artefacts, loader encoding.
* Integration tests (`*IT.java`, `@Tag("integration")`, excluded from `mvn test` by Surefire;
  run with `mvn verify -Pintegration` when the native stack is up): PostgreSQL schema/append-only
  trigger/dedup-index/idempotent-insert, Kafka create-produce-consume-delete with the Java clients.

---

## 2. Service CLI (frozen contract)

```powershell
java -Xms2g -Xmx2g -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError ^
  -Dwxeo.failpoints.enabled=true ^
  -jar artifact/target/wxeo-service.jar ^
  --wxeo.mechanism=M0^|M1-kafka^|M1x^|M2^|M3^|M4^|M2+M3^|M4+M3^|M1x+M3 ^
  --wxeo.ordering=strict^|early ^
  --wxeo.run-id=<run_id> --wxeo.instance-id=1 ^
  --wxeo.bootstrap=127.0.0.1:19092 ^
  --wxeo.db-url=jdbc:postgresql://127.0.0.1:15433/wxeo --wxeo.db-user=wxeo_app ^
  --wxeo.db-schema=wxeo_run_<run_id> ^
  --wxeo.source-topic=wxeo.src.<run_id> --wxeo.sink-topic=wxeo.sink.<run_id> ^
  --wxeo.group=wxeo.cg.<run_id> ^
  --wxeo.output-dir=<local-scratch-dir> --wxeo.faults-log=<faults.log> ^
  [--wxeo.failpoint.enabled=true --wxeo.failpoint.name=<name> ^
   --wxeo.failpoint.action=crash^|throw^|abort_txn^|pause[:ms]^|none --wxeo.failpoint.after=N]
```

Notes:

* Spring Boot context is headless (no web server) and intentionally does not run auto-configuration;
  the service is wired through `ServiceConfiguration` (args → `EffectiveConfig` → `ServiceRunner`).
* The measured consumer loop uses the native Kafka clients directly (exact control of poll
  boundaries, offset commits, transaction boundaries and rebalance callbacks — these are the
  experimental instruments). spring-kafka is used for producer construction (publication stream)
  and is on the classpath for the Kafka configuration constants. All Kafka/PG settings are pinned
  in `EffectiveConfig` (§4.11 + Appendix C of the method) and recorded in the config artefact.
* `--wxeo.schema-init=none|auto` (default `none`): the runner normally creates the per-run schema
  with `com.wxeo.sink.SchemaTool` (below); `auto` makes the service create missing tables if the
  DB user has the rights.
* `--wxeo.require-append-only-attestation=true|false` (default `false`): fail fast if the
  application role can `UPDATE`/`DELETE` `posting` or the append-only trigger is missing. The
  attestation result is always recorded in `app_<run>_<instance>.json` and logged.

### Graceful stop vs crash

* **Graceful**: create `<output-dir>/stop_<run_id>_<instance>.flag` — polled every 200 ms — or send
  SIGINT/SIGTERM (JVM shutdown hook). The service finishes the current poll batch, stops the relay
  (M2), writes `app_<run_id>_<instance>.json` + histogram dumps, exits 0.
* **Crash**: `taskkill /F /PID <pid>` kills the JVM with no artefacts — this is the modelled crash.
  A failpoint with action `crash` exits with code **99** after writing exactly one `fired` receipt.

### Artefacts written by the service (`--wxeo.output-dir`)

| File | Content |
|---|---|
| `config_<run>_<instance>.json` | full effective config, written+fsynced **before** consuming; contains `config_sha256` (full), `shared_sha256` (G6), `intrinsic_sha256`, and the `identity`/`shared`/`intrinsic` blocks |
| `ready_<run>_<instance>.json` | extension: readiness probe (`status`: `subscribed`/`assigned`, `partitions`, `group`, `ts_utc`) |
| `app_<run>_<instance>.json` | shutdown artefact: mechanism, instance, ordering, counters, effective config JSON, JVM args, start/stop UTC, stop reason, fatal error, attestation, histogram paths |
| `hist_<run>_<instance>.hlog` | HdrHistogram compressed dump of **per-batch** e2e latencies (µs), 3 significant digits, 1 µs..60 s (CONTRACTS §6) |
| `hist_<run>_<instance>_event.hlog` | HdrHistogram dump of **per-event** e2e latencies (µs) — primary definition (method §7.2) |
| `hist_<run>_<instance>.json` | percentiles for event and batch histograms (p50/p90/p99/p99.9/max in ms), counts, paths |

End-to-end latency per event = `commit_observed_ms − scheduled_at_ms` (one host clock); for all DB
mechanisms the observation is the PostgreSQL `COMMIT` return, for M1-kafka it is
`commitTransaction` returning. Per-batch latency uses the newest event's schedule time.

### Failpoints (Level-1 deterministic, `docs/method.md` §5.5, Appendix B)

Fixed names: `after_fetch, before_effect_commit, after_effect_commit, before_progress_commit,
after_progress_commit, before_publish, before_relay_mark`.

* Armed by `--wxeo.failpoint.enabled=true` **or** `-Dwxeo.failpoints.enabled=true`; one failpoint per
  process; fires at most once, on the N-th hit (`--wxeo.failpoint.after=N`, default 1).
* Actions: `crash` (`Runtime.halt(99)`), `throw` (generic injected fault: roll back, no progress
  commit, replay), `abort_txn` (abort the open Kafka/DB transaction, no progress commit, replay),
  `pause[:ms]` (sleep, then continue), `none` (sham: receipts only, no effect).
* Receipts append to the S4-provided `--wxeo.faults-log`, one compact NDJSON line per event with
  exactly `{ts_utc, run_id, fault, event, detail}`; `fault` = failpoint name; `event` =
  `fired`|`restored`. `crash` writes `fired` only (terminal); all other actions write `fired` then
  `restored` (after rollback/pause). Example:
  `{"ts_utc":"2026-11-24T12:00:00.123Z","run_id":"a-m0-k3-1k-p3-r07","fault":"before_effect_commit","event":"fired","detail":"action=crash;point=before_effect_commit;hit=1;after=1;mechanism=M0;instance=1;run_id=..."}`
  The detail field also carries the phase marker (`point`), action, hit/after counts, mechanism and
  instance. S4's external injectors append their own receipts to the same file.

---

## 3. Native stack harness

`artifact/harness/native-stack.ps1` starts/stops the native-process substrate (there is no Docker on
this host). Runtime data goes to `%EOS_RUNTIME_ROOT%\run` (local, non-cloud-synced; D11). Requires
**PowerShell 5.1+** (works with both `powershell` and `pwsh`).

```powershell
powershell -ExecutionPolicy Bypass -File artifact/harness/native-stack.ps1 start-pg
powershell -ExecutionPolicy Bypass -File artifact/harness/native-stack.ps1 start-kafka
powershell -ExecutionPolicy Bypass -File artifact/harness/native-stack.ps1 start-toxiproxy
powershell -ExecutionPolicy Bypass -File artifact/harness/native-stack.ps1 status
powershell -ExecutionPolicy Bypass -File artifact/harness/native-stack.ps1 stop-kafka
powershell -ExecutionPolicy Bypass -File artifact/harness/native-stack.ps1 restart-all   # D6: every 50 runs
```

Verbs: `start-kafka | stop-kafka | start-pg | stop-pg | start-toxiproxy | stop-toxiproxy | status |
restart-all`. Vendored binaries are resolved from `EOS_RUNTIME_ROOT\vendor` (default
`%LOCALAPPDATA%\eos-runtime\vendor`); `artifact/harness/stack.json` holds ids/ports/paths/settings.
Every start/stop writes the state file:

```
$EOS_RUNTIME_ROOT\run\stack-state.json      # schema wxeo.stack-state.v1: pids, command lines, ports, paths
```

S4's injectors read process ids and log paths from there (F1 broker kill, F2 consumer kill start
from this file). Logs under `$EOS_RUNTIME_ROOT\run\logs` and per-node `run/kafka/node<id>/logs`.

| Component | Layout |
|---|---|
| Kafka 3.7.2 KRaft | 3 combined broker+controller nodes; brokers `127.0.0.1:9091-9093`, controllers `127.0.0.1:9192-9194`; RF=3, `min.insync.replicas=2`, transaction-state RF=3, `transaction.state.log.min.isr=2`, `offsets.topic.replication.factor=3`, `unclean.leader.election.enable=false`, `auto.create.topics.enable=false`, `group.initial.rebalance.delay.ms=0`; data `run/kafka/node<id>/data` |
| PostgreSQL 16.11 | `127.0.0.1:5433`, database `wxeo`, trust auth for localhost; data `run/pg/data`, log `run/pg/postgres.log`; Appendix-C settings (`fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, `shared_buffers=2GB`, `wal_level=replica`, `track_commit_timestamp=on`, …); roles `postgres` (admin), `wxeo_app` (LOGIN; granted INSERT/SELECT on `posting`, no UPDATE/DELETE), `wxeo_read` (LOGIN, read-only) |
| Toxiproxy 2.9.0 | API `127.0.0.1:8474`; proxies `kafka` `127.0.0.1:19092 → 127.0.0.1:9092`, `db` `127.0.0.1:15433 → 127.0.0.1:5433` (contract), **plus** extension proxies `kafka-produce` `19093 → 9092` and `kafka-consume` `19094 → 9092` for S4's semantic-role targeting of F3 (produce-only) and F4a (offset-commit link); ignore the extras if not needed |

Kafka is invoked via `java -cp "<kafka>\libs\*" kafka.Kafka server.properties` because the stock
`.bat` wrappers exceed the Windows command-line limit (see `docs/SUBSTRATE-VALIDATION.md` §2).

### Per-run state helpers (S4 fresh-state protocol, CONTRACTS §10)

```powershell
# create the mechanism-specific schema/objects (idempotent). DDL identical for all DB mechanisms;
# dedup index only for M3/M2+M3/M4+M3/M1x+M3; aux tables only where the mechanism needs them.
java -cp artifact/target/wxeo-service.jar com.wxeo.sink.SchemaTool `
  --mode=init --db-url=jdbc:postgresql://127.0.0.1:5433/wxeo --db-user=postgres `
  --schema=wxeo_run_<run_id> --mechanism=M3 --app-role=wxeo_app --read-role=wxeo_read

# attestation check / drop (fresh state)
java -cp artifact/target/wxeo-service.jar com.wxeo.sink.SchemaTool --mode=verify ...
java -cp artifact/target/wxeo-service.jar com.wxeo.sink.SchemaTool --mode=drop ...

# G5 cross-check with the independent Python oracle (prints gt_sha256 + sample lines)
java -cp artifact/target/wxeo-service.jar com.wxeo.tools.GroundTruthDigest `
  --seed-hex=<16hex> --events=100 --partitions=6
```

Topics are created by the runner (`kafka.tools.TopicCommand` or AdminClient), e.g.:

```powershell
$kafka = "$env:EOS_RUNTIME_ROOT\vendor\kafka"
$java = "$env:EOS_RUNTIME_ROOT\vendor\jdk21\bin\java.exe"
& $java -cp "$kafka\libs\*" kafka.tools.TopicCommand --bootstrap-server 127.0.0.1:9091 `
  --create --topic wxeo.src.<run_id> --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

---

## 4. Load generator (frozen CLI)

```powershell
java -cp artifact/target/wxeo-service.jar com.wxeo.load.LoadGenerator ^
  --run-id=<run_id> --bootstrap=127.0.0.1:19092 --topic=wxeo.src.<run_id> ^
  --events=1000 --partitions=3 --rate=max ^
  --data-seed-hex=<16hex> --injection-seed-hex=<16hex> ^
  --gt-out=<scratch>\gt_<run_id>.ndjson --manifest-out=<scratch>\manifest_<run_id>.json ^
  [--produced-out=<scratch>\produced_<run_id>.ndjson] [--ack-timeout-ms=120000]
```

* `--rate=<events/s>` = open-loop fixed rate (`scheduled_at_ms` from a per-seq schedule, no
  closed-loop feedback); `--rate=max` = saturation/unpaced (scheduled time = send initiation).
* Phase 1 derives the complete event set, sorts the postings by `(event_id, leg_no)`, writes
  `gt_<run_id>.ndjson` + `gt_<run_id>.sha256` + the manifest and **fsyncs all three before the first
  produce** (A1 §4, D10). Phase 2 produces from the same derivation (never reads GT back), explicit
  partition per event, `acks=all`, `enable.idempotence=true`, and records per-event ack timestamps.
* `produced_<run_id>.ndjson` (written after all acks, fsynced, ordered by seq) lines:
  `{"event_id":...,"partition":P,"offset":O,"ack_ts_ms":T,"scheduled_at_ms":S}`.
* Manifest keys: `spec`, `run_id`, `master_seed` (canonical decimal used in the hashes),
  `master_seed_raw`, `data_seed_hex`, `injection_seed_hex`, `events`, `partitions`, `gt_path`,
  `gt_sha256`, `gt_lines` (=2·events), `produced_path`, `topic`, `rate`, `created_utc`,
  `loader_java_version`.
* Seed handling (P2-RULINGS R1): the per-repetition `data_seed` is an unsigned 64-bit value rendered
  as 16 hex chars; the loader converts it to canonical decimal ASCII and uses *that text* in both
  domain-separated SHA-256 inputs. `--master-seed=<u64 decimal>` remains accepted.

---

## 5. Canonical formats (normative; G5)

Derivation (`docs/method.md` §1.4.1, with the P2 erratum approved as ruling R5 — see §6):

```
seed_text = decimal(master_seed)                                  # u64, no sign/leading zeros
h1 = SHA-256("wxeo1|event|" + seed_text + "|" + decimal(seq))
event_id   = lower_hex(h1[0:16])          # 32 hex chars
account_d  = "acct:" + lower_hex(h1[16:22])   # 6 bytes -> 12 hex
account_c  = "acct:" + lower_hex(h1[22:28])
amount_minor = 1 + (uint64_be(h1[28:36]) mod 100000)   # erratum: effective 4-byte slice h1[28:32]
h2 = SHA-256("wxeo1|part|" + seed_text + "|" + decimal(seq))
partition = uint64_be(h2[0:8]) mod P
```

**Spec erratum (R5, pre-freeze, approved):** the original §1.4.1 window `h1[28:36]` exceeds the
32-byte SHA-256 digest, so the frozen effective form is the literal in-bounds slice
`uint32_be(h1[28:32]) mod 100000` (the 4 available bytes of the requested window, big-endian).
`docs/derive-vectors.csv` holds the normative Java/Python vectors and a unit test loads and asserts
every row; `experiments/oracle/g5-vectors.json` (S4) is cross-checked by
`com.wxeo.tools.GroundTruthDigest`. This changes no cell, N, factor or statistic.

Canonical source-event value (Kafka record value, UTF-8, compact JSON, fixed key order):

```
{"spec":"wxeo.ledger-event.v1","event_id":"...","seq":N,"currency":"BRL",
 "legs":[{"account_id":"acct:...","side":"D","amount_minor":A},
         {"account_id":"acct:...","side":"C","amount_minor":A}],
 "scheduled_at_ms":T}
```

Canonical publication-stream entry value (`wxeo.sink.<run_id>`, key = `event_id`, same partition as
the source record):

```
{"spec":"wxeo.entry.v1","event_id":"...","seq":N,
 "legs":[{"account_id":"...","side":"D","amount_minor":A},
         {"account_id":"...","side":"C","amount_minor":A}]}
```

Ground-truth line (one posting leg per line, LF endings, ordered by `(event_id, leg_no)` with
byte-wise event_id ordering, `line_no` 0-based, `leg_no` 1-based):

```
{"line_no":0,"event_id":"...","leg_no":1,"account_id":"...","side":"D","amount_minor":A,"currency":"BRL"}
```

`gt_<run_id>.sha256` = lowercase hex SHA-256 of the exact GT file bytes + one LF.

---

## 6. Known deviations from `docs/method.md` / `docs/CONTRACTS.md`

| # | Deviation | Reason / impact |
|---|---|---|
| D-S3-1 | `h1[28:36]` amount window evaluated as the available 4-byte slice (lead ruling R5, docs updated) | spec erratum; no invariant/statistic impact; normative vectors shared with S4 |
| D-S3-2 | PostgreSQL schema named `wxeo_run_<run_id>` (CONTRACTS §2 updated by lead; method §8.3 agrees) | naming only |
| D-S3-3 | M2/M2+M3 `outbox` table carries an extra `source_partition int NOT NULL` column | method §1.4.4/§4.5 require publication "to the same partition index as the source record"; the frozen outbox DDL had no place for it. Auxiliary table only; posting DDL unchanged; no invariant impact |
| D-S3-4 | M4 `consumer_progress` writes use an upsert (`INSERT … ON CONFLICT DO UPDATE … WHERE next_offset < EXCLUDED.next_offset`) instead of the literal `UPDATE` of method §4.7 | the literal UPDATE would never create the first row for a partition, so M4 could not persist progress; the monotone guard is preserved exactly |
| D-S3-5 | Toxiproxy also exposes additive split proxies `kafka-produce` (19093) / `kafka-consume` (19094) beside the contract `kafka`/`db` proxies | method §5.5 F3/F4a require a produce-only / offset-commit-only link; additive, ignore if unused |
| D-S3-6 | Service runs a Spring Boot context without auto-configuration; native Kafka clients drive the measured loop, spring-kafka builds the publication producer | the experiment needs exact control of poll boundaries, offset commits and transaction boundaries; spring-kafka's container would hide crash points. Same Kafka client library either way |
| D-S3-7 | `ready_<run>_<instance>.json` and `--wxeo.schema-init` / `--wxeo.require-append-only-attestation` / `--wxeo.failpoint.action=none` are additive extensions | readiness/fresh-state/shammode helpers for S4; core contract files unchanged |
| D-S3-8 | Batch e2e histogram = latency of the newest event in the batch relative to its schedule time | per-event histogram is the primary definition (method §7.2); per-batch is the secondary diagnostic required by CONTRACTS §6 |
| D-S3-9 | Poison records are skipped and their offsets committed (documented policy §4.10) | matches §4.10 poison-record rule; counted in `poison_records` |

Environment deviation (native processes instead of containers, D6, D11) is documented in
`docs/prereg.md` §11 and `docs/SUBSTRATE-VALIDATION.md`; runtime data lives in
`%EOS_RUNTIME_ROOT%\run`, never in the cloud-synced repository.

---

## 7. Mechanism map (implementation)

| Mechanism | Class | Effect commit | Progress commit | Publication | Dedup |
|---|---|---|---|---|---|
| M0 | `mechanism/M0` | DB txn, plain INSERT | Kafka offsets after effect | inline, non-txn | none |
| M1-kafka | `mechanism/M1Kafka` | Kafka transaction (sink topic) | same Kafka transaction | the sink itself | Kafka txn |
| M1x | `mechanism/M1x` | DB txn **outside** the Kafka txn | Kafka txn (offsets + stream) | inside Kafka txn | none |
| M2 | `mechanism/M2` | DB txn {postings, inbox claim, outbox row} | Kafka offsets after effect | outbox relay (at-least-once) | inbox PK |
| M3 | `mechanism/M3` | DB txn `ON CONFLICT DO NOTHING` | Kafka offsets after effect | inline, non-txn | posting unique index |
| M4 | `mechanism/M4` | DB txn {postings, consumer_progress} | same DB txn; seek on assignment; Kafka offsets observability-only | inline, non-txn | none |
| M2+M3 | `mechanism/M2M3` | DB txn {idem postings, idem outbox} | Kafka offsets after effect | outbox relay | posting index |
| M4+M3 | `mechanism/M4M3` | as M4 with idem postings | same DB txn | inline | posting index |
| M1x+M3 | `mechanism/M1xM3` | as M1x with idem postings | Kafka txn | inside Kafka txn | posting index |

`ordering=early` (D2 probe for every two-commit mechanism) swaps the two commits so that progress
becomes durable before the effect — it is recorded in the config/`app` artefacts and is the witness
for W2/H3; canonical mechanisms run `strict`. M1-kafka has one atomic commit, so the flag is
recorded as n/a. The declared duty order per mechanism/ordering is executable:
`mechanism/CommitPlan` + `MechanismFactory.plan(...)`, asserted by unit tests.

Retry semantics are exactly §4.10 (`sink/RetryPolicy`, `sink/SqlRetry`): SQLSTATE 40xxx retried
immediately (≤5), 08xxx retried with 100/300/900 ms backoff (≤3), unknown COMMIT outcome handled per
mechanism (no write retry for M0/M1x; idempotent retry for M2/M3 family; re-read own progress for
M4 family), Kafka offset-commit exhaustion stops the instance (progress stays behind).
