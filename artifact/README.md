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
| D-S3-10 | **F-M2 fixed**: all durability-critical writes use real fsync (`FileChannel.force(true)` / `FileDescriptor.sync`); the previous `instanceof FileOutputStream` check was a silent no-op | S5 finding; proof: `DurabilityTest` child-JVM halt+reopen test |
| D-S3-11 | **F-M1 extensions**: `--wxeo.warmup-events=N` (prereg §4.5 exclusion, recorded in `performance`), per-event `send_ts_ms` in `produced_*.ndjson`, explicit throughput numerator/denominator, split `--wxeo.producer-bootstrap` / `--wxeo.consumer-bootstrap` for F3/F4a semantic targeting | additive; core CONTRACTS files unchanged; S5/S4 hand-off |
| D-S3-12 | Harness creates a `wxeo_fault` role (LOGIN): SELECT on all tables + UPDATE only on `outbox`/`consumer_progress`; never on `posting` | needed for S4's F6/K7 state injection; append-only attestation stays non-vacuous |
| D-S3-13 | Harness verb `reset-kafka`: stop brokers, wipe `%EOS_RUNTIME_ROOT%\run\kafka`, reformat a fresh KRaft cluster | abrupt broker kills (F1) plus runner topic deletions can leave stray log dirs that make Kafka 3.7 fail with `KafkaStorageException`; documented recovery path (D6 full-stack restart) |
| D-S3-14 | `com.wxeo.tools.TopicTool` (AdminClient) replaces the Kafka distribution topic-tool classes in the harness/scripts | Kafka 3.x moved `kafka.tools.TopicCommand` to `org.apache.kafka.tools.*`; a jar-provided helper is version-stable |
| D-S3-15 | M1-kafka also fires `before/after_effect_commit` and `before_publish` around its single atomic commit | S5 finding: K2/K5/K6 must be realisable on the positive control; semantics unchanged (one atomic commit) |
| D-S3-16 | Toxiproxy 2.9.0 rejects the default Windows-PowerShell user agent with HTTP 403; harness and S4 must send an explicit `User-Agent` (harness uses `wxeo-harness/1.0`) | toolchain behaviour; documented in README §8.2 |
| D-S3-17 | **Kafka topic deletion is DISABLED** (`delete.topic.enable=false`) and `log.retention.check.interval.ms=3600000`; no harness/script ever calls AdminClient `deleteTopics` | on this Windows KRaft setup partition-dir rename during deletion fails, taking all log dirs offline and shutting the brokers down (observed 2026-11-24: `Error while renaming dir for <topic>-N` -> `Shutdown broker because all log dirs ... have failed`). Cleanup of per-run topics = `reset-kafka` (wipes and reformats the whole cluster, D6 cadence). Config is recorded per run; identical for all mechanisms, so no fairness impact, but the config hash changes once and must be re-frozen by S4 |

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


---

## 8. Hand-off contract for S4, and S5 red-team responses

### 8.1 `stack-state.json` (schema `wxeo.stack-state.v1`)

Path: `$EOS_RUNTIME_ROOT\run\stack-state.json` (default
`%LOCALAPPDATA%\eos-runtime\run\stack-state.json`). Rewritten by every `native-stack.ps1`
start/stop; `status` prints the same facts as JSON. S4 should re-read it after any `restart-all`.
Exact shape (values abbreviated here; real command lines/logs paths are included):

```json
{
  "schema": "wxeo.stack-state.v1",
  "updated_utc": "2026-11-24T13:29:14.000Z",
  "runtime_root": "C:\\Users\\<user>\\AppData\\Local\\eos-runtime",
  "toolchain": { "versions_file": "../../tools/TOOLCHAIN-VERSIONS.txt", "checks": {...}, "ok": true },
  "components": {
    "kafka": {
      "status": "running", "started_utc": "...", "cluster_id": "<base64url>",
      "bootstrap": "127.0.0.1:9091",
      "controller_ports": [9192, 9193, 9194],
      "nodes": [
        { "id": 1, "broker_port": 9091, "controller_port": 9192, "pid": 5008,
          "data_dir": "...", "config": "...", "log_dir": "...", "command_line": "..." }
      ],
      "log_root": "..."
    },
    "postgres": {
      "status": "running", "started_utc": "...", "host": "127.0.0.1", "port": 5433,
      "pid": 27244, "data_dir": "...", "log_file": "...\\run\\pg\\data\\logs\\postgres.log",
      "database": "wxeo",
      "roles": { "admin": "postgres", "app": "wxeo_app", "read": "wxeo_read", "fault": "wxeo_fault" },
      "jdbc_url": "jdbc:postgresql://127.0.0.1:5433/wxeo",
      "jdbc_url_proxy": "jdbc:postgresql://127.0.0.1:15433/wxeo",
      "command_line": "..."
    },
    "toxiproxy": {
      "status": "running", "started_utc": "...", "api": "http://127.0.0.1:8474",
      "pid": 10780,
      "proxies": [
        { "name": "kafka", "listen": "127.0.0.1:19092", "upstream": "127.0.0.1:9092", "enabled": true, "role": "..." },
        { "name": "db", "listen": "127.0.0.1:15433", "upstream": "127.0.0.1:5433", "enabled": true, "role": "..." },
        { "name": "kafka-produce", "listen": "127.0.0.1:19093", "upstream": "127.0.0.1:9092", "enabled": true, "role": "extension: F3" },
        { "name": "kafka-consume", "listen": "127.0.0.1:19094", "upstream": "127.0.0.1:9092", "enabled": true, "role": "extension: F4a" }
      ],
      "log_dir": "..."
    }
  }
}
```

A stopped component is `{"status":"stopped","stopped_utc":"...","stopped_pids":[...]}` (the other keys
are dropped). Pids: Kafka broker pids in `components.kafka.nodes[].pid` (controller role shares the
same JVM/pid — combined KRaft nodes); PostgreSQL is the postmaster pid; Toxiproxy is the server pid
(its proxy listeners are listed under `proxies[].listen`; the proxy objects have no separate pids).

### 8.2 Toxiproxy proxies and the exact endpoints per fault

| Proxy name | Listener | Upstream | Use |
|---|---|---|---|
| `kafka` | `127.0.0.1:19092` | `127.0.0.1:9092` | contract "app↔Kafka" link (all Kafka traffic) |
| `db` | `127.0.0.1:15433` | `127.0.0.1:5433` | contract "app↔PostgreSQL" link (F5) |
| `kafka-produce` | `127.0.0.1:19093` | `127.0.0.1:9092` | extension: producer-only link (F3 produce timeout / unknown outcome) |
| `kafka-consume` | `127.0.0.1:19094` | `127.0.0.1:9092` | extension: consumer+offset-commit link (F4a offset-commit failure) |

The service supports split bootstraps (additive extension, defaults = the single frozen
`--wxeo.bootstrap`):

```powershell
# F0 / F1 / F2 / F6 (and F3/F4a controls): everything direct
--wxeo.bootstrap=127.0.0.1:9091 --wxeo.db-url=jdbc:postgresql://127.0.0.1:5433/wxeo
# F3: producer (and transaction coordinator) through kafka-produce, consumer direct
--wxeo.bootstrap=127.0.0.1:9091 --wxeo.producer-bootstrap=127.0.0.1:19093
# F4a: consumer + offset commits through kafka-consume, producer direct
--wxeo.bootstrap=127.0.0.1:9091 --wxeo.consumer-bootstrap=127.0.0.1:19094
# F5: DB through the db proxy
--wxeo.db-url=jdbc:postgresql://127.0.0.1:15433/wxeo
```

Toxiproxy HTTP API (2.9.0): create a toxic with
`POST /proxies/{name}/toxics` and body
`{"name":"t1","type":"timeout","attributes":{"timeout":5000},"stream":"downstream"}`; remove with
`DELETE /proxies/{name}/toxics/{toxic}`; the `timeout` toxic's attribute is **`timeout`** in
milliseconds; link cut = `POST /proxies/{name}` with `{"enabled":false}` or the
`toxiproxy-cli toggle <name>` command. **PowerShell caveat:** Toxiproxy 2.9.0 answers HTTP **403
"User agent not allowed"** to the default Windows-PowerShell user agent; use `toxiproxy-cli` or pass
an explicit agent (`Invoke-RestMethod -UserAgent 'wxeo-runner/1.0'`). The harness itself does this.

### 8.3 Readiness and shutdown artefacts (what the runner polls)

* `ready_<run_id>_<instance>.json` (extension; in `--wxeo.output-dir`), compact JSON keys:
  `schema` (`wxeo.ready.v1`), `run_id`, `instance_id`, `mechanism`, `group`,
  `status` = `subscribed` then `assigned`, `partitions` = sorted int array (latest assignment),
  `ts_utc`. Poll for `status == "assigned"`; for C=2 both instances must report their own file.
* Graceful stop: create `<output-dir>/stop_<run_id>_<instance>.flag` (polled every 200 ms); SIGTERM/
  SIGINT (JVM shutdown hook) also works. `taskkill /F` is a crash: no app artefacts by design.
* On graceful stop: `app_<run_id>_<instance>.json`, `hist_<run_id>_<instance>.hlog` (batch),
  `hist_<run_id>_<instance>_event.hlog` (per-event), `hist_<run_id>_<instance>.json`
  (percentiles + performance block). `app_*.json` keys: `schema`(`wxeo.app.v1`), `run_id`,
  `instance_id`, `mechanism`, `ordering`, `start_utc`, `stop_utc`, `stop_reason`, `fatal_error`,
  `jvm_args`, `counters` (see RunMetrics.COUNTER_NAMES), **`performance`** (8.5),
  `effective_config` (full config object), `config_sha256`, `shared_sha256`, `mechanism_impl`,
  `assigned_partitions`, `attestation` (can_update/can_delete/trigger_count/append_only),
  `histograms` {event_histogram_path, batch_histogram_path, percentiles_path}.

### 8.4 S5 finding F-M2 — real durability (fixed)

The previous `fsync` checks were silent no-ops because `Files.newOutputStream` does **not** return a
`FileOutputStream`; all durability-critical writes now use explicit
`FileChannel.force(true)` / `FileDescriptor.sync()`:

| Artefact | Path / code |
|---|---|
| ground-truth NDJSON | `ledger/GroundTruth.write` — `FileChannel.force(true)` before return |
| `gt_*.sha256` sidecar | `IoUtil.writeFsync` — `FileChannel.force(true)` |
| manifest JSON | `IoUtil.writeStringFsync` (same path) |
| `produced_*.ndjson` | `load/LoadGenerator` — `FileChannel.force(true)` |
| each `faults.log` receipt | `IoUtil.appendLineFsync` — `FileOutputStream.getFD().sync()` |
| `config_*.json`, `app_*.json`, `hist_*.json` | `IoUtil.writeAtomicFsync` — temp+force+atomic rename |

Proof in the unit suite: `DurabilityTest` spawns a child JVM (`DurabilityWriteProbe`) that performs a
forced write and then `Runtime.halt(0)` without any graceful close; the parent reopens the file and
verifies byte-identical content (`mvn test`).

### 8.5 S5 finding F-M1 — warm-up exclusion, throughput window, per-event send timestamp

* `--wxeo.warmup-events=N` (prereg §4.5): the first N **committed** events are counted
  (`committed_events_total`) and marked `warmup_events_excluded`, but are excluded from the
  latency histograms and from the throughput window. S4 passes `max(2000, 10 % of run events)`.
* `app_*.json` → `performance` (also mirrored in `hist_*.json`):
  `committed_events_total`, `warmup_events_configured`, `warmup_events_excluded`,
  `steady_state_events`, `steady_state_start_ms`, `steady_state_end_ms`,
  `steady_state_duration_ms`, `throughput_eps`,
  **`throughput_numerator_events`**, **`throughput_denominator_ms`**,
  `warmup_boundary_commit_ms`, `latency_events_recorded`.
* `produced_<run_id>.ndjson` lines: `{event_id,partition,offset,ack_ts_ms,scheduled_at_ms,send_ts_ms}`
  — `send_ts_ms` is the real per-event send-initiation epoch ms (producer-side timestamp),
  `ack_ts_ms` is the producer acknowledgement-callback time, `scheduled_at_ms` is the open-loop
  schedule. Written and fsynced after all acks, ordered by `seq`.
* Latency definition unchanged: `commit_observed_ms − scheduled_at_ms` per event; per-batch uses the
  newest event's schedule.

### 8.6 Failpoint coverage matrix (mechanism × crash class C1–C12)

`after_fetch` is armed for every mechanism; the table below lists, per crash class, the hook S4
arms (in-process Level-1) or the external injector. `n/a` = structurally unreachable (§5.3, §5.4);
`ext` = external injector (Toxiproxy / process kill / offset rewind).

| Crash class (point) | M0 | M1-kafka | M1x | M2 | M3 | M4 | M2+M3 | M4+M3 | M1x+M3 |
|---|---|---|---|---|---|---|---|---|---|
| C1 after poll (K1) | after_fetch | after_fetch | after_fetch | after_fetch | after_fetch | after_fetch | after_fetch | after_fetch | after_fetch |
| C2 inside effect txn (K1) | before_effect_commit | before_effect_commit (=atomic commit) | before_effect_commit | before_effect_commit | before_effect_commit | before_effect_commit (same txn as progress) | before_effect_commit | before_effect_commit | before_effect_commit |
| C3 commit ack unknown (K2) | ext F3/F5 or before_effect_commit crash+restart | ext / before_effect_commit crash+restart | ext / same | ext / same | ext / same | ext; resolver re-reads progress | ext / same | ext / same | ext / same |
| C4 effect durable, progress not (K3) | after_effect_commit (throw/abort/crash) | n/a (single commit) | after_effect_commit | after_effect_commit | after_effect_commit | n/a strict (atomic) | after_effect_commit | n/a strict | after_effect_commit |
| C5 progress durable, effect not (K4) | ordering=early + before_effect_commit | n/a | ordering=early + before_effect_commit | ordering=early + before_effect_commit | ordering=early + before_effect_commit | ordering=early probe only | ordering=early + before_effect_commit | ordering=early probe only | ordering=early + before_effect_commit |
| C6 duplicate delivery (K5) | ext F6 rewind | protocol replay of uncommitted txn only; **never** a Kafka offset rewind (D5) | ext F6 rewind | ext F6 rewind | ext F6 rewind | ext F6 (UPDATE consumer_progress, wxeo_fault role) | ext F6 rewind | ext F6 progress rewind | ext F6 rewind |
| C7 zombie writer (K6) | before_effect_commit pause > max.poll.interval.ms | before atomic commit pause > transaction.timeout.ms (fencing) | before_effect_commit pause | before_effect_commit pause | before_effect_commit pause | before_effect_commit pause | before_effect_commit pause | before_effect_commit pause | before_effect_commit pause |
| C8 publish window (K7) | before_publish | before_publish | before_publish (inside txn) | before_publish / before_relay_mark | before_publish | before_publish | before_relay_mark | before_publish | before_publish |
| C9 sink link loss during commit (K2) | ext db toxic | n/a (no DB) | ext db toxic | ext db toxic | ext db toxic | ext db toxic (re-read progress) | ext db toxic | ext db toxic | ext db toxic |
| C10 Kafka-side progress disruption (K3) | ext F1/toxics | ext (fenced/aborted txn) | ext (aborted txn → stream divergence) | ext | ext | ext (Kafka offsets observability only) | ext | ext | ext |
| C11 ack-before-durability | excluded by AS3/AS4 | excluded | excluded | excluded | excluded | excluded | excluded | excluded | excluded |
| C12 partial-batch durability | unreachable (one DB txn/batch) | unreachable | unreachable | unreachable | unreachable | unreachable | unreachable | unreachable | unreachable |

Action menu for every in-process hook: `crash` (exit 99, fired-only receipt), `throw` (rollback +
forced replay), `abort_txn` (abort open Kafka/DB txn + forced replay), `pause[:ms]` (block at the
crash point; used for C7), `none` (sham receipts only, for G4 sham runs). Receipts are fsynced and
the *armed* hook fires at most once, on the N-th hit (`--wxeo.failpoint.after=N`).

Realisation recipes (distinguishable, no external rewind needed for K5/K6):
* **K6 zombie (M0/M1x/plain-M2/M4 families)** — arm `before_effect_commit` with
  `pause:310000` (longer than `max.poll.interval.ms=300000`) on instance A; the group reassigns the
  partition to B, B completes later batches, A wakes and commits its stale transaction → duplicate
  for mechanisms without dedup/atomicity; M3/M2/M4+M3 absorb it in the sink; M1 family is fenced by
  the producer epoch (pause longer than `transaction.timeout.ms=60000` → commit fails).
* **K5 protocol replay** — stop consumers, rewind the *mechanism's own* resume point (§5.5 F6:
  AdminClient `alterConsumerGroupOffsets` for offset-store mechanisms; bounded
  `UPDATE ... consumer_progress` with the `wxeo_fault` role for M4 family), restart. Never rewind
  M1-kafka committed offsets (D5). In-process alternative: arm `after_effect_commit` with `throw`
  (effect durable, progress behind, forced replay) for DB mechanisms; for M1-kafka arm
  `before_effect_commit` (atomic commit point) with `abort_txn` — the transaction aborts and only
  the in-transaction records/offsets vanish, so nothing is replayed.

### 8.7 M1-kafka publication stream = ledger definition for quiescence/oracle

M1-kafka writes exactly one `wxeo.entry.v1` record per consumed journal entry (key = `event_id`,
value = canonical entry JSON, same partition index as the source record) **inside the same Kafka
transaction** that carries the consumer offsets, then commits atomically. A consumer with
`isolation.level=read_committed` therefore observes exactly the committed entry set; completion can
be gauged by (a) the read_committed consumer's lag reaching 0 and (b) its position reaching the last
stable offset (LSO) — records of aborted transactions are never visible. There is no DB sink and no
PG schema for M1-kafka (`SchemaTool` is not needed); the ledger invariants are evaluated on the
stream multiset by `(event_id, leg_no)` exactly as for the DB postings.

### 8.8 `wxeo_fault` role for F6 state injection (write-capable but posting-safe)

`native-stack.ps1 start-pg` creates `wxeo_fault` (LOGIN) next to `wxeo_app`/`wxeo_read`; `SchemaTool
--mode=init` grants it `SELECT` on every table plus `UPDATE` on `outbox` (M2/M2+M3) and
`consumer_progress` (M4/M4+M3) **only**. It can never mutate `posting` (no grant, trigger blocks it
anyway), so append-only attestation stays non-vacuous. F6 rewind for M4 family (consumers stopped,
bounded k, recorded per partition):

```sql
-- connect as wxeo_fault; schema = wxeo_run_<run_id>
UPDATE wxeo_run_<run_id>.consumer_progress
   SET next_offset = GREATEST(0, next_offset - :k)
 WHERE partition = :p;
-- record before/after rows: SELECT partition, next_offset FROM ...consumer_progress ORDER BY partition;
```

K7 relay resend (explicit, labelled exploratory) uses
`UPDATE ...outbox SET published_at = NULL WHERE event_id = :eid` with the same role. Kafka offset
rewinds are done by S4 with the AdminClient; M1-kafka must never be rewound (D5).

### 8.9 Append-only privilege attestation is non-vacuous and provable

`SchemaDdl.init` emits `REVOKE ALL ON SCHEMA ... FROM PUBLIC`, `REVOKE UPDATE, DELETE ON posting FROM
PUBLIC`, `REVOKE ALL ON posting FROM PUBLIC`, then grants the application role **only** `INSERT,
SELECT` on `posting` (+ sequence usage) and creates the `posting_append_only` `BEFORE UPDATE OR
DELETE` trigger. Proof commands (oracle-side, read-only):

```sql
SELECT has_table_privilege('wxeo_app', 'wxeo_run_<run_id>.posting', 'UPDATE'),  -- f
       has_table_privilege('wxeo_app', 'wxeo_run_<run_id>.posting', 'DELETE');  -- f
SELECT count(*) FROM pg_trigger
 WHERE tgrelid = 'wxeo_run_<run_id>.posting'::regclass AND NOT tgisinternal;     -- 1
```

`SchemaTool --mode=verify --db-user=wxeo_app` returns the same facts as JSON
(`checks.can_update=false`, `checks.can_delete=false`, `checks.append_only_triggers>=1`,
`checks.dedup_index_present` per mechanism); the service records its own role's attestation in
`app_*.json.attestation` and logs it at startup. An UPDATE attempt fails with
`posting is append-only`.

### 8.10 Fresh-state helpers (topics + schema) used by the acceptance scripts

```powershell
# topics (AdminClient helper in the shipped jar; Kafka 3.x tool class names are not stable)
java -cp artifact/target/wxeo-service.jar com.wxeo.tools.TopicTool --action=create `
  --bootstrap=127.0.0.1:9091 --topic=wxeo.src.<run_id> --partitions 3 --replication-factor 3 --min-insync 2
java -cp artifact/target/wxeo-service.jar com.wxeo.tools.TopicTool --action=delete --bootstrap=127.0.0.1:9091 --topic=...
# per-run schema (mechanism-specific aux tables/dedup index; identical posting DDL)
java -cp artifact/target/wxeo-service.jar com.wxeo.sink.SchemaTool --mode=init `
  --db-url=jdbc:postgresql://127.0.0.1:5433/wxeo --db-user=postgres `
  --schema=wxeo_run_<run_id> --mechanism=<M0|M1x|M2|M3|M4|M2+M3|M4+M3|M1x+M3> `
  --app-role=wxeo_app --read-role=wxeo_read --fault-role=wxeo_fault
```
### 8.11 Stack ownership and schema-name saniting (live-path findings from S4)

* **One owner at a time.** The harness verbs are not a concurrent service: `start-kafka` is now
  idempotent (if all three broker ports answer, it adopts the running cluster and refuses to start a
  second set) and refuses partially-running clusters; `reset-kafka` refuses to wipe while broker
  ports are still listening. The runner must not start/stop the stack while another party (e.g. a
  manual acceptance run) owns it; read `$EOS_RUNTIME_ROOT\run\stack-state.json` / run `status`
  before acting.
* **PostgreSQL schema names.** `SchemaDdl` accepts `[A-Za-z0-9_]{1,63}` only (safe SQL identifiers),
  so hyphenated `run_id`s cannot appear literally in the schema. S4's runner sanitises
  `wxeo_run_<run_id>` by replacing `-` with `_`; the service and `SchemaTool` receive that exact
  string via `--wxeo.db-schema`, so the contract is: **the runner owns the final schema string**;
  the artifact validates it and performs no rewriting. (Recorded as DEVIATIONS S5-9 in S4's log.)
* Confirmed field names for the runner: `--wxeo.warmup-events=N` (exact) and
  `send_ts_ms` in every `produced_<run_id>.ndjson` line (exact).
### 8.12 Topic lifecycle on this host: never delete topics

Kafka topic deletion is **fatal on this Windows KRaft cluster**: the broker's rename-based removal of
`<topic>-<partition>` directories fails (`Error while renaming dir ...` ->
`Utils.atomicMoveWithFallback` -> `Shutdown broker because all log dirs ... have failed`), which then
breaks every subsequent E2E step. Therefore:

* `artifact/harness/stack.json` pins `delete.topic.enable=false` and
  `log.retention.check.interval.ms=3600000` (safety net + fewer background deletes).
* `artifact/harness/acceptance-suite.ps1` and `smoke-e2e.ps1` **never call AdminClient deleteTopics**;
  the Kafka smoke step validates create/produce/consume only and records
  `delete_skipped = "SKIPPED: topic deletion is fatal on Windows KRaft ..."`.
* The experiment runner must not delete topics per run either (S4 is changing to unique topics per
  run + periodic `reset-kafka`); `reset-kafka` (wipe + reformat) is the only topic-cleanup path.
* `com.wxeo.tools.TopicTool` still offers `--action=delete` for an operator on a healthy cluster, but
  no script uses it on this stack.

The acceptance suite now also reports pass/fail honestly: per-test PASS/FAIL lines, a final
`ALL ACCEPTANCE TESTS PASSED` or `ACCEPTANCE TESTS FAILED: <names>`, `ok = all(tests.passed)`, and
the smoke scripts' `error` strings are surfaced in the suite log via
`WXEO_SMOKE_RESULT_PATH=<path>` (the suite re-reads the smoke-result JSON instead of scraping stdout).