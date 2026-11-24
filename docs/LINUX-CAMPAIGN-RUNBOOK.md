# Linux/container campaign runbook (approved P2 substrate)

Author decision (2026-11-24): the confirmatory campaign runs on a **Linux/container host** with the
full frozen design; all P2 items (matrix amendment M1, Amendment A2 with AC-1…AC-8 as recommended,
K6 non-realisability, M7 calibration block) are **approved**. Stage A/B/C are therefore unblocked on
that host only; the Windows host is retired for confirmatory runs (its evidence stays as
`docs/prereg-amendment-A3.md`).

This runbook is the complete operational contract. The frozen runner owns run order, seeds, reset
cadence, records and RUNLOG entries; nothing below may be shortcut inside a run.

---

## 1. Host requirements

| Item | Minimum | Check |
|---|---|---|
| OS | Ubuntu 22.04/24.04 (or equivalent) x86_64 | `lsb_release -a` |
| CPU / RAM | 8 vCPU / 16 GB (16 vCPU / 32 GB recommended for B/C) | `nproc; free -g` |
| Disk | ≥ 150 GB free (`/` or a mounted data volume) | `df -h` |
| Docker | engine ≥ 24 + compose v2 plugin, user in `docker` group | `docker compose version` |
| JDK | 21 (headless) | `java -version` |
| Maven | 3.9+ | `mvn -v` |
| Python | 3.12 + venv | `python3.12 --version` |
| PostgreSQL client | `psql` 16 | `psql --version` |
| LaTeX (paper only) | TeX Live with pdflatex + bibtex | `pdflatex --version` |

No Docker Desktop, no WSL: the container stack is plain `docker compose`.

## 2. One-time bootstrap

```bash
git clone https://github.com/jcorreiabrito/Where-Exactly-Once-Ends.git
cd Where-Exactly-Once-Ends
bash deploy/linux/bootstrap.sh     # toolchain, venv, jar build, stack up, checks, pre-freeze
```

`bootstrap.sh` installs the pinned Python deps (`deploy/linux/requirements.txt`), builds
`artifact/target/wxeo-service.jar`, starts `deploy/docker/docker-compose.yml`, waits for the runner's
liveness view (3 brokers + PostgreSQL + ≥2 Toxiproxy proxies), runs `--check-env`, Maven tests,
harness tests and issues the single pre-campaign freeze. The S3 PowerShell acceptance suite is
Windows-only; on Linux its role is covered by (i) Maven tests, (ii) the harness test suite and
(iii) the live end-to-end smoke in phase T0 below. This substitution is recorded as a documented
platform deviation (A4).

## 3. Stack (what is running and why the ports match the frozen code)

`deploy/docker/docker-compose.yml`, project `wxeo-campaign`:

| Service | Host ports | Notes |
|---|---|---|
| `kafka-1..3` | 9091/9092/9093 | `apache/kafka:3.7.2`, KRaft static quorum, RF=3, `min.insync=2`, transaction-state RF=3, `auto.create.topics.enable=false`, `delete.topic.enable=false`, index caps 1 MiB (A3/M9 parity), retention 2 h |
| `postgres` | 5433 | `postgres:16`, database `wxeo`, roles `wxeo_app`/`wxeo_read`/`wxeo_fault`, trust auth bound to loopback only |
| `toxiproxy` | 8474 + 19092/19093/19094/15433 | `ghcr.io/shopify/toxiproxy:2.9.0` with the four proxies the fault endpoints use (`deploy/docker/toxiproxy.json`) |

The stack is **only** reached on `127.0.0.1` (the compose port mappings); no external exposure is
intended. A reset is `docker compose down -v && up -d --wait`, performed by the runner at the frozen
M5 cadence (this is why the Windows no-topic-deletion constraint does not apply here, while the
no-deletion broker safety property stays set for record parity).

Useful commands:

```bash
export WXEO_STACK_MODE=docker
python -m experiments.stack status | up | down | reset
docker compose -f deploy/docker/docker-compose.yml -p wxeo-campaign logs --tail 100 kafka-1
```

## 4. Campaign phases (in order; each is a resumable runner invocation)

Always start a phase inside `tmux`/`screen` or with `nohup` so an SSH drop cannot kill it, and keep the
phase log:

```bash
export WXEO_STACK_MODE=docker
export EOS_RUNTIME_ROOT="$HOME/eos-runtime"
source .venv/bin/activate
mkdir -p results/phase-logs
tmux new -s wxeo
```

| # | Command | What it produces | Gate to the next phase |
|---|---|---|---|
| T0 | `bash deploy/linux/run-campaign.sh smoke` | fault-free 1k run + one app-owned and one S4-owned fault run + `results/harness/live-smoke-evidence.json passed=true` | evidence green; artifact jar matches freeze |
| T1 | `bash deploy/linux/run-campaign.sh calibrate` | the approved **M7 block (54 runs: 9 mechanisms × {10k,100k} × P6 × N=3)** and `experiments/rates.json` derived by `experiments/rates.py` (50 % of the minimum measured mechanism capacity; deterministic rule) | calibration records valid; rates.json written |
| T2 | `python experiments/freeze.py --write --force` then `--check` | re-issued freeze binding rates.json + the M7/R10/R12 code; records `git_sha` | freeze aggregate recorded in `docs/prereg-freeze.json`; **S5 re-confirms it** before Stage A |
| T3 | Gate-control runs (G1/G4 per class: M0 K3/K5 attested, sham, self-tests; the reserved validation slate) | G1–G8 positive evidence | S5's Stage-A conditions closed |
| T4 | `bash deploy/linux/run-campaign.sh pilot` | 216 planned / 198 executable + 26 validation pilot rows; projection worksheet via `tools/projection.py` | pilot complete; P2 projection updated with measured 10k/100k times |
| T5 | `bash deploy/linux/run-campaign.sh A` then `B` then `C` | confirmatory matrix 7,488 executable runs (Stage A 1,530; B 3,960; C 1,800) | every row has a schema-valid record; exclusion rate within AC-8 |
| T6 | `bash deploy/linux/run-campaign.sh analysis` | A2 analyzer results (AC-3b pooled stratified sign test, H5-A with vacuity rejection), `tools/analyze.py` tables, `tools/make-figures.py` vector figures, `macros.tex` filled from results | analysis outputs traceable to records; RUNLOG rows complete |
| T7 | Paper: `bash build.ps1` is Windows-only — on Linux use `pdflatex → bibtex → pdflatex ×2` (or `latexmk` if installed), then the anonymized review PDF | final PDF ≤25 pp + LaTeX source + cover letter + checklist | page/compliance checks; anonymity scan |

Notes:

* The runner is resumable: completed run records are skipped; invalid rows are retried up to
  `R_max` with the same seeds. If a phase is interrupted, re-run the same command.
* Per-run reset cadence is automatic (1k→50, 10k→25, 100k→20, 1M→1 runs; topic cap 400). Resets are
  cheap on docker and are recorded in every run's `config.runner` block and `results/fault_restart.log`.
* Timing: the Windows host measured ~44 s per 1k run because of native-process overhead. On the
  container host the same protocol should be dominated by service startup/attach (~5–8 s), the 10 s
  quiescence window, dumps and oracle verification; the T4 pilot produces the authoritative
  projection (the frozen formula, p90 basis, +15 % contingency).
* Never run two phases concurrently: one runner process owns the stack.

## 5. Environment variables that are part of the record

| Variable | Purpose |
|---|---|
| `WXEO_STACK_MODE=docker` | selects the container stack adapter (`experiments/stack.py`) |
| `WXEO_COMPOSE_FILE` | override compose file (default `deploy/docker/docker-compose.yml`) |
| `EOS_RUNTIME_ROOT` | scratch root for GT/produced/dumps/logs (default `$HOME/eos-runtime`) |
| `WXEO_DISABLE_PERIODIC_RESET=1` | pilot-only convenience; **must not** be set for Stage A/B/C |
| `WXEO_PYTHON` | python binary used by the runner wrapper scripts |

Every run record carries `host`, `versions`, `process_limits`, `config.runner.stack_mode`,
`config.runner.restart_before_run/block_id/reset_epoch` and the freeze `git_sha`; the RUNLOG records
each external command with its exit status.

## 6. Disk accounting

Per-run growth is bounded by one source + one sink topic pair (3 partitions, RF=3) with 1 MiB index
caps — roughly 8–15 MB per 1k run before a reset, plus GT/produced/dumps for retained runs. The
projection from the pilot (T4) plus the free-space floor of 3 GB (checked before every run) decide
any additional reset cadence. Raw artifacts are local-only (`results/raw/artifacts/` is git-ignored);
only scrubbed run records, oracle JSONs and digest manifests are committed.

## 7. Failure handling

| Symptom | Action |
|---|---|
| liveness preflight fails | runner resets once; if still down the run is recorded invalid and the campaign aborts loudly. Check `docker compose ps`, `logs`, disk, RAM |
| `frozen pipeline mismatch` | someone edited a frozen file: `git status`, restore, or if the edit is intended, follow a formal amendment + `freeze --write --force` and get S5 re-confirmation BEFORE any further run |
| repeated invalid rows in one cell | stop the phase, inspect `results/raw/summaries/attempts.csv` and the attempt logs; treat as a pipeline defect (not as data); fix under a new amendment + re-freeze, archive the invalid rows, re-run |
| host reboot / container loss | `bash deploy/linux/bootstrap.sh` is idempotent (stack up + checks); re-run the interrupted phase; records already written are kept |
| disk pressure | the 1M block is the first cut (frozen cut ladder step 1); record it in `results/DEVIATIONS.md` |

## 8. Hand-back checklist (what the lead needs from the host)

1. `docs/prereg-freeze.json` final aggregate + `results/phase-logs/*`.
2. `results/raw/run-records/**`, `results/raw/oracle/**`, `results/raw/summaries/**`,
   `results/harness/live-smoke-evidence.json` (all scrubbed; no absolute paths).
3. `experiments/rates.json` + the M7 calibration records that justify it.
4. A2 analyzer outputs (`results/analysis/**`) and figure files under `figures/`.
5. The final PDF + LaTeX source + cover letter + checklist + anonymized review PDF.
6. `git log` + a green GitHub Actions run for the tag `v1.0-submission`.

Anything that deviates from this runbook is recorded as a numbered deviation in
`results/DEVIATIONS.md` with its effect on validity; numbers are reported only through the frozen
analysis pipeline.

---

## 9. REVISION R2 -- fault-routing redesign (S3, after red-team rereview §11)

This revision supersedes the port/proxy table in section 3 and the phase list in section 4 where
they differ. It is the substrate contract S4's injectors consume and the operator must verify.

### 9.1 Topology (container host)

| Component | Container name | Host (loopback) | Container listener | Advertised / upstream |
|---|---|---|---|---|
| kafka-1 | `wxeo-campaign-kafka-1` | 9091 (direct compat), 19091 (proxy) | EXTERNAL 29091, INTERNAL 29095, CONTROLLER 9192 | EXTERNAL `127.0.0.1:19091`, INTERNAL `kafka-1:29095` |
| kafka-2 | `wxeo-campaign-kafka-2` | 9092 (direct compat), 19092 (proxy) | EXTERNAL 29092, INTERNAL 29095, CONTROLLER 9193 | EXTERNAL `127.0.0.1:19092`, INTERNAL `kafka-2:29095` |
| kafka-3 | `wxeo-campaign-kafka-3` | 9093 (direct compat), 19093 (proxy) | EXTERNAL 29093, INTERNAL 29095, CONTROLLER 9194 | EXTERNAL `127.0.0.1:19093`, INTERNAL `kafka-3:29095` |
| postgres | `wxeo-campaign-postgres` | 5433 | 5432 | -- |
| toxiproxy | `wxeo-campaign-toxiproxy` | 8474 API | 8474 | proxies below |

Toxiproxy proxies (`deploy/docker/toxiproxy.json`, JSON array):

| Proxy | Listen | Upstream | Purpose |
|---|---|---|---|
| `kafka-1` | 19091 | `kafka-1:29091` | broker-1 client path |
| `kafka-2` | 19092 | `kafka-2:29092` | broker-2 client path |
| `kafka-3` | 19093 | `kafka-3:29093` | broker-3 client path; also the frozen runner's producer-bootstrap endpoint 19093 |
| `kafka-bootstrap` | 19094 | `kafka-1:29091` | bootstrap-only alias so the frozen runner's consumer-bootstrap endpoint 19094 stays usable; never in a toxic group |
| `db` | 15433 | `postgres:5432` | F5 / DB sink path |

**Why every client connection traverses Toxiproxy:** Kafka advertises the three proxy ports
(`127.0.0.1:19091..19093`), so after the initial metadata exchange a host client (service, runner,
loader, oracle) connects to brokers only through Toxiproxy, regardless of which endpoint it used to
bootstrap (`9091..9093` direct, `19091..19094` proxy, or the frozen split bootstraps). The direct
`127.0.0.1:9091..9093` published ports exist only for bootstrap/admin calls (broker-count probes,
`TopicTool`); they cannot carry partition/coordinator traffic after metadata.

The INTERNAL listener moved from 29092 to **29095**: EXTERNAL 29092 is broker-2's client listener,
and an EXTERNAL 29092 would collide with the old INTERNAL 29092 inside broker-2's network namespace.
Broker-to-broker semantics, controller quorum (9192/9193/9194) and replication settings are
unchanged; record this as a substrate realisation note (no measurement impact).

Every published port is bound to `127.0.0.1` (compose `"127.0.0.1:<host>:<container>"`), which
satisfies the A4/runbook "loopback only" claim (red-team L6). No host firewall rule is required for
isolation; do not change the bindings to `0.0.0.0`.

Container names are pinned in the compose file and are the F1/F2 injector targets; verify with:
`docker compose -f deploy/docker/docker-compose.yml -p wxeo-campaign ps --format 'table {{.Name}}\t{{.State}}\t{{.Ports}}'`.
Kafka/PG/Toxiproxy have healthchecks; `--wait` returns only when they are healthy.

### 9.2 Fault realisations (S4 injectors consume `experiments/stack.py`)

Logical fault groups live in `deploy/docker/proxy-groups.json` and are read via
`stack.proxy_groups()`; `stack.proxy_host_port(name)` returns the loopback host port. All Kafka role
groups map to the same three per-broker proxies (a Kafka client multiplexes produce and fetch on
broker connections; role names are the frozen injector vocabulary, not separate physical links).

| Fault | Realisation | API / command |
|---|---|---|
| F1 broker kill | `docker kill wxeo-campaign-kafka-N`, restore with `docker start` | `stack.broker_kill(instance)`, `stack.broker_start(instance)`, `stack.broker_restart(instance)` -- each returns `{action, cmd, rc, ok, stdout_tail, stderr_tail, container, instance}` for the injector receipt |
| F2 consumer kill | SIGKILL the consumer JVM PID on Linux | `stack.pid_kill_linux(pid) -> bool` (False if already dead; raises on permission error/non-POSIX) |
| F3 produce timeout / unknown outcome | `timeout` toxic on the `kafka-produce` group (all three proxies); remove afterwards | REST `POST /proxies/{p}/toxics {"name","type":"timeout","attributes":{"timeout":500},"stream":"downstream"}` for each p in `proxy_groups()["kafka-produce"]`; DELETE by toxic name |
| F4a offset-commit failure | same toxics on the `kafka-consume` group (offset coordinator connections follow the advertised proxies) | same REST API over `proxy_groups()["kafka-consume"]` |
| F4b ordering probe | app-owned failpoint (`--wxeo.ordering=early` + `before_effect_commit`); no network toxic | artifact failpoint |
| F5 DB partition | disable the `db` proxy (`POST /proxies/db {"enabled": false}`) or `timeout`/`reset_peer` toxic; restore `{"enabled": true}` | REST via 8474; DB URL stays `jdbc:postgresql://127.0.0.1:15433/wxeo` |
| F6 progress rewind | Kafka: AdminClient `alterConsumerGroupOffsets`; M4-family: bounded `UPDATE consumer_progress` via the `wxeo_fault` role | runner/injector SQL (no stack helper needed) |
| K2 unknown outcome | F3-style toxics on `kafka` group for M1-kafka; DB-link toxics for DB sinks | as above |

`delete.topic.enable=false` stays set as a safety net; the campaign never deletes topics (unique
topic per run; `docker compose down -v` reclaims the volumes). Deleting consumer groups is safe and
may be used for residue clean-up.

### 9.3 Fault-efficacy gate (fail closed, before any campaign run)

`deploy/linux/fault-efficacy.py` proves the paths, not just the presence of toxics: for each group it
round-trips a produce/consume through the proxies, applies the same `timeout` toxic the injector uses
to **all** proxies of the group, asserts the operation actually fails within 20 s, removes the toxics
and asserts recovery within 60 s; it also proves F1 (broker count 3 -> 2 -> 3 with a produce during
the outage and a post-restore round trip) and F2 (spawned consumer PID killed; group member
disappears). The `db` check toggles the proxy and verifies TCP reachability.

```bash
export WXEO_STACK_MODE=docker
bash deploy/linux/run-campaign.sh fault-efficacy
# or directly:
python deploy/linux/fault-efficacy.py --out results/harness/linux-fault-efficacy.json
```

Exit 0 == `passed: true`; any failed check exits 1 and must block all campaign phases. The JSON
path `results/harness/linux-fault-efficacy.json` is **required evidence** at the final freeze
(`freeze.py --require-evidence` refuses without it).

### 9.4 Phase list amendments (supersedes section 4)

| # | Command | Additional gates in Revision R2 |
|---|---|---|
| T0 | `run-campaign.sh smoke` | now runs fault-free rows for M2, M3 (dedup index), M1-kafka (stream ledger), plus app-owned F4b and S4-owned F2; `live_smoke_evidence.py` unchanged |
| T0b | `run-campaign.sh fault-efficacy` | the 9.3 proof; run after every stack restart change (image/version change) |
| T1 | `run-campaign.sh calibrate` | Amendment **A5**: `--stage M7cal` now executes **117 runs** (M7-cal 54 + M7spot 63) that derive and validate the fixed-rate mapping rule; T1 **fails closed** if `experiments/rates.json.validated is not true` (spot-check failed). `rates.json` records the rule id |
| T2 | `run-campaign.sh freeze` | single **final** freeze: `python experiments/freeze.py --write --force --require-evidence`, issued **after T0 smoke + T1 (rates.json validated)** and **before T3/Stage A**; bootstrap's freeze is provisional (`--write --force` without `--require-evidence`); S5 re-confirms |
| T3 | `run-campaign.sh gate-controls` | runs the 26 reserved validation rows and writes `results/harness/linux-gate-controls.json`; fails closed unless: M0 K3/K5 negative control shows a violation (G1), M3 dedup index is attested on the live stack, M1-kafka F0 ledger is clean (G2), and all validation classes are present |
| T4 | `run-campaign.sh pilot` | unchanged; requires T2 freeze confirmed |
| T5 | `A` then `B` then `C` | unchanged |
| T6 | `run-campaign.sh analysis` | `python tools/analyze.py` (single frozen CLI; no `--stage` argument) then `tools/make-figures.py` |

Freeze procedure (Revision R2): (1) `bootstrap.sh` writes a **provisional** freeze with
`freeze.py --write --force` (no evidence requirement); (2) run T0 smoke and T1 calibration
(`--stage M7cal` = 117 runs, then `rates.py --write`, validated=true); (3) issue the single FINAL
freeze with `python experiments/freeze.py --write --force --require-evidence` -- freeze.py refuses
unless `experiments/rates.json` (validated), `results/harness/live-smoke-evidence.json` and
`results/harness/linux-fault-efficacy.json` are present; it binds those as required evidence plus the
container image digests and the T0/T3 evidence; (4) S5 re-confirms the reported freeze before T3/
Stage A. No campaign phase may run between T1 and the final freeze.

Windows-results archive (red-team L5, **precondition**): the lead has archived the Windows evidence
under `results/windows-pilot/` (run-records, oracle, summaries, evidence-only, artifacts, fault logs,
live-smoke-evidence.json) with digests, and `results/raw/` is empty except its `README`/`.gitkeep`.
Do not restore Windows run records there: `results/raw/**` is regenerated on the Linux host, and a
populated `results/raw/run-records/` would let the runner's resume logic skip same-named Linux rows.
`bootstrap.sh` still performs the archive defensively when it finds rows and aborts if it cannot.

### 9.5 Acceptance-suite substitution (Windows S3 suite -> Linux host), item by item

| Windows acceptance item | Linux substitute | Command |
|---|---|---|
| 1. `mvn -B -ntp test` (unit tests, no Docker) | identical (host is Linux, no services needed) | `mvn -f artifact/pom.xml test` (bootstrap) |
| 2. `mvn -B -ntp package` (runnable jar) | identical + recorded `sha256sum` and jar hash bound to the freeze | `mvn -f artifact/pom.xml package`; `sha256sum artifact/target/wxeo-service.jar` |
| 3. native-stack Kafka smoke (start-kafka, Java-client produce/consume) | container stack live probe + artifact integration ITs: `KafkaSmokeIT` (create/produce/consume against the stack; its topic-delete cleanup is expected to be refused because `delete.topic.enable=false` and is caught by the IT) and `PostgresSchemaIT` (DDL/append-only trigger/dedup index/idempotent insert against PG 5433) | `mvn -f artifact/pom.xml verify -Pintegration -Dwxeo.it.kafka.bootstrap=127.0.0.1:29091 -Dwxeo.it.db.url=jdbc:postgresql://127.0.0.1:5433/postgres` (bootstrap) |
| 4. fault-free 1,000-event E2E for M0 and M3 (postings in PG, GT+manifest+app artefacts) | T0 smoke rows `pilot-m0-f4b-1k-p3-r00` (fault-driven but end-to-end) + `pilot-m2-f0-1k-p3-r00`, `pilot-m3-f0-1k-p3-r00`, `pilot-m1-kafka-f0-1k-p3-r00`; oracle + run-record + artefact digests are validated by the runner, and the M3 dedup index is asserted in T3 | `run-campaign.sh smoke` then `run-campaign.sh gate-controls` |
| 5. failpoint crash smoke (`before_effect_commit`+crash: exit 99, one fired receipt) | covered by the T0 app-owned F4b row (app-owned failpoint receipts attested by `live_smoke_evidence.py`), the T3 `V.selftest` rows and the G4 attestation in `linux-gate-controls.json`; the crash subprocess path itself remains covered by `artifact/pom.xml` unit test `FailpointCrashTest` | `run-campaign.sh smoke` + `run-campaign.sh gate-controls` |

Anything not covered by these substitutes (e.g. the exact PowerShell harness verbs) is Windows-only
and is recorded as a platform deviation in A4/DEVIATIONS; the Linux acceptance evidence is the
bootstrap log, `results/harness/linux-fault-efficacy.json`, the T0/T3 evidence above and the
artifact IT/unit logs.

### 9.6 Operator notes

1. Docker group membership must be effective in the current shell: `docker info` must succeed;
   `bootstrap.sh` now hard-fails with instructions instead of continuing (red-team L7).
2. Liveness/health: `python -m experiments.stack status`; `docker compose ... ps --format` shows the
   pinned container names. Toxiproxy has a compose healthcheck (`toxiproxy-cli list`).
3. Toxiproxy REST: use `curl`/urllib with an explicit User-Agent on any host whose PowerShell clients
   are used (the Windows 403 UA quirk does not apply on Linux; still send
   `User-Agent: wxeo-runner/1.0` for consistency). List proxies: `curl -s 127.0.0.1:8474/proxies`.
4. Never delete topics: `delete.topic.enable=false`; use `docker compose down -v` (runner reset
   cadence) to reclaim storage. Deleting consumer groups is safe.
5. If `fault-efficacy` fails: do not start the campaign; capture the JSON, check
   `docker compose ps`/logs, and treat it as a substrate defect (requiring an amendment + re-freeze
   after the fix). The most common causes are a missing proxy (config shape) or a stale stack.
6. `M1-kafka` ledger definition for quiescence/oracle is unchanged: one `wxeo.entry.v1` record per
   entry, key `event_id`, same partition, inside the offsets transaction, read with
   `read_committed`; completion is LSO/lag-based (the R20 limitation note still applies).
7. Record `docker image inspect` repo digests for kafka/postgres/toxiproxy in the bootstrap log; the
   final freeze binds them (container-image equivalent of the A4 image-digest pinning).