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
| `toxiproxy` | 8474 + 19091/19092/19093/15433 | `ghcr.io/shopify/toxiproxy:2.9.0` with the four proxies the fault endpoints use (`deploy/docker/toxiproxy.json`) |

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
| T1a | `bash deploy/linux/run-campaign.sh calibrate` | M7/M7spot **108 runs** (`--stage M7cal`, M7-cal 54 + M7spot 54); `rates.py --derive` writes the CANDIDATE fixed-rate table; fails closed unless the A5 block coverage/spot checks pass (the table must be `validated=false` here) |
| T1b | `bash deploy/linux/run-campaign.sh saturation-validate` | **A6**: `rates.py --derive` candidate write, then `runner.py --stage M7sat` (9 mechanisms x 100k/P6 saturation at the derived 2.5x-max rate), then `rates.py --write`; fails closed unless `validated=true` **and** `saturation_validation.validated=true` |
| T3 | Gate-control runs (G1/G4 per class: M0 K3/K5 attested, sham, self-tests; the reserved validation slate) | G1â€“G8 positive evidence | S5's Stage-A conditions closed |
| T2 | `python experiments/freeze.py --write --force --require-evidence` then `--check` | **FINAL freeze, after T3** (see §9.4; bootstrap freeze is provisional). Bind rates.json (validated), live-smoke-evidence, fault-efficacy, gate-controls and (B5) container-images.json |
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
| `kafka-3` | 19093 | `kafka-3:29093` | broker-3 client path |
| `db` | 15433 | `postgres:5432` | F5 / DB sink path |

**Why every client connection traverses Toxiproxy:** Kafka advertises the three proxy ports
(`127.0.0.1:19091..19093`), so after the initial metadata exchange a host client (service, runner,
loader, oracle) connects to brokers only through Toxiproxy, regardless of which endpoint it used to
bootstrap (`9091..9093` direct, `19091..19093` proxy, or the runner's `fault_bootstrap`). The direct
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

**Endpoint contract (S5 B7):** `deploy/docker/endpoints.json` (`wxeo.endpoints.v1`) is the single
shared source of truth for the runner, the injectors and the fault-efficacy proof. `fault_bootstrap`
is `127.0.0.1:19091` and MUST be a member of every Kafka proxy group (so the bootstrap connection
itself is inside the injected toxics); `group_member_proxies` are the three proxy ports 19091-19093;
The former bootstrap-only alias on 19094 is **removed** (A6/B7 follow-up) now that the runner uses
`kafka.fault_bootstrap` = `127.0.0.1:19091` (a group member) for every Kafka path.
`fault-efficacy.py --check-endpoints` validates the contract against the deployed proxy configs
without Docker; the full run asserts the same before any toxic is applied.

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
(`freeze.py --require-evidence` refuses without it). Likewise `results/harness/container-images.json`
(B5; three pinned images with `repo_digests` + `image_id`) is required evidence at the final freeze;
it is produced by `deploy/linux/record_container_images.py` from `bootstrap.sh` after `docker compose
pull`, and `bootstrap.sh` fails closed if any repo digest is missing.

### 9.4 Phase list amendments (supersedes section 4)

| # | Command | Additional gates in Revision R2 |
|---|---|---|
| T0 | `run-campaign.sh smoke` | now runs fault-free rows for M2, M3 (dedup index), M1-kafka (stream ledger), plus app-owned F4b and S4-owned F2; `live_smoke_evidence.py` unchanged ; includes the clean fault-free M0 row `pilot-m0-f0-1k-p3-r00` (B8) |
| T0b | `run-campaign.sh fault-efficacy` | the 9.3 proof; run after every stack restart change (image/version change) |
| T1a | `run-campaign.sh calibrate` | Amendment **A5**: `--stage M7cal` executes **108 runs** (M7-cal 54 + M7spot 54); `rates.py --derive` writes the CANDIDATE table (A5 validated, A6 pending) and fails closed unless the A5 spot checks pass |
| T1b | `run-campaign.sh saturation-validate` | Amendment **A6**: `rates.py --derive` + `runner.py --stage M7sat` (9 runs, 100k/P6 saturation at the derived 2.5x-max rate) + `rates.py --write`; fails closed unless `validated=true` **and** `saturation_validation.validated=true` |
| T3 | `run-campaign.sh gate-controls` | runs the 26 reserved validation rows **under the provisional bootstrap freeze and BEFORE T2** (S5 B3); writes `results/harness/linux-gate-controls.json`; fails closed unless G1 M0 K3/K5 shows the expected violation, M3 dedup index is attested, M1-kafka F0 ledger is clean (G2) and all validation classes are present |
| T2 | `run-campaign.sh freeze` | **FINAL** freeze `python experiments/freeze.py --write --force --require-evidence`; runs **AFTER T0+T0b+T1+T3** and is the **last procedural step before T4**; requires `rates.json` (validated), `results/harness/live-smoke-evidence.json`, `results/harness/linux-fault-efficacy.json`, `results/harness/linux-gate-controls.json` and (B5) `results/harness/container-images.json`; bootstrap freeze is provisional; S5 re-confirms |
| T4 | `run-campaign.sh pilot` | unchanged; requires T2 freeze confirmed |
| T5 | `A` then `B` then `C` | unchanged |
| T6 | `run-campaign.sh analysis` | `python tools/analyze.py` (single frozen CLI; no `--stage` argument) then `tools/make-figures.py` |

Freeze procedure (Revision R3, S5 B3/B5): (1) `bootstrap.sh` writes a **provisional** freeze with
`freeze.py --write --force` (no evidence requirement) and records the container image repo digests at
`results/harness/container-images.json` (schema `wxeo.container-images.v1`; fail closed if any digest
is missing); (2) run **T0 smoke**, then **T0b fault-efficacy**; (3) run **T1** calibration
(`--stage M7cal` = 108 runs, then `rates.py --derive`, candidate table); (3b) run **T1b**
`saturation-validate` (A6: `rates.py --derive` -> `runner.py --stage M7sat` 9x 100k/P6 at the derived
2.5x-max rate -> `rates.py --write`; must print `validated=true` and
`saturation_validation.validated=true`); (4) run **T3**
`gate-controls` **under the provisional freeze** -- it writes `results/harness/linux-gate-controls.json`;
(5) issue the single **FINAL** freeze with `python experiments/freeze.py --write --force
--require-evidence`; freeze.py refuses unless `experiments/rates.json` (validated),
`results/harness/live-smoke-evidence.json`, `results/harness/linux-fault-efficacy.json` and
`results/harness/linux-gate-controls.json` are present, and the lead's freeze binds
`results/harness/container-images.json` as required evidence; this final freeze MUST be the last
procedural step before T4 pilot; (6) S5 re-confirms the reported freeze before T4/Stage A. No campaign
phase may run between T3 and the final freeze; T2-before-T3 ordering is superseded.

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
| 4. fault-free 1,000-event E2E for M0 and M3 (postings in PG, GT+manifest+app artefacts) | T0 smoke slate now includes the **clean fault-free** row `pilot-m0-f0-1k-p3-r00` plus `pilot-m2-f0-1k-p3-r00`, `pilot-m3-f0-1k-p3-r00`, `pilot-m1-kafka-f0-1k-p3-r00`, and the fault-driven `pilot-m0-f4b-1k-p3-r00`; concrete checks: oracle verdict clean for the F0 rows, 1,000 events / 2,000 postings for M0 and M3, M3 `posting_dedup_idx` present in the run schema (T3 SQL attestation), M1-kafka one entry record per event on the read_committed stream, and run-record + artefact digests validated by the runner | `run-campaign.sh smoke` (T0) -> `run-campaign.sh gate-controls` (T3) |
| 5. failpoint crash smoke (`before_effect_commit`+crash: exit 99, one fired receipt) | covered by the T0 app-owned F4b row (app-owned failpoint receipts attested by `live_smoke_evidence.py`), the T3 `V.selftest` rows and the G4 attestation in `linux-gate-controls.json`; the crash subprocess path itself remains covered by `artifact/pom.xml` unit test `FailpointCrashTest` | `run-campaign.sh smoke` + `run-campaign.sh gate-controls` |

Anything not covered by these substitutes (e.g. the exact PowerShell harness verbs) is Windows-only
and is recorded as a platform deviation in A4/DEVIATIONS; the Linux acceptance evidence is the
bootstrap log, `results/harness/linux-fault-efficacy.json`, the T0/T3 evidence above and the
artifact IT/unit logs.

### 9.5a Exact operator sequence (labels are the runbook phase ids)

```bash
bash deploy/linux/bootstrap.sh                          # tools/jar/stack/images.json/effiacy/provisional freeze
bash deploy/linux/run-campaign.sh smoke                 # T0
bash deploy/linux/run-campaign.sh fault-efficacy        # T0b
bash deploy/linux/run-campaign.sh calibrate             # T1a  (candidate rates)
bash deploy/linux/run-campaign.sh saturation-validate   # T1b  (A6; final validated rates)
bash deploy/linux/run-campaign.sh gate-controls         # T3   (under the provisional freeze)
bash deploy/linux/run-campaign.sh freeze                # T2   FINAL freeze; last step before T4
bash deploy/linux/run-campaign.sh pilot                 # T4
bash deploy/linux/run-campaign.sh A && bash deploy/linux/run-campaign.sh B && bash deploy/linux/run-campaign.sh C
bash deploy/linux/run-campaign.sh analysis              # T6
```

The final `freeze.py --write --force --require-evidence` binds as required evidence: `experiments/rates.json`
(with `validated=true` and `saturation_validation.validated=true`), `results/harness/live-smoke-evidence.json`,
`results/harness/linux-fault-efficacy.json`, `results/harness/linux-gate-controls.json` and
`results/harness/container-images.json` (`wxeo.container-images.v1`, repo digests of the three pinned images).

### 9.6 Operator notes

1. Docker group membership must be effective in the current shell: `docker info` must succeed;
   `bootstrap.sh` now hard-fails with instructions instead of continuing (red-team L7).
2. Liveness/health: `python -m experiments.stack status`; `docker compose ... ps --format` shows the
   pinned container names. Toxiproxy has a compose healthcheck (`toxiproxy-cli list`).
3. Endpoint contract: never hard-code 19094 for a fault path -- read `deploy/docker/endpoints.json` (or `experiments.stack.proxy_host_port`) so bootstrap and traffic are
inside the same proxy group (B7). The deprecated `kafka-bootstrap` alias may be removed from
`toxiproxy.json` once the runner no longer references 19094.
4. Toxiproxy REST: use `curl`/urllib with an explicit User-Agent on any host whose PowerShell clients
   are used (the Windows 403 UA quirk does not apply on Linux; still send
   `User-Agent: wxeo-runner/1.0` for consistency). List proxies: `curl -s 127.0.0.1:8474/proxies`.
5. Never delete topics: `delete.topic.enable=false`; use `docker compose down -v` (runner reset
   cadence) to reclaim storage. Deleting consumer groups is safe.
6. If `fault-efficacy` fails: do not start the campaign; capture the JSON, check
   `docker compose ps`/logs, and treat it as a substrate defect (requiring an amendment + re-freeze
   after the fix). The most common causes are a missing proxy (config shape) or a stale stack.
7. `M1-kafka` ledger definition for quiescence/oracle is unchanged: one `wxeo.entry.v1` record per
   entry, key `event_id`, same partition, inside the offsets transaction, read with
   `read_committed`; completion is LSO/lag-based (the R20 limitation note still applies).
8. `results/harness/container-images.json` (B5) is produced by bootstrap with `docker image inspect` repo digests for kafka/postgres/toxiproxy in the bootstrap log; the
   final freeze binds them (container-image equivalent of the A4 image-digest pinning).