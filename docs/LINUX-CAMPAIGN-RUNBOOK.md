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
