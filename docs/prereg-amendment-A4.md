# Pre-registration Amendment A4 — approved Linux/container substrate and post-pilot injector fixes

Status: normative addendum to `docs/prereg.md`, Amendment A2 and Amendment A3. Issued after the
author's P2 decision of 2026-11-24 (`docs/P2-APPROVAL.md`): the confirmatory campaign moves to a
**Linux/container host** and executes the **full approved design** (matrix amendment M1 + A2 +
M7 calibration block). Nothing in A4 changes a hypothesis, cell, N, seed, ordering, outcome
definition or statistic; it fixes the *execution substrate* and two injector-realisability defects
found in the Windows pilot (M16). The Windows pilot rows remain calibration data and are disclosed.

## A4.1 Execution substrate: Linux + Docker Compose (approved)

The campaign runs on a Linux host (Ubuntu 22.04/24.04-class, no Docker Desktop/WSL) with the stack in
`deploy/docker/docker-compose.yml`: three `apache/kafka:3.7.2` KRaft brokers (RF=3, min.insync=2,
transaction-state log RF=3, `auto.create.topics.enable=false`, `delete.topic.enable=false`, 1 MiB
Kafka index caps for disk parity with A3/M9), `postgres:16` (database `wxeo`, roles
`wxeo_app`/`wxeo_read`/`wxeo_fault`, trust auth bound to loopback), and `toxiproxy:2.9.0` with the
four frozen proxies. All services are reached on `127.0.0.1` at the same ports the frozen runner,
artifact and oracle expect (Kafka 9091–9093, PostgreSQL 5433, Toxiproxy 8474/19092/19093/19094/15433).

`experiments/stack.py` selects the substrate (`WXEO_STACK_MODE=docker|native`). In docker mode the
M5 reset is `docker compose down -v && up -d --wait`, which recreates broker log dirs and the
PostgreSQL data volume. This is the documented reason the Windows M5 "never delete topics" constraint
no longer governs storage (the broker safety properties are still set for record parity), and it is
recorded in each run's `config.runner` block (`stack_mode`, reset epoch/block, rc fields).

The Windows native stack and its live evidence remain valid and reproducible (A3), but no
confirmatory Stage A/B/C row may be executed on it.

## A4.2 Tool substitution and acceptance on Linux

The S3 acceptance suite (`artifact/harness/acceptance-suite.ps1`) is Windows/PowerShell-only. On the
Linux host its role is covered by: Maven artifact tests, the frozen harness test suite, the runner
`--check-env` preflight, and the live end-to-end smoke phase (`deploy/linux/run-campaign.sh smoke`,
fault-free + app-owned + S4-owned attested runs, generating `results/harness/live-smoke-evidence.json`
`passed=true`). This is an environment substitution, not a relaxation of any gate: G1–G8 and G3
mutation evidence stay exactly as frozen.

## A4.3 Injector realisability fixes (M16 / S5 Stage-A condition)

1. **Marker-driven rate floor for all S4-owned external classes.** `MARKER_DRIVEN_FAULTS` now covers
   F1, F2, F3, F4a, F5, F6, K2, K5 (previously F1/F3/F5 were missing). When `experiments/rates.json`
   is absent, those classes run at the fixed rate `max(50, load_events/5)` (~5 s load window) so the
   seeded progress-marker injection fires *inside* the processing window. Out-of-window fires remain
   excludable under the frozen `uninformative_fire_outside_window` reason.
2. **App-failpoint threshold geometry (F4b).** The in-process failpoint counter advances once per
   **effect transaction** (one polled batch of `MAX_POLL_RECORDS=500` events per consumer instance),
   not per event. At 1k events with C=2 this yields exactly one effect transaction per instance, so a
   seeded threshold in [50,150] could never fire (the observed 300 s `fault_not_fired` timeout). The
   runner now computes `expected_effect_transactions(load_events, instances)` and uses the
   deterministic first hit (`after=1`) when ≤1 transaction exists, otherwise the seeded mid-stream
   threshold. This is a realisability fix, not a change of the injector semantics; the actual trigger
   value is recorded in every run's `config`/logs, and F4b cells whose mechanisms have no
   `before_effect_commit` hook are re-declared n/a with a reachability proof rather than forced to run.
3. `validate_record.STAGES` admits the approved M7 calibration block (`stage="M7"`), which is a
   separate measurement block (54 runs, `docs/P2-GATE.md` §3) outside the confirmatory 7,506/7,488
   counts.

## A4.4 Consequences for the record

- Every run record now carries `config.runner.stack_mode` and the docker reset bookkeeping; the
  `host`/`versions`/`process_limits` blocks record the Linux host truthfully (A3.1 heaps remain
  512m/1g unless the host gives a reason to restore 2g — the value actually used is what is recorded).
- The Linux pilot (runbook phase T4) produces the authoritative projection; the Windows pilot's
  ~44 s/run is not carried into any paper number.
- Invalid Windows pilot rows caused by the two defects above are archived as pilot data and re-run on
  the Linux host under the frozen seeds; they were never part of a confirmatory cell.
- This amendment changes no analysis artifact. The single final re-freeze (`freeze.py --write
  --force`) after R10/R12/M7 completion binds this amendment, the Linux stack code and the calibration
  block; S5 re-confirms that freeze before Stage A executes.
