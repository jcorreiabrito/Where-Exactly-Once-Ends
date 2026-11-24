# Project status — 2026-11-24 (end of live bring-up)

Single-author project: João Brito (UFABC). This file is the operational snapshot between the P1
approval and the P2 hard stop. It is not part of the paper.

## 1. What is now true (verified, committed, CI-green)

| Item | Status | Evidence |
|---|---|---|
| Manuscript draft + frozen bibliography (63 entries, DOI-verified) | done (draft, 33 pp; page compression to ≤25 pp still pending) | `main.tex`, `refs/refs.bib`, `refs/verified.csv`, CI job “Verify bibliography integrity” |
| Formal model + TLC | bounded machine-checked witnesses only (8/8 configurations match expectations); unbounded claims are model/pen arguments | `formal/ledger.tla`, `formal/tlc-results/tlc-summary.json`, `formal/README.md` |
| Artifact implementation (Java 21/Spring Boot; 9 mechanisms; failpoints; loader; SchemaTool; 51 unit tests) | accepted; Linux CI Maven tests green | `artifact/**`, `artifact/target/wxeo-service.jar` SHA-256 `6e8223751706d64e7b77752b42df5694d4231ba8bd3522034806bae25e8e50d3` |
| Oracle + runner + matrix (7,506 planned / 7,488 executable + 26 validation) | implemented and frozen; 73 harness tests green | `experiments/**`, `docs/prereg-freeze.json` aggregate `fd271416aebb6f9cb88da680694e4fd43e692b94db8c1e9f78c3dfd8d84e788a` |
| Oracle mutation testing (G3) | 1,800/1,800 defects detected and correctly attributed (18/18 classes), clean + input-integrity controls pass — SYNTHETIC corpus | `results/harness/g3-mutation.json` |
| Gate falsifiability (G1–G8 negative controls) | 8/8 gates fail as required | `results/harness/gate-negative-controls.json` |
| S3 live acceptance | 4/4 passed | `results/harness/acceptance/acceptance-result.json` |
- **Live end-to-end evidence (Windows substrate, archived):** fault-free run valid,
  source_integrity=true, 2,000/2,000 postings, 0 duplicates, 0 losses —
  `results/windows-pilot/run-records/run_pilot-m2-f0-1k-p3-r00.json`; app-owned K3 attested with 44
  predicted duplicate postings (`run_pilot-valid-g1k3-r22.json`); S4-owned K2 attested
  (`results/windows-pilot/evidence-only/run_a-m3-k2-1k-p3-r02.json`); consolidated evidence
  `results/windows-pilot/live-smoke-evidence.json` `passed=true`. The Linux campaign regenerates its
  own live evidence/freeze; Windows outputs are archived (M17) and excluded from analysis.
| Post-P1 platform deviations | disclosed normatively | `docs/prereg-amendment-A3.md`, `results/DEVIATIONS.md` §6 |
| CI | **green** at `09e08c7` (4/4 jobs) | GitHub Actions run #17 |

## 2. Current state (2026-11-24 ~17:05 local)

- **P2: APPROVED by the author** (`docs/P2-APPROVAL.md`): matrix amendment M1, Amendment A2 with the
  recommended AC choices, K6 non-realisability, M7 calibration block — and the campaign moves to a
  **Linux/container host with the full frozen design**. The Windows host is retired for confirmatory
  runs; its pilot rows stay as disclosed calibration data (M14–M16).
- **Linux/container package is turnkey**: `deploy/docker/docker-compose.yml` (3 KRaft brokers, PG 16,
  Toxiproxy 2.9 with the four proxies; same ports as the frozen code), `experiments/stack.py`
  (native/docker adapter; docker reset = `compose down -v && up -d --wait`),
  `deploy/linux/bootstrap.sh`, `deploy/linux/run-campaign.sh`, and the full operational contract
  `docs/LINUX-CAMPAIGN-RUNBOOK.md`. Platform/injector changes are normative in
  `docs/prereg-amendment-A4.md`.
- **S5 Stage-A conditions being closed:** R10 A2 analyzer implemented and tested (AC-3b pooled
  stratified sign test, H5-A + non-vacuity, canonical H2/H4, P3, v1a/v1b+P11, matrix pin, record
  re-validation); R12 `experiments/rates.py` implemented (refuses to write until the M7 block exists);
  M7 calibration blocks (M7-cal 54 + M7spot 54 + M7sat 9 = 117 runs; Amendments A5/A6) added as a separate accounting block (confirmatory counts untouched:
  7,506/7,488 + 26); injector fixes: F1/F3/F5 now marker-driven, F4b threshold scaled to effect-
  transaction geometry. Harness tests: **151/151 green**.
- **Paper:** S6 completed the 33 pp → **24 pp** compression (≤25 pp target met) with 0 undefined
  references/citations and no fabricated data: §7 carries a `Data status` paragraph and 19 `PENDING`
  markers; result insertion on the Linux host is estimated at +1.5–2.5 pp, with reserve cuts recorded
  in `submission/CHECKLIST.md` (K2/K6) to stay ≤25 pp. Submission checklist/VENUE-RULES updated;
  anonymized review PDF pending results insertion.
- The Windows stack is stopped; no experiment process is running.

## 2b. S5 Stage-A blocker closure status (review at `docs/redteam-rereview.md` §11)

| S5 blocker | Owner | Status |
|---|---|---|
| R10 analyzer implemented but not wired into the CLI; M7 records not excluded; no `--stage` | S4 | in progress (wiring + CLI tests) |
| R12/M7 chain: record-stage token mismatch, missing `performance.committed_events`, only 2/8 fixed-rate blocks calibrated, g7 never called, saturation rule wrong | S4 (+ A5 author approval) | in progress; A5 approved: mapping rule + 63 spot-checks, saturation = 2.5×max, fail-closed `validated` |
| F1/F2 injectors Windows-only | S3 (stack helpers) + S4 (injectors) | in progress (docker broker kill/start, POSIX pid kill) |
| Toxiproxy Kafka fault paths structurally off-path (INTERNAL upstream, unresolvable advertised addresses) | S3 | in progress (per-broker proxied advertised listeners + proxy groups + live efficacy test) |
| Provenance/freeze: Windows results resume-skip, rates/evidence not frozen, freeze placement | lead + S4 | Windows outputs archived under `results/windows-pilot/`; `results/raw/` emptied; `freeze.py --require-evidence` added; final freeze moved after T0/T1 |
| Gate evidence T0/T3 incomplete (no K3/K5, dedup/index, M1-kafka ledger, F1/F3/F5 live validation) | S3 (phases) + host | T3 gate-control phase added to the runbook; executes on the Linux host after the final freeze |

Stage A remains blocked until S5 re-reviews the fixed tree and re-confirms the Linux-issued final
freeze. No B/C claim may exist before the corresponding runs.

## 3. Remaining path to submission (all specified in the runbook)

1. On the Linux host: `deploy/linux/bootstrap.sh` → phase T0 smoke evidence → T1 M7 calibration +
   `rates.py --write` → single final re-freeze → S5 re-confirmation of that freeze.
2. Pilot (T4) → measured projection → confirm the cut-ladder/budget against the container-host times.
3. Stage A → B → C (full 7,488 executable runs) with the frozen cadence; records auto-logged.
4. A2 analysis + figures + macros; insert results into the paper; rebuild ≤25 pp PDF; anonymized
   review PDF; submission package; tag `v1.0-submission`; green CI.
5. No B/C result may be claimed before its runs exist; no numbers are estimated or invented.

## 4. Honest limitations recorded so far

- Native Windows processes instead of containers (no Docker/WSL2/admin); single-host memory/disk
  pressure forced smaller JVM heaps and 1 MB Kafka index files (A3, M8/M9).
- Topic deletion is fatal on this KRaft/Windows deployment, so the campaign never deletes topics and
  relies on unique per-run topics plus periodic full-stack resets (M5/M11).
- TLC results are bounded witnesses; the zombie/partial-publication claims are model-level only (K6
  is not experimentally realisable within T_max=300 s).
- Oracle mutation testing is on synthetic fixtures (G3); it validates the checker, not mechanisms.
