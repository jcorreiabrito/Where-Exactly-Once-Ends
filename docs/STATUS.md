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
| **Live end-to-end evidence** | **fault-free run valid, source_integrity=true, 2,000/2,000 postings, 0 duplicates, 0 losses** | `results/raw/run-records/run_pilot-m2-f0-1k-p3-r00.json` |
| **App-owned fault attestation (K3)** | **valid, 1 app fired + 1 restored, M0 reproduces 44 duplicate postings (I3 violation) and 0 losses** | `results/raw/run-records/run_pilot-valid-g1k3-r22.json` |
| **S4-owned fault attestation (K2)** | **valid, one phase-verified S4 fire inside the load window, 2,000/2,000, no duplicates/losses** | `results/raw/run-records/run_a-m3-k2-1k-p3-r02.json` |
| Consolidated live evidence | `passed=true` (all 12 checks) | `results/harness/live-smoke-evidence.json` |
| Post-P1 platform deviations | disclosed normatively | `docs/prereg-amendment-A3.md`, `results/DEVIATIONS.md` §6 |
| CI | **green** at `09e08c7` (4/4 jobs) | GitHub Actions run #17 |

## 2. Current state (2026-11-24 ~16:45 local)

- **Pilot: paused** after 9 post-freeze rows (of 198 executable + 26 validation) because three
  structural injector defects surfaced in the first rows (F3/F1/F5 fire outside the ~1 s 1k/max load
  window; the app-owned F4b `before_effect_commit` failpoint never fires under `ordering=early` for
  M1x+M3). Rows are kept; invalid rows will be re-run after the fixes. Deviation **M16**.
- **Measured projection:** six valid post-freeze runs give median 43.7 s / p90 45.4 s per 1k run, so
  Stage A alone projects to ≈19.3 h (p90). **The pre-registered 7,506-run programme cannot fit the
  approved 8–24 h budget on this host.** The author must choose the reduced design (P2-GATE §3.2).
- **S5 verdict:** PILOT gate PASS-WITH-CONDITIONS (retroactive pilot launch accepted); Stage A/B/C
  remain BLOCKED under the five conditions in `docs/redteam-rereview.md` §10.
- **Live evidence** regenerated against the committed freeze `fd271416…`: `passed=true`, with
  app-owned K3 and S4-owned F2 attestations (`results/harness/live-smoke-evidence.json`).
- The stack is stopped; no experiment process is running.

## 3. P2 decision point (hard stop — author input required)

Requested at `docs/P2-GATE.md` §4: (1) matrix amendment M1; (2) Amendment A2 items/AC choices;
(3) K6 non-realisability; (4) **budget/scope** given the measured infeasibility; (5) M7 calibration
block; (6) acknowledgement of the S5 conditions. Stage A/B/C will not start before this approval.
No B/C result may be claimed in the paper unless the corresponding runs are executed.

## 4. Honest limitations recorded so far

- Native Windows processes instead of containers (no Docker/WSL2/admin); single-host memory/disk
  pressure forced smaller JVM heaps and 1 MB Kafka index files (A3, M8/M9).
- Topic deletion is fatal on this KRaft/Windows deployment, so the campaign never deletes topics and
  relies on unique per-run topics plus periodic full-stack resets (M5/M11).
- TLC results are bounded witnesses; the zombie/partial-publication claims are model-level only (K6
  is not experimentally realisable within T_max=300 s).
- Oracle mutation testing is on synthetic fixtures (G3); it validates the checker, not mechanisms.
