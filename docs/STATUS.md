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

## 2. Running now

- **Pilot**: 198 executable rows + 26 validation runs, launched 16:14 local with
  `WXEO_DISABLE_PERIODIC_RESET=1` (deviation M11). Measured pace ≈ 41 s/run at 1k events/3
  partitions/2 consumer instances, so the pilot should finish around 18:30–19:00 local.
- Kafka index preallocation grows the runtime by ≈36 MB/run; a manual broker-log reset is planned
  mid-pilot (≈run 80) to stay well above the 3 GB free-space floor. The pilot is resumable: completed
  run records are skipped on restart.

## 3. P2 decision point — measured feasibility warning (preliminary, to be finalised with pilot p90)

From the first valid pilot runs the 1k/P3/C=2 run takes ≈36–45 s end-to-end (service start, load,
fault+restore, quiescence, dumps, oracle, archive). Projected with the frozen formula (§9.2,
p90 basis) and the frozen run counts:

| Block | runs | measured basis | projected h (p90) |
|---|---|---|---|
| Validation | 26 | 36–43 s | ≈0.3 |
| Stage A (crash classes + controls) | 1,530 | 45.4 s (preliminary) | ≈19.3 (+15 % = 22.2) |
| Stage B (core/S1/S2, 10k–100k) | 3,960 | **not measured** (needs M7 calibration block) | — |
| Stage C (fixed-rate/saturation/1M) | 1,800 | **not measured** | — |

**Consequence:** even the *largest* cut-ladder step defined in A1 §7 (reduce correctness N to 20)
leaves Stage A at ≈1,020 runs ≈13 h, and Stage B/C cannot be projected at all until the pilot
calibration block exists. The full 7,506-run programme is **not executable within the approved
8–24 h unattended budget on this host**. This is a genuine P2 decisions item, not an implementation
bug: the author must choose a reduced, still pre-registered design (e.g. Stage A only with
reduced N and B/C explicitly out of scope, or a narrow B subset after the M7 calibration block) or
extend the budget/host.

## 4. Waiting on

- **S5 final focused re-review** of R1–R9 + R11 + live evidence (PILOT gate verdict + residual
  Stage-A conditions). `docs/redteam-rereview.md` is being updated.
- **Pilot completion**, then the measured projection worksheet (`tools/projection.py`) and the
  **P2 gate ask to the author** (matrix amendment M1, Amendment A2, K6 non-realisability, M7
  calibration block, budget/scope decision). Stage A/B/C remain hard-stopped until then.

## 5. Honest limitations recorded so far

- Native Windows processes instead of containers (no Docker/WSL2/admin); single-host memory/disk
  pressure forced smaller JVM heaps and 1 MB Kafka index files (A3, M8/M9).
- Topic deletion is fatal on this KRaft/Windows deployment, so the campaign never deletes topics and
  relies on unique per-run topics plus periodic full-stack resets (M5/M11).
- TLC results are bounded witnesses; the zombie/partial-publication claims are model-level only (K6
  is not experimentally realisable within T_max=300 s).
- Oracle mutation testing is on synthetic fixtures (G3); it validates the checker, not mechanisms.
