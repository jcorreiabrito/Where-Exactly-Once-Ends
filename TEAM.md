# TEAM — assignments, status, blockers

Last updated: 2026-11-24 — **P1 APPROVED; P2 in progress**.

| Agent | Role | Assignment | Status | Artifact paths | Acceptance test |
|---|---|---|---|---|---|
| lead | Lead Researcher / orchestrator | Integration, venue rules, toolchain, formal-model review, manuscript, final QA | ACTIVE | `docs/`, `submission/`, `main.tex`, `build.ps1`, `tools/` | Definition of Done in BRIEF §1 |
| S1 lit-scout | Literature Scout | Search strategy, retrieval, Crossref verification, related-work matrix | **COMPLETE** | `refs/refs.bib` (63 entries), `refs/verified.csv`, `refs/RETRIEVALS.md`, `lit/matrix.csv`, `lit/search-strategy.md`, `data/raw/lit/` (195 cached sources), `tools/verify-refs.py`, `tools/fetch-lit.py` | `python tools/verify-refs.py --live` → 228/228 checks PASS |
| S2 method | Methodologist | RQs, formal model draft, invariant propositions, experiment design, metrics, pre-registered statistics, threats, decision register | **COMPLETE (P1, approved)** | `docs/method.md`, `docs/prereg.md`, `formal/ledger.tla`, `formal/README.md` | P1 gate approved by author 2026-11-24 (`docs/P1-APPROVAL.md`) |
| S3 artifact-engineer | Artifact Engineer | Java 21/Spring Boot service implementing M0–M4 + combinations, `strict/early` ordering, failpoints, deterministic load generator, native-stack harness library, unit tests | **ACTIVE (P2)** | `artifact/**` | `mvn test` green; jar builds; fault-free M0/M3 smoke writes documented artefacts; config hash stable |
| S4 experiment-runner | Experiment Runner | Independent Python oracle/checker (built before mechanisms), matrix/run-order generation, runner, F1–F6 injectors, faults.log, RUNLOG, deviations, analysis+figure scripts, then pilot | **ACTIVE (P2)** | `experiments/**`, `results/**`, `tools/analyze.py`, `tools/make-figures.py` | Checker mutation testing 100 % (G3); pilot (216+20 runs) records valid; P2 projection worksheet filled from measured timings |
| S5 red-team-reviewer | Red-Team Reviewer | Early review of oracle design before Stage A + full rejection memo on draft | **NOT STARTED** (spawn after oracle lands) | `docs/redteam.md` | Every objection answered in paper or conceded |
| S6 compliance-editor | Compliance Editor | Venue rules, formatting, page limits, references, AI declaration, submission folder | **NOT STARTED** (P7) | `submission/` | Checklist complete |

## Author decisions (P1 gate, 2026-11-24)
Approved method + prereg as drafted; **all D1–D15 draft recommendations accepted**; budget
**H = 8–24 h unattended**; **ordering probe added** to all two-commit mechanisms (H3 testable);
**TLC check at P2** and formal appendix. Full record: `docs/P1-APPROVAL.md`.

## AMENDMENT A1 (binding, 2026-11-24)
M0 negative control; M1-kafka positive control; M1x; M2; M3; M4; combinations M2+M3, M4+M3,
M1x+M3; H1–H5; I1–I4; crash points C1–C12; independent oracle built before mechanisms; 100 %
checker mutation testing; two-level fault injection; staged design A–D; pre-registered statistics
(Clopper–Pearson, rule of three, Fisher/McNemar + Holm, Friedman/Nemenyi/Cliff's delta);
`docs/prereg.md` mandatory; P1/P2 hard approval stops. Text: `docs/AMENDMENT-A1.md`.

## Environment status
- Native process substrate validated: JDK 21.0.13.1, Maven 3.9.9, Kafka 3.7.2 3-broker KRaft,
  PostgreSQL 16.11, Toxiproxy 2.9.0 (evidence `docs/SUBSTRATE-VALIDATION.md`; digests
  `tools/TOOLCHAIN-VERSIONS.txt`).
- Public repo pushed; CI covers paper build, reference verification, secret scan, Maven tests.
- Interfaces between S3 and S4 frozen in `docs/CONTRACTS.md`.

## Current gate
**P2 in progress — pre-pilot.** S3 artifact **ACCEPTED** (acceptance suite 4/4: Kafka smoke; M0 and
M3 E2E 1000 events → 2000/2000 postings, 0 duplicates; crash failpoint exit 99 with one fired
receipt; 51/51 unit tests; CI green including Maven tests). S5's re-review of the fixed
implementation found **BLOCK-level regressions R1–R4** in the runner/injectors (live injector
dispatch removed; app-owned receipt ownership contradiction; K2/K5 matrix injector_mode wrong;
F-I3 phase window unwired); S4 is fixing them. The pilot starts only after S5 re-clears the fixes
and a live smoke yields a schema-valid record with attested receipts. Then **P2 gate** = pilot +
projection worksheet + cut-ladder decision + author approval of Amendment A2, the matrix amendment
and the budget.

## P2 progress log
- Formal model: **machine-checked** with TLC 2.19 (tla2tools v1.7.4), bounds N=3/BatchSize=2; all 8
  configurations matched pre-registered expectations (`formal/tlc-results/tlc-summary.json`); scope
  narrowed per S5 (C7/I2/liveness/publication-stream not modelled).
- **Platform constraints discovered and mitigated:** topic deletion is fatal on this Windows KRaft
  deployment (partition-dir rename → all log dirs offline); PowerShell 5.1 parses BOM-less UTF-8 as
  ANSI (all harness scripts must be pure ASCII); service needed non-final `@Configuration`.
  Mitigations: no per-run topic deletion, unique topics + periodic `reset-kafka` (M5 cadence
  1k→50/10k→25/100k→20/1M→1, D-S3-17 `delete.topic.enable=false`), ASCII-only script rule.
- S4: G3 mutation testing **1,800/1,800 detected + attributed**; gates G1–G8 falsifiable with
  negative controls; freeze binds jar SHA-256 + 66-file source digest + configs; runner M5
  lifecycle with liveness/disk guards; `live_smoke_evidence.py`; 52 tests green.
- **S5 re-review status:** acceptance evidence verified GENUINE (GT digests reproduce, crash receipt
  real); matrix/A2 arithmetic recompute exactly (7,514 rows; A 1,530; B 3,960; C 1,800; seeds
  reproduce). **Blocking R1–R4 in the fix-pass code** (see Current gate) — S4 fixing; S5 will
  re-verify before pilot.
- S2: Amendment A2 revised for the new matrix (7,506 planned / 7,488 executable + 26 validation;
  K6 non-realisability; H1 pooled stratified sign-test option AC-3b; E3/C7 model-only) — hash pins
  will be refreshed after R3's matrix regeneration.
- Manuscript: all 10 sections drafted and integrated; ~33 pp and must be compressed to ≤25 in
  P6/P7; S6 compression plan measured (−11.9 pp).
- S6 compliance audit complete: `submission/CHECKLIST.md` (72 items) + `docs/compliance-report.md`;
  applied AI-declaration rewrite, COI/ethics, bibliography rendering/labels, Portuguese
  "partidas dobradas", PDF metadata; venue facts added (double-blind review → anonymized review PDF;
  CC BY-NC-ND; 20-day revision; PT/ES/EN).

## Activity log
- 2026-11-24: recon; foundation; template recovery; toolchain bootstrap; substrate validation.
- 2026-11-24: A1 amendment applied; S1 verified bibliography; S2 method+prereg+formal draft.
- 2026-11-24: public repo first push; CI skeleton; secret scanner hardened; **P1 approved by author**.
- 2026-11-24: **P2 started** — S3 implementation, S4 oracle/runner/injectors; contracts frozen.
