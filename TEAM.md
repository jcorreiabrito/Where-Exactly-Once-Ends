# TEAM — assignments, status, blockers

Last updated: 2026-11-24 (UTC-03) — P1 gate.

| Agent | Role | Assignment | Status | Artifact paths | Acceptance test |
|---|---|---|---|---|---|
| lead | Lead Researcher / orchestrator | Integration, venue rules, toolchain, formal-model review routing, manuscript, final QA | ACTIVE | `docs/`, `submission/`, `main.tex`, `build.ps1`, `tools/` | Definition of Done in BRIEF §1 |
| S1 lit-scout | Literature Scout | Search strategy, retrieval, Crossref verification, related-work matrix | **COMPLETE** | `refs/refs.bib` (63 entries), `refs/verified.csv`, `refs/RETRIEVALS.md`, `lit/matrix.csv`, `lit/search-strategy.md`, `data/raw/lit/` (195 cached sources), `tools/verify-refs.py`, `tools/fetch-lit.py` | `python tools/verify-refs.py --live` → 63 entries, 49 Crossref-verified peer DOIs, 14 gray (22.2 %), **228/228 checks PASS** |
| S2 method | Methodologist | RQs, formal model draft, invariant propositions, experiment design, metrics, pre-registered statistics, threats, decision register | **COMPLETE (P1 draft)** | `docs/method.md` (24.6k words), `docs/prereg.md` (6.5k words), `formal/ledger.tla`, `formal/README.md` | 12 required sections + 15 decisions + readiness verdict; 72-cell fault table; 63-cell crash-class table; 12 propositions; 8 counterexample traces; 7,866-run matrix + compute projection method |
| S3 artifact-engineer | Artifact Engineer | Independent oracle first, then Java 21 service (M0–M4 + combinations), Level-1 failpoints, Level-2 injectors, load generator, tests, CI | **HELD — awaiting P1 approval** | `artifact/`, `experiments/` | Compiles; tests green; each injector demonstrated against real stack; G1–G8 gates pass |
| S4 experiment-runner | Experiment Runner | Pilot → projection worksheet → Stage A–D execution, RUNLOG, raw data, deviations | **HELD — awaiting P1; P2 stop after pilot** | `results/` | Every run logged; no unlogged numbers; P2 projection from measured pilot values |
| S5 red-team-reviewer | Red-Team Reviewer | Early review of oracle design (before Stage A) + full rejection memo on draft | **NOT STARTED** (oracle review scheduled after P1) | `docs/redteam.md` | Every objection answered in paper or conceded |
| S6 compliance-editor | Compliance Editor | Venue rules, formatting, page limits, references, AI declaration, submission folder | **NOT STARTED** | `submission/` | Checklist complete; VENUE-RULES.md dated + URL'd |

## AMENDMENT A1 (2026-11-24, author) — binding
Mechanism set: **M0 (negative control), M1-kafka (positive control), M1x, M2, M3, M4, and M2+M3,
M4+M3, M1x+M3**; hypotheses **H1–H5**; invariants **I1–I4**; crash-point model C1..Cn; independent
oracle + harness-validation gate (checker mutation testing 100 %); two-level fault injection;
**staged design A–D**; statistics (Clopper–Pearson, rule of three, Fisher/McNemar + Holm,
Friedman/Nemenyi/Cliff's delta); **docs/prereg.md mandatory before P1**; **P1 and P2 are hard stops
for author approval**. Author owns the formal model and proofs; S2 drafts options/trade-offs. Full
text: `docs/AMENDMENT-A1.md`.

## Environment status (2026-11-24)
- No Docker, no admin rights, WSL2 disabled → **native Windows process substrate** validated:
  JDK 21.0.13.1, Maven 3.9.9, Kafka 3.7.2 3-node KRaft (SHA-512 verified), PostgreSQL 16.11,
  Toxiproxy 2.9.0. Evidence: `docs/SUBSTRATE-VALIDATION.md`.
- RITA template official URL 404 → archived official zip (Wayback), SHA-256 pinned; class compiled
  at 12 pt; manuscript skeleton builds; `build.ps1` green.
- Reference base verified (S1). Secret scanner clean. CI skeleton committed (paper + refs + scan;
  artifact tests auto-enable when `artifact/pom.xml` lands).

## Current gate and blockers
**P1 STOP — awaiting author approval** of `docs/method.md` + `docs/prereg.md` + decisions D1–D15
and the formal-model draft (`docs/P1-GATE.md`). Implementation (S3/S4) is held until approval.
Known facts the author must weigh: nominal matrix 7,866 runs; projection only after pilot (P2);
host constraints 12 CPUs / 16 GB RAM / ≈11.7 GB free disk.

## Activity log
- 2026-11-24: recon; workspace empty; repo reachable but empty; goal armed.
- 2026-11-24: foundation (BRIEF, TEAM, .gitignore, guard-secrets, build.ps1, venue rules); official
  template recovered and test-built; portable toolchain downloaded.
- 2026-11-24: **A1 amendment received and applied**; S2 produced revised method + prereg + formal
  draft; S1 produced 63-entry verified bibliography (228/228 checks); substrate validated
  (3-broker KRaft up, PostgreSQL up, paper builds); P1 gate package assembled (`docs/P1-GATE.md`).
