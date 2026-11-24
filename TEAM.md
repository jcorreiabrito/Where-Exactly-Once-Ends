# TEAM — assignments, status, blockers

Last updated: 2026-11-24 (UTC-03)

| Agent | Role | Assignment | Status | Artifact paths | Acceptance test |
|---|---|---|---|---|---|
| lead | Lead Researcher / orchestrator | Integration, venue rules, toolchain, manuscript, final QA | ACTIVE | `docs/BRIEF.md`, `submission/` | Definition of Done in BRIEF §1 |
| S1 literature-scout | Literature Scout | Search strategy, retrieval, Crossref verification, related-work matrix | STARTING | `refs/refs.bib`, `refs/verified.csv`, `refs/RETRIEVALS.md`, `lit/matrix.csv`, `lit/search-strategy.md` | Every bib entry has verified DOI + quote + cached source; ≥35 peer-reviewed refs |
| S2 methodologist | Methodologist | RQs, formal ledger/EOS model, invariant propositions, experiment design, metrics, pre-registered statistics, threats | STARTING | `docs/method.md` | Human-reviewable; falsifiable prediction table; no hand-waving; frozen before code |
| S3 artifact-engineer | Artifact Engineer | Java 21 Spring Boot service, ledger DB, M1/M2/M3, 6 fault injectors, load generator, tests, CI | NOT STARTED (blocked by S2 freeze) | `artifact/`, `experiments/`, `.github/workflows/ci.yml` | Compiles; tests green; each injector demonstrated against real stack |
| S4 experiment-runner | Experiment Runner | Execute matrix, log RUNLOG, raw data, figures data | NOT STARTED (blocked by S3 + toolchain) | `results/` | Every run logged; no unlogged numbers |
| S5 red-team-reviewer | Red-Team Reviewer | Adversarial internal-validity attack; hunt unbacked numbers | NOT STARTED (blocked by draft) | `docs/redteam.md` | Every objection has a paper location or is conceded |
| S6 compliance-editor | Compliance Editor | Venue rules, formatting, page limits, references, AI declaration | NOT STARTED | `submission/` | Checklist complete; VENUE-RULES.md dated+URL'd |

## Environment blockers / decisions
- No Docker, no admin, WSL2 unavailable → native Windows process stack (Kafka 3 KRaft brokers,
  PostgreSQL 16 portable, Toxiproxy). Decision recorded in `docs/method.md` threats + paper §threats.
- RITA2018.zip official URL 404 → archived/Overleaf template, provenance logged.
- `gh` absent; git credential helper `store`; push availability TBD (test before relying on it).

## AMENDMENT A1 (2026-11-24, author) — binding
Mechanism set expanded to **M0 (negative control), M1-kafka (positive control), M1x, M2, M3, M4,
plus combinations M2+M3, M4+M3, M1x+M3**; hypotheses replaced by **H1–H5**; invariants **I1–I4**;
crash-point model C1..Cn; independent oracle + harness-validation gate (mutation testing 100%);
two-level fault injection; **staged experiment design A–D** replacing the full factorial; new
statistics (Clopper–Pearson, rule of three, Fisher/McNemar + Holm, Friedman/Nemenyi/Cliff's delta);
**docs/prereg.md mandatory before P1**; **P1 and P2 are hard stops for author approval**.
Author owns the formal model and proofs; S2 drafts options/trade-offs. Full text:
`docs/AMENDMENT-A1.md`.
Impact: S2 revises `docs/method.md` in place and drafts `docs/prereg.md`; S1 extends literature
scope; S3/S4 pipeline code held until P1 approval; scaffold/toolchain/build/CI continue.

## Activity log
- 2026-11-24: recon complete; workspace empty; repo reachable but empty; goal armed.
- 2026-11-24: foundation written (BRIEF, TEAM, .gitignore, guard-secrets, build.ps1); RITA template
  recovered from Wayback (official URL 404) and test-built OK (5 pages); portable JDK21/Maven/Kafka/
  Toxiproxy/PostgreSQL downloads started; **A1 amendment received and applied**; P1 gate pending.
