# PROJECT BRIEF — Authoritative operating contract (for all agents)

Working dir: repository root (local absolute path deliberately omitted from committed files).
Venue: RITA — Revista de Informática Teórica e Aplicada (UFRGS, Brazil). Research Article, 12–25 pp.
Working title: **Where Exactly-Once Ends: Preserving Double-Entry Ledger Invariants When Apache Kafka Pipelines Write to External Stores**

## 1. Deliverable
Submission-ready English research article in LaTeX + reproducible public artifact.
Repo: https://github.com/jcorreiabrito/Where-Exactly-Once-Ends (public, MIT).

Author (single author): **João Brito**, Universidade Federal do ABC (UFABC), Santo André, SP, Brazil.
Email: correiadebrito.joao@gmail.com. No other affiliations, no invented co-authors.

## 2. Integrity rules (override everything)
- NEVER invent a number, table cell, or figure value. Every quantitative claim traces to a file
  under `results/`, produced by a command recorded in `results/RUNLOG.md` (command, UTC timestamp,
  git commit SHA, host, seed). If an experiment cannot run, report it as a limitation — never
  simulate, extrapolate, or "illustratively" fill data.
- NEVER cite a paper not actually opened. Every BibTeX entry needs: DOI verified via Crossref API,
  a row in `refs/verified.csv` (key, DOI, retrieval date, exact supporting sentence), and the raw
  source cached under `data/raw/lit/`. Delete unverifiable references.
- AI is not an author. AI-generated research data is prohibited. Authors own all errors.
- All statistics pre-specified in `docs/method.md` BEFORE looking at results (no p-hacking).
- Honest deviations: if the planned matrix is reduced for feasibility, record exact deviations in
  `results/DEVIATIONS.md` and the paper's limitations. Truthful N matters more than planned N.

## 3. CONFIDENTIALITY FIREWALL (never expires)
The author works in the financial sector. NOTHING about any employer may appear anywhere in the
repo, paper, commit messages, logs, or metadata: no company/product names, internal hostnames,
ticket IDs, repo URLs, team names, org charts, system names, or the author's resume contents.
Build the artifact from scratch using public technology only. Before EVERY push and at final
submission run `powershell -File tools/guard-secrets.ps1`; ABORT on any hit. The confidential
deny-list is local only (`tools/forbidden.local.txt`, gitignored) — never commit it.

## 4. Research questions and hypotheses (must be stated, tested, honestly confirmed/falsified)
RQ1 Which failure modes make Kafka EOS fail to preserve double-entry ledger invariants when the
sink is an external relational DB?
RQ2 How do three mechanisms compare — M1 Kafka transactions with atomic offset+write (EOS used as
if it covered the sink), M2 transactional outbox/inbox, M3 idempotency-key dedup with a unique
index — on duplicate postings, lost postings, invariant violations, throughput, p99 e2e latency?
RQ3 What is the minimal mechanism set sufficient for each failure class?
H1 M1 alone preserves invariants only for Kafka-internal state; loses them whenever DB commit and
offset commit can diverge.
H2 M2 is sufficient across all modelled failure modes but costs throughput and e2e latency.
H3 M3 alone (no unique index, client-side retry) loses/duplicates postings when the insert succeeds
and the acknowledgement is lost; with a unique index + server-side idempotent retry it is safe.

## 5. Formal part (required; makes this a journal article)
Define the ledger state machine: accounts, journal entries, balanced debit/credit legs, append-only
postings, sum-to-zero invariant. Define precisely the EOS boundary of the pipeline. For each
mechanism × failure class give an invariant-preservation argument (theorem/proof sketch or explicit
counterexample), with stated assumptions. Propositions must be falsifiable predictions, and the
experiment is the test.

## 6. Experiment design (adapted to real machine capability — see §9)
Real scripted fault injectors, no thought experiments:
F1 broker killed mid-produce; F2 consumer killed during processing (rebalance); F3 produce timeout
with unknown outcome; F4 DB commit succeeded + offset commit failed (and inverse); F5 network
partition app↔DB; F6 duplicate redelivery after rebalance.
Dimensions: mechanisms {M1,M2,M3} × faults {F1..F6, none} × loads {10k,100k,1M events} ×
partitions {3,6,12} × repetitions (N=30 target; feasibility deviations logged).
Metrics: duplicate postings, lost postings, invariant violations (exact SQL), throughput (events/s),
p50/p99 end-to-end latency, time-to-consistent-state.
Ground truth: generator knows every intended posting; compare against DB after quiescence.
Stats (pre-registered): median + IQR per cell; bootstrap 95% CI; Friedman + Nemenyi post-hoc across
mechanisms; alpha=0.05; Cliff's delta effect sizes.
Threats: single-host fidelity (3 broker processes, not a cluster), simplified ledger model, fault
injector determinism, benchmark limits — all addressed explicitly in the paper.

## 7. Artifact and repo conventions
Repo layout:
```
main.tex refs.bib sections/ figures/ build.ps1 CITATION.cff LICENSE README.md TEAM.md
artifact/            Java 21 + Spring Boot 3 + spring-kafka service, Maven project
experiments/         matrix.yaml, runner scripts, fault injectors, load generator
results/             RUNLOG.md, raw per-run records, summary CSVs, analysis/
tools/               bootstrap-toolchain.ps1, guard-secrets.ps1, fetch scripts
refs/                refs.bib, verified.csv, RETRIEVALS.md
lit/                 matrix.csv, search-strategy.md
docs/                BRIEF.md, method.md, redteam.md, ...
submission/          PDF, LaTeX source, cover letter, checklist, VENUE-RULES.md
.github/workflows/   ci.yml (builds paper + runs artifact tests, uploads PDF)
```
Git: conventional commits, feature branches, one PR per unit (artifact, experiments, manuscript).
Version tags; final `v1.0-submission`. Never commit secrets, unlicensed data, employer material,
`tools/vendor/`, `tools/forbidden.local.txt`, or build outputs.

## 8. Logging protocol
Every experiment command appended to `results/RUNLOG.md` with: UTC timestamp, git SHA, host
(Windows build), seed, exact command, output paths, exit status. Seeds recorded per run; the same
seed must reproduce the same run. Raw per-run JSON/CSV kept under `results/raw/` and committed
(compressed if large) so figures can be regenerated from committed data.

## 9. Machine reality (verified 2026-11-24, do not re-litigate)
- Windows 11, 12 logical CPUs, 16 GB RAM, ~14 GB free disk. Workspace is cloud-synced, so runtime
  data and toolchain live on a local non-synced scratch path (see `tools/bootstrap-toolchain.ps1`).
- NO Docker, NO admin rights, WSL2 cannot boot (virtualization disabled), no `gh`, no `mvn`.
- Java 22 (`java`/`javac` 22.0.1); MiKTeX 25.12 with pdflatex/bibtex/biber; `latexmk` UNUSABLE.
- Python 3.12 + pip (install scipy/matplotlib as needed); bundled DSH Python has numpy/pandas.
- Maven, JDK 21, Kafka, PostgreSQL 16, Toxiproxy are downloaded as portable binaries into
  `tools/vendor/` (gitignored) by `tools/bootstrap-toolchain.ps1`.
- Kafka runs as 3 native KRaft broker processes; PostgreSQL 16 as `pg_ctl` service; Toxiproxy for
  network faults. Fault injection kills real processes / cuts real TCP proxies.
- Paper build: `powershell -ExecutionPolicy Bypass -File build.ps1 -Main main.tex -Clean`
  (pdflatex → bibtex → pdflatex ×2, fails on errors/unresolved citations).

## 10. Literature/venue facts (verified 2026-11-24)
- RITA author guidelines: https://seer.ufrgs.br/index.php/rita/AuthorGuidelines — sequence: title;
  author names with affiliations/emails in footnote; abstract (≤200 words) + keywords; text;
  acknowledgments; references. Numbered references cited in brackets. Single-spaced, 12 pt,
  italics not underline, figures/tables in text, PDF (LaTeX) for review.
- RITA AI guidelines: https://seer.ufrgs.br/index.php/rita/iaguidelines — AI not an author; AI may
  assist formatting/editing/translation/readability only, never replace analytical work; mandatory
  section titled exactly `Declaration of AI-Assisted Use in Writing` immediately before References;
  AI-generated data/images prohibited; authors fully responsible for hallucination/plagiarism/bias.
- Template historically `http://www.inf.ufrgs.br/revista/RITA2018.zip` (currently 404; use archived
  copy or Overleaf RITA template, record provenance in submission/VENUE-RULES.md).
- Diamond open access, no APC, Scopus-indexed. Submission via SEER.

## 11. Team protocol
- Six subagents: S1 literature, S2 methodologist, S3 artifact engineer, S4 experiment runner,
  S5 red-team reviewer, S6 compliance editor. Real subagents only; no role-play.
- Every claim of "done" requires an artifact path a stranger can open + an acceptance test result.
- Update `TEAM.md` with assignment/status/blockers; use shared task board for deliverables.
- Serialize true dependencies: S2 method frozen before S3/S4 experiment code; artifact tested
  before full matrix; draft before red-team; red-team answered before compliance.
- Report progress at each phase gate P1–P7.
