# Where Exactly-Once Ends

**Preserving Double-Entry Ledger Invariants When Apache Kafka Pipelines Write to External Stores**

Research article (RITA — Revista de Informática Teórica e Aplicada, UFRGS) and its fully
reproducible artifact.

- Author: João Brito — Universidade Federal do ABC (UFABC), Santo André, SP, Brazil
- Paper: `main.tex` → compiled PDF in `submission/`
- Artifact: `artifact/` (Java 21 / Spring Boot 3 / spring-kafka), `experiments/` (matrix, fault
  injectors, load generator), `results/` (every run logged in `results/RUNLOG.md`)

## What the paper studies

Kafka's exactly-once semantics (EOS) atomically cover Kafka-internal log appends and consumer
offsets. When the useful side effect lands in an external store — a PostgreSQL ledger, a payment
rail — that atomicity ends. In a double-entry ledger a duplicate posting is not a redundant row:
it violates the accounting invariant and silently corrupts auditability.

The paper (i) defines the ledger state machine and the precise EOS boundary, (ii) proves or
counterexamples invariant preservation per mechanism x crash point, and (iii) measures, under
scripted faults, five mechanisms and three combinations:

| Mechanism | Idea | Role |
|-----------|------|------|
| M0 | naive at-least-once, plain `INSERT`, offset after DB write | negative control |
| M1-kafka | Kafka EOS only, Kafka-internal sink | positive control |
| M1x | Kafka EOS, non-atomic external DB write | what practitioners deploy |
| M2 | transactional outbox | candidate |
| M3 | idempotency key + unique index, `ON CONFLICT DO NOTHING` | candidate |
| M4 | offsets stored in the same DB transaction as postings | candidate |
| M2+M3, M4+M3, M1x+M3 | combinations for RQ3 | minimal sufficient sets |

Faults: F1 broker killed mid-produce, F2 consumer killed (rebalance), F3 produce timeout with
unknown outcome, F4 DB-commit/offset-commit divergence (both orderings), F5 network partition to
the DB, F6 duplicate redelivery after rebalance. Ground truth is written by the load generator
before events enter Kafka and checked by an independent oracle.

## Reproduce

Prerequisites on the target machine: Java 21+, Python 3.12+, PowerShell 7, MiKTeX (or TeX Live),
and either Docker (preferred) or the native-process path used for the reported runs.

```powershell
# 1. Toolchain (portable, pinned; skipped when already present)
powershell -ExecutionPolicy Bypass -File tools/bootstrap-toolchain.ps1

# 2. Artifact tests
cd artifact; mvn -q test; cd ..

# 3. Full experiment matrix (writes results/RUNLOG.md, results/raw/)
powershell -ExecutionPolicy Bypass -File experiments/run-matrix.ps1 -Stage pilot

# 4. Analysis and figures (regenerates every number/plot from results/raw/)
python tools/analyze.py --results results/raw --out results/analysis
python tools/make-figures.py --analysis results/analysis --out figures

# 5. Paper
powershell -ExecutionPolicy Bypass -File build.ps1 -Main main.tex -Clean -OutDir submission
```

Environment facts of the reported run (honest deviations from a containerised setup) are listed
in `results/DEVIATIONS.md`, `docs/prereg.md`, and the paper's threats-to-validity section.

## Repository map

```
main.tex refs.bib sections/ figures/ build.ps1 build.sh SelfArx.cls   manuscript
artifact/            Java service implementing M0-M4 and combinations
experiments/         matrix spec, fault injectors, load generator, runner
results/             RUNLOG.md, raw per-run data, summary CSVs, analysis outputs
refs/ lit/           verified bibliography and related-work matrix
docs/                BRIEF.md, AMENDMENT-A1.md, method.md, prereg.md, redteam.md
submission/          camera-ready PDF, source, cover letter, checklist, VENUE-RULES.md
tools/               bootstrap, secret guard, analysis, figure scripts
```

## Integrity

Every number in the paper traces to a file under `results/` produced by a command recorded in
`results/RUNLOG.md` with UTC timestamp, git commit SHA, host and seed. The reference base is
verified entry-by-entry (`refs/verified.csv`, DOI via Crossref, quoted support sentence). The
repository is checked before every push by `tools/guard-secrets.ps1`.

## License

MIT (see `LICENSE`). The manuscript text is © 2026 João Brito; the artifact is MIT-licensed.
