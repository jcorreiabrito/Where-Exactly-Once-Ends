# results/ — evidence and provenance (S4-owned)

Everything a paper number depends on lives here or is linked from `results/RUNLOG.md`. The
integrity rules are `docs/BRIEF.md` §2 and `docs/prereg.md` §10–§12.

## Layout

| Path | Content | Committed? |
|---|---|---|
| `RUNLOG.md` | every experiment-affecting command: UTC, git SHA, host, stage/run, seed, exact command, outputs, exit status | yes (append-only) |
| `raw/run-records/` | one `wxeo.run.v1` JSON per run (prereg §5 schema; validator: `experiments/validate_record.py`) | yes |
| `raw/oracle/` | `oracle_<run>.json` per run (counts, violation flags, digests, paths) | yes |
| `raw/artifacts/<run_id>/` | curated retained artefacts + `digest-manifest.json` (policy per prereg §5) | yes (digest manifest always) |
| `raw/summaries/` | per-stage summaries | yes |
| `faults.log` | append-only NDJSON injector/failpoint receipts (exactly one fired + one restored per armed injection; crash failpoints: S3 fired-only + S4 restored) | yes (rotated if large) |
| `fault_restart.log` | periodic full-stack restart records (D6, every 50 runs) | yes |
| `harness/g3-mutation.json` | G3 checker mutation-testing evidence (100 % detection required) | yes |
| `DEVIATIONS.md` | GENERATED planned-vs-executed diff + frozen interpretation/erratum register (prereg §12) | yes |
| `analysis/` | frozen-pipeline outputs (`macros.tex`, tables t1–t6, tests.json, decision-table.csv, per-run CSVs) | yes, from `tools/analyze.py` |
| `dryrun/`, `synthetic/` | **SYNTHETIC fixtures and dry-run outputs — NOT results**, clearly labelled; never analysed as campaign data | local demonstration only |

## Rules enforced by the harness

* Every run record must pass `experiments/validate_record.py` before it is written.
* A run is excluded only for the exhaustive provenance reasons of `docs/prereg.md` §8.2, never for
  an outcome reason; exclusions and non-fired faults are counted and reported per cell.
* Bulk dumps/GT files/HdrHistogram `.hlog` stay on the local non-synced scratch
  (`%LOCALAPPDATA%\eos-runtime\runs`, D11); the repository keeps digests + curated copies.
* `results/analysis/` is a pure function of `results/raw/` plus `docs/prereg-freeze.json`; the
  scripts are hashed in the freeze before Stage A.

## Current status (P2)

No campaign runs yet. The frozen matrix/run-order, the independent oracle (G3 green), the
runner dry-run and the frozen analysis/figure pipeline are validated on synthetic fixtures
(`results/dryrun/`, `results/synthetic/`). The pilot starts only after S3's artifact acceptance
tests and the lead's P2 integration confirmation.
