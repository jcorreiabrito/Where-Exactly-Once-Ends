# experiments/ — S4 experiment harness (matrix, runner, injectors, oracle fixtures)

Owner: S4 (experiment runner). Frozen interface with S3: `docs/CONTRACTS.md`. Normative design:
`docs/prereg.md` (matrices, run protocol, run record) and `docs/method.md` (§1.4 derivation,
§2.5 predicates, §5.6 faults, §7.4 oracle). Every artefact under `results/` is produced by a
command recorded in `results/RUNLOG.md`.

**Status (P2):** oracle + G3 mutation testing green; matrix/run-order frozen and hashed;
runner dry-run validated on synthetic fixtures; frozen analysis/figure pipeline validated on a
synthetic 30-run fixture. The live pilot has NOT started — it waits for S3's artifact acceptance
tests and the lead's P2 integration confirmation.

## Layout

| Path | Purpose |
|---|---|
| `oracle/derive.py` | independent Python re-implementation of the deterministic event derivation and canonical GT digest (no code shared with the Java artifact) |
| `oracle/oracle.py` | independent checker: GT+source-integrity verification (gate G5), I1a/I1b/I2/I3/I4 predicates, publication metrics, `oracle_<run>.json` writer; live read-only dump helpers |
| `oracle/mutation_test.py` | G3 checker mutation testing (≥100 seeded mutations per defect class, 100 % detection + correct attribution) |
| `oracle/g5-vectors.json` | Java↔Python GT-digest contract vectors (S3's Java reproduced them byte-for-byte) |
| `fixtures/` | synthetic fixtures (clearly labelled, never results): mutation fixture, dry-run fixture builder, 30-run analysis fixture builder |
| `matrix_design.py` | frozen design constants (mechanisms, faults, reachability, n/a cells) |
| `matrix.py` | generates `matrix.yaml` + `run_order.csv` with the exact prereg counts and §4.3 randomisation constraints |
| `matrix.yaml`, `run_order.csv`, `matrix.sha256` | frozen, hashed design artefacts (7,866 planned / 7,851 executable + 20 validation) |
| `runner.py` | per-run sequence `docs/prereg.md` §4.1; dry-run mode against synthetic fixtures; RUNLOG/faults/deviation plumbing; free-space floor; restart-all every 50 runs |
| `run-matrix.ps1` | thin PowerShell wrapper around `runner.py` |
| `injectors.py` | F1–F6 real fault injectors (process kills, Toxiproxy TCP cuts/timeouts, offset rewind) + Level-1 failpoint watch + receipt protocol/G4 validator |
| `validate_record.py` | run-record schema validator (`wxeo.run.v1`) |
| `deviations.py` | generates `results/DEVIATIONS.md` (planned-vs-executed diff + frozen interpretation/erratum register) |
| `freeze.py` | writes/verifies `docs/prereg-freeze.json` (hashes, git SHA, UTC, seeds) |
| `tests/test_oracle.py` | unit tests: normative vectors (`docs/derive-vectors.csv`), G5 digests, predicates, mutation classes, tamper detection |

## Exact commands

```powershell
# independent oracle self-test + unit suite
python experiments/oracle/oracle.py selftest
python -m pytest experiments/tests -q

# G3 checker mutation testing -> results/harness/g3-mutation.json
python experiments/oracle/mutation_test.py --mutations 120

# matrix + run order (frozen before execution; hashed)
python experiments/matrix.py
python experiments/matrix.py --check

# runner dry-run on a synthetic fixture (no live stack)
python experiments/fixtures/build_dryrun_fixture.py --run-id pilot-m2-f0-1k-p3-r00
python experiments/runner.py --dry-run --fixture results/dryrun/fixtures/pilot-m2-f0-1k-p3-r00 `
    --stage pilot --limit 2 --results-dir results/dryrun
# thin wrapper: pwsh -File experiments/run-matrix.ps1 -DryRun -Fixture <dir> -Stage pilot -Limit 2

# S3 interface preflight (jar + harness + stack state)
python experiments/runner.py --check-env

# live stage (ONLY after S3 acceptance tests + lead confirmation)
pwsh -File experiments/run-matrix.ps1 -Stage pilot

# frozen analysis + figures (from committed run records only)
python tools/analyze.py --records results/raw/run-records --out results/analysis
python tools/make-figures.py --analysis results/analysis --out figures

# analysis freeze + deviations register
python experiments/freeze.py --write
python experiments/freeze.py --check
python experiments/runner.py --write-deviations
```

## Independence properties (auditable)

1. The oracle is Python and imports only the standard library in its predicate path; it never
   imports or reads `artifact/**` classes, and the expected posting multiset is re-derived from
   `(per-run data_seed, seq)` using `docs/method.md` §1.4.1 text.
2. Ground truth is written by the Java load generator before the first produce; the oracle
   verifies *file digest == regenerated digest == source-dump digest* (gate G5). Java↔Python
   agreement is pinned by two independent anchors: `docs/derive-vectors.csv` (lead's normative
   rows) and `experiments/oracle/g5-vectors.json` (S3 reproduced all four digests byte-for-byte).
3. The oracle is mutation-tested before the campaign (G3): every synthetic defect class must be
   detected at 100 % with the correct invariant attribution; a clean fixture must produce zero
   violations.
4. All inputs are streamed in `event_id`-prefix shards, so the 1M-event runs stay memory-bounded
   and the full GT file is never materialised.

## Notes for reviewers (S5)

* Synthetic fixtures are labelled `SYNTHETIC FIXTURE — NOT RESULTS` inside every record/file and
  live outside `results/raw/`; `tools/analyze.py`/`tools/make-figures.py` propagate the label.
* Interpretation register (lead rulings R1–R5, oracle verdict rule, G4 crash-receipt protocol,
  validation composition, freeze scope) is in `results/DEVIATIONS.md` §4.
* The live path implements the frozen contracts but could not be end-to-end validated on the
  stack until S3's artifact acceptance tests pass; `--check-env` reports exactly what is missing.
