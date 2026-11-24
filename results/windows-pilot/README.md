# results/windows-pilot — archived Windows-substrate campaign outputs (do not mix with Linux data)

These files were produced on the retired Windows/container-less host (native processes; see
`docs/prereg-amendment-A3.md`, `results/DEVIATIONS.md` M5–M16). They are kept for provenance and for
the paper's environment/limitation discussion, but they are **NOT** part of the Linux confirmatory
campaign and must never be merged into its analysis set:

* `run-records/` — 14 pilot/validation run records (6 valid post-freeze rows; invalid rows document
  the M16 injector defects).
* `oracle/` — the matching oracle outputs.
* `summaries/` — lifecycle/attempt summaries from the Windows runs.
* `evidence-only/` — the two pre-approval Stage-A records quarantined by S5 (M14): kept as evidence
  for the S4-owned fault path, never counted in Stage A.
* `artifacts/` — retained raw dumps (local-only; also git-ignored).
* `faults.log`, `fault_restart.log` — the Windows fault/receipt log and reset log.
* `live-smoke-evidence.json` — the Windows live evidence (freeze `fd271416…` at the time it was
  generated; historical). The Linux campaign writes a fresh `results/harness/live-smoke-evidence.json`
  against its own final freeze.

Why archived: the frozen runner's `--resume` skips run_ids whose record already exists, so leaving
Windows records under `results/raw/run-records/` would silently skip the corresponding Linux rows and
mix substrates. The Linux host starts from an empty `results/raw/**`.

All records here are already scrubbed by `experiments/pathsafe.py`; no absolute host paths are stored.
