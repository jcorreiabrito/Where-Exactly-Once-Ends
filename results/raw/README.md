# results/raw — campaign evidence (populated by the runner)

This directory receives one run at a time during the campaign:

* `run-records/run_<run_id>.json` — the per-run record (prereg §5), validated by
  `experiments/validate_record.py` before it is written;
* `oracle/oracle_<run_id>.json` — the independent oracle output (counts, flags, digests, paths);
* `artifacts/<run_id>/` — curated retained artefacts + `digest-manifest.json` (local-only, git-ignored);
* `summaries/` — per-stage summaries produced by the runner.

Every file here must be traceable to a `results/RUNLOG.md` row. Bulk dumps, ground-truth files and
HdrHistogram `.hlog` files stay on the local scratch (`$EOS_RUNTIME_ROOT/runs`) per the retention
policy (prereg §5, D11); only violating runs, control runs and the pre-registered systematic sample
have their full dumps retained in the repository.

Status: **empty by design** — the Windows-substrate outputs are archived under
`results/windows-pilot/` and the approved Linux/container campaign populates this tree from scratch
(`docs/LINUX-CAMPAIGN-RUNBOOK.md`).
