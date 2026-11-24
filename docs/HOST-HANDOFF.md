# Linux campaign host — hand-off package

This is the operator hand-off for the approved confirmatory campaign. It assumes the author decision
in `docs/P2-APPROVAL.md` (full design on a Linux/container host; Amendments A2/A5/A6) and the frozen
tree at the commit named below. S5's final confirmation of the host freeze is the last gate before
Stage A (`docs/redteam-rereview.md`, current verdict pending §13).

## 1. Pin the exact tree

| Item | Value at hand-off |
|---|---|
| Commit to run | `29a33fd` (`fix(campaign): A5-r2/A6 calibration closure …`) |
| CI on that commit | GitHub Actions run **#27, all 4 jobs green** (paper build, Maven tests, bibliography, secret scan) |
| Harness suite | `python -m pytest experiments/tests -q` → **151 passed** |
| Matrix | `experiments/matrix.yaml` sha256 `5d348efe72106eadb538e6ec554bca2623c815ab4ff32661b07fb5cfb933ece6`; `experiments/run_order.csv` sha256 `f4e731a2e2bf2b346803f213944c302c90ab9b6e0d6714bc2eb9d932b79d651a`; `matrix.sha256` `aa9ad0b947589053678581262e33f49970f3c7c1c24d40abf262fcce53ebd250` |
| Run order | 7,631 rows = 7,514 confirmatory/validation slots + 117 calibration (M7-cal 54 + M7spot 54 + M7sat 9) |
| Confirmatory accounting | 7,506 planned / 7,488 executable / 26 validation (unchanged) |
| Artifact jar (Windows build) | `6e8223751706d64e7b77752b42df5694d4231ba8bd3522034806bae25e8e50d3` (the host rebuilds it; the host freeze binds its own jar digest) |

If anything above differs on the host, stop and report — do not run.

## 2. Host prerequisites

Ubuntu 22.04/24.04-class, ≥8 vCPU / 16 GB RAM (16/32 recommended for B/C), ≥150 GB free, Docker engine
+ compose v2 with the current user in the `docker` group, JDK 21, Maven 3.9+, Python ≥3.10 + venv,
`psql` 16. LaTeX is only needed for the final paper build.

## 3. Bootstrap and phases (exact order)

```bash
git clone https://github.com/jcorreiabrito/Where-Exactly-Once-Ends.git
cd Where-Exactly-Once-Ends
git checkout bd41133

bash deploy/linux/bootstrap.sh      # tools, jar, stack pull/up, container image digests JSON,
                                    # fault-efficacy, artifact unit+integration tests, harness tests,
                                    # PROVISIONAL freeze, preflight

bash deploy/linux/run-campaign.sh smoke               # T0  fault-free M0/M2/M3/M1-kafka + app F4b + S4 F2
bash deploy/linux/run-campaign.sh fault-efficacy      # T0b required JSON gate
bash deploy/linux/run-campaign.sh calibrate            # T1a 108 runs (M7-cal+M7spot) -> rates --derive candidate
bash deploy/linux/run-campaign.sh saturation-validate # T1b 9 runs at 2.5x-max -> rates --write validated
bash deploy/linux/run-campaign.sh gate-controls       # T3 under the PROVISIONAL freeze
bash deploy/linux/run-campaign.sh freeze              # T2 FINAL freeze: freeze.py --write --force --require-evidence
bash deploy/linux/run-campaign.sh pilot               # T4
bash deploy/linux/run-campaign.sh A && bash deploy/linux/run-campaign.sh B && bash deploy/linux/run-campaign.sh C
bash deploy/linux/run-campaign.sh analysis            # T6
```

Rules: one phase at a time; run inside `tmux`/`nohup`; the runner is resumable (re-run the same
phase after an interruption); T2 must be the **last** step before T4, and T3 must complete before T2
(freeze.py `--require-evidence` depends on gate-controls evidence).

## 4. What the lead needs back from the host

1. **Commit/CI evidence:** `git rev-parse HEAD`, `git status --porcelain` (clean), CI run id for the
   host tree if the host pushes changes (otherwise the `29a33fd` run #25).
2. **Freeze evidence** (S5 re-verifies read-only): `docs/prereg-freeze.json` file SHA-256 +
   `aggregate_pipeline_sha256` + `frozen_at_utc` + `git_sha` + `missing_at_freeze: []` + jar/source
   digests; `python experiments/freeze.py --check` → `ok:true`; the `pre_freeze_harness_evidence`
   block must contain `experiments/rates.json`, `results/harness/live-smoke-evidence.json`,
   `results/harness/linux-fault-efficacy.json`, `results/harness/linux-gate-controls.json`,
   `results/harness/container-images.json`.
3. **Bootstrap host log** (`results/phase-logs/`): OS/kernel, Docker+Compose versions, container image
   repo digests, JDK/Maven/Python/psql versions, jar SHA-256, `mvn package`/`mvn verify` summaries,
   pytest count, `--check-env`, 3-broker + proxy-inventory liveness.
4. **T0/T0b:** `live-smoke-evidence.json passed=true`; `linux-fault-efficacy.json passed=true`.
5. **T1a/T1b:** 117 calibration records (54+54+9) and `rates.json` with `rule_id=A5.partition-scale.v1`,
   `validation.validated=true`, `saturation_validation.mode="post-rates"`,
   `saturation_validation.validated=true`, `validated=true`; `rates.py --check` exit 0 with G7 ok.
6. **T3:** `linux-gate-controls.json passed=true` (produced before the final freeze).
7. **T2:** the final freeze JSON and a `freeze --check` transcript.
8. **T4/T5/T6:** per-phase logs, `results/raw/run-records/**`, `results/raw/oracle/**`,
   `results/raw/summaries/**`, analysis outputs (`a2-analysis.json`, `a2-hypotheses.json`,
   `hypotheses.json` source=A2, tables, figures), RUNLOG rows for every phase.
9. **Paper:** rebuilt ≤25 pp PDF (results inserted, reserve cuts per `submission/CHECKLIST.md` K2/K6),
   anonymized review PDF, cover letter, checklist/VENUE-RULES updates.

Anything deviating is recorded as a numbered entry in `results/DEVIATIONS.md` with its validity
effect; numbers must pass only through the frozen analysis pipeline.

## 5. What remains blocked until the host returns

- S5 final confirmation of the Linux-issued freeze (P2-APPROVAL boxes stay pending).
- No B/C result may be claimed or inserted in the paper before T5 data exists.
- The Windows-substrate outputs are archival only (`results/windows-pilot/`).
