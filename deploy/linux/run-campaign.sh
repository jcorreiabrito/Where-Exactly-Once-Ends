#!/usr/bin/env bash
# WXEO campaign driver for the Linux/container host. The frozen runner owns the run order, seeds,
# reset cadence and records; this script only sequences the approved phases and keeps logs.
#
#   bash deploy/linux/run-campaign.sh smoke      # T0: build + live fault-free + attested fault run
#   bash deploy/linux/run-campaign.sh calibrate  # M7: 54-run calibration block (9 mech x {10k,100k})
#   bash deploy/linux/run-campaign.sh pilot      # pilot (198 executable + 26 validation)
#   bash deploy/linux/run-campaign.sh A|B|C      # confirmatory stages (after P2 approvals + freeze)
#   bash deploy/linux/run-campaign.sh analysis   # A2 analyzer + figures + paper macros
#
# Every invocation appends its own RUNLOG row through the runner and writes a phase log under
# results/phase-logs/.  The stack is reset by the runner's M5 cadence (cheap on docker).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"
PHASE="${1:-}"
if [[ -z "$PHASE" ]]; then echo "usage: $0 smoke|calibrate|pilot|A|B|C|analysis|status"; exit 2; fi

export WXEO_STACK_MODE=docker
export EOS_RUNTIME_ROOT="${EOS_RUNTIME_ROOT:-$HOME/eos-runtime}"
PY="${PY:-.venv/bin/python}"
if [[ ! -x "$PY" ]]; then PY="python3"; fi
mkdir -p results/phase-logs "$EOS_RUNTIME_ROOT/run"
LOG="results/phase-logs/${PHASE}-$(date -u +%Y%m%dT%H%M%SZ).log"

phase_stack_up() {
  docker compose -f deploy/docker/docker-compose.yml -p wxeo-campaign up -d --wait --remove-orphans
}

phase() {
  case "$PHASE" in
    status)
      docker compose -f deploy/docker/docker-compose.yml -p wxeo-campaign ps
      "$PY" experiments/runner.py --check-env
      ;;
    smoke)
      phase_stack_up
      # T0: fault-free control + one app-owned failpoint class (F4b) + one S4-owned external class
      # (F2).  K3/K5 are Stage-A classes (the pilot slate has none), so the app-owned smoke uses the
      # pilot's app-owned F4b row; the live evidence tool attests both owners.
      "$PY" experiments/runner.py --stage pilot --run-id pilot-m2-f0-1k-p3-r00 --no-resume
      "$PY" experiments/runner.py --stage pilot --run-id pilot-m0-f4b-1k-p3-r00 --no-resume
      "$PY" experiments/runner.py --stage pilot --run-id pilot-m1x-f2-1k-p3-r00 --no-resume
      "$PY" experiments/live_smoke_evidence.py --run-id pilot-m2-f0-1k-p3-r00 \
        --attested-runs pilot-m0-f4b-1k-p3-r00,pilot-m1x-f2-1k-p3-r00
      ;;
    calibrate)
      "$PY" experiments/runner.py --stage M7cal --no-resume
      "$PY" experiments/rates.py --write   # R12: derives experiments/rates.json from the block
      ;;
    pilot)
      "$PY" experiments/runner.py --stage pilot
      ;;
    A|B|C)
      "$PY" experiments/runner.py --stage "$PHASE"
      ;;
    analysis)
      "$PY" tools/analyze.py --stage all
      "$PY" tools/make-figures.py
      ;;
    *)
      echo "unknown phase: $PHASE"; exit 2;;
  esac
}

git_sha(){ git rev-parse --short HEAD; }
{
  echo "phase=$PHASE utc=$(date -u +%Y-%m-%dT%H:%M:%SZ) git=$(git_sha)"
  echo "stack_mode=$WXEO_STACK_MODE runtime_root=$EOS_RUNTIME_ROOT"
} | tee "$LOG"
phase 2>&1 | tee -a "$LOG"
echo "phase $PHASE finished; log: $LOG"
