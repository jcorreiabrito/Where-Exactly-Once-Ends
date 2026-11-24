#!/usr/bin/env bash
# WXEO campaign driver for the Linux/container host. The frozen runner owns the run order, seeds,
# reset cadence and records; this script only sequences the approved phases and keeps logs.
#
#   bash deploy/linux/run-campaign.sh status          # stack + runner preflight
#   bash deploy/linux/run-campaign.sh fault-efficacy  # T0b: toxics/kill paths actually work
#   bash deploy/linux/run-campaign.sh smoke           # T0: fault-free + app-owned + S4-owned runs
#   bash deploy/linux/run-campaign.sh calibrate       # T1: M7 54-run calibration + rates.json
#   bash deploy/linux/run-campaign.sh freeze          # T2: final freeze after calibration
#   bash deploy/linux/run-campaign.sh gate-controls   # T3: reserved validation slate (G1/G4/G5)
#   bash deploy/linux/run-campaign.sh pilot           # T4: pilot (198 executable + 26 validation)
#   bash deploy/linux/run-campaign.sh A|B|C           # T5: confirmatory stages
#   bash deploy/linux/run-campaign.sh analysis        # T6: A2 analyzer + figures
#
# Every invocation appends its own RUNLOG row through the runner and writes a phase log under
# results/phase-logs/.  The stack is reset by the runner's M5 cadence (cheap on docker).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"
PHASE="${1:-}"
if [[ -z "$PHASE" ]]; then
  echo "usage: $0 status|fault-efficacy|smoke|calibrate|freeze|gate-controls|pilot|A|B|C|analysis"
  exit 2
fi

export WXEO_STACK_MODE=docker
export EOS_RUNTIME_ROOT="${EOS_RUNTIME_ROOT:-$HOME/eos-runtime}"
PY="${PY:-.venv/bin/python}"
if [[ ! -x "$PY" ]]; then PY="python3"; fi
mkdir -p results/phase-logs results/harness "$EOS_RUNTIME_ROOT/run"
LOG="results/phase-logs/${PHASE}-$(date -u +%Y%m%dT%H%M%SZ).log"

phase_stack_up() {
  python -m experiments.stack up
}

run_one() {
  echo "+ runner --run-id $1"
  "$PY" experiments/runner.py --stage pilot --run-id "$1" --no-resume
}

phase() {
  case "$PHASE" in
    status)
      python -m experiments.stack status
      "$PY" experiments/runner.py --check-env
      ;;
    fault-efficacy)
      phase_stack_up
      "$PY" deploy/linux/fault-efficacy.py --out results/harness/linux-fault-efficacy.json
      ;;
    smoke)
      phase_stack_up
      # T0: fault-free controls for the three evidence mechanisms, one app-owned failpoint class
      # (F4b) and one S4-owned external class (F2). M3 proves the dedup index on the live path and
      # M1-kafka proves the ledger-definition stream (positive control); both are fault-free rows.
      run_one pilot-m2-f0-1k-p3-r00
      run_one pilot-m3-f0-1k-p3-r00
      run_one pilot-m1-kafka-f0-1k-p3-r00
      run_one pilot-m0-f4b-1k-p3-r00
      run_one pilot-m1x-f2-1k-p3-r00
      "$PY" experiments/live_smoke_evidence.py --run-id pilot-m2-f0-1k-p3-r00 \
        --attested-runs pilot-m0-f4b-1k-p3-r00,pilot-m1x-f2-1k-p3-r00
      ;;
    calibrate)
      # Amendment A5: --stage M7cal now executes 117 runs (M7-cal 54 + M7spot 63) used to derive
      # and validate the fixed-rate mapping rule.
      "$PY" experiments/runner.py --stage M7cal --no-resume
      "$PY" experiments/rates.py --write   # R12: derives experiments/rates.json from both blocks
      # T1 fails closed unless the derived table carries validated=true (A5 rule id recorded there).
      "$PY" - <<'PY'
import json, sys
with open("experiments/rates.json", encoding="utf-8") as fh:
    rates = json.load(fh)
if rates.get("validated") is not True:
    raise SystemExit("T1 FAIL: experiments/rates.json.validated is not true; the fixed-rate "
                     "mapping rule failed its A5 spot-check. Do not proceed to T2/T3.")
print("rates.json validated=true rule=%s" % rates.get("rule", rates.get("rule_id", "?")))
PY
      ;;
    freeze)
      # T2: the single FINAL freeze on the campaign host, after T0 smoke + T1 M7/M7spot + rates.json.
      # --require-evidence makes freeze.py refuse unless rates.json (validated), live-smoke-evidence
      # and linux-fault-efficacy.json are present. The bootstrap freeze is provisional (no flag).
      test -f experiments/rates.json || { echo "ERROR: experiments/rates.json missing; run T1 first" >&2; exit 1; }
      "$PY" experiments/freeze.py --write --force --require-evidence
      "$PY" experiments/freeze.py --check
      ;;
    gate-controls)
      # T3: the reserved validation slate (stage label V): G1 negative control (M0 K3/K5), sham and
      # injector self-tests (G4), oracle/read path (G5/G3), C=1 robustness and quiescence calibration.
      V_ROWS="$("$PY" - <<'PY'
import csv
rows = []
with open("experiments/run_order.csv", newline="", encoding="utf-8") as fh:
    for row in csv.DictReader(fh):
        if row.get("stage") == "pilot" and row.get("stage_label") == "V" and row.get("executed") == "True":
            rows.append(row["run_id"])
print(" ".join(rows))
PY
)"
      if [[ -z "$V_ROWS" ]]; then echo "ERROR: no reserved validation rows found" >&2; exit 1; fi
      for rid in $V_ROWS; do run_one "$rid"; done
      # Gate summary (fail closed): required record classes present; M0 K3/K5 negative control must
      # actually violate (G1); M3 dedup index attested (G3/G6); M1-kafka ledger clean (G2).
      "$PY" - <<'PY'
import csv, glob, json, os, subprocess

with open("experiments/run_order.csv", newline="", encoding="utf-8") as fh:
    v_rows = [r for r in csv.DictReader(fh)
              if r.get("stage") == "pilot" and r.get("stage_label") == "V"
              and r.get("executed") == "True"]

REQUIRED = {
    "g1k3": "M0 K3 negative control (I3 duplicate expected)",
    "g1k5": "M0 K5 negative control (I3 duplicate expected)",
    "sham": "sham injection attestation",
    "selftest": "injector self-test",
    "oracle": "oracle read path",
    "c1": "C=1 robustness",
    "quiesce": "quiescence calibration",
}


def load(rid):
    for pat in ("results/raw/run-records/run_%s.json" % rid,
                "results/raw/**/run_%s.json" % rid):
        hits = glob.glob(pat, recursive=True)
        if hits:
            try:
                with open(hits[0], encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:  # noqa: BLE001
                return None
    return None


summary = {"schema": "wxeo.linux-gate-controls.v1", "checks": [], "passed": False}
ok = True
by_class = {}
for kind, purpose in REQUIRED.items():
    ids = [r["run_id"] for r in v_rows if kind in r.get("cell_id", "").lower()]
    records = [(rid, load(rid)) for rid in ids]
    good = bool(ids) and all(rec is not None for _, rec in records)
    by_class[kind] = records
    summary["checks"].append({"class": kind, "purpose": purpose, "run_ids": ids,
                              "records": sum(1 for _, rec in records if rec is not None),
                              "passed": good})
    ok = ok and good

# G1: the M0 negative control must show a violation in K3 and/or K5 with the injector attested.
m0_flagged = False
for kind in ("g1k3", "g1k5"):
    for rid, rec in by_class.get(kind, []):
        if rec and (rec.get("mechanism") == "M0"):
            v = rec.get("violations") or {}
            if v.get("v3") or v.get("v4") or v.get("violating_run"):
                m0_flagged = True
summary["m0_negative_control_violating"] = m0_flagged
ok = ok and m0_flagged

# M3 dedup index attestation on the live container stack (read-only SQL as postgres, loopback).
dedup_ok = False
try:
    proc = subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", "5433", "-U", "postgres", "-d", "wxeo", "-tAc",
         "SELECT schemaname FROM pg_indexes WHERE indexname='posting_dedup_idx' LIMIT 1"],
        capture_output=True, text=True, timeout=30)
    dedup_ok = bool((proc.stdout or "").strip())
except Exception:  # noqa: BLE001
    dedup_ok = False
summary["m3_dedup_index_attested"] = dedup_ok
ok = ok and dedup_ok

# M1-kafka ledger definition: the fault-free positive-control smoke record must be clean.
m1_rec = load("pilot-m1-kafka-f0-1k-p3-r00")
m1_clean = bool(m1_rec) and not any((m1_rec.get("violations") or {}).values())
summary["m1_kafka_ledger_clean"] = m1_clean
ok = ok and m1_clean

summary["passed"] = ok
os.makedirs("results/harness", exist_ok=True)
with open("results/harness/linux-gate-controls.json", "w", encoding="utf-8") as fh:
    json.dump(summary, fh, indent=2)
    fh.write("\n")
print("gate-controls summary: results/harness/linux-gate-controls.json")
if not ok:
    raise SystemExit("gate-controls: required evidence missing or negative control clean")
PY
      ;;
    pilot)
      "$PY" experiments/runner.py --stage pilot
      ;;
    A|B|C)
      "$PY" experiments/runner.py --stage "$PHASE"
      ;;
    analysis)
      # R10: tools/analyze.py is the single frozen analysis CLI (no --stage argument).
      "$PY" tools/analyze.py
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
