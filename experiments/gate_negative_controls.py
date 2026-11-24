#!/usr/bin/env python3
"""Gate falsifiability proof: purpose-built negative fixtures for each gate G1–G8 (S5 F-G1).

The S5 review's central gate objection was that most gates could only ever PASS.  This script
constructs, for **every** gate, a minimal engineered input that must make the gate FAIL, and
writes the evidence to ``results/harness/gate-negative-controls.json``.  The frozen analysis
pipeline only marks a gate PASS when this file records ``failed_as_required`` for it, so a gate
that cannot fail can never be presented as passed.

Run::

    python experiments/gate_negative_controls.py
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from experiments import pathsafe  # noqa: E402
from tools import analyze as A  # noqa: E402

REQUIRED_KEYS = list(A.COUNTS_KEYS) if hasattr(A, "COUNTS_KEYS") else []


def _record(*, run_id: str, mechanism: str, fault: str, stage: str = "A",
            cell_id: str | None = None, valid: bool = True, excluded: bool = False,
            v3: bool = False, violating: bool = False, attested: bool = True,
            source_integrity: bool = True, clock_check: bool = True,
            residue_check: bool = True, disk: float = 10.0,
            fault_level: str = "L1", receipts: list | None = None,
            counts: dict | None = None) -> dict:
    """Build a minimal extracted-analyser record (the fixture input to the gate predicates)."""
    base_counts = {k: 0 for k in (
        "produced", "consumed_estimate", "expected_postings", "observed_postings",
        "dup_postings", "dup_entries", "lost_postings", "lost_entries", "imbalanced_entries",
        "structurally_malformed_entries", "imbalanced_sum_entries", "ledger_entries_total",
        "mutated_postings", "spurious_postings")}
    if counts:
        base_counts.update(counts)
    return {
        "run_id": run_id, "stage": stage, "cell_id": cell_id or f"{stage}.{mechanism}.{fault}",
        "mechanism": mechanism, "is_validation": (cell_id or "").startswith("V."),
        "ordering_variant": "strict", "fault": fault, "fault_level": fault_level,
        "load_events": 1000, "partitions": 3, "consumer_instances": 2, "rep": 0,
        "valid": valid, "excluded": excluded, "exclude_reason": None,
        "violating_run": bool(violating), "violating_run_raw": bool(violating),
        "censored": False,
        "v1": False, "v2": False, "v3": bool(v3), "v4": bool(violating and not v3),
        "v5": False,
        **{k: base_counts[k] for k in base_counts},
        "throughput": None, "p50": None, "p99": None, "p999": None,
        "t_consistent_ms": None,
        "injected": fault not in ("F0", "none", "ctrl"),
        "attested": attested, "config_sha256": "0" * 64, "config": {},
        "source_integrity": source_integrity,
        "gt_sha256": ("a" * 64) if source_integrity else "b" * 64,
        "expected_sha256": "a" * 64,
        "clock_check": clock_check, "residue_check": residue_check,
        "disk_free_start_gb": disk, "publication": {}, "histogram_path": None,
        "notes": [], "git_sha": "0" * 40, "data_seed": "0" * 16,
        "injection_seed": "0" * 16, "generator_lag_p99_ms": None,
        "fault_receipts": receipts or [],
    }


def _gates(recs, *, harness_dir, artifacts_dir=None, rates_path=None) -> dict[str, str]:
    table = A.build_gate_table(
        recs, harness_dir, reserved=None, artifacts_dir=artifacts_dir,
        rates_path=rates_path, negative_controls_required=False)
    return {row["gate"]: row["status"] for row in table}


def main(argv=None) -> int:
    work = tempfile.mkdtemp(prefix="wxeo-gate-neg-")
    harness_dir = os.path.join(work, "harness")
    os.makedirs(harness_dir, exist_ok=True)
    artifacts = os.path.join(work, "artifacts")
    results: dict = {}

    def record(gate: str, expected: str, statuses: dict[str, str], note: str):
        status = statuses.get(gate)
        results[gate] = {
            "expected": expected,
            "observed": status,
            "failed_as_required": status == expected,
            "reason": note,
        }

    # G1: M0 K3/K5 runs present but no I3 violation -> must FAIL
    recs = [_record(run_id="a-m0-k3-1k-p3-r00", mechanism="M0", fault="K3", v3=False),
            _record(run_id="a-m0-k5-1k-p3-r00", mechanism="M0", fault="K5", v3=False)]
    record("G1", "FAIL", _gates(recs, harness_dir=harness_dir),
           "M0 K3/K5 present but produced no I3 violation")

    # G2: M1-kafka fault-free control with a violation -> must FAIL
    recs = [_record(run_id="a-m1-kafka-ctrl-1k-p3-r00", mechanism="M1-kafka", fault="none",
                    violating=True, fault_level="none")]
    record("G2", "FAIL", _gates(recs, harness_dir=harness_dir),
           "M1-kafka fault-free control shows a violation")

    # G3: mutation evidence with passed=false -> must FAIL
    g3_dir = os.path.join(work, "harness_g3")
    os.makedirs(g3_dir, exist_ok=True)
    json.dump({"passed": False, "corpora": {}, "groups": {}},
              open(os.path.join(g3_dir, "g3-mutation.json"), "w"), indent=2)
    record("G3", "FAIL", _gates([], harness_dir=g3_dir),
           "g3-mutation.json reports passed=false")

    # G4: an injected run with no receipts (not attested) -> must FAIL
    recs = [_record(run_id="a-m0-k3-1k-p3-r00", mechanism="M0", fault="K3",
                    attested=False, receipts=[])]
    record("G4", "FAIL", _gates(recs, harness_dir=harness_dir),
           "injected run with no fired/restored receipts")

    # G5: source-integrity failure on an executed run -> must FAIL
    recs = [_record(run_id="a-m1x-k3-1k-p3-r00", mechanism="M1x", fault="K3",
                    source_integrity=False)]
    record("G5", "FAIL", _gates(recs, harness_dir=harness_dir),
           "executed run with source_integrity=false")

    # G6: two mechanisms in the same cell with different shared_sha256 -> must FAIL
    os.makedirs(os.path.join(artifacts, "t6-run-a"), exist_ok=True)
    os.makedirs(os.path.join(artifacts, "t6-run-b"), exist_ok=True)
    json.dump({"shared_sha256": "a" * 64, "intrinsic_sha256": "1" * 64,
               "mechanism": "M2"},
              open(os.path.join(artifacts, "t6-run-a", "config"), "w"))
    json.dump({"shared_sha256": "c" * 64, "intrinsic_sha256": "2" * 64,
               "mechanism": "M3"},
              open(os.path.join(artifacts, "t6-run-b", "config"), "w"))
    recs = [_record(run_id="t6-run-a", mechanism="M2", fault="none",
                    cell_id="C.M2.F0.100k.p6", stage="C", fault_level="none"),
            _record(run_id="t6-run-b", mechanism="M3", fault="none",
                    cell_id="C.M2.F0.100k.p6", stage="C", fault_level="none")]
    record("G6", "FAIL", _gates(recs, harness_dir=harness_dir, artifacts_dir=artifacts),
           "two mechanisms share a cell but have different shared_sha256")

    # G7: rate table present, generator lag p99 > 5 ms -> must FAIL
    rates = os.path.join(work, "rates.json")
    json.dump({"fixed_rate": {}, "saturation_rate": 1000}, open(rates, "w"))
    r7 = _record(run_id="c-m2-f0-100k-p6-r00", mechanism="M2", fault="F0", stage="C",
                 fault_level="none")
    r7["generator_lag_p99_ms"] = 55
    record("G7", "FAIL", _gates([r7], harness_dir=harness_dir, rates_path=rates),
           "generator lag p99 = 55 ms > 5 ms flag")

    # G8: clock probe failure on an executed run -> must FAIL
    recs = [_record(run_id="a-m2-k1-1k-p3-r00", mechanism="M2", fault="K1",
                    clock_check=False)]
    record("G8", "FAIL", _gates(recs, harness_dir=harness_dir),
           "executed run with clock_check=false (wall clock stepped)")

    doc = {
        "schema": "wxeo.gate-negative-controls.v1",
        "synthetic": True,
        "purpose": "prove every harness gate can FAIL (S5 F-G1); the analysis marks a gate PASS "
                   "only when its negative control fails as required",
        "generated_by": "experiments/gate_negative_controls.py",
        "gates": results,
        "all_passed": all(v["failed_as_required"] for v in results.values()),
    }
    out = os.path.join(_REPO, "results", "harness", "gate-negative-controls.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    doc = pathsafe.scrub(doc)
    pathsafe.assert_clean(doc, "gate-negative-controls")
    json.dump(doc, open(out, "w", encoding="utf-8", newline=""), indent=2)
    print(json.dumps({"out": pathsafe.repo_rel(out),
                      "all_passed": doc["all_passed"],
                      "gates": {g: v["observed"] for g, v in results.items()}}, indent=2))
    return 0 if doc["all_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
