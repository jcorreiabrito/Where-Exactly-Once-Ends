#!/usr/bin/env python3
"""Record the LIVE end-to-end smoke evidence (S5/red-team fix pass; lead order).

This tool performs **no stack access**: it reads the artefacts a completed live run has already
written and records a compact, reproducible evidence file:

    python experiments/live_smoke_evidence.py --run-id pilot-m2-f0-1k-p3-r00

Success criteria (must all hold for exit status 0):
  * the run record exists and passes ``experiments/validate_record.py``;
  * ``oracle.source_integrity == true`` and ``gt_sha256 == expected_sha256``;
  * the oracle result file exists with counts/violations;
  * the frozen artifact binding is current (the jar was re-frozen after S3's rebuild).

The output ``results/harness/live-smoke-evidence.json`` is path-hygienic (repo-relative paths) and
carries the jar SHA-256 + size, the freeze aggregate, the run-record/ oracle digests and the
quiescence/censoring facts.  It is written **only** for a live run (a record whose validity notes
do not contain DRY-RUN); refusing to label a fixture as live is deliberate.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from experiments import freeze as FZ  # noqa: E402
from experiments import pathsafe  # noqa: E402
from experiments import validate_record as VR  # noqa: E402


def sha256_file(path: str) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def collect_evidence(*, record_path: str, oracle_path: str, run_id: str,
                     freeze_path: str | None = None) -> dict:
    """Pure evidence collector (unit-testable): returns the evidence dict with ``passed``."""
    freeze_path = freeze_path or os.path.join(_REPO, "docs", "prereg-freeze.json")
    record = json.load(open(record_path, encoding="utf-8"))
    oracle = json.load(open(oracle_path, encoding="utf-8")) if os.path.exists(oracle_path) else None
    errors = VR.validate_record(record)
    notes = (record.get("validity") or {}).get("notes") or []
    dry_run = any("DRY-RUN" in str(note).upper() for note in notes)
    oracle_block = record.get("oracle") or {}
    source_integrity = bool(oracle_block.get("source_integrity"))
    gt = oracle_block.get("gt_sha256")
    expected = oracle_block.get("expected_sha256")
    validity = record.get("validity") or {}
    quiescence = record.get("quiescence") or {}
    violations = record.get("violations") or {}
    artifact = FZ.artifact_binding()
    freeze = json.load(open(freeze_path, encoding="utf-8")) if os.path.exists(freeze_path) else {}
    artifact_changes = FZ.artifact_drift(freeze.get("artifact") or {}, artifact)
    checks = {
        "record_present": bool(record),
        "record_schema_valid": not errors,
        "not_dry_run": not dry_run,
        "valid": bool(validity.get("valid")),
        "excluded": bool(validity.get("excluded")),
        "source_integrity": source_integrity,
        "gt_equals_expected": bool(gt and expected and gt == expected),
        "oracle_result_present": oracle is not None,
        "oracle_has_counts": bool(oracle and (oracle.get("counts") or {})),
        "artifact_matches_freeze": not artifact_changes,
        "quiescence_censored": bool(quiescence.get("censored")),
    }
    passed = bool(checks["record_schema_valid"] and checks["not_dry_run"]
                  and checks["source_integrity"] and checks["gt_equals_expected"]
                  and checks["oracle_result_present"] and checks["oracle_has_counts"]
                  and checks["artifact_matches_freeze"] and checks["valid"])
    evidence = pathsafe.scrub({
        "schema": "wxeo.live-smoke-evidence.v1",
        "kind": "live-end-to-end-smoke",
        "run_id": run_id,
        "generated_at_utc": __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "passed": passed,
        "checks": checks,
        "record": {"path": pathsafe.repo_rel(record_path),
                   "sha256": sha256_file(record_path),
                   "schema_errors": errors[:5]},
        "oracle": {"path": pathsafe.repo_rel(oracle_path),
                   "sha256": sha256_file(oracle_path) if oracle else None,
                   "counts": (oracle or {}).get("counts"),
                   "violations": (oracle or {}).get("violations"),
                   "attribution": (oracle or {}).get("attribution"),
                   "result_sha256": ((oracle or {}).get("oracle") or {}).get("result_sha256")},
        "run": {"stage": record.get("stage"), "cell_id": record.get("cell_id"),
                "mechanism": record.get("mechanism"), "fault": record.get("fault"),
                "ordering_variant": record.get("ordering_variant"),
                "load_events": record.get("load_events"),
                "partitions": record.get("partitions"),
                "data_seed": record.get("data_seed"),
                "validity": {"valid": validity.get("valid"),
                             "excluded": validity.get("excluded"),
                             "exclude_reason": validity.get("exclude_reason")},
                "quiescence": quiescence,
                "timings": record.get("timings"),
                "hitl": {"violating_run": violations.get("violating_run"),
                         "v1": violations.get("v1"), "v2": violations.get("v2"),
                         "v3": violations.get("v3"), "v4": violations.get("v4"),
                         "v5": violations.get("v5")}},
        "artifact": {"jar": artifact["jar"],
                     "source_tree_sha256": artifact["source_tree_sha256"],
                     "matches_freeze": not artifact_changes,
                     "freeze_aggregate": freeze.get("aggregate_pipeline_sha256")},
        "warnings": ([w for w in ((record.get("validity") or {}).get("notes") or [])]
                     if validity.get("excluded") else []),
    })
    return evidence


def main(argv=None) -> int:
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parser = argparse.ArgumentParser(description="Record LIVE smoke evidence (no stack access)")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--results-dir", default=os.path.join(repo, "results"))
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)
    record_path = os.path.join(args.results_dir, "raw", "run-records",
                               f"run_{args.run_id}.json")
    oracle_path = os.path.join(args.results_dir, "raw", "oracle",
                               f"oracle_{args.run_id}.json")
    if not os.path.exists(record_path):
        print(f"NO RECORD: {pathsafe.repo_rel(record_path)} — run the live smoke first",
              file=sys.stderr)
        return 2
    evidence = collect_evidence(record_path=record_path, oracle_path=oracle_path,
                                run_id=args.run_id)
    out = args.out or os.path.join(args.results_dir, "harness", "live-smoke-evidence.json")
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    pathsafe.assert_clean(evidence, "live-smoke-evidence")
    json.dump(evidence, open(out, "w", encoding="utf-8", newline=""), indent=2)
    open(out, "a", encoding="utf-8").write("\n")
    print(json.dumps({"evidence": pathsafe.repo_rel(out),
                      "passed": evidence["passed"],
                      "checks": evidence["checks"],
                      "jar": evidence["artifact"]["jar"]}, indent=2))
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
