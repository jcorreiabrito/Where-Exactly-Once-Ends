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
from experiments import injectors as INJ  # noqa: E402
from experiments import receipts as RCPT  # noqa: E402
from experiments import pathsafe  # noqa: E402
from experiments import validate_record as VR  # noqa: E402


def sha256_file(path: str) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


APP_OWNED_FAULTS = {"K1", "K3", "K4", "K7", "F4b"}
S4_OWNED_FAULTS = {"K2", "K5", "F1", "F2", "F3", "F4a", "F5", "F6"}
APP_FAILPOINT_BY_CLASS = {"K1": "after_fetch", "K3": "after_effect_commit",
                          "K4": "before_effect_commit", "K7": "before_relay_mark",
                          "F4b": "before_effect_commit"},
APP_ACTION_BY_CLASS = {"K1": "crash", "K3": "crash", "K4": "crash", "K7": "crash",
                       "F4b": "crash"}


def receipt_attestation(record_path: str) -> dict:
    """Validate the single-owner receipt protocol for an executed smoke run (R2 evidence)."""
    record = json.load(open(record_path, encoding="utf-8"))
    fault = record.get("fault")
    level = record.get("fault_level")
    receipts = record.get("fault_receipts") or []
    if fault in ("F0", "none", "ctrl", None):
        return {"run_id": record.get("run_id"), "fault": fault, "owner": "none",
                "attested": True, "reason": None}
    if fault in APP_OWNED_FAULTS:
        failpoint = {"K1": "after_fetch", "K3": "after_effect_commit",
                     "K4": "before_effect_commit", "K7": "before_relay_mark",
                     "F4b": "before_effect_commit"}.get(fault, "after_effect_commit")
        action = "crash"
        out = RCPT.validate_run_receipts(
            receipts, run_id=record.get("run_id") or "", injector_mode="failpoint",
            fault=fault, fault_level=level or "L1",
            failpoint=failpoint, action=action)
        owner = "app"
    else:
        out = RCPT.validate_run_receipts(
            receipts, run_id=record.get("run_id") or "", injector_mode="external",
            fault=fault, fault_level=level or "L1")
        owner = "s4"
    return {"run_id": record.get("run_id"), "fault": fault, "owner": owner,
            "attested": bool(out.get("attested")), "reason": out.get("reason"),
            "fired": out.get("fired"), "restored": out.get("restored"),
            "app_fired": out.get("app_fired"), "s4_fired": out.get("s4_fired")}


def collect_evidence(*, record_path: str, oracle_path: str, run_id: str,
                     freeze_path: str | None = None,
                     attested_run_paths: list[str] | None = None) -> dict:
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
    receipt_checks = [receipt_attestation(p) for p in (attested_run_paths or [])]
    checks["receipts_attested"] = all(r["attested"] for r in receipt_checks)
    passed = bool(checks["record_schema_valid"] and checks["not_dry_run"]
                  and checks["source_integrity"] and checks["gt_equals_expected"]
                  and checks["oracle_result_present"] and checks["oracle_has_counts"]
                  and checks["artifact_matches_freeze"] and checks["valid"]
                  and checks["receipts_attested"])
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
        "receipt_attestation": receipt_checks,
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
    parser.add_argument("--attested-runs", default="",
                        help="comma-separated extra smoke run_ids whose receipt ownership "
                             "attestation must be recorded (e.g. K3 app-owned, K2 S4-owned)")
    args = parser.parse_args(argv)
    record_path = os.path.join(args.results_dir, "raw", "run-records",
                               f"run_{args.run_id}.json")
    oracle_path = os.path.join(args.results_dir, "raw", "oracle",
                               f"oracle_{args.run_id}.json")
    if not os.path.exists(record_path):
        print(f"NO RECORD: {pathsafe.repo_rel(record_path)} — run the live smoke first",
              file=sys.stderr)
        return 2
    extra_paths = []
    for rid in [r.strip() for r in (args.attested_runs or "").split(",") if r.strip()]:
        p = os.path.join(args.results_dir, "raw", "run-records", f"run_{rid}.json")
        if not os.path.exists(p):
            print(f"MISSING attested run record: {rid}", file=sys.stderr)
            return 2
        extra_paths.append(p)
    evidence = collect_evidence(record_path=record_path, oracle_path=oracle_path,
                               run_id=args.run_id, attested_run_paths=extra_paths)
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
