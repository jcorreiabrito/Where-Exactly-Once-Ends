#!/usr/bin/env python3
"""Write / verify ``docs/prereg-freeze.json`` — the analysis-code freeze (prereg §10).

Before the first Stage A run the whole analysis pipeline must be committed and hashed:

  * the independent oracle (derivation, predicates, mutation harness) and the G5 vectors;
  * the matrix/run-order generators and their generated artefacts;
  * the run-record validator, runner, injectors and deviations generator;
  * the frozen statistics and figure generators (``tools/analyze.py``,
    ``tools/make-figures.py``);
  * the seeds and their generation procedure;
  * the pre-freeze harness evidence (G3 mutation result, G5 cross-implementation digests).

The freeze file records SHA-256 of each file, the git SHA, the UTC timestamp and the seed
procedure.  After the freeze, ANY change to a hashed file requires a ``results/DEVIATIONS.md``
entry (prereg §10.3); ``--check`` reports drift so this cannot happen silently.

Usage::

    python experiments/freeze.py --write     # write the freeze (refuses to overwrite)
    python experiments/freeze.py --check     # verify current tree against the freeze
    python experiments/freeze.py --print     # print the freeze document without writing
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sys
from datetime import datetime, timezone

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FREEZE_PATH = os.path.join(_REPO, "docs", "prereg-freeze.json")

#: Files frozen by the freeze (relative to the repo root).
FROZEN_FILES = [
    "experiments/oracle/__init__.py",
    "experiments/oracle/derive.py",
    "experiments/oracle/oracle.py",
    "experiments/oracle/mutation_test.py",
    "experiments/oracle/g5-vectors.json",
    "experiments/pathsafe.py",
    "experiments/receipts.py",
    "experiments/kafka_admin.py",
    "experiments/gate_negative_controls.py",
    "experiments/live_smoke_evidence.py",
    "experiments/fixtures/__init__.py",
    "experiments/fixtures/make_fixtures.py",
    "experiments/fixtures/build_dryrun_fixture.py",
    "experiments/fixtures/make_synthetic_analysis_fixture.py",
    "experiments/matrix_design.py",
    "experiments/matrix.py",
    "experiments/matrix.yaml",
    "experiments/run_order.csv",
    "experiments/matrix.sha256",
    "experiments/injectors.py",
    "experiments/runner.py",
    "experiments/validate_record.py",
    "experiments/deviations.py",
    "experiments/freeze.py",
    "experiments/run-matrix.ps1",
    "experiments/tests/test_oracle.py",
    "experiments/tests/test_redteam_regressions.py",
    "experiments/tests/test_runner_lifecycle.py",
    "experiments/tests/test_freeze_artifact.py",
    "experiments/tests/test_redteam_fixes_r1_r4.py",
    "experiments/tests/test_redteam_fixes_r5_r9.py",
    "experiments/tests/test_kafka_admin_mapping.py",
    "tools/analyze.py",
    "tools/make-figures.py",
]

#: Provenance inputs (hash recorded, not "pipeline code").
PROVENANCE_FILES = [
    "docs/prereg.md",
    "docs/method.md",
    "docs/CONTRACTS.md",
    "docs/P2-RULINGS.md",
    "docs/derive-vectors.csv",
    "docs/P1-APPROVAL.md",
]

#: Pre-freeze harness evidence whose hash is recorded (not part of the pipeline).
EVIDENCE_FILES = [
    "results/harness/g3-mutation.json",
    "results/harness/gate-negative-controls.json",
]

#: Artifact binding: the measured service is the shipped jar; the freeze records the jar digest
#: and a deterministic source-tree digest so a rebuild is always visible (S5 F-P6 / lead order).
ARTIFACT_JAR = "artifact/target/wxeo-service.jar"
ARTIFACT_SOURCE_GLOBS = [
    "artifact/pom.xml",
    "artifact/README.md",
    "artifact/src/**/*.java",
    "artifact/src/**/*.properties",
    "artifact/src/**/*.yml",
    "artifact/harness/*.ps1",
    "artifact/harness/*.json",
]

ANALYSIS_OUTPUT_CONTRACT = [
    "results/analysis/macros.tex",
    "results/analysis/tests.json",
    "results/analysis/decision-table.csv",
    "results/analysis/correctness-by-cell.csv",
    "results/analysis/correctness-by-mechanism-fault.csv",
    "results/analysis/counts-by-mechanism.csv",
    "results/analysis/performance-by-cell.csv",
    "results/analysis/per-run-performance.csv",
    "results/analysis/tables/t1-validation.tex",
    "results/analysis/tables/t2-correctness.tex",
    "results/analysis/tables/t3-counts.tex",
    "results/analysis/tables/t4-performance.tex",
    "results/analysis/tables/t5-tlc.tex",
    "results/analysis/tables/t6-decision.tex",
    "figures/fig-correctness.pdf",
    "figures/fig-latency-ecdf.pdf",
    "figures/fig-cost-coverage.pdf",
    "figures/fig-throughput.pdf",
]


def artifact_source_files() -> list[str]:
    """Deterministic list of artifact source/config files (excludes build outputs)."""
    import glob as _glob
    files: list[str] = []
    for pattern in ARTIFACT_SOURCE_GLOBS:
        files.extend(_glob.glob(os.path.join(_REPO, pattern), recursive=True))
    return sorted(set(files))


def artifact_binding() -> dict:
    """Jar digest + deterministic source-tree digest (the measured artifact identity)."""
    jar_path = os.path.join(_REPO, ARTIFACT_JAR)
    jar = {"path": ARTIFACT_JAR, "sha256": None, "bytes": None}
    if os.path.exists(jar_path):
        jar["sha256"] = sha256_file(jar_path)
        jar["bytes"] = os.path.getsize(jar_path)
    tree_hasher = hashlib.sha256()
    files = artifact_source_files()
    for path in files:
        rel = os.path.relpath(path, _REPO).replace(os.sep, "/")
        tree_hasher.update(rel.encode("utf-8"))
        tree_hasher.update(b"\0")
        tree_hasher.update(sha256_file(path).encode("ascii"))
        tree_hasher.update(b"\n")
    return {"jar": jar, "source_tree_sha256": tree_hasher.hexdigest(),
            "source_files_hashed": len(files),
            "note": "jar rebuilt by S3 after service fixes is bound here and verified by the "
                    "runner preflight; a changed jar/source tree shows up as freeze drift and "
                    "must be re-frozen before a live run"}


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def git_sha() -> str:
    head = os.path.join(_REPO, ".git", "HEAD")
    try:
        text = open(head, encoding="utf-8").read().strip()
    except OSError:
        return "0000000"
    if text.startswith("ref: "):
        ref = text[5:].strip()
        ref_path = os.path.join(_REPO, ".git", *ref.split("/"))
        if os.path.exists(ref_path):
            return open(ref_path, encoding="utf-8").read().strip()
        packed = os.path.join(_REPO, ".git", "packed-refs")
        if os.path.exists(packed):
            for line in open(packed, encoding="utf-8"):
                parts = line.split()
                if len(parts) == 2 and parts[1] == ref:
                    return parts[0]
    return text if len(text) >= 7 else "0000000"


def seed_constants() -> dict:
    """Import the matrix module's frozen seed constants without regenerating the matrix."""
    sys.path.insert(0, _REPO)
    import importlib

    mod = importlib.import_module("experiments.matrix")
    return {
        "master_seed": mod.MASTER_SEED,
        "order_seed": mod.ORDER_SEED,
        "bootstrap_seed": mod.BOOTSTRAP_SEED,
        "seed_phrases": mod.SEED_PHRASES,
        "seed_generation": "sha256(UTF-8(phrase)).hexdigest()[:32] for each phrase; the "
                           "phrases are fixed constants in experiments/matrix.py and were "
                           "generated once before any run",
        "data_seed_formula": 'first 16 hex chars of sha256(UTF-8(master_seed + "data" + '
                             'stage_token + condition_token + decimal(rep)))',
        "injection_seed_formula": 'first 16 hex chars of sha256(UTF-8(master_seed + "inj" + '
                                  'stage_token + condition_token + decimal(rep)))',
        "bootstrap_resampling": "numpy.random.default_rng(int(sha256(bootstrap_seed + "
                                "'|' + label)[:16], 16)); B = 10,000; percentile method "
                                "primary, BCa sensitivity",
    }


def build_freeze_document() -> dict:
    files = {}
    missing = []
    for rel in FROZEN_FILES:
        path = os.path.join(_REPO, rel)
        if os.path.exists(path):
            files[rel] = {"sha256": sha256_file(path), "bytes": os.path.getsize(path)}
        else:
            missing.append(rel)
    provenance = {}
    for rel in PROVENANCE_FILES:
        path = os.path.join(_REPO, rel)
        if os.path.exists(path):
            provenance[rel] = sha256_file(path)
    evidence = {}
    g3_passed = None
    for rel in EVIDENCE_FILES:
        path = os.path.join(_REPO, rel)
        if os.path.exists(path):
            evidence[rel] = sha256_file(path)
    g3_path = os.path.join(_REPO, "results", "harness", "g3-mutation.json")
    if os.path.exists(g3_path):
        try:
            doc = json.load(open(g3_path, encoding="utf-8"))
            g3_passed = bool(doc.get("passed"))
        except Exception:  # noqa: BLE001
            g3_passed = None
    aggregate = hashlib.sha256()
    for rel in sorted(files):
        aggregate.update(rel.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(files[rel]["sha256"].encode("ascii"))
    return {
        "schema": "wxeo.prereg-freeze.v1",
        "document": "docs/prereg-freeze.json",
        "purpose": "Analysis-code freeze required by docs/prereg.md §10 before the first "
                   "Stage A run: oracle, metric computation, statistics, figure generators, "
                   "run-record validator, plus the seeds and their generation procedure.",
        "frozen_at_utc": utc_now(),
        "git_sha": git_sha(),
        "host": {"platform": platform.platform(), "python": sys.version.split()[0],
                 "machine": platform.machine()},
        "seeds": seed_constants(),
        "files": files,
        "provenance_hashes": provenance,
        "pre_freeze_harness_evidence": {
            "files_sha256": evidence,
            "g3_mutation": {"sha256": evidence.get("results/harness/g3-mutation.json"),
                            "passed": g3_passed,
                            "requirement": "100% detection with correct attribution "
                                           "(docs/method.md §7.6 G3)"},
            "g5_cross_implementation": {
                "python_vectors": "experiments/oracle/g5-vectors.json",
                "java_side_confirmed": True,
                "evidence": "S3 Java GroundTruthDigest reproduced all four committed digests "
                            "byte-for-byte; normative rows docs/derive-vectors.csv are asserted "
                            "by experiments/tests/test_oracle.py (14 tests green)",
            },
        },
        "aggregate_pipeline_sha256": aggregate.hexdigest(),
        "artifact": artifact_binding(),
        "analysis_outputs_contract": ANALYSIS_OUTPUT_CONTRACT,
        "missing_at_freeze": missing,
        "freeze_policy": "Any change to a file in `files` after this freeze requires a "
                         "results/DEVIATIONS.md entry naming the file, the reason and the "
                         "affected analyses; affected results are labelled "
                         "'post-freeze analysis' (docs/prereg.md §10.3).",
    }


def evidence_drift(frozen: dict) -> list[dict]:
    """R11: gate-input evidence tampering detection (sha256 of every EVIDENCE_FILE)."""
    drift: list[dict] = []
    frozen_evidence = ((frozen.get("pre_freeze_harness_evidence") or {})
                       .get("files_sha256") or {})
    for rel, expected in frozen_evidence.items():
        path = os.path.join(_REPO, rel)
        if not os.path.exists(path):
            drift.append({"file": rel, "problem": "gate-input evidence missing"})
            continue
        current = sha256_file(path)
        if current != expected:
            drift.append({"file": rel, "problem": "gate-input evidence tampered/changed",
                          "frozen": expected, "current": current})
    return drift


def artifact_drift(frozen_artifact: dict | None, current: dict | None = None) -> list[dict]:
    """Pure comparison of the frozen artifact binding vs the current jar/source tree."""
    frozen_artifact = frozen_artifact or {}
    current = current or artifact_binding()
    drift: list[dict] = []
    if not frozen_artifact:
        return [{"file": "artifact binding", "kind": "artifact",
                 "problem": "freeze has no artifact block"}]
    frozen_jar = (frozen_artifact.get("jar") or {}).get("sha256")
    current_jar = (current.get("jar") or {}).get("sha256")
    if frozen_jar != current_jar:
        drift.append({"file": ARTIFACT_JAR, "kind": "artifact_jar",
                      "frozen": frozen_jar, "current": current_jar,
                      "problem": ("jar missing at freeze time" if not frozen_jar
                                  else "jar rebuilt/changed after the freeze")})
    frozen_tree = frozen_artifact.get("source_tree_sha256")
    current_tree = current.get("source_tree_sha256")
    if frozen_tree != current_tree:
        drift.append({"file": "artifact source tree", "kind": "artifact_source_tree",
                      "frozen": frozen_tree, "current": current_tree,
                      "problem": "artifact sources changed after the freeze"})
    return drift


def check(verbose: bool = True) -> dict:
    if not os.path.exists(FREEZE_PATH):
        return {"ok": False, "problem": f"freeze file missing: {FREEZE_PATH}"}
    with open(FREEZE_PATH, "r", encoding="utf-8") as fh:
        frozen = json.load(fh)
    drift = []
    missing = []
    for rel, info in frozen.get("files", {}).items():
        path = os.path.join(_REPO, rel)
        if not os.path.exists(path):
            missing.append(rel)
            continue
        current = sha256_file(path)
        if current != info.get("sha256"):
            drift.append({"file": rel, "frozen": info.get("sha256"), "current": current})
    # artifact binding drift (jar rebuilt / sources changed => re-freeze before a live run)
    current_artifact = artifact_binding()
    artifact_changes = artifact_drift(frozen.get("artifact") or {}, current_artifact)
    gate_evidence_changes = evidence_drift(frozen)
    report = {"ok": not (drift or missing or artifact_changes or gate_evidence_changes),
              "drift": drift, "gate_evidence_drift": gate_evidence_changes,
              "artifact_drift": artifact_changes,
              "artifact": current_artifact["jar"], "missing": missing,
              "frozen_at_utc": frozen.get("frozen_at_utc"),
              "aggregate_pipeline_sha256": frozen.get("aggregate_pipeline_sha256")}
    if verbose:
        print(json.dumps(report, indent=2))
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Write/verify the WXEO analysis freeze")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--print", dest="do_print", action="store_true")
    parser.add_argument("--force", action="store_true",
                        help="overwrite an existing freeze (requires a DEVIATIONS entry)")
    args = parser.parse_args(argv)
    if args.check:
        report = check()
        return 0 if report.get("ok") else 1
    doc = build_freeze_document()
    if args.do_print:
        print(json.dumps(doc, indent=2))
        return 0
    if not args.write:
        parser.error("one of --write/--check/--print is required")
    if doc["missing_at_freeze"]:
        print("REFUSING to freeze: missing files: " + ", ".join(doc["missing_at_freeze"]),
              file=sys.stderr)
        return 2
    if os.path.exists(FREEZE_PATH) and not args.force:
        existing = json.load(open(FREEZE_PATH, encoding="utf-8"))
        if existing.get("aggregate_pipeline_sha256") == doc["aggregate_pipeline_sha256"]:
            print("freeze already current (aggregate unchanged); nothing written")
            return 0
        print("REFUSING to overwrite a different freeze without --force "
              "(a post-freeze change is a deviation, prereg §10.3)", file=sys.stderr)
        return 2
    os.makedirs(os.path.dirname(FREEZE_PATH), exist_ok=True)
    with open(FREEZE_PATH, "w", encoding="utf-8", newline="") as fh:
        json.dump(doc, fh, indent=2)
        fh.write("\n")
    print(json.dumps({"freeze": FREEZE_PATH, "frozen_at_utc": doc["frozen_at_utc"],
                      "git_sha": doc["git_sha"],
                      "aggregate_pipeline_sha256": doc["aggregate_pipeline_sha256"],
                      "n_files": len(doc["files"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
