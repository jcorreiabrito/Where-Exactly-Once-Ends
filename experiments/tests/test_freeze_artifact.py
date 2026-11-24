"""Freeze artifact-binding + live-smoke evidence collector tests (no stack access)."""

from __future__ import annotations

import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments import freeze as FZ  # noqa: E402
from experiments import live_smoke_evidence as LSE  # noqa: E402


def test_freeze_document_binds_artifact_jar_and_source_tree():
    doc = FZ.build_freeze_document()
    artifact = doc.get("artifact") or {}
    assert artifact.get("jar", {}).get("path") == FZ.ARTIFACT_JAR
    assert isinstance(artifact.get("source_tree_sha256"), str)
    assert len(artifact["source_tree_sha256"]) == 64
    assert artifact.get("source_files_hashed", 0) > 0


def test_artifact_drift_detection():
    current = FZ.artifact_binding()
    frozen_ok = {"jar": dict(current["jar"]),
                 "source_tree_sha256": current["source_tree_sha256"]}
    assert FZ.artifact_drift(frozen_ok, current) == []
    tampered_jar = {"jar": {"path": FZ.ARTIFACT_JAR, "sha256": "0" * 64},
                    "source_tree_sha256": current["source_tree_sha256"]}
    drift = FZ.artifact_drift(tampered_jar, current)
    assert drift and drift[0]["kind"] == "artifact_jar"
    tampered_tree = {"jar": dict(current["jar"]), "source_tree_sha256": "0" * 64}
    assert FZ.artifact_drift(tampered_tree, current)[0]["kind"] == "artifact_source_tree"
    assert FZ.artifact_drift(None, current)[0]["problem"].startswith("freeze has no artifact")


def test_live_smoke_collector_reads_a_completed_run(tmp_path):
    """Uses the committed synthetic dry-run artefacts as a *structural* fixture only."""
    rec_dir = os.path.join(REPO, "results", "dryrun", "raw", "run-records")
    oracle_dir = os.path.join(REPO, "results", "dryrun", "raw", "oracle")
    if not os.path.isdir(rec_dir):
        pytest.skip("dry-run fixture missing")
    names = [n for n in sorted(os.listdir(rec_dir)) if n.endswith(".json")]
    record_path = os.path.join(rec_dir, names[0])
    run_id = names[0][len("run_"):-len(".json")]
    oracle_path = os.path.join(oracle_dir, f"oracle_{run_id}.json")
    evidence = LSE.collect_evidence(record_path=record_path, oracle_path=oracle_path,
                                   run_id=run_id)
    assert evidence["schema"] == "wxeo.live-smoke-evidence.v1"
    assert evidence["checks"]["oracle_result_present"] is True
    assert evidence["checks"]["source_integrity"] is True
    # a DRY-RUN record must never be accepted as live evidence
    assert evidence["checks"]["not_dry_run"] is False
    assert evidence["passed"] is False
    json.dumps(evidence)  # serialisable


def test_live_smoke_cli_refuses_missing_record(tmp_path):
    rc = LSE.main(["--run-id", "does-not-exist", "--results-dir", str(tmp_path)])
    assert rc == 2
