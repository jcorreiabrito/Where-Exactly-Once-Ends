"""Unit tests for the Windows-KRaft-safe topic lifecycle (lead directive M5).

No live stack is touched: these tests exercise the pure cadence/configuration helpers, verify
that the runner never deletes topics, and prove the run-record lifecycle fields remain
schema-valid.
"""

from __future__ import annotations

import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments import runner as RN  # noqa: E402
from experiments import validate_record as VR  # noqa: E402


def test_no_per_run_topic_deletion_in_runner_source():
    src = open(os.path.join(REPO, "experiments", "runner.py"), encoding="utf-8").read()
    assert ".delete_topic(" not in src, "runner must never delete topics (M5)"
    assert "delete_topics(" not in src, "runner must never delete topics (M5)"
    assert "--delete" not in src, "runner must never call a topic-delete CLI (M5)"
    assert "reset-kafka" in src
    assert "reset_cadence_for" in src
    assert "MAX_LIVE_TOPICS" in src


def test_reset_cadence_by_load():
    assert RN.reset_cadence_for(1_000) == 50
    assert RN.reset_cadence_for(10_000) == 25
    assert RN.reset_cadence_for(100_000) == 20
    assert RN.reset_cadence_for(1_000_000) == 1
    assert RN.reset_cadence_for(100_000, "saturation") == 20
    assert RN.reset_cadence_for(5_000) == 50


def test_should_periodic_reset_cadence_and_topic_cap():
    due, reason = RN.should_periodic_reset(49, 1_000, live_topics=10)
    assert due is False and reason is None
    due, reason = RN.should_periodic_reset(50, 1_000, live_topics=10)
    assert (due, reason) == (True, "cadence")
    due, reason = RN.should_periodic_reset(1, 1_000, live_topics=RN.MAX_LIVE_TOPICS + 1)
    assert (due, reason) == (True, "topic_cap")
    due, reason = RN.should_periodic_reset(0, 1_000_000, live_topics=2)
    assert (due, reason) == (True, "cadence")


def test_broker_safety_properties_parser():
    good = ("\n".join([
        "broker.id=1", "delete.topic.enable=false",
        "log.retention.check.interval.ms=3600000", "log.dirs=/tmp/kafka"]) + "\n")
    assert RN.check_broker_properties(good)["ok"] is True
    missing = RN.check_broker_properties("broker.id=1\n")
    assert missing["ok"] is False
    assert set(missing["missing"]) == set(RN.BROKER_REQUIRED_PROPERTIES)
    wrong = RN.check_broker_properties(
        "delete.topic.enable=true\nlog.retention.check.interval.ms=300000\n")
    assert wrong["ok"] is False
    assert len(wrong["wrong"]) == 2


def test_runner_has_liveness_and_reset_machinery():
    for name in ("_ensure_stack_alive", "_perform_reset", "_stack_liveness",
                 "_maybe_periodic_reset", "_live_topic_count", "_disk_footprint",
                 "_lifecycle_block"):
        assert hasattr(RN.Runner, name), f"missing runner lifecycle method {name}"


def test_lifecycle_block_contains_required_record_fields():
    runner = RN.Runner(RN.RunnerOptions(results_dir=os.path.join(REPO, "results")))
    block = runner._lifecycle_block(None, {"restart_before_run": True, "block_id": 3,
                                           "reset_epoch": 2, "cadence": 50,
                                           "runs_since_reset_before": 50,
                                           "topic_count_before": 120})
    for key in ("attempt", "restart_before_run", "block_id", "reset_epoch",
                "reset_cadence", "topic_count_before", "topic_lifecycle",
                "max_live_topics", "disk_free_start_gb"):
        assert key in block, key
    assert block["restart_before_run"] is True and block["block_id"] == 3


def test_run_record_with_lifecycle_fields_is_schema_valid():
    fixture_dir = os.path.join(REPO, "results", "synthetic", "run-fixture", "run-records")
    if not os.path.isdir(fixture_dir):
        pytest.skip("synthetic fixture missing")
    name = sorted(os.listdir(fixture_dir))[0]
    record = json.load(open(os.path.join(fixture_dir, name), encoding="utf-8"))
    record["config"]["runner"] = {
        "attempt": 1, "topic_suffix": "", "source_topic": "wxeo.src.x",
        "sink_topic": "wxeo.sink.x", "db_schema": "wxeo_run_x",
        "restart_before_run": False, "block_id": 0, "reset_epoch": 0,
        "runs_since_reset_before": 0, "reset_cadence": 50, "reset_reason": None,
        "topic_count_before": 4, "topic_count_after": 4,
        "topic_lifecycle": "no per-run deletion (M5); periodic reset-kafka wipe",
        "max_live_topics": 400, "disk_free_start_gb": 10.0,
        "disk_free_end_gb": 9.9, "scratch_bytes": 123456,
    }
    assert VR.validate_record(record) == []


def test_M5_registered_in_deviations_register():
    from experiments import deviations as DEV

    ids = {item["id"] for item in DEV.INTERPRETATIONS}
    assert "M5" in ids
    m5 = next(i for i in DEV.INTERPRETATIONS if i["id"] == "M5")
    assert "reset-kafka" in m5["decision"]
    assert "1k" in m5["decision"] and "50" in m5["decision"]
