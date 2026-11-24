"""P1 regression (S5 §13): mechanism-committed performance basis and A6 evidence gates.

These tests are pure: no Kafka/DB/stack access. They pin the measurement contract that the paper's
throughput claims and the rate calibration depend on.
"""

from __future__ import annotations

import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments import rates as R  # noqa: E402
from experiments import runner  # noqa: E402
from experiments import matrix_design as MD  # noqa: E402


def _app_doc(run_id, instance, total, steady, start, end):
    return {
        "schema": "wxeo.app.v1",
        "run_id": run_id,
        "instance_id": instance,
        "performance": {
            "committed_events_total": total,
            "steady_state_events": steady,
            "steady_state_start_ms": start,
            "steady_state_end_ms": end,
            "steady_state_duration_ms": (end - start) if start and end else 0,
            "throughput_eps": steady / ((end - start) / 1000.0) if end > start else 0.0,
            "warmup_events_excluded": 2000,
        },
    }


def _fake_sequence(tmp_path, run_id="m7-m0-cal-100k-p6-r00", instances=2):
    seq = runner.LiveSequence.__new__(runner.LiveSequence)
    outputs = tmp_path / "outputs"
    outputs.mkdir(parents=True, exist_ok=True)
    per_instance = []
    for i in range(1, instances + 1):
        doc = _app_doc(run_id, i, total=50_000, steady=45_000,
                       start=1_000 + i, end=1_000 + i + 45_000)
        (outputs / f"app_{run_id}_{i}.json").write_text(json.dumps(doc), encoding="utf-8")
        per_instance.append(doc)
    seq.p = type("P", (), {"outputs": str(outputs),
                           "produced": str(tmp_path / "produced.ndjson")})()
    seq.run_id = run_id
    seq.run = {"run_id": run_id, "mechanism": "M0", "consumer_instances": instances,
               "load_events": 100_000}
    seq.loader_result = None
    seq.quiescence = {"censored": False, "t_consistent_ms": 1234}
    seq.r = type("R", (), {"opts": type("O", (), {"warmup_events": None})()})()
    return seq


def test_performance_block_uses_app_committed_basis(tmp_path):
    seq = _fake_sequence(tmp_path)
    perf = seq._performance_block()
    assert perf["committed_basis"] == "app-committed"
    assert perf["committed_events"] == 90_000  # sum of steady-state committed events
    assert perf["committed_events_total"] == 100_000
    assert perf["n_instances_merged"] == 2
    # aggregate throughput = sum(events) / (max(end) - min(start)); never from producer acks
    assert perf["throughput_eps_median"] == pytest.approx(90_000 / 45.001, rel=1e-6)
    assert len(perf["app_blocks"]) == 2
    assert "producer_ack_throughput_eps" not in perf


def test_rates_capacity_rejects_producer_ack_basis():
    ack_based = {
        "stage": "M7", "run_id": "m7-m0-cal-100k-p6-r00", "mechanism": "M0",
        "load_events": 100_000, "partitions": 6,
        "performance": {"committed_events": 90_000,
                        "committed_basis": "producer-acks",
                        "throughput_eps_median": 1000.0,
                        "perf_window": {"start": 0, "end": 90_000}},
        "quiescence": {"censored": False},
        "exit_status": {"load_generator": 0},
        "timings": {"t_load_start": 0, "t_load_end": 90_000},
        "config": {"runner": {"rate_mode": "calibration-max"}},
    }
    assert R._load_records_from_list([ack_based]) == []
    assert R.committed_throughput_eps(ack_based) is None


def test_rates_capacity_rejects_censored_or_loader_failure():
    base = {
        "stage": "M7", "run_id": "m7-m0-cal-100k-p6-r00", "mechanism": "M0",
        "load_events": 100_000, "partitions": 6,
        "performance": {"committed_events": 90_000, "committed_basis": "app-committed",
                        "throughput_eps_median": 1000.0,
                        "perf_window": {"start": 0, "end": 90_000}},
        "quiescence": {"censored": False}, "exit_status": {"load_generator": 0},
        "timings": {"t_load_start": 0, "t_load_end": 90_000},
        "config": {"runner": {"rate_mode": "calibration-max"}},
    }
    censored = json.loads(json.dumps(base))
    censored["quiescence"]["censored"] = True
    failed = json.loads(json.dumps(base))
    failed["exit_status"]["load_generator"] = 1
    assert R._load_records_from_list([censored]) == []
    assert R._load_records_from_list([failed]) == []
    assert len(R._load_records_from_list([base])) == 1


M7SAT_CAPACITY = 120_000 / 54.0


def _m7sat(mech, *, censored=False, loader_exit=0, throughput=M7SAT_CAPACITY, basis="app-committed"):
    return {
        "stage": "M7", "run_id": f"m7sat-{MD.MECH_TOKEN[mech]}-cal-100k-p6-r00",
        "mechanism": mech, "load_events": 100_000, "partitions": 6,
        "cell_id": f"M7.{mech}.cal.100k.p6.sat",
        "validity": {"valid": True},
        "quiescence": {"censored": censored},
        "exit_status": {"consumer": 0, "load_generator": loader_exit},
        "config": {"runner": {"rate_mode": "saturation-validation", "offered_rate": 5555}},
        "performance": {"throughput_eps_median": throughput,
                        "committed_events": 90_000,
                        "committed_events_total": 100_000,
                        "committed_basis": basis,
                        "perf_window": {"start": 0, "end": 36_000, "events": 90_000},
                        "generator_lag_p99_ms": 1, "generator_lag_flag": False},
        "timings": {"t_load_start": 0, "t_load_end": 36_000},
    }


def test_a6_rejects_censored_loader_failure_and_ack_basis():
    rates_doc = {"saturation_rate": {R.SATURATION_VALIDATION_BLOCK: {"rate": 5555}},
                 "saturation_capacity_by_mechanism": {m: M7SAT_CAPACITY for m in MD.MECHANISMS}}
    records = [_m7sat(m) for m in MD.MECHANISMS]
    assert R.saturation_validation_report(records, rates_doc)["validated"] is True
    for mutated, needle in (
        (_m7sat("M0", censored=True), "censored"),
        (_m7sat("M0", loader_exit=1), "load-generator"),
        (_m7sat("M0", basis="producer-acks"), "performance basis"),
        (_m7sat("M0", throughput=M7SAT_CAPACITY * 0.5), "throughput"),
    ):
        bad = [_m7sat(m) for m in MD.MECHANISMS]
        bad[0] = mutated
        report = R.saturation_validation_report(bad, rates_doc)
        assert report["validated"] is False, needle
        assert any(needle.lower() in p.lower() for p in report["problems"]), (
            needle, report["problems"])


def test_per_instance_warmup_splits_the_frozen_per_run_value():
    # Q4: C=2 must split max(2000,10%) instead of applying it to each instance
    assert runner.per_instance_warmup(2000, 2) == 1000
    assert runner.per_instance_warmup(2000, 1) == 2000
    assert runner.per_instance_warmup(1000, 3) == 334
    assert runner.per_instance_warmup(0, 2) == 1


def test_partial_crash_run_is_not_a_comparable_throughput_measurement(tmp_path):
    # Q5: crash/partial runs keep the per-instance blocks but must not publish a throughput that
    # looks like a complete mechanism measurement.
    seq = _fake_sequence(tmp_path, instances=2)
    os.remove(os.path.join(seq.p.outputs, f"app_{seq.run_id}_2.json"))
    perf = seq._performance_block()
    assert perf["committed_basis"] == "app-committed-partial"
    assert perf["partial"] is True
    assert perf["instances_expected"] == 2
    assert perf["n_instances_merged"] == 1
    assert perf.get("throughput_eps_median") is None
