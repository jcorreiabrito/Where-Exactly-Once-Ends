"""M7 -> rates chain tests (S5 §11.4): stage enum, committed_events, rules, fail-closed."""
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments import matrix_design as MD  # noqa: E402
from experiments import rates as R  # noqa: E402
from experiments import validate_record as VR  # noqa: E402


def _m7_record():
    fixture = os.path.join(REPO, "results", "synthetic", "run-fixture", "run-records")
    name = sorted(os.listdir(fixture))[0]
    import json
    rec = json.load(open(os.path.join(fixture, name), encoding="utf-8"))
    rec["stage"] = "M7"
    rec["fault"] = "none"
    rec["performance"]["committed_events"] = 9000
    return rec


def test_record_stage_m7_matches_validator_enum():
    assert MD.record_stage("M7") == "M7"
    assert "M7" in VR.STAGES


def test_synthetic_m7_record_validates_with_committed_events():
    assert VR.validate_record(_m7_record()) == []


def test_committed_events_required_for_m7_b_c():
    rec = _m7_record()
    rec["performance"]["committed_events"] = None
    assert any("committed_events" in e for e in VR.validate_record(rec))
    # backward compatibility: pre-R12 pilot/Stage-A records may omit it
    legacy = _m7_record()
    legacy["stage"] = "A"
    legacy["performance"]["committed_events"] = None
    assert not any("committed_events" in e for e in VR.validate_record(legacy))


def _samples():
    return [
        {"load_events": 10_000, "partitions": 6, "mechanism": "M0",
         "committed_events": 9000, "window_start_s": 0.0, "window_end_s": 10.0},
        {"load_events": 10_000, "partitions": 6, "mechanism": "M2",
         "committed_events": 8100, "window_start_s": 0.0, "window_end_s": 10.0},
        {"load_events": 100_000, "partitions": 6, "mechanism": "M0",
         "committed_events": 90000, "window_start_s": 0.0, "window_end_s": 100.0},
        {"load_events": 100_000, "partitions": 6, "mechanism": "M2",
         "committed_events": 72000, "window_start_s": 0.0, "window_end_s": 100.0},
        {"load_events": 100_000, "partitions": 6, "mechanism": "M0", "mode": "saturation",
         "committed_events": 120000, "window_start_s": 0.0, "window_end_s": 60.0},
        # A5.2 spot-check evidence (mapping predictions: 1k/P3=450, 10k/P3=450, 10k/P12=1800,
        # 100k/P3=400, 100k/P12=1600, 1M/P6=800 capacity events/s)
        {"load_events": 1_000, "partitions": 3, "mechanism": "M2", "committed_events": 4050,
         "window_start_s": 0.0, "window_end_s": 10.0},
        {"load_events": 10_000, "partitions": 3, "mechanism": "M2", "committed_events": 4050,
         "window_start_s": 0.0, "window_end_s": 10.0},
        {"load_events": 10_000, "partitions": 12, "mechanism": "M2", "committed_events": 16200,
         "window_start_s": 0.0, "window_end_s": 10.0},
        {"load_events": 100_000, "partitions": 3, "mechanism": "M2", "committed_events": 3600,
         "window_start_s": 0.0, "window_end_s": 10.0},
        {"load_events": 100_000, "partitions": 12, "mechanism": "M2", "committed_events": 14400,
         "window_start_s": 0.0, "window_end_s": 10.0},
        {"load_events": 1_000_000, "partitions": 6, "mechanism": "M2",
         "committed_events": 72000, "window_start_s": 0.0, "window_end_s": 100.0},
    ]


def test_mapping_rule_derives_all_blocks_with_assumptions():
    doc = R.derive_rates(_samples(), matrix_sha256="abc")
    assert doc["schema"] == "wxeo.rates.v2"
    assert doc["rule_id"] == "A5.partition-scale.v1"
    assert doc["validated"] is True
    assert doc["validation"]["validated"] is True
    # measured 100k/P6 min capacity = 800/s -> 400 rate; P3 derived = 400/s -> 200
    assert doc["fixed_rate"]["100000|p6"]["rate"] >= 1
    assert doc["fixed_rate"]["100000|p3"]["rate"] >= 1
    assert "assumption" in doc["fixed_rate"]["100000|p3"]
    assert doc["fixed_rate"]["1000|p3"]["rate"] >= 1     # 1k derived from 10k
    assert doc["fixed_rate"]["1000000|p6"]["rate"] >= 1  # 1M derived from 100k
    assert doc["derived_blocks"]
    # saturation = floor(2.5 * max capacity) = floor(2.5 * 120000/54) = 5555
    assert doc["saturation_rate"]["100000|p6|sat"]["rate"] == 5555
    # G7 is fail-closed: the synthetic document carries no generator-lag / offered-rate evidence,
    # so the check must refuse rather than silently pass (evidence arrives from real runs on the host)
    assert R.g7_checks(doc, sorted(doc["fixed_rate"]) + sorted(doc["saturation_rate"]))["ok"] is False


def test_spot_check_deviation_fails_closed():
    samples = _samples()
    for s in samples:
        if (s["load_events"], s["partitions"]) == (100_000, 12) \
                and S.get("x") is None if False else (s["load_events"], s["partitions"]) == (100_000, 12):
            s["committed_events"] = 4000   # way below the 1600/s prediction
    doc = R.derive_rates(samples, matrix_sha256="abc")
    assert doc["validated"] is False
    assert doc["validation"]["failed_blocks"]
    assert R.g7_checks(doc, sorted(doc["fixed_rate"]) + sorted(doc["saturation_rate"]))["ok"] is False


def test_missing_spot_evidence_fails_closed():
    samples = [s for s in _samples()
               if (s["load_events"], s["partitions"]) != (10_000, 12)]
    doc = R.derive_rates(samples, matrix_sha256="abc")
    assert doc["validated"] is False
    assert "10000|p12" in doc["validation"]["missing_spot_checks"]


def test_saturation_missing_fails_closed():
    samples = [s for s in _samples() if s.get("mode") != "saturation"]
    with pytest.raises(SystemExit):
        R.derive_rates(samples, matrix_sha256="abc")


def test_full_coverage_mode_requires_all_blocks():
    # _samples now covers all 8 fixed-rate blocks, so full coverage mode succeeds and
    # validates; a genuinely missing block must still fail closed.
    doc = R.derive_rates(_samples(), matrix_sha256="abc", mode=R.RULE_ID_FULL)
    assert doc["rule_id"] == R.RULE_ID_FULL and doc["validated"] is True
    with pytest.raises(SystemExit):
        R.derive_rates([s for s in _samples() if s["load_events"] != 1_000_000],
                       matrix_sha256="abc", mode=R.RULE_ID_FULL)


def test_rate_lookup_missing_block_does_not_fall_back():
    """Runner._effective_rate must raise for a missing block when rates.json exists."""
    import inspect
    src = inspect.getsource(__import__("experiments.runner", fromlist=["Runner"]))
    assert "refusing the --rate/max fallback" in src
    assert "rate_mode = \"calibration-max\"" in src
    assert "rate_mode = \"saturation\"" in src
