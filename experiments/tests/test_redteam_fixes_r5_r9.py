"""Regression tests R5-R9 from the S5 re-review (no live stack)."""
from __future__ import annotations

import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments import injectors as INJ  # noqa: E402
from experiments import receipts as RCPT  # noqa: E402
from experiments import validate_record as VR  # noqa: E402


def _rec(event, fault, source=None, detail="", **kw):
    r = {"ts_utc": "2026-11-24T00:00:00.000Z", "run_id": "rid", "fault": fault,
         "event": event, "detail": detail}
    if source:
        r["source"] = source
    r.update(kw)
    return r


# ---- R5: exactly one designated failpoint instance ----------------------------------------
def test_R5_two_app_fired_receipts_are_rejected():
    recs = [_rec("fired", "after_effect_commit", detail="instance=1"),
            _rec("fired", "after_effect_commit", detail="instance=2"),
            _rec("restored", "K3", source="s4")]
    out = RCPT.validate_run_receipts(recs, run_id="rid", injector_mode="failpoint",
                                     fault="K3", fault_level="L1",
                                     failpoint="after_effect_commit", action="crash",
                                     failpoint_instance=1)
    assert out["attested"] is False, out


def test_R5_designated_instance_is_accepted():
    recs = [_rec("fired", "after_effect_commit", detail="action=crash;instance=1"),
            _rec("restored", "K3", source="s4")]
    out = RCPT.validate_run_receipts(recs, run_id="rid", injector_mode="failpoint",
                                     fault="K3", fault_level="L1",
                                     failpoint="after_effect_commit", action="crash",
                                     failpoint_instance=1)
    assert out["attested"] is True, out
    src = open(os.path.join(REPO, "experiments", "runner.py"), encoding="utf-8").read()
    assert 'int(instance) == int(self._failpoint_instance)' in src
    assert '--wxeo.failpoint.instance=' in src
    assert '"failpoint_instance"' in src


# ---- R6: K5 marker wait ------------------------------------------------------------------
def _k5_ctx(tmp_path, probe, timeout_s=300):
    return INJ.InjectorContext(
        run_id="rid", mechanism="M0", ordering_variant="strict", fault="K5",
        fault_level="L1", injector_mode="external", seed_hex="0f0e0d0c0b0a0908",
        scratch_dir=str(tmp_path), faults_log=str(tmp_path / "faults.log"),
        bootstrap="127.0.0.1:9091", progress_probe=probe,
        stop_consumers=lambda: None, start_consumers=lambda: None,
        wait_group_stable=lambda **k: True,
        rewind_progress=lambda k: {"moved": True, "k": k},
        extra={"load_events": 1000, "marker_timeout_s": timeout_s})


def test_R6_k5_waits_for_durable_marker(tmp_path):
    state = {"value": 0}

    def probe():
        state["value"] = min(1000, state["value"] + 120)
        return state["value"]

    ctx = _k5_ctx(tmp_path, probe)
    inj = INJ.K5DuplicateDelivery(ctx)
    inj.arm()
    out = inj.fire()
    assert out["fired"] is True
    assert inj.state["progress_marker"] >= 1
    marker = inj.state["progress_marker"]
    receipts = INJ.read_receipts(ctx.faults_log)
    fired = [r for r in receipts if r["event"] == "fired"]
    assert len(fired) == 1
    assert str(marker) in json.dumps(fired[0])


def test_R6_k5_marker_timeout_is_not_fired(tmp_path):
    ctx = _k5_ctx(tmp_path, lambda: 0, timeout_s=0.2)
    inj = INJ.K5DuplicateDelivery(ctx)
    inj.arm()
    with pytest.raises(INJ.InjectionNotVerified):
        inj.fire()
    fired = [r for r in INJ.read_receipts(ctx.faults_log) if r["event"] == "fired"]
    assert fired == []


# ---- R7: F1 AdminClient drop + PowerShell restore ----------------------------------------
def test_R7_f1_uses_live_broker_count_for_verification(tmp_path):
    seq = {"n": 0}

    def probe():
        seq["n"] += 1
        return 3 if seq["n"] == 1 else 2     # first read 3, after the kill 2

    ctx = INJ.InjectorContext(
        run_id="rid", mechanism="M0", ordering_variant="strict", fault="F1",
        fault_level="L2", injector_mode="live", seed_hex="0f0e0d0c0b0a0908",
        scratch_dir=str(tmp_path), faults_log=str(tmp_path / "faults.log"),
        bootstrap="127.0.0.1:9091", broker_count_probe=probe)
    inj = INJ.F1BrokerKill(ctx)
    inj.state["target"] = {"id": 1, "pid": -1}   # -1 is not alive
    verified, detail = inj._verify({"broker_count": 3}, {})
    assert verified is True, detail
    assert detail["brokers_after"] == 2
    src = open(os.path.join(REPO, "experiments", "injectors.py"), encoding="utf-8").read()
    assert '"-File", self.ctx.native_stack_ps1, "start-kafka"' in src
    assert "KafkaAdmin" in src


# ---- R8: consumer exit codes --------------------------------------------------------------
def _synthetic_record():
    d = os.path.join(REPO, "results", "synthetic", "run-fixture", "run-records")
    if not os.path.isdir(d):
        pytest.skip("synthetic fixture missing")
    name = sorted(n for n in os.listdir(d) if n.endswith(".json"))[0]
    return json.load(open(os.path.join(d, name), encoding="utf-8"))


def test_R8_consumer_exit_codes_present_and_enforced():
    record = _synthetic_record()
    record["exit_status"] = {"consumer": {"1": 0, "2": 0}, "load_generator": 0}
    assert VR.validate_record(record) == []
    broken = _synthetic_record()
    broken["exit_status"] = {"load_generator": 0}
    assert any("exit_status" in e for e in VR.validate_record(broken))
    src = open(os.path.join(REPO, "experiments", "runner.py"), encoding="utf-8").read()
    assert "self._consumer_exit = {" in src
    assert src.index("self._consumer_exit = {") < src.index("self.processes.clear()")


# ---- R9: A2 §3.4 equations ----------------------------------------------------------------
def test_R9_positive_record_passes():
    assert VR.validate_record(_synthetic_record()) == []


@pytest.mark.parametrize("mutate,needle", [
    (lambda r: (r["violations"].update({"v3": True, "violating_run": False}), r)[1],
     "v3=true requires violating_run=true"),
    (lambda r: (r["counts"].update({"structurally_malformed_entries": 1}),
                r["violations"].update({"v1": False}), r)[2],
     "structurally_malformed_entries>0 requires v1=true"),
    (lambda r: (r["violations"].update({"v1": False, "v2": False, "v3": False,
                                       "v4": False, "v5": False, "violating_run": True}), r)[1],
     "violating_run must equal"),
    (lambda r: (r["counts"].update({"structurally_malformed_entries": 0,
                                    "imbalanced_sum_entries": 0}),
                r["violations"].update({"v1": True}), r)[2],
     "v1 must equal"),
])
def test_R9_negative_records_rejected(mutate, needle):
    record = _synthetic_record()
    record = mutate(record)
    errors = VR.validate_record(record)
    assert any(needle in e for e in errors), errors
