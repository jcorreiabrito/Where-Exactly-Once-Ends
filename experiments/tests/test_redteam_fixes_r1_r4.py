"""Regression tests for S5 re-review findings R1-R4 (frozen matrix x injector consistency)."""

from __future__ import annotations

import csv
import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments import injectors as INJ  # noqa: E402
from experiments import matrix_design as MD  # noqa: E402
from experiments import receipts as RCPT  # noqa: E402
from experiments import runner as RN  # noqa: E402

APP_OWNED = {"K1", "K3", "K4", "K7"}
S4_OWNED = {"K2", "K5"}


def _rows():
    with open(os.path.join(REPO, "experiments", "run_order.csv"), encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _ctx(row):
    return INJ.InjectorContext(
        run_id=row["run_id"], mechanism=row["mechanism"],
        ordering_variant=row["ordering_variant"], fault=row["fault"],
        fault_level=row["fault_level"], injector_mode=row["injector_mode"],
        seed_hex=row["data_seed"], scratch_dir=REPO,
        faults_log=os.path.join(REPO, "results", "faults.log"),
        bootstrap="127.0.0.1:9091")


# ---- R1: dispatch for every frozen (fault x mode) combination ------------------------------
def test_R1_make_injector_dispatch_for_every_matrix_row():
    rows = _rows()
    assert rows, "run_order missing"
    failures = []
    for row in rows:
        try:
            inj = INJ.make_injector(_ctx(row))
        except Exception as exc:  # noqa: BLE001
            failures.append((row["run_id"], row["fault"], row["injector_mode"],
                             f"{type(exc).__name__}: {exc}"))
            continue
        fault = row["fault"]
        if row["injector_mode"] in ("none", "fixture") or fault in ("F0", "none", "ctrl"):
            assert inj is None, row["run_id"]
        elif row["injector_mode"] == "sham":
            assert inj is not None, row["run_id"]
        elif fault in APP_OWNED or fault == "F4b":
            assert isinstance(inj, INJ.FailpointWatch), (row["run_id"], type(inj))
        elif fault in S4_OWNED or fault in ("F1", "F2", "F3", "F4a", "F5", "F6"):
            assert isinstance(inj, INJ.Injector) and not isinstance(inj, INJ.FailpointWatch)
    assert not failures, failures[:5]


# ---- R3: matrix injector_mode consistency + validator agreement ---------------------------
def test_R3_matrix_injector_modes_are_correct():
    for row in _rows():
        fault, mode = row["fault"], row["injector_mode"]
        if row["stage_label"] == "V":
            continue
        if fault in APP_OWNED:
            assert mode == "failpoint", (row["run_id"], fault, mode)
        elif fault in S4_OWNED:
            assert mode == "external", (row["run_id"], fault, mode)
        elif fault == "F4b":
            # app-owned ordering probe; Stage B rows are "live", Stage-A K4 is a failpoint
            assert mode in ("live", "failpoint"), (row["run_id"], fault, mode)
        elif fault in ("F1", "F2", "F3", "F4a", "F5", "F6"):
            assert mode == "live", (row["run_id"], fault, mode)
        elif fault in ("F0", "none", "ctrl"):
            assert mode == "none", (row["run_id"], fault, mode)
        else:
            raise AssertionError((row["run_id"], fault))


def test_R3_counts_unchanged_after_mode_fix():
    import yaml
    doc = yaml.safe_load(open(os.path.join(REPO, "experiments", "matrix.yaml"), encoding="utf-8"))
    t = doc["accounting"]["totals"]
    assert t["runs_planned"] == 7506
    assert t["runs_executable"] == 7488
    assert t["validation_runs"] == 26
    assert t["nominal_total_executable_plus_validation"] == 7514


# ---- R2: single-owner receipt protocol ----------------------------------------------------
def _receipt(event, fault, source=None, **kw):
    r = {"ts_utc": "2026-11-24T00:00:00.000Z", "run_id": "rid", "fault": fault,
         "event": event, "detail": ""}
    if source:
        r["source"] = source
    r.update(kw)
    return r


def test_R2_app_owned_crash_accepts_app_fired_plus_s4_restored():
    recs = [_receipt("fired", "after_effect_commit"),  # app receipt: no `source` field
            _receipt("restored", "K3", source="s4")]
    out = RCPT.validate_run_receipts(recs, run_id="rid", injector_mode="failpoint",
                                     fault="K3", fault_level="L1",
                                     failpoint="after_effect_commit", action="crash")
    assert out["attested"] is True, out
    assert out["app_fired"] == 1 and out["s4_fired"] == 0


def test_R2_app_owned_rejects_s4_fired_and_duplicate_restored():
    recs = [_receipt("fired", "after_effect_commit"),
            _receipt("fired", "K3", source="s4"),
            _receipt("restored", "K3", source="s4")]
    out = RCPT.validate_run_receipts(recs, run_id="rid", injector_mode="failpoint",
                                     fault="K3", fault_level="L1",
                                     failpoint="after_effect_commit", action="crash")
    assert out["attested"] is False


def test_R2_app_owned_non_terminal_accepts_app_pair_only():
    recs = [_receipt("fired", "before_effect_commit"),
            _receipt("restored", "before_effect_commit")]
    ok = RCPT.validate_run_receipts(recs, run_id="rid", injector_mode="failpoint",
                                    fault="K4", fault_level="L1",
                                    failpoint="before_effect_commit", action="pause:200")
    assert ok["attested"] is True, ok
    dup = recs + [_receipt("restored", "K4", source="s4")]
    assert RCPT.validate_run_receipts(dup, run_id="rid", injector_mode="failpoint",
                                      fault="K4", fault_level="L1",
                                      failpoint="before_effect_commit",
                                      action="pause:200")["attested"] is False


def test_R2_s4_owned_run_rejects_app_receipts_and_requires_verified_phase():
    phase = {"load_start_ms": 1000, "load_end_ms": 5000, "fire_ms": 3000,
             "consumers_alive": True}
    ok = RCPT.validate_run_receipts(
        [_receipt("fired", "K2", source="s4", action_verified=True, phase=phase),
         _receipt("restored", "K2", source="s4")],
        run_id="rid", injector_mode="external", fault="K2", fault_level="L1")
    assert ok["attested"] is True, ok
    contaminated = [_receipt("fired", "K2", source="s4", action_verified=True, phase=phase),
                    _receipt("restored", "K2", source="s4"),
                    _receipt("fired", "after_effect_commit")]
    assert RCPT.validate_run_receipts(
        contaminated, run_id="rid", injector_mode="external", fault="K2",
        fault_level="L1")["attested"] is False


# ---- R4: phase window enforcement --------------------------------------------------------
def test_R4_in_window_and_out_of_window_s4_receipts():
    def phase(fire):
        return {"load_start_ms": 1000, "load_end_ms": 5000, "fire_ms": fire,
                "consumers_alive": True}
    ok = RCPT.validate_run_receipts(
        [_receipt("fired", "F1", source="s4", action_verified=True, phase=phase(3000)),
         _receipt("restored", "F1", source="s4")],
        run_id="rid", injector_mode="live", fault="F1", fault_level="L2")
    assert ok["attested"] is True, ok
    late = RCPT.validate_run_receipts(
        [_receipt("fired", "F1", source="s4", action_verified=True, phase=phase(9000)),
         _receipt("restored", "F1", source="s4")],
        run_id="rid", injector_mode="live", fault="F1", fault_level="L2")
    assert late["attested"] is False
    flagged = phase(3000)
    flagged["outside_window"] = True
    assert RCPT.validate_run_receipts(
        [_receipt("fired", "F1", source="s4", action_verified=True, phase=flagged),
         _receipt("restored", "F1", source="s4")],
        run_id="rid", injector_mode="live", fault="F1", fault_level="L2")["attested"] is False


def test_R4_runner_wires_real_load_window_and_liveness():
    src = open(os.path.join(REPO, "experiments", "runner.py"), encoding="utf-8").read()
    assert "self._inj_ctx.load_start_ms = self.timings[\"t_load_start\"]" in src
    assert "self._inj_ctx.load_end_ms = self.timings[\"t_load_end\"]" in src
    assert "def _consumers_alive" in src
    assert "window_kind" in src
    assert "real_start" in src and "real_end" in src
