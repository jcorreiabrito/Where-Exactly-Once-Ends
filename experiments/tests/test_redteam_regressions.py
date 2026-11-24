"""Red-team regression tests — one test per code-level finding from docs/redteam-oracle.md.

These tests were written **before** the fix and failed on the reviewed snapshot; they encode the
required behaviour so the same defects cannot silently return.  Run with::

    python -m pytest experiments/tests/test_redteam_regressions.py -q

Finding map: F-O1/O2/O3/O4/O5/O7, F-I1/I2/I3/I4/I5, F-E1/E2/E4, F-G2, F-P4/P5,
F-RN1/RN3, path hygiene (lead P0 addendum).  F-P1 (author-level rules) and artifact-side
findings (F-M2) are out of scope here and are tracked in results/redteam-fixes.md.
"""

from __future__ import annotations

import importlib
import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)


# --------------------------------------------------------------------------------------------
# F-O1 — live dump path must be executable
# --------------------------------------------------------------------------------------------
def test_f_O1_oracle_imports_time():
    from experiments.oracle import oracle as O

    assert hasattr(O, "time"), "oracle.py must import time (F-O1)"
    import inspect

    for fn in (O.dump_source_topic, O.dump_kafka_topic):
        src = inspect.getsource(fn)
        assert "time.time()" in src or "time.monotonic()" in src
    src = inspect.getsource(O.dump_source_topic)
    assert "time." in src


def test_f_O1_dump_source_topic_runs_with_fake_consumer():
    """Exercise the real dump function end-to-end with a fake consumer (never possible via fixtures)."""
    from experiments.oracle import oracle as O

    assert hasattr(O, "dump_source_topic")
    # The function must accept an injectable consumer factory for testability.
    assert "consumer_factory" in __import__("inspect").signature(
        O.dump_source_topic).parameters
    out = os.path.join(REPO, "results", "dryrun", "_tmp_dump_test.ndjson")
    fake = _FakeConsumer([
        _FakeMsg(topic="t", partition=0, offset=0, value=b'{"event_id":"' + b"a" * 32 + b'"}'),
        _FakeMsg(topic="t", partition=0, offset=1, value=b"not-json"),
    ], watermarks={0: 2})
    info = O.dump_source_topic(bootstrap="fake:9092", topic="t", out_ndjson=out,
                               group="g", timeout_s=1.0, consumer_factory=lambda: fake)
    assert info["records"] == 2
    assert os.path.exists(out)
    with open(out, encoding="utf-8") as fh:
        rows = [json.loads(line) for line in fh if line.strip()]
    assert rows[0]["event_id"] == "a" * 32
    assert len(rows[0]["value_sha256"]) == 64
    os.remove(out)


class _FakeMsg:
    def __init__(self, topic, partition, offset, value):
        self._t, self._p, self._o, self._v = topic, partition, offset, value

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v

    def key(self):
        return None

    def error(self):
        return None


class _FakeConsumer:
    def __init__(self, messages, watermarks):
        self._messages = messages
        self._watermarks = watermarks
        self._i = 0

    def list_topics(self, topic, timeout=30):
        part = type("P", (), {"partitions": {0: object()}})()
        return type("MD", (), {"topics": {topic: part}})()

    def assign(self, tps):
        self._tps = tps

    def get_watermark_offsets(self, tp, timeout=30, cached=False):
        return (0, self._watermarks.get(tp.partition, 0))

    def seek(self, tp):
        pass

    def poll(self, timeout):
        if self._i < len(self._messages):
            m = self._messages[self._i]
            self._i += 1
            return m
        return None

    def close(self):
        pass


# --------------------------------------------------------------------------------------------
# F-O2 — missing/failing append-only attestation must not score clean
# --------------------------------------------------------------------------------------------
def test_f_O2_missing_attestation_is_a_violation():
    from experiments.oracle import oracle as O

    att = O.evaluate_attestation(None, required=True)
    assert att["failed"] is True, "missing attestation for a DB sink must fail I2(a), not warn"


def test_f_O2_attestation_queries_app_role_privileges():
    from experiments.oracle import oracle as O

    import inspect

    src = inspect.getsource(O.dump_pg)
    assert "wxeo_app" in src or "app_role" in src, \
        "attestation must query the application role's privileges (F-O2)"


# --------------------------------------------------------------------------------------------
# F-O3 — I1a/I1b split must be observable
# --------------------------------------------------------------------------------------------
def test_f_O3_whole_entry_duplication_has_structural_and_sum_breakdown():
    from experiments.oracle import oracle as O

    import inspect

    src = inspect.getsource(O.evaluate_observed)
    assert "imbalanced_sum_entries" in src, "oracle must report I1b (signed sum) separately"
    assert "structurally_malformed_entries" in src, "oracle must report I1a separately"


# --------------------------------------------------------------------------------------------
# F-O4 — balanced fabricated entry must be explicitly attributable (v5/SPURIOUS)
# --------------------------------------------------------------------------------------------
def test_f_O4_spurious_is_part_of_the_violation_vector():
    from experiments.oracle import oracle as O

    import inspect

    src = inspect.getsource(O.evaluate_observed)
    assert '"v5"' in src or "'v5'" in src, (
        "spurious postings must be an explicit violation clause v5 (author-ratified amendment), "
        "not an implicit extension")


# --------------------------------------------------------------------------------------------
# F-O5 — documented signed divergence + publication content checks
# --------------------------------------------------------------------------------------------
def test_f_O5_publication_content_and_signed_divergence():
    from experiments.oracle import oracle as O

    import inspect

    src = inspect.getsource(O)
    assert "pub_mutated" in src, "publication-stream content must be validated for DB mechanisms"
    assert "signed" in src.lower() or "stream_entries" in src, \
        "divergence must be the documented signed difference (stream entries - complete ledger)"


# --------------------------------------------------------------------------------------------
# F-O7 — malformed observed row must not be silently dropped
# --------------------------------------------------------------------------------------------
def test_f_O7_malformed_row_is_hard_error():
    from experiments.oracle import oracle as O

    import inspect

    src = inspect.getsource(O.evaluate_observed)
    assert "malformed" in src
    assert "raise" in src, "a malformed observed row must raise/invalidate, never be dropped"


# --------------------------------------------------------------------------------------------
# F-I1/I2 — single-owner receipt protocol
# --------------------------------------------------------------------------------------------
def _receipt(event, fault, source=None, **kw):
    r = {"ts_utc": "2026-11-24T00:00:00.000Z", "run_id": "x", "fault": fault,
         "event": event, "source": source, "detail": kw.pop("detail", ""), **kw}
    return r


def test_f_I1_external_injection_requires_s4_fired_and_restored():
    from experiments import receipts as R

    phase = {"load_start_ms": 1000, "load_end_ms": 5000, "fire_ms": 3000,
             "consumers_alive": True}
    recs = [_receipt("fired", "F1", source="s4", action_verified=True, phase=phase),
            _receipt("restored", "F1", source="s4")]
    out = R.validate_run_receipts(recs, run_id="x", injector_mode="live",
                                  fault="F1", fault_level="L2")
    assert out["attested"] is True, out
    recs_missing_fired = [_receipt("restored", "F1", source="s4")]
    out2 = R.validate_run_receipts(recs_missing_fired, run_id="x", injector_mode="live",
                                   fault="F1", fault_level="L2")
    assert out2["attested"] is False


def test_f_I2_l1_pause_has_single_owner_receipts():
    from experiments import receipts as R

    recs = [_receipt("fired", "before_effect_commit", source="app"),
            _receipt("restored", "before_effect_commit", source="app")]
    out = R.validate_run_receipts(recs, run_id="x", injector_mode="failpoint",
                                  fault="K5", fault_level="L1",
                                  failpoint="before_effect_commit", action="pause:200")
    assert out["attested"] is True, out
    # a second S4 restored must be rejected
    dup = recs + [_receipt("restored", "K5", source="s4")]
    assert R.validate_run_receipts(dup, run_id="x", injector_mode="failpoint",
                                   fault="K5", fault_level="L1",
                                   failpoint="before_effect_commit",
                                   action="pause:200")["attested"] is False


def test_f_I2_l1_crash_s4_writes_the_restored():
    from experiments import receipts as R

    recs = [_receipt("fired", "after_effect_commit", source="app"),
            _receipt("restored", "K3", source="s4")]
    out = R.validate_run_receipts(recs, run_id="x", injector_mode="failpoint",
                                  fault="K3", fault_level="L1",
                                  failpoint="after_effect_commit", action="crash")
    assert out["attested"] is True, out


def test_f_I3_fired_receipt_requires_verified_action():
    from experiments import receipts as R

    recs = [_receipt("fired", "F1", source="s4", action_verified=False),
            _receipt("restored", "F1", source="s4")]
    out = R.validate_run_receipts(recs, run_id="x", injector_mode="live",
                                  fault="F1", fault_level="L2")
    assert out["attested"] is False, "an unverified action must not count as fired"


# --------------------------------------------------------------------------------------------
# F-I4 — rerun path + invalid record always written
# --------------------------------------------------------------------------------------------
def test_f_I4_runner_has_rerun_and_invalid_record_paths():
    from experiments import runner as RN

    assert hasattr(RN.Runner, "_attempt_loop"), "runner needs the R_max rerun loop (F-I4)"
    assert hasattr(RN.Runner, "_write_invalid_record"), \
        "runner must always write an invalid run record (F-I4)"


# --------------------------------------------------------------------------------------------
# F-I5 — fire-rate table
# --------------------------------------------------------------------------------------------
def test_f_I5_fire_rate_computation_exists():
    from experiments import deviations as DEV

    assert hasattr(DEV, "compute_fire_rates"), "fire-rate/exclusion table missing (F-I5)"


# --------------------------------------------------------------------------------------------
# F-E1 — Stage-A tokens + K4 in failpoint map
# --------------------------------------------------------------------------------------------
def _ctx(fault, level="L1", mode="failpoint", ordering="strict"):
    from experiments import injectors as I

    return I.InjectorContext(run_id="x", mechanism="M0", ordering_variant=ordering,
                             fault=fault, fault_level=level, injector_mode=mode,
                             seed_hex="0f0e0d0c0b0a0908", scratch_dir=REPO,
                             faults_log=os.path.join(REPO, "results", "faults.log"),
                             bootstrap="127.0.0.1:9091")


def test_f_E1_uppercase_and_lowercase_stage_a_tokens_arm():
    from experiments import injectors as I

    for token in ("K3", "k3"):
        cfg = I.failpoint_config_for_run(_ctx(token))
        assert cfg and cfg.get("enabled") is True, f"token {token!r} must arm (F-E1)"


def test_f_E1_K4_failpoint_exists():
    from experiments import injectors as I

    cfg = I.failpoint_config_for_run(_ctx("K4", ordering="early"))
    assert cfg and cfg.get("enabled") is True, "K4 must be in the failpoint map (F-E1)"


def test_f_E2_K5_and_K6_are_realisable_or_declared_na():
    from experiments import injectors as I

    # K6 must be declared not-realisable (no silent no-op action)
    assert "K6" in I.STRUCTURALLY_UNREACHABLE or "K6" not in I.CRASH_CLASS_FAILPOINT
    # K5 must have a real replay realisation, not a pause no-op
    k5 = I.CRASH_CLASS_FAILPOINT.get("K5", {})
    assert k5.get("action") != "pause:200", "K5 must be a protocol replay, not a pause (F-E2)"


def test_f_E4_f6_excluded_for_m1_kafka():
    from experiments import matrix_design as MD

    assert "F6" in MD.STAGE_B_NA.get("M1-kafka", set()), \
        "F6 must be n/a for M1-kafka to preserve the D5 positive control (F-E4)"


def test_f_E4_rewind_uses_write_capable_role():
    from experiments import injectors as I

    import inspect

    src = inspect.getsource(I.F6DuplicateRedelivery) + inspect.getsource(I.InjectorContext)
    assert "write-capable" in src or "admin_dsn" in src, \
        "M4 progress rewind must use a write-capable admin role (S5 F-E4)"


# --------------------------------------------------------------------------------------------
# F-P4 — canonical stage/fault tokens in matrix
# --------------------------------------------------------------------------------------------
def test_f_P4_matrix_stage_a_fault_tokens_are_canonical():
    import csv

    with open(os.path.join(REPO, "experiments", "run_order.csv"), encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    stage_a = [r for r in rows if r["stage"] == "a"]
    assert stage_a, "no Stage-A runs"
    bad = {r["fault"] for r in stage_a} - {"K1", "K2", "K3", "K4", "K5", "K6", "K7", "none"}
    assert not bad, f"Stage-A fault tokens must be canonical upper-case; found {sorted(bad)}"


# --------------------------------------------------------------------------------------------
# F-G2 — quiescence via AdminClient; M1-kafka stream definition
# --------------------------------------------------------------------------------------------
def test_f_G2_kafka_admin_module_and_quiescence_definitions():
    admin = importlib.import_module("experiments.kafka_admin")
    assert hasattr(admin, "KafkaAdmin")
    assert hasattr(admin, "wait_consumer_quiescence")
    assert hasattr(admin, "wait_publication_quiescence")


# --------------------------------------------------------------------------------------------
# F-RN1 — canonical record mapping + always-valid records
# --------------------------------------------------------------------------------------------
def test_f_RN1_canonical_record_mapping():
    from experiments import matrix_design as MD

    assert MD.record_stage("P") == "pilot"
    assert MD.record_stage("V") == "pilot"
    assert MD.record_stage("A") == "A"
    assert MD.record_fault("k3") == "K3"
    assert MD.record_fault("ctrl") == "none"
    assert MD.record_fault("f3") == "F3"


def test_f_RN3_clock_probe_exists():
    from experiments import runner as RN

    assert hasattr(RN, "clock_sanity_probe"), "clock check must not be hard-coded (F-RN3)"


# --------------------------------------------------------------------------------------------
# F-P5 — analyzer accounting
# --------------------------------------------------------------------------------------------
def test_f_P5_censored_counts_as_violating_and_planned_is_frozen():
    from tools import analyze as A

    rec = {
        "run_id": "pilot-m0-f0-1k-p3-r00", "stage": "pilot", "cell_id": "P.M0.F0.1k.p3",
        "mechanism": "M0", "ordering_variant": "strict", "fault": "F0",
        "fault_level": "none", "load_events": 1000, "partitions": 3,
        "consumer_instances": 2, "rep": 0,
        "validity": {"valid": True, "excluded": False, "exclude_reason": None,
                     "residue_check": True, "clock_check": True,
                     "disk_free_start_gb": 10.0, "notes": []},
        "violations": {"v1": False, "v2": False, "v3": False, "v4": False,
                       "violating_run": False},
        "counts": {k: 0 for k in ("produced", "consumed_estimate", "expected_postings",
                                  "observed_postings", "dup_postings", "dup_entries",
                                  "lost_postings", "lost_entries", "imbalanced_entries",
                                  "mutated_postings", "spurious_postings")},
        "performance": {}, "quiescence": {"censored": True, "t_consistent_ms": None},
        "fault_receipts": [], "config": {}, "oracle": {"source_integrity": True},
        "publication": {},
    }
    thin = A.extract(rec)
    assert thin["violating_run"] is True, "censored runs are violating outcomes (method §7.3)"
    rows = A.correctness_by_cell([thin], "unit", reserved={"P.M0.F0.1k.p3": 30})
    assert rows[0]["n_planned"] == 30, "n_planned must be the frozen N, not len(records) (F-P5)"


# --------------------------------------------------------------------------------------------
# Path hygiene (lead P0 addendum)
# --------------------------------------------------------------------------------------------
def test_path_hygiene_module_and_validator():
    from experiments import pathsafe
    from experiments import validate_record as VR

    dirty = {"path": "C:\\Users\\alice\\workspace\\x\\y.json", "ok": "results/raw/x.json"}
    violations = pathsafe.find_violations(dirty)
    assert violations, "absolute local paths must be detected"
    bad_rec = {"artifacts": {"gt": "C:\\Users\\alice\\workspace\\gt.ndjson"}}
    assert VR.path_violations(bad_rec), "validate_record must reject absolute local paths"


def test_fixture_and_analysis_outputs_are_path_clean():
    """Committed synthetic/dry-run artefacts must not contain absolute local paths."""
    from experiments import pathsafe

    roots = [os.path.join(REPO, "results", "synthetic"),
             os.path.join(REPO, "results", "dryrun")]
    for root in roots:
        for dirpath, _dirs, files in os.walk(root):
            for name in files:
                path = os.path.join(dirpath, name)
                if name.endswith(".json"):
                    obj = json.load(open(path, encoding="utf-8"))
                    violations = pathsafe.find_violations(obj)
                elif name.endswith(".ndjson"):
                    violations = []
                    for line in open(path, encoding="utf-8", errors="replace"):
                        line = line.strip()
                        if line:
                            try:
                                violations += pathsafe.find_violations(json.loads(line))
                            except json.JSONDecodeError:
                                violations += pathsafe.text_violations(line)
                elif name.endswith((".csv", ".tex", ".md")):
                    violations = pathsafe.text_violations(
                        open(path, encoding="utf-8", errors="replace").read())
                else:
                    continue
                assert not violations, \
                    f"absolute local path leaked into {os.path.relpath(path, REPO)}: {violations[:2]}"
