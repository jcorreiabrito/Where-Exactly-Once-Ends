"""M7 -> rates chain tests (S5 §11.4, Amendment A5 r2, Amendment A6): stage enum,
committed_events, mapping rules, two-step derive/validate, fail-closed saturation validation,
M7cal/M7sat run selection and the S5-B7 endpoint contract.
"""
import csv
import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments import matrix as MX  # noqa: E402
from experiments import matrix_design as MD  # noqa: E402
from experiments import rates as R  # noqa: E402
from experiments import runner as RN  # noqa: E402
from experiments import validate_record as VR  # noqa: E402

M7SAT_DERIVED_RATE = 5555
SAT_KEY = "100000|p6|sat"
M7_RUNS_CSV = os.path.join(REPO, "experiments", "run_order.csv")
M7_MATRIX_YAML = os.path.join(REPO, "experiments", "matrix.yaml")


# --------------------------------------------------------------------------------------------
# fixtures / helpers
# --------------------------------------------------------------------------------------------
def _m7_record():
    fixture = os.path.join(REPO, "results", "synthetic", "run-fixture", "run-records")
    name = sorted(os.listdir(fixture))[0]
    rec = json.load(open(os.path.join(fixture, name), encoding="utf-8"))
    rec["stage"] = "M7"
    rec["fault"] = "none"
    rec["performance"]["committed_events"] = 9000
    return rec


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
        # A5.2 spot-check evidence (mapping predictions: 10k/P3=450, 10k/P12=1800,
        # 100k/P3=400, 100k/P12=1600, 1M/P6=800 capacity events/s)
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


def _calibration_record(load, partitions, mechanism, committed_events, seconds, rate_mode):
    """Minimal M7 calibration record as ingested by rates._load_records (A5 evidence).

    P1 (S5 §13): capacity samples must carry the mechanism-committed basis from the artifact's
    per-instance performance blocks, never producer acks.
    """
    return {"stage": "M7", "run_id": f"m7-{mechanism.lower()}-{load}-p{partitions}",
            "mechanism": mechanism, "load_events": load, "partitions": partitions,
            "performance": {"committed_events": committed_events,
                            "committed_events_total": committed_events,
                            "committed_basis": "app-committed",
                            "throughput_eps_median": committed_events / seconds,
                            "perf_window": {"start": 0, "end": int(seconds * 1000),
                                            "events": committed_events}},
            "quiescence": {"censored": False},
            "exit_status": {"consumer": {"1": 0}, "load_generator": 0},
            "timings": {"t_load_start": 0, "t_load_end": int(seconds * 1000)},
            "config": {"runner": {"rate_mode": rate_mode}}}


def _all_calibration_records():
    recs = []
    for s in _samples():
        mode = "calibration-saturation" if s.get("mode") == "saturation" else "fixed"
        recs.append(_calibration_record(s["load_events"], s["partitions"], s["mechanism"],
                                        s["committed_events"],
                                        s["window_end_s"] - s["window_start_s"], mode))
    return recs


def _m7sat_record(mechanism, rate=M7SAT_DERIVED_RATE, *, offered=None, throughput=None,
                  lag=1, valid=True, load_generator_exit=0, rate_mode="saturation-validation",
                  run_id=None, load_events=100_000, partitions=6):
    rid = run_id or (f"m7sat-{MD.MECH_TOKEN[mechanism]}-cal-100k-p6-r00")
    return {
        "schema": VR.SCHEMA_VERSION,
        "run_id": rid,
        "stage": "M7",
        "cell_id": f"M7.{mechanism}.cal.100k.p6.sat",
        "mechanism": mechanism,
        "load_events": load_events,
        "partitions": partitions,
        "validity": {"valid": valid},
        "exit_status": {"consumer": 0, "load_generator": load_generator_exit},
        "config": {"config_sha256": "0" * 64,
                   "runner": {"rate_mode": rate_mode,
                              "offered_rate": rate if offered is None else offered}},
        "performance": {"throughput_eps_median": rate if throughput is None else throughput,
                        "committed_events": 90_000,
                        "committed_events_total": 100_000,
                        "committed_basis": "app-committed",
                        "perf_window": {"start": 0, "end": 36_000, "events": 90_000},
                        "generator_lag_p99_ms": lag,
                        "generator_lag_flag": lag > 5},
        "timings": {"t_load_start": 0, "t_load_end": 36_000},
    }


def _all_m7sat_records(rate=M7SAT_DERIVED_RATE):
    return [_m7sat_record(mech, rate) for mech in MD.MECHANISMS]


def _write_records(records_dir, records):
    os.makedirs(records_dir, exist_ok=True)
    for rec in records:
        with open(os.path.join(records_dir, f"run_{rec['run_id']}.json"), "w",
                  encoding="utf-8") as fh:
            json.dump(rec, fh)


def _derive_doc(samples=None):
    return R.derive_rates(samples or _samples(), matrix_sha256="abc")


def _plan_rows():
    with open(M7_RUNS_CSV, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


# --------------------------------------------------------------------------------------------
# record/stage enum + committed_events (S5 §11.4 / R12)
# --------------------------------------------------------------------------------------------
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


# --------------------------------------------------------------------------------------------
# A5/A6 matrix accounting + run-order composition + stage selection
# --------------------------------------------------------------------------------------------
def test_a6_design_constants():
    assert MD.M7_CAL_RUNS == 54
    assert (1_000, 3) not in MD.M7_SPOT_BLOCKS, "1k/P3 is not calibratable (A5.1)"
    assert MD.M7_SPOT_BLOCKS == ((10_000, 3), (10_000, 12), (100_000, 3),
                                 (100_000, 12), (1_000_000, 6))
    assert MD.M7_SPOT_RUNS == 54          # 9 mechanisms x (5 fixed + 1 saturation)
    assert MD.M7_SAT_RUNS == 9            # A6: one 100k/P6 saturation run per mechanism
    assert MD.M7_CALIBRATION_RUNS == 117
    assert MD.ORDER_ROWS == 7_631
    assert MD.CONFIRMATORY_PLANNED == 7_506
    assert MD.CONFIRMATORY_EXECUTABLE == 7_488
    assert MD.ORDER_SLOTS == 7_514
    assert MD.validate_design() == []


def test_matrix_yaml_calibration_accounting_a5r2_a6():
    import yaml
    doc = yaml.safe_load(open(M7_MATRIX_YAML, encoding="utf-8"))
    acc = doc["accounting"]
    assert acc["calibration"]["blocks"] == {"M7-cal": 54, "M7spot": 54, "M7sat": 9}
    assert acc["calibration"]["runs"] == 117
    assert acc["calibration"]["cells"] == 81
    assert acc["totals"]["runs_planned"] == 7_506
    assert acc["totals"]["runs_executable"] == 7_488
    assert acc["totals"]["validation_runs"] == 26
    assert acc["totals"]["nominal_total_executable_plus_validation"] == 7_514
    cal = doc["calibration_blocks"]
    assert cal["M7sat"]["run_id_prefix"] == "m7sat"
    assert cal["M7spot"]["run_id_prefix"] == "m7sopt"
    assert cal["total_runs"] == 117 and cal["excluded_from_confirmatory"] is True


def test_matrix_check_outputs_passes_with_new_hashes():
    report = MX.check_outputs()
    assert report["ok"] is True, report["problems"]
    assert report["rows"] == 7_631


def test_run_order_calibration_composition_and_no_1k_rows():
    rows = _plan_rows()
    assert len(rows) == 7_631
    cal = [r for r in rows if r["stage"] == "m7"]
    by_block = {}
    for row in cal:
        by_block.setdefault(RN.run_block(row), []).append(row)
    assert set(by_block) == {"M7-cal", "M7spot", "M7sat"}
    assert {b: len(v) for b, v in by_block.items()} == {"M7-cal": 54, "M7spot": 54,
                                                        "M7sat": 9}
    # A5.1: no 1k calibration/spot row may exist anywhere in the plan
    assert not [r for r in cal if int(r["load_events"]) == 1_000]
    assert not [r for r in rows
                if RN.run_block(r).startswith("M7") and int(r["load_events"]) == 1_000]
    # A5 spot ids are m7sopt-... for the six spot blocks only (five fixed + saturation)
    spot = by_block["M7spot"]
    assert all(r["run_id"].startswith("m7sopt-") for r in spot)
    spot_keys = {(int(r["load_events"]), int(r["partitions"]), r["load_mode"]) for r in spot}
    expected_spot = ({(load, p, "fixed-rate") for load, p in MD.M7_SPOT_BLOCKS}
                     | {(100_000, 6, "saturation")})
    assert spot_keys == expected_spot
    for mech in MD.MECHANISMS:
        assert len([r for r in spot if r["mechanism"] == mech]) == 6
    # A6 M7sat: exactly one 100k/P6 saturation run per mechanism, m7sat- ids
    sat = by_block["M7sat"]
    assert all(r["run_id"].startswith("m7sat-") for r in sat)
    assert {r["mechanism"] for r in sat} == set(MD.MECHANISMS)
    assert all(int(r["load_events"]) == 100_000 and int(r["partitions"]) == 6
               and r["load_mode"] == "saturation" for r in sat)
    # M7-cal ids keep the m7- prefix
    assert all(r["run_id"].startswith("m7-") for r in by_block["M7-cal"])


def test_selected_runs_m7cal_vs_m7sat(tmp_path):
    cal_runner = RN.Runner(RN.RunnerOptions(stage="M7cal",
                                           results_dir=str(tmp_path / "res-cal")))
    cal_runs = cal_runner.selected_runs()
    assert len(cal_runs) == 108
    assert {RN.run_block(r) for r in cal_runs} == {"M7-cal", "M7spot"}
    assert not [r for r in cal_runs if r["run_id"].startswith("m7sat-")]
    sat_runner = RN.Runner(RN.RunnerOptions(stage="M7sat",
                                           results_dir=str(tmp_path / "res-sat")))
    sat_runs = sat_runner.selected_runs()
    assert len(sat_runs) == 9
    assert {RN.run_block(r) for r in sat_runs} == {"M7sat"}
    assert {r["stage"] for r in sat_runs} == {"m7"}
    assert {r["mechanism"] for r in sat_runs} == set(MD.MECHANISMS)
    assert all(r["load_mode"] == "saturation" for r in sat_runs)


# --------------------------------------------------------------------------------------------
# A5 mapping rule (unchanged, but A6 makes the candidate non-final)
# --------------------------------------------------------------------------------------------
def test_mapping_rule_derives_all_blocks_with_assumptions():
    doc = _derive_doc()
    assert doc["schema"] == "wxeo.rates.v2"
    assert doc["rule_id"] == "A5.partition-scale.v1"
    # A6: the arithmetic rule alone can never produce a final validated table; it is a candidate
    # until the 9 M7sat runs are exercised (T1b).
    assert doc["validation"]["validated"] is True
    assert doc["validated"] is False
    assert doc["saturation_validation"]["mode"] == "post-rates"
    assert doc["saturation_validation"]["validated"] is False
    assert doc["saturation_validation"]["status"] == "pending-m7sat"
    # measured 100k/P6 min capacity = 800/s -> 400 rate; P3 derived = 400/s -> 200
    assert doc["fixed_rate"]["100000|p6"]["rate"] >= 1
    assert doc["fixed_rate"]["100000|p3"]["rate"] >= 1
    assert "assumption" in doc["fixed_rate"]["100000|p3"]
    # B2: 1k blocks are excluded (Stage A/pilot run at max), no required key
    assert "1000|p3" not in doc["fixed_rate"]
    assert doc["fixed_rate"]["1000000|p6"]["rate"] >= 1  # 1M derived from 100k
    assert doc["derived_blocks"]
    # saturation = floor(2.5 * max capacity) = floor(2.5 * 120000/54) = 5555
    assert doc["saturation_rate"][SAT_KEY]["rate"] == M7SAT_DERIVED_RATE
    # G7 is fail-closed: the candidate carries no A6 saturation evidence yet
    assert R.g7_checks(doc, sorted(doc["fixed_rate"]) + sorted(doc["saturation_rate"]))["ok"] \
        is False


def test_spot_check_deviation_fails_closed():
    samples = _samples()
    for s in samples:
        if (s["load_events"], s["partitions"]) == (100_000, 12):
            s["committed_events"] = 4000   # way below the 1600/s prediction
    doc = _derive_doc(samples)
    assert doc["validation"]["validated"] is False
    assert doc["validation"]["failed_blocks"]
    assert doc["validated"] is False
    assert R.g7_checks(doc, sorted(doc["fixed_rate"]) + sorted(doc["saturation_rate"]))["ok"] \
        is False


def test_missing_spot_evidence_fails_closed():
    samples = [s for s in _samples()
               if (s["load_events"], s["partitions"]) != (10_000, 12)]
    doc = _derive_doc(samples)
    assert doc["validation"]["validated"] is False
    assert "10000|p12" in doc["validation"]["missing_spot_checks"]
    assert doc["validated"] is False


def test_saturation_missing_fails_closed():
    samples = [s for s in _samples() if s.get("mode") != "saturation"]
    with pytest.raises(SystemExit):
        R.derive_rates(samples, matrix_sha256="abc")


def test_full_coverage_mode_requires_all_blocks():
    # _samples covers all 8 fixed-rate blocks, so full coverage mode succeeds and validates;
    # a genuinely missing block must still fail closed.  A6 keeps the overall table non-final.
    doc = R.derive_rates(_samples(), matrix_sha256="abc", mode=R.RULE_ID_FULL)
    assert doc["rule_id"] == R.RULE_ID_FULL
    assert doc["validation"]["validated"] is True
    assert doc["validated"] is False
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
    # A6: the M7sat path must fail closed instead of offering 'max'
    assert "A6 saturation validation" in src
    assert "saturation-validation" in src


# --------------------------------------------------------------------------------------------
# A6 saturation validation (report-level)
# --------------------------------------------------------------------------------------------
def test_a6_missing_m7sat_evidence_means_not_validated():
    doc = _derive_doc()
    report = R.saturation_validation_report([], doc)
    assert report["mode"] == "post-rates"
    assert report["validated"] is False
    assert report["observed_runs"] == 0
    assert report["mechanisms_missing"] == list(MD.MECHANISMS)
    combined = R.derive_rates(_samples(), matrix_sha256="abc", saturation_validation=report)
    assert combined["validated"] is False
    assert combined["validation"]["validated"] is True


def test_a6_all_nine_pass_validates_both_layers():
    doc = _derive_doc()
    report = R.saturation_validation_report(_all_m7sat_records(), doc)
    assert report["validated"] is True, report["problems"]
    assert report["observed_runs"] == 9
    assert report["derived_rate"] == M7SAT_DERIVED_RATE
    assert all(entry["valid"] for entry in report["runs"].values())
    combined = R.derive_rates(_samples(), matrix_sha256="abc", saturation_validation=report)
    assert combined["validated"] is True
    checks = R.g7_checks(combined, sorted(combined["fixed_rate"])
                         + sorted(combined["saturation_rate"]))
    assert checks["ok"] is True, checks["problems"]


@pytest.mark.parametrize("mutate,expected", [
    (lambda recs: [r for r in recs if r["mechanism"] != "M3"], "missing"),
    (lambda recs: recs + [dict(recs[0])], "duplicate"),
    (lambda recs: recs + [_m7sat_record("M0", run_id="m7sat-m0-cal-100k-p6-r99")],
     "duplicate"),
    (lambda recs: [dict(r, load_events=10_000) if r["mechanism"] == "M0" else r
                   for r in recs], "100k"),
    (lambda recs: [dict(r, partitions=3) if r["mechanism"] == "M0" else r
                   for r in recs], "100k"),
    (lambda recs: [dict(r, validity={"valid": False}) if r["mechanism"] == "M0" else r
                   for r in recs], "not valid"),
    (lambda recs: [dict(r, exit_status={"consumer": 0, "load_generator": 1})
                   if r["mechanism"] == "M0" else r for r in recs], "load-generator"),
    (lambda recs: [dict(r, config={"runner": {"rate_mode": "saturation-validation"}})
                   if r["mechanism"] == "M0" else r for r in recs], "offered_rate"),
    (lambda recs: [dict(r, config={"runner": {"offered_rate": M7SAT_DERIVED_RATE}})
                   if r["mechanism"] == "M0" else r for r in recs], "rate_mode"),
    (lambda recs: [_m7sat_record("M0", offered=M7SAT_DERIVED_RATE - 1)]
                  + [r for r in recs if r["mechanism"] != "M0"], "offered rate"),
    (lambda recs: [dict(r, performance={**r["performance"],
                                        "generator_lag_p99_ms": None})
                   if r["mechanism"] == "M0" else r for r in recs], "generator-lag"),
    (lambda recs: [_m7sat_record("M0", lag=6)] + [r for r in recs if r["mechanism"] != "M0"],
     "generator-lag"),
    (lambda recs: [_m7sat_record("M0", throughput=M7SAT_DERIVED_RATE * 0.5)]
                  + [r for r in recs if r["mechanism"] != "M0"], "throughput"),
    (lambda recs: [dict(r, performance={k: v for k, v in r["performance"].items()
                                        if k not in ("throughput_eps_median",
                                                     "committed_events")})
                   if r["mechanism"] == "M0" else r for r in recs], "throughput"),
])
def test_a6_fail_closed_on_missing_or_bad_evidence(mutate, expected):
    doc = _derive_doc()
    recs = mutate(_all_m7sat_records())
    report = R.saturation_validation_report(recs, doc)
    assert report["validated"] is False
    assert report["problems"], "no problems recorded"
    assert any(expected.lower() in p.lower() for p in report["problems"]), report["problems"]
    combined = R.derive_rates(_samples(), matrix_sha256="abc",
                              saturation_validation=report)
    assert combined["validated"] is False


def test_a6_requires_exactly_the_derived_rate_for_validation():
    # a report built against a different derived rate must not validate
    doc = _derive_doc()
    other = R.derive_rates(_samples(), matrix_sha256="abc")
    other["saturation_rate"][SAT_KEY]["rate"] = M7SAT_DERIVED_RATE + 100
    report = R.saturation_validation_report(_all_m7sat_records(), other)
    assert report["validated"] is False
    assert report["derived_rate"] == M7SAT_DERIVED_RATE + 100
    assert report["problems"]


# --------------------------------------------------------------------------------------------
# A6 two-step CLI: --derive (candidate) / --write (validated) / --check
# --------------------------------------------------------------------------------------------
def test_a6_cli_derive_then_write_two_step(tmp_path):
    records = tmp_path / "records"
    _write_records(str(records), _all_calibration_records())
    out = tmp_path / "rates.json"

    # T1a: derive the candidate table (validated=false, A6 pending)
    rc = R.main(["--records-dir", str(records), "--out", str(out), "--derive"])
    assert rc == 0
    candidate = json.loads(out.read_text())
    assert candidate["validated"] is False
    assert candidate["validation"]["validated"] is True      # A5 spot checks passed
    assert candidate["saturation_validation"]["mode"] == "post-rates"
    assert candidate["saturation_validation"]["validated"] is False
    assert candidate["saturation_rate"][SAT_KEY]["rate"] == M7SAT_DERIVED_RATE
    # --check reports both layers and refuses (not final yet)
    assert R.main(["--out", str(out), "--check"]) == 1

    # T1b: the 9 M7sat runs exist -> final validation passes and writes validated=true
    _write_records(str(records), _all_m7sat_records())
    rc = R.main(["--records-dir", str(records), "--out", str(out), "--write"])
    assert rc == 0
    final = json.loads(out.read_text())
    assert final["validated"] is True
    assert final["saturation_validation"]["validated"] is True
    assert final["saturation_validation"]["observed_runs"] == 9
    assert R.main(["--out", str(out), "--check"]) == 0


def test_a6_cli_write_without_m7sat_evidence_fails_closed(tmp_path):
    records = tmp_path / "records"
    _write_records(str(records), _all_calibration_records())
    out = tmp_path / "rates.json"
    assert R.main(["--records-dir", str(records), "--out", str(out), "--derive"]) == 0
    # --write before T1b must fail closed (validated=false, exit 1)
    assert R.main(["--records-dir", str(records), "--out", str(out), "--write"]) == 1
    doc = json.loads(out.read_text())
    assert doc["validated"] is False
    assert doc["saturation_validation"]["validated"] is False
    assert doc["saturation_validation"]["observed_runs"] == 0


def test_a6_cli_write_with_bad_saturation_record_fails_closed(tmp_path):
    records = tmp_path / "records"
    _write_records(str(records), _all_calibration_records())
    out = tmp_path / "rates.json"
    assert R.main(["--records-dir", str(records), "--out", str(out), "--derive"]) == 0
    bad = _all_m7sat_records()
    bad[0] = _m7sat_record("M0", lag=50)   # G7 generator-lag violation
    _write_records(str(records), bad)
    assert R.main(["--records-dir", str(records), "--out", str(out), "--write"]) == 1
    doc = json.loads(out.read_text())
    assert doc["validated"] is False
    assert any("generator-lag" in p for p in doc["saturation_validation"]["problems"])


def test_a6_load_records_never_ingests_confirmatory_stages(tmp_path):
    records = tmp_path / "records"
    # include an explicit M1-kafka calibration record: A5.2 covers all nine mechanisms, so the
    # loader must NOT filter the ledger mechanism out of the capacity statistics.
    _write_records(str(records), _all_calibration_records() + [
        _calibration_record(100_000, 6, "M1-kafka", 90_000, 100.0, "fixed")])
    m7sat = _m7sat_record("M0")
    _write_records(str(records), [m7sat])
    confirmatory = _m7sat_record("M2")
    confirmatory.update({"run_id": "b-m2-f0-100k-p6-r00", "stage": "B"})
    _write_records(str(records), [confirmatory])
    samples = R._load_records(str(records))
    assert samples, "calibration samples missing"
    assert "M1-kafka" in {s["mechanism"] for s in samples}
    modes = {s["mode"] for s in samples}
    assert "saturation_validation" in modes     # M7sat may be ingested for validation
    assert "fixed" in modes and "saturation" in modes
    # exactly one saturation_validation sample (only the M7sat record; Stage-B never ingested)
    assert len([s for s in samples if s["mode"] == "saturation_validation"]) == 1
    m7sat_records = R._load_m7sat_records(str(records))
    assert [r["run_id"] for r in m7sat_records] == [m7sat["run_id"]]


# --------------------------------------------------------------------------------------------
# runner: A6 rate selection (fail-closed candidate read) and calibration discipline
# --------------------------------------------------------------------------------------------
def _rate_sequence(row, rate_table):
    class _R:
        opts = type("Opts", (), {"rate_table": rate_table})()
    seq = object.__new__(RN.LiveSequence)
    seq.run = row
    seq.r = _R()
    seq.rate_mode = None
    seq.offered_rate = None
    return seq


def test_runner_m7sat_offers_exactly_the_candidate_saturation_rate(tmp_path):
    table = tmp_path / "rates.json"
    table.write_text(json.dumps({"validated": False,
                                 "saturation_rate": {SAT_KEY: {"rate": 5555}}}))
    row = next(r for r in _plan_rows() if r["run_id"].startswith("m7sat-"))
    seq = _rate_sequence(row, str(table))
    assert seq._effective_rate() == "5555"
    assert seq.rate_mode == "saturation-validation"
    assert seq.offered_rate == 5555


@pytest.mark.parametrize("payload", [
    None,                                                          # file missing
    {"validated": False},                                          # no saturation_rate
    {"saturation_rate": {}},                                       # no candidate block
    {"saturation_rate": {SAT_KEY: {}}},                            # no rate
    {"saturation_rate": {SAT_KEY: {"rate": None}}},                # not an int
    {"saturation_rate": {SAT_KEY: {"rate": 0}}},                   # below the MIN_RATE floor
])
def test_runner_m7sat_rate_fails_closed_without_candidate(tmp_path, payload):
    table = tmp_path / "rates.json"
    if payload is not None:
        table.write_text(json.dumps(payload))
    row = next(r for r in _plan_rows() if r["run_id"].startswith("m7sat-"))
    seq = _rate_sequence(row, str(table))
    with pytest.raises(RuntimeError, match="A6 saturation validation"):
        seq._effective_rate()


def test_runner_m7cal_and_m7spot_stay_at_max(tmp_path):
    table = tmp_path / "rates.json"
    table.write_text(json.dumps({"saturation_rate": {SAT_KEY: {"rate": 5555}}}))
    rows = _plan_rows()
    cal_row = next(r for r in rows if RN.run_block(r) == "M7-cal")
    seq = _rate_sequence(cal_row, str(table))
    assert seq._effective_rate() == "max"
    assert seq.rate_mode == "calibration-max"
    assert seq.offered_rate is None
    spot_sat = next(r for r in rows if RN.run_block(r) == "M7spot"
                    and r["load_mode"] == "saturation")
    seq = _rate_sequence(spot_sat, str(table))
    assert seq._effective_rate() == "max"
    assert seq.rate_mode == "calibration-saturation"


def test_runner_cli_accepts_m7sat_stage():
    parser = RN.build_parser()
    assert parser.parse_args(["--stage", "M7sat"]).stage == "M7sat"
    choices = next(action.choices for action in parser._actions
                   if action.dest == "stage")
    assert {"M7cal", "M7sat"} <= set(choices)


# --------------------------------------------------------------------------------------------
# S5 B7 endpoint contract: deploy/docker/endpoints.json drives the Kafka fault paths
# --------------------------------------------------------------------------------------------
def test_endpoint_contract_reads_group_member_fault_bootstrap():
    contract = RN.load_endpoints_contract(RN.ENDPOINTS_CONTRACT_PATH)
    assert contract["fault_bootstrap"] == "127.0.0.1:19091"
    assert contract["path"] == "deploy/docker/endpoints.json"
    assert len(contract["sha256"]) == 64
    assert contract["fault_bootstrap"] in contract["doc"]["kafka"]["group_member_proxies"]


def test_endpoint_contract_fails_closed_on_missing_or_bad_contract(tmp_path):
    with pytest.raises(RuntimeError, match="endpoint contract"):
        RN.load_endpoints_contract(str(tmp_path / "missing.json"))
    bad_schema = tmp_path / "bad-schema.json"
    bad_schema.write_text(json.dumps({"kafka": {"fault_bootstrap": "127.0.0.1:19091"}}))
    with pytest.raises(RuntimeError, match="endpoint contract"):
        RN.load_endpoints_contract(str(bad_schema))
    no_kafka = tmp_path / "no-kafka.json"
    no_kafka.write_text(json.dumps({"schema": "wxeo.endpoints.v1"}))
    with pytest.raises(RuntimeError, match="kafka"):
        RN.load_endpoints_contract(str(no_kafka))
    no_key = tmp_path / "no-key.json"
    no_key.write_text(json.dumps({"schema": "wxeo.endpoints.v1",
                                  "kafka": {"group_member_proxies": ["127.0.0.1:19091"]}}))
    with pytest.raises(RuntimeError, match="fault_bootstrap"):
        RN.load_endpoints_contract(str(no_key))
    not_member = tmp_path / "not-member.json"
    not_member.write_text(json.dumps({
        "schema": "wxeo.endpoints.v1",
        "kafka": {"fault_bootstrap": "127.0.0.1:19094",
                  "group_member_proxies": ["127.0.0.1:19091", "127.0.0.1:19092",
                                           "127.0.0.1:19093"]}}))
    with pytest.raises(RuntimeError, match="group_member_proxies"):
        RN.load_endpoints_contract(str(not_member))


def test_endpoints_for_run_kafka_fault_paths_use_contract_bootstrap():
    fb = "127.0.0.1:19091"
    assert RN.is_kafka_fault_path({"fault": "F3", "mechanism": "M2"})
    assert RN.is_kafka_fault_path({"fault": "F4a", "mechanism": "M2"})
    assert RN.is_kafka_fault_path({"fault": "K2", "mechanism": "M1-kafka"})
    assert not RN.is_kafka_fault_path({"fault": "K2", "mechanism": "M2"})
    f3 = RN.endpoints_for_run({"fault": "F3", "mechanism": "M2"}, fb)
    assert f3["producer"] == fb and f3["consumer"] is None
    f4a = RN.endpoints_for_run({"fault": "F4a", "mechanism": "M2"}, fb)
    assert f4a["consumer"] == fb and f4a["producer"] is None
    k2_kafka = RN.endpoints_for_run({"fault": "K2", "mechanism": "M1-kafka"}, fb)
    assert k2_kafka["producer"] == fb
    # non-fault/native paths unchanged
    native = RN.endpoints_for_run({"fault": "F0", "mechanism": "M2"}, None)
    assert native == {"bootstrap": "127.0.0.1:9091", "producer": None, "consumer": None,
                      "db_url": "jdbc:postgresql://127.0.0.1:5433/wxeo"}
    f5 = RN.endpoints_for_run({"fault": "F5", "mechanism": "M2"}, None)
    assert f5["db_url"].endswith(":15433/wxeo")
    k2_db = RN.endpoints_for_run({"fault": "K2", "mechanism": "M2"}, None)
    assert k2_db["db_url"].endswith(":15433/wxeo")


def test_endpoints_for_run_kafka_fault_path_fails_closed_without_contract():
    with pytest.raises(RuntimeError, match="fault bootstrap"):
        RN.endpoints_for_run({"fault": "F3", "mechanism": "M2"}, None)
    with pytest.raises(RuntimeError, match="fault bootstrap"):
        RN.endpoints_for_run({"fault": "K2", "mechanism": "M1-kafka"}, None)


def test_live_sequence_endpoints_use_fail_closed_contract(tmp_path, monkeypatch):
    missing = tmp_path / "missing-endpoints.json"
    monkeypatch.delenv("WXEO_ENDPOINTS_FILE", raising=False)
    monkeypatch.setattr(RN, "ENDPOINTS_CONTRACT_PATH", str(missing))
    seq = object.__new__(RN.LiveSequence)
    seq.run = {"fault": "F3", "mechanism": "M2"}
    seq.r = RN.Runner(RN.RunnerOptions(results_dir=str(tmp_path / "res")))
    with pytest.raises(RuntimeError, match="endpoint contract"):
        seq._endpoints()

    good = tmp_path / "endpoints.json"
    good.write_text(json.dumps({
        "schema": "wxeo.endpoints.v1",
        "kafka": {"fault_bootstrap": "127.0.0.1:19091",
                  "group_member_proxies": ["127.0.0.1:19091", "127.0.0.1:19092",
                                           "127.0.0.1:19093"]},
        "postgres": {"proxy_host_port": 15433},
        "toxiproxy": {"api": "http://127.0.0.1:8474"}}))
    monkeypatch.setattr(RN, "ENDPOINTS_CONTRACT_PATH", str(good))
    ep = seq._endpoints()
    assert ep["producer"] == "127.0.0.1:19091"
    # non-Kafka-fault rows never require the contract
    native = object.__new__(RN.LiveSequence)
    native.run = {"fault": "F0", "mechanism": "M2"}
    native.r = seq.r
    assert native._endpoints()["bootstrap"] == "127.0.0.1:9091"


def test_lifecycle_block_records_a6_rate_and_b7_contract(tmp_path, monkeypatch):
    monkeypatch.delenv("WXEO_ENDPOINTS_FILE", raising=False)
    runner = RN.Runner(RN.RunnerOptions(results_dir=str(tmp_path / "res")))
    block = runner._lifecycle_block(None, {})
    assert "offered_rate" in block and block["offered_rate"] is None
    contract = block["endpoint_contract"]
    assert contract["fault_bootstrap"] == "127.0.0.1:19091"
    assert contract["path"] == "deploy/docker/endpoints.json"
    assert len(contract["sha256"]) == 64
