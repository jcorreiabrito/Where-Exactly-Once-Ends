"""A2 analyzer + R12 rates tests (synthetic fixtures only; no live data)."""
import importlib.util
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)


def _load_analyze():
    spec = importlib.util.spec_from_file_location(
        "analyze_a2_test", os.path.join(REPO, "tools", "analyze.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


A = _load_analyze()


# ---- decision-rule table ------------------------------------------------------------------
def test_decision_rules_on_item14_fixture():
    records, expected = A.a2_item14_fixture()
    report = A.a2_analyze(records)
    assert report["H1"]["verdict"] == expected["h1_verdict"]
    pooled = report["H1"]["pooled"]
    assert (pooled["b"], pooled["c"]) == (expected["h1_pooled_b"],
                                          expected["h1_pooled_c"])
    assert report["H3"]["verdict"] == expected["h3_verdict"]
    assert report["H3"]["loss_arm_observed"] >= 1
    assert report["H2"]["clean_fail"] >= expected["h2"]["censored_fails"]
    assert report["P11"]["confirmed"] is True
    assert report["evidence_classes"]["k6"]["status"] == "n/a (structurally unreachable)"
    assert report["record_validation"]["n_records"] > 0
    assert report["record_validation"]["equation_violations"] == []


def test_pooled_stratified_sign_test_exact_values():
    small = A.a2_pooled_sign_test({"A-K2": [(True, False)] * 3,
                                   "B-F1": [(True, False)] * 2})
    assert (small["b"], small["c"]) == (5, 0)
    assert small["cells_contributing"] == 2
    assert abs(small["p_one_sided"] - 1 / 32) < 1e-12
    assert small["significant"] is True and small["analysable"] is True
    four = A.a2_pooled_sign_test({"A-K2": [(True, False)] * 4})
    assert abs(four["p_one_sided"] - 1 / 16) < 1e-12  # 0.0625 > alpha -> not significant
    assert four["significant"] is False
    empty = A.a2_pooled_sign_test({"A-K2": [], "B-F1": []})
    assert empty["analysable"] is False and empty["significant"] is False


def test_v1a_v1b_derivation_and_restated_p11():
    records, _ = A.a2_item14_fixture()
    dup = [r for r in records if r["run_id"] == "A-M0-K3-r00"][0]
    assert A.a2_derive_v1(dup) == (True, False)
    v5 = [r for r in records if r["violations"]["v5"]][0]
    assert A.a2_derive_v1(v5) == (False, False)
    p11 = A.a2_p11(records)
    assert p11["test"].startswith("v3=1 and v1b=0")
    assert p11["confirmed"] is True


def test_v5_is_a_violation_not_an_invalid_run():
    records, _ = A.a2_item14_fixture()
    v5 = [r for r in records if r["violations"]["v5"]][0]
    assert v5["validity"]["valid"] is True
    assert v5["violations"]["violating_run"] is True


def test_censoring_counts_as_clean_fail():
    record = {"violations": {"violating_run": False},
              "quiescence": {"censored": True}}
    assert A.a2_clean_fail(record) is True
    assert A.a2_clean_fail({"violations": {"violating_run": False},
                            "quiescence": {"censored": False}}) is False


def test_h5_vacuity_rejection():
    # no canonical-clean mechanism with adequate power -> inconclusive, never confirmed
    records, _ = A.a2_item14_fixture()
    report = A.a2_analyze(records)
    h5 = report["H5"]
    assert h5["vacuity_guard"]
    if h5["adequate_power"] == 0:
        assert h5["verdict"] == "inconclusive"
    # a fully clean M1x+M3 with no performance data must not be declared dominated
    clean = [{"stage": "A", "mechanism": "M1x+M3", "fault": f, "validity": {"valid": True},
              "violations": {"violating_run": False}, "quiescence": {"censored": False},
              "counts": {}} for f in ("K1", "K2", "K3", "K5")]
    assert A.a2_h5a(clean)["verdict"] in ("inconclusive", "confirmed")


def test_matrix_pin_fails_closed():
    records, _ = A.a2_item14_fixture()
    try:
        A.a2_analyze(records, matrix_pin_ok=False)
    except SystemExit as exc:
        assert "matrix hash" in str(exc)
    else:
        raise AssertionError("analyzer must fail closed when the matrix pin mismatches")


# ---- R12 rates ----------------------------------------------------------------------------
def test_rates_frozen_rule_deterministic():
    from experiments import rates as R

    samples = [
        {"load_events": 10_000, "partitions": 6, "mechanism": "M0",
         "committed_events": 9000, "window_start_s": 0.0, "window_end_s": 10.0},
        {"load_events": 10_000, "partitions": 6, "mechanism": "M2",
         "committed_events": 8000, "window_start_s": 0.0, "window_end_s": 10.0},
        {"load_events": 100_000, "partitions": 6, "mechanism": "M0",
         "committed_events": 90000, "window_start_s": 0.0, "window_end_s": 100.0},
        {"load_events": 100_000, "partitions": 6, "mechanism": "M2",
         "committed_events": 70000, "window_start_s": 0.0, "window_end_s": 100.0},
    ]
    doc = R.derive_rates(samples, matrix_sha256="abc")
    # warm-up-excluded capacity: M2 10k = 8000/(9s) = 888.9 -> floor(0.5*888.9)=444
    assert doc["fixed_rate"]["10000|p6"]["rate"] == 444
    # 100k: M2 = 70000/90 = 777.8 -> 388 (minimum mechanism)
    assert doc["fixed_rate"]["100000|p6"]["rate"] == 388
    assert doc["fixed_rate"]["100000|p6"]["bottleneck_capacity"] == 777.777778
    again = R.derive_rates(list(reversed(samples)), matrix_sha256="abc")
    assert again == doc
    assert R.g7_checks(doc, ["10000|p6", "100000|p6"], need_saturation=True)["ok"] is True
    assert R.g7_checks(doc, ["10000|p6"], need_saturation=True)["ok"] is True
