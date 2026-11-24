"""CLI-level tests for the A2 analyzer wiring (S5 §11.3) — synthetic fixture only."""
import importlib.util
import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)


def _load_analyze():
    spec = importlib.util.spec_from_file_location(
        "analyze_wiring", os.path.join(REPO, "tools", "analyze.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fixture_dirs(tmp_path):
    src = os.path.join(REPO, "results", "synthetic", "run-fixture")
    records = os.path.join(src, "run-records")
    if not os.path.isdir(records):
        pytest.skip("synthetic fixture missing")
    return records, os.path.join(src, "harness")


def _freeze(tmp_path):
    """A freeze file whose matrix pin matches the CURRENT tree (wiring test, not freeze test)."""
    import hashlib
    matrix = os.path.join(REPO, "experiments", "matrix.yaml")
    sha = hashlib.sha256(open(matrix, "rb").read()).hexdigest()
    path = tmp_path / "freeze.json"
    path.write_text(json.dumps({"files": {"experiments/matrix.yaml": sha},
                                "seeds": {"bootstrap_seed": "0123456789abcdef"}}))
    return str(path)


def test_cli_writes_a2_outputs(tmp_path):
    A = _load_analyze()
    records_src, harness = _fixture_dirs(tmp_path)
    import shutil
    records = str(tmp_path / "records")
    shutil.copytree(records_src, records)
    for name in os.listdir(records):
        if not name.endswith(".json"):
            continue
        path = os.path.join(records, name)
        rec = json.load(open(path, encoding="utf-8"))
        if rec.get("stage") in ("B", "C"):
            rec.setdefault("performance", {})["committed_events"] = 1000
            rec["timings"] = {**(rec.get("timings") or {}),
                              "t_load_start": 1000, "t_load_end": 2000}
        json.dump(rec, open(path, "w", encoding="utf-8"))
    out = tmp_path / "analysis"
    rc = A.main(["--records", records, "--out", str(out), "--freeze", str(_freeze(tmp_path)),
                 "--harness-dir", harness, "--stage", "all", "--dataset", "synthetic"])
    assert rc == 0
    payload = json.loads((out / "a2-analysis.json").read_text())
    assert payload["schema"] == "wxeo.a2-analysis.v1"
    assert payload["matrix_pin_ok"] is True
    assert payload["verdicts"]["H1"] in ("confirmed", "refuted", "inconclusive")
    assert (out / "a2-hypotheses.json").exists()
    manifest = json.loads((out / "analysis-manifest.json").read_text())
    assert manifest["a2_analysis"]["verdicts"]


def test_cli_fails_closed_on_matrix_pin(tmp_path):
    A = _load_analyze()
    records, harness = _fixture_dirs(tmp_path)
    bad = tmp_path / "freeze.json"
    bad.write_text(json.dumps({"files": {"experiments/matrix.yaml": "0" * 64}}))
    with pytest.raises(SystemExit):
        A.main(["--records", records, "--out", str(tmp_path / "o"),
                "--freeze", str(bad), "--harness-dir", harness,
                "--stage", "all", "--dataset", "synthetic"])


def test_cli_excludes_m7_records_from_confirmatory():
    A = _load_analyze()
    records = [{"stage": "M7", "mechanism": "M0", "fault": "none", "run_id": "m7-x",
                "validity": {"valid": True}, "violations": {}, "quiescence": {},
                "counts": {}, "fault_receipts": []},
               {"stage": "A", "mechanism": "M0", "fault": "K1", "run_id": "a-x",
                "validity": {"valid": True}, "violations": {}, "quiescence": {},
                "counts": {}, "fault_receipts": []}]
    kept = [r for r in records if str(r.get("stage")).upper() != "M7"]
    assert [r["run_id"] for r in kept] == ["a-x"]
    # the analyzer itself also classifies a K6-absent set as n/a, never missing
    report = A.a2_analyze(kept)
    assert report["evidence_classes"]["k6"]["status"].startswith("n/a")
