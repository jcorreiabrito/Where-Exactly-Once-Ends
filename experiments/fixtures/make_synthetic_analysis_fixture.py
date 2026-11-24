#!/usr/bin/env python3
"""Build the **synthetic 30-run fixture** used to dry-run the frozen analysis pipeline.

This produces 30 schema-valid ``wxeo.run.v1`` run records in
``results/synthetic/run-fixture/run-records/`` covering the code paths of
``tools/analyze.py`` (Clopper–Pearson CIs, McNemar/Fisher, Friedman/Nemenyi, Cliff's delta,
bootstrap CIs, zero-event bounds, decision table, macros, tables) and
``tools/make-figures.py`` (all four vector figures).

Design (5 mechanisms × 6 runs = 30), fully synthetic:
  * Stage A control, Stage A K3 (W1 duplicate defect for M0/M1x), Stage A K4 early probe
    (W2 loss defect for M3) — correctness paths;
  * Stage C 10k/p3, 10k/p6, 100k/p3 blocks — performance paths (Friedman has k=5, N=3 blocks).

The records are labelled ``SYNTHETIC FIXTURE`` in ``validity.notes``; ``tools/analyze.py``
propagates that label into every output and ``tools/make-figures.py`` renders a red banner.
**Nothing here is measurement data and no value may be cited as a result.**

Usage::

    python experiments/fixtures/make_synthetic_analysis_fixture.py
    python tools/analyze.py --records results/synthetic/run-fixture/run-records \
        --out results/synthetic/analysis --harness-dir results/harness
"""

from __future__ import annotations

import hashlib
import json
import os
import sys

import numpy as np

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from experiments import validate_record as VR  # noqa: E402

MECHANISMS = ["M0", "M1x", "M2", "M3", "M4"]
FIXTURE_BASE_SEED = "5e11f1c7a8b9d0e1"

#: per-mechanism synthetic performance profile (clearly synthetic constants, not measurements)
PERF_PROFILE = {
    # throughput at 100k/p6 (events/s), p50 (ms), p99 (ms), t_consistent (ms)
    "M0": {"thr": 1500.0, "p50": 4.0, "p99": 22.0, "ttc": 2500},
    "M1x": {"thr": 1150.0, "p50": 6.0, "p99": 31.0, "ttc": 3200},
    "M2": {"thr": 650.0, "p50": 12.0, "p99": 48.0, "ttc": 8000},
    "M3": {"thr": 1050.0, "p50": 7.0, "p99": 26.0, "ttc": 3500},
    "M4": {"thr": 980.0, "p50": 7.5, "p99": 24.0, "ttc": 3800},
}
LOAD_SCALE = {(10_000, 6): 0.92, (10_000, 3): 0.80, (100_000, 3): 0.90,
              (100_000, 6): 1.00, (10_000, 12): 0.95, (100_000, 12): 1.03}


def _hex64(seed_text: str) -> str:
    return hashlib.sha256(seed_text.encode("utf-8")).hexdigest()


def _run_plan() -> list[dict]:
    """5 mechanisms × 6 runs: 3 correctness (A) + 3 performance (C)."""
    runs = []
    for mi, mech in enumerate(MECHANISMS):
        # --- correctness: control, K3 (W1), K4 early probe (W2) ---------------------------
        runs.append({"mechanism": mech, "stage": "A", "fault": "none", "fault_level": "none",
                     "ordering": "strict", "load": 1_000, "partitions": 3, "rep": 0,
                     "cell_kind": "ctrl", "defect": None})
        runs.append({"mechanism": mech, "stage": "A", "fault": "K3", "fault_level": "L1",
                     "ordering": "strict", "load": 1_000, "partitions": 3, "rep": 0,
                     "cell_kind": "K3", "defect": "dup" if mech in ("M0", "M1x") else None})
        runs.append({"mechanism": mech, "stage": "A", "fault": "K4", "fault_level": "L1",
                     "ordering": "early", "load": 1_000, "partitions": 3, "rep": 0,
                     "cell_kind": "K4", "defect": "loss" if mech == "M3" else None})
        # --- performance: three Stage C blocks ------------------------------------------
        for (load, parts) in ((10_000, 6), (10_000, 3), (100_000, 3)):
            runs.append({"mechanism": mech, "stage": "C", "fault": "F0", "fault_level": "none",
                         "ordering": "strict", "load": load, "partitions": parts, "rep": 0,
                         "cell_kind": "C", "defect": None})
    assert len(runs) == 30, len(runs)
    return runs


def _performance_values(run: dict, run_index: int) -> dict:
    """Deterministic synthetic performance values (NOT measurements)."""
    profile = PERF_PROFILE[run["mechanism"]]
    key = (run["load"], run["partitions"])
    scale = LOAD_SCALE.get(key, 1.0)
    rng = np.random.default_rng(int(_hex64(f"perf|{run['mechanism']}|{key}")[:16], 16))
    jitter = float(rng.normal(0.0, 0.04))
    thr = profile["thr"] * scale * (1.0 + jitter)
    p50 = profile["p50"] / scale * (1.0 + float(rng.normal(0, 0.05)))
    p99 = profile["p99"] / scale * (1.0 + float(rng.normal(0, 0.06)))
    p999 = p99 * 1.8
    ttc = int(profile["ttc"] * (1.0 + float(rng.normal(0, 0.1))))
    return {
        "throughput_eps_median": round(thr, 2),
        "p50_ms": round(p50, 2), "p90_ms": round(p50 * 1.7, 2),
        "p99_ms": round(p99, 2), "p999_ms": round(p999, 2),
        "max_ms": round(p999 * 1.4, 2),
        "t_consistent_ms": ttc, "censored": False,
    }


def _receipts(run_id: str, fault: str) -> list[dict]:
    if fault in ("F0", "none"):
        return []
    failpoint = {"K3": "after_effect_commit", "K4": "before_effect_commit"}.get(fault, fault)
    return [
        {"ts_utc": "2026-11-24T00:00:00.000Z", "run_id": run_id, "fault": failpoint,
         "event": "fired", "detail": f"action=crash;point={failpoint};hit=1;after=1;"
                                     "mechanism=synthetic;instance=1", "level": "L1"},
        {"ts_utc": "2026-11-24T00:00:05.000Z", "run_id": run_id, "fault": failpoint,
         "event": "restored", "detail": "S4: instance restarted after terminal crash failpoint",
         "level": "L1"},
    ]


def _counts_for(run: dict) -> tuple[dict, dict]:
    """Synthetic counts + violation vector consistent with the modelling defect."""
    events = run["load"]
    counts = {
        "produced": events, "consumed_estimate": events,
        "expected_postings": 2 * events, "observed_postings": 2 * events,
        "dup_postings": 0, "dup_entries": 0, "lost_postings": 0, "lost_entries": 0,
        "imbalanced_entries": 0, "mutated_postings": 0, "spurious_postings": 0,
    }
    violations = {"v1": False, "v2": False, "v3": False, "v4": False,
                  "violating_run": False}
    defect = run["defect"]
    if defect == "dup":
        counts["dup_postings"] = 2 * (events // 4)
        counts["dup_entries"] = events // 4
        violations["v3"] = True
    elif defect == "loss":
        counts["lost_postings"] = events // 2
        counts["lost_entries"] = events // 4
        violations["v4"] = True
    violations["violating_run"] = any(violations[k] for k in ("v1", "v2", "v3", "v4"))
    return counts, violations


def build_record(run: dict, index: int, *, git_sha: str, master_seed: str) -> dict:
    mech = run["mechanism"]
    stage = run["stage"]
    fault = run["fault"]
    load = run["load"]
    parts = run["partitions"]
    order_token = "early" if run["ordering"] == "early" else "strict"
    cond = fault.lower() if fault in ("F0", "F1", "F2", "F3", "F4a", "F4b", "F5", "F6",
                                      "K1", "K2", "K3", "K4", "K5", "K6", "K7") else "ctrl"
    mech_token = {"M0": "m0", "M1-kafka": "m1-kafka", "M1x": "m1x", "M2": "m2", "M3": "m3",
                  "M4": "m4", "M2+M3": "m2m3", "M4+M3": "m4m3", "M1x+M3": "m1xm3"}[mech]
    load_token = {1_000: "1k", 10_000: "10k", 100_000: "100k", 1_000_000: "1m"}[load]
    run_id = (f"{stage.lower()}-{mech_token}-{cond}-{load_token}-p{parts}-"
              f"r{run['rep']:02d}")
    data_seed = _hex64(f"{master_seed}|synthetic|{index}")[:16]
    count, viol = _counts_for(run)
    perf = _performance_values(run, index)
    if mech == "M0" and run["cell_kind"] == "C" and run["partitions"] == 3:
        perf["censored"] = True  # exercise the censored/outcome path
        perf["t_consistent_ms"] = None
    config = {
        "mechanism": mech, "ordering_variant": run["ordering"],
        "load_events": load, "partitions": parts, "consumer_instances": 2,
        "synthetic_fixture": True,
    }
    config["config_sha256"] = hashlib.sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    timings = {
        "t_start": 1_764_000_000_000 + index * 100_000,
        "t_load_start": 1_764_000_000_000 + index * 100_000 + 5_000,
        "t_load_end": 1_764_000_000_000 + index * 100_000 + 95_000,
        "t_fault_fire": (1_764_000_000_000 + index * 100_000 + 50_000
                         if fault not in ("F0", "none") else None),
        "t_restore": (1_764_000_000_000 + index * 100_000 + 55_000
                      if fault not in ("F0", "none") else None),
        "t_quiesce": 1_764_000_000_000 + index * 100_000 + 105_000,
        "t_observation": 1_764_000_000_000 + index * 100_000 + 105_100,
        "t_end": 1_764_000_000_000 + index * 100_000 + 110_000,
    }
    record = {
        "schema": VR.SCHEMA_VERSION,
        "run_id": run_id,
        "stage": "pilot" if stage == "V" else stage,
        "cell_id": f"{stage}.{mech}.{cond}.{load_token}.p{parts}",
        "mechanism": mech,
        "ordering_variant": run["ordering"],
        "fault": fault,
        "fault_level": run["fault_level"],
        "load_events": load,
        "partitions": parts,
        "consumer_instances": 2,
        "rep": run["rep"],
        "data_seed": data_seed,
        "injection_seed": _hex64(f"{master_seed}|synthetic|inj|{index}")[:16],
        "master_seed": master_seed,
        "git_sha": git_sha,
        "host": {"os_build": "SYNTHETIC", "cpu_model": "SYNTHETIC", "logical_cpus": 12,
                 "ram_gb": 16.0, "disk_free_start_gb": 10.0},
        "versions": {"java": "synthetic", "kafka": "synthetic", "postgresql": "synthetic",
                     "toxiproxy": "synthetic", "powershell": "synthetic", "python": "3.12"},
        "config": config,
        "process_limits": {"synthetic_fixture": True},
        "timings": timings,
        "quiescence": {
            "lag_per_partition": {str(p): 0 for p in range(parts)},
            "quiet_period_ms": 10_000,
            "censored": bool(perf["censored"]),
            "t_consistent_ms": perf["t_consistent_ms"],
        },
        "performance": {
            "throughput_eps_median": perf["throughput_eps_median"],
            "p50_ms": perf["p50_ms"], "p90_ms": perf["p90_ms"], "p99_ms": perf["p99_ms"],
            "p999_ms": perf["p999_ms"], "max_ms": perf["max_ms"],
            "histogram_path": None,
            "warmup_events_excluded": max(2_000, int(load * 0.1)),
            "perf_window": {"synthetic": True},
        },
        "counts": count,
        "violations": viol,
        "publication": {"pub_records": count["expected_postings"] // 2,
                        "dup_records": 0, "lost_records": 0, "divergence": 0},
        "oracle": {
            "gt_sha256": _hex64(f"gt|{run_id}"),
            "expected_sha256": _hex64(f"gt|{run_id}"),
            "source_integrity": True,
            "oracle_script_sha256": _hex64("synthetic-oracle"),
            "result_sha256": _hex64(f"result|{run_id}"),
        },
        "fault_receipts": _receipts(run_id, fault),
        "validity": {
            "valid": True, "excluded": False, "exclude_reason": None,
            "residue_check": True, "clock_check": True,
            "disk_free_start_gb": 10.0,
            "notes": ["SYNTHETIC FIXTURE for analysis/figure dry-run — NOT a measurement, "
                      "never part of results/raw"],
        },
        "artifacts": {},
        "stdout_path": None,
        "stderr_path": None,
        "exit_status": {"consumer": "synthetic", "load_generator": "synthetic"},
    }
    if order_token == "early":
        record["ordering_variant"] = "early"
    return record


def main(argv=None) -> int:
    out_root = os.path.join(_REPO, "results", "synthetic", "run-fixture")
    records_dir = os.path.join(out_root, "run-records")
    os.makedirs(records_dir, exist_ok=True)
    git_sha = _git_sha()
    master_seed = _master_seed()
    runs = _run_plan()
    written = []
    for index, run in enumerate(runs):
        record = build_record(run, index, git_sha=git_sha, master_seed=master_seed)
        errors = VR.validate_record(record)
        if errors:
            raise SystemExit(f"synthetic record invalid: {record['run_id']}:\n"
                             + "\n".join(errors))
        path = os.path.join(records_dir, f"run_{record['run_id']}.json")
        with open(path, "w", encoding="utf-8", newline="") as fh:
            json.dump(record, fh, indent=2)
            fh.write("\n")
        written.append(os.path.relpath(path, _REPO))
    marker = os.path.join(out_root, "SYNTHETIC-NOT-RESULTS.md")
    with open(marker, "w", encoding="utf-8", newline="") as fh:
        fh.write("# SYNTHETIC FIXTURE — NOT RESULTS\n\n"
                 "These 30 run records are **synthetic**, machine-generated by\n"
                 "`experiments/fixtures/make_synthetic_analysis_fixture.py`. They exist only to\n"
                 "exercise the frozen analysis pipeline (`tools/analyze.py`) and figure\n"
                 "generator (`tools/make-figures.py`) before real data exists. Every number in\n"
                 "them is invented by construction and must never be cited, plotted as a\n"
                 "result, or moved into `results/raw/`.\n")
    print(json.dumps({"records_dir": records_dir, "n_records": len(written),
                      "synthetic": True,
                      "note": "SYNTHETIC FIXTURE — not results"}, indent=2))
    return 0


def _git_sha() -> str:
    head = os.path.join(_REPO, ".git", "HEAD")
    try:
        text = open(head, encoding="utf-8").read().strip()
    except OSError:
        return "0000000"
    if text.startswith("ref: "):
        ref_path = os.path.join(_REPO, ".git", *text[5:].split("/"))
        if os.path.exists(ref_path):
            return open(ref_path, encoding="utf-8").read().strip()[:40]
    return text[:40] if len(text) >= 7 else "0000000"


def _master_seed() -> str:
    try:
        import yaml

        doc = yaml.safe_load(open(os.path.join(_REPO, "experiments", "matrix.yaml"),
                                  encoding="utf-8"))
        return doc["seeds"]["master_seed"]
    except Exception:  # noqa: BLE001
        return "90650742b511adfb662272117444cf43"


if __name__ == "__main__":
    raise SystemExit(main())
