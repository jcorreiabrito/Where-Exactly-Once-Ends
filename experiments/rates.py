#!/usr/bin/env python3
"""Fixed/saturation rate derivation (R12), frozen by the author at P2.

Frozen rule (A3.3/R12): the fixed **rate** for a (load, partitions) block is
``floor(0.5 * min_over_mechanisms(capacity))`` where capacity is the committed event rate
measured from the pilot's warm-up-excluded windows (``WARMUP_FRACTION = 0.10`` of each run's
streaming window is discarded). The saturation rate for 100k/P6 is derived by the same rule
over the saturation-mode capacity samples. The rule is deterministic: inputs are sorted by
(load, partitions, mechanism) and the minimum is taken, so it does not depend on file order.

G7 wiring expectations: Stage B/C runs require a rates document with
``schema="wxeo.rates.v1"``, a ``fixed_rate`` entry for every (load, partitions) block used by
the stage, ``saturation_rate`` for Stage C saturation, and ``provenance.matrix_sha256`` pinning
the matrix generation that produced the calibration records.
"""
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

WARMUP_FRACTION = 0.10
FIXED_RATE_FRACTION = 0.5
MIN_RATE = 1


def capacity_from_window(committed_events, window_start_s, window_end_s,
                         min_events=10):
    """Committed events/s in the warm-up-excluded streaming window (deterministic)."""
    start = float(window_start_s) + WARMUP_FRACTION * (float(window_end_s) - float(window_start_s))
    span = float(window_end_s) - start
    if span <= 0 or float(committed_events) < min_events:
        return None
    return float(committed_events) / span


def derive_rates(samples, matrix_sha256=None, source="pilot calibration records"):
    """samples: iterable of {load_events, partitions, mechanism, committed_events,
    window_start_s, window_end_s, mode ("fixed"|"saturation")}."""
    fixed_caps = {}
    sat_caps = {}
    for sample in sorted(samples, key=lambda s: (int(s["load_events"]), int(s["partitions"]),
                                                 str(s["mechanism"]))):
        cap = capacity_from_window(sample["committed_events"],
                                   sample["window_start_s"], sample["window_end_s"])
        if cap is None:
            continue
        key = (int(sample["load_events"]), int(sample["partitions"]))
        bucket = sat_caps if sample.get("mode") == "saturation" else fixed_caps
        bucket.setdefault(key, []).append({"mechanism": sample["mechanism"],
                                           "capacity_events_s": round(cap, 6)})
    def _rates(buckets):
        out = {}
        for key in sorted(buckets):
            minima = min(b["capacity_events_s"] for b in buckets[key])
            rate = max(MIN_RATE, int(FIXED_RATE_FRACTION * minima))
            out[f"{key[0]}|p{key[1]}"] = {
                "rate": rate, "bottleneck_capacity": minima,
                "mechanisms_considered": [b["mechanism"] for b in buckets[key]],
                "rule": "floor(0.5 * min capacity)",
            }
        return out
    fixed = _rates(fixed_caps)
    saturation = _rates(sat_caps) or {k: dict(v, rule="floor(0.5 * min saturation capacity)")
                                      for k, v in fixed.items() if k.startswith("100000")}
    return {
        "schema": "wxeo.rates.v1",
        "generated_by": "experiments/rates.py",
        "frozen_rule": {"fraction_of_min_capacity": FIXED_RATE_FRACTION,
                        "warmup_fraction_excluded": WARMUP_FRACTION,
                        "capacity_definition": "committed events / warm-up-excluded seconds",
                        "deterministic_minimum": True},
        "source": source,
        "matrix_sha256": matrix_sha256,
        "fixed_rate": fixed,
        "saturation_rate": saturation,
    }


def g7_checks(rates_doc, required_blocks, need_saturation=False):
    """G7 wiring expectations: the doc must cover every block a stage will execute."""
    problems = []
    if rates_doc.get("schema") != "wxeo.rates.v1":
        problems.append("schema != wxeo.rates.v1")
    for block in required_blocks:
        entry = (rates_doc.get("fixed_rate") or {}).get(block)
        if not entry or int(entry.get("rate") or 0) < MIN_RATE:
            problems.append(f"missing/invalid fixed_rate for {block}")
    if need_saturation and not rates_doc.get("saturation_rate"):
        problems.append("missing saturation_rate")
    if not (rates_doc.get("provenance") or {}).get("matrix_sha256") and \
            not rates_doc.get("matrix_sha256"):
        problems.append("missing matrix pin")
    return {"ok": not problems, "problems": problems}


def _load_records(records_dir):
    import glob
    samples = []
    for path in sorted(glob.glob(os.path.join(records_dir, "run_*.json"))):
        try:
            rec = json.load(open(path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        if rec.get("stage") not in ("pilot", "cal", "m7", "M7cal") or \
                rec.get("mechanism") == "M1-kafka":
            continue
        perf = rec.get("performance") or {}
        timings = rec.get("timings") or {}
        if not perf.get("committed_events") or not timings.get("t_load_start"):
            continue
        samples.append({
            "load_events": rec.get("load_events"), "partitions": rec.get("partitions"),
            "mechanism": rec.get("mechanism"),
            "committed_events": perf["committed_events"],
            "window_start_s": timings["t_load_start"] / 1000.0,
            "window_end_s": (timings.get("t_load_end") or timings["t_load_start"]) / 1000.0,
            "mode": "saturation" if rec.get("load_mode") == "saturation" else "fixed",
        })
    return samples


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="Derive fixed/saturation rates (R12)")
    parser.add_argument("--records-dir", default=os.path.join(REPO, "results", "raw",
                                                              "run-records"))
    parser.add_argument("--out", default=os.path.join(REPO, "experiments", "rates.json"))
    parser.add_argument("--write", action="store_true",
                        help="write experiments/rates.json (campaign-script form: rates.py --write)")
    args = parser.parse_args(argv)
    samples = _load_records(args.records_dir)
    if not samples:
        print("R12: no pilot calibration records with streaming windows yet; refusing to "
              "write rates.json (Stage B/C stay blocked)", file=sys.stderr)
        return 2
    import hashlib
    matrix_sha = None
    mpath = os.path.join(REPO, "experiments", "matrix.sha256")
    if os.path.exists(mpath):
        for line in open(mpath, encoding="utf-8"):
            if line.strip().endswith("matrix.yaml"):
                matrix_sha = line.split()[0]
    doc = derive_rates(samples, matrix_sha256=matrix_sha)
    json.dump(doc, open(args.out, "w", encoding="utf-8", newline=""), indent=2)
    open(args.out, "a", encoding="utf-8").write("\n")
    print(json.dumps({"rates": os.path.relpath(args.out, REPO),
                      "fixed_blocks": sorted(doc["fixed_rate"]),
                      "saturation_blocks": sorted(doc["saturation_rate"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
