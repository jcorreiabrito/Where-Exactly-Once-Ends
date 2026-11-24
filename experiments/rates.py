#!/usr/bin/env python3
"""M7/R12 rates derivation — fail-closed, frozen rule ids (S5 §11.4).

Modes (one frozen rule module, author decision pending on coverage):
* ``M7-R1-mapping`` (default): measured P6 capacity for 10k/100k; derive 1k from 10k/P6,
  1M from 100k/P6, and P-scaling by P/6; every assumption recorded per block; spot-checks
  for derived blocks must be within ±25 % else validation fails closed.
* ``M7-R2-full-coverage``: all nine block combos must be measured, missing => SystemExit.
Fixed rate = floor(0.5 * capacity); saturation = floor(2.5 * max measured saturation
capacity), and missing saturation samples fail closed.
"""
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

WARMUP_FRACTION = 0.10
FIXED_RATE_FRACTION = 0.5
SATURATION_MULTIPLIER = 2.5
SPOT_CHECK_TOLERANCE = 0.25
MIN_RATE = 1
RULE_ID_MAPPING = "A5.partition-scale.v1"
RULE_ID_FULL = "M7-R2-full-coverage"
SATURATION_RULE_ID = "SAT-2.5x-max"
REQUIRED_BLOCKS = (
    ("A", 1_000, 3, "fixed"), ("B", 10_000, 3, "fixed"), ("B", 10_000, 6, "fixed"),
    ("B", 10_000, 12, "fixed"), ("C", 100_000, 3, "fixed"), ("C", 100_000, 6, "fixed"),
    ("C", 100_000, 12, "fixed"), ("C", 1_000_000, 6, "fixed"),
    ("C-sat", 100_000, 6, "saturation"),
)


def capacity_from_window(committed_events, window_start_s, window_end_s, min_events=10):
    start = float(window_start_s) + WARMUP_FRACTION * (
        float(window_end_s) - float(window_start_s))
    span = float(window_end_s) - start
    if span <= 0 or float(committed_events) < min_events:
        return None
    return float(committed_events) / span


def _measurements(samples):
    caps = {}
    for sample in sorted(samples, key=lambda s: (int(s["load_events"]), int(s["partitions"]),
                                                 str(s["mechanism"]),
                                                 str(s.get("mode") or "fixed"))):
        cap = capacity_from_window(sample["committed_events"], sample["window_start_s"],
                                   sample["window_end_s"])
        if cap is None:
            continue
        key = (int(sample["load_events"]), int(sample["partitions"]),
               str(sample.get("mode") or "fixed"))
        caps.setdefault(key, {})[str(sample["mechanism"])] = round(cap, 6)
    return caps


def _block_key(load, partitions, mode="fixed"):
    return f"{load}|p{partitions}" + ("|sat" if mode == "saturation" else "")


def _min_capacity(measured):
    return min(measured.values()) if measured else None


#: A5.2 spot-check blocks that must have evidence in the mapping rule
SPOT_CHECK_BLOCKS = ((1_000, 3), (10_000, 3), (10_000, 12), (100_000, 3),
                     (100_000, 12), (1_000_000, 6))


def _derive_mapping(caps):
    base = {(load): _min_capacity(m) for (load, partitions, mode), m in caps.items()
            if partitions == 6 and mode == "fixed"}
    fixed, derived = {}, {}
    for _stage, load, partitions, mode in REQUIRED_BLOCKS:
        if mode == "saturation":
            continue
        key = _block_key(load, partitions, mode)
        measured = caps.get((load, partitions, mode))
        if measured and partitions == 6:
            cap = _min_capacity(measured)
            fixed[key] = {"rate": max(MIN_RATE, int(FIXED_RATE_FRACTION * cap)),
                          "capacity": cap, "source": "measured",
                          "rule": "floor(0.5 * min measured capacity at P6)"}
            continue
        if load in base and partitions == 6:
            cap, note = base[load], f"measured at {load}/P6"
        elif load in base:
            cap = base[load] * (partitions / 6.0)
            note = f"derived from {load}/P6 capacity x P/6 (linear partition scaling)"
        elif load == 1_000 and 10_000 in base:
            cap = base[10_000] * (partitions / 6.0)
            note = "derived from 10k/P6 capacity x P/6 (same events/s assumption)"
        elif load == 1_000_000 and 100_000 in base:
            cap = base[100_000]
            note = "derived from 100k/P6 capacity (same events/s assumption)"
        else:
            raise SystemExit(f"rates: cannot derive block {key}: no P6 measurement for load "
                             f"{load}")
        fixed[key] = {"rate": max(MIN_RATE, int(FIXED_RATE_FRACTION * cap)),
                      "capacity": round(cap, 6), "source": "derived", "assumption": note,
                      "rule": "floor(0.5 * derived capacity)"}
        derived[key] = {"predicted_capacity": round(cap, 6), "assumption": note}
    checks = {}
    for key, entry in derived.items():
        load = int(key.split("|")[0])
        part = int(key.split("p")[1])
        measured = caps.get((load, part, "fixed"))
        if not measured:
            continue
        actual = _min_capacity(measured)
        pred = entry["predicted_capacity"]
        rel = abs(pred - actual) / actual if actual else 1.0
        checks[key] = {"predicted": pred, "measured": round(actual, 6),
                       "relative_error": round(rel, 6),
                       "within_25pct": rel <= SPOT_CHECK_TOLERANCE}
    missing = [f"{load}|p{p}" for load, p in SPOT_CHECK_BLOCKS
               if not caps.get((load, p, "fixed"))]
    failed = [k for k, v in checks.items() if not v["within_25pct"]]
    validated = (not failed) and (not missing) and bool(checks)
    return fixed, derived, {"validated": validated,
                            "status": "ok" if validated else "failed",
                            "failed_blocks": failed, "missing_spot_checks": missing,
                            "spot_checks": checks}


def _derive_full(caps):
    fixed, missing = {}, []
    for _stage, load, partitions, mode in REQUIRED_BLOCKS:
        if mode == "saturation":
            continue
        measured = caps.get((load, partitions, mode))
        if not measured:
            missing.append(_block_key(load, partitions, mode))
            continue
        cap = _min_capacity(measured)
        fixed[_block_key(load, partitions, mode)] = {
            "rate": max(MIN_RATE, int(FIXED_RATE_FRACTION * cap)), "capacity": cap,
            "source": "measured", "rule": "floor(0.5 * min measured capacity)"}
    if missing:
        raise SystemExit(f"rates: full-coverage mode missing measured blocks: {missing}")
    return fixed, {}, {"validated": True, "status": "ok", "failed_blocks": [],
                       "missing_spot_checks": [], "spot_checks": {}}


def derive_rates(samples, matrix_sha256=None, mode=RULE_ID_MAPPING,
                 source="pilot M7 calibration records"):
    caps = _measurements(samples)
    if mode == RULE_ID_FULL:
        fixed, derived, validation = _derive_full(caps)
        rule_id = RULE_ID_FULL
    else:
        fixed, derived, validation = _derive_mapping(caps)
        rule_id = RULE_ID_MAPPING
    sat_measured = caps.get((100_000, 6, "saturation"))
    if not sat_measured:
        raise SystemExit(
            "rates: no saturation-mode samples at 100k/P6; saturation rule "
            f"{SATURATION_RULE_ID} (2.5 x max measured capacity) refuses to fabricate an "
            "entry from fixed samples (S5 §11.4(iv))")
    sat_cap = max(sat_measured.values())
    saturation = {_block_key(100_000, 6, "saturation"):
                  {"rate": max(MIN_RATE, int(SATURATION_MULTIPLIER * sat_cap)),
                   "capacity": round(sat_cap, 6),
                   "rule": "floor(2.5 * max measured capacity)",
                   "source": "measured saturation samples"}}
    return {"schema": "wxeo.rates.v2", "generated_by": "experiments/rates.py",
            "rule_id": rule_id, "saturation_rule_id": SATURATION_RULE_ID,
            "frozen_rule": {"fraction_of_min_capacity": FIXED_RATE_FRACTION,
                            "warmup_fraction_excluded": WARMUP_FRACTION,
                            "saturation_multiplier": SATURATION_MULTIPLIER,
                            "spot_check_tolerance": SPOT_CHECK_TOLERANCE},
            "source": source, "matrix_sha256": matrix_sha256,
            "calibrated_blocks": sorted(_block_key(*k) for k, v in caps.items() if v),
            "derived_blocks": sorted(derived), "assumptions": derived,
            "fixed_rate": fixed, "saturation_rate": saturation,
            "validated": bool(validation.get("validated")),
            "validation": validation}


def g7_checks(rates_doc, required_blocks, need_saturation=True):
    problems = []
    if rates_doc.get("schema") != "wxeo.rates.v2":
        problems.append("schema != wxeo.rates.v2")
    if rates_doc.get("rule_id") not in (RULE_ID_MAPPING, RULE_ID_FULL):
        problems.append("unknown rule_id")
    if need_saturation and not rates_doc.get("saturation_rate"):
        problems.append("saturation_rate entry missing")
    validation = rates_doc.get("validation") or {}
    if not (rates_doc.get("validated") and validation.get("validated")):
        problems.append("A5 spot-check validation failed (validated=false)")
    for block in required_blocks:
        entry = (rates_doc.get("fixed_rate") or {}).get(block)
        if not entry or int(entry.get("rate") or 0) < MIN_RATE:
            problems.append(f"missing/invalid fixed_rate for {block}")
    if not rates_doc.get("matrix_sha256"):
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
        if str(rec.get("stage")) not in ("M7", "pilot", "B", "C"):
            continue
        if rec.get("mechanism") == "M1-kafka":
            continue
        perf, timings = rec.get("performance") or {}, rec.get("timings") or {}
        if not isinstance(perf.get("committed_events"), int):
            continue
        t0, t1 = timings.get("t_load_start"), timings.get("t_load_end")
        if not isinstance(t0, int) or not isinstance(t1, int) or t1 <= t0:
            continue
        samples.append({"load_events": rec.get("load_events"),
                        "partitions": rec.get("partitions"),
                        "mechanism": rec.get("mechanism"),
                        "committed_events": perf["committed_events"],
                        "window_start_s": t0 / 1000.0, "window_end_s": t1 / 1000.0,
                        "mode": "saturation" if (rec.get("config") or {}).get(
                            "runner", {}).get("rate_mode") == "saturation" else "fixed"})
    return samples


def _matrix_pin():
    mpath = os.path.join(REPO, "experiments", "matrix.sha256")
    if not os.path.exists(mpath):
        return None
    for line in open(mpath, encoding="utf-8"):
        if line.strip().endswith("matrix.yaml"):
            return line.split()[0]
    return None


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="M7/R12 rates derivation (fail-closed)")
    parser.add_argument("--records-dir", default=os.path.join(REPO, "results", "raw",
                                                              "run-records"))
    parser.add_argument("--out", default=os.path.join(REPO, "experiments", "rates.json"))
    parser.add_argument("--mode", choices=[RULE_ID_MAPPING, RULE_ID_FULL],
                        default=RULE_ID_MAPPING)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    if args.check:
        doc = json.load(open(args.out, encoding="utf-8"))
        blocks = [_block_key(load, p, m) for _, load, p, m in REQUIRED_BLOCKS]
        result = g7_checks(doc, blocks)
        print(json.dumps(result, indent=2))
        return 0 if result["ok"] else 1
    samples = _load_records(args.records_dir)
    if not samples:
        print("R12: no M7/pilot calibration records with performance.committed_events and a "
              "streaming window; refusing to write rates.json (fail-closed)", file=sys.stderr)
        return 2
    doc = derive_rates(samples, matrix_sha256=_matrix_pin(), mode=args.mode)
    if not args.write:
        print(json.dumps(doc, indent=2)[:2000])
        return 0
    json.dump(doc, open(args.out, "w", encoding="utf-8", newline=""), indent=2)
    open(args.out, "a", encoding="utf-8").write("\n")
    checks = g7_checks(doc, sorted(doc["fixed_rate"]) + sorted(doc["saturation_rate"]))
    print(json.dumps({"rates": os.path.relpath(args.out, REPO),
                      "rule_id": doc["rule_id"], "validated": doc["validated"],
                      "g7": checks}, indent=2))
    if not doc["validated"]:
        print("rates: A5 spot-check validation FAILED or evidence missing; refusing to write "
              "a usable rates.json (fail-closed)", file=sys.stderr)
        return 1
    return 0 if checks["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
