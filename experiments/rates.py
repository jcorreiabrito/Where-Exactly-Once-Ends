#!/usr/bin/env python3
"""M7/R12 rates derivation — fail-closed, frozen rule ids (S5 §11.4; A5 r2; A6).

Modes (one frozen rule module, author decision pending on coverage):
* ``M7-R1-mapping`` (default): measured P6 capacity for 10k/100k; derive 1k from 10k/P6,
  1M from 100k/P6, and P-scaling by P/6; every assumption recorded per block; spot-checks
  for derived blocks must be within ±25 % else validation fails closed.
* ``M7-R2-full-coverage``: all nine block combos must be measured, missing => SystemExit.
Fixed rate = floor(0.5 * capacity); saturation = floor(2.5 * max measured saturation
capacity), and missing saturation samples fail closed.

Two-step validation (Amendment A6, docs/prereg-amendment-A6.md):

1. ``--derive`` (T1a, after M7-cal + M7spot exist): write the **candidate**
   ``experiments/rates.json`` — ``validated=false``,
   ``saturation_validation.mode="post-rates"``, the derived fixed/saturation rates included.
2. ``--stage M7sat`` (T1b): the 9 saturation-validation runs are executed at exactly
   ``saturation_rate["100000|p6|sat"].rate`` (offered rate recorded per run).
3. ``--write`` (T1b end): validate the A5 spot checks AND the A6 saturation evidence.  The final
   ``validated=true`` requires BOTH: every M7sat run is valid, carries ``config.runner.
   offered_rate`` equal to the derived rate, and sustained it (committed-event throughput within
   the frozen ±25 % tolerance, G7 generator-lag p99 <= 5 ms, no load-generator failure).  Any
   missing evidence fails closed (no silent fallback, no partial validation).

``--check`` reports both validation layers for an existing ``rates.json``.
"""
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments import matrix_design as DESIGN  # noqa: E402

WARMUP_FRACTION = 0.10
FIXED_RATE_FRACTION = 0.5
SATURATION_MULTIPLIER = 2.5
SPOT_CHECK_TOLERANCE = 0.25
MIN_RATE = 1
RULE_ID_MAPPING = "A5.partition-scale.v1"
RULE_ID_FULL = "M7-R2-full-coverage"
SATURATION_RULE_ID = "SAT-2.5x-max"
REQUIRED_BLOCKS = (
    ("B", 10_000, 3, "fixed"), ("B", 10_000, 6, "fixed"),
    ("B", 10_000, 12, "fixed"), ("C", 100_000, 3, "fixed"), ("C", 100_000, 6, "fixed"),
    ("C", 100_000, 12, "fixed"), ("C", 1_000_000, 6, "fixed"),
    ("C-sat", 100_000, 6, "saturation"),
)

#: A6.2 frozen validation bounds (documented, pre-registered before T1b execution).
#: Committed-event throughput of a saturation-validation run must be within ±25 % of the offered
#: derived rate (the same tolerance class as the A5.3 spot checks); the G7 generator-lag p99 bound
#: is the analyzer's frozen 5 ms threshold (tools/analyze.py G7).
SATURATION_THROUGHPUT_TOLERANCE = 0.25
GENERATOR_LAG_P99_MAX_MS = 5
#: A6.1: the M7sat block is exactly the nine frozen mechanisms, one 100k/P6 saturation run each.
SATURATION_VALIDATION_BLOCK = "100000|p6|sat"
SATURATION_VALIDATION_LOAD = 100_000
SATURATION_VALIDATION_PARTITIONS = 6
M7SAT_RUN_PREFIX = "m7sat-"
SATURATION_VALIDATION_RATE_MODE = "saturation-validation"


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
SPOT_CHECK_BLOCKS = ((10_000, 3), (10_000, 12), (100_000, 3),
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


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def committed_throughput_eps(record):
    """Mechanism-committed throughput of a run record (events/s), or None if evidence is absent.

    P1 (S5 §13): the primary evidence is the artifact's steady-state committed-event throughput
    (``performance.throughput_eps_median`` aggregated from the per-instance app performance blocks,
    ``committed_basis=="app-committed"``), window from ``performance.perf_window``. Producer
    acknowledgement rates are never accepted as the mechanism capacity metric (method §7.2).
    """
    perf = record.get("performance") or {}
    if perf.get("committed_basis") != "app-committed":
        return None
    eps = perf.get("throughput_eps_median")
    if _is_number(eps) and eps > 0:
        return float(eps)
    committed = perf.get("committed_events")
    window = perf.get("perf_window") or {}
    t0, t1 = window.get("start"), window.get("end")
    if not _is_int(committed) or not _is_int(t0) or not _is_int(t1) or t1 <= t0:
        return None
    return capacity_from_window(committed, t0 / 1000.0, t1 / 1000.0)


def pending_saturation_validation(derived_rate):
    """A6.2 step 1: the candidate document's saturation_validation block (not yet exercised)."""
    return {
        "mode": "post-rates",
        "validated": False,
        "status": "pending-m7sat",
        "derived_rate": derived_rate,
        "required_runs": DESIGN.M7_SAT_RUNS,
        "observed_runs": 0,
        "tolerance": {"throughput_relative": SATURATION_THROUGHPUT_TOLERANCE,
                      "generator_lag_p99_ms_max": GENERATOR_LAG_P99_MAX_MS},
        "mechanisms_expected": list(DESIGN.MECHANISMS),
        "mechanisms_missing": list(DESIGN.MECHANISMS),
        "runs": {},
        "problems": ["no M7sat saturation-validation records yet (T1b not executed)"],
        "note": "A6.2: validated=true only after every M7sat run is valid and sustained the "
                "offered derived 2.5x-max rate",
    }


def saturation_validation_report(records, rates_doc):
    """A6.2 step 3: validate the 9 M7sat records against the derived rate; fail closed.

    Every criterion is mandatory (missing evidence = failure, never a silent pass):
      * exactly the nine frozen mechanisms, one record each, stage M7 / run-id ``m7sat-``,
        100k events / P6 (A6.1);
      * the run record is valid (not excluded), the load generator exited 0;
      * ``config.runner.rate_mode == "saturation-validation"`` and
        ``config.runner.offered_rate`` equals the derived rate exactly (the run exercised the
        derived rate, not some other rate);
      * ``performance.generator_lag_p99_ms`` present and <= 5 ms (G7 bound) with
        ``generator_lag_flag`` not true;
      * committed-event throughput present and within ±25 % of the offered/derived rate.
    """
    derived_entry = (rates_doc.get("saturation_rate") or {}).get(SATURATION_VALIDATION_BLOCK)
    derived_rate = derived_entry.get("rate") if isinstance(derived_entry, dict) else None
    report = {
        "mode": "post-rates",
        "validated": False,
        "status": "failed",
        "derived_rate": derived_rate if _is_int(derived_rate) else None,
        "required_runs": DESIGN.M7_SAT_RUNS,
        "observed_runs": len(records),
        "tolerance": {"throughput_relative": SATURATION_THROUGHPUT_TOLERANCE,
                      "generator_lag_p99_ms_max": GENERATOR_LAG_P99_MAX_MS},
        "mechanisms_expected": list(DESIGN.MECHANISMS),
        "mechanisms_missing": [],
        "runs": {},
        "problems": [],
        "note": "A6.2 post-rates saturation validation (G7 generator-lag bound + "
                "committed-event throughput)",
    }
    problems = report["problems"]
    if not _is_int(derived_rate) or derived_rate < MIN_RATE:
        problems.append("rates document has no integer derived saturation rate for "
                        f"{SATURATION_VALIDATION_BLOCK!r}; cannot validate the M7sat block")
        report["mechanisms_missing"] = list(DESIGN.MECHANISMS)
        return report
    seen_ids: set[str] = set()
    seen_mechs: dict[str, str] = {}
    for record in sorted(records, key=lambda r: str(r.get("run_id") or "")):
        run_id = str(record.get("run_id") or "")
        mech = str(record.get("mechanism") or "")
        run_problems: list[str] = []
        if not run_id:
            problems.append("M7sat record without run_id")
            continue
        if run_id in seen_ids:
            problems.append(f"duplicate M7sat record {run_id}")
            continue
        seen_ids.add(run_id)
        if mech not in DESIGN.MECHANISMS:
            run_problems.append(f"mechanism {mech!r} is not one of the nine frozen mechanisms")
        elif mech in seen_mechs:
            run_problems.append(f"duplicate M7sat run for mechanism {mech} "
                                f"({seen_mechs[mech]} and {run_id})")
        else:
            seen_mechs[mech] = run_id
        if str(record.get("stage")) not in ("M7", "M7sat"):
            run_problems.append("record stage must be the calibration stage M7/M7sat")
        if not run_id.startswith(M7SAT_RUN_PREFIX):
            run_problems.append(f"run_id must use the A6 {M7SAT_RUN_PREFIX!r} prefix")
        if (record.get("load_events") != SATURATION_VALIDATION_LOAD
                or record.get("partitions") != SATURATION_VALIDATION_PARTITIONS):
            run_problems.append("M7sat run must be 100k events / P6 (A6.1)")
        validity = record.get("validity")
        if not isinstance(validity, dict) or validity.get("valid") is not True:
            run_problems.append("run is not valid (validity.valid != true)")
        runner_cfg = ((record.get("config") or {}).get("runner") or {})
        rate_mode = runner_cfg.get("rate_mode")
        offered = runner_cfg.get("offered_rate")
        if rate_mode != SATURATION_VALIDATION_RATE_MODE:
            run_problems.append(
                f"missing/incorrect config.runner.rate_mode evidence (got {rate_mode!r}, want "
                f"{SATURATION_VALIDATION_RATE_MODE!r})")
        if not _is_int(offered):
            run_problems.append("missing config.runner.offered_rate evidence")
        elif offered != derived_rate:
            run_problems.append(
                f"offered rate {offered} != derived saturation rate {derived_rate} "
                f"(the derived rate was not exercised)")
        exit_status = record.get("exit_status") or {}
        if exit_status.get("load_generator") != 0:
            run_problems.append(
                "load-generator failure/missing exit evidence "
                f"(exit_status.load_generator={exit_status.get('load_generator')!r})")
        if (record.get("quiescence") or {}).get("censored") is True:
            run_problems.append("M7sat run is quiescence-censored (A6 requires a sustained, "
                                "non-censored run)")
        perf = record.get("performance") or {}
        if perf.get("committed_basis") != "app-committed":
            run_problems.append("missing mechanism-committed performance basis "
                                "(performance.committed_basis must be 'app-committed'; producer "
                                "acks are not accepted for A6)")
        lag = perf.get("generator_lag_p99_ms")
        if not _is_int(lag):
            run_problems.append("missing generator-lag evidence "
                                "(performance.generator_lag_p99_ms)")
        elif lag > GENERATOR_LAG_P99_MAX_MS:
            run_problems.append(f"G7 generator-lag violation: p99={lag} ms > "
                                f"{GENERATOR_LAG_P99_MAX_MS} ms")
        if perf.get("generator_lag_flag") is True:
            run_problems.append("G7 generator-lag flag set")
        throughput = committed_throughput_eps(record)
        if throughput is None:
            run_problems.append("missing committed-event throughput evidence")
        else:
            rel = abs(throughput - derived_rate) / derived_rate if derived_rate else 1.0
            if rel > SATURATION_THROUGHPUT_TOLERANCE:
                run_problems.append(
                    f"committed-event throughput {throughput:.3f}/s is outside ±"
                    f"{SATURATION_THROUGHPUT_TOLERANCE:.0%} of the offered rate "
                    f"{derived_rate}/s (relative error {rel:.3f})")
        report["runs"][run_id] = {
            "mechanism": mech,
            "offered_rate": offered,
            "derived_rate": derived_rate,
            "throughput_eps": round(throughput, 3) if throughput is not None else None,
            "generator_lag_p99_ms": lag,
            "valid": not run_problems,
            "problems": run_problems,
        }
        problems.extend(f"{run_id}: {p}" for p in run_problems)
    missing = [m for m in DESIGN.MECHANISMS if m not in seen_mechs]
    report["mechanisms_missing"] = missing
    if missing:
        problems.append(f"missing M7sat evidence for mechanisms: {missing}")
    if len(seen_mechs) != DESIGN.M7_SAT_RUNS or len(seen_ids) != DESIGN.M7_SAT_RUNS:
        problems.append(f"M7sat evidence must be exactly {DESIGN.M7_SAT_RUNS} runs "
                        f"(one per mechanism); observed {len(seen_ids)} records")
    report["observed_runs"] = len(seen_ids)
    report["validated"] = not problems
    report["status"] = "ok" if report["validated"] else "failed"
    return report


def derive_rates(samples, matrix_sha256=None, mode=RULE_ID_MAPPING,
                 source="pilot M7 calibration records", saturation_validation=None):
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
    derived_rate = int(saturation[_block_key(100_000, 6, "saturation")]["rate"])
    sat_report = saturation_validation or pending_saturation_validation(derived_rate)
    a5_validated = bool(validation.get("validated"))
    return {"schema": "wxeo.rates.v2", "generated_by": "experiments/rates.py",
            "rule_id": rule_id, "saturation_rule_id": SATURATION_RULE_ID,
            "frozen_rule": {"fraction_of_min_capacity": FIXED_RATE_FRACTION,
                            "warmup_fraction_excluded": WARMUP_FRACTION,
                            "saturation_multiplier": SATURATION_MULTIPLIER,
                            "spot_check_tolerance": SPOT_CHECK_TOLERANCE,
                            "saturation_throughput_tolerance": SATURATION_THROUGHPUT_TOLERANCE,
                            "generator_lag_p99_ms_max": GENERATOR_LAG_P99_MAX_MS},
            "source": source, "matrix_sha256": matrix_sha256,
            "calibrated_blocks": sorted(_block_key(*k) for k, v in caps.items() if v),
            "derived_blocks": sorted(derived), "assumptions": derived,
            "fixed_rate": fixed, "saturation_rate": saturation,
            "saturation_validation": sat_report,
            # A6.2: overall validation requires BOTH the A5 spot checks and the A6 saturation
            # validation; a candidate document is therefore always validated=false.
            "validated": bool(a5_validated and sat_report.get("validated")),
            "validation": validation}


def g7_checks(rates_doc, required_blocks, need_saturation=True):
    """G7 gate over a rates document: schema/rule, A5 spot checks, A6 saturation validation,
    required fixed/saturation entries and the matrix pin.  Fail closed on anything missing."""
    problems = []
    if rates_doc.get("schema") != "wxeo.rates.v2":
        problems.append("schema != wxeo.rates.v2")
    if rates_doc.get("rule_id") not in (RULE_ID_MAPPING, RULE_ID_FULL):
        problems.append("unknown rule_id")
    saturation_rate = rates_doc.get("saturation_rate") or {}
    if need_saturation and not saturation_rate:
        problems.append("saturation_rate entry missing")
    validation = rates_doc.get("validation") or {}
    if validation.get("validated") is not True:
        problems.append("A5 spot-check validation failed (validation.validated=false)")
    if rates_doc.get("validated") is not True:
        problems.append("rates table is not final (validated=false): A5 and/or A6 evidence "
                        "missing")
    sat_validation = rates_doc.get("saturation_validation") or {}
    if sat_validation.get("mode") != "post-rates":
        problems.append("A6 saturation_validation.mode must be 'post-rates'")
    if sat_validation.get("validated") is not True:
        problems.append("A6 saturation validation failed or not exercised "
                        "(saturation_validation.validated=false)")
    elif int(sat_validation.get("observed_runs") or 0) != DESIGN.M7_SAT_RUNS:
        problems.append(f"A6 saturation validation observed "
                        f"{sat_validation.get('observed_runs')} != {DESIGN.M7_SAT_RUNS} runs")
    for block in required_blocks:
        if block.endswith("|sat"):
            entry = saturation_rate.get(block)
        else:
            entry = (rates_doc.get("fixed_rate") or {}).get(block)
        if not entry or int(entry.get("rate") or 0) < MIN_RATE:
            problems.append(f"missing/invalid {'saturation' if block.endswith('|sat') else 'fixed'}"
                            f"_rate for {block}")
    if not rates_doc.get("matrix_sha256"):
        problems.append("missing matrix pin")
    return {"ok": not problems, "problems": problems}


def _record_is_m7sat(record):
    if str(record.get("stage")) == "M7sat":
        return True
    if str(record.get("run_id") or "").startswith(M7SAT_RUN_PREFIX):
        return True
    runner_cfg = ((record.get("config") or {}).get("runner") or {})
    return runner_cfg.get("rate_mode") == SATURATION_VALIDATION_RATE_MODE


def _load_records(records_dir):
    """Calibration samples for the A5 mapping rule (reads the records directory)."""
    import glob
    records = []
    for path in sorted(glob.glob(os.path.join(records_dir, "run_*.json"))):
        try:
            records.append(json.load(open(path, encoding="utf-8")))
        except Exception:  # noqa: BLE001
            continue
    return _load_records_from_list(records)


def _load_records_from_list(records):
    """Calibration capacity samples from already-loaded run records.

    May ingest M7sat records for validation purposes, but NEVER confirmatory stages (A5.4:
    ``stage in {m7, M7}`` only).  All nine frozen mechanisms are included in the capacity
    statistics, as A5.2 requires ("min over the nine mechanisms"); there is no per-mechanism
    exclusion.  P1 (S5 §13): only mechanism-committed (``app-committed``) evidence with a
    non-censored, successful load is capacity evidence; producer acks are rejected.
    """
    samples = []
    for rec in records:
        if str(rec.get("stage")) not in ("M7", "M7sat"):
            continue
        perf, timings = rec.get("performance") or {}, rec.get("timings") or {}
        # P1 (S5 §13): capacity must be mechanism-committed, never producer-ack based; rejected
        # runs (censored / load-generator failure) are not capacity evidence.
        if perf.get("committed_basis") != "app-committed":
            continue
        if (rec.get("quiescence") or {}).get("censored") is True:
            continue
        if ((rec.get("exit_status") or {}).get("load_generator") or 0) != 0:
            continue
        if not isinstance(perf.get("committed_events"), int):
            continue
        window = perf.get("perf_window") or {}
        t0, t1 = window.get("start"), window.get("end")
        if not isinstance(t0, int) or not isinstance(t1, int) or t1 <= t0:
            t0, t1 = timings.get("t_load_start"), timings.get("t_load_end")
        if not isinstance(t0, int) or not isinstance(t1, int) or t1 <= t0:
            continue
        runner_cfg = ((rec.get("config") or {}).get("runner") or {})
        if _record_is_m7sat(rec) or runner_cfg.get("rate_mode") == \
                SATURATION_VALIDATION_RATE_MODE:
            mode = "saturation_validation"
        elif runner_cfg.get("rate_mode") in ("calibration-saturation", "saturation"):
            mode = "saturation"
        else:
            mode = "fixed"
        samples.append({"load_events": rec.get("load_events"),
                        "partitions": rec.get("partitions"),
                        "mechanism": rec.get("mechanism"),
                        "committed_events": perf["committed_events"],
                        "window_start_s": t0 / 1000.0, "window_end_s": t1 / 1000.0,
                        "mode": mode})
    return samples


def _load_m7sat_records(records_dir):
    """The A6 M7sat run records (validation evidence; never part of the A5 capacity statistics)."""
    import glob
    records = []
    for path in sorted(glob.glob(os.path.join(records_dir, "run_*.json"))):
        try:
            rec = json.load(open(path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        if str(rec.get("stage")) not in ("M7", "M7sat"):
            continue
        if _record_is_m7sat(rec):
            records.append(rec)
    return records


def _matrix_pin():
    mpath = os.path.join(REPO, "experiments", "matrix.sha256")
    if not os.path.exists(mpath):
        return None
    for line in open(mpath, encoding="utf-8"):
        if line.strip().endswith("matrix.yaml"):
            return line.split()[0]
    return None


def _write_rates_doc(doc, path):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        json.dump(doc, fh, indent=2)
        fh.write("\n")


def _required_block_keys():
    return [_block_key(load, p, m) for _stage, load, p, m in REQUIRED_BLOCKS]


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="M7/R12 rates derivation (fail-closed)")
    parser.add_argument("--records-dir", default=os.path.join(REPO, "results", "raw",
                                                              "run-records"))
    parser.add_argument("--out", default=os.path.join(REPO, "experiments", "rates.json"))
    parser.add_argument("--mode", choices=[RULE_ID_MAPPING, RULE_ID_FULL],
                        default=RULE_ID_MAPPING)
    parser.add_argument("--derive", action="store_true",
                        help="T1a: write the CANDIDATE rates table (validated=false, "
                             "saturation_validation.mode=post-rates)")
    parser.add_argument("--write", action="store_true",
                        help="T1b end: validate A5 spot checks + A6 saturation evidence and "
                             "write the final table (validated=true only if both pass)")
    parser.add_argument("--check", action="store_true",
                        help="report both validation layers for an existing rates.json")
    args = parser.parse_args(argv)
    if args.check:
        if not os.path.exists(args.out):
            print(f"rates: {args.out} does not exist", file=sys.stderr)
            return 2
        doc = json.load(open(args.out, encoding="utf-8"))
        g7 = g7_checks(doc, _required_block_keys())
        sat = doc.get("saturation_validation") or {}
        a5 = doc.get("validation") or {}
        report = {
            "rates": os.path.relpath(args.out, REPO),
            "rule_id": doc.get("rule_id"),
            "validated": doc.get("validated"),
            "matrix_sha256": doc.get("matrix_sha256"),
            "a5_validation": {"validated": a5.get("validated"),
                              "status": a5.get("status"),
                              "failed_blocks": a5.get("failed_blocks") or [],
                              "missing_spot_checks": a5.get("missing_spot_checks") or []},
            "saturation_validation": {"mode": sat.get("mode"),
                                      "validated": sat.get("validated"),
                                      "status": sat.get("status"),
                                      "derived_rate": sat.get("derived_rate"),
                                      "observed_runs": sat.get("observed_runs"),
                                      "required_runs": sat.get("required_runs"),
                                      "problems": sat.get("problems") or []},
            "g7": g7,
            "ok": bool(g7.get("ok") and doc.get("validated") is True),
        }
        print(json.dumps(report, indent=2))
        return 0 if report["ok"] else 1
    samples = _load_records(args.records_dir)
    if not samples:
        print("R12: no M7/pilot calibration records with performance.committed_events and a "
              "streaming window; refusing to write rates.json (fail-closed)", file=sys.stderr)
        return 2
    doc = derive_rates(samples, matrix_sha256=_matrix_pin(), mode=args.mode)
    if not (args.derive or args.write):
        print(json.dumps(doc, indent=2)[:2000])
        return 0
    sat_records = _load_m7sat_records(args.records_dir)
    sat_report = saturation_validation_report(sat_records, doc)
    a5_validated = bool((doc.get("validation") or {}).get("validated"))
    if args.derive:
        # A6.2 step 1 (T1a): the candidate table is deliberately validated=false; the A6
        # saturation block has not been exercised yet at derive time.
        candidate = dict(sat_report)
        candidate["validated"] = False
        candidate["status"] = ("pending-m7sat" if not sat_records else "candidate")
        doc["saturation_validation"] = candidate
        doc["validated"] = False
        _write_rates_doc(doc, args.out)
        print(json.dumps({"rates": os.path.relpath(args.out, REPO),
                          "mode": "derive-candidate",
                          "rule_id": doc["rule_id"],
                          "validated": doc["validated"],
                          "a5_validated": a5_validated,
                          "saturation_rate": doc["saturation_rate"],
                          "saturation_validation": {
                              "mode": candidate.get("mode"),
                              "validated": candidate.get("validated"),
                              "status": candidate.get("status")},
                          "matrix_sha256": doc.get("matrix_sha256")}, indent=2))
        if not a5_validated:
            print("rates: A5 spot-check validation FAILED; the candidate table is written for "
                  "inspection but T1a must not proceed to T1b (fail-closed)", file=sys.stderr)
            return 1
        return 0
    # A6.2 step 3 (T1b end): final validation requires BOTH layers.
    doc["saturation_validation"] = sat_report
    doc["validated"] = bool(a5_validated and sat_report.get("validated"))
    _write_rates_doc(doc, args.out)
    checks = g7_checks(doc, _required_block_keys())
    print(json.dumps({"rates": os.path.relpath(args.out, REPO),
                      "mode": "final-validation",
                      "rule_id": doc["rule_id"],
                      "validated": doc["validated"],
                      "a5_validated": a5_validated,
                      "saturation_validation": {
                          "mode": sat_report.get("mode"),
                          "validated": sat_report.get("validated"),
                          "status": sat_report.get("status"),
                          "observed_runs": sat_report.get("observed_runs")},
                      "g7": checks}, indent=2))
    if not doc["validated"]:
        print("rates: A5 spot-check validation and/or A6 saturation validation FAILED or "
              "evidence missing; refusing to write a usable rates.json (fail-closed)",
              file=sys.stderr)
        return 1
    return 0 if checks["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
