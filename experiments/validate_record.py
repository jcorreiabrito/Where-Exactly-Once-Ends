#!/usr/bin/env python3
"""Validate one or more per-run records against the frozen schema (docs/prereg.md §5).

The schema is hand-coded here (no external schema file needed) and mirrors the field table
exactly: every field is required, unknown values are ``null`` accompanied by a ``notes`` entry,
and the ``counts`` / ``violations`` keys are exactly those of the oralce contract.  A record that
fails validation is not analysable and must not enter results/raw/run-records/.

Usage::

    python experiments/validate_record.py results/raw/run-records/*.json
    python experiments/validate_record.py --schema-only   # print the JSON-schema summary

Exit code 0 iff every file validates.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

SCHEMA_VERSION = "wxeo.run.v1"
STAGES = {"pilot", "A", "B", "C", "D"}
MECHANISMS = {"M0", "M1-kafka", "M1x", "M2", "M3", "M4", "M2+M3", "M4+M3", "M1x+M3"}
ORDERING = {"strict", "early"}
FAULTS = {"F0", "F1", "F2", "F3", "F4a", "F4b", "F5", "F6",
          "K1", "K2", "K3", "K4", "K5", "K6", "K7", "none"}
FAULT_LEVELS = {"L1", "L2", "none"}
HEX16 = re.compile(r"^[0-9a-f]{16}$")
HEX32 = re.compile(r"^[0-9a-f]{32}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")

COUNTS_KEYS = {
    "produced", "consumed_estimate", "expected_postings", "observed_postings",
    "dup_postings", "dup_entries", "lost_postings", "lost_entries", "imbalanced_entries",
    "mutated_postings", "spurious_postings",
}
VIOLATION_KEYS = {"v1", "v2", "v3", "v4", "violating_run"}
PUBLICATION_KEYS = {"pub_records", "dup_records", "lost_records", "divergence"}
ORACLE_KEYS = {"gt_sha256", "expected_sha256", "source_integrity", "oracle_script_sha256",
               "result_sha256"}
HOST_KEYS = {"os_build", "cpu_model", "ram_gb", "disk_free_start_gb"}
VERSIONS_REQUIRED = {"java", "kafka", "postgresql", "toxiproxy", "powershell", "python"}
TIMING_KEYS = {"t_start", "t_load_start", "t_load_end", "t_fault_fire", "t_restore",
               "t_quiesce", "t_observation", "t_end"}
QUIESCENCE_KEYS = {"lag_per_partition", "quiet_period_ms", "censored", "t_consistent_ms"}
PERF_KEYS = {"throughput_eps_median", "p50_ms", "p90_ms", "p99_ms", "p999_ms", "max_ms",
             "histogram_path", "warmup_events_excluded", "perf_window"}
VALIDITY_KEYS = {"valid", "excluded", "exclude_reason", "residue_check", "clock_check",
                 "disk_free_start_gb", "notes"}
EXIT_KEYS = {"consumer", "load_generator"}
TOP_REQUIRED = {
    "schema", "run_id", "stage", "cell_id", "mechanism", "ordering_variant", "fault",
    "fault_level", "load_events", "partitions", "consumer_instances", "rep", "data_seed",
    "injection_seed", "master_seed", "git_sha", "host", "versions", "config",
    "process_limits", "timings", "quiescence", "performance", "counts", "violations",
    "publication", "oracle", "fault_receipts", "validity", "artifacts", "stdout_path",
    "stderr_path", "exit_status",
}


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validate_record(rec: dict, *, path: str = "") -> list[str]:
    errors: list[str] = []

    def err(msg: str) -> None:
        errors.append(f"{path}: {msg}" if path else msg)

    if not isinstance(rec, dict):
        return [f"{path}: record is not a JSON object"]
    missing = TOP_REQUIRED - set(rec)
    if missing:
        err(f"missing top-level fields: {sorted(missing)}")
    extra = set(rec) - TOP_REQUIRED
    if extra:
        err(f"unknown top-level fields: {sorted(extra)}")
    if rec.get("schema") != SCHEMA_VERSION:
        err(f"schema must be {SCHEMA_VERSION!r}")
    if not isinstance(rec.get("run_id"), str) or not re.fullmatch(r"[a-z0-9-]+",
                                                                 rec.get("run_id", "")):
        err("run_id must be a lower-case [a-z0-9-] string")
    if rec.get("stage") not in STAGES:
        err("stage must be one of pilot/A/B/C/D")
    if not isinstance(rec.get("cell_id"), str) or not rec.get("cell_id"):
        err("cell_id must be a non-empty string")
    if rec.get("mechanism") not in MECHANISMS:
        err("mechanism must be one of the 9 frozen configurations")
    if rec.get("ordering_variant") not in ORDERING:
        err("ordering_variant must be strict|early")
    if rec.get("fault") not in FAULTS:
        err("fault must be F0..F6, K1..K7 or none")
    if rec.get("fault_level") not in FAULT_LEVELS:
        err("fault_level must be L1|L2|none")
    for key in ("load_events", "partitions", "consumer_instances", "rep"):
        if not _is_int(rec.get(key)) or rec.get(key, -1) < 0:
            err(f"{key} must be a non-negative integer")
    for key, pat in (("data_seed", HEX16), ("injection_seed", HEX16),
                     ("master_seed", HEX32)):
        value = rec.get(key)
        if not isinstance(value, str) or not pat.fullmatch(value):
            err(f"{key} must be a {pat.pattern}-hex string")
    if not isinstance(rec.get("git_sha"), str) or not re.fullmatch(r"[0-9a-f]{7,40}",
                                                                   rec.get("git_sha", "")):
        err("git_sha must be a hex commit id")
    # host / versions
    host = rec.get("host")
    if not isinstance(host, dict) or not HOST_KEYS <= set(host):
        err(f"host must be an object with {sorted(HOST_KEYS)}")
    versions = rec.get("versions")
    if not isinstance(versions, dict) or not VERSIONS_REQUIRED <= set(versions):
        err(f"versions must be an object with at least {sorted(VERSIONS_REQUIRED)}")
    config = rec.get("config")
    if not isinstance(config, dict) or not isinstance(config.get("config_sha256"), str):
        err("config must be an object with config_sha256")
    if not isinstance(rec.get("process_limits"), dict):
        err("process_limits must be an object")
    timings = rec.get("timings")
    if not isinstance(timings, dict) or not TIMING_KEYS <= set(timings):
        err(f"timings must contain {sorted(TIMING_KEYS)}")
    else:
        for key in TIMING_KEYS:
            value = timings.get(key)
            if value is not None and not _is_int(value):
                err(f"timings.{key} must be an integer UTC ms or null")
    q = rec.get("quiescence")
    if not isinstance(q, dict) or not QUIESCENCE_KEYS <= set(q):
        err(f"quiescence must contain {sorted(QUIESCENCE_KEYS)}")
    else:
        if not isinstance(q.get("censored"), bool):
            err("quiescence.censored must be a boolean")
        if not isinstance(q.get("lag_per_partition"), dict):
            err("quiescence.lag_per_partition must be an object")
        if q.get("t_consistent_ms") is not None and not _is_int(q.get("t_consistent_ms")):
            err("quiescence.t_consistent_ms must be an integer or null")
    perf = rec.get("performance")
    if not isinstance(perf, dict) or not PERF_KEYS <= set(perf):
        err(f"performance must contain {sorted(PERF_KEYS)}")
    else:
        for key in ("throughput_eps_median", "p50_ms", "p90_ms", "p99_ms", "p999_ms",
                    "max_ms"):
            if perf.get(key) is not None and not _is_number(perf.get(key)):
                err(f"performance.{key} must be a number or null")
    counts = rec.get("counts")
    if not isinstance(counts, dict) or set(counts) != COUNTS_KEYS:
        err(f"counts must contain exactly {sorted(COUNTS_KEYS)}")
    else:
        for key, value in counts.items():
            if value is not None and (not _is_int(value) or value < 0):
                err(f"counts.{key} must be a non-negative integer or null")
    violations = rec.get("violations")
    if not isinstance(violations, dict) or set(violations) != VIOLATION_KEYS:
        err(f"violations must contain exactly {sorted(VIOLATION_KEYS)}")
    else:
        for key in VIOLATION_KEYS:
            if not isinstance(violations.get(key), bool):
                err(f"violations.{key} must be a boolean")
    publication = rec.get("publication")
    if not isinstance(publication, dict) or not PUBLICATION_KEYS <= set(publication):
        err(f"publication must contain {sorted(PUBLICATION_KEYS)}")
    oracle = rec.get("oracle")
    if not isinstance(oracle, dict) or not ORACLE_KEYS <= set(oracle):
        err(f"oracle must contain {sorted(ORACLE_KEYS)}")
    else:
        if not isinstance(oracle.get("source_integrity"), bool):
            err("oracle.source_integrity must be a boolean")
    if not isinstance(rec.get("fault_receipts"), list):
        err("fault_receipts must be an array")
    validity = rec.get("validity")
    if not isinstance(validity, dict) or not VALIDITY_KEYS <= set(validity):
        err(f"validity must contain {sorted(VALIDITY_KEYS)}")
    else:
        for key in ("valid", "excluded", "residue_check", "clock_check"):
            if not isinstance(validity.get(key), bool):
                err(f"validity.{key} must be a boolean")
        if validity.get("exclude_reason") is not None and not isinstance(
                validity.get("exclude_reason"), str):
            err("validity.exclude_reason must be a string or null")
        if not isinstance(validity.get("notes"), list):
            err("validity.notes must be a list of strings")
    artifacts = rec.get("artifacts")
    if not isinstance(artifacts, dict):
        err("artifacts must be an object of relative paths")
    for key in ("stdout_path", "stderr_path"):
        if not isinstance(rec.get(key), (str, type(None))):
            err(f"{key} must be a string path or null")
    exit_status = rec.get("exit_status")
    if not isinstance(exit_status, dict):
        err("exit_status must be an object")
    # cross-field rules from the exclusion policy (prereg §8.2)
    if isinstance(validity, dict) and isinstance(oracle, dict) and isinstance(violations, dict):
        excluded = validity.get("excluded") is True
        if excluded and not validity.get("exclude_reason"):
            err("excluded run must carry an exclude_reason")
        if oracle.get("source_integrity") is False and not excluded:
            err("source-integrity failure must exclude the run (prereg §8.2 item 2)")
        if violations.get("violating_run") and validity.get("valid") is False and not excluded:
            err("invalid run must be excluded or rerun (prereg §8.2)")
    return errors


def schema_summary() -> dict:
    return {
        "schema": SCHEMA_VERSION,
        "top_level_required": sorted(TOP_REQUIRED),
        "counts_keys": sorted(COUNTS_KEYS),
        "violation_keys": sorted(VIOLATION_KEYS),
        "publication_keys": sorted(PUBLICATION_KEYS),
        "oracle_keys": sorted(ORACLE_KEYS),
        "host_keys": sorted(HOST_KEYS),
        "versions_required": sorted(VERSIONS_REQUIRED),
        "timing_keys": sorted(TIMING_KEYS),
        "quiescence_keys": sorted(QUIESCENCE_KEYS),
        "performance_keys": sorted(PERF_KEYS),
        "validity_keys": sorted(VALIDITY_KEYS),
        "stage_enum": sorted(STAGES),
        "mechanism_enum": sorted(MECHANISMS),
        "fault_enum": sorted(FAULTS),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Validate WXEO run records")
    parser.add_argument("paths", nargs="*", help="run record JSON files")
    parser.add_argument("--schema-only", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    if args.schema_only:
        print(json.dumps(schema_summary(), indent=2))
        return 0
    if not args.paths:
        parser.error("no run-record paths given")
    failed = 0
    for path in args.paths:
        try:
            with open(path, "r", encoding="utf-8") as fh:
                rec = json.load(fh)
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL {path}: cannot read JSON: {exc}")
            failed += 1
            continue
        errors = validate_record(rec, path=path)
        if errors:
            failed += 1
            print(f"FAIL {path}")
            for e in errors:
                print(f"  - {e}")
        elif not args.quiet:
            print(f"PASS {path}")
    if args.paths:
        print(f"\n{len(args.paths) - failed}/{len(args.paths)} records valid")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
