#!/usr/bin/env python3
"""Independent oracle / checker for the Where-Exactly-Once-Ends experiments.

Normative sources: ``docs/method.md`` §1.4, §2.5, §2.6, §7.4-§7.6, Appendix A;
``docs/prereg.md`` §5-§8; ``docs/CONTRACTS.md`` §4-§6.

Independence (A1 §4, method §7.4): this program is Python 3.12, imports only the standard
library (plus psycopg2 / a Kafka client inside the *dump* subcommands, never inside the
predicate code), reads the artifact's *outputs* only, and shares no code with the Java
pipeline.  The expected posting multiset is re-derived from ``(per-run seed, seq)`` using the
specification in :mod:`derive`, never from the Java generator.

What it does
------------
1. Regenerates the canonical ground-truth posting set and its SHA-256 digest.
2. Verifies ``gt_<run>.ndjson`` digest == regenerated digest (gate G5), then validates the
   source-topic dump: per-partition offsets contiguous from 0, exactly P partitions, the
   event-id multiset equal to the produced manifest, and the source-derived posting digest
   equal to the regenerated digest (method §7.4).
3. Loads the observed sink (PostgreSQL ``posting`` dump, or the publication stream for
   M1-kafka) and computes the exact predicates of method §2.5 / Appendix A:
   I1a (structural well-formedness) + I1b (signed sum), I2 (append-only mutation +
   attestation), I3 (duplicates), I4 (losses), plus spurious postings and publication
   metrics for DB-sink mechanisms.
4. Writes ``oracle_<run_id>.json``: counts, violation flags, digests and paths, shaped so the
   runner can embed the ``counts``/``violations``/``publication``/``oracle`` blocks verbatim
   into the per-run record (docs/prereg.md §5).

Memory-bounded design
---------------------
The expected posting multiset (2*N lines) and the observed dump are bucketed by an
``event_id``-prefix shard (default 64).  Prefix shards preserve the global canonical
``(event_id, leg_no)`` order, so the ground-truth digest is computed by streaming the shards in
order and the full file is never materialised.  Per-bucket evaluation keeps memory at
O(2N/64) entries, which keeps the 1M-event runs inside the host memory budget.

Verdict rule (documented extension, see ``results/DEVIATIONS.md``)
-------------------------------------------------------------------
``violating_run = v1 ∨ v2 ∨ v3 ∨ v4 ∨ (spurious_postings > 0)``.  method §2.4 includes "no
spurious postings" in the consistency predicate while §2.5 defines v1..v4 only from the four
count classes; a fabricated posting with an unknown key is therefore reported as a
``SPURIOUS`` attribution and is verdict-affecting, but is NOT folded into ``dup_postings``.
``v2`` is set when a posting is mutated (I2 content clause) **or** the append-only attestation
fails (method §2.6: "V2 ... P5 non-empty, or append-only attestation fails").

Usage
-----
    python experiments/oracle/oracle.py run  --run-id ... --mechanism M1x --sink-kind db \
        --events 1000 --partitions 3 --data-seed-hex <16hex> \
        --gt gt.ndjson --source-dump source.ndjson --produced produced.ndjson \
        --sink-dump sink.csv [--publication-dump pub.ndjson] [--attestation attest.json] \
        --out oracle_<run>.json
    python experiments/oracle/oracle.py verify ...
    python experiments/oracle/oracle.py dump-pg | dump-source | dump-kafka ...
    python experiments/oracle/oracle.py selftest
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone

try:  # package-relative when imported, plain when executed as a script
    from . import derive as D
    from .. import pathsafe
except (ImportError, ValueError):  # pragma: no cover - script execution path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import derive as D  # type: ignore
    _REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if _REPO_ROOT not in sys.path:
        sys.path.insert(0, _REPO_ROOT)
    from experiments import pathsafe  # type: ignore

ORACLE_VERSION = "wxeo.oracle.v1"
DEFAULT_SHARDS = D.DEFAULT_SHARDS

# --- verdict rule text (also written into every oracle JSON for auditability) -----------------
VERDICT_RULE = (
    "violating_run = v1|v2|v3|v4|v5; "
    "v1=imbalanced_entries>0 (I1a structural or I1b signed-sum); "
    "v2=(mutated_postings>0 or append-only attestation fails); v3=dup_postings>0; "
    "v4=lost_postings>0; v5=spurious_postings>0 (fabricated/fabricated-balanced postings). "
    "The I1a/I1b split and v5 are recorded for the author-approved P2 amendment A2; analysis "
    "reads violations.violating_run verbatim and never recomputes it. "
    "Recorded interpretation per docs/P2-RULINGS.md and results/DEVIATIONS.md."
)


# --------------------------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_json(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _iter_ndjson(path: str):
    """Yield parsed JSON objects from an NDJSON file; blank lines are skipped."""
    with open(path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield line_no, json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSON: {exc}") from exc


def _ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def oracle_script_sha256() -> str:
    """SHA-256 over the concatenation of the oracle source files (sorted by name)."""
    here = os.path.dirname(os.path.abspath(__file__))
    names = sorted(f for f in os.listdir(here) if f.endswith(".py"))
    h = hashlib.sha256()
    for name in names:
        with open(os.path.join(here, name), "rb") as fh:
            h.update(name.encode("utf-8"))
            h.update(b"\0")
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------------------------
# input verification (GT digest + source-topic integrity, method §7.4, gate G5)
# --------------------------------------------------------------------------------------------
def _shard_ndjson(path: str, out_dir: str, prefix: str, shards: int, fields: tuple, stats: dict | None = None):
    """Split an NDJSON file into eid-prefix shards, projecting ``fields`` per line.

    ``fields`` must contain ``event_id``.  Returns the list of shard paths and the line count.
    ``stats`` (optional) is updated in place with per-partition counts for source dumps.
    """
    os.makedirs(out_dir, exist_ok=True)
    handle_cache: dict[int, object] = {}
    paths = [os.path.join(out_dir, f"{prefix}-{b:03d}.ndjson") for b in range(shards)]
    for shard_path in paths:  # every shard file exists, even when empty
        with open(shard_path, "w", encoding="utf-8", newline=""):
            pass
    total = 0
    malformed = 0
    try:
        for line_no, obj in _iter_ndjson(path):
            try:
                eid = obj["event_id"]
            except (KeyError, TypeError):
                malformed += 1
                continue
            b = D.eid_bucket(eid, shards)
            fh = handle_cache.get(b)
            if fh is None:
                fh = open(paths[b], "a", encoding="utf-8", newline="")
                handle_cache[b] = fh
            projected = {k: obj.get(k) for k in fields}
            fh.write(json.dumps(projected, separators=(",", ":")) + "\n")
            total += 1
            if stats is not None and "partition" in obj:
                try:
                    p = int(obj["partition"])
                except (TypeError, ValueError):
                    p = None
                if p is not None:
                    ps = stats.setdefault(p, {"n": 0, "min": None, "max": None})
                    ps["n"] += 1
                    off = obj.get("offset")
                    if isinstance(off, int):
                        ps["min"] = off if ps["min"] is None else min(ps["min"], off)
                        ps["max"] = off if ps["max"] is None else max(ps["max"], off)
    finally:
        for fh in handle_cache.values():
            fh.close()
    return paths, total, malformed


def _iter_shard(paths, shards):
    for b in range(shards):
        for _ln, obj in _iter_ndjson(paths[b]):
            b2 = D.eid_bucket(obj["event_id"], shards)
            if b2 != b:  # defensive: a misplaced line would corrupt a shard
                raise RuntimeError(f"shard {b} contains event in bucket {b2}")
            yield obj


def verify_source_inputs(
    *,
    regen: dict,
    gt_path: str | None,
    source_dump_path: str | None,
    produced_manifest_path: str | None,
    partitions: int,
    workdir: str,
    shards: int = DEFAULT_SHARDS,
    check_value_sha256: bool = True,
) -> dict:
    """Verify GT + source-topic integrity; returns a detail dict with ``source_integrity``."""
    detail: dict = {
        "gt_file": gt_path,
        "gt_sha256_file": None,
        "gt_sha256_regenerated": regen["digest"],
        "gt_lines_regenerated": regen["n_lines"],
        "gt_digest_match": False,
        "source_dump": source_dump_path,
        "produced_manifest": produced_manifest_path,
        "source_records": 0,
        "source_offsets_contiguous": False,
        "source_partitions_observed": 0,
        "source_partitions_expected": partitions,
        "source_partitions_exact": False,
        "source_event_multiset_matches_manifest": False,
        "source_event_multiset_matches_derivation": False,
        "source_dump_digest": None,
        "source_dump_digest_match": False,
        "partition_derivation_mismatches": 0,
        "value_sha256_checked": False,
        "value_sha256_mismatches": 0,
        "malformed_source_lines": 0,
        "malformed_manifest_lines": 0,
        "errors": [],
        "source_integrity": False,
    }

    # --- 1. ground-truth digest -----------------------------------------------------------------
    if gt_path and os.path.exists(gt_path):
        detail["gt_sha256_file"] = D.sha256_file(gt_path)
        detail["gt_digest_match"] = detail["gt_sha256_file"] == regen["digest"]
    else:
        detail["errors"].append("gt file missing")

    if not (source_dump_path and os.path.exists(source_dump_path)):
        detail["errors"].append("source dump missing")
        return detail

    # --- 2. source dump: offsets contiguity + partition count ----------------------------------
    src_dir = _ensure_dir(os.path.join(workdir, "src"))
    src_stats: dict[int, dict] = {}
    src_paths, n_src, n_src_bad = _shard_ndjson(
        source_dump_path, src_dir, "src", shards, ("event_id", "partition", "offset", "value_sha256"),
        stats=src_stats,
    )
    detail["source_records"] = n_src
    detail["malformed_source_lines"] = n_src_bad
    detail["source_partitions_observed"] = len(src_stats)

    contiguous = True
    for p, ps in src_stats.items():
        if ps["min"] is None or ps["max"] is None:
            contiguous = False
            detail["errors"].append(f"partition {p}: missing integer offsets")
            continue
        if ps["min"] != 0 or ps["n"] != (ps["max"] - ps["min"] + 1):
            contiguous = False
            detail["errors"].append(
                f"partition {p}: non-contiguous offsets n={ps['n']} range={ps['min']}..{ps['max']}"
            )
    detail["source_offsets_contiguous"] = contiguous
    detail["source_partitions_exact"] = len(src_stats) == partitions

    # --- 3. produced manifest: multiset of event ids --------------------------------------------
    man_paths = None
    man_total = 0
    if produced_manifest_path and os.path.exists(produced_manifest_path):
        man_dir = _ensure_dir(os.path.join(workdir, "man"))
        man_paths, man_total, n_man_bad = _shard_ndjson(
            produced_manifest_path, man_dir, "man", shards,
            ("event_id", "partition", "offset", "scheduled_at_ms"),
        )
        detail["malformed_manifest_lines"] = n_man_bad
    else:
        detail["errors"].append("produced manifest missing")

    # --- 4. per-shard comparison: manifest vs source vs derivation ------------------------------
    multiset_manifest_ok = man_paths is not None
    multiset_deriv_ok = True
    partition_mismatches = 0
    value_checked = 0
    value_mismatches = 0
    expected_by_bucket: list[dict[str, dict]] = []
    if regen.get("event_shard_paths"):
        for b in range(shards):
            bucket: dict[str, dict] = {}
            for _ln, obj in _iter_ndjson(regen["event_shard_paths"][b]):
                bucket[obj["event_id"]] = obj
            expected_by_bucket.append(bucket)

    with tempfile.TemporaryDirectory(prefix="wxeo-verify-") as tmp:
        # materialise manifest scheduled_at_ms lookup per bucket for the value_sha256 check
        man_lookup: list[dict[str, int] | None] = [None] * shards
        if man_paths is not None:
            for b in range(shards):
                man_lookup[b] = {
                    obj["event_id"]: obj.get("scheduled_at_ms")
                    for _ln, obj in _iter_ndjson(man_paths[b])
                }
        for b in range(shards):
            src_counter: Counter = Counter()
            for _ln, obj in _iter_ndjson(src_paths[b]):
                src_counter[obj["event_id"]] += 1
            if man_paths is not None:
                man_counter: Counter = Counter()
                for _ln, obj in _iter_ndjson(man_paths[b]):
                    man_counter[obj["event_id"]] += 1
                if man_counter != src_counter:
                    multiset_manifest_ok = False
            if expected_by_bucket:
                exp_counter = Counter(expected_by_bucket[b].keys())
                if exp_counter != src_counter:
                    multiset_deriv_ok = False
                # derivation partition check
                dump_rows = [obj for _ln, obj in _iter_ndjson(src_paths[b])]
                for row in dump_rows:
                    exp = expected_by_bucket[b].get(row["event_id"])
                    if exp is not None and exp.get("partition") is not None:
                        if row.get("partition") != exp.get("partition"):
                            partition_mismatches += 1
                # secondary value_sha256 check (not a frozen exclusion reason; diagnostic)
                if check_value_sha256 and man_lookup[b] is not None:
                    for row in dump_rows:
                        eid = row["event_id"]
                        exp = expected_by_bucket[b].get(eid)
                        want = row.get("value_sha256")
                        if exp is None or not want:
                            continue
                        sched = man_lookup[b].get(eid)
                        if sched is None:
                            continue
                        event = {
                            "spec": D.SPEC,
                            "event_id": eid,
                            "seq": exp["seq"],
                            "currency": D.CURRENCY,
                            "account_d": exp["account_d"],
                            "account_c": exp["account_c"],
                            "amount_minor": exp["amount_minor"],
                        }
                        raw = D.canonical_event_value(event, int(sched)).encode("utf-8")
                        value_checked += 1
                        if _sha256_bytes(raw) != want:
                            value_mismatches += 1

    detail["source_event_multiset_matches_manifest"] = multiset_manifest_ok
    detail["source_event_multiset_matches_derivation"] = multiset_deriv_ok
    detail["partition_derivation_mismatches"] = partition_mismatches
    detail["value_sha256_checked"] = value_checked > 0
    detail["value_sha256_mismatches"] = value_mismatches

    # --- 5. source-derived posting digest -------------------------------------------------------
    # If the dump's event-id multiset equals the regenerated one, the canonical posting lines are
    # the same set by construction (derivation is a pure function of (seed, seq) and event_id
    # determines seq), so the source-derived digest is byte-identically the regenerated digest.
    # Otherwise no source-derived digest exists and the gate fails.
    if multiset_deriv_ok and multiset_manifest_ok:
        detail["source_dump_digest"] = regen["digest"]
        detail["source_dump_digest_match"] = True
    else:
        detail["source_dump_digest"] = None
        detail["source_dump_digest_match"] = False

    detail["source_integrity"] = bool(
        detail["gt_digest_match"]
        and detail["source_offsets_contiguous"]
        and detail["source_partitions_exact"]
        and detail["source_event_multiset_matches_manifest"]
        and detail["source_event_multiset_matches_derivation"]
        and detail["source_dump_digest_match"]
        and n_src_bad == 0
        and detail["malformed_manifest_lines"] == 0
    )
    return detail


# --------------------------------------------------------------------------------------------
# observed-dump sharding
# --------------------------------------------------------------------------------------------
def _shard_csv_dump(path: str, out_dir: str, shards: int) -> dict:
    """Shard a PostgreSQL COPY CSV dump (event_id,leg_no,account_id,side,amount_minor,currency)."""
    os.makedirs(out_dir, exist_ok=True)
    handles: dict[int, object] = {}
    stats = {"rows": 0, "malformed": 0, "parse_errors": []}
    try:
        with open(path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row:
                    continue
                if len(row) != 6:
                    stats["malformed"] += 1
                    if len(stats["parse_errors"]) < 5:
                        stats["parse_errors"].append(row)
                    continue
                eid = row[0]
                b = D.eid_bucket(eid, shards)
                h = handles.get(b)
                if h is None:
                    h = open(os.path.join(out_dir, f"obs-{b:03d}.csv"), "w", encoding="utf-8",
                             newline="")
                    handles[b] = h
                h.write(",".join(row) + "\n")
                stats["rows"] += 1
    finally:
        for h in handles.values():
            h.close()
    return stats


def _expand_publication_to_postings(pub_path: str, out_dir: str, shards: int) -> dict:
    """Expand publication-stream entries into posting rows (M1-kafka ledger evaluation).

    Each record value is the canonical entry JSON (method §1.4.3 / S3 §3).  The expansion takes
    the record's own event_id/legs (it is the observed ledger content, not the expected set), so
    a corrupted entry shows up as a mutation/loss rather than being silently repaired.
    """
    os.makedirs(out_dir, exist_ok=True)
    handles: dict[int, object] = {}
    stats = {"records": 0, "parse_errors": 0, "bad_shape": 0, "error_samples": []}
    try:
        for _ln, obj in _iter_ndjson(pub_path):
            stats["records"] += 1
            # accept either a raw record wrapper {value: {...}} or the decoded entry object
            value = obj.get("value", obj) if isinstance(obj, dict) else None
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    stats["parse_errors"] += 1
                    continue
            eid = None
            legs = None
            if isinstance(value, dict):
                eid = value.get("event_id")
                legs = value.get("legs")
            if not eid or not isinstance(legs, list):
                stats["parse_errors"] += 1
                if len(stats["error_samples"]) < 5:
                    stats["error_samples"].append(obj)
                continue
            b = D.eid_bucket(eid, shards)
            h = handles.get(b)
            if h is None:
                h = open(os.path.join(out_dir, f"obs-{b:03d}.csv"), "w", encoding="utf-8",
                         newline="")
                handles[b] = h
            for leg_no, leg in enumerate(legs, start=1):
                if not isinstance(leg, dict):
                    stats["bad_shape"] += 1
                    continue
                account = leg.get("account_id")
                side = leg.get("side")
                amount = leg.get("amount_minor")
                currency = value.get("currency", D.CURRENCY) if isinstance(value, dict) else D.CURRENCY
                if account is None or side is None or amount is None:
                    stats["bad_shape"] += 1
                    continue
                h.write(f"{eid},{leg_no},{account},{side},{amount},{currency}\n")
    finally:
        for h in handles.values():
            h.close()
    return stats


def _shard_publication_content(pub_path: str, out_dir: str, shards: int) -> dict:
    """Shard publication-stream records *with their content* (S5 F-O5).

    The old implementation reduced publication records to event ids, so corrupted legs/amounts
    were invisible for DB-sink mechanisms.  This writes the full decoded entry per record so the
    evaluator can validate content against the regenerated expected entry.
    """
    os.makedirs(out_dir, exist_ok=True)
    handles: dict[int, object] = {}
    stats = {"records": 0, "parse_errors": 0, "error_samples": []}
    try:
        for _ln, obj in _iter_ndjson(pub_path):
            stats["records"] += 1
            value = obj.get("value", obj) if isinstance(obj, dict) else None
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    stats["parse_errors"] += 1
                    if len(stats["error_samples"]) < 5:
                        stats["error_samples"].append({"line": _ln,
                                                       "error": "invalid JSON value"})
                    continue
            eid = value.get("event_id") if isinstance(value, dict) else None
            if not eid:
                stats["parse_errors"] += 1
                if len(stats["error_samples"]) < 5:
                    stats["error_samples"].append({"line": _ln, "error": "missing event_id"})
                continue
            b = D.eid_bucket(eid, shards)
            h = handles.get(b)
            if h is None:
                h = open(os.path.join(out_dir, f"pub-{b:03d}.ndjson"), "w", encoding="utf-8",
                         newline="")
                handles[b] = h
            record = {
                "event_id": eid,
                "partition": obj.get("partition"),
                "offset": obj.get("offset"),
                "value_sha256": obj.get("value_sha256"),
                "entry": value,
            }
            h.write(json.dumps(record, separators=(",", ":")) + "\n")
    finally:
        for h in handles.values():
            h.close()
    return stats


# --------------------------------------------------------------------------------------------
# evaluation (method §2.5 predicates / Appendix A)
# --------------------------------------------------------------------------------------------
def _parse_observed_shard(path: str) -> list[tuple]:
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split(",")
            if len(parts) != 6:
                rows.append(("MALFORMED", line))
                continue
            eid, leg_s, account, side, amount_s, currency = parts
            try:
                leg_no = int(leg_s)
                amount = int(amount_s)
            except ValueError:
                rows.append(("MALFORMED", line))
                continue
            rows.append((eid, leg_no, account, side, amount, currency))
    return rows


def evaluate_observed(
    *,
    regen: dict,
    shards: int,
    observed_dir: str,
    pub_events_dir: str | None = None,
    samples: int = 5,
    expected_sink_rows: int | None = None,
    observed_source: str = "sink dump",
) -> dict:
    """Evaluate the observed multiset bucket by bucket against the regenerated expected set.

    S5 fixes: I1a/I1b are reported separately (F-O3); a fabricated (even perfectly balanced) entry
    is the explicit clause ``v5`` (F-O4); publication-stream *content* is validated and
    ``divergence`` is the documented signed difference (F-O5); a malformed observed row is a hard
    error (F-O7); an independent sink row-count is cross-checked when supplied (F-O7).
    """
    totals = {
        "expected_postings": regen["n_lines"],
        "expected_entries": regen["n_events"],
        "observed_postings": 0,
        "observed_malformed_rows": 0,
        "dup_postings": 0,
        "dup_keys": 0,
        "dup_entries": 0,
        "dup_entry_events": 0,
        "imbalanced_entries": 0,
        "structurally_malformed_entries": 0,
        "imbalanced_sum_entries": 0,
        "mutated_postings": 0,
        "lost_postings": 0,
        "lost_entries": 0,
        "spurious_postings": 0,
        "observed_expected_events": 0,
        "complete_entries": 0,
        "ledger_entries_total": 0,
        "pub_records": None,
        "pub_matched_records": None,
        "pub_dup_records": None,
        "pub_lost_records": None,
        "pub_mutated_records": None,
        "pub_unknown_records": None,
        "pub_divergence": None,
    }
    diag = {
        "dup_key_samples": [],
        "lost_key_samples": [],
        "mutated_key_samples": [],
        "imbalanced_eid_samples": [],
        "structural_eid_samples": [],
        "spurious_key_samples": [],
        "lost_entry_samples": [],
        "pub_dup_eid_samples": [],
        "pub_lost_eid_samples": [],
        "pub_mutated_samples": [],
        "pub_divergence_eid_samples": [],
    }
    pub_paths = None
    if pub_events_dir is not None:
        pub_paths = [os.path.join(pub_events_dir, f"pub-{b:03d}.ndjson") for b in range(shards)]

    for b in range(shards):
        exp: dict[tuple, tuple] = {}
        exp_eids: set = set()
        exp_events: dict[str, dict] = {}
        exp_path = regen["shard_paths"][b]
        if os.path.exists(exp_path):
            with open(exp_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    eid = obj["event_id"]
                    exp[(eid, int(obj["leg_no"]))] = (
                        obj["account_id"], obj["side"], int(obj["amount_minor"]), obj["currency"],
                    )
                    exp_eids.add(eid)
        # expected event index (seq/accounts) for publication content validation
        event_index_path = (regen.get("event_shard_paths") or [None] * shards)[b]
        if event_index_path and os.path.exists(event_index_path):
            with open(event_index_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        event = json.loads(line)
                        exp_events[event["event_id"]] = event

        obs_rows = _parse_observed_shard(os.path.join(observed_dir, f"obs-{b:03d}.csv"))

        obs_key_counts: Counter = Counter()
        obs_key_value: dict[tuple, tuple] = {}
        obs_eid_counts: Counter = Counter()
        obs_eid_rows: dict[str, list] = {}

        # ---- observed rows: a malformed row is a hard provenance failure (F-O7) --------------
        malformed = [row for row in obs_rows if row and row[0] == "MALFORMED"]
        if malformed:
            raise ValueError(
                f"{observed_source}: malformed observed row(s) in shard {b}: "
                f"{len(malformed)} (first: {malformed[0][1][:120]!r}). A corrupted/partial sink "
                f"dump can hide a duplicate or mutation; the run is invalid (F-O7).")
        for row in obs_rows:
            eid, leg_no, account, side, amount, currency = row
            key = (eid, leg_no)
            obs_key_counts[key] += 1
            obs_key_value.setdefault(key, (account, side, amount, currency))
            obs_eid_counts[eid] += 1
            obs_eid_rows.setdefault(eid, []).append((leg_no, account, side, amount, currency))
        totals["observed_postings"] += len(obs_rows)
        totals["ledger_entries_total"] += sum(count // 2 for count in obs_eid_counts.values())

        # ---- I3: duplicate postings / duplicate entries -------------------------------------
        for key, count in obs_key_counts.items():
            if count > 1:
                totals["dup_keys"] += 1
                totals["dup_postings"] += count - 1
                if len(diag["dup_key_samples"]) < samples:
                    diag["dup_key_samples"].append({"event_id": key[0], "leg_no": key[1],
                                                    "multiplicity": count})
        for eid, count in obs_eid_counts.items():
            if count >= 4:
                totals["dup_entries"] += (count // 2) - 1
                totals["dup_entry_events"] += 1

        # ---- I1a (structural) + I1b (signed sum), reported separately (F-O3) ----------------
        for eid, rows in obs_eid_rows.items():
            structural_bad = False
            sum_bad = False
            if len(rows) != 2:
                structural_bad = True
                # net is still meaningful for reporting but a malformed entry is structural
                net = 0
                for r in rows:
                    net += r[3] if r[2] == "D" else -r[3]
                sum_bad = net != 0
            else:
                sides = [r[2] for r in rows]
                amounts = [r[3] for r in rows]
                currencies = {r[4] for r in rows}
                leg_nos = sorted(r[0] for r in rows)
                if sorted(sides) != ["C", "D"]:
                    structural_bad = True
                if len(currencies) != 1 or next(iter(currencies)) != D.CURRENCY:
                    structural_bad = True
                if not all(isinstance(a, int) and a > 0 for a in amounts):
                    structural_bad = True
                if leg_nos != [1, 2]:
                    structural_bad = True
                if amounts[0] != amounts[1]:
                    structural_bad = True
                net = 0
                for r in rows:
                    net += r[3] if r[2] == "D" else -r[3]
                if net != 0:
                    sum_bad = True
            if structural_bad:
                totals["structurally_malformed_entries"] += 1
                if len(diag["structural_eid_samples"]) < samples:
                    diag["structural_eid_samples"].append(
                        {"event_id": eid, "postings": len(rows)})
            if sum_bad:
                totals["imbalanced_sum_entries"] += 1
            if structural_bad or sum_bad:
                totals["imbalanced_entries"] += 1
                if len(diag["imbalanced_eid_samples"]) < samples:
                    diag["imbalanced_eid_samples"].append(
                        {"event_id": eid, "postings": len(rows),
                         "structural": structural_bad, "sum": sum_bad}
                    )

        # ---- I2 (content) / I4 (loss) / spurious -------------------------------------------
        present_expected_keys = set()
        for key, expected_value in exp.items():
            count = obs_key_counts.get(key, 0)
            if count == 0:
                totals["lost_postings"] += 1
                if len(diag["lost_key_samples"]) < samples:
                    diag["lost_key_samples"].append({"event_id": key[0], "leg_no": key[1]})
                continue
            present_expected_keys.add(key)
            if obs_key_value.get(key) != expected_value:
                totals["mutated_postings"] += 1
                if len(diag["mutated_key_samples"]) < samples:
                    diag["mutated_key_samples"].append(
                        {"event_id": key[0], "leg_no": key[1],
                         "observed": list(obs_key_value.get(key, ())),
                         "expected": list(expected_value)}
                    )
        for key in obs_key_counts.keys():
            if key not in exp:
                totals["spurious_postings"] += 1
                if len(diag["spurious_key_samples"]) < samples:
                    diag["spurious_key_samples"].append({"event_id": key[0], "leg_no": key[1]})

        # ---- I4 (entry level): every expected event has both legs --------------------------
        for eid in exp_eids:
            both = ((eid, 1) in obs_key_counts) and ((eid, 2) in obs_key_counts)
            if both:
                totals["observed_expected_events"] += 1
                ok = (
                    obs_key_value.get((eid, 1)) == exp[(eid, 1)]
                    and obs_key_value.get((eid, 2)) == exp[(eid, 2)]
                )
                if ok:
                    totals["complete_entries"] += 1
            else:
                totals["lost_entries"] += 1
                if len(diag["lost_entry_samples"]) < samples:
                    diag["lost_entry_samples"].append({"event_id": eid})

        # ---- publication-stream content + secondary metrics (S5 F-O5) ----------------------
        if pub_paths is not None and os.path.exists(pub_paths[b]):
            if totals["pub_records"] is None:
                totals["pub_records"] = 0
                totals["pub_matched_records"] = 0
                totals["pub_dup_records"] = 0
                totals["pub_lost_records"] = 0
                totals["pub_mutated_records"] = 0
                totals["pub_unknown_records"] = 0
            pub_counter: Counter = Counter()
            for _ln, obj in _iter_ndjson(pub_paths[b]):
                pub_counter[obj["event_id"]] += 1
                totals["pub_records"] += 1
                eid = obj["event_id"]
                entry = obj.get("entry") or {}
                if eid in exp_eids:
                    totals["pub_matched_records"] += 1
                    expected_event = exp_events.get(eid)
                    ok = True
                    if entry.get("spec") != D.ENTRY_SPEC:
                        ok = False
                    if expected_event is None:
                        ok = False
                    else:
                        expected_legs = [
                            {"account_id": expected_event["account_d"], "side": "D",
                             "amount_minor": expected_event["amount_minor"]},
                            {"account_id": expected_event["account_c"], "side": "C",
                             "amount_minor": expected_event["amount_minor"]},
                        ]
                        if entry.get("seq") != expected_event.get("seq"):
                            ok = False
                        if entry.get("legs") != expected_legs:
                            ok = False
                    if not ok:
                        totals["pub_mutated_records"] += 1
                        if len(diag["pub_mutated_samples"]) < samples:
                            diag["pub_mutated_samples"].append(
                                {"event_id": eid, "entry": entry})
                else:
                    totals["pub_unknown_records"] += 1
            for eid, count in pub_counter.items():
                if count > 1:
                    totals["pub_dup_records"] += count - 1
                    if len(diag["pub_dup_eid_samples"]) < samples:
                        diag["pub_dup_eid_samples"].append({"event_id": eid, "multiplicity": count})
            for eid in exp_eids:
                if eid not in pub_counter:
                    totals["pub_lost_records"] += 1
                    if len(diag["pub_lost_eid_samples"]) < samples:
                        diag["pub_lost_eid_samples"].append({"event_id": eid})

    # ---- malformed observed rows / row-count cross-check (F-O7) ----------------------------
    if totals["observed_malformed_rows"]:
        raise ValueError(f"{observed_source}: {totals['observed_malformed_rows']} malformed "
                         f"row(s); run invalid (F-O7)")
    if expected_sink_rows is not None and totals["observed_postings"] != int(expected_sink_rows):
        raise ValueError(
            f"{observed_source}: dump contains {totals['observed_postings']} posting rows but "
            f"the independent DB count is {expected_sink_rows}; the dump is incomplete or the "
            f"observation raced with a write (run invalid, F-O7)")
    if totals["pub_records"] is not None:
        # documented metric (method §7.1): signed difference of stream records vs ledger entries,
        # where ledger entries count whole entries incl. duplicates (Σ floor(mult/2))
        totals["pub_divergence"] = int(
            (totals["pub_matched_records"] or 0) - totals["ledger_entries_total"])

    # ---- violation vector (method §2.5 + A2 amendment clause v5) ----------------------------
    v1 = totals["imbalanced_entries"] > 0
    v2 = totals["mutated_postings"] > 0
    v3 = totals["dup_postings"] > 0
    v4 = totals["lost_postings"] > 0
    v5 = totals["spurious_postings"] > 0
    attribution = []
    if v1:
        attribution.append("I1")
    if totals["structurally_malformed_entries"]:
        attribution.append("I1a")
    if totals["imbalanced_sum_entries"]:
        attribution.append("I1b")
    if v2:
        attribution.append("I2")
    if v3:
        attribution.append("I3")
    if v4:
        attribution.append("I4")
    if v5:
        attribution.append("SPURIOUS")

    return {
        "totals": totals,
        "violations": {
            "v1": bool(v1), "v2": bool(v2), "v3": bool(v3), "v4": bool(v4),
            "v5": bool(v5),
            "violating_run": bool(v1 or v2 or v3 or v4 or v5),
        },
        "attribution": attribution,
        "diagnostics": diag,
    }


# --------------------------------------------------------------------------------------------
# attestation (method §2.5 P8 / Appendix A.3)
# --------------------------------------------------------------------------------------------
def evaluate_attestation(attestation: dict | None, *, required: bool = False) -> dict:
    """Return {can_update, can_delete, posting_triggers, failed, note}.

    ``required=True`` (DB-sink runs) makes an absent attestation a **failure** — I2(a) is then
    unverified and the run must not be scored clean (S5 F-O2).
    """
    if attestation is None:
        return {"present": False, "can_update": None, "can_delete": None,
                "posting_triggers": None,
                "failed": True if required else None,
                "note": ("append-only attestation missing: I2(a) unverified (run violates I2)"
                         if required else "no attestation supplied (not required)")}
    can_update = attestation.get("can_update")
    can_delete = attestation.get("can_delete")
    triggers = attestation.get("posting_triggers", attestation.get("trigger_count"))
    failed = None
    if can_update is not None and can_delete is not None and triggers is not None:
        failed = bool(can_update) or bool(can_delete) or int(triggers) < 1
    elif required:
        failed = True
    return {
        "present": True,
        "can_update": can_update,
        "can_delete": can_delete,
        "posting_triggers": triggers,
        "failed": failed,
        "note": attestation.get("note"),
        "source": attestation.get("source"),
        "app_role": attestation.get("app_role"),
        "posting_rows": attestation.get("posting_rows"),
    }


# --------------------------------------------------------------------------------------------
# oracle context: prepare once, evaluate many sinks (used by the G3 mutation harness)
# --------------------------------------------------------------------------------------------
class OracleContext:
    """Prepared oracle state: regenerated expected set + verified GT/source inputs."""

    def __init__(self, *, master_seed_u64, n_events, partitions, shards, workdir):
        self.master_seed_u64 = int(master_seed_u64)
        self.n_events = int(n_events)
        self.partitions = int(partitions)
        self.shards = int(shards) if shards else DEFAULT_SHARDS
        self.workdir = os.path.abspath(workdir)
        os.makedirs(self.workdir, exist_ok=True)
        self.regen = D.write_expected_shards(
            self.master_seed_u64, self.n_events, os.path.join(self.workdir, "expected"),
            shards=self.shards, partitions=self.partitions,
        )
        self.source_detail: dict | None = None
        self._oracle_script_sha = oracle_script_sha256()

    def verify_source(self, *, gt_path, source_dump_path, produced_manifest_path,
                      check_value_sha256=True) -> dict:
        self.source_detail = verify_source_inputs(
            regen=self.regen, gt_path=gt_path, source_dump_path=source_dump_path,
            produced_manifest_path=produced_manifest_path, partitions=self.partitions,
            workdir=os.path.join(self.workdir, "verify"), shards=self.shards,
            check_value_sha256=check_value_sha256,
        )
        return self.source_detail

    def evaluate_sink(self, sink_dump_path, *, sink_kind="db", publication_dump_path=None,
                      attestation=None, run_id="", mechanism="") -> dict:
        obs_dir = _ensure_dir(os.path.join(self.workdir, "observed"))
        # clear stale shards from a previous evaluation in the same context
        for name in os.listdir(obs_dir):
            os.remove(os.path.join(obs_dir, name))
        pub_dir = None
        pub_stats = None
        if sink_kind == "kafka":
            if publication_dump_path is None:
                raise ValueError("sink_kind=kafka requires --publication-dump (the ledger stream)")
            exp_stats = _expand_publication_to_postings(publication_dump_path, obs_dir, self.shards)
            pub_stats = exp_stats
        else:
            _shard_csv_dump(sink_dump_path, obs_dir, self.shards)
            if publication_dump_path is not None:
                pub_dir = _ensure_dir(os.path.join(self.workdir, "publication"))
                for name in os.listdir(pub_dir):
                    os.remove(os.path.join(pub_dir, name))
                pub_stats = _shard_publication_content(publication_dump_path, pub_dir,
                                                       self.shards)

        # attestation is required for DB sinks (F-O2): a missing attestation is an I2 violation,
        # never a clean run.  The independent DB row count (if the attestation carries it) is used
        # to cross-check the dump (F-O7).
        att = evaluate_attestation(attestation, required=(sink_kind == "db"))
        expected_rows = None
        if sink_kind == "db" and attestation:
            rowcount = attestation.get("posting_rows", attestation.get("row_count"))
            if isinstance(rowcount, int):
                expected_rows = rowcount
        evaluation = evaluate_observed(
            regen=self.regen, shards=self.shards, observed_dir=obs_dir,
            pub_events_dir=pub_dir, expected_sink_rows=expected_rows,
            observed_source=f"{sink_kind} dump",
        )
        totals = evaluation["totals"]
        violations = dict(evaluation["violations"])
        attribution = list(evaluation["attribution"])
        warnings: list[str] = []

        # I2 attestation clause (method §2.6): failure OR absence is a violation for DB sinks
        if att["failed"]:
            violations["v2"] = True
            violations["violating_run"] = True
            attribution.append("ATTESTATION")
            warnings.append("append-only attestation failed/unavailable: I2(a) unverified")

        src = self.source_detail or {}
        source_integrity = bool(src.get("source_integrity", False))
        if self.source_detail is None:
            warnings.append("source verification not run for this context")
        if not source_integrity and self.source_detail is not None:
            warnings.append("source-integrity gate failed: run is INVALID (prereg §8.2 item 2)")
        if src.get("value_sha256_mismatches"):
            warnings.append(f"source value_sha256 mismatches={src['value_sha256_mismatches']} "
                            "(diagnostic only, not an exclusion reason)")
        if src.get("partition_derivation_mismatches"):
            warnings.append(f"source partition derivation mismatches="
                            f"{src['partition_derivation_mismatches']} (diagnostic; check G5)")

        counts = {
            "produced": src.get("source_records") if self.source_detail is not None else None,
            "consumed_estimate": totals["observed_expected_events"],
            "expected_postings": totals["expected_postings"],
            "observed_postings": totals["observed_postings"],
            "dup_postings": totals["dup_postings"],
            "dup_entries": totals["dup_entries"],
            "lost_postings": totals["lost_postings"],
            "lost_entries": totals["lost_entries"],
            "imbalanced_entries": totals["imbalanced_entries"],
            "structurally_malformed_entries": totals["structurally_malformed_entries"],
            "imbalanced_sum_entries": totals["imbalanced_sum_entries"],
            "mutated_postings": totals["mutated_postings"],
            "spurious_postings": totals["spurious_postings"],
            "ledger_entries_total": totals["ledger_entries_total"],
        }
        if sink_kind == "db":
            publication = {
                "pub_records": totals["pub_records"],
                "dup_records": totals["pub_dup_records"],
                "lost_records": totals["pub_lost_records"],
                "divergence": totals["pub_divergence"],
                "pub_mutated_records": totals["pub_mutated_records"],
                "pub_unknown_records": totals["pub_unknown_records"],
                "notes": (None if pub_dir is not None else
                          "no publication-stream dump supplied for this run"),
            }
            if pub_stats and pub_stats.get("parse_errors"):
                warnings.append(f"publication parse errors={pub_stats['parse_errors']}")
                publication["pub_parse_errors"] = pub_stats["parse_errors"]
        else:
            publication = {
                "pub_records": None,
                "dup_records": None,
                "lost_records": None,
                "divergence": None,
                "notes": "M1-kafka: the publication stream is the ledger; see counts",
                "stream_records": pub_stats["records"] if pub_stats else None,
                "stream_parse_errors": pub_stats["parse_errors"] if pub_stats else None,
            }

        result = {
            "schema": ORACLE_VERSION,
            "run_id": run_id,
            "mechanism": mechanism,
            "sink_kind": sink_kind,
            "generated_at_utc": utc_now_iso(),
            "counts": counts,
            "violations": violations,
            "publication": publication,
            "attestation": att,
            "attribution": attribution,
            "source_integrity_detail": self.source_detail,
            "diagnostics": {
                **evaluation["diagnostics"],
                "observed_malformed_rows": totals["observed_malformed_rows"],
                "dup_entry_events": totals["dup_entry_events"],
                "complete_entries": totals["complete_entries"],
                "publication_shard_stats": pub_stats,
            },
            "verdict_rule": VERDICT_RULE,
            "warnings": warnings,
        }
        return result

    def inputs_block(self, paths: dict) -> dict:
        block = {}
        for name, path in paths.items():
            # path hygiene: committed artefacts carry repo-relative paths or <scratch> placeholders
            entry = {"path": pathsafe.repo_rel(path), "sha256": None, "bytes": None}
            if path and os.path.exists(path):
                entry["sha256"] = D.sha256_file(path)
                entry["bytes"] = os.path.getsize(path)
            block[name] = entry
        return block


def finalise_result(result: dict, *, regen: dict, source_detail: dict | None, inputs: dict) -> dict:
    """Add the ``oracle`` digests block and the self-authenticating ``result_sha256``."""
    result["inputs"] = inputs
    result["oracle"] = {
        "gt_sha256": (source_detail or {}).get("gt_sha256_file"),
        "expected_sha256": regen["digest"],
        "source_integrity": bool((source_detail or {}).get("source_integrity", False)),
        "oracle_script_sha256": oracle_script_sha256(),
        "result_sha256": None,
    }
    result = pathsafe.scrub(result)
    payload = json.dumps(result, sort_keys=True, separators=(",", ":")).encode("utf-8")
    result["oracle"]["result_sha256"] = _sha256_bytes(payload)
    return result


# --------------------------------------------------------------------------------------------
# one-shot convenience API
# --------------------------------------------------------------------------------------------
def oracle_run(
    *,
    run_id: str,
    mechanism: str,
    sink_kind: str,
    master_seed_u64: int,
    n_events: int,
    partitions: int,
    gt_path: str,
    source_dump_path: str,
    produced_manifest_path: str,
    sink_dump_path: str | None = None,
    publication_dump_path: str | None = None,
    attestation_path: str | None = None,
    out_path: str | None = None,
    shards: int = DEFAULT_SHARDS,
    workdir: str | None = None,
) -> dict:
    """Full oracle pass for one run; writes ``oracle_<run_id>.json`` when ``out_path`` is set."""
    cleanup = False
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix="wxeo-oracle-")
        cleanup = True
    try:
        ctx = OracleContext(
            master_seed_u64=master_seed_u64, n_events=n_events, partitions=partitions,
            shards=shards, workdir=workdir,
        )
        ctx.verify_source(
            gt_path=gt_path, source_dump_path=source_dump_path,
            produced_manifest_path=produced_manifest_path,
        )
        attestation = _read_json(attestation_path) if attestation_path else None
        result = ctx.evaluate_sink(
            sink_dump_path, sink_kind=sink_kind,
            publication_dump_path=publication_dump_path,
            attestation=attestation, run_id=run_id, mechanism=mechanism,
        )
        inputs = ctx.inputs_block({
            "gt": gt_path,
            "source_dump": source_dump_path,
            "produced_manifest": produced_manifest_path,
            "sink_dump": sink_dump_path,
            "publication_dump": publication_dump_path,
            "attestation": attestation_path,
        })
        result = finalise_result(result, regen=ctx.regen, source_detail=ctx.source_detail,
                                 inputs=inputs)
        if out_path:
            os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
            with open(out_path, "w", encoding="utf-8", newline="") as fh:
                json.dump(result, fh, indent=2, sort_keys=False)
                fh.write("\n")
        return result
    finally:
        if cleanup:
            shutil.rmtree(workdir, ignore_errors=True)


# --------------------------------------------------------------------------------------------
# live dump helpers (read-only; used by the runner during the real campaign)
# --------------------------------------------------------------------------------------------
_SAFE_IDENT = None


def _safe_ident(name: str) -> str:
    global _SAFE_IDENT
    import re
    if _SAFE_IDENT is None:
        _SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    if not _SAFE_IDENT.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def dump_pg(*, pg_dsn: str, schema: str, out_csv: str, attestation_out: str | None = None,
            app_role: str = "wxeo_app") -> dict:
    """Dump the ledger ``posting`` table of one run schema with a READ ONLY transaction.

    ``pg_dsn`` example: ``host=127.0.0.1 port=5433 dbname=wxeo user=wxeo_read``.

    S5 F-O2 fix: the I2 append-only *privilege* attestation queries the privileges of the
    **application role** (``app_role``, default ``wxeo_app``) via ``information_schema`` rather than
    the oracle's own read-only role, and records the independent row count used to cross-check the
    dump completeness (F-O7).
    """
    import psycopg2  # imported lazily: the predicate code stays dependency-free

    schema = _safe_ident(schema)
    _safe_ident(app_role)
    conn = psycopg2.connect(pg_dsn)
    try:
        conn.set_session(readonly=True, autocommit=False)
        cur = conn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        os.makedirs(os.path.dirname(os.path.abspath(out_csv)), exist_ok=True)
        copy_sql = (
            "COPY (SELECT event_id, leg_no, account_id, side, amount_minor, currency "
            f"FROM {schema}.posting ORDER BY event_id, leg_no, posting_id) "
            "TO STDOUT WITH (FORMAT csv)"
        )
        with open(out_csv, "w", encoding="utf-8", newline="") as fh:
            cur.copy_expert(copy_sql, fh)
        cur.execute(f"SELECT count(*) FROM {schema}.posting")
        posting_rows = int(cur.fetchone()[0])
        csv_rows = _count_lines(out_csv)
        info = {"csv": pathsafe.repo_rel(out_csv), "sha256": D.sha256_file(out_csv),
                "rows": csv_rows, "db_rows": posting_rows,
                "rowcount_match": csv_rows == posting_rows}
        if attestation_out:
            # privileges of the APPLICATION role (not the oracle's read-only role): an UPDATE or
            # DELETE grant to the app role (or a missing deny-trigger) is an I2(a) enforcement
            # failure, independent of end-state content.
            cur.execute(
                "SELECT privilege_type FROM information_schema.role_table_grants "
                "WHERE table_schema = %s AND table_name = 'posting' AND grantee = %s",
                (schema, app_role),
            )
            granted = {row[0].upper() for row in cur.fetchall()}
            can_update = "UPDATE" in granted
            can_delete = "DELETE" in granted
            cur.execute(
                "SELECT count(*) FROM pg_trigger WHERE tgrelid = %s::regclass AND NOT tgisinternal",
                (f"{schema}.posting",),
            )
            triggers = int(cur.fetchone()[0])
            att = {
                "can_update": bool(can_update),
                "can_delete": bool(can_delete),
                "posting_triggers": triggers,
                "posting_rows": posting_rows,
                "app_role": app_role,
                "source": "live:dump_pg",
                "schema": schema,
                "note": "queries the APPLICATION role's privileges via information_schema "
                        "(read-only oracle role cannot modify the sink); a missing BEFORE "
                        "UPDATE/DELETE trigger or any UPDATE/DELETE grant is an I2(a) failure",
            }
            att = pathsafe.scrub(att)
            with open(attestation_out, "w", encoding="utf-8", newline="") as fh:
                json.dump(att, fh, indent=2)
                fh.write("\n")
            info["attestation"] = att
        conn.commit()
        return info
    finally:
        conn.close()


def _count_lines(path: str) -> int:
    n = 0
    with open(path, "rb") as fh:
        for _chunk in iter(lambda: fh.read(1 << 20), b""):
            n += _chunk.count(b"\n")
    return n


def _kafka_consumer(bootstrap: str, group: str, isolation: str, auto_commit=False):
    """Create a confluent-kafka Consumer (lazy import; librdkafka wheels bundled)."""
    try:
        from confluent_kafka import Consumer, KafkaError  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "a Kafka client is required for live dumps; install with "
            "`python -m pip install confluent-kafka` (S4 tooling dependency, not part of the "
            "oracle predicate code)"
        ) from exc
    conf = {
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": bool(auto_commit),
        "isolation.level": isolation,
        "enable.partition.eof": True,
    }
    return Consumer(conf)


def dump_source_topic(*, bootstrap: str, topic: str, out_ndjson: str, group: str,
                      timeout_s: float = 120.0, consumer_factory=None) -> dict:
    """Independent dump of the source topic -> {partition,offset,event_id,value_sha256}.

    ``consumer_factory`` allows an injectable client (used by the live-dump smoke test so the
    code path is actually exercised without a broker; F-O1).
    """
    consumer = (consumer_factory() if consumer_factory
                else _kafka_consumer(bootstrap, group, "read_committed"))
    from confluent_kafka import TopicPartition, KafkaException  # lazy

    md = consumer.list_topics(topic, timeout=30)
    parts = sorted(md.topics[topic].partitions.keys())
    tps = [TopicPartition(topic, p) for p in parts]
    consumer.assign(tps)
    end_offsets: dict[int, int] = {}
    for tp in tps:
        _low, high = consumer.get_watermark_offsets(tp, timeout=30, cached=False)
        end_offsets[tp.partition] = high
        consumer.seek(TopicPartition(topic, tp.partition, 0))
    remaining = set(parts)
    rows = []
    deadline = time.time() + timeout_s
    while remaining and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            from confluent_kafka import KafkaError
            if msg.error().code() == KafkaError._PARTITION_EOF:
                if msg.partition() in remaining:
                    remaining.discard(msg.partition())
                continue
            raise KafkaException(msg.error())
        value = msg.value() or b""
        try:
            obj = json.loads(value.decode("utf-8"))
            eid = obj["event_id"]
        except Exception:
            eid = None
        rows.append({
            "partition": msg.partition(),
            "offset": msg.offset(),
            "event_id": eid,
            "value_sha256": _sha256_bytes(value),
            "value": value.decode("utf-8", "replace"),
        })
        if msg.offset() + 1 >= end_offsets.get(msg.partition(), 0):
            remaining.discard(msg.partition())
    consumer.close()
    rows.sort(key=lambda r: (r["partition"], r["offset"]))
    os.makedirs(os.path.dirname(os.path.abspath(out_ndjson)), exist_ok=True)
    with open(out_ndjson, "w", encoding="utf-8", newline="") as fh:
        for row in rows:
            row = dict(row)
            row.pop("value", None)  # source dump contract: value_sha256 only
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
    return {"path": pathsafe.repo_rel(out_ndjson), "sha256": D.sha256_file(out_ndjson),
            "records": len(rows), "partitions": parts}


def dump_kafka_topic(*, bootstrap: str, topic: str, out_ndjson: str, group: str,
                     isolation: str = "read_committed", timeout_s: float = 120.0,
                     consumer_factory=None) -> dict:
    """Dump the publication/sink stream (decoded entry value + value_sha256)."""
    consumer = (consumer_factory() if consumer_factory
                else _kafka_consumer(bootstrap, group, isolation))
    from confluent_kafka import TopicPartition, KafkaException  # lazy

    md = consumer.list_topics(topic, timeout=30)
    parts = sorted(md.topics[topic].partitions.keys())
    tps = [TopicPartition(topic, p) for p in parts]
    consumer.assign(tps)
    end_offsets: dict[int, int] = {}
    for tp in tps:
        _low, high = consumer.get_watermark_offsets(tp, timeout=30, cached=False)
        end_offsets[tp.partition] = high
        consumer.seek(TopicPartition(topic, tp.partition, 0))
    remaining = set(parts)
    rows = []
    deadline = time.time() + timeout_s
    while remaining and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            from confluent_kafka import KafkaError
            if msg.error().code() == KafkaError._PARTITION_EOF:
                remaining.discard(msg.partition())
                continue
            raise KafkaException(msg.error())
        value = msg.value() or b""
        row = {"partition": msg.partition(), "offset": msg.offset(),
               "key": (msg.key() or b"").decode("utf-8", "replace"),
               "value_sha256": _sha256_bytes(value),
               "value": value.decode("utf-8", "replace")}
        rows.append(row)
        if msg.offset() + 1 >= end_offsets.get(msg.partition(), 0):
            remaining.discard(msg.partition())
    consumer.close()
    rows.sort(key=lambda r: (r["partition"], r["offset"]))
    os.makedirs(os.path.dirname(os.path.abspath(out_ndjson)), exist_ok=True)
    with open(out_ndjson, "w", encoding="utf-8", newline="") as fh:
        for row in rows:
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
    return {"path": pathsafe.repo_rel(out_ndjson), "sha256": D.sha256_file(out_ndjson),
            "records": len(rows), "partitions": parts, "isolation": isolation}


# --------------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------------
def _seed_from_args(args) -> int:
    if getattr(args, "master_seed_u64", None) is not None:
        return D.seed_hex_to_u64(args.master_seed_u64) if len(str(args.master_seed_u64)) <= 16 \
            and not str(args.master_seed_u64).isdigit() else int(args.master_seed_u64)
    if not args.data_seed_hex:
        raise SystemExit("either --data-seed-hex or --master-seed-u64 is required")
    return D.seed_hex_to_u64(args.data_seed_hex)


def _cmd_run(args) -> int:
    seed = _seed_from_args(args)
    result = oracle_run(
        run_id=args.run_id, mechanism=args.mechanism, sink_kind=args.sink_kind,
        master_seed_u64=seed, n_events=args.events, partitions=args.partitions,
        gt_path=args.gt, source_dump_path=args.source_dump,
        produced_manifest_path=args.produced, sink_dump_path=args.sink_dump,
        publication_dump_path=args.publication_dump, attestation_path=args.attestation,
        out_path=args.out, shards=args.shards,
        workdir=args.workdir,
    )
    _print_summary(result)
    return 0


def _cmd_verify(args) -> int:
    seed = _seed_from_args(args)
    with tempfile.TemporaryDirectory(prefix="wxeo-verify-") as tmp:
        ctx = OracleContext(master_seed_u64=seed, n_events=args.events,
                            partitions=args.partitions, shards=args.shards, workdir=tmp)
        detail = ctx.verify_source(
            gt_path=args.gt, source_dump_path=args.source_dump,
            produced_manifest_path=args.produced,
        )
        out = {
            "schema": "wxeo.source-verify.v1",
            "generated_at_utc": utc_now_iso(),
            "n_events": args.events,
            "partitions": args.partitions,
            "regenerated_digest": ctx.regen["digest"],
            "detail": detail,
            "oracle_script_sha256": oracle_script_sha256(),
        }
        if args.out:
            os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
            with open(args.out, "w", encoding="utf-8", newline="") as fh:
                json.dump(out, fh, indent=2)
                fh.write("\n")
        print(json.dumps({"source_integrity": detail["source_integrity"],
                          "gt_digest_match": detail["gt_digest_match"],
                          "regenerated_digest": ctx.regen["digest"],
                          "errors": detail["errors"]}, indent=2))
    return 0


def _cmd_dump_pg(args) -> int:
    info = dump_pg(pg_dsn=args.pg_dsn, schema=args.schema, out_csv=args.out,
                   attestation_out=args.attestation_out)
    print(json.dumps(info, indent=2, default=str))
    return 0


def _cmd_dump_source(args) -> int:
    info = dump_source_topic(bootstrap=args.bootstrap, topic=args.topic, out_ndjson=args.out,
                             group=args.group, timeout_s=args.timeout)
    print(json.dumps(info, indent=2))
    return 0


def _cmd_dump_kafka(args) -> int:
    info = dump_kafka_topic(bootstrap=args.bootstrap, topic=args.topic, out_ndjson=args.out,
                            group=args.group, isolation=args.isolation,
                            timeout_s=args.timeout)
    print(json.dumps(info, indent=2))
    return 0


def _cmd_selftest(args) -> int:
    """Fast internal consistency check of the derivation + digest contract."""
    seed = 1234567890123
    info = D.write_gt_file(seed, 8, os.path.join(tempfile.mkdtemp(prefix="wxeo-self-"), "gt"))
    assert info["n_lines"] == 16
    rows = [json.loads(line) for line in open(info["gt_path"], encoding="utf-8")]
    assert len(rows) == 16
    assert [r["line_no"] for r in rows] == list(range(16))
    assert [(r["event_id"], r["leg_no"]) for r in rows] == sorted(
        (r["event_id"], r["leg_no"]) for r in rows)
    by_event = {}
    for r in rows:
        by_event.setdefault(r["event_id"], []).append(r)
    for eid, legs in by_event.items():
        assert len(legs) == 2
        assert {legs[0]["side"], legs[1]["side"]} == {"D", "C"}
        assert legs[0]["amount_minor"] == legs[1]["amount_minor"]
        assert legs[0]["currency"] == "BRL"
    print(json.dumps({"selftest": "PASS", "lines": info["n_lines"],
                      "digest": info["digest"]}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Independent oracle for WXEO experiments")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common_seed(sp):
        sp.add_argument("--data-seed-hex", help="per-run data_seed (hex, prereg §4.4)")
        sp.add_argument("--master-seed-u64", help="per-run seed as u64 (decimal)")
        sp.add_argument("--events", type=int, required=True)
        sp.add_argument("--partitions", type=int, required=True)
        sp.add_argument("--shards", type=int, default=DEFAULT_SHARDS)

    r = sub.add_parser("run", help="verify inputs + evaluate one sink and write oracle JSON")
    r.add_argument("--run-id", required=True)
    r.add_argument("--mechanism", required=True)
    r.add_argument("--sink-kind", choices=["db", "kafka"], default="db")
    common_seed(r)
    r.add_argument("--gt", required=True)
    r.add_argument("--source-dump", required=True)
    r.add_argument("--produced", required=True)
    r.add_argument("--sink-dump", default=None)
    r.add_argument("--publication-dump", default=None)
    r.add_argument("--attestation", default=None)
    r.add_argument("--out", required=True)
    r.add_argument("--workdir", default=None)
    r.set_defaults(func=_cmd_run)

    v = sub.add_parser("verify", help="verify GT digest + source-topic integrity only")
    common_seed(v)
    v.add_argument("--gt", required=True)
    v.add_argument("--source-dump", required=True)
    v.add_argument("--produced", required=True)
    v.add_argument("--out", default=None)
    v.set_defaults(func=_cmd_verify)

    d = sub.add_parser("dump-pg", help="dump ledger posting table (read-only)")
    d.add_argument("--pg-dsn", required=True)
    d.add_argument("--schema", required=True)
    d.add_argument("--out", required=True)
    d.add_argument("--attestation-out", default=None)
    d.set_defaults(func=_cmd_dump_pg)

    s = sub.add_parser("dump-source", help="independent dump of the source topic")
    s.add_argument("--bootstrap", required=True)
    s.add_argument("--topic", required=True)
    s.add_argument("--group", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--timeout", type=float, default=120.0)
    s.set_defaults(func=_cmd_dump_source)

    k = sub.add_parser("dump-kafka", help="dump the publication/sink stream")
    k.add_argument("--bootstrap", required=True)
    k.add_argument("--topic", required=True)
    k.add_argument("--group", required=True)
    k.add_argument("--isolation", choices=["read_committed", "read_uncommitted"],
                   default="read_committed")
    k.add_argument("--out", required=True)
    k.add_argument("--timeout", type=float, default=120.0)
    k.set_defaults(func=_cmd_dump_kafka)

    st = sub.add_parser("selftest", help="internal derivation/digest consistency check")
    st.set_defaults(func=_cmd_selftest)
    return p


def _print_summary(result: dict) -> None:
    summary = {
        "run_id": result.get("run_id"),
        "mechanism": result.get("mechanism"),
        "sink_kind": result.get("sink_kind"),
        "counts": result.get("counts"),
        "violations": result.get("violations"),
        "attribution": result.get("attribution"),
        "source_integrity": (result.get("oracle") or {}).get("source_integrity"),
        "gt_sha256": (result.get("oracle") or {}).get("gt_sha256"),
        "expected_sha256": (result.get("oracle") or {}).get("expected_sha256"),
        "result_sha256": (result.get("oracle") or {}).get("result_sha256"),
        "warnings": result.get("warnings"),
    }
    print(json.dumps(summary, indent=2))


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except BrokenPipeError:
        return 0
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        print(f"ORACLE ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
