"""Synthetic fixtures for the WXEO experiments (clearly synthetic, never results).

Everything produced here is labelled ``synthetic`` in its metadata and lives outside
``results/raw/``.  Fixtures exist for three purposes only:

1. **G3 mutation testing** — a clean, internally consistent run bundle (ground truth + source dump +
   produced manifest + sink dump + publication dump + attestation) into which the mutation harness
   injects exactly one known defect at a time.
2. **Runner dry-run** — exercises the full run-record assembly, oracle invocation, artefact
   archiving and RUNLOG path without any live Kafka/PostgreSQL/Toxiproxy process.
3. **G5 cross-implementation vectors** — deterministic (seed, N, P) -> GT digest inputs that the
   Java load generator must reproduce byte-for-byte.

The fixtures emulate the subtree of artefacts that S3's processes would write; they make no
claim about measured behaviour.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
from datetime import datetime, timezone

try:  # repo-root import when executed as a module
    from experiments.oracle import derive as D
except ImportError:  # script execution: add repo root
    import sys
    _REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.insert(0, _REPO)
    from experiments.oracle import derive as D  # type: ignore

RUN_SPEC = "wxeo.run-fixture.v1"
#: fully synthetic base timestamp for produced manifests (2026-11-24T12:00:00Z)
SYNTHETIC_EPOCH_MS = 1_763_995_200_000


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def write_clean_run(
    out_dir: str,
    *,
    run_id: str = "pilot-m2-f0-1k-p3-r00",
    mechanism: str = "M2",
    sink_kind: str = "db",
    seed_hex: str = "0f0e0d0c0b0a0908",
    n_events: int = 200,
    partitions: int = 3,
    base_rate: float = 2000.0,
    observed_events: int | None = None,
    drop_events: int = 0,
    label: str = "synthetic-clean",
) -> dict:
    """Write a self-consistent run bundle emulating S3's per-run artefacts.

    ``observed_events``/``drop_events`` allow a *deliberately incomplete* sink (for dry-run demos);
    the default is a perfectly clean run.  The GT and source dumps are always complete and valid.
    Returns a dict describing the fixture paths and the expected oracle outcome metadata.
    """
    os.makedirs(out_dir, exist_ok=True)
    seed_u64 = D.seed_hex_to_u64(seed_hex)
    n = int(n_events)
    if observed_events is None:
        observed_events = n
    observed_events = max(0, min(int(observed_events), n))

    gt_path = os.path.join(out_dir, f"gt_{run_id}.ndjson")
    gt_info = D.write_gt_file(seed_u64, n, gt_path)
    gt_sha = gt_info["gt_sha256"]
    with open(os.path.join(out_dir, f"gt_{run_id}.sha256"), "w", encoding="utf-8", newline="") as fh:
        fh.write(gt_sha + "\n")

    # ---- produced manifest + source dump (one record per event) --------------------------------
    rows_expected = []  # posting rows in canonical observation order (eid, leg_no)
    source_rows = []
    produced_rows = []
    pub_rows = []
    offsets = {p: 0 for p in range(partitions)}
    interval_ms = 1000.0 / base_rate
    for seq in range(n):
        ev = D.derive_event(seed_u64, seq, partitions)
        eid = ev["event_id"]
        part = ev["partition"]
        offset = offsets[part]
        offsets[part] += 1
        scheduled = int(SYNTHETIC_EPOCH_MS + seq * interval_ms)
        ack = scheduled + 3
        value = D.canonical_event_value(ev, scheduled).encode("utf-8")
        source_rows.append({
            "partition": part,
            "offset": offset,
            "event_id": eid,
            "value_sha256": hashlib.sha256(value).hexdigest(),
        })
        produced_rows.append({
            "event_id": eid,
            "partition": part,
            "offset": offset,
            "ack_ts_ms": ack,
            "scheduled_at_ms": scheduled,
        })
        entry = D.canonical_entry_value(ev).encode("utf-8")
        pub_rows.append({
            "partition": part,
            "offset": offset,
            "key": eid,
            "event_id": eid,
            "seq": seq,
            "value_sha256": hashlib.sha256(entry).hexdigest(),
            "value": D.canonical_entry_value(ev),
        })
        for leg_no, leg in enumerate(D.event_legs(ev), start=1):
            rows_expected.append(
                (eid, leg_no, leg["account_id"], leg["side"], leg["amount_minor"], D.CURRENCY)
            )

    source_rows.sort(key=lambda r: (r["partition"], r["offset"]))
    _write_ndjson(os.path.join(out_dir, f"source_dump_{run_id}.ndjson"), source_rows)
    _write_ndjson(os.path.join(out_dir, f"produced_{run_id}.ndjson"), produced_rows)
    _write_ndjson(os.path.join(out_dir, f"publication_{run_id}.ndjson"), pub_rows)

    # ---- observed sink ------------------------------------------------------------------------
    rng = random.Random(int.from_bytes(hashlib.sha256(b"synthetic-observed").digest()[:8], "big"))
    observed_eids = sorted({r[0] for r in rows_expected})
    if observed_events < n:
        drop_n = n - observed_events
        dropped = set(rng.sample(observed_eids, min(drop_n, len(observed_eids))))
    else:
        dropped = set()
    if drop_events and observed_events < n:
        # additionally drop whole events from the *sink* (loss beyond the truncation)
        extra = set(rng.sample(observed_eids, min(drop_events, len(observed_eids) - len(dropped))))
        dropped |= extra
    sink_rows = [r for r in rows_expected if r[0] not in dropped]
    sink_csv = os.path.join(out_dir, f"sink_dump_{run_id}.csv")
    with open(sink_csv, "w", encoding="utf-8", newline="") as fh:
        for row in sink_rows:
            fh.write(",".join(str(x) for x in row) + "\n")

    # ---- manifest + attestation + fixture descriptor ------------------------------------------
    manifest = {
        "spec": "wxeo.gt-manifest.v1",
        "run_id": run_id,
        "master_seed_raw": str(seed_u64),
        "master_seed": str(seed_u64),
        "events": n,
        "partitions": partitions,
        "gt_path": os.path.basename(gt_path),
        "gt_sha256": gt_sha,
        "gt_lines": gt_info["n_lines"],
        "produced_path": f"produced_{run_id}.ndjson",
        "topic": f"wxeo.src.{run_id}",
        "rate": base_rate,
        "created_utc": _utc_now(),
        "synthetic": True,
    }
    _write_json(os.path.join(out_dir, f"manifest_{run_id}.json"), manifest)

    attestation = {
        "can_update": False,
        "can_delete": False,
        "posting_triggers": 1,
        "source": "fixture:attestation",
        "note": "synthetic attestation: append-only privileges/trigger as specified",
    }
    _write_json(os.path.join(out_dir, f"attestation_{run_id}.json"), attestation)

    descriptor = {
        "schema": RUN_SPEC,
        "synthetic": True,
        "run_id": run_id,
        "mechanism": mechanism,
        "sink_kind": sink_kind,
        "seed_hex": seed_hex,
        "seed_u64": seed_u64,
        "events": n,
        "partitions": partitions,
        "observed_postings": len(sink_rows),
        "expected_postings": len(rows_expected),
        "paths": {
            "gt": os.path.basename(gt_path),
            "source_dump": f"source_dump_{run_id}.ndjson",
            "produced": f"produced_{run_id}.ndjson",
            "sink_dump": os.path.basename(sink_csv),
            "publication_dump": f"publication_{run_id}.ndjson",
            "manifest": f"manifest_{run_id}.json",
            "attestation": f"attestation_{run_id}.json",
        },
        "gt_sha256": gt_sha,
        "generated_at_utc": _utc_now(),
        "label": label,
        "note": "SYNTHETIC fixture - not a measurement, never part of results/raw",
    }
    _write_json(os.path.join(out_dir, "fixture.json"), descriptor)
    return descriptor


def load_clean_rows(sink_csv: str) -> list[list[str]]:
    rows = []
    with open(sink_csv, "r", encoding="utf-8", newline="") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line:
                rows.append(line.split(","))
    return rows


def write_sink_rows(path: str, rows) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fh:
        for row in rows:
            fh.write(",".join(str(x) for x in row) + "\n")


def mutate_rows(rows: list[list[str]], defect_class: str, rng: random.Random,
                expected_event_ids: set[str] | None = None) -> tuple[list[list[str]], dict]:
    """Apply exactly one synthetic defect of ``defect_class``; returns (rows, mutation_info).

    Defect classes (docs/method.md §7.6 gate G3, extended after the S5 review):
      ``drop_row``             — remove one existing posting row            (I4 loss)
      ``delete_event``         — remove both legs of one event              (I4 loss)
      ``duplicate_row``        — insert an exact copy of one posting row    (I3 dup)
      ``duplicate_entry``      — duplicate the whole balanced entry         (I3 + I1a)  [F-O3/O6]
      ``flip_leg_sign``        — swap the side of one row (D <-> C)         (I2 mutation)
      ``update_row_value``     — change account_id or amount of one row     (I2 mutation)
      ``insert_spurious_row``  — insert one unbalanced fabricated posting   (v5 SPURIOUS)
      ``balanced_spurious_entry`` — insert a balanced fabricated entry      (v5 SPURIOUS) [F-O4]
      ``currency_mix``         — mix currencies inside one entry            (I1a/I2)     [F-O6]
      ``negative_amount``      — negative amount on both legs (sum zero)    (I1a/I2)     [F-O6]
    """
    out = [list(r) for r in rows]
    info: dict = {"defect_class": defect_class}
    if defect_class == "drop_row":
        idx = rng.randrange(len(out))
        info.update(row_index=idx, row=out[idx])
        del out[idx]
    elif defect_class == "delete_event":
        eids = sorted({r[0] for r in out})
        eid = rng.choice(eids)
        removed = [r for r in out if r[0] == eid]
        out = [r for r in out if r[0] != eid]
        info.update(event_id=eid, removed_rows=len(removed))
    elif defect_class == "duplicate_row":
        idx = rng.randrange(len(out))
        copy = list(out[idx])
        pos = rng.randrange(len(out) + 1)
        out.insert(pos, copy)
        info.update(source_index=idx, inserted_at=pos, row=copy)
    elif defect_class == "duplicate_entry":
        eids = sorted({r[0] for r in out})
        eid = rng.choice(eids)
        legs = [list(r) for r in out if r[0] == eid]
        baseline = len(out)
        # insert the duplicate pair right after the original entry's last row
        insert_at = max(i for i, r in enumerate(out) if r[0] == eid) + 1
        for offset, leg in enumerate(legs):
            out.insert(insert_at + offset, list(leg))
        info.update(event_id=eid, duplicated_rows=len(legs), baseline_rows=baseline)
    elif defect_class == "flip_leg_sign":
        idx = rng.randrange(len(out))
        row = out[idx]
        row[3] = "C" if row[3] == "D" else "D"
        info.update(row_index=idx, row=row)
    elif defect_class == "update_row_value":
        idx = rng.randrange(len(out))
        row = out[idx]
        field = rng.choice(["account_id", "amount_minor"])
        if field == "account_id":
            row[2] = "acct:" + "%012x" % rng.getrandbits(48)
        else:
            row[4] = str(int(row[4]) + rng.randrange(1, 1000))
        info.update(row_index=idx, field=field, row=row)
    elif defect_class == "insert_spurious_row":
        eid = _fresh_eid(rng, expected_event_ids)
        row = [eid, "1", "acct:" + "%012x" % rng.getrandbits(48), "D",
               str(rng.randrange(1, 100000)), D.CURRENCY]
        pos = rng.randrange(len(out) + 1)
        out.insert(pos, row)
        info.update(inserted_at=pos, row=row)
    elif defect_class == "balanced_spurious_entry":
        eid = _fresh_eid(rng, expected_event_ids)
        amount = rng.randrange(1, 100000)
        debit = [eid, "1", "acct:" + "%012x" % rng.getrandbits(48), "D", str(amount),
                 D.CURRENCY]
        credit = [eid, "2", "acct:" + "%012x" % rng.getrandbits(48), "C", str(amount),
                  D.CURRENCY]
        pos = rng.randrange(len(out) + 1)
        out.insert(pos, debit)
        out.insert(pos + 1, credit)
        info.update(inserted_at=pos, event_id=eid, amount=amount)
    elif defect_class == "currency_mix":
        eids = sorted({r[0] for r in out})
        eid = rng.choice(eids)
        legs = [r for r in out if r[0] == eid]
        other = rng.choice(["USD", "EUR"])
        target = legs[rng.randrange(len(legs))]
        target[5] = other
        info.update(event_id=eid, currency=other)
    elif defect_class == "negative_amount":
        eids = sorted({r[0] for r in out})
        eid = rng.choice(eids)
        for row in out:
            if row[0] == eid:
                row[4] = str(-abs(int(row[4])))
        info.update(event_id=eid, mutated="negative amounts, sum still zero")
    else:
        raise ValueError(f"unknown defect class {defect_class!r}")
    return out, info


def _fresh_eid(rng: random.Random, expected_event_ids: set[str] | None) -> str:
    eid = "%032x" % rng.getrandbits(128)
    if expected_event_ids:
        while eid in expected_event_ids:
            eid = "%032x" % rng.getrandbits(128)
    return eid


def mutate_publication_records(records: list[dict], defect_class: str, rng: random.Random,
                               expected_event_ids: set[str] | None = None
                               ) -> tuple[list[dict], dict]:
    """Mutate the publication stream (used for the M1-kafka ledger corpus and pub metrics).

    Classes: ``duplicate_publication_record`` (I3), ``drop_publication_record`` (I4),
    ``mutate_publication_leg`` (I2/I1a), ``insert_balanced_publication_record`` (v5 SPURIOUS).
    """
    out = [json.loads(json.dumps(rec)) for rec in records]
    info: dict = {"defect_class": defect_class}
    if defect_class == "duplicate_publication_record":
        idx = rng.randrange(len(out))
        out.insert(rng.randrange(len(out) + 1), json.loads(json.dumps(out[idx])))
        info.update(source_index=idx)
    elif defect_class == "drop_publication_record":
        idx = rng.randrange(len(out))
        info.update(dropped_index=idx, event_id=out[idx].get("event_id"))
        del out[idx]
    elif defect_class == "mutate_publication_leg":
        idx = rng.randrange(len(out))
        rec = out[idx]
        entry = rec.get("entry") or {}
        legs = entry.get("legs") or []
        if not legs:
            raise ValueError("publication record has no legs")
        leg = legs[rng.randrange(len(legs))]
        field = rng.choice(["account_id", "amount_minor"])
        if field == "account_id":
            leg["account_id"] = "acct:" + "%012x" % rng.getrandbits(48)
        else:
            leg["amount_minor"] = int(leg.get("amount_minor") or 1) + rng.randrange(1, 1000)
        rec["entry"] = entry
        info.update(record_index=idx, event_id=rec.get("event_id"), field=field)
    elif defect_class == "insert_balanced_publication_record":
        eid = _fresh_eid(rng, expected_event_ids)
        amount = rng.randrange(1, 100000)
        entry = {
            "spec": D.ENTRY_SPEC, "event_id": eid, "seq": -1,
            "legs": [
                {"account_id": "acct:" + "%012x" % rng.getrandbits(48), "side": "D",
                 "amount_minor": amount},
                {"account_id": "acct:" + "%012x" % rng.getrandbits(48), "side": "C",
                 "amount_minor": amount},
            ],
        }
        record = {"partition": 0, "offset": -1, "key": eid, "value_sha256": None,
                  "value": json.dumps(entry, separators=(",", ":"))}
        out.insert(rng.randrange(len(out) + 1), record)
        info.update(event_id=eid, amount=amount)
    else:
        raise ValueError(f"unknown publication defect class {defect_class!r}")
    return out, info


def write_publication_records(path: str, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fh:
        for rec in records:
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")


def load_publication_records(path: str) -> list[dict]:
    records = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


#: DB-sink defect classes and their expected primary invariant attribution.
#: ``kind=publication`` classes are secondary metrics (method §2.7) and are checked by an
#: explicit metric predicate instead of the run verdict.
DEFECT_CLASSES = (
    "drop_row",
    "delete_event",
    "duplicate_row",
    "duplicate_entry",
    "flip_leg_sign",
    "update_row_value",
    "insert_spurious_row",
    "balanced_spurious_entry",
    "currency_mix",
    "negative_amount",
)

EXPECTED_ATTRIBUTION = {
    "drop_row": {"primary": "I4", "allowed_extra": {"I1", "I1a"}},
    "delete_event": {"primary": "I4", "allowed_extra": set()},
    "duplicate_row": {"primary": "I3", "allowed_extra": {"I1", "I1a"}},
    "duplicate_entry": {"primary": "I3", "allowed_extra": {"I1", "I1a"}},
    "flip_leg_sign": {"primary": "I2", "allowed_extra": {"I1", "I1a", "I1b"}},
    "update_row_value": {"primary": "I2", "allowed_extra": {"I1", "I1a", "I1b"}},
    "insert_spurious_row": {"primary": "SPURIOUS", "allowed_extra": {"I1", "I1a"}},
    "balanced_spurious_entry": {"primary": "SPURIOUS", "allowed_extra": set()},
    "currency_mix": {"primary": "I1", "allowed_extra": {"I1a", "I2"}},
    "negative_amount": {"primary": "I1", "allowed_extra": {"I1a", "I2"}},
}

#: attestation defect classes (oracle-level, no sink mutation)
ATTESTATION_CLASSES = ("attestation_granted", "attestation_missing")
ATTESTATION_EXPECTED = {
    "attestation_granted": {"primary": "I2", "allowed_extra": set()},
    "attestation_missing": {"primary": "I2", "allowed_extra": set()},
}

#: publication-stream defect classes (secondary metrics; detection predicate, not verdict)
PUBLICATION_CLASSES = ("pub_record_lost", "pub_content_corrupt")
PUBLICATION_EXPECTED = {
    "pub_record_lost": "lost_records>=1 and divergence<0",
    "pub_content_corrupt": "pub_mutated_records>=1",
}

#: M1-kafka ledger corpus: mutations applied to the publication stream, which IS the ledger
KAFKA_CLASSES = (
    "duplicate_publication_record",
    "drop_publication_record",
    "mutate_publication_leg",
    "insert_balanced_publication_record",
)
KAFKA_EXPECTED = {
    "duplicate_publication_record": {"primary": "I3", "allowed_extra": {"I1", "I1a"}},
    "drop_publication_record": {"primary": "I4", "allowed_extra": {"I1", "I1a"}},
    "mutate_publication_leg": {"primary": "I2", "allowed_extra": {"I1", "I1a", "I1b"}},
    "insert_balanced_publication_record": {"primary": "SPURIOUS", "allowed_extra": set()},
}


def _write_json(path: str, obj) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fh:
        json.dump(obj, fh, indent=2)
        fh.write("\n")


def _write_ndjson(path: str, rows) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fh:
        for row in rows:
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")


def make_g5_vectors(n_events: int = 100, partitions: int = 6) -> dict:
    """Hard-coded Java<->Python cross-implementation vectors (gate G5).

    Each vector fixes ``(seed_hex, events, partitions)`` and the exact GT digest plus four sample
    canonical lines.  S3's Java load generator must reproduce these byte-for-byte.
    """
    vectors = []
    seeds = ["0000000000000001", "ffffffffffffffff", "0f0e0d0c0b0a0908", "deadbeefcafebabe"]
    for seed_hex in seeds:
        seed_u64 = D.seed_hex_to_u64(seed_hex)
        import tempfile

        with tempfile.TemporaryDirectory(prefix="wxeo-vec-") as tmp:
            info = D.write_gt_file(seed_u64, n_events, os.path.join(tmp, "gt.ndjson"))
            lines = []
            with open(info["gt_path"], "r", encoding="utf-8") as fh:
                for i, line in enumerate(fh):
                    if i < 2 or i >= info["n_lines"] - 2:
                        lines.append(line.rstrip("\n"))
            first_event = D.derive_event(seed_u64, 0, partitions)
            partition_of_seq = [D.derive_event(seed_u64, s, partitions)["partition"]
                                for s in range(min(n_events, 32))]
            vectors.append({
                "seed_hex": seed_hex,
                "seed_u64_canonical_decimal": D.canonical_u64_decimal(seed_u64),
                "events": n_events,
                "partitions": partitions,
                "gt_lines": info["n_lines"],
                "gt_sha256": info["digest"],
                "sample_lines": lines,
                "first_event": {
                    "event_id": first_event["event_id"],
                    "account_d": first_event["account_d"],
                    "account_c": first_event["account_c"],
                    "amount_minor": first_event["amount_minor"],
                    "partition": first_event["partition"],
                },
                "partitions_of_first_32_seqs": partition_of_seq,
            })
    return {
        "schema": "wxeo.g5-vectors.v1",
        "generated_by": "experiments/oracle/derive.py (independent Python implementation)",
        "normative": "docs/method.md §1.4.1 + docs/CONTRACTS.md §4 + docs/P2-RULINGS.md R1",
        "note": "Both implementations must reproduce gt_sha256 and sample_lines exactly; "
                "mismatch = specification divergence, invalidate runs (gate G5).",
        "n_events": n_events,
        "partitions": partitions,
        "vectors": vectors,
    }


__all__ = [
    "RUN_SPEC",
    "DEFECT_CLASSES",
    "EXPECTED_ATTRIBUTION",
    "write_clean_run",
    "load_clean_rows",
    "write_sink_rows",
    "mutate_rows",
    "make_g5_vectors",
]
