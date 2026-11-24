"""Independent Python re-implementation of the deterministic event derivation.

Normative sources (frozen):
  * ``docs/method.md`` §1.4.1 — the derivation specification.
  * ``docs/CONTRACTS.md`` §3-§4 — domain prefixes ``wxeo1|event|`` / ``wxeo1|part|`` and
    the canonical posting-line / ground-truth-digest contract.

Byte-level disambiguations (agreed with S3 in the P2 interface exchange, recorded in
``results/DEVIATIONS.md`` — the frozen documents do not pin them):

  * ``line_no`` in the GT file is **0-based** (0 .. 2N-1); the canonical order is ascending
    ``(event_id, leg_no)``, event_id compared byte-wise (lower-case hex is ASCII), and
    ``line_no`` is assigned after sorting.
  * The load generator takes ``--master-seed=<u64>``; the string concatenated into the hash
    input is the **canonical decimal** of that u64 (no sign, no leading zeros).  The per-run
    value supplied by the runner is ``data_seed`` (prereg §4.4), a 64-bit value recorded in
    hex (prereg §5) and passed to the generator in decimal.  The oracle converts hex -> u64
    -> canonical decimal, so the reproduction check is independent of the CLI representation.
  * Canonical event-value / publication-entry-value JSON shapes are S3's spec (compact JSON,
    fixed key order), used here only for the *secondary* ``value_sha256`` spot-check.

This module shares NO code with the Java artifact (``artifact/**``): it is written from the
specification text only, so agreement between the Java load generator and this module is a real
cross-implementation check (gate G5).  Standard library only.

Derivation (verbatim from docs/method.md §1.4.1, with S3's seed representation)::

    h1 = SHA-256("wxeo1|event|" + canonical_decimal(master_seed_u64) + "|" + decimal(seq))
    event_id   = lowercase_hex(h1[0:16])            // 32 hex chars
    account_d  = "acct:" + hex(h1[16:22])          // 12 hex chars
    account_c  = "acct:" + hex(h1[22:28])
    amount     = 1 + (uint64_be(h1[28:36]) mod 100000)
    h2 = SHA-256("wxeo1|part|" + canonical_decimal(master_seed_u64) + "|" + decimal(seq))
    partition  = uint64_be(h2[0:8]) mod P
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Iterator

SPEC = "wxeo.ledger-event.v1"
ENTRY_SPEC = "wxeo.entry.v1"
CURRENCY = "BRL"
EVENT_DOMAIN = "wxeo1|event|"
PART_DOMAIN = "wxeo1|part|"
AMOUNT_MODULUS = 100_000
LINE_KEYS = ("line_no", "event_id", "leg_no", "account_id", "side", "amount_minor", "currency")

#: Number of eid-prefix buckets used for the memory-bounded canonical digest / evaluation.
DEFAULT_SHARDS = 64

_U64_MAX = (1 << 64) - 1


def _sha256_text(text: str) -> bytes:
    return hashlib.sha256(text.encode("utf-8")).digest()


def canonical_u64_decimal(value: int | str) -> str:
    """Return the canonical decimal ASCII representation of a u64.

    Accepts an ``int`` or a string (decimal digits or a hex string prefixed ``0x``; a bare
    16-character hex string is also accepted when it is not all-decimal, as produced by the
    prereg seed formula).  Raises on out-of-range / malformed input.
    """
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            raise ValueError("empty seed")
        if text.lower().startswith("0x"):
            n = int(text, 16)
        else:
            n = None
            try:
                n = int(text, 10)
            except ValueError:
                n = int(text, 16)  # hex data_seed as recorded by prereg §4.4
    else:
        n = int(value)
    if n < 0 or n > _U64_MAX:
        raise ValueError(f"seed {value!r} is not a u64")
    return str(n)


def seed_hex_to_u64(hex_seed: str) -> int:
    """Convert the recorded 16-hex-char per-run seed to its u64 value."""
    text = hex_seed.strip()
    if text.lower().startswith("0x"):
        text = text[2:]
    if not (1 <= len(text) <= 16):
        raise ValueError(f"seed hex {hex_seed!r} must be 1..16 hex chars")
    n = int(text, 16)
    if n > _U64_MAX:
        raise ValueError(f"seed hex {hex_seed!r} exceeds u64")
    return n


def derive_event(master_seed: int | str, seq: int, partitions: int | None = None) -> dict:
    """Derive one journal entry.  Pure function of ``(master_seed_u64, seq)``."""
    if seq < 0:
        raise ValueError("seq must be >= 0")
    seed = canonical_u64_decimal(master_seed)

    h1 = _sha256_text(EVENT_DOMAIN + seed + "|" + str(seq))
    h2 = _sha256_text(PART_DOMAIN + seed + "|" + str(seq))

    event_id = h1[0:16].hex()
    account_d = "acct:" + h1[16:22].hex()
    account_c = "acct:" + h1[22:28].hex()
    amount = 1 + (int.from_bytes(h1[28:36], "big") % AMOUNT_MODULUS)

    event = {
        "spec": SPEC,
        "event_id": event_id,
        "seq": seq,
        "currency": CURRENCY,
        "account_d": account_d,
        "account_c": account_c,
        "amount_minor": amount,
    }
    if partitions is not None:
        if partitions <= 0:
            raise ValueError("partitions must be > 0")
        event["partition"] = int.from_bytes(h2[0:8], "big") % partitions
    return event


def event_legs(event: dict) -> list[dict]:
    return [
        {"account_id": event["account_d"], "side": "D", "amount_minor": event["amount_minor"]},
        {"account_id": event["account_c"], "side": "C", "amount_minor": event["amount_minor"]},
    ]


def event_postings(event: dict) -> list[dict]:
    """Return the two intended postings of an event (posting dicts, no line_no)."""
    eid = event["event_id"]
    return [
        {
            "event_id": eid,
            "leg_no": 1,
            "account_id": event["account_d"],
            "side": "D",
            "amount_minor": event["amount_minor"],
            "currency": event["currency"],
        },
        {
            "event_id": eid,
            "leg_no": 2,
            "account_id": event["account_c"],
            "side": "C",
            "amount_minor": event["amount_minor"],
            "currency": event["currency"],
        },
    ]


def canonical_posting_line(
    line_no: int,
    event_id: str,
    leg_no: int,
    account_id: str,
    side: str,
    amount_minor: int,
    currency: str,
) -> str:
    """Canonical JSON posting line: compact, exact key order, no trailing space."""
    return json.dumps(
        {
            "line_no": line_no,
            "event_id": event_id,
            "leg_no": leg_no,
            "account_id": account_id,
            "side": side,
            "amount_minor": amount_minor,
            "currency": currency,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )


def canonical_event_value(event: dict, scheduled_at_ms: int) -> str:
    """Canonical source-record value (S3 spec §2); no trailing newline."""
    return json.dumps(
        {
            "spec": SPEC,
            "event_id": event["event_id"],
            "seq": event["seq"],
            "currency": event["currency"],
            "legs": event_legs(event),
            "scheduled_at_ms": int(scheduled_at_ms),
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )


def canonical_entry_value(event: dict) -> str:
    """Canonical publication-stream entry value (method §1.4.3, S3 spec §3); no newline."""
    return json.dumps(
        {
            "spec": ENTRY_SPEC,
            "event_id": event["event_id"],
            "seq": event["seq"],
            "legs": event_legs(event),
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )


def eid_bucket(event_id: str, shards: int = DEFAULT_SHARDS) -> int:
    """Stable eid-prefix bucket.  Prefix buckets preserve the global canonical eid order."""
    if shards < 1 or shards > 256:
        raise ValueError("shards must be in 1..256")
    return (int(event_id[0:2], 16) * shards) // 256


def iter_events(master_seed: int | str, n_events: int, partitions: int | None = None) -> Iterator[dict]:
    for seq in range(n_events):
        yield derive_event(master_seed, seq, partitions)


def write_expected_shards(
    master_seed: int | str,
    n_events: int,
    out_dir: str,
    shards: int = DEFAULT_SHARDS,
    partitions: int | None = None,
    write_event_index: bool = True,
) -> dict:
    """Regenerate the expected posting multiset and write canonical, shard-sorted files.

    The shards are eid-prefix ordered, so the concatenation ``bucket 0 .. K-1`` of the files
    (each sorted by ``(event_id, leg_no)``) is exactly the canonical ground-truth byte stream.
    The digest is computed by streaming the shards in bucket order; the full 2N-line file is
    never materialised in memory.

    Returns: ``digest`` (hex), ``n_lines``, ``n_events``, ``shard_paths``, ``line_bytes``,
    ``shards``, ``event_ids`` (only for small N; used by fixtures) and ``seq_of_event_id``.
    """
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    buckets: list[list[tuple[str, int, str, str, int, str]]] = [[] for _ in range(shards)]
    event_index: list[list[dict]] = [[] for _ in range(shards)]
    seq_of_eid: dict[str, int] = {}
    p = partitions
    for seq in range(n_events):
        event = derive_event(master_seed, seq, p)
        eid = event["event_id"]
        if eid in seq_of_eid:
            raise RuntimeError(f"event_id collision at seq {seq} (128-bit truncation)")
        seq_of_eid[eid] = seq
        b = eid_bucket(eid, shards)
        for leg_no, leg in enumerate(event_legs(event), start=1):
            buckets[b].append(
                (eid, leg_no, leg["account_id"], leg["side"], leg["amount_minor"], leg["currency"])
            )
        if write_event_index:
            index = {
                "event_id": eid,
                "seq": seq,
                "account_d": event["account_d"],
                "account_c": event["account_c"],
                "amount_minor": event["amount_minor"],
            }
            if p is not None:
                index["partition"] = event["partition"]
            event_index[b].append(index)

    paths: list[str] = []
    event_paths: list[str] = []
    hasher = hashlib.sha256()
    n_lines = 0
    line_bytes = 0
    for b in range(shards):
        rows = sorted(buckets[b], key=lambda r: (r[0], r[1]))
        path = os.path.join(out_dir, f"expected-{b:03d}.jsonl")
        with open(path, "wb") as fh:
            for eid, leg_no, account_id, side, amount, currency in rows:
                line = canonical_posting_line(
                    n_lines, eid, leg_no, account_id, side, amount, currency
                )
                raw = (line + "\n").encode("utf-8")
                fh.write(raw)
                hasher.update(raw)
                line_bytes += len(raw)
                n_lines += 1
        paths.append(path)
        buckets[b] = []
        if write_event_index:
            epath = os.path.join(out_dir, f"event-{b:03d}.jsonl")
            with open(epath, "w", encoding="utf-8", newline="") as fh:
                for index in event_index[b]:
                    fh.write(json.dumps(index, separators=(",", ":")) + "\n")
            event_paths.append(epath)
            event_index[b] = []
    info = {
        "digest": hasher.hexdigest(),
        "n_lines": n_lines,
        "n_events": n_events,
        "shard_paths": paths,
        "event_shard_paths": event_paths,
        "line_bytes": line_bytes,
        "shards": shards,
        "partitions": p,
        "seq_of_event_id": seq_of_eid,
    }
    return info


def write_gt_file(master_seed: int | str, n_events: int, gt_path: str, shards: int = DEFAULT_SHARDS) -> dict:
    """Write a ground-truth file exactly as specified (docs/CONTRACTS.md §4 + S3 §4).

    Used by fixtures and independent tests; the real GT file is written by S3's load generator.
    """
    import tempfile

    gt_path = os.path.abspath(gt_path)
    os.makedirs(os.path.dirname(gt_path), exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="wxeo-gt-") as tmp:
        info = write_expected_shards(master_seed, n_events, tmp, shards=shards)
        with open(gt_path, "wb") as out:
            for path in info["shard_paths"]:
                with open(path, "rb") as src:
                    for chunk in iter(lambda: src.read(1 << 20), b""):
                        out.write(chunk)
    digest = sha256_file(gt_path)
    info.pop("seq_of_event_id", None)
    info["gt_path"] = gt_path
    info["gt_sha256"] = digest
    info["digest"] = digest
    return info


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


__all__ = [
    "SPEC",
    "ENTRY_SPEC",
    "CURRENCY",
    "EVENT_DOMAIN",
    "PART_DOMAIN",
    "AMOUNT_MODULUS",
    "DEFAULT_SHARDS",
    "canonical_u64_decimal",
    "seed_hex_to_u64",
    "derive_event",
    "event_legs",
    "event_postings",
    "canonical_posting_line",
    "canonical_event_value",
    "canonical_entry_value",
    "eid_bucket",
    "iter_events",
    "write_expected_shards",
    "write_gt_file",
    "sha256_file",
    "sha256_bytes",
]
