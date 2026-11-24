"""Oracle unit tests: derivation vectors, canonical digest, predicates, mutation classes.

Run with::

    python -m pytest experiments/tests -q

The hard-coded vectors come from frozen, committed files:

* ``docs/derive-vectors.csv``  — lead-ruled normative derivation vectors (R5): each row must be
  reproduced from the specification text by this independent implementation (gate G5).
* ``experiments/oracle/g5-vectors.json`` — Python-generated Java↔Python digest contract vectors;
  S3's Java load generator must reproduce the same ``gt_sha256`` byte-for-byte.
"""

from __future__ import annotations

import csv
import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments.fixtures import make_fixtures as F  # noqa: E402
from experiments.oracle import derive as D  # noqa: E402
from experiments.oracle import oracle as O  # noqa: E402


# --------------------------------------------------------------------------------------------
# G5: normative derivation vectors
# --------------------------------------------------------------------------------------------
def test_derive_vectors_csv_reproduced():
    """Reproduce every row of the lead-ruled normative vector file (R5 / G5)."""
    path = os.path.join(REPO, "docs", "derive-vectors.csv")
    assert os.path.exists(path), "normative vector file missing"
    with open(path, "r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) >= 6
    for row in rows:
        seed_u64 = D.seed_hex_to_u64(row["seed_hex"])
        seq = int(row["seq"])
        ev6 = D.derive_event(seed_u64, seq, 6)
        ev3 = D.derive_event(seed_u64, seq, 3)
        assert ev6["event_id"] == row["event_id"], row
        assert ev6["account_d"] == row["account_d"], row
        assert ev6["account_c"] == row["account_c"], row
        assert ev6["amount_minor"] == int(row["amount_minor"]), row
        assert ev6["partition"] == int(row["partition_p6"]), row
        assert ev3["partition"] == int(row["partition_p3"]), row


def test_g5_digest_vectors():
    """The committed Java↔Python digest vectors must be reproduced byte-for-byte."""
    path = os.path.join(REPO, "experiments", "oracle", "g5-vectors.json")
    with open(path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    import tempfile

    for vec in doc["vectors"]:
        with tempfile.TemporaryDirectory() as tmp:
            info = D.write_gt_file(D.seed_hex_to_u64(vec["seed_hex"]), vec["events"],
                                   os.path.join(tmp, "gt.ndjson"))
            assert info["n_lines"] == vec["gt_lines"]
            assert info["digest"] == vec["gt_sha256"], vec["seed_hex"]
            with open(info["gt_path"], "r", encoding="utf-8") as fh:
                lines = [line.rstrip("\n") for line in fh]
            assert lines[:2] + lines[-2:] == vec["sample_lines"]
        ev = D.derive_event(D.seed_hex_to_u64(vec["seed_hex"]), 0, vec["partitions"])
        assert ev["event_id"] == vec["first_event"]["event_id"]
        assert ev["account_d"] == vec["first_event"]["account_d"]
        assert ev["account_c"] == vec["first_event"]["account_c"]
        assert ev["amount_minor"] == vec["first_event"]["amount_minor"]
        assert ev["partition"] == vec["first_event"]["partition"]


def test_line_numbering_is_zero_based_and_sorted():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        info = D.write_gt_file(42, 25, os.path.join(tmp, "gt.ndjson"))
        rows = [json.loads(line) for line in open(info["gt_path"], encoding="utf-8")]
        assert info["digest"] == D.sha256_file(info["gt_path"])
    assert len(rows) == 50
    assert [r["line_no"] for r in rows] == list(range(50))
    keys = [(r["event_id"], r["leg_no"]) for r in rows]
    assert keys == sorted(keys)


def test_canonical_event_and_entry_values():
    ev = D.derive_event(7, 3, 6)
    val = D.canonical_event_value(ev, 1_763_995_200_123)
    assert " " not in val
    obj = json.loads(val)
    assert list(obj.keys()) == ["spec", "event_id", "seq", "currency", "legs", "scheduled_at_ms"]
    assert obj["legs"][0]["side"] == "D" and obj["legs"][1]["side"] == "C"
    ent = json.loads(D.canonical_entry_value(ev))
    assert list(ent.keys()) == ["spec", "event_id", "seq", "legs"]


# --------------------------------------------------------------------------------------------
# predicates on a clean synthetic fixture
# --------------------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def clean_fixture(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("clean")
    desc = F.write_clean_run(str(tmp), run_id="unit-clean", mechanism="M2", seed_hex="0f0e0d0c0b0a0908",
                             n_events=60, partitions=3)
    paths = {k: os.path.join(str(tmp), v) for k, v in desc["paths"].items()}
    return desc, paths


def _evaluate(paths, tmp_path):
    ctx = O.OracleContext(master_seed_u64=D.seed_hex_to_u64("0f0e0d0c0b0a0908"), n_events=60,
                          partitions=3, shards=8, workdir=str(tmp_path))
    ctx.verify_source(gt_path=paths["gt"], source_dump_path=paths["source_dump"],
                      produced_manifest_path=paths["produced"])
    att = json.load(open(paths["attestation"], encoding="utf-8"))
    return ctx, ctx.evaluate_sink(paths["sink_dump"], sink_kind="db",
                                  publication_dump_path=paths["publication_dump"],
                                  attestation=att, run_id="unit", mechanism="M2")


def test_clean_fixture_is_consistent(clean_fixture, tmp_path):
    desc, paths = clean_fixture
    ctx, result = _evaluate(paths, tmp_path)
    assert result["violations"] == {"v1": False, "v2": False, "v3": False, "v4": False,
                                    "v5": False, "violating_run": False}
    assert result["attribution"] == []
    assert result["counts"]["dup_postings"] == 0
    assert result["counts"]["lost_postings"] == 0
    detail = ctx.source_detail
    assert detail["source_integrity"] is True
    assert detail["gt_digest_match"] is True
    assert detail["source_dump_digest"] == detail["gt_sha256_file"]
    assert detail["value_sha256_mismatches"] == 0
    assert detail["partition_derivation_mismatches"] == 0


@pytest.mark.parametrize("defect_class,primary,allowed", [
    ("drop_row", "I4", {"I1", "I1a", "I1b"}),
    ("delete_event", "I4", set()),
    ("duplicate_row", "I3", {"I1", "I1a", "I1b"}),
    ("flip_leg_sign", "I2", {"I1", "I1a", "I1b"}),
    ("update_row_value", "I2", {"I1", "I1a", "I1b"}),
    ("insert_spurious_row", "SPURIOUS", {"I1", "I1a", "I1b"}),
])
def test_single_defect_detected_and_attributed(clean_fixture, tmp_path, defect_class,
                                               primary, allowed):
    import random

    desc, paths = clean_fixture
    ctx, _ = _evaluate(paths, tmp_path)
    rows = F.load_clean_rows(paths["sink_dump"])
    eids = {r[0] for r in rows}
    mutated, _info = F.mutate_rows(rows, defect_class, random.Random(12345), eids)
    out = str(tmp_path / f"{defect_class}.csv")
    F.write_sink_rows(out, mutated)
    att = json.load(open(paths["attestation"], encoding="utf-8"))
    result = ctx.evaluate_sink(out, sink_kind="db",
                               publication_dump_path=paths["publication_dump"],
                               attestation=att, run_id="unit-defect", mechanism="M2")
    attribution = set(result["attribution"])
    assert result["violations"]["violating_run"] is True
    assert primary in attribution
    assert (attribution - {primary}) <= allowed


def test_attestation_failure_is_i2(clean_fixture, tmp_path):
    desc, paths = clean_fixture
    ctx, _ = _evaluate(paths, tmp_path)
    att = {"can_update": True, "can_delete": False, "posting_triggers": 1}
    result = ctx.evaluate_sink(paths["sink_dump"], sink_kind="db",
                               publication_dump_path=paths["publication_dump"],
                               attestation=att, run_id="unit-att", mechanism="M2")
    assert result["violations"]["v2"] is True
    assert result["violations"]["violating_run"] is True
    assert "ATTESTATION" in result["attribution"]


def test_tampered_gt_fails_source_integrity(clean_fixture, tmp_path):
    desc, paths = clean_fixture
    gt = open(paths["gt"], encoding="utf-8").read()
    tampered_path = str(tmp_path / "gt-tampered.ndjson")
    with open(tampered_path, "w", encoding="utf-8", newline="") as fh:
        fh.write(gt.replace('"line_no":0', '"line_no":7', 1))
    ctx = O.OracleContext(master_seed_u64=D.seed_hex_to_u64("0f0e0d0c0b0a0908"), n_events=60,
                          partitions=3, shards=8, workdir=str(tmp_path / "tamper"))
    detail = ctx.verify_source(gt_path=tampered_path, source_dump_path=paths["source_dump"],
                               produced_manifest_path=paths["produced"])
    assert detail["gt_digest_match"] is False
    assert detail["source_integrity"] is False


def test_result_sha256_is_self_consistent(clean_fixture, tmp_path):
    desc, paths = clean_fixture
    result = O.oracle_run(
        run_id="unit-sha", mechanism="M2", sink_kind="db",
        master_seed_u64=D.seed_hex_to_u64("0f0e0d0c0b0a0908"), n_events=60, partitions=3,
        gt_path=paths["gt"], source_dump_path=paths["source_dump"],
        produced_manifest_path=paths["produced"], sink_dump_path=paths["sink_dump"],
        publication_dump_path=paths["publication_dump"],
        attestation_path=paths["attestation"], out_path=str(tmp_path / "oracle.json"),
        shards=8, workdir=str(tmp_path / "run"),
    )
    expected = result["oracle"]["result_sha256"]
    result["oracle"]["result_sha256"] = None
    import hashlib

    payload = json.dumps(result, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert hashlib.sha256(payload).hexdigest() == expected
