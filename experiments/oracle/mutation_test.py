#!/usr/bin/env python3
"""G3 checker mutation testing — 100 % detection required (docs/method.md §7.6, prereg §8.1).

Extended after the S5 review (F-O3/O4/O5/O6) to cover the paper's own signatures, not just the
five canonical classes:

* **DB-sink corpus** — drop row, delete event, duplicate row, duplicate whole entry (the balanced
  M1x trace-X1 signature), flip leg sign, update row value, insert spurious row, insert
  *balanced* fabricated entry, currency mix, negative amounts;
* **attestation corpus** — app role granted UPDATE (I2(a) failure) and missing attestation;
* **publication-stream corpus** (DB mechanisms, secondary metrics per method §2.7) — lost
  publication record (the M1x abort signature: detection requires ``lost_records>=1`` and the
  signed ``divergence<0``), corrupted publication content (``pub_mutated_records>=1``);
* **M1-kafka ledger corpus** — the publication stream *is* the ledger: duplicate record, dropped
  record, mutated leg, balanced fabricated record, evaluated through the
  ``_expand_publication_to_postings`` path.

Every class is seeded, one defect per mutated dump, with a clean control for every corpus (zero
false positives).  The output goes to ``results/harness/g3-mutation.json``.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import tempfile
from datetime import datetime, timezone

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from experiments import pathsafe  # noqa: E402
from experiments.fixtures import make_fixtures as F  # noqa: E402
from experiments.oracle import derive as D  # noqa: E402
from experiments.oracle import oracle as O  # noqa: E402

SCHEMA = "wxeo.g3-mutation.v2"
MUTATION_BASE_SEED = 20261124


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _mutation_seed(group_index: int, class_index: int, mutation_index: int) -> int:
    return (MUTATION_BASE_SEED * 1_000_003 + group_index * 10_000_019
            + class_index * 100_003 + mutation_index * 7_919) & 0xFFFFFFFF


def _class_result(*, mutations: int, detected: int, attributed: int,
                  expected_primary: str, allowed: set, attribution_sets: dict,
                  failures: list, extra: dict | None = None) -> dict:
    out = {
        "mutations": mutations,
        "detected": detected,
        "detected_pct": 100.0 * detected / mutations if mutations else 0.0,
        "correctly_attributed": attributed,
        "attributed_pct": 100.0 * attributed / mutations if mutations else 0.0,
        "expected_primary_attribution": expected_primary,
        "allowed_co_attribution": sorted(allowed),
        "attribution_histogram": dict(sorted(attribution_sets.items())),
        "failures": failures[:8],
        "passed": mutations > 0 and detected == mutations and attributed == mutations,
    }
    if extra:
        out.update(extra)
    return out


def run_mutation_test(*, mutations_per_class: int, n_events: int, partitions: int,
                      seed_hex: str, workdir: str | None = None) -> dict:
    cleanup = False
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix="wxeo-g3-")
        cleanup = True
    os.makedirs(workdir, exist_ok=True)
    try:
        fixture_dir = os.path.join(workdir, "fixture")
        descriptor = F.write_clean_run(
            fixture_dir, run_id="g3-fixture", mechanism="M2", sink_kind="db",
            seed_hex=seed_hex, n_events=n_events, partitions=partitions,
            label="synthetic-G3")
        paths = {k: os.path.join(fixture_dir, v) for k, v in descriptor["paths"].items()}
        seed_u64 = D.seed_hex_to_u64(seed_hex)
        ctx = O.OracleContext(master_seed_u64=seed_u64, n_events=n_events,
                              partitions=partitions, shards=8,
                              workdir=os.path.join(workdir, "oracle"))
        source_detail = ctx.verify_source(
            gt_path=paths["gt"], source_dump_path=paths["source_dump"],
            produced_manifest_path=paths["produced"])
        clean_rows = F.load_clean_rows(paths["sink_dump"])
        expected_event_ids = {row[0] for row in clean_rows}
        clean_attestation = json.load(open(paths["attestation"], encoding="utf-8"))
        clean_publication = F.load_publication_records(paths["publication_dump"])
        publication_path = paths["publication_dump"]

        # ---- clean controls (false-positive guards) -------------------------------------
        db_clean = ctx.evaluate_sink(paths["sink_dump"], sink_kind="db",
                                     publication_dump_path=publication_path,
                                     attestation=clean_attestation,
                                     run_id="g3-clean-db", mechanism="M2")
        clean_ledger_path = os.path.join(workdir, "kafka_clean_ledger.ndjson")
        F.write_publication_records(clean_ledger_path, clean_publication)
        kafka_clean = ctx.evaluate_sink(None, sink_kind="kafka",
                                        publication_dump_path=clean_ledger_path,
                                        attestation=None, run_id="g3-clean-kafka",
                                        mechanism="M1-kafka")
        controls = {
            "db_sink_clean": {"violating_run": db_clean["violations"]["violating_run"],
                              "attribution": db_clean["attribution"],
                              "counts": db_clean["counts"],
                              "passed": (not db_clean["violations"]["violating_run"]
                                         and db_clean["attribution"] == [])},
            "kafka_ledger_clean": {"violating_run": kafka_clean["violations"]
                                   ["violating_run"],
                                   "attribution": kafka_clean["attribution"],
                                   "counts": kafka_clean["counts"],
                                   "passed": (not kafka_clean["violations"]["violating_run"]
                                              and kafka_clean["attribution"] == [])},
            "attestation_valid_clean": {"attribution": db_clean["attribution"]},
        }

        groups: dict = {}
        mutated_dir = os.path.join(workdir, "mutated")
        os.makedirs(mutated_dir, exist_ok=True)

        # ---- group 1: DB-sink posting defects ------------------------------------------
        print(f"[G3] DB-sink corpus: {len(F.DEFECT_CLASSES)} classes × "
              f"{mutations_per_class} mutations", flush=True)
        db_classes = {}
        for ci, defect_class in enumerate(F.DEFECT_CLASSES):
            spec = F.EXPECTED_ATTRIBUTION[defect_class]
            primary, allowed = spec["primary"], set(spec["allowed_extra"])
            detected = attributed = 0
            attribution_sets: dict[str, int] = {}
            failures: list[dict] = []
            for i in range(mutations_per_class):
                rng = random.Random(_mutation_seed(0, ci, i))
                rows, minfo = F.mutate_rows(clean_rows, defect_class, rng,
                                            expected_event_ids)
                sink_path = os.path.join(mutated_dir, f"{defect_class}-{i:04d}.csv")
                F.write_sink_rows(sink_path, rows)
                result = ctx.evaluate_sink(sink_path, sink_kind="db",
                                           publication_dump_path=publication_path,
                                           attestation=clean_attestation,
                                           run_id=f"g3-{defect_class}-{i:04d}",
                                           mechanism="M2")
                attribution = set(result["attribution"])
                key = "+".join(sorted(attribution)) if attribution else "(none)"
                attribution_sets[key] = attribution_sets.get(key, 0) + 1
                is_detected = bool(result["violations"]["violating_run"])
                ok = (primary in attribution) and ((attribution - {primary}) <= allowed)
                detected += int(is_detected)
                attributed += int(is_detected and ok)
                if not (is_detected and ok):
                    failures.append({
                        "mutation_index": i, "detected": is_detected,
                        "attribution": sorted(attribution), "expected_primary": primary,
                        "counts": result["counts"], "mutation": minfo})
            db_classes[defect_class] = _class_result(
                mutations=mutations_per_class, detected=detected, attributed=attributed,
                expected_primary=primary, allowed=allowed,
                attribution_sets=attribution_sets, failures=failures)
        groups["db_sink_mutations"] = {"classes": db_classes}

        # ---- group 2: attestation defects ----------------------------------------------
        print("[G3] attestation corpus", flush=True)
        att_classes = {}
        att_variants = {
            "attestation_granted": {"can_update": True, "can_delete": False,
                                    "posting_triggers": 1, "posting_rows": len(clean_rows)},
            "attestation_missing": None,
        }
        for ci, defect_class in enumerate(F.ATTESTATION_CLASSES):
            spec = F.ATTESTATION_EXPECTED[defect_class]
            primary, allowed = spec["primary"], set(spec["allowed_extra"])
            detected = attributed = 0
            attribution_sets: dict[str, int] = {}
            failures = []
            for i in range(mutations_per_class):
                att = att_variants[defect_class]
                result = ctx.evaluate_sink(paths["sink_dump"], sink_kind="db",
                                           publication_dump_path=publication_path,
                                           attestation=att,
                                           run_id=f"g3-{defect_class}-{i:04d}",
                                           mechanism="M2")
                attribution = set(result["attribution"])
                key = "+".join(sorted(attribution)) if attribution else "(none)"
                attribution_sets[key] = attribution_sets.get(key, 0) + 1
                is_detected = bool(result["violations"]["violating_run"])
                ok = (primary in attribution) and ((attribution - {primary}) <= allowed)
                detected += int(is_detected)
                attributed += int(is_detected and ok)
                if not (is_detected and ok):
                    failures.append({"mutation_index": i, "detected": is_detected,
                                     "attribution": sorted(attribution),
                                     "expected_primary": primary})
            att_classes[defect_class] = _class_result(
                mutations=mutations_per_class, detected=detected, attributed=attributed,
                expected_primary=primary, allowed=allowed,
                attribution_sets=attribution_sets, failures=failures)
        groups["attestation_mutations"] = {"classes": att_classes}

        # ---- group 3: publication-stream secondary metrics (DB mechanisms) -------------
        print("[G3] publication-metrics corpus", flush=True)
        pub_classes = {}
        for ci, defect_class in enumerate(F.PUBLICATION_CLASSES):
            detected = attributed = 0
            failure_samples = []
            for i in range(mutations_per_class):
                rng = random.Random(_mutation_seed(1, ci, i))
                records = [json.loads(json.dumps(r)) for r in clean_publication]
                if defect_class == "pub_record_lost":
                    if not records:
                        raise RuntimeError("empty publication stream")
                    del records[rng.randrange(len(records))]
                else:  # pub_content_corrupt
                    rec = records[rng.randrange(len(records))]
                    value = rec.get("value")
                    entry = json.loads(value) if isinstance(value, str) else value
                    legs = entry.get("legs") or []
                    leg = legs[rng.randrange(len(legs))]
                    if rng.random() < 0.5:
                        leg["account_id"] = "acct:" + "%012x" % rng.getrandbits(48)
                    else:
                        leg["amount_minor"] = int(leg.get("amount_minor") or 1) + 7
                    rec["value"] = json.dumps(entry, separators=(",", ":"))
                pub_path = os.path.join(mutated_dir, f"{defect_class}-{i:04d}.ndjson")
                F.write_publication_records(pub_path, records)
                result = ctx.evaluate_sink(paths["sink_dump"], sink_kind="db",
                                           publication_dump_path=pub_path,
                                           attestation=clean_attestation,
                                           run_id=f"g3-{defect_class}-{i:04d}",
                                           mechanism="M2")
                pub = result["publication"]
                if defect_class == "pub_record_lost":
                    ok = ((pub.get("lost_records") or 0) >= 1
                          and isinstance(pub.get("divergence"), int)
                          and pub["divergence"] < 0)
                else:
                    ok = (pub.get("pub_mutated_records") or 0) >= 1
                detected += int(bool(ok))
                attributed += int(bool(ok))
                if not ok:
                    failure_samples.append({"mutation_index": i, "publication": pub})
            pub_classes[defect_class] = _class_result(
                mutations=mutations_per_class, detected=detected, attributed=attributed,
                expected_primary=F.PUBLICATION_EXPECTED[defect_class],
                allowed=set(), attribution_sets={"metric_detected": detected},
                failures=failure_samples)
        groups["publication_mutations"] = {"classes": pub_classes}

        # ---- group 4: M1-kafka ledger corpus -------------------------------------------
        print("[G3] M1-kafka ledger corpus", flush=True)
        kafka_classes = {}
        for ci, defect_class in enumerate(F.KAFKA_CLASSES):
            spec = F.KAFKA_EXPECTED[defect_class]
            primary, allowed = spec["primary"], set(spec["allowed_extra"])
            detected = attributed = 0
            attribution_sets: dict[str, int] = {}
            failures = []
            for i in range(mutations_per_class):
                rng = random.Random(_mutation_seed(2, ci, i))
                records, minfo = F.mutate_publication_records(
                    clean_publication, defect_class, rng, expected_event_ids)
                ledger_path = os.path.join(mutated_dir, f"kafka-{defect_class}-{i:04d}.ndjson")
                F.write_publication_records(ledger_path, records)
                result = ctx.evaluate_sink(None, sink_kind="kafka",
                                           publication_dump_path=ledger_path,
                                           attestation=None,
                                           run_id=f"g3-kafka-{defect_class}-{i:04d}",
                                           mechanism="M1-kafka")
                attribution = set(result["attribution"])
                key = "+".join(sorted(attribution)) if attribution else "(none)"
                attribution_sets[key] = attribution_sets.get(key, 0) + 1
                is_detected = bool(result["violations"]["violating_run"])
                ok = (primary in attribution) and ((attribution - {primary}) <= allowed)
                detected += int(is_detected)
                attributed += int(is_detected and ok)
                if not (is_detected and ok):
                    failures.append({"mutation_index": i, "detected": is_detected,
                                     "attribution": sorted(attribution),
                                     "expected_primary": primary,
                                     "counts": result["counts"], "mutation": minfo})
            kafka_classes[defect_class] = _class_result(
                mutations=mutations_per_class, detected=detected, attributed=attributed,
                expected_primary=primary, allowed=allowed,
                attribution_sets=attribution_sets, failures=failures)
        groups["kafka_ledger_mutations"] = {
            "classes": kafka_classes,
            "note": "the publication stream is the ledger for M1-kafka (method §2.7); mutations "
                    "exercise the _expand_publication_to_postings path",
        }

        # ---- input-integrity controls --------------------------------------------------
        input_controls = _input_integrity_controls(ctx, paths, workdir)

        all_classes = {}
        for group in ("db_sink_mutations", "attestation_mutations", "kafka_ledger_mutations",
                      "publication_mutations"):
            all_classes.update(groups[group]["classes"])
        mutations_total = sum(c["mutations"] for c in all_classes.values())
        detected_total = sum(c["detected"] for c in all_classes.values())
        attributed_total = sum(c["correctly_attributed"] for c in all_classes.values())
        report = {
            "schema": SCHEMA,
            "gate": "G3",
            "generated_at_utc": _utc_now(),
            "requirement": "100% detection with correct invariant attribution for every defect "
                           "class; >=100 seeded mutations per class (docs/method.md §7.6)",
            "synthetic": True,
            "note": "SYNTHETIC mutation corpus on synthetic fixtures - harness validation, not a "
                    "mechanism measurement",
            "mutation_base_seed": MUTATION_BASE_SEED,
            "mutations_per_class": mutations_per_class,
            "corpora": {
                "db_sink": list(F.DEFECT_CLASSES),
                "attestation": list(F.ATTESTATION_CLASSES),
                "publication_metrics": list(F.PUBLICATION_CLASSES),
                "kafka_ledger": list(F.KAFKA_CLASSES),
            },
            "fixture": {
                "run_id": descriptor["run_id"], "mechanism": descriptor["mechanism"],
                "seed_hex": descriptor["seed_hex"], "events": n_events,
                "partitions": partitions, "expected_postings": descriptor["expected_postings"],
                "gt_sha256": descriptor["gt_sha256"],
                "source_integrity": source_detail["source_integrity"],
            },
            "controls": controls,
            "groups": groups,
            "input_integrity_controls": input_controls,
            "overview": {
                "classes": len(all_classes),
                "mutations": mutations_total,
                "detected": detected_total,
                "correctly_attributed": attributed_total,
                "detected_pct": 100.0 * detected_total / max(1, mutations_total),
                "attributed_pct": 100.0 * attributed_total / max(1, mutations_total),
                "all_classes_passed": all(c["passed"] for c in all_classes.values()),
                "clean_controls_passed": all(c["passed"] for c in controls.values()
                                             if "passed" in c),
                "input_controls_passed": input_controls["passed"],
            },
            "oracle": {
                "oracle_script_sha256": O.oracle_script_sha256(),
                "derive_sha256": D.sha256_file(os.path.join(
                    os.path.dirname(os.path.abspath(O.__file__)), "derive.py")),
            },
        }
        report["passed"] = bool(
            report["overview"]["all_classes_passed"]
            and report["overview"]["clean_controls_passed"]
            and input_controls["passed"])
        return pathsafe.scrub(report)
    finally:
        if cleanup:
            import shutil

            shutil.rmtree(workdir, ignore_errors=True)


def _input_integrity_controls(ctx: O.OracleContext, paths: dict, workdir: str) -> dict:
    controls_dir = os.path.join(workdir, "tamper")
    os.makedirs(controls_dir, exist_ok=True)
    results = {}

    def _verify(gt=paths["gt"], source=paths["source_dump"], produced=paths["produced"]):
        return ctx.verify_source(gt_path=gt, source_dump_path=source,
                                 produced_manifest_path=produced)

    results["clean_baseline"] = {"source_integrity": _verify()["source_integrity"]}
    gt_path = os.path.join(controls_dir, "gt_tampered.ndjson")
    text = open(paths["gt"], encoding="utf-8").read()
    tampered = text.replace('"line_no":0', '"line_no":9', 1)
    if tampered == text:
        tampered = text[:-2] + ("0" if text[-2] != "0" else "1") + "\n"
    with open(gt_path, "w", encoding="utf-8", newline="") as fh:
        fh.write(tampered)
    d = _verify(gt=gt_path)
    results["gt_tampered"] = {"gt_digest_match": d["gt_digest_match"],
                              "source_integrity": d["source_integrity"]}
    src_lines = [ln for ln in open(paths["source_dump"], encoding="utf-8") if ln.strip()]
    gap_path = os.path.join(controls_dir, "source_gap.ndjson")
    half = max(1, len(src_lines) // 2)
    with open(gap_path, "w", encoding="utf-8", newline="") as fh:
        fh.writelines(src_lines[:half] + src_lines[half + 1:])
    d = _verify(source=gap_path)
    results["source_missing_record"] = {
        "offsets_contiguous": d["source_offsets_contiguous"],
        "multiset_matches_manifest": d["source_event_multiset_matches_manifest"],
        "source_integrity": d["source_integrity"]}
    man_lines = [ln for ln in open(paths["produced"], encoding="utf-8") if ln.strip()]
    man_path = os.path.join(controls_dir, "produced_dup.ndjson")
    with open(man_path, "w", encoding="utf-8", newline="") as fh:
        fh.writelines(man_lines + [man_lines[0]])
    d = _verify(produced=man_path)
    results["manifest_duplicated_record"] = {
        "multiset_matches_manifest": d["source_event_multiset_matches_manifest"],
        "source_integrity": d["source_integrity"]}
    _verify()
    passed = (results["clean_baseline"]["source_integrity"] is True
              and results["gt_tampered"]["source_integrity"] is False
              and results["source_missing_record"]["source_integrity"] is False
              and results["manifest_duplicated_record"]["source_integrity"] is False)
    return {"controls": results, "passed": bool(passed)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="WXEO G3 oracle mutation testing")
    parser.add_argument("--mutations", type=int, default=120)
    parser.add_argument("--events", type=int, default=200)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--seed-hex", default="0f0e0d0c0b0a0908")
    parser.add_argument("--out", default=os.path.join(_REPO, "results", "harness",
                                                      "g3-mutation.json"))
    parser.add_argument("--workdir", default=None)
    args = parser.parse_args(argv)
    if args.mutations < 100:
        print("REFUSING: G3 requires >=100 mutations per defect class", file=sys.stderr)
        return 2
    report = run_mutation_test(mutations_per_class=args.mutations, n_events=args.events,
                               partitions=args.partitions, seed_hex=args.seed_hex,
                               workdir=args.workdir)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    pathsafe.assert_clean(report, "g3-mutation report")
    with open(args.out, "w", encoding="utf-8", newline="") as fh:
        json.dump(report, fh, indent=2)
        fh.write("\n")
    print(json.dumps({
        "out": pathsafe.repo_rel(args.out),
        "passed": report["passed"],
        "mutations_per_class": report["mutations_per_class"],
        "overview": report["overview"],
        "per_group": {g: {k: ({"detected_pct": v["detected_pct"],
                              "attributed_pct": v["attributed_pct"]})
                          for k, v in group["classes"].items()}
                      for g, group in report["groups"].items()},
    }, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
