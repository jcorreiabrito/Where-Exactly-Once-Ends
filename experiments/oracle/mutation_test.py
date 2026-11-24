#!/usr/bin/env python3
"""G3 checker mutation testing — 100 % detection required (docs/method.md §7.6, prereg §8.1).

For every defect class required by the harness-validation gate —

    drop a row, duplicate a row, flip a leg sign, update a row value, insert a spurious row,
    delete a whole event (superset of the five pre-registered classes)

— this harness injects ``--mutations`` seeded synthetic defects into a clean synthetic oracle
fixture, one defect per mutated sink dump, and asserts that the oracle:

  (i)  flags the run as violating (``violations.violating_run == True``), and
  (ii) attributes the defect to the correct invariant class
       (``I4`` loss, ``I3`` duplicate, ``I2`` mutation, ``SPURIOUS`` fabricated posting; a
       co-occurring structural ``I1`` is allowed because a partial entry is also malformed).

A clean control must produce **zero** violations and no attribution (false-positive guard).
Input-integrity controls additionally prove that GT/source tampering is caught by the
source-integrity gate (gate G5's detection side).

The mutation corpus is fully seeded and re-runnable; the result is written to
``results/harness/g3-mutation.json``.  Everything here is synthetic and is never presented as a
measurement of any mechanism.
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

from experiments.fixtures import make_fixtures as F  # noqa: E402
from experiments.oracle import derive as D  # noqa: E402
from experiments.oracle import oracle as O  # noqa: E402

SCHEMA = "wxeo.g3-mutation.v1"
#: base seed for the mutation corpus (recorded; changing it is a new corpus, not a fix)
MUTATION_BASE_SEED = 20261124


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _mutation_seed(class_index: int, mutation_index: int) -> int:
    return (MUTATION_BASE_SEED * 1_000_003 + class_index * 100_003 + mutation_index * 7_919) & 0xFFFFFFFF


def run_mutation_test(
    *,
    mutations_per_class: int,
    n_events: int,
    partitions: int,
    seed_hex: str,
    workdir: str | None = None,
) -> dict:
    cleanup = False
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix="wxeo-g3-")
        cleanup = True
    os.makedirs(workdir, exist_ok=True)
    try:
        fixture_dir = os.path.join(workdir, "fixture")
        descriptor = F.write_clean_run(
            fixture_dir,
            run_id="g3-fixture",
            mechanism="M2",
            sink_kind="db",
            seed_hex=seed_hex,
            n_events=n_events,
            partitions=partitions,
            label="synthetic-G3",
        )
        paths = {k: os.path.join(fixture_dir, v) for k, v in descriptor["paths"].items()}
        seed_u64 = D.seed_hex_to_u64(seed_hex)

        ctx = O.OracleContext(
            master_seed_u64=seed_u64, n_events=n_events, partitions=partitions,
            shards=16, workdir=os.path.join(workdir, "oracle"),
        )
        source_detail = ctx.verify_source(
            gt_path=paths["gt"], source_dump_path=paths["source_dump"],
            produced_manifest_path=paths["produced"],
        )

        clean_rows = F.load_clean_rows(paths["sink_dump"])
        expected_event_ids = {row[0] for row in clean_rows}

        # ---- clean control: zero false positives ---------------------------------------------
        clean_result = ctx.evaluate_sink(
            paths["sink_dump"], sink_kind="db",
            publication_dump_path=paths["publication_dump"],
            attestation=json.load(open(paths["attestation"], encoding="utf-8")),
            run_id="g3-clean-control", mechanism="M2",
        )
        clean_control = {
            "violating_run": clean_result["violations"]["violating_run"],
            "attribution": clean_result["attribution"],
            "counts": clean_result["counts"],
            "passed": (not clean_result["violations"]["violating_run"]
                       and clean_result["attribution"] == []),
        }

        # ---- seeded mutation corpus, one defect per mutated dump ------------------------------
        mutated_dir = os.path.join(workdir, "mutated")
        os.makedirs(mutated_dir, exist_ok=True)
        classes_report: dict = {}
        overview = {"mutations": 0, "detected": 0, "attributed": 0, "failures": 0}
        for class_index, defect_class in enumerate(F.DEFECT_CLASSES):
            expected = F.EXPECTED_ATTRIBUTION[defect_class]
            primary = expected["primary"]
            allowed_extra = set(expected["allowed_extra"])
            failures = []
            attribution_sets: dict[str, int] = {}
            detected = 0
            attributed = 0
            sanity_counts: dict[str, list] = {}
            for i in range(mutations_per_class):
                rng = random.Random(_mutation_seed(class_index, i))
                rows, minfo = F.mutate_rows(clean_rows, defect_class, rng, expected_event_ids)
                sink_path = os.path.join(mutated_dir, f"{defect_class}-{i:04d}.csv")
                F.write_sink_rows(sink_path, rows)
                result = ctx.evaluate_sink(
                    sink_path, sink_kind="db",
                    publication_dump_path=paths["publication_dump"],
                    attestation=json.load(open(paths["attestation"], encoding="utf-8")),
                    run_id=f"g3-{defect_class}-{i:04d}", mechanism="M2",
                )
                attribution = set(result["attribution"])
                key = "+".join(sorted(attribution)) if attribution else "(none)"
                attribution_sets[key] = attribution_sets.get(key, 0) + 1
                is_detected = bool(result["violations"]["violating_run"])
                ok_primary = primary in attribution
                ok_noise = (attribution - {primary}) <= allowed_extra
                is_attributed = ok_primary and ok_noise
                detected += int(is_detected)
                attributed += int(is_attributed and is_detected)
                counts = result["counts"]
                for metric in ("dup_postings", "lost_postings", "mutated_postings",
                               "spurious_postings", "imbalanced_entries"):
                    sanity_counts.setdefault(metric, []).append(counts[metric])
                if not (is_detected and is_attributed):
                    failures.append({
                        "mutation_index": i,
                        "seed": _mutation_seed(class_index, i),
                        "detected": is_detected,
                        "attribution": sorted(attribution),
                        "expected_primary": primary,
                        "counts": counts,
                        "mutation": minfo,
                    })
            classes_report[defect_class] = {
                "mutations": mutations_per_class,
                "detected": detected,
                "detected_pct": 100.0 * detected / mutations_per_class,
                "correctly_attributed": attributed,
                "attributed_pct": 100.0 * attributed / mutations_per_class,
                "expected_primary_attribution": primary,
                "allowed_co_attribution": sorted(allowed_extra),
                "attribution_histogram": dict(sorted(attribution_sets.items())),
                "metric_ranges": {
                    metric: {"min": min(vals), "max": max(vals)} for metric, vals in sanity_counts.items()
                },
                "failures": failures[:10],
                "passed": detected == mutations_per_class and attributed == mutations_per_class,
            }
            overview["mutations"] += mutations_per_class
            overview["detected"] += detected
            overview["attributed"] += attributed
            overview["failures"] += len(failures)

        # ---- input-integrity controls (tamper detection; G5 support) --------------------------
        input_controls = _input_integrity_controls(ctx, paths, fixture_dir, workdir, n_events)

        report = {
            "schema": SCHEMA,
            "gate": "G3",
            "generated_at_utc": _utc_now(),
            "requirement": "100% detection with correct invariant attribution for every defect "
                           "class; >=100 seeded mutations per class (docs/method.md §7.6)",
            "synthetic": True,
            "note": "SYNTHETIC mutation corpus on a synthetic fixture - harness validation, "
                    "not a mechanism measurement",
            "mutation_base_seed": MUTATION_BASE_SEED,
            "mutations_per_class": mutations_per_class,
            "defect_classes": list(F.DEFECT_CLASSES),
            "fixture": {
                "run_id": descriptor["run_id"],
                "mechanism": descriptor["mechanism"],
                "seed_hex": descriptor["seed_hex"],
                "events": n_events,
                "partitions": partitions,
                "expected_postings": descriptor["expected_postings"],
                "gt_sha256": descriptor["gt_sha256"],
                "source_integrity": source_detail["source_integrity"],
            },
            "clean_control": clean_control,
            "classes": classes_report,
            "input_integrity_controls": input_controls,
            "overview": {
                **overview,
                "detected_pct": 100.0 * overview["detected"] / max(1, overview["mutations"]),
                "attributed_pct": 100.0 * overview["attributed"] / max(1, overview["mutations"]),
                "clean_control_passed": clean_control["passed"],
                "input_controls_passed": input_controls["passed"],
                "all_classes_passed": all(c["passed"] for c in classes_report.values()),
            },
            "oracle": {
                "oracle_script_sha256": O.oracle_script_sha256(),
                "derive_sha256": D.sha256_file(os.path.join(
                    os.path.dirname(os.path.abspath(O.__file__)), "derive.py")),
            },
        }
        report["passed"] = bool(
            report["overview"]["all_classes_passed"]
            and clean_control["passed"]
            and input_controls["passed"]
        )
        return report
    finally:
        if cleanup:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)


def _input_integrity_controls(ctx: O.OracleContext, paths: dict, fixture_dir: str,
                              workdir: str, n: int) -> dict:
    """Prove that GT/source tampering is caught (the detection half of G5)."""
    controls_dir = os.path.join(workdir, "tamper")
    os.makedirs(controls_dir, exist_ok=True)
    results = {}

    def _verify(gt=paths["gt"], source=paths["source_dump"], produced=paths["produced"]):
        return ctx.verify_source(gt_path=gt, source_dump_path=source,
                                 produced_manifest_path=produced)

    # 1. clean baseline
    results["clean_baseline"] = {"source_integrity": _verify()["source_integrity"]}

    # 2. GT byte tampering (flip one digit inside the file)
    gt_path = os.path.join(controls_dir, "gt_tampered.ndjson")
    with open(paths["gt"], "r", encoding="utf-8") as fh:
        text = fh.read()
    tampered = text.replace('"line_no":0', '"line_no":9', 1)
    if tampered == text:  # fallback: alter last character before newline
        tampered = text[:-2] + ("0" if text[-2] != "0" else "1") + "\n"
    with open(gt_path, "w", encoding="utf-8", newline="") as fh:
        fh.write(tampered)
    d = _verify(gt=gt_path)
    results["gt_tampered"] = {"gt_digest_match": d["gt_digest_match"],
                              "source_integrity": d["source_integrity"]}

    # 3. source dump with a missing middle record (offset gap)
    src_lines = [line for line in open(paths["source_dump"], encoding="utf-8") if line.strip()]
    gap_path = os.path.join(controls_dir, "source_gap.ndjson")
    with open(gap_path, "w", encoding="utf-8", newline="") as fh:
        fh.writelines(src_lines[: max(1, len(src_lines) // 2)] + src_lines[max(1, len(src_lines) // 2) + 1:])
    d = _verify(source=gap_path)
    results["source_missing_record"] = {
        "offsets_contiguous": d["source_offsets_contiguous"],
        "multiset_matches_manifest": d["source_event_multiset_matches_manifest"],
        "source_integrity": d["source_integrity"],
    }

    # 4. produced manifest with a duplicated record (multiset mismatch)
    man_lines = [line for line in open(paths["produced"], encoding="utf-8") if line.strip()]
    man_path = os.path.join(controls_dir, "produced_dup.ndjson")
    with open(man_path, "w", encoding="utf-8", newline="") as fh:
        fh.writelines(man_lines + [man_lines[0]])
    d = _verify(produced=man_path)
    results["manifest_duplicated_record"] = {
        "multiset_matches_manifest": d["source_event_multiset_matches_manifest"],
        "source_integrity": d["source_integrity"],
    }

    # restore clean verification for any later use of this context
    _verify()
    passed = (
        results["clean_baseline"]["source_integrity"] is True
        and results["gt_tampered"]["source_integrity"] is False
        and results["source_missing_record"]["source_integrity"] is False
        and results["manifest_duplicated_record"]["source_integrity"] is False
    )
    return {"controls": results, "passed": bool(passed)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="WXEO G3 oracle mutation testing")
    parser.add_argument("--mutations", type=int, default=120,
                        help="seeded mutations per defect class (>=100 required)")
    parser.add_argument("--events", type=int, default=200, help="synthetic fixture event count")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--seed-hex", default="0f0e0d0c0b0a0908")
    parser.add_argument("--out", default=os.path.join(_REPO, "results", "harness", "g3-mutation.json"))
    parser.add_argument("--workdir", default=None)
    args = parser.parse_args(argv)
    if args.mutations < 100:
        print("REFUSING: G3 requires >=100 mutations per defect class", file=sys.stderr)
        return 2
    report = run_mutation_test(
        mutations_per_class=args.mutations, n_events=args.events,
        partitions=args.partitions, seed_hex=args.seed_hex, workdir=args.workdir,
    )
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8", newline="") as fh:
        json.dump(report, fh, indent=2)
        fh.write("\n")
    print(json.dumps({
        "out": args.out,
        "passed": report["passed"],
        "mutations_per_class": report["mutations_per_class"],
        "overview": report["overview"],
        "per_class": {k: {"detected_pct": v["detected_pct"],
                          "attributed_pct": v["attributed_pct"],
                          "attribution_histogram": v["attribution_histogram"]}
                      for k, v in report["classes"].items()},
    }, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
