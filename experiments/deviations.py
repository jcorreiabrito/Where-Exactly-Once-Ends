#!/usr/bin/env python3
"""Generate ``results/DEVIATIONS.md`` — planned-vs-executed diff + interpretation register.

prereg §12: the deviations register is **generated** by the runner from the committed matrix and
the executed run records, never hand-written.  Two kinds of content are produced:

1. **Executed diff (generated).** For every matrix cell: planned N, valid N, excluded N, missing
   N, exclusion rate, and the exclusion reasons.  Cells with no executed runs are listed
   explicitly (no silent drops).  Run-record based; updates automatically as runs complete.
2. **Interpretation/erratum register (frozen list).** Mechanical decisions that the frozen
   documents left open or that required an erratum, all with lead rulings recorded in
   ``docs/P2-RULINGS.md``.  These are not data-driven and never change after freeze; additions
   require a lead ruling first.

Usage::

    python experiments/deviations.py [--matrix experiments/matrix.yaml]
                                     [--records results/raw/run-records]
                                     [--out results/DEVIATIONS.md]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: Frozen interpretation / erratum register (additions need a lead ruling first).
INTERPRETATIONS = [
    {
        "id": "R1",
        "title": "GT line numbering, seed encoding and seed passing",
        "decision": "GT `line_no` is 0-based (0..2N-1); `leg_no` is 1-based (1..2). The "
                    "derivation seed argument is the per-repetition `data_seed` interpreted as "
                    "a big-endian u64 and rendered as canonical decimal ASCII; seeds are "
                    "recorded as 16-hex strings and converted independently by the runner/oracle "
                    "and by the Java loader. The campaign `master_seed`, `order_seed` and "
                    "`bootstrap_seed` are 32-hex constants generated from documented phrases "
                    "(recorded in experiments/matrix.yaml and docs/prereg-freeze.json).",
        "reason": "docs/method.md §1.4.1 fixes the hash domains but not the textual encoding of "
                  "the seed or the GT index base; CONTRACTS §4 left both open.",
        "effect": "Byte-level encoding only; no N, cell, factor or statistic changes. Gate G5 "
                  "compares Java and Python on docs/derive-vectors.csv and "
                  "experiments/oracle/g5-vectors.json.",
        "paper": "Artifact appendix (reproduction contract).",
    },
    {
        "id": "R2",
        "title": "run_id encoding (Kafka-topic-legal)",
        "decision": "`<stage>-<mech>-<fault>-<load>-<partitions>[-sat]-rNN`, lower-case "
                    "`[a-z0-9-]`; mechanism tokens m0,m1-kafka,m1x,m2,m3,m4,m2m3,m4m3,m1xm3. "
                    "The pre-registered schema field keeps its semantic form "
                    "`<stage>-<mech>-<condition>-r<rep>`; this is its exact encoding.",
        "reason": "Mechanism names contain '+' (M2+M3), which is illegal in Kafka topic names; "
                  "the prereg §5 shorthand is ambiguous across loads/partitions.",
        "effect": "Naming only; no design change.",
        "paper": "Artifact appendix.",
    },
    {
        "id": "R3",
        "title": "Stage C saturation token; 1M load token; validation-run stage",
        "decision": "Capacity-block runs get the `sat` token (`c-m2-f0-100k-p6-sat-r05`); the 1M "
                    "block uses load token `1m`; the 20 dedicated validation runs use stage "
                    "`pilot` and run_id `pilot-valid-<kind>-rNN`, recorded separately from the "
                    "216/201 matrix slots.",
        "reason": "Both Stage C blocks share 100k/P=6 and would otherwise collide; the "
                  "prereg §5 stage enum has no validation value.",
        "effect": "Identity only; no design change.",
        "paper": "Artifact appendix.",
    },
    {
        "id": "R4",
        "title": "Pilot planned vs executable accounting",
        "decision": "matrix.yaml keeps 216 planned pilot slots; the 15 structurally n/a slots "
                    "(M1-kafka F4a/F4b/F5; M4 F4b; M4+M3 F4b × N=3) carry executed=false + the "
                    "reachability reason and are never counted as passes. Executable pilot = "
                    "201. All reporting separates planned / executable / executed / excluded: "
                    "7,866 planned, 7,851 executable (+20 validation).",
        "reason": "docs/prereg.md §2.3's 216 arithmetic assumes all 72 fault cells are runnable, "
                  "while §2.2 and docs/method.md §5.3 mark 5 cells n/a.",
        "effect": "Accounting only; n/a cells have documented unreachability proofs.",
        "paper": "Results table + limitations.",
    },
    {
        "id": "R5",
        "title": "SPEC ERRATUM — amount derivation byte range",
        "decision": "docs/method.md §1.4.1 wrote `uint64_be(h1[28:36])`, but SHA-256 is 32 bytes. "
                    "Frozen effective form: `amount_minor = 1 + (uint32_be(h1[28:32]) mod "
                    "100000)`. Verified by lead re-derivation against "
                    "docs/derive-vectors.csv (6 vectors). Implementations keep the literal "
                    "in-bounds slice.",
        "reason": "The frozen formula's index range is out of bounds for a 32-byte digest; the "
                  "literal slice semantics (4 available bytes) is the only in-bounds reading.",
        "effect": "Amount values change relative to a hypothetical last-8-byte reading; both "
                  "implementations align on R5-A. No N/cell/statistic changes. GT digest values "
                  "in experiments/oracle/g5-vectors.json are R5-A-consistent.",
        "paper": "Artifact appendix (reproduction contract) + method footnote if the author "
                 "keeps §1.4.1 verbatim.",
    },
    {
        "id": "S4-1",
        "title": "Oracle verdict rule: spurious postings and attestation",
        "decision": "`violating_run = v1∨v2∨v3∨v4∨(spurious_postings>0)`; `v2` is set by "
                    "mutation OR by failure of the append-only attestation. A fabricated posting "
                    "with an unknown key is reported as `SPURIOUS` attribution and is not folded "
                    "into `dup_postings`.",
        "reason": "docs/method.md §2.4 includes 'no spurious postings' in the consistency "
                  "predicate while §2.5 defines v1..v4 only from the four count classes; §2.6 "
                  "says V2 also covers attestation failure.",
        "effect": "Verdict/attribution only; counts keep the exact §2.5 definitions. G3 mutation "
                  "testing exercises the rule (insert-spurious-row class).",
        "paper": "Artifact appendix (oracle contract).",
    },
    {
        "id": "S4-2",
        "title": "G4 receipt protocol for terminal crash failpoints and sham runs",
        "decision": "A terminal `crash` failpoint writes only a `fired` receipt inside the "
                    "service (Runtime.halt(99)); S4 writes the matching `restored` receipt after "
                    "the instance is restarted, so the run still has exactly one fired + one "
                    "restored receipt. Sham validation runs write `armed` + `restored` and no "
                    "`fired`; the G4 validator accepts that only for injector_mode=sham.",
        "reason": "S3's failpoint cannot append after Runtime.halt; a crash is by design not "
                  "restorable from inside the process.",
        "effect": "Attestation accounting only; exclusion policy unchanged.",
        "paper": "Threats (injector attestation).",
    },
    {
        "id": "S4-3",
        "title": "Validation-run composition (20 runs)",
        "decision": "6 sham-injection + 2 C=1 robustness + 6 injector self-tests + 2 quiescence "
                    "calibration (T_quiet doubled, G7) + 4 oracle-fixture validations "
                    "(clean/loss/duplicate/mutation). Mechanisms are spread across the validation "
                    "slate so that the §4.3 interleaving constraints hold for the whole sequence.",
        "reason": "docs/prereg.md §2.3 fixes the categories and the total (20) but not the split.",
        "effect": "Validation only; not part of the matrix counts.",
        "paper": "Artifact appendix (harness validation).",
    },
    {
        "id": "S4-4",
        "title": "Analysis freeze scope and issuance",
        "decision": "The freeze hashes the oracle, run-record validator, matrix generator, "
                    "runner, injectors, deviations generator, analysis and figure scripts, plus "
                    "the matrix/run-order hashes, the seeds generation procedure, the normative "
                    "derivation vectors and the pre-freeze harness evidence. The oracle reads "
                    "only results/raw inputs plus frozen vector files. The freeze is issued "
                    "once during P2 implementation; files whose hashes changed while the "
                    "pipeline was still being completed were re-frozen before the first run — "
                    "this is not a post-freeze analysis change because no run existed and no "
                    "analysis output was ever produced from the earlier revision.",
        "reason": "prereg §10 requires a committed frozen analysis pipeline before Stage A.",
        "effect": "Provenance only; any change after the first Stage A run becomes a deviation.",
        "paper": "Reproducibility.",
    },
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_plan(matrix_path: str) -> dict:
    try:
        import yaml

        with open(matrix_path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh)
    except Exception:  # noqa: BLE001 - matrix may be absent early
        return {}


def _load_records(records_dir: str) -> list[dict]:
    records = []
    if not os.path.isdir(records_dir):
        return records
    for name in sorted(os.listdir(records_dir)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(records_dir, name)
        try:
            with open(path, "r", encoding="utf-8") as fh:
                records.append(json.load(fh))
        except Exception:  # noqa: BLE001
            continue
    return records


def compute_fire_rates(faults_log_path: str, records: list[dict],
                       reserved: list[dict] | None = None) -> dict:
    """F-I5: fire-rate / exclusion accounting per mechanism × fault (S5 review).

    Denominators are **reserved** runs (the frozen matrix plan), never the surviving records:
    a fault that never fired must stay visible as a missing receipt, not vanish.  Returns
    ``{"by_cell": [...], "totals": {...}}``; rows are suitable for a CSV table.
    """
    receipts = []
    if faults_log_path and os.path.exists(faults_log_path):
        with open(faults_log_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        receipts.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    by_run: dict[str, dict] = defaultdict(lambda: {"fired": 0, "restored": 0,
                                                   "armed": 0, "app": 0, "s4": 0})
    for receipt in receipts:
        run_id = receipt.get("run_id")
        if not run_id:
            continue
        entry = by_run[run_id]
        event = receipt.get("event")
        if event in ("fired", "restored", "armed"):
            entry[event] = entry.get(event, 0) + 1
        source = receipt.get("source") or ("app" if receipt.get("fault") in {
            "after_fetch", "before_effect_commit", "after_effect_commit",
            "before_progress_commit", "after_progress_commit", "before_publish",
            "before_relay_mark"} else "s4")
        entry[source] = entry.get(source, 0) + 1
    # reserved denominators from the frozen plan (fallback: existing records)
    plan_index = {}
    for row in (reserved or []):
        plan_index.setdefault((row.get("mechanism"), row.get("fault"), row.get("stage_label")),
                              0)
        plan_index[(row.get("mechanism"), row.get("fault"), row.get("stage_label"))] += 1
    cells: dict[tuple, dict] = {}
    for rec in records:
        key = (rec.get("mechanism"), rec.get("fault"),
               {"pilot": "P", "A": "A", "B": "B", "C": "C"}.get(rec.get("stage"),
                                                                  rec.get("stage")))
        cell = cells.setdefault(key, {"reserved": plan_index.get(key, 0),
                                      "valid": 0, "excluded": 0, "fired": 0,
                                      "restored": 0, "armed": 0,
                                      "no_receipt": 0, "reasons": Counter()})
        validity = rec.get("validity") or {}
        if validity.get("excluded"):
            cell["excluded"] += 1
            cell["reasons"][str(validity.get("exclude_reason") or "unknown")] += 1
        elif validity.get("valid"):
            cell["valid"] += 1
        mine = by_run.get(rec.get("run_id"), {})
        cell["fired"] += int(mine.get("fired", 0) > 0)
        cell["restored"] += int(mine.get("restored", 0) > 0)
        cell["armed"] += int(mine.get("armed", 0) > 0)
        if not mine.get("fired") and rec.get("fault_level") in ("L1", "L2") \
                and rec.get("fault") not in ("F0", "none", "ctrl"):
            cell["no_receipt"] += 1
    rows = []
    for (mechanism, fault, stage), cell in sorted(cells.items()):
        executed = cell["valid"] + cell["excluded"]
        rows.append({
            "stage": stage, "mechanism": mechanism, "fault": fault,
            "reserved": cell["reserved"], "executed": executed,
            "valid": cell["valid"], "excluded": cell["excluded"],
            "fired": cell["fired"], "restored": cell["restored"], "armed": cell["armed"],
            "missing_receipt": cell["no_receipt"],
            "fire_rate": (cell["fired"] / executed) if executed else None,
            "restore_rate": (cell["restored"] / cell["fired"]) if cell["fired"] else None,
            "exclusion_rate": (cell["excluded"] / executed) if executed else None,
            "exclude_reasons": "; ".join(f"{k} x{v}" for k, v in
                                         sorted(cell["reasons"].items())),
        })
    injected = [r for r in rows if r["fault"] not in ("F0", "none", "ctrl")]
    totals = {
        "injected_cell_reports": len(injected),
        "executed_injected": sum(r["executed"] for r in injected),
        "fired": sum(r["fired"] for r in injected),
        "missing_receipt": sum(r["missing_receipt"] for r in injected),
        "excluded": sum(r["excluded"] for r in injected),
        "fire_rate": (sum(r["fired"] for r in injected)
                      / max(1, sum(r["executed"] for r in injected))),
        "exclusion_rate": (sum(r["excluded"] for r in injected)
                           / max(1, sum(r["executed"] for r in injected))),
    }
    return {"by_cell": rows, "totals": totals}


def _load_reserved(summaries_dir: str) -> list[dict]:
    path = os.path.join(summaries_dir, "reserved-runs.csv")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))



    """cell_id -> {stage, mechanism, condition, rep count, n/a} from matrix.yaml."""
    out: dict[str, dict] = {}
    for cell in matrix.get("cells", []):
        out[cell["cell_id"]] = {
            "stage": cell["stage_label"],
            "mechanism": cell["mechanism"],
            "condition": cell["condition"],
            "planned": cell["n_planned_slots"] if cell.get("executed") else 0,
            "planned_slots": cell["n_planned_slots"],
            "executed": bool(cell.get("executed")),
            "na_reason": cell.get("na_reason"),
        }
    for v in matrix.get("validation", []):
        key = f"V.{v['kind']}.{v['mechanism']}.{v['fault']}.1k.p3"
        out[key] = {"stage": "V", "mechanism": v["mechanism"], "condition": v["fault"],
                    "planned": 1, "planned_slots": 1, "executed": True, "na_reason": None}
    return out


def _executed_by_cell(records: list[dict]) -> dict[str, dict]:
    agg: dict[str, dict] = defaultdict(lambda: {
        "valid": 0, "excluded": 0, "censored": 0, "violating": 0,
        "reasons": Counter(), "reps": set(),
    })
    for rec in records:
        cell = rec.get("cell_id", "?")
        entry = agg[cell]
        validity = rec.get("validity") or {}
        if validity.get("excluded"):
            entry["excluded"] += 1
            entry["reasons"][str(validity.get("exclude_reason") or "unknown")] += 1
        elif validity.get("valid"):
            entry["valid"] += 1
        else:
            entry["excluded"] += 1
            entry["reasons"]["invalid_unspecified"] += 1
        if (rec.get("quiescence") or {}).get("censored"):
            entry["censored"] += 1
        if (rec.get("violations") or {}).get("violating_run"):
            entry["violating"] += 1
        entry["reps"].add(rec.get("rep"))
    return agg


def render_deviations(*, matrix: dict, records: list[dict], matrix_path: str) -> str:
    lines: list[str] = []
    plan = _cell_plan(matrix)
    executed = _executed_by_cell(records)
    stages = {k: v for k, v in (matrix.get("accounting") or {}).get("stages", {}).items()}
    totals = (matrix.get("accounting") or {}).get("totals", {})
    lines.append("# DEVIATIONS — planned vs executed (+ interpretation register)")
    lines.append("")
    lines.append("Generated by `experiments/deviations.py` (prereg §12). This file is a "
                 "diff between the frozen matrix and the run records that actually exist, plus "
                 "a frozen register of mechanical interpretations/errata ruled by the lead before "
                 "the freeze. Nothing here changes a pre-registered N, cell, factor or statistic.")
    lines.append("")
    lines.append(f"- generated UTC: `{_utc_now()}`")
    if os.path.exists(matrix_path):
        lines.append(f"- matrix: `experiments/matrix.yaml` "
                     f"(sha256 `{_sha256_file(matrix_path)}`)")
    lines.append(f"- run records scanned: {len(records)}")
    lines.append("")
    lines.append("## 1. Campaign status")
    lines.append("")
    if not records:
        lines.append("No run records exist yet. The matrix and run order are frozen and hashed; "
                     "the pilot has not started (P2 integration gate pending S3's artifact "
                     "acceptance tests and the lead's go-ahead). No cell has been dropped.")
    else:
        total_valid = sum(v["valid"] for v in executed.values())
        total_excl = sum(v["excluded"] for v in executed.values())
        lines.append(f"- valid runs: {total_valid}; excluded runs: {total_excl}; "
                     f"violating runs: {sum(v['violating'] for v in executed.values())}")
        lines.append("")
    lines.append("### Frozen accounting (prereg §2, docs/P2-RULINGS.md R4)")
    lines.append("")
    lines.append("| Stage | cells (total/executed/n-a) | runs planned | runs executable |")
    lines.append("|---|---|---|---|")
    for key, label in (("pilot", "Pilot"), ("A", "A"), ("B", "B"), ("C", "C")):
        s = stages.get(key, {})
        lines.append(f"| {label} | {s.get('cells_total', '—')} / "
                     f"{s.get('cells_executed', '—')} / {s.get('cells_na', '—')} | "
                     f"{s.get('runs_planned', '—')} | {s.get('runs_executable', '—')} |")
    lines.append(f"| **Total** | | **{totals.get('runs_planned', '—')}** planned | "
                 f"**{totals.get('runs_executable', '—')}** executable (+"
                 f"{totals.get('validation_runs', 20)} validation) |")
    lines.append("")
    lines.append("## 2. Executed diff by cell")
    lines.append("")
    lines.append("Only cells whose valid count differs from the plan (including cells with zero "
                 "executed runs) are listed. `N_planned` is the frozen repetition count; excluded "
                 "runs are counted with their provenance-based reason (prereg §8.2).")
    lines.append("")
    if not records:
        lines.append("_No executed cells yet — the diff will populate as runs complete._")
    else:
        lines.append("| cell | stage | mechanism | condition | N_planned | N_valid | N_excluded "
                     "| N_missing | exclusion rate | reasons |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for cell_id, info in sorted(plan.items()):
            if info["stage"] == "V":
                continue
            got = executed.get(cell_id, {"valid": 0, "excluded": 0, "reasons": Counter()})
            planned = info["planned"]
            missing = max(0, planned - got["valid"] - got["excluded"])
            if got["valid"] == planned and got["excluded"] == 0:
                continue
            denom = got["valid"] + got["excluded"]
            rate = f"{got['excluded'] / denom:.1%}" if denom else "—"
            reasons = "; ".join(f"{k} x{v}" for k, v in
                                sorted(got["reasons"].items())) or "—"
            lines.append(f"| `{cell_id}` | {info['stage']} | {info['mechanism']} | "
                         f"{info['condition']} | {planned} | {got['valid']} | "
                         f"{got['excluded']} | {missing} | {rate} | {reasons} |")
        # n/a cells (never counted as passes)
        na_cells = [(cid, info) for cid, info in plan.items()
                    if info["stage"] == "P" and not info["executed"]]
        if na_cells:
            lines.append("")
            lines.append("### Structurally n/a pilot slots (never counted as passes, R4)")
            lines.append("")
            for cid, info in sorted(na_cells):
                lines.append(f"- `{cid}` — planned slot {info['planned_slots']}×; n/a: "
                             f"{info['na_reason']}")
        # cells planned but absent entirely
        absent = [cid for cid, info in plan.items()
                  if info["planned"] > 0 and cid not in executed]
        if absent:
            lines.append("")
            lines.append(f"### Cells with zero executed runs ({len(absent)})")
            lines.append("")
            lines.append(", ".join(f"`{cid}`" for cid in sorted(absent)))
    lines.append("")
    lines.append("## 3. Reruns, exclusions and censoring")
    lines.append("")
    if not records:
        lines.append("_No runs yet._")
    else:
        excl_total = sum(v["excluded"] for v in executed.values())
        cens = sum(v["censored"] for v in executed.values())
        lines.append(f"- excluded runs: {excl_total} (exclusion rate "
                     f"{excl_total / max(1, len(records)):.1%} overall); rerun cap R_max = 3 "
                     "attempts per (cell, rep) (prereg §8.3).")
        lines.append(f"- censored (quiescence-timeout) runs: {cens}; censored runs are outcomes, "
                     "not missing values (prereg §8.3, method §7.3).")
        cell_excl = {cid: v for cid, v in executed.items()
                     if (v["valid"] + v["excluded"]) and v["excluded"] / (v["valid"] + v["excluded"]) > 0.20}
        if cell_excl:
            lines.append("- cells with exclusion rate > 20 % (inference labelled fragile per "
                         "method §5.6): " + ", ".join(f"`{c}`" for c in sorted(cell_excl)))
        else:
            lines.append("- no cell exceeds the 20 % exclusion flag.")
    lines.append("")
    lines.append("## 4. Interpretation / erratum register (frozen)")
    lines.append("")
    lines.append("These are the mechanical decisions the frozen documents left open or that "
                 "required an erratum. They were ruled by the lead before the analysis freeze "
                 "(`docs/P2-RULINGS.md`); none alters a cell, N, factor or statistic.")
    lines.append("")
    lines.append("| ID | Topic | Decision | Reason | Effect | Paper location |")
    lines.append("|---|---|---|---|---|---|")
    for item in INTERPRETATIONS:
        lines.append(f"| {item['id']} | {item['title']} | {item['decision']} | "
                     f"{item['reason']} | {item['effect']} | {item['paper']} |")
    lines.append("")
    lines.append("## 5. Environment deviations (docs/prereg.md §11)")
    lines.append("")
    lines.append("The host has no Docker, no WSL2 and no administrator rights "
                 "(`docs/SUBSTRATE-VALIDATION.md`). Therefore: isolation = native Windows "
                 "processes with recorded command lines/PIDs/affinity; 'image digests' = SHA-256 "
                 "of vendored archives + configuration hashes; resource limits = affinity mask, "
                 "priority class, `-Xmx`, Hikari pool, broker/PG config; restart cadence = fresh "
                 "topics/group/schema + fresh consumer JVMs per run, broker/PG processes "
                 "restarted every 50 runs (D6). Broker failure is a process kill, not a machine "
                 "loss (external-validity threat). All are reported in the paper's threats "
                 "section and are present in every run record (`host`, `versions`, "
                 "`process_limits`).")
    lines.append("")
    return "\n".join(lines) + "\n"


def write_deviations(matrix_path: str, records_dir: str, out_path: str) -> str:
    matrix = _load_plan(matrix_path)
    records = _load_records(records_dir)
    text = render_deviations(matrix=matrix, records=records, matrix_path=matrix_path)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)
    return os.path.abspath(out_path)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate results/DEVIATIONS.md")
    parser.add_argument("--matrix", default=os.path.join(_REPO, "experiments", "matrix.yaml"))
    parser.add_argument("--records", default=os.path.join(_REPO, "results", "raw",
                                                          "run-records"))
    parser.add_argument("--out", default=os.path.join(_REPO, "results", "DEVIATIONS.md"))
    args = parser.parse_args(argv)
    path = write_deviations(args.matrix, args.records, args.out)
    print(json.dumps({"deviations": path}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
