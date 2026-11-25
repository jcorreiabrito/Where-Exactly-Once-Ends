#!/usr/bin/env python3
"""Reduced-study analysis (Amendment A7): descriptive correctness + committed throughput.

Reads ONLY executed run records under results/raw/run-records (plus the run order for cell
metadata), and writes results/analysis-reduced/:
  * correctness-by-cell.csv / correctness-by-mechanism-fault.csv  (proportions + Clopper-Pearson CI)
  * performance-by-cell.csv / performance-by-mechanism-load.csv   (committed-event throughput)
  * validation-summary.csv
  * summary.json, tables/t1-correctness.tex, tables/t2-performance.tex, macros.tex
No hypothesis tests are performed: with N=3 the pre-registered A2 inference is not identified
(docs/prereg-amendment-A7.md). This is descriptive/exploratory reporting only.

Usage: python tools/analyze_reduced.py [--records results/raw/run-records]
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import statistics
import sys
from collections import defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)


def cp_interval(k: int, n: int, alpha: float = 0.05):
    """Two-sided Clopper-Pearson interval for k successes out of n."""
    if n <= 0:
        return (None, None)
    try:
        from scipy.stats import beta
    except Exception:  # noqa: BLE001
        p = k / n
        return (p, p)
    lo = 0.0 if k == 0 else float(beta.ppf(alpha / 2, k, n - k + 1))
    hi = 1.0 if k == n else float(beta.ppf(1 - alpha / 2, k + 1, n - k))
    return (round(lo, 4), round(hi, 4))


def load_records(records_dir: str) -> list[dict]:
    out = []
    for path in sorted(glob.glob(os.path.join(records_dir, "run_*.json"))):
        try:
            with open(path, encoding="utf-8") as fh:
                out.append(json.load(fh))
        except Exception:  # noqa: BLE001
            continue
    return out


def correctness(records: list[dict], rows_by_id: dict) -> dict:
    cells = defaultdict(list)
    for rec in records:
        if rec.get("stage") != "A":
            continue
        cells[rec.get("cell_id") or "?"].append(rec)
    by_cell, by_mf = [], defaultdict(list)
    for cell_id, recs in sorted(cells.items()):
        meta = rows_by_id.get(recs[0].get("run_id"), {}) or {}
        mechan = recs[0].get("mechanism") or meta.get("mechanism")
        fault = recs[0].get("fault") or meta.get("fault")
        valid = [r for r in recs if (r.get("validity") or {}).get("valid")]
        violating = [r for r in valid if (r.get("violations") or {}).get("violating_run")]
        n, k = len(valid), len(violating)
        lo, hi = cp_interval(k, n)
        counts = {"dup_postings": 0, "lost_postings": 0, "spurious_postings": 0, "imbalanced_entries": 0}
        for r in valid:
            c = r.get("counts") or {}
            counts["dup_postings"] += int(c.get("dup_postings") or 0)
            counts["lost_postings"] += int(c.get("lost_postings") or 0)
            counts["spurious_postings"] += int(c.get("spurious_postings") or 0)
            counts["imbalanced_entries"] += int(c.get("imbalanced_entries") or 0)
        row = {"cell_id": cell_id, "mechanism": mechan, "fault": fault,
               "n_valid": n, "n_attempted": len(recs), "violating": k,
               "violation_rate": (k / n) if n else None, "ci_low": lo, "ci_high": hi,
               **counts}
        by_cell.append(row)
        by_mf[(mechan, fault)].append(row)
    agg = []
    for (mechan, fault), rows in sorted(by_mf.items()):
        n = sum(r["n_valid"] for r in rows)
        k = sum(r["violating"] for r in rows)
        lo, hi = cp_interval(k, n)
        agg.append({"mechanism": mechan, "fault": fault, "n_valid": n, "violating": k,
                    "violation_rate": (k / n) if n else None, "ci_low": lo, "ci_high": hi,
                    "dup_postings": sum(r["dup_postings"] for r in rows),
                    "lost_postings": sum(r["lost_postings"] for r in rows),
                    "spurious_postings": sum(r["spurious_postings"] for r in rows),
                    "imbalanced_entries": sum(r["imbalanced_entries"] for r in rows)})
    by_fault = []
    for fault in sorted({r["fault"] for r in agg}, key=str):
        rows = [r for r in agg if r["fault"] == fault]
        n = sum(r["n_valid"] for r in rows)
        k = sum(r["violating"] for r in rows)
        lo, hi = cp_interval(k, n)
        by_fault.append({"fault": fault, "n_valid": n, "violating": k,
                         "violation_rate": (k / n) if n else None,
                         "ci_low": lo, "ci_high": hi})
    by_mechanism = []
    for mech in sorted({r["mechanism"] for r in agg}, key=str):
        rows = [r for r in agg if r["mechanism"] == mech]
        n = sum(r["n_valid"] for r in rows)
        k = sum(r["violating"] for r in rows)
        lo, hi = cp_interval(k, n)
        by_mechanism.append({"mechanism": mech, "n_valid": n, "violating": k,
                            "violation_rate": (k / n) if n else None,
                            "ci_low": lo, "ci_high": hi})
    return {"by_cell": by_cell, "by_mechanism_fault": agg,
            "by_fault": by_fault, "by_mechanism": by_mechanism}


def performance(records: list[dict]) -> dict:
    cells = defaultdict(list)
    for rec in records:
        if str(rec.get("stage")) != "M7":
            continue
        perf = rec.get("performance") or {}
        basis = perf.get("committed_basis")
        if basis not in ("app-committed", "app-committed-partial"):
            continue
        cell_id = rec.get("cell_id") or "?"
        if "10k" not in cell_id and "100k" not in cell_id:
            continue
        cells[cell_id].append(rec)
    by_cell, by_ml = [], defaultdict(list)
    for cell_id, recs in sorted(cells.items()):
        mechan = recs[0].get("mechanism")
        load = recs[0].get("load_events")
        samples = []
        for r in recs:
            perf = r.get("performance") or {}
            eps = perf.get("throughput_eps_median")
            events = perf.get("committed_events")
            if r.get("validity", {}).get("valid") is True \
                    and (r.get("quiescence") or {}).get("censored") is not True \
                    and isinstance(eps, (int, float)) and eps > 0:
                samples.append({"eps": float(eps), "committed": int(events or 0),
                                "window": perf.get("perf_window")})
        if not samples:
            continue
        eps = [s["eps"] for s in samples]
        row = {"cell_id": cell_id, "mechanism": mechan, "load_events": load,
               "partitions": recs[0].get("partitions"), "n": len(samples),
               "throughput_median_eps": round(statistics.median(eps), 3),
               "throughput_min_eps": round(min(eps), 3),
               "throughput_max_eps": round(max(eps), 3),
               "committed_events_median": int(statistics.median(
                   [s["committed"] for s in samples]))}
        by_cell.append(row)
        by_ml[(mechan, load)].extend(eps)
    mech_rows = []
    for (mechan, load), vals in sorted(by_ml.items(), key=lambda x: (x[0][1], x[0][0])):
        mech_rows.append({"mechanism": mechan, "load_events": load,
                          "n_measurements": len(vals),
                          "throughput_median_eps": round(statistics.median(vals), 3) if vals else None,
                          "throughput_min_eps": round(min(vals), 3) if vals else None,
                          "throughput_max_eps": round(max(vals), 3) if vals else None})
    return {"by_cell": by_cell, "by_mechanism_load": mech_rows}


def validation_summary(records: list[dict]) -> list[dict]:
    out = []
    for rec in records:
        rid = rec.get("run_id") or ""
        if not (rec.get("stage") == "pilot" and rid.startswith("pilot-valid-")):
            continue
        v = rec.get("validity") or {}
        out.append({"run_id": rid, "valid": v.get("valid"),
                    "exclude_reason": v.get("exclude_reason"),
                    "notes": (v.get("notes") or [])[-1:]})
    return out


def write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write("")
        return
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def tex_escape(text) -> str:
    s = str(text)
    return (s.replace("\\", r"\textbackslash{}").replace("&", r"\&").replace("%", r"\%")
            .replace("$", r"\$").replace("#", r"\#").replace("_", r"\_")
            .replace("+", r"+" ))


def fmt_num(v, nd=1):
    return "--" if v is None else f"{float(v):.{nd}f}"


def write_tables(out_dir: str, correctness: dict, performance: dict) -> dict:
    tdir = os.path.join(out_dir, "tables")
    os.makedirs(tdir, exist_ok=True)
    files = {}
    lines = [r"% Auto-generated by tools/analyze_reduced.py (Amendment A7) -- descriptive only.",
             r"\begin{tabular}{llrrr}", r"\toprule",
             r"Mechanism & Condition & $n$ & violating & 95\,\% CP CI \\", r"\midrule"]
    for row in correctness["by_mechanism_fault"]:
        lines.append(
            f"{tex_escape(row['mechanism'])} & {tex_escape(row['fault'])} & {row['n_valid']} & "
            f"{row['violating']} & [{fmt_num(row['ci_low'], 2)}, {fmt_num(row['ci_high'], 2)}] \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    path = os.path.join(tdir, "t1-correctness.tex")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(lines) + "\n")
    files["t1"] = path

    lines = [r"% Auto-generated by tools/analyze_reduced.py (Amendment A7) -- descriptive only.",
             r"\begin{tabular}{lrrrr}", r"\toprule",
             r"Mechanism & Load & $n$ & median (ev/s) & range (ev/s) \\", r"\midrule"]
    for row in performance["by_mechanism_load"]:
        lines.append(
            f"{tex_escape(row['mechanism'])} & {row['load_events']} & {row['n_measurements']} & "
            f"{fmt_num(row['throughput_median_eps'], 0)} & "
            f"[{fmt_num(row['throughput_min_eps'], 0)}, {fmt_num(row['throughput_max_eps'], 0)}] \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    path = os.path.join(tdir, "t2-performance.tex")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(lines) + "\n")
    files["t2"] = path

    # compact by-mechanism table (paper Table 2)
    lines = [r"% Auto-generated by tools/analyze_reduced.py (Amendment A7) -- descriptive only.",
             r"\begin{tabular}{lrrr}", r"\toprule",
             r"Mechanism & $n$ & violating & rate (95\,\% CP) \\", r"\midrule"]
    for row in correctness["by_mechanism"]:
        lines.append(
            f"{tex_escape(row['mechanism'])} & {row['n_valid']} & {row['violating']} & "
            f"{fmt_num((row['violation_rate'] or 0) * 100, 0)}\\,\\% "
            f"[{fmt_num((row['ci_low'] or 0) * 100, 0)}, {fmt_num((row['ci_high'] or 0) * 100, 0)}] \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    path = os.path.join(tdir, "t1-mechanism.tex")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(lines) + "\n")
    files["t1m"] = path

    # compact by-fault table (paper Table 3)
    lines = [r"% Auto-generated by tools/analyze_reduced.py (Amendment A7) -- descriptive only.",
             r"\begin{tabular}{lrrr}", r"\toprule",
             r"Fault class & $n$ & violating & rate (95\,\% CP) \\", r"\midrule"]
    for row in correctness["by_fault"]:
        lines.append(
            f"{tex_escape(row['fault'])} & {row['n_valid']} & {row['violating']} & "
            f"{fmt_num((row['violation_rate'] or 0) * 100, 0)}\\,\\% "
            f"[{fmt_num((row['ci_low'] or 0) * 100, 0)}, {fmt_num((row['ci_high'] or 0) * 100, 0)}] \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    path = os.path.join(tdir, "t1-fault.tex")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(lines) + "\n")
    files["t1f"] = path

    # violating cells detail (paper Table 4)
    lines = [r"% Auto-generated by tools/analyze_reduced.py (Amendment A7) -- descriptive only.",
             r"\begin{tabular}{llrrrr}", r"\toprule",
             r"Mechanism & Fault & $n$ & violating & duplicate & lost \\", r"\midrule"]
    for row in correctness["by_cell"]:
        if not row["violating"]:
            continue
        lines.append(
            f"{tex_escape(row['mechanism'])} & {tex_escape(row['fault'])} & {row['n_valid']} & "
            f"{row['violating']} & {row['dup_postings']} & {row['lost_postings']} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    path = os.path.join(tdir, "t1-violating-cells.tex")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(lines) + "\n")
    files["t1v"] = path
    return files


_DIGITS = {"0": "Zero", "1": "One", "2": "Two", "3": "Three", "4": "Four",
           "5": "Five", "6": "Six", "7": "Seven", "8": "Eight", "9": "Nine"}


def _safe_macro(name) -> str:
    """Control-sequence-safe token: letters only (digits are not letters in TeX)."""
    out = []
    for ch in str(name):
        if ch.isalpha():
            out.append(ch)
        elif ch in _DIGITS:
            out.append(_DIGITS[ch])
        # '+' and '-' simply separate tokens
    return "".join(out)


def write_macros(out_dir: str, records, correctness, performance) -> dict:
    valid = [r for r in records if (r.get("validity") or {}).get("valid")]
    total_violating = sum(1 for r in valid if (r.get("violations") or {}).get("violating_run"))
    macros = {"ResTotalRecords": len(records), "ResTotalValidRuns": len(valid), "ResTotalExcludedRuns": len(records) - len(valid),
              "ResTotalViolatingRuns": total_violating}
    per_mech = defaultdict(lambda: [0, 0])
    for r in valid:
        if r.get("stage") != "A":
            continue
        mech = r.get("mechanism")
        per_mech[mech][1] += 1
        if (r.get("violations") or {}).get("violating_run"):
            per_mech[mech][0] += 1
    lines = [r"% Auto-generated by tools/analyze_reduced.py (Amendment A7)."]
    for name, val in macros.items():
        lines.append(f"\\newcommand{{\\{name}}}{{{val}}}")
    for mech, (k, n) in sorted(per_mech.items(), key=lambda x: str(x[0])):
        safe = _safe_macro(mech)
        lines.append(f"\\newcommand{{\\ResMech{safe}ViolatingRuns}}{{{k}}}")
        lines.append(f"\\newcommand{{\\ResMech{safe}ValidRuns}}{{{n}}}")
    # per-fault macros for the narrative
    for row in correctness["by_fault"]:
        safe = _safe_macro(row["fault"])
        lines.append(f"\\newcommand{{\\ResFault{safe}ValidRuns}}{{{row['n_valid']}}}")
        lines.append(f"\\newcommand{{\\ResFault{safe}ViolatingRuns}}{{{row['violating']}}}")
    lines.append(f"\\newcommand{{\\ResStageACells}}{{{len(correctness['by_cell'])}}}")
    lines.append(f"\\newcommand{{\\ResPerfCells}}{{{len(performance['by_cell'])}}}")
    path = os.path.join(out_dir, "macros.tex")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(lines) + "\n")
    return macros


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", default=os.path.join(REPO, "results", "raw", "run-records"))
    ap.add_argument("--out", default=os.path.join(REPO, "results", "analysis-reduced"))
    args = ap.parse_args(argv)
    os.makedirs(args.out, exist_ok=True)
    records = load_records(args.records)
    rows_by_id = {}
    order = os.path.join(REPO, "experiments", "run_order.csv")
    if os.path.exists(order):
        with open(order, encoding="utf-8", newline="") as fh:
            rows_by_id = {r["run_id"]: r for r in csv.DictReader(fh)}
    corr = correctness(records, rows_by_id)
    perf = performance(records)
    val = validation_summary(records)
    write_csv(os.path.join(args.out, "correctness-by-cell.csv"), corr["by_cell"])
    write_csv(os.path.join(args.out, "correctness-by-mechanism-fault.csv"),
              corr["by_mechanism_fault"])
    write_csv(os.path.join(args.out, "correctness-by-fault.csv"), corr["by_fault"])
    write_csv(os.path.join(args.out, "correctness-by-mechanism.csv"),
              corr["by_mechanism"])
    write_csv(os.path.join(args.out, "performance-by-cell.csv"), perf["by_cell"])
    write_csv(os.path.join(args.out, "performance-by-mechanism-load.csv"),
              perf["by_mechanism_load"])
    write_csv(os.path.join(args.out, "validation-summary.csv"), val)
    files = write_tables(args.out, corr, perf)
    macros = write_macros(args.out, records, corr, perf)
    summary = {
        "schema": "wxeo.reduced-analysis.v1",
        "amendment": "A7 (reduced programme, descriptive only, no hypothesis tests)",
        "n_records": len(records),
        "n_valid": macros["ResTotalValidRuns"],
        "n_violating": macros["ResTotalViolatingRuns"],
        "correctness_cells": len(corr["by_cell"]),
        "performance_cells": len(perf["by_cell"]),
        "validation_runs": len(val),
        "tables": {k: os.path.relpath(v, REPO) for k, v in files.items()},
        "warning": ("Descriptive/exploratory only: Stage-A N=3 and M7 performance N per cell are "
                    "below the pre-registered design, so no confirmatory hypothesis verdict is "
                    "reported (Amendment A7)."),
    }
    with open(os.path.join(args.out, "summary.json"), "w", encoding="utf-8", newline="") as fh:
        json.dump(summary, fh, indent=2)
        fh.write("\n")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
