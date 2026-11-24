#!/usr/bin/env python3
"""P2 compute-projection worksheet (docs/prereg.md 9.2).

Reads ONLY measured pilot run records (results/raw/run-records/run_pilot-*.json) plus the frozen
matrix counts, and prints the P2 projection table.  No estimate is ever substituted for a measured
value: run types without pilot measurements are reported as NOT MEASURED (they require the M7
calibration block before they can be projected).

Usage: python tools/projection.py [--results results] [--out reports/p2-projection.md]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import statistics
import sys
from collections import defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    idx = min(len(xs) - 1, max(0, int(round(q * (len(xs) - 1)))))
    return xs[idx]


def run_seconds(rec: dict) -> float | None:
    t = rec.get("timings") or {}
    start, end = t.get("t_start"), t.get("t_end")
    if not isinstance(start, int) or not isinstance(end, int) or end <= start:
        return None
    return (end - start) / 1000.0


def load_matrix_counts() -> dict:
    try:
        import yaml
    except ImportError:  # pragma: no cover
        return {}
    path = os.path.join(REPO, "experiments", "matrix.yaml")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        matrix = yaml.safe_load(fh)
    counts = defaultdict(int)
    block_map = {
        ("a", "A-crash"): "A.fault", ("a", "A-control"): "A.ctrl",
        ("b", "B-core"): "B.core", ("b", "B-S1"): "B.S1", ("b", "B-S2"): "B.S2",
        ("c", "C-core"): "C.core", ("c", "C-1m"): "C.1M",
        ("c", "C-capacity"): "C.capacity",
    }
    for cell in matrix.get("cells", []):
        if not cell.get("executed", True):
            continue
        key = block_map.get((cell.get("stage"), cell.get("block")))
        if key is None:
            continue
        counts[key] += int(cell.get("n_planned_slots") or cell.get("reps") or 0)
    return dict(counts)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=os.path.join(REPO, "results"))
    ap.add_argument("--out", default="")
    args = ap.parse_args(argv)

    records = []
    for path in sorted(glob.glob(os.path.join(args.results, "raw", "run-records",
                                              "run_pilot-*.json"))):
        try:
            with open(path, encoding="utf-8") as fh:
                records.append(json.load(fh))
        except Exception:  # noqa: BLE001 - partial malformed record must not crash the worksheet
            continue

    buckets: dict[str, list[float]] = defaultdict(list)
    valid_count = defaultdict(int)
    censored = 0
    disk_min = None
    for rec in records:
        run_id = rec.get("run_id") or ""
        secs = run_seconds(rec)
        validity = rec.get("validity") or {}
        if validity.get("valid"):
            valid_count[run_id.split("-r")[0]] += 1
        q = rec.get("quiescence") or {}
        if q.get("censored"):
            censored += 1
        d = validity.get("disk_free_start_gb")
        if isinstance(d, (int, float)):
            disk_min = d if disk_min is None else min(disk_min, d)
        if secs is None:
            continue
        if run_id.startswith("pilot-valid-"):
            buckets["pilot.validation"].append(secs)
            continue
        fault = (rec.get("fault") or "F0").upper()
        if rec.get("stage") != "pilot":
            buckets["other." + str(rec.get("stage"))].append(secs)
            continue
        if fault in ("F0", "NONE", "CTRL"):
            buckets["pilot.fault-free"].append(secs)
        else:
            buckets[f"pilot.{fault}"].append(secs)
            buckets["pilot.fault-all"].append(secs)
        buckets["pilot.all"].append(secs)

    counts = load_matrix_counts()

    def row(label: str, n: int | None, key: str) -> dict:
        vals = buckets.get(key, [])
        med = statistics.median(vals) if vals else None
        p90 = pct(vals, 0.90)
        proj_h = (n * p90 / 3600.0) if (n and p90 is not None) else None
        return {"type": label, "n": n, "measured_n": len(vals), "median_s": med,
                "p90_s": p90, "hours_p90": proj_h}

    rows = [
        row("Pilot written (all records with timings)", len(records), "pilot.all"),
        row("  pilot fault-free cells", None, "pilot.fault-free"),
        row("  pilot fault cells (all classes)", None, "pilot.fault-all"),
        row("Pilot validation runs", 26, "pilot.validation"),
    ]
    for cls in ("K1", "K2", "K3", "K4", "K5", "K7", "F1", "F2", "F3", "F4A", "F4B",
                "F5", "F6", "F0"):
        if f"pilot.{cls}" in buckets:
            rows.append(row(f"  pilot class {cls}", None, f"pilot.{cls}"))

    a_p90 = pct(buckets.get("pilot.fault-all", []) + buckets.get("pilot.fault-free", []), 0.90)
    b_needs = "NOT MEASURED (needs M7 calibration block)"

    table = ["| Run type | n | measured n | median (s) | p90 (s) | projected h (p90) | notes |",
             "|---|---|---|---|---|---|---|"]
    for r in rows:
        med = "-" if r["median_s"] is None else f'{r["median_s"]:.1f}'
        p90 = "-" if r["p90_s"] is None else f'{r["p90_s"]:.1f}'
        hours = "-" if r["hours_p90"] is None else f'{r["hours_p90"]:.2f}'
        table.append(f'| {r["type"]} | {r["n"] if r["n"] is not None else r["measured_n"]} | '
                     f'{r["measured_n"]} | {med} | {p90} | {hours} | |')

    a_n = counts.get("A.fault", 0) + counts.get("A.ctrl", 0)
    a_hours = (a_n * a_p90 / 3600.0) if (a_n and a_p90 is not None) else None
    for label, key, note in (
        ("A crash-class + controls", a_n, "projected from pilot 1k fault/fault-free p90" if a_hours
         else "NOT MEASURED"),
        ("B core (100k/P6)", counts.get("B.core", 0), b_needs),
        ("B S1 (10k/P6)", counts.get("B.S1", 0), b_needs),
        ("B S2 (100k/P3,P12)", counts.get("B.S2", 0), b_needs),
        ("C core (fixed-rate)", counts.get("C.core", 0), b_needs),
        ("C 1M", counts.get("C.1M", 0), b_needs),
        ("C capacity (saturation)", counts.get("C.capacity", 0), b_needs),
    ):
        hours = "-"
        if key == a_n and a_hours is not None:
            hours = f"{a_hours:.2f}"
        table.append(f"| {label} | {key or '-'} | - | - | - | {hours} | {note} |")

    total_measured = None
    if a_hours is not None:
        total_measured = a_hours
        table.append(f"| **Subtotal measured (A only, pre-calibration)** | **{a_n}** | - | - | - |"
                     f" **{total_measured:.2f}** | +15% contingency ="
                     f" **{total_measured * 1.15:.2f} h** |")

    meta = [
        f"- pilot records parsed: {len(records)}; valid: {sum(valid_count.values())}; "
        f"censored: {censored}",
        f"- lowest recorded free disk: {disk_min if disk_min is not None else 'n/a'} GB",
        f"- matrix executable counts used: {counts}",
        f"- A projection p90 basis: {a_p90:.1f} s" if a_p90 else "- A projection basis: n/a",
        "- B/C rows remain NOT MEASURED by design: the pilot is 1k-only, and docs/prereg.md 9.2 "
        "forbids estimated values. They require the M7 calibration block (9 mechanisms x "
        "{10k,100k} x P6 x N=3 = 54 runs) before they can be filled.",
    ]

    text = ("# P2 compute projection worksheet (measured values only)\n\n"
            + "\n".join(table) + "\n\n" + "\n".join(meta) + "\n")
    print(text)
    if args.out:
        out = args.out if os.path.isabs(args.out) else os.path.join(REPO, args.out)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)
        print(f"written: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
