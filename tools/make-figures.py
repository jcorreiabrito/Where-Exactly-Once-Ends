#!/usr/bin/env python3
"""Vector-figure generator — Where Exactly-Once Ends (CONTRACTS §11, prereg §10).

Figures are generated **only** from committed CSV files produced by the frozen
``tools/analyze.py`` pipeline (never from run records directly, never from hand-entered
numbers).  Every figure is a vector PDF with fixed metadata (no timestamps) so re-running on the
same CSVs produces byte-identical files.

Inputs (``--analysis``, default ``results/analysis``):
    correctness-by-mechanism-fault.csv   -> fig-correctness.pdf
    per-run-performance.csv              -> fig-latency-ecdf.pdf
    counts-by-mechanism.csv, performance-by-cell.csv -> fig-cost-coverage.pdf
    performance-by-cell.csv              -> fig-throughput.pdf

Outputs (``--out``, default ``figures``):
    fig-correctness.pdf, fig-latency-ecdf.pdf, fig-cost-coverage.pdf, fig-throughput.pdf

A synthetic dataset (``dataset = synthetic-fixture`` in every CSV) renders an explicit
"SYNTHETIC FIXTURE — NOT RESULTS" banner so a synthetic plot can never be mistaken for a result.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

ALPHA = 0.05
PDF_METADATA = {"CreationDate": None, "ModDate": None, "Creator": "tools/make-figures.py",
                "Producer": "matplotlib (vector PDF)"}


def _repo_rel(path: str | None) -> str | None:
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if not path:
        return None
    rel = os.path.relpath(os.path.abspath(path), repo)
    if rel.startswith(".."):
        return "<scratch>/" + os.path.basename(path)
    return rel.replace(os.sep, "/")


def read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def to_float(v):
    if v is None or v == "" or v == "None" or v == "null":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def dataset_of(rows: list[dict]) -> str:
    for row in rows:
        ds = (row.get("dataset") or "").strip()
        if ds:
            return ds
    return "campaign"


def synthetic_banner(fig, dataset: str) -> None:
    if dataset and dataset != "campaign":
        fig.text(0.5, 0.985, "SYNTHETIC FIXTURE — NOT RESULTS", ha="center", va="top",
                 fontsize=9, color="crimson", fontweight="bold")


def save(fig, path: str, dataset: str) -> None:
    synthetic_banner(fig, dataset)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fig.savefig(path, format="pdf", bbox_inches="tight", metadata=PDF_METADATA)
    plt.close(fig)


def style() -> None:
    plt.rcParams.update({
        "font.size": 8, "axes.titlesize": 9, "axes.labelsize": 8,
        "xtick.labelsize": 7, "ytick.labelsize": 7, "legend.fontsize": 7,
        "figure.dpi": 100, "savefig.dpi": 300, "axes.grid": True,
        "grid.alpha": 0.3, "grid.linewidth": 0.5, "lines.linewidth": 1.2,
    })


# --------------------------------------------------------------------------------------------
# figures
# --------------------------------------------------------------------------------------------
def fig_correctness(rows: list[dict], out_path: str, dataset: str) -> dict | None:
    """Mechanism × fault-class violating-run proportion with CP 95 % CI."""
    if not rows:
        return None
    mechs = sorted({r["mechanism"] for r in rows})
    faults = sorted({r["fault"] for r in rows
                     if r["fault"] not in ("none", "ctrl")})
    if not faults:
        faults = sorted({r["fault"] for r in rows})
    data = {(r["mechanism"], r["fault"]): r for r in rows}
    fig, ax = plt.subplots(figsize=(7.2, 2.9))
    n_f = max(1, len(faults))
    width = 0.8 / len(mechs)
    for mi, mech in enumerate(mechs):
        xs, ys, lo, hi = [], [], [], []
        for fi, fault in enumerate(faults):
            row = data.get((mech, fault))
            if not row or to_float(row.get("n_valid")) in (None, 0):
                continue
            rate = to_float(row.get("violating_rate"))
            if rate is None:
                continue
            xs.append(fi + (mi - (len(mechs) - 1) / 2) * width)
            ys.append(rate)
            lo.append(rate - (to_float(row.get("cp_lo")) or 0.0))
            hi.append((to_float(row.get("cp_hi")) or 0.0) - rate)
        if xs:
            ax.errorbar(xs, ys, yerr=[lo, hi], fmt="o", markersize=3.5,
                        capsize=2, label=mech)
    ax.set_xticks(range(n_f))
    ax.set_xticklabels(faults)
    ax.set_xlabel("fault condition")
    ax.set_ylabel("violating-run proportion")
    ax.set_ylim(-0.02, 1.02)
    ax.set_title("Correctness by mechanism × fault class (CP 95 % CI)")
    ax.legend(ncol=min(9, len(mechs)), loc="upper center", bbox_to_anchor=(0.5, -0.22),
              frameon=False)
    save(fig, out_path, dataset)
    return {"figure": _repo_rel(out_path), "n_mechanisms": len(mechs),
            "n_faults": len(faults)}


def fig_latency_ecdf(rows: list[dict], out_path: str, dataset: str) -> dict | None:
    """ECDF of per-run p99 latency for fault-free runs, one curve per mechanism."""
    usable = [r for r in rows if r.get("fault") in ("F0", "none", "ctrl")
              and to_float(r.get("p99_ms")) is not None]
    if not usable:
        return None
    mechs = sorted({r["mechanism"] for r in usable})
    fig, ax = plt.subplots(figsize=(5.6, 3.4))
    for mech in mechs:
        vals = sorted(to_float(r["p99_ms"]) for r in usable if r["mechanism"] == mech)
        vals = [v for v in vals if v is not None]
        if not vals:
            continue
        arr = np.asarray(vals, dtype=float)
        xs = np.sort(arr)
        ys = np.arange(1, xs.size + 1) / xs.size
        ax.step(xs, ys, where="post", label=mech)
    ax.set_xlabel("per-run p99 end-to-end latency (ms)")
    ax.set_ylabel("ECDF")
    ax.set_title("Fault-free latency ECDF per mechanism")
    ax.legend(ncol=min(3, len(mechs)), frameon=False)
    save(fig, out_path, dataset)
    return {"figure": _repo_rel(out_path), "n_mechanisms": len(mechs)}


def fig_cost_coverage(count_rows: list[dict], perf_rows: list[dict], out_path: str,
                      dataset: str) -> dict | None:
    """Violations closed vs p99 overhead: violating-run rate (y) vs median p99 (x)."""
    counts = {r["mechanism"]: r for r in count_rows}
    fault_free = [r for r in perf_rows if r.get("fault") in ("F0", "none", "ctrl")]
    if not counts or not fault_free:
        return None
    p99 = {}
    for r in fault_free:
        mech = r["mechanism"]
        val = to_float(r.get("p99_median"))
        key = (to_float(r.get("load_events")), to_float(r.get("partitions")))
        if val is None:
            continue
        p99.setdefault(mech, []).append(val)
    mechs = sorted(m for m in counts if m in p99)
    if not mechs:
        return None
    fig, ax = plt.subplots(figsize=(4.6, 3.4))
    for mech in mechs:
        n = to_float(counts[mech].get("n_valid")) or 0
        bad = sum(to_float(counts[mech].get(f"{m}_runs_gt0")) or 0
                  for m in ("dup_postings", "lost_postings", "imbalanced_entries",
                            "mutated_postings", "spurious_postings"))
        viol_rate = (bad / n) if n else 0.0
        x = float(np.median(p99[mech]))
        ax.scatter([x], [viol_rate], s=28)
        ax.annotate(mech, (x, viol_rate), textcoords="offset points", xytext=(4, 3),
                    fontsize=7)
    ax.set_xlabel("median p99 latency (ms), fault-free")
    ax.set_ylabel("run-level violation count / valid runs")
    ax.set_title("Cost vs invariant coverage")
    save(fig, out_path, dataset)
    return {"figure": _repo_rel(out_path), "n_mechanisms": len(mechs)}


def fig_throughput(rows: list[dict], out_path: str, dataset: str) -> dict | None:
    """Median throughput by mechanism for each load × partition block (fault-free)."""
    usable = [r for r in rows if r.get("fault") in ("F0", "none", "ctrl")
              and to_float(r.get("throughput_median")) is not None]
    if not usable:
        return None
    mechs = sorted({r["mechanism"] for r in usable})
    blocks = sorted({(to_float(r["load_events"]), to_float(r["partitions"]))
                     for r in usable})
    fig, ax = plt.subplots(figsize=(7.0, 3.0))
    width = 0.8 / max(1, len(mechs))
    for mi, mech in enumerate(mechs):
        xs, ys, hi, lo = [], [], [], []
        for bi, (load, parts) in enumerate(blocks):
            row = next((r for r in usable if r["mechanism"] == mech
                        and to_float(r["load_events"]) == load
                        and to_float(r["partitions"]) == parts), None)
            if not row:
                continue
            med = to_float(row.get("throughput_median"))
            if med is None:
                continue
            xs.append(bi + (mi - (len(mechs) - 1) / 2) * width)
            ys.append(med)
            hi.append((to_float(row.get("throughput_hi")) or med) - med)
            lo.append(med - (to_float(row.get("throughput_lo")) or med))
        if xs:
            ax.bar(xs, ys, width=width, yerr=[lo, hi], capsize=1.5, label=mech)
    labels = [f"{int(load) if load else '?'}/p{int(parts) if parts else '?'}"
              for load, parts in blocks]
    ax.set_xticks(range(len(blocks)))
    ax.set_xticklabels(labels)
    ax.set_xlabel("load events / partitions")
    ax.set_ylabel("median throughput (events/s)")
    ax.set_title("Fault-free throughput by load × partitions")
    ax.legend(ncol=min(9, len(mechs)), loc="upper center", bbox_to_anchor=(0.5, -0.22),
              frameon=False)
    save(fig, out_path, dataset)
    return {"figure": _repo_rel(out_path), "n_mechanisms": len(mechs),
            "n_blocks": len(blocks)}


# --------------------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------------------
def main(argv=None) -> int:
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parser = argparse.ArgumentParser(description="Generate WXEO vector figures from CSVs")
    parser.add_argument("--analysis", default=os.path.join(repo, "results", "analysis"))
    parser.add_argument("--out", default=os.path.join(repo, "figures"))
    args = parser.parse_args(argv)
    style()
    analysis = os.path.abspath(args.analysis)
    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)

    cbmf = read_csv(os.path.join(analysis, "correctness-by-mechanism-fault.csv"))
    per_run = read_csv(os.path.join(analysis, "per-run-performance.csv"))
    counts = read_csv(os.path.join(analysis, "counts-by-mechanism.csv"))
    perf = read_csv(os.path.join(analysis, "performance-by-cell.csv"))
    dataset = dataset_of(cbmf or per_run or counts or perf)

    outputs = {}
    for name, fn in (
        ("fig-correctness.pdf", lambda: fig_correctness(cbmf, os.path.join(out_dir,
                                                                          "fig-correctness.pdf"),
                                                        dataset)),
        ("fig-latency-ecdf.pdf", lambda: fig_latency_ecdf(per_run,
                                                          os.path.join(out_dir,
                                                                       "fig-latency-ecdf.pdf"),
                                                          dataset)),
        ("fig-cost-coverage.pdf", lambda: fig_cost_coverage(counts, perf,
                                                           os.path.join(out_dir,
                                                                        "fig-cost-coverage.pdf"),
                                                           dataset)),
        ("fig-throughput.pdf", lambda: fig_throughput(perf, os.path.join(out_dir,
                                                                         "fig-throughput.pdf"),
                                                      dataset)),
    ):
        info = fn()
        outputs[name] = info
        print(f"{name}: {'generated' if info else 'SKIPPED (no input data)'}")
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    manifest = {
        "schema": "wxeo.figures-manifest.v1",
        "dataset": dataset,
        "synthetic": dataset != "campaign",
        "warning": ("SYNTHETIC FIXTURE — NOT RESULTS" if dataset != "campaign" else None),
        "inputs": {"analysis_dir": _repo_rel(analysis)},
        "figures": outputs,
    }
    with open(os.path.join(out_dir, "figures-manifest.json"), "w", encoding="utf-8",
              newline="") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    print(json.dumps({"out": out_dir, "dataset": dataset,
                      "generated": [k for k, v in outputs.items() if v]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
