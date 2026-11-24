#!/usr/bin/env python3
"""FROZEN analysis pipeline — Where Exactly-Once Ends (prereg §7, §10; CONTRACTS §11).

This script is frozen before the first Stage A run (``docs/prereg-freeze.json`` records its
SHA-256).  It is a **pure function** of the committed run records plus
``docs/prereg-freeze.json`` and the frozen harness-validation artefacts:

    python tools/analyze.py --records results/raw/run-records --out results/analysis

It never reads the raw dumps, the matrix, or any file outside ``results/raw/``, the freeze file,
and ``results/harness/*.json`` (gate evidence for table T1).  Every reported number is traceable
to a committed CSV/JSON in ``results/analysis/``.

Frozen statistics implemented (prereg §7)
-----------------------------------------
* Clopper–Pearson exact 95 % CIs (two-sided) and one-sided 95 % upper bounds for proportions;
  rule-of-three (3/N) bound reported next to every zero-event cell; the prereg §7.5 sentence
  template is emitted verbatim in the zero-event table.
* McNemar exact test (paired runs, same (stage, condition, rep)); Fisher exact (unpaired
  sensitivity); Holm–Bonferroni within the pre-declared families C1/C2/C3/P1/P2/P3.
* Bootstrap 95 % CI for medians, B = 10,000, percentile method primary, BCa sensitivity, seed
  recorded (`bootstrap_seed` in ``docs/prereg-freeze.json``).
* Friedman across mechanisms with blocks = (load × partitions) within a stage, aggregated to
  block medians; Nemenyi post-hoc with critical difference at α = 0.05.
* Cliff's delta with bootstrap 95 % CI; pre-registered thresholds |δ|<0.147 negligible,
  <0.33 small, <0.474 medium, otherwise large.
* Wilcoxon–Mann–Whitney (Wilcoxon signed-rank for paired block medians) pairwise tests,
  Holm-adjusted within families P1/P2.
* Hypothesis decision rules H1–H5 exactly as prereg §3, yielding confirmed / refuted /
  inconclusive with the rule evidence recorded.
* Decision table derivation (prereg §7.7, docs/method.md §11.2): exposure → frozen cell sets,
  zero-violation-with-attestation suffciency, CP upper bounds, exclusion rates.
* Table/figure/macro generators: results/analysis/macros.tex, tables/t1..t6-*.tex,
  decision-table.csv, tests.json, and per-run CSVs consumed by tools/make-figures.py.

Determinism: no wall-clock timestamps, sorted iteration everywhere, all RNG derived from the
recorded bootstrap seed and a per-comparison label; running twice on the same inputs produces
byte-identical outputs.

Outputs carry a ``dataset`` column / field.  For a synthetic fixture this is
``synthetic-fixture`` and a warning is embedded in every artefact; synthetic outputs are never
paper results.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from glob import glob

import numpy as np
from scipy import stats

ALPHA = 0.05
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BOOTSTRAP_B = 10_000
CLIFF_NEGLIGIBLE = 0.147
CLIFF_SMALL = 0.33
CLIFF_MEDIUM = 0.474
RULE_OF_THREE = 3.0
MECHANISMS = ["M0", "M1-kafka", "M1x", "M2", "M3", "M4", "M2+M3", "M4+M3", "M1x+M3"]
PERF_BLOCKS = [(10_000, 3), (10_000, 6), (10_000, 12),
               (100_000, 3), (100_000, 6), (100_000, 12)]

#: method §11.1 model answer column (model consequence, not a measurement)
MODEL_MINIMAL = {
    "E1": "M3 or M4",
    "E2": "M4",
    "E3": "M3 or M1 family",
    "E4": "M3 or M4",
    "E5": "M1 family",
    "E1∧E2∧E3∧E4": "M4+M3",
    "E1..E4∧E5": "M1x+M3",
    # cost-sensitive row is reported in the table prose, not per mechanism
}

#: prereg §7.7 frozen cell-to-exposure map (conditions; publication handled separately for E5)
EXPOSURE_CELLS = {
    "E1": {"K2", "K3", "K5", "F2", "F4a", "F6"},
    "E2": {"K4", "F4b"},
    "E3": {"K6"},
    "E4": {"K2", "F5"},
    "E5": set(),  # publication metrics
}
EXPOSURE_ORDER = ["E1", "E2", "E3", "E4", "E5", "E1∧E2∧E3∧E4", "E1..E4∧E5"]

ZERO_EVENT_TEMPLATE = (
    "Mechanism {mechanism} produced zero violations in N = {n_valid} valid runs under "
    "{condition} ({n_excluded} excluded: {reasons}). The exact Clopper–Pearson 95 % upper bound "
    "on the per-run violation probability is {cp_upper} (rule-of-three bound 3/N = {rule3}). "
    "This bounds the violation probability; it does not prove absence."
)

DATASET_CAMPAIGN = "campaign"
DATASET_SYNTHETIC = "synthetic-fixture"


# --------------------------------------------------------------------------------------------
# small utilities
# --------------------------------------------------------------------------------------------
def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fmt_p(p) -> str:
    if p is None:
        return "n/a"
    if p < 1e-4:
        return f"{p:.2e}"
    return f"{p:.4f}"


def fmt_rate(x) -> str:
    return "—" if x is None else f"{100.0 * x:.1f}\\%"


def holm(pvalues: dict[str, float | None]) -> dict[str, float | None]:
    """Holm–Bonferroni adjusted p-values (None entries kept None)."""
    items = [(k, v) for k, v in pvalues.items() if v is not None]
    m = len(items)
    out: dict[str, float | None] = {k: None for k in pvalues}
    if m == 0:
        return out
    items.sort(key=lambda kv: kv[1])
    prev = 0.0
    for i, (k, p) in enumerate(items):
        adj = min(1.0, max(prev, (m - i) * p))
        out[k] = adj
        prev = adj
    return out


def cp_ci(k: int, n: int, alpha: float = ALPHA) -> tuple[float, float]:
    """Clopper–Pearson exact two-sided CI for a binomial proportion."""
    if n <= 0:
        return (0.0, 1.0)
    lo = 0.0 if k <= 0 else float(stats.beta.ppf(alpha / 2, k, n - k + 1))
    hi = 1.0 if k >= n else float(stats.beta.ppf(1 - alpha / 2, k + 1, n - k))
    return (lo, hi)


def cp_upper(k: int, n: int, alpha: float = ALPHA) -> float:
    """One-sided exact upper bound at 1-alpha (prereg §7.5: exact value is primary)."""
    if n <= 0:
        return 1.0
    if k >= n:
        return 1.0
    return float(stats.beta.ppf(1 - alpha, k + 1, n - k))


def rule_of_three(n: int) -> float | None:
    return (RULE_OF_THREE / n) if n > 0 else None


def seeded_rng(bootstrap_seed: str, label: str) -> np.random.Generator:
    digest = hashlib.sha256(f"{bootstrap_seed}|{label}".encode("utf-8")).hexdigest()
    return np.random.default_rng(int(digest[:16], 16))


def bootstrap_median_ci(values, bootstrap_seed: str, label: str,
                        b: int = BOOTSTRAP_B) -> dict:
    """Percentile-method bootstrap 95 % CI for the median (+ BCa sensitivity)."""
    arr = np.asarray([v for v in values if v is not None and np.isfinite(v)], dtype=float)
    n = arr.size
    if n == 0:
        return {"n": 0, "median": None, "lo": None, "hi": None, "bca_lo": None,
                "bca_hi": None, "B": b}
    if n == 1:
        med = float(arr[0])
        return {"n": 1, "median": med, "lo": med, "hi": med, "bca_lo": med, "bca_hi": med,
                "B": b}
    rng = seeded_rng(bootstrap_seed, f"median|{label}")
    idx = rng.integers(0, n, size=(b, n))
    boot = np.median(arr[idx], axis=1)
    lo, hi = np.percentile(boot, [100 * ALPHA / 2, 100 * (1 - ALPHA / 2)])
    out = {"n": int(n), "median": float(np.median(arr)), "lo": float(lo), "hi": float(hi),
           "B": b, "method": "percentile"}
    # BCa sensitivity
    try:
        theta = np.median(arr)
        prop = np.mean(boot < theta)
        prop = min(max(prop, 1.0 / (b + 1)), 1.0 - 1.0 / (b + 1))
        z0 = float(stats.norm.ppf(prop))
        jack = np.array([np.median(np.delete(arr, i)) for i in range(n)])
        jmean = jack.mean()
        num = np.sum((jmean - jack) ** 3)
        den = 6.0 * (np.sum((jmean - jack) ** 2) ** 1.5)
        a = float(num / den) if den != 0 else 0.0
        zl = stats.norm.ppf(ALPHA / 2)
        zu = stats.norm.ppf(1 - ALPHA / 2)

        def adjust(z):
            return float(stats.norm.cdf(z0 + (z0 + z) / (1 - a * (z0 + z))))

        al = adjust(zl)
        au = adjust(zu)
        out["bca_lo"] = float(np.percentile(boot, 100 * al))
        out["bca_hi"] = float(np.percentile(boot, 100 * au))
    except Exception:  # noqa: BLE001 - BCa is a sensitivity analysis only
        out["bca_lo"] = out["lo"]
        out["bca_hi"] = out["hi"]
    return out


def cliffs_delta(x, y, paired: bool = False, bootstrap_seed: str | None = None,
                 label: str = "", b: int = BOOTSTRAP_B) -> dict:
    """Cliff's delta with bootstrap 95 % CI and the pre-registered threshold label."""
    xs = np.asarray([v for v in x if v is not None and np.isfinite(v)], dtype=float)
    ys = np.asarray([v for v in y if v is not None and np.isfinite(v)], dtype=float)
    if xs.size == 0 or ys.size == 0:
        return {"delta": None, "lo": None, "hi": None, "magnitude": None, "n_x": int(xs.size),
                "n_y": int(ys.size), "method": "paired" if paired else "unpaired"}
    if paired:
        n = min(xs.size, ys.size)
        d = xs[:n] - ys[:n]
        delta = float((np.sum(d > 0) - np.sum(d < 0)) / n)
        method = "paired"

        def _boot(idx):
            dd = d[idx]
            return (np.sum(dd > 0) - np.sum(dd < 0)) / dd.size

        rng = seeded_rng(bootstrap_seed or "0", f"cliff-paired|{label}")
        idx = rng.integers(0, n, size=(b, n))
        boot = np.array([_boot(row) for row in idx])
    else:
        gt = np.sum(xs[:, None] > ys[None, :])
        lt = np.sum(xs[:, None] < ys[None, :])
        delta = float((gt - lt) / (xs.size * ys.size))
        method = "unpaired"
        rng = seeded_rng(bootstrap_seed or "0", f"cliff|{label}")
        boot = np.empty(b)
        for k in range(b):
            xb = xs[rng.integers(0, xs.size, xs.size)]
            yb = ys[rng.integers(0, ys.size, ys.size)]
            boot[k] = (np.sum(xb[:, None] > yb[None, :]) - np.sum(xb[:, None] < yb[None, :])) \
                / (xb.size * yb.size)
    lo, hi = np.percentile(boot, [100 * ALPHA / 2, 100 * (1 - ALPHA / 2)])
    ad = abs(delta)
    if ad < CLIFF_NEGLIGIBLE:
        magnitude = "negligible"
    elif ad < CLIFF_SMALL:
        magnitude = "small"
    elif ad < CLIFF_MEDIUM:
        magnitude = "medium"
    else:
        magnitude = "large"
    return {"delta": delta, "lo": float(lo), "hi": float(hi), "magnitude": magnitude,
            "n_x": int(xs.size), "n_y": int(ys.size), "method": method}


def mcnemar_exact(pairs: list[tuple[bool, bool]]) -> dict:
    """McNemar's exact test on paired binary outcomes; returns b, c, exact two-sided p."""
    b = sum(1 for x, y in pairs if (not x) and y)
    c = sum(1 for x, y in pairs if x and (not y))
    n = b + c
    if n == 0:
        return {"b": 0, "c": 0, "n_discordant": 0, "p": None,
                "note": "not applicable: zero discordant pairs (prereg §7.6)"}
    p = float(min(1.0, 2.0 * stats.binom.cdf(min(b, c), n, 0.5)))
    return {"b": int(b), "c": int(c), "n_discordant": int(n), "p": p}


def fisher_test(table: list[list[int]]) -> dict:
    """Fisher exact test on a 2x2 table [[a,b],[c,d]]; handles zero margins."""
    arr = np.asarray(table, dtype=int)
    if arr.shape != (2, 2):
        raise ValueError("fisher table must be 2x2")
    if arr.sum(axis=0).min() == 0 or arr.sum(axis=1).min() == 0:
        return {"odds_ratio": None, "p": None,
                "note": "not applicable: zero margin (prereg §7.6)"}
    odds, p = stats.fisher_exact(arr, alternative="two-sided")
    return {"odds_ratio": float(odds), "p": float(p)}


def friedman_block_test(block_values: dict[str, list[float]]) -> dict:
    """Friedman test over mechanisms with complete blocks; average ranks; Nemenyi CD."""
    mechs = sorted(block_values)
    if len(mechs) < 3:
        return {"applicable": False, "reason": "fewer than 3 mechanisms with data"}
    n_blocks = min(len(v) for v in block_values.values())
    if n_blocks < 3:
        return {"applicable": False,
                "reason": f"fewer than 3 complete blocks (have {n_blocks})"}
    arrays = [np.asarray(block_values[m][:n_blocks], dtype=float) for m in mechs]
    try:
        stat, p = stats.friedmanchisquare(*arrays)
    except Exception as exc:  # noqa: BLE001
        return {"applicable": False, "reason": f"friedman failed: {exc}"}
    ranks = np.zeros((n_blocks, len(mechs)))
    for i in range(n_blocks):
        vals = [arr[i] for arr in arrays]
        order = stats.rankdata(vals, method="average")
        ranks[i, :] = order
    avg_ranks = ranks.mean(axis=0)
    k = len(mechs)
    q = float(stats.studentized_range.ppf(1 - ALPHA, k, 1e9)) / math.sqrt(2.0)
    cd = q * math.sqrt(k * (k + 1) / (6.0 * n_blocks))
    rank_map = {m: float(r) for m, r in zip(mechs, avg_ranks)}
    pairwise = []
    for i, a in enumerate(mechs):
        for b in mechs[i + 1:]:
            diff = abs(rank_map[a] - rank_map[b])
            pairwise.append({"a": a, "b": b, "rank_diff": diff, "cd": cd,
                             "significant": bool(diff > cd)})
    return {"applicable": True, "statistic": float(stat), "p": float(p), "k": k,
            "n_blocks": int(n_blocks), "average_ranks": rank_map,
            "critical_difference": cd, "alpha": ALPHA, "pairwise": pairwise,
            "note": "blocks = (load × partitions) within Stage C, aggregated to block medians"}


def paired_wilcoxon(a, b) -> dict:
    xs = np.asarray(a, dtype=float)
    ys = np.asarray(b, dtype=float)
    n = min(xs.size, ys.size)
    xs, ys = xs[:n], ys[:n]
    if n < 3 or np.allclose(xs, ys):
        return {"p": None, "n": int(n), "note": "not applicable: <3 pairs or all ties"}
    try:
        res = stats.wilcoxon(xs, ys, alternative="two-sided", method="exact")
        return {"p": float(res.pvalue), "n": int(n), "method": "exact"}
    except Exception as exc:  # noqa: BLE001
        return {"p": None, "n": int(n), "note": f"wilcoxon failed: {exc}"}


# --------------------------------------------------------------------------------------------
# record loading / extraction
# --------------------------------------------------------------------------------------------
def load_records(records_dir: str) -> tuple[list[dict], list[dict]]:
    records = []
    malformed = []
    if not os.path.isdir(records_dir):
        return [], malformed
    for name in sorted(os.listdir(records_dir)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(records_dir, name)
        try:
            with open(path, "r", encoding="utf-8") as fh:
                rec = json.load(fh)
            rec["_path"] = os.path.join(os.path.basename(records_dir), name)
            records.append(rec)
        except Exception as exc:  # noqa: BLE001
            malformed.append({"path": path, "error": str(exc)})
    return records, malformed


def load_freeze(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def _repo_rel(path: str | None) -> str | None:
    """Repo-relative POSIX path for committed analysis artefacts (path hygiene)."""
    if not path:
        return None
    try:
        rel = os.path.relpath(os.path.abspath(path),
                              os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    except (OSError, ValueError):
        return "<scratch>/" + os.path.basename(path)
    if rel.startswith(".."):
        return "<scratch>/" + os.path.basename(path)
    return rel.replace(os.sep, "/")


def load_reserved(records_dir: str) -> list[dict]:
    """Load the frozen reserved-run list emitted by the runner (F-P5 n_planned source)."""
    summaries = os.path.join(os.path.dirname(records_dir), "summaries")
    path = os.path.join(summaries, "reserved-runs.csv")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def dataset_label(records: list[dict], freeze: dict) -> str:
    if freeze.get("dataset"):
        return str(freeze["dataset"])
    for rec in records:
        for note in (rec.get("validity") or {}).get("notes", []) or []:
            if "SYNTHETIC FIXTURE" in note.upper():
                return DATASET_SYNTHETIC
    return DATASET_CAMPAIGN


def extract(rec: dict) -> dict:
    validity = rec.get("validity") or {}
    violations = rec.get("violations") or {}
    counts = rec.get("counts") or {}
    perf = rec.get("performance") or {}
    quiescence = rec.get("quiescence") or {}
    receipts = rec.get("fault_receipts") or []
    injected = (rec.get("fault_level") in ("L1", "L2")
                or rec.get("fault") not in ("F0", "none", "ctrl", None))
    attested = True
    if injected:
        fired = [r for r in receipts if r.get("event") == "fired"]
        restored = [r for r in receipts if r.get("event") == "restored"]
        armed = [r for r in receipts if r.get("event") == "armed"]
        if any(r.get("detail", "").startswith("sham") for r in receipts) or armed:
            attested = len(armed) == 1 and len(fired) == 0
        elif (rec.get("fault_level") == "L1") or rec.get("fault") == "F4b":
            # app-owned failpoint: app fired (terminal crash: S4 restored after restart)
            attested = len(fired) == 1
        else:
            attested = len(fired) == 1 and len(restored) == 1
    censored = bool(quiescence.get("censored"))
    raw_violating = bool(violations.get("violating_run"))
    cell_id = rec.get("cell_id") or ""
    return {
        "run_id": rec.get("run_id"), "stage": rec.get("stage"),
        "cell_id": cell_id, "mechanism": rec.get("mechanism"),
        "is_validation": cell_id.startswith("V."),
        "ordering_variant": rec.get("ordering_variant"), "fault": rec.get("fault"),
        "fault_level": rec.get("fault_level"),
        "load_events": rec.get("load_events"), "partitions": rec.get("partitions"),
        "consumer_instances": rec.get("consumer_instances"), "rep": rec.get("rep"),
        "valid": bool(validity.get("valid")), "excluded": bool(validity.get("excluded")),
        "exclude_reason": validity.get("exclude_reason"),
        # method §7.3: censored runs are included in the correctness primary outcome as
        # violating runs (S5 F-P5 fix). The raw oracle verdict is kept separately.
        "violating_run": bool(raw_violating or censored),
        "violating_run_raw": raw_violating,
        "censored": censored,
        "v1": bool(violations.get("v1")), "v2": bool(violations.get("v2")),
        "v3": bool(violations.get("v3")), "v4": bool(violations.get("v4")),
        "v5": bool(violations.get("v5")),
        "dup_postings": counts.get("dup_postings"),
        "dup_entries": counts.get("dup_entries"),
        "lost_postings": counts.get("lost_postings"),
        "lost_entries": counts.get("lost_entries"),
        "imbalanced_entries": counts.get("imbalanced_entries"),
        "structurally_malformed_entries": counts.get("structurally_malformed_entries"),
        "imbalanced_sum_entries": counts.get("imbalanced_sum_entries"),
        "mutated_postings": counts.get("mutated_postings"),
        "spurious_postings": counts.get("spurious_postings"),
        "throughput": perf.get("throughput_eps_median"),
        "p50": perf.get("p50_ms"), "p99": perf.get("p99_ms"),
        "p999": perf.get("p999_ms"),
        "t_consistent_ms": quiescence.get("t_consistent_ms"),
        "injected": injected, "attested": attested,
        "config_sha256": (rec.get("config") or {}).get("config_sha256"),
        "config": rec.get("config") or {},
        "source_integrity": (rec.get("oracle") or {}).get("source_integrity"),
        "gt_sha256": (rec.get("oracle") or {}).get("gt_sha256"),
        "expected_sha256": (rec.get("oracle") or {}).get("expected_sha256"),
        "clock_check": bool(validity.get("clock_check")),
        "residue_check": bool(validity.get("residue_check")),
        "disk_free_start_gb": validity.get("disk_free_start_gb"),
        "publication": rec.get("publication") or {},
        "histogram_path": perf.get("histogram_path"),
        "notes": validity.get("notes") or [],
        "git_sha": rec.get("git_sha"),
        "data_seed": rec.get("data_seed"), "injection_seed": rec.get("injection_seed"),
    }


def confirmatory(recs: list[dict]) -> list[dict]:
    """Exclude the dedicated validation runs (cell_id ``V.*``) from confirmatory tables.

    prereg §2.3: the validation runs are recorded separately and are not part of the matrix;
    mixing them into mechanism×cell tables would contaminate the estimands (S5 F-P5 item 3).
    """
    return [r for r in recs if not r.get("is_validation")]


def _group_by(records: list[dict], key) -> dict:
    out: dict = defaultdict(list)
    for rec in records:
        out[key(rec)].append(rec)
    return out


# --------------------------------------------------------------------------------------------
# estimator tables
# --------------------------------------------------------------------------------------------
def correctness_by_cell(recs: list[dict], dataset: str,
                        reserved: dict[str, int] | None = None) -> list[dict]:
    """Per-cell correctness with the FROZEN N_planned from the reserved-run list (F-P5).

    ``reserved`` maps cell_id -> reserved run count.  ``n_planned`` is never redefined to the
    number of surviving records: ``n_missing = max(0, reserved - valid - excluded)``.
    """
    reserved = reserved or {}
    rows = []
    groups = _group_by(recs, lambda r: (r["stage"], r["mechanism"], r["fault"],
                                        r["ordering_variant"], r["load_events"],
                                        r["partitions"]))
    for (stage, mech, fault, ordering, load, parts), group in sorted(groups.items()):
        valid = [r for r in group if r["valid"] and not r["excluded"]]
        excluded = [r for r in group if r["excluded"]]
        n = len(valid)
        k = sum(1 for r in valid if r["violating_run"])
        lo, hi = cp_ci(k, n)
        reasons = Counter(str(r["exclude_reason"]) for r in excluded)
        cell_id = group[0].get("cell_id")
        planned = reserved.get(cell_id, len(group)) if cell_id else len(group)
        n_missing = max(0, int(planned) - n - len(excluded))
        row = {
            "dataset": dataset, "cell_id": cell_id, "stage": stage, "mechanism": mech,
            "fault": fault, "ordering_variant": ordering, "load_events": load,
            "partitions": parts,
            "n_planned": int(planned),
            "n_planned_source": ("reserved-runs.csv" if cell_id in reserved
                                 else "records-only (reserved list unavailable)"),
            "n_valid": n, "n_excluded": len(excluded), "n_missing": n_missing,
            "exclusion_rate": (len(excluded) / (n + len(excluded)))
            if (n + len(excluded)) else None,
            "violating_runs": k, "violating_rate": (k / n) if n else None,
            "cp_lo": lo, "cp_hi": hi, "cp_upper_one_sided": cp_upper(k, n),
            "rule_of_three": rule_of_three(n),
            "exclude_reasons": "; ".join(f"{a} x{b}" for a, b in sorted(reasons.items())),
        }
        for inv in ("v1", "v2", "v3", "v4", "v5"):
            kk = sum(1 for r in valid if r[inv])
            row[f"{inv}_runs"] = kk
            row[f"{inv}_rate"] = (kk / n) if n else None
            lo_i, hi_i = cp_ci(kk, n)
            row[f"{inv}_cp_lo"], row[f"{inv}_cp_hi"] = lo_i, hi_i
        rows.append(row)
    return rows


def correctness_by_mechanism_fault(recs: list[dict], dataset: str) -> list[dict]:
    """T2: mechanism × fault-class violating runs / N + one-sided 95 % upper bound."""
    rows = []
    groups = _group_by(recs, lambda r: (r["mechanism"], r["fault"]))
    for (mech, fault), group in sorted(groups.items()):
        valid = [r for r in group if r["valid"] and not r["excluded"]]
        excluded = [r for r in group if r["excluded"]]
        n = len(valid)
        k = sum(1 for r in valid if r["violating_run"])
        rows.append({
            "dataset": dataset, "mechanism": mech, "fault": fault,
            "n_valid": n, "n_excluded": len(excluded), "violating_runs": k,
            "violating_rate": (k / n) if n else None,
            "cp_upper_one_sided": cp_upper(k, n), "rule_of_three": rule_of_three(n),
            "cp_lo": cp_ci(k, n)[0], "cp_hi": cp_ci(k, n)[1],
            "stages": ",".join(sorted({r["stage"] or "" for r in valid})),
            "injections_attested": all(r["attested"] for r in valid) if valid else None,
        })
    return rows


def counts_by_mechanism(recs: list[dict], dataset: str) -> list[dict]:
    """T3: duplicate/loss/invariant count distributions by mechanism."""
    rows = []
    metrics = ["dup_postings", "dup_entries", "lost_postings", "lost_entries",
               "imbalanced_entries", "mutated_postings", "spurious_postings"]
    groups = _group_by(recs, lambda r: r["mechanism"])
    for mech, group in sorted(groups.items()):
        valid = [r for r in group if r["valid"] and not r["excluded"]]
        row = {"dataset": dataset, "mechanism": mech, "n_valid": len(valid)}
        for m in metrics:
            vals = sorted(v for v in (r[m] for r in valid) if isinstance(v, (int, float)))
            if vals:
                arr = np.asarray(vals, dtype=float)
                row[f"{m}_median"] = float(np.median(arr))
                row[f"{m}_q1"] = float(np.percentile(arr, 25))
                row[f"{m}_q3"] = float(np.percentile(arr, 75))
                row[f"{m}_runs_gt0"] = int(np.sum(arr > 0))
                row[f"{m}_max"] = float(arr.max())
            else:
                for suffix in ("median", "q1", "q3", "runs_gt0", "max"):
                    row[f"{m}_{suffix}"] = None
        rows.append(row)
    return rows


def performance_by_cell(recs: list[dict], dataset: str, bootstrap_seed: str) -> list[dict]:
    """T4: per-cell performance medians/IQR + bootstrap CI (fault-free, fixed-rate)."""
    rows = []
    groups = _group_by(recs, lambda r: (r["stage"], r["mechanism"], r["load_events"],
                                        r["partitions"], r["fault"]))
    for (stage, mech, load, parts, fault), group in sorted(groups.items()):
        valid = [r for r in group if r["valid"] and not r["excluded"]]
        row = {"dataset": dataset, "stage": stage, "mechanism": mech, "load_events": load,
               "partitions": parts, "fault": fault, "n_valid": len(valid)}
        for metric in ("throughput", "p50", "p99", "p999"):
            vals = [r[metric] for r in valid if isinstance(r[metric], (int, float))]
            ci = bootstrap_median_ci(vals, bootstrap_seed,
                                     f"perf|{stage}|{mech}|{load}|{parts}|{fault}|{metric}")
            row[f"{metric}_n"] = ci["n"]
            row[f"{metric}_median"] = ci["median"]
            row[f"{metric}_lo"] = ci["lo"]
            row[f"{metric}_hi"] = ci["hi"]
            row[f"{metric}_bca_lo"] = ci["bca_lo"]
            row[f"{metric}_bca_hi"] = ci["bca_hi"]
            if vals:
                arr = np.asarray(vals, dtype=float)
                row[f"{metric}_q1"] = float(np.percentile(arr, 25))
                row[f"{metric}_q3"] = float(np.percentile(arr, 75))
            else:
                row[f"{metric}_q1"] = row[f"{metric}_q3"] = None
        tt = [r["t_consistent_ms"] for r in valid
              if isinstance(r["t_consistent_ms"], (int, float)) and not r["censored"]]
        row["n_consistent"] = len(tt)
        row["n_censored"] = sum(1 for r in valid if r["censored"])
        ci_t = bootstrap_median_ci(tt, bootstrap_seed,
                                   f"ttc|{stage}|{mech}|{load}|{parts}|{fault}")
        row["t_consistent_median_ms"] = ci_t["median"]
        row["t_consistent_lo_ms"] = ci_t["lo"]
        row["t_consistent_hi_ms"] = ci_t["hi"]
        rows.append(row)
    return rows


def per_run_performance(recs: list[dict], dataset: str) -> list[dict]:
    """Run-level performance rows (the resampling unit) for figures/tables."""
    rows = []
    for r in sorted(recs, key=lambda x: (x["stage"] or "", x["mechanism"] or "",
                                         x["run_id"] or "")):
        rows.append({
            "dataset": dataset, "run_id": r["run_id"], "stage": r["stage"],
            "mechanism": r["mechanism"], "fault": r["fault"],
            "load_events": r["load_events"], "partitions": r["partitions"],
            "rep": r["rep"], "valid": r["valid"], "excluded": r["excluded"],
            "violating_run": r["violating_run"],
            "throughput": r["throughput"], "p50_ms": r["p50"], "p99_ms": r["p99"],
            "p999_ms": r["p999"],
            "n_censored": 1 if r["censored"] else 0,
            "t_consistent_ms": r["t_consistent_ms"],
        })
    return rows


# --------------------------------------------------------------------------------------------
# hypothesis tests (prereg §3)
# --------------------------------------------------------------------------------------------
def _paired_pairs(recs: list[dict], mech_a: str, mech_b: str, *, conditions: set | None = None,
                  stages: set | None = None, field: str = "violating_run"):
    """Pair runs by (stage, fault, load, partitions, rep) and return pairs for McNemar."""
    index: dict[tuple, dict] = {}
    for r in recs:
        if not (r["valid"] and not r["excluded"]):
            continue
        if mech_a not in (r["mechanism"], mech_b):
            continue
        if conditions is not None and r["fault"] not in conditions:
            continue
        if stages is not None and r["stage"] not in stages:
            continue
        key = (r["stage"], r["fault"], r["load_events"], r["partitions"], r["ordering_variant"],
               r["rep"])
        index.setdefault(key, {})[r["mechanism"]] = r
    pairs = []
    for key, by_mech in sorted(index.items()):
        if mech_a in by_mech and mech_b in by_mech:
            pairs.append((bool(by_mech[mech_a][field]), bool(by_mech[mech_b][field])))
    return pairs


def _mcnemar_for(recs, a, b, conditions, stages, field="violating_run") -> dict:
    pairs = _paired_pairs(recs, a, b, conditions=conditions, stages=stages, field=field)
    res = mcnemar_exact(pairs)
    res["n_pairs"] = len(pairs)
    res["comparison"] = f"{a} vs {b}"
    return res


def decide_hypotheses(recs: list[dict], tests: dict, dataset: str) -> dict:
    """prereg §3 decision rules, evaluated exactly (confirmed / refuted / inconclusive)."""
    valid = [r for r in recs if r["valid"] and not r["excluded"]]
    out: dict = {"dataset": dataset, "alpha": ALPHA}

    def cell_runs(mech, conditions, stages=None):
        return [r for r in valid if r["mechanism"] == mech
                and r["fault"] in conditions
                and (stages is None or r["stage"] in stages)]

    # ---------------- H1: M1x violates, M1-kafka does not --------------------------------
    m1x_bad = cell_runs("M1x", {"K2", "K3"}, {"A"})
    # H1's clean arm: the M1-kafka cells named by the decision rule (Stage A/B F0-F3,F5-F6
    # reachable set); exploratory cells are not allowed to affect the hypothesis (F-P5 item 6).
    mk_all = cell_runs("M1-kafka", {"F0", "F1", "F2", "F3", "F6", "K1", "K2", "K3", "K5"},
                       {"A", "B"})
    m1x_viol = [r for r in m1x_bad if r["violating_run"]]
    mk_viol = [r for r in mk_all if r["violating_run"]]
    k3_test = tests["families"].get("C1", {}).get("tests", {}).get(
        "stageA_K3_M1x_vs_M1-kafka")
    sig = bool(k3_test and k3_test.get("p_adjusted") is not None
               and k3_test["p_adjusted"] < ALPHA
               and (k3_test.get("b", 0) + k3_test.get("c", 0)) > 0)
    attested = all(r["attested"] for r in m1x_bad) if m1x_bad else False
    if not m1x_bad or not mk_all:
        h1 = "inconclusive"
        h1_why = "no data for the M1x K2/K3 cells or the M1-kafka control cells"
    elif mk_viol:
        h1 = "refuted"
        h1_why = (f"M1-kafka shows {len(mk_viol)} violating run(s) (also fails gate G2, halts "
                  "the campaign)")
    elif not attested:
        h1 = "inconclusive"
        h1_why = "K2/K3 injections not reliably attested"
    elif m1x_viol and k3_test and sig:
        h1 = "confirmed"
        h1_why = (f"M1x has {len(m1x_viol)} violating run(s) in K2/K3 with attested injection; "
                  f"M1-kafka clean; K3 McNemar p_adj={fmt_p(k3_test.get('p_adjusted'))}")
    elif m1x_viol and k3_test:
        h1 = "inconclusive"
        h1_why = (f"M1x violates but the K3 comparison is not significant "
                  f"(p_adj={fmt_p(k3_test.get('p_adjusted'))})")
    elif not m1x_viol and attested:
        h1 = "refuted"
        h1_why = "M1x is clean in all K2/K3 cells with attested injections"
    else:
        h1 = "inconclusive"
        h1_why = "insufficient evidence"
    out["H1"] = {"verdict": h1, "reason": h1_why,
                 "evidence": {"m1x_k2k3_runs": len(m1x_bad),
                              "m1x_k2k3_violating": len(m1x_viol),
                              "m1kafka_runs": len(mk_all), "m1kafka_violating": len(mk_viol),
                              "k3_mcnemar": k3_test}}

    # ---------------- H2: M2 sufficient with cost --------------------------------------
    m2_cells = [r for r in valid if r["mechanism"] == "M2"
                and (r["stage"] in ("A", "B") or r["fault"] not in ("F0", "none", "ctrl"))]
    m2_viol = [r for r in m2_cells if r["violating_run"]]
    cost = tests["families"].get("H2-cost", {})
    cost_conf = bool(cost.get("confirmed"))
    if not m2_cells:
        h2 = "inconclusive"
        h2_why = "no M2 correctness data"
    elif m2_viol:
        h2 = "refuted"
        h2_why = f"M2 has {len(m2_viol)} violating run(s) with attested injection"
    elif cost_conf:
        h2 = "confirmed"
        h2_why = "M2 clean in executed Stage A/B cells; pre-registered cost contrast confirmed"
    elif cost.get("tests"):
        h2 = "partially confirmed"
        h2_why = ("correctness component confirmed (0 violating runs) but the pre-registered "
                  "cost component was not confirmed")
    else:
        h2 = "partially confirmed"
        h2_why = ("correctness component confirmed (0 violating runs); cost component not "
                  "assessable (no Stage C data)")
    out["H2"] = {"verdict": h2, "reason": h2_why,
                 "evidence": {"m2_runs": len(m2_cells), "m2_violating": len(m2_viol),
                              "cost_component": cost}}

    # ---------------- H3: M3 zero duplicates, loses under ordering probe ---------------
    m3_all = [r for r in valid if r["mechanism"] == "M3"]
    m3_i3 = [r for r in m3_all if r["v3"]]
    early = [r for r in m3_all if r["ordering_variant"] == "early"
             and r["fault"] in ("K4", "F4b")]
    early_loss = [r for r in early if r["v4"]]
    c2 = tests["families"].get("C2", {})
    c2_tests = c2.get("tests", {})
    c2_sig = any(t.get("p_adjusted") is not None and t["p_adjusted"] < ALPHA
                 and (t.get("b", 0) + t.get("c", 0)) > 0 for t in c2_tests.values())
    early_attested = all(r["attested"] for r in early) if early else False
    if not m3_all:
        h3 = "inconclusive"
        h3_why = "no M3 data"
    elif m3_i3:
        h3 = "refuted"
        h3_why = f"M3 shows {len(m3_i3)} I3-violating run(s) (duplicate arm refuted)"
    elif not early:
        h3 = "inconclusive"
        h3_why = "ordering probe not run (loss arm untested; D2)"
    elif early_loss and early_attested and c2_sig:
        h3 = "confirmed"
        h3_why = (f"M3 clean of I3; {len(early_loss)} I4 loss run(s) on the early-ordering "
                  f"probe with attested injection; C2 McNemar significant")
    elif early_loss and early_attested:
        h3 = "inconclusive"
        h3_why = (f"M3 shows {len(early_loss)} loss run(s) on the probe but the C2 comparison "
                  "is not significant")
    elif early_attested:
        h3 = "refuted"
        h3_why = ("M3 shows no I4 violation at N with an attested early-ordering probe; the "
                  "design could not demonstrate the predicted loss")
    else:
        h3 = "inconclusive"
        h3_why = "probe injection not reliably attested"
    out["H3"] = {"verdict": h3, "reason": h3_why,
                 "evidence": {"m3_runs": len(m3_all), "m3_i3_violating": len(m3_i3),
                              "early_probe_runs": len(early),
                              "early_probe_loss_runs": len(early_loss),
                              "c2": c2}}

    # ---------------- H4: M4/M4+M3 correctness + latency vs M2 -------------------------
    def reachable_zero(mech):
        runs = [r for r in valid if r["mechanism"] == mech and r["stage"] in ("A", "B")]
        return runs, [r for r in runs if r["violating_run"]]

    m4_runs, m4_bad = reachable_zero("M4")
    m43_runs, m43_bad = reachable_zero("M4+M3")
    lat = tests["families"].get("H4-latency", {})
    lat_conf = bool(lat.get("confirmed"))
    if not (m4_runs or m43_runs):
        h4 = "inconclusive"
        h4_why = "no M4/M4+M3 correctness data"
    elif m4_bad or m43_bad:
        h4 = "refuted"
        h4_why = (f"correctness component fails (M4 violating={len(m4_bad)}, "
                  f"M4+M3 violating={len(m43_bad)})")
    elif lat_conf:
        h4 = "confirmed"
        h4_why = "M4/M4+M3 clean in reachable cells; pre-registered latency contrast confirmed"
    elif lat.get("tests"):
        h4 = "partially confirmed"
        h4_why = "correctness confirmed; latency component not confirmed"
    else:
        h4 = "partially confirmed"
        h4_why = "correctness confirmed; latency component not assessable (no Stage C data)"
    out["H4"] = {"verdict": h4, "reason": h4_why,
                 "evidence": {"m4_runs": len(m4_runs), "m4_violating": len(m4_bad),
                              "m4m3_runs": len(m43_runs), "m4m3_violating": len(m43_bad),
                              "latency_component": lat}}

    # ---------------- H5: no single mechanism dominates --------------------------------
    h5 = tests["families"].get("H5-dominance", {})
    dom = h5.get("dominant_mechanisms")
    if h5.get("applicable") is False:
        out["H5"] = {"verdict": "inconclusive", "reason": h5.get("reason", "not assessable"),
                     "evidence": h5}
    elif dom:
        out["H5"] = {"verdict": "refuted",
                     "reason": f"mechanism(s) satisfy both dominance clauses: {', '.join(dom)}",
                     "evidence": h5}
    else:
        out["H5"] = {"verdict": "confirmed",
                     "reason": "no mechanism satisfies both dominance clauses",
                     "evidence": h5}
    return out


# --------------------------------------------------------------------------------------------
# test families
# --------------------------------------------------------------------------------------------
def build_test_families(recs: list[dict], bootstrap_seed: str) -> dict:
    valid = [r for r in recs if r["valid"] and not r["excluded"]]
    families: dict = {}

    # ---- C1: H1 M1x vs M1-kafka (Stage A K3 + Stage B per-fault) ----------------------
    c1_tests: dict[str, dict] = {}
    t = _mcnemar_for(valid, "M1x", "M1-kafka", {"K3"}, {"A"})
    c1_tests["stageA_K3_M1x_vs_M1-kafka"] = {"family": "C1", **t}
    for fault in ("F0", "F1", "F2", "F3", "F6"):
        tt = _mcnemar_for(valid, "M1x", "M1-kafka", {fault}, {"B"})
        c1_tests[f"stageB_{fault}_M1x_vs_M1-kafka"] = {"family": "C1", **tt}
    c1_adj = holm({k: v.get("p") for k, v in c1_tests.items()})
    for k, v in c1_tests.items():
        v["p_adjusted"] = c1_adj[k]
    families["C1"] = {"description": "H1: M1x vs M1-kafka (McNemar exact, Holm)",
                      "tests": c1_tests}

    # ---- C2: H3 M3-early vs M3-strict on I4 ------------------------------------------
    c2_tests: dict[str, dict] = {}

    def _paired_field(mech_a, mech_b, field, conditions, stages, condition_a=None):
        index: dict[tuple, dict] = {}
        for r in valid:
            if r["mechanism"] not in (mech_a, mech_b):
                continue
            if r["fault"] not in conditions:
                continue
            if stages and r["stage"] not in stages:
                continue
            key = (r["stage"], r["fault"], r["load_events"], r["partitions"], r["rep"])
            index.setdefault(key, {})[r["mechanism"]] = r
        pairs = []
        for key, by_mech in sorted(index.items()):
            if mech_a in by_mech and mech_b in by_mech:
                pairs.append((bool(by_mech[mech_a][field]), bool(by_mech[mech_b][field])))
        return mcnemar_exact(pairs), len(pairs)

    res, n = _paired_field("M3", "M3", "v4", {"K4"}, {"A"})
    if n == 0:
        # compare M3 K4 (early) against M3 control (strict): pair by rep across cells
        early = {r["rep"]: r for r in valid if r["mechanism"] == "M3" and r["fault"] == "K4"}
        ctrl = {r["rep"]: r for r in valid if r["mechanism"] == "M3" and r["fault"] == "none"
                and r.get("cell_id", "").endswith(".ctrl.1k.p3")}
        pairs = [(bool(ctrl[i]["v4"]), bool(early[i]["v4"])) for i in sorted(early)
                 if i in ctrl]
        res = mcnemar_exact(pairs)
        n = len(pairs)
    res["note"] = "M3 early (K4) vs M3 strict (control), paired by rep"
    c2_tests["stageA_K4_M3_early_vs_strict"] = {"family": "C2", **res, "n_pairs": n}
    res2, n2 = _paired_field("M3", "M3", "v4", {"F4b"}, {"B"})
    if n2:
        c2_tests["stageB_F4b_M3_early_vs_strict"] = {"family": "C2", **res2, "n_pairs": n2}
    c2_adj = holm({k: v.get("p") for k, v in c2_tests.items()})
    for k, v in c2_tests.items():
        v["p_adjusted"] = c2_adj[k]
    families["C2"] = {"description": "H3: M3-early vs M3-strict on I4 (McNemar, Holm)",
                      "tests": c2_tests}

    # ---- C3: H4 correctness comparisons vs M1x (poison control) -----------------------
    c3_tests = {}
    for mech in ("M4", "M4+M3"):
        tt = _mcnemar_for(valid, mech, "M1x", None, {"A", "B"})
        c3_tests[f"{mech}_vs_M1x"] = {"family": "C3", **tt}
    c3_adj = holm({k: v.get("p") for k, v in c3_tests.items()})
    for k, v in c3_tests.items():
        v["p_adjusted"] = c3_adj[k]
    families["C3"] = {"description": "H4: M4/M4+M3 vs M1x correctness (McNemar, Holm)",
                      "tests": c3_tests}

    # ---- performance families P1/P2: block medians from Stage C ----------------------
    stage_c = [r for r in valid if r["stage"] == "C" and r["fault"] in ("F0", "none", "ctrl")]
    blocks = {}
    for (load, parts) in PERF_BLOCKS:
        block_recs = [r for r in stage_c if r["load_events"] == load
                      and r["partitions"] == parts]
        by_mech = _group_by(block_recs, lambda r: r["mechanism"])
        med: dict[str, list] = {}
        for mech, runs in sorted(by_mech.items()):
            thr = [r["throughput"] for r in runs if isinstance(r["throughput"], (int, float))]
            p99 = [r["p99"] for r in runs if isinstance(r["p99"], (int, float))]
            med.setdefault(mech, [])
            med[mech] = {"throughput": float(np.median(thr)) if thr else None,
                         "p99": float(np.median(p99)) if p99 else None}
        blocks[(load, parts)] = med
    mechs_with_data = sorted({m for blk in blocks.values() for m in blk})
    for family, metric in (("P1", "throughput"), ("P2", "p99")):
        block_med: dict[str, list[float]] = {m: [] for m in mechs_with_data}
        used_blocks = []
        for (load, parts), med in sorted(blocks.items()):
            if not med:
                continue
            if not all(m in med and med[m][metric] is not None for m in mechs_with_data):
                continue
            used_blocks.append({"load_events": load, "partitions": parts})
            for m in mechs_with_data:
                block_med[m].append(float(med[m][metric]))
        fried = friedman_block_test(block_med) if used_blocks else {
            "applicable": False, "reason": "no complete Stage C performance blocks"}
        # pairwise: Wilcoxon signed-rank on block medians + Cliff's delta + Holm
        pairwise = []
        if fried.get("applicable"):
            for i, a in enumerate(mechs_with_data):
                for b in mechs_with_data[i + 1:]:
                    wx = paired_wilcoxon(block_med[a], block_med[b])
                    cd = cliffs_delta(block_med[a], block_med[b], paired=True,
                                      bootstrap_seed=bootstrap_seed, label=f"{family}|{a}|{b}")
                    pairwise.append({"a": a, "b": b, **wx,
                                     "cliff": cd,
                                     "n_blocks": len(block_med[a])})
            p_adj = holm({f"{p['a']}|{p['b']}": p.get("p") for p in pairwise})
            for p in pairwise:
                p["p_adjusted"] = p_adj.get(f"{p['a']}|{p['b']}")
        families[family] = {
            "description": f"{'Throughput' if metric == 'throughput' else 'p99 latency'} "
                           "block-median family (Friedman + Nemenyi gate; Wilcoxon pairwise "
                           "Holm; Cliff's delta with CI)",
            "metric": metric, "blocks": used_blocks, "friedman": fried,
            "pairwise": pairwise, "alpha": ALPHA,
        }

    # ---- H2 cost / H4 latency components ---------------------------------------------
    def _cost_confirmed(family: str, better: str, worse: str,
                        direction: int = -1) -> dict:
        fam = families.get(family, {})
        for p in fam.get("pairwise", []):
            if p.get("a") == better and p.get("b") == worse:
                cd = p.get("cliff") or {}
                delta = cd.get("delta")
                sig = p.get("p_adjusted") is not None and p["p_adjusted"] < ALPHA
                # direction-aware (F-P5): the sign must match the pre-registered direction.
                # direction=-1 => better < worse in the metric; +1 => better > worse.
                fam_blocks = fam.get("blocks") or []
                metric = fam.get("metric")
                sign_ok = None
                if metric and fam_blocks:
                    key = "throughput" if metric == "throughput" else "p99"
                    med_a = [b.get("median", {}).get(key) if isinstance(b.get("median"), dict)
                             else None for b in fam_blocks]
                    sign_ok = True  # block medians recorded in pairwise cliff delta sign
                ok = (delta is not None and abs(delta) >= CLIFF_SMALL and sig
                      and (delta * direction) > 0)
                return {"family": family, "comparison": f"{better} vs {worse}",
                        "cliff_delta": delta, "magnitude": cd.get("magnitude"),
                        "direction_expected": direction, "direction_ok": bool(
                            delta is not None and delta * direction > 0),
                        "p_adjusted": p.get("p_adjusted"), "confirmed": bool(ok),
                        "note": f"pre-registered direction: {better} {'worse than' if direction < 0 else 'better than'} {worse}"}
        return {"family": family, "confirmed": False,
                "note": "no Stage C data for the pre-registered contrast"}

    h2_tests = {}
    for other in ("M3", "M4"):
        # p99: M2 worse => higher => direction +1; throughput: M2 worse => lower => -1
        h2_tests[f"M2_vs_{other}_p99"] = _cost_confirmed("P2", "M2", other, direction=1)
        h2_tests[f"M2_vs_{other}_throughput"] = _cost_confirmed("P1", "M2", other,
                                                                direction=-1)
    families["H2-cost"] = {
        "description": "H2 cost component: M2 worse than M3 or M4 on throughput or p99 with "
                       "|delta| >= 0.33 (small) and Holm p < 0.05",
        "tests": h2_tests,
        "confirmed": any(t.get("confirmed") for t in h2_tests.values()),
    }
    h4_tests = {}
    for load in (10_000, 100_000):
        for p in (6,):
            pass
    # H4 latency: M4 p99 lower than M2 at P=6, either load (family P2 direction reversed)
    for other in ("M2",):
        # H4: M4 p99 lower than M2 => direction -1; throughput non-inferiority is H5's job
        h4_tests["M4_vs_M2_p99"] = _cost_confirmed("P2", "M4", other, direction=-1)
    families["H4-latency"] = {
        "description": "H4 latency component: M4 median p99 (and p50 secondary) lower than M2 "
                       "in at least one Stage C load at P=6 with |delta| >= 0.33 and Holm p < .05",
        "tests": h4_tests,
        "confirmed": any(t.get("confirmed") for t in h4_tests.values()),
    }

    # ---- P3: time-to-consistent-state ------------------------------------------------
    p3_tests = {}
    # prereg §7.4 is per cell; the confirmatory contrast uses the fault-free Stage-C
    # 100k/P=6 cell (S5 F-P5 item 5: do not pool fault conditions across stages)
    ttc = [r for r in valid if r["load_events"] == 100_000 and r["partitions"] == 6
           and r["stage"] == "C" and r["fault"] in ("F0", "none", "ctrl")]
    by_mech = _group_by(ttc, lambda r: r["mechanism"])
    props = {}
    for mech, runs in sorted(by_mech.items()):
        n = len(runs)
        k = sum(1 for r in runs if not r["censored"])
        props[mech] = {"n": n, "consistent": k, "rate": k / n if n else None,
                       "cp_lo": cp_ci(k, n)[0] if n else None,
                       "cp_hi": cp_ci(k, n)[1] if n else None}
    mechs3 = sorted(props)
    for i, a in enumerate(mechs3):
        for b in mechs3[i + 1:]:
            pa, pb = props[a], props[b]
            if pa["n"] == 0 or pb["n"] == 0:
                continue
            table = [[pa["consistent"], pa["n"] - pa["consistent"]],
                     [pb["consistent"], pb["n"] - pb["consistent"]]]
            ft = fisher_test(table)
            p3_tests[f"{a}_vs_{b}"] = {"family": "P3", **ft,
                                       "n_a": pa["n"], "n_b": pb["n"]}
    p3_adj = holm({k: v.get("p") for k, v in p3_tests.items()})
    for k, v in p3_tests.items():
        v["p_adjusted"] = p3_adj[k]
    families["P3"] = {"description": "Time-to-consistent-state proportions at 100k/P=6 "
                                     "(Fisher exact, Holm)",
                      "proportions": props, "tests": p3_tests}

    # ---- H5 dominance ----------------------------------------------------------------
    dom_block = {}
    if families.get("P1", {}).get("friedman", {}).get("applicable") and \
            len(families.get("P1", {}).get("blocks", [])) >= 4:
        mechs_dom = mechs_with_data
        worst_correct = []
        for mech in mechs_dom:
            mech_runs = [r for r in valid if r["mechanism"] == mech and r["stage"] in ("A", "B")]
            if not mech_runs:
                worst_correct.append((mech, False))
                continue
            zero_viol = not any(r["violating_run"] for r in mech_runs)
            attested = all(r["attested"] for r in mech_runs if r["injected"])
            worst_correct.append((mech, bool(zero_viol and attested)))
        noninf = {}
        for mech in mechs_dom:
            ok_blocks = 0
            for (load, parts), med in sorted(blocks.items()):
                if mech not in med:
                    continue
                others = [m for m in med if m != mech and med[m]["throughput"] is not None]
                if not others:
                    continue
                best_other = max(others, key=lambda m: med[m]["throughput"])
                key = f"{load_events_key(load)}|p{parts}"
                mine = []
                theirs = []
                block_recs = [r for r in stage_c if r["load_events"] == load
                              and r["partitions"] == parts and r["mechanism"] in (mech, best_other)]
                bym = _group_by(block_recs, lambda r: r["mechanism"])
                mine = [r["throughput"] for r in bym.get(mech, [])
                        if isinstance(r["throughput"], (int, float))]
                theirs = [r["throughput"] for r in bym.get(best_other, [])
                          if isinstance(r["throughput"], (int, float))]
                if not mine or not theirs:
                    continue
                cd = cliffs_delta(mine, theirs, paired=False, bootstrap_seed=bootstrap_seed,
                                  label=f"H5|{mech}|{load}|{parts}")
                ratio_ci = bootstrap_median_ratio(mine, theirs, bootstrap_seed,
                                                  f"H5ratio|{mech}|{load}|{parts}")
                non_inf = (cd.get("delta") is not None
                           and abs(cd["delta"]) < CLIFF_NEGLIGIBLE
                           and ratio_ci.get("ci_contains_one"))
                if non_inf:
                    ok_blocks += 1
            p99_ok = 0
            for (load, parts), med in sorted(blocks.items()):
                if mech not in med or med[mech]["p99"] is None:
                    continue
                others = [m for m in med if m != mech and med[m]["p99"] is not None]
                if not others:
                    continue
                best_other = min(others, key=lambda m: med[m]["p99"])
                block_recs = [r for r in stage_c if r["load_events"] == load
                              and r["partitions"] == parts and r["mechanism"] in (mech, best_other)]
                bym = _group_by(block_recs, lambda r: r["mechanism"])
                mine = [r["p99"] for r in bym.get(mech, []) if isinstance(r["p99"], (int, float))]
                theirs = [r["p99"] for r in bym.get(best_other, [])
                          if isinstance(r["p99"], (int, float))]
                if not mine or not theirs:
                    continue
                cd = cliffs_delta(mine, theirs, paired=False, bootstrap_seed=bootstrap_seed,
                                  label=f"H5p99|{mech}|{load}|{parts}")
                ratio_ci = bootstrap_median_ratio(mine, theirs, bootstrap_seed,
                                                  f"H5p99ratio|{mech}|{load}|{parts}")
                if (cd.get("delta") is not None and abs(cd["delta"]) < CLIFF_NEGLIGIBLE
                        and ratio_ci.get("ci_contains_one")):
                    p99_ok += 1
            noninf[mech] = {"blocks_throughput_non_inferior": ok_blocks,
                            "blocks_p99_non_inferior": p99_ok,
                            "clause_ii": ok_blocks >= 4 and p99_ok >= 4}
        dominant = [m for m, corr in worst_correct if corr and noninf.get(m, {}).get("clause_ii")]
        families["H5-dominance"] = {
            "applicable": True,
            "definition": "mechanism dominates iff (i) zero violating runs in every executed "
                          "correctness cell with attested injections and (ii) non-inferior to the "
                          "best other mechanism in >=4 of 6 Stage C blocks on both throughput and "
                          "p99 (|Cliff delta|<0.147 and bootstrap CI of median ratio contains 1)",
            "correctness_clause_i": dict(worst_correct),
            "non_inferiority_clause_ii": noninf,
            "dominant_mechanisms": dominant,
        }
    else:
        families["H5-dominance"] = {
            "applicable": False,
            "reason": "no complete Stage C performance blocks; dominance clause (ii) not "
                      "assessable",
            "correctness_clause_i": {},
        }
    return families


def load_events_key(load: int) -> str:
    return {"10000": "10k", "100000": "100k", "1000000": "1m"}.get(str(load), f"{load}")


def bootstrap_median_ratio(x, y, bootstrap_seed: str, label: str,
                           b: int = BOOTSTRAP_B) -> dict:
    """Bootstrap CI of median(x)/median(y); flags whether it contains 1 (non-inferiority)."""
    xs = np.asarray([v for v in x if v is not None and np.isfinite(v)], dtype=float)
    ys = np.asarray([v for v in y if v is not None and np.isfinite(v)], dtype=float)
    if xs.size == 0 or ys.size == 0 or np.median(ys) == 0:
        return {"ratio": None, "lo": None, "hi": None, "ci_contains_one": False}
    rng = seeded_rng(bootstrap_seed, f"ratio|{label}")
    ratios = np.empty(b)
    for k in range(b):
        xb = xs[rng.integers(0, xs.size, xs.size)]
        yb = ys[rng.integers(0, ys.size, ys.size)]
        my = np.median(yb)
        ratios[k] = np.median(xb) / my if my != 0 else np.nan
    ratios = ratios[np.isfinite(ratios)]
    if ratios.size == 0:
        return {"ratio": None, "lo": None, "hi": None, "ci_contains_one": False}
    lo, hi = np.percentile(ratios, [100 * ALPHA / 2, 100 * (1 - ALPHA / 2)])
    return {"ratio": float(np.median(xs) / np.median(ys)), "lo": float(lo), "hi": float(hi),
            "ci_contains_one": bool(lo <= 1.0 <= hi), "B": b}


# --------------------------------------------------------------------------------------------
# decision table (prereg §7.7, method §11.2)
# --------------------------------------------------------------------------------------------
def build_decision_table(recs: list[dict], dataset: str) -> list[dict]:
    valid = [r for r in recs if r["valid"] and not r["excluded"]]
    rows = []
    mechs = [m for m in MECHANISMS if any(r["mechanism"] == m for r in valid)]
    exposure_sets = dict(EXPOSURE_CELLS)
    exposure_sets["E1∧E2∧E3∧E4"] = set().union(*[EXPOSURE_CELLS[e] for e in
                                                  ("E1", "E2", "E3", "E4")])
    exposure_sets["E1..E4∧E5"] = exposure_sets["E1∧E2∧E3∧E4"]
    for exposure in EXPOSURE_ORDER:
        conditions = exposure_sets[exposure]
        for mech in mechs:
            if exposure == "E5" or exposure == "E1..E4∧E5":
                pub = [r for r in valid if r["mechanism"] == mech
                       and (r["publication"] or {}).get("pub_records") is not None]
                n = len(pub)
                pub_bad = sum(1 for r in pub
                              if ((r["publication"] or {}).get("dup_records") or 0) > 0
                              or ((r["publication"] or {}).get("lost_records") or 0) > 0)
                excl = [r for r in recs if r["mechanism"] == mech and r["excluded"]]
                n_all = n + len(excl)
                rows.append({
                    "dataset": dataset, "exposure": exposure, "mechanism": mech,
                    "cells_considered": ",".join(sorted(conditions)) or "publication",
                    "n_valid": n, "n_excluded": len(excl),
                    "exclusion_rate": (len(excl) / n_all) if n_all else None,
                    "violating_runs": pub_bad,
                    "cp_upper_one_sided": cp_upper(pub_bad, n) if n else None,
                    "observed_consistent_with_sufficiency": bool(n > 0 and pub_bad == 0
                                                                and (len(excl) / n_all if n_all
                                                                     else 1.0) <= 0.20),
                    "model_minimal": (MODEL_MINIMAL.get(exposure) if exposure in
                                      MODEL_MINIMAL else None),
                    "theta": 0.01,
                    "residual_rate_resolved": (cp_upper(pub_bad, n) if n else None),
                    "note": "publication-integrity exposure (secondary metrics; method §2.7)",
                })
                continue
            cell_recs = [r for r in valid if r["mechanism"] == mech
                         and r["fault"] in conditions]
            excl = [r for r in recs if r["mechanism"] == mech and r["excluded"]
                    and r["fault"] in conditions]
            n = len(cell_recs)
            bad = sum(1 for r in cell_recs if r["violating_run"])
            n_all = n + len(excl)
            excl_rate = (len(excl) / n_all) if n_all else None
            attested = all(r["attested"] for r in cell_recs if r["injected"])
            consistent = bool(n > 0 and bad == 0 and attested
                              and (excl_rate is not None and excl_rate <= 0.20))
            rows.append({
                "dataset": dataset, "exposure": exposure, "mechanism": mech,
                "cells_considered": ",".join(sorted(conditions)),
                "n_valid": n, "n_excluded": len(excl), "exclusion_rate": excl_rate,
                "violating_runs": bad,
                "cp_upper_one_sided": cp_upper(bad, n) if n else None,
                "observed_consistent_with_sufficiency": consistent,
                "model_minimal": MODEL_MINIMAL.get(exposure),
                "theta": 0.01,
                "residual_rate_resolved": (cp_upper(bad, n) if bad == 0 and n else None),
                "note": ("zero violating runs with attested injections and exclusion rate "
                         "<= 20%" if consistent else
                         ("no data" if n == 0 else "not observed consistent")),
            })
    return rows


# --------------------------------------------------------------------------------------------
# harness gates (T1) — reads frozen gate evidence only
# --------------------------------------------------------------------------------------------
def _reserved_index(reserved: list[dict] | None) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in (reserved or []):
        cell = row.get("cell_id")
        if cell:
            out[cell] = out.get(cell, 0) + 1
    return out


def _gate_negative_controls(harness_dir: str) -> dict:
    path = os.path.join(harness_dir, "gate-negative-controls.json")
    if not os.path.exists(path):
        return {"present": False, "gates": {}}
    try:
        doc = json.load(open(path, encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {"present": False, "gates": {}}
    return {"present": True, "gates": doc.get("gates", {}),
            "all_passed": bool(doc.get("all_passed")),
            "warning": ("SYNTHETIC FIXTURE" if doc.get("synthetic") else None)}


def build_gate_table(recs: list[dict], harness_dir: str,
                     reserved: list[dict] | None = None,
                     faults_log: str | None = None,
                     artifacts_dir: str | None = None,
                     rates_path: str | None = None,
                     negative_controls_required: bool = True) -> list[dict]:
    """Falsifiable harness gates G1-G8 (S5 F-G1 fix).

    Each gate is computed from provenance/records over ALL executed runs (valid and excluded),
    never only from ``valid`` runs.  A gate can only PASS when its positive predicate holds AND a
    purpose-built negative-control fixture for that gate fails it
    (``results/harness/gate-negative-controls.json``).  Gates whose required artefact is not yet
    delivered are reported PENDING with the exact missing evidence, never PASS.
    """
    executed = [r for r in recs if r["valid"] or r["excluded"]]
    valid = [r for r in recs if r["valid"] and not r["excluded"]]
    reserved_idx = _reserved_index(reserved)
    neg = _gate_negative_controls(harness_dir)
    gates: list[dict] = []

    def add(gate: str, requirement: str, status: str, evidence: str):
        if status == "PASS" and negative_controls_required:
            control = (neg.get("gates") or {}).get(gate, {})
            if not neg.get("present"):
                status = "PENDING"
                evidence += "; negative-control fixture set missing (G1-G8 falsifiability " \
                            "not demonstrated)"
            elif not control.get("failed_as_required"):
                status = "FAIL"
                evidence += f"; negative control did not fail the gate " \
                            f"({control.get('reason') or 'missing'})"
            else:
                evidence += "; negative control FAILED as required"
        gates.append({"gate": gate, "requirement": requirement, "status": status,
                      "evidence": evidence,
                      "negative_control": (neg.get("gates") or {}).get(gate)})

    # ---- G1 negative control: M0 shows I3 in deterministic K3 AND K5, per class ----------
    def _g1_class(klass: str):
        runs = [r for r in executed if r["mechanism"] == "M0"
                and r["fault"] == "K3" or (r["mechanism"] == "M0" and r["fault"] == "K5")
                and klass in (r["fault"], "K3" if klass == "K3" else "K5")]
        runs = [r for r in executed if r["mechanism"] == "M0" and r["fault"] == klass]
        good = [r for r in runs if r["v3"] and r["attested"]]
        return len(runs), len(good), runs

    k3_n, k3_good, _ = _g1_class("K3")
    k5_n, k5_good, _ = _g1_class("K5")
    if k3_n == 0 and k5_n == 0:
        add("G1", "M0 negative control shows I3 violations in K3 AND K5 separately with "
                  "attested injection", "PENDING", "no M0 K3/K5 runs yet (gate-control "
                  "validation runs pilot-valid-g1k3/g1k5 are pre-Stage-A)")
    else:
        ok = k3_good > 0 and k5_good > 0
        add("G1", "M0 negative control shows I3 violations in K3 AND K5 separately with "
                  "attested injection", "PASS" if ok else "FAIL",
            f"K3 runs={k3_n} I3+attested={k3_good}; K5 runs={k5_n} I3+attested={k5_good}")

    # ---- G2 positive controls: M1-kafka and M4+M3 clean in fault-free controls -----------
    ctrl = [r for r in executed if r["mechanism"] in ("M1-kafka", "M4+M3")
            and r["fault"] in ("F0", "none", "ctrl")]
    if ctrl:
        bad = [r for r in ctrl if r["violating_run_raw"]]
        add("G2", "M1-kafka and M4+M3 zero violations in all fault-free control runs",
            "PASS" if not bad else "FAIL",
            f"control runs={len(ctrl)} violating={len(bad)} "
            f"CP95 upper={cp_upper(len(bad), len(ctrl)):.4f}")
    else:
        add("G2", "M1-kafka and M4+M3 zero violations in all fault-free control runs",
            "PENDING", "no fault-free control runs yet")

    # ---- G3 checker mutation testing (frozen evidence file) ------------------------------
    g3_path = os.path.join(harness_dir, "g3-mutation.json")
    g3 = None
    if os.path.exists(g3_path):
        try:
            g3 = json.load(open(g3_path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            g3 = None
    if g3:
        corpora = g3.get("corpora") or {}
        required = {"db_sink", "attestation", "publication_metrics", "kafka_ledger"}
        covered = required <= set(corpora)
        per_class_ok = all(c.get("mutations", 0) >= 100
                           for grp in (g3.get("groups") or {}).values()
                           for c in (grp.get("classes") or {}).values())
        ok = bool(g3.get("passed")) and covered and per_class_ok
        add("G3", "oracle mutation testing: 100% detection with correct attribution for every "
                  "class, >=100 mutations/class, all four corpora covered (DB sink, attestation, "
                  "publication metrics, M1-kafka ledger)",
            "PASS" if ok else "FAIL",
            f"g3 passed={g3.get('passed')} corpora={sorted(corpora)} "
            f"classes={g3.get('overview', {}).get('classes')} "
            f"detected={g3.get('overview', {}).get('detected')}/"
            f"{g3.get('overview', {}).get('mutations')}")
    else:
        add("G3", "oracle mutation testing 100% (>=100/class, all corpora)", "PENDING",
            "results/harness/g3-mutation.json not found")

    # ---- G4 injector attestation over ALL reserved injected runs -------------------------
    injected = [r for r in executed if r["injected"]]
    if injected:
        unattested = [r for r in injected if not r["attested"]]
        excluded_receipt = [r for r in injected
                            if r["excluded"] and "fault_receipts_invalid" ==
                            (r.get("exclude_reason") or "")]
        executed_planned = sum(1 for r in executed if r["injected"])
        requested = sum(v for cell, v in reserved_idx.items()
                        if cell.startswith(("A.", "B.", "pilot.")) and
                        not cell.split(".")[2].lower().startswith(("f0", "ctrl")))
        add("G4", "every armed injection has exactly one fired and one restored receipt "
                  "(single-owner protocol); receipt failures excluded and counted",
            "PASS" if not unattested else "FAIL",
            f"executed injected runs={executed_planned} unattested={len(unattested)} "
            f"receipt-excluded={len(excluded_receipt)} "
            f"(reserved injected slots={requested if reserved else 'n/a'})")
    else:
        add("G4", "every armed injection attested (fired+restored, single-owner protocol)",
            "PENDING", "no executed injected runs yet")

    # ---- G5 ground-truth independence over ALL executed runs -----------------------------
    if executed:
        missing = []
        mismatched = []
        for r in executed:
            if r["source_integrity"] is not True:
                missing.append(r["run_id"])
            elif (r["gt_sha256"] and r["expected_sha256"]
                  and r["gt_sha256"] != r["expected_sha256"]):
                mismatched.append(r["run_id"])
        ok = not missing and not mismatched
        add("G5", "GT digest = regenerated digest = source-dump digest for every executed run; "
                  "normative vectors reproduced",
            "PASS" if ok else "FAIL",
            f"executed runs={len(executed)} source-integrity failures={len(missing)} "
            f"digest mismatches={len(mismatched)} (vectors: experiments/tests/"
            f"test_oracle.py asserts docs/derive-vectors.csv + g5-vectors.json)")
    else:
        add("G5", "GT digest = regenerated digest = source-dump digest for every executed run",
            "PENDING", "no executed runs yet")

    # ---- G6 configuration equality across mechanisms (app-emitted shared_sha256) --------
    if artifacts_dir:
        configs = {}
        for cell_dir in glob(os.path.join(artifacts_dir, "*", "config")):
            run_id = os.path.basename(os.path.dirname(cell_dir))
            doc = None
            try:
                doc = json.load(open(cell_dir, encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue
            rec = next((r for r in executed if r["run_id"] == run_id), None)
            if rec is None:
                continue
            configs.setdefault(rec["cell_id"], {})[rec["mechanism"]] = {
                "shared": doc.get("shared_sha256"),
                "intrinsic": doc.get("intrinsic_sha256"),
                "mechanism": doc.get("mechanism"),
            }
        comparable = {cell: vals for cell, vals in configs.items() if len(vals) >= 2}
        bad = []
        for cell, vals in comparable.items():
            shared = {v.get("shared") for v in vals.values()}
            if len(shared) != 1 or None in shared:
                bad.append(cell)
            mechs = {v.get("mechanism") for v in vals.values()}
            if len(mechs) != len(vals):
                bad.append(cell + ":mechanism-key")
        if comparable:
            add("G6", "shared configuration hash identical across mechanisms within a cell; "
                      "only mechanism-intrinsic keys differ",
                "PASS" if not bad else "FAIL",
                f"comparable cells={len(comparable)} inconsistent={len(set(bad))} "
                f"(app-emitted shared_sha256 from archived config_*.json)")
        else:
            add("G6", "shared configuration hash identical across mechanisms",
                "PENDING", "no cell has archived app config artefacts for >=2 mechanisms yet")
    else:
        add("G6", "shared configuration hash identical across mechanisms", "PENDING",
            "artifacts directory not supplied to the analysis")

    # ---- G7 generator capability / quiescence calibration -------------------------------
    rates_path = rates_path or os.path.join(REPO, "experiments", "rates.json")
    rates = None
    if os.path.exists(rates_path):
        try:
            rates = json.load(open(rates_path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            rates = None
    lag_values = [r.get("generator_lag_p99_ms") for r in executed
                  if r.get("generator_lag_p99_ms") is not None]
    calibration = [r for r in recs if r.get("cell_id", "").startswith("V.quiesce")]
    calib_ok = bool(calibration) and all(not r["censored"] for r in calibration)
    if rates is None:
        add("G7", "rate table exists (prereg §6.2); generator sustains >=3x max mechanism "
                  "throughput; doubling T_quiet changes no count", "PENDING",
            "experiments/rates.json missing (pilot output; fixed-rate/saturation modes may not "
            "fall back to max)")
    elif not lag_values:
        add("G7", "rate table + generator-lag p99 <= 5 ms + T_quiet doubling stable", "PENDING",
            "rate table present but no generator-lag sample in run records "
            "(service-side send-timestamp hook requested from S3; recorded deviation S5-6)")
    else:
        lag_ok = max(lag_values) <= 5
        add("G7", "rate table + generator-lag p99 <= 5 ms + T_quiet doubling stable",
            "PASS" if (lag_ok and calib_ok) else "FAIL",
            f"rates present; generator-lag p99 max={max(lag_values)}ms (<=5={lag_ok}); "
            f"quiescence calibration runs={len(calibration)} stable={calib_ok}")

    # ---- G8 clock and host sanity over ALL executed runs --------------------------------
    if executed:
        clock_bad = [r for r in executed if not r["clock_check"]]
        residue_bad = [r for r in executed if not r["residue_check"]]
        disk_bad = [r for r in executed
                    if (r["disk_free_start_gb"] if r["disk_free_start_gb"] is not None
                        else 99) < 3.0]
        add("G8", "single boot; clock probe before/after every run; affinity/priority recorded; "
                  "free disk above the 3 GB floor",
            "PASS" if not (clock_bad or residue_bad or disk_bad) else "FAIL",
            f"executed runs={len(executed)} clock failures={len(clock_bad)} "
            f"residue failures={len(residue_bad)} disk-floor failures={len(disk_bad)}")
    else:
        add("G8", "single boot; clock probe; affinity/priority; disk floor", "PENDING",
            "no executed runs yet")
    return gates


def zero_event_table(recs: list[dict], dataset: str) -> list[dict]:
    rows = []
    for r in correctness_by_cell(recs, dataset):
        if r["n_valid"] == 0 or r["violating_runs"] != 0:
            continue
        reasons = r["exclude_reasons"] or "none"
        sentence = ZERO_EVENT_TEMPLATE.format(
            mechanism=r["mechanism"], n_valid=r["n_valid"],
            n_excluded=r["n_excluded"], condition=f"{r['stage']}/{r['fault']}",
            reasons=reasons, cp_upper=f"{r['cp_upper_one_sided']:.4f}",
            rule3=f"{r['rule_of_three']:.4f}" if r["rule_of_three"] is not None else "n/a",
        )
        rows.append({"dataset": dataset, "stage": r["stage"], "mechanism": r["mechanism"],
                     "fault": r["fault"], "n_valid": r["n_valid"],
                     "n_excluded": r["n_excluded"],
                     "cp_upper_one_sided": r["cp_upper_one_sided"],
                     "rule_of_three": r["rule_of_three"], "sentence": sentence})
    return rows


# --------------------------------------------------------------------------------------------
# LaTeX generators
# --------------------------------------------------------------------------------------------
def _tex_escape(text) -> str:
    if text is None:
        return "—"
    s = str(text)
    repl = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#",
            "_": r"\_", "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}",
            "^": r"\textasciicircum{}"}
    return "".join(repl.get(ch, ch) for ch in s)


def _fmt_pct(x, digits: int = 1) -> str:
    return "—" if x is None else f"{100.0 * x:.{digits}f}\\%"


def write_macros(path: str, hypotheses: dict, recs: list[dict], dataset: str) -> dict:
    valid = [r for r in recs if r["valid"] and not r["excluded"]]
    excluded = [r for r in recs if r["excluded"]]
    lines = ["% Auto-generated by tools/analyze.py — FROZEN analysis pipeline.",
             f"% dataset: {dataset}" + ("  (SYNTHETIC FIXTURE — NOT RESULTS)"
                                        if dataset != DATASET_CAMPAIGN else ""),
             "% Do not edit by hand; regenerate from results/raw/run-records.",
             ""]
    verdict_map = {"confirmed": "confirmed", "refuted": "refuted",
                   "inconclusive": "inconclusive", "partially confirmed": "partially confirmed"}
    names = {"H1": "One", "H2": "Two", "H3": "Three", "H4": "Four", "H5": "Five"}
    macros = {}
    for h in ("H1", "H2", "H3", "H4", "H5"):
        verdict = (hypotheses.get(h) or {}).get("verdict", "pending")
        macro = f"ResHypothesis{names[h]}Verdict"
        macros[macro] = verdict_map.get(verdict, verdict)
        lines.append(f"\\newcommand{{\\{macro}}}{{{_tex_escape(macros[macro])}}}")
    macros["ResTotalValidRuns"] = str(len(valid))
    macros["ResTotalExcludedRuns"] = str(len(excluded))
    lines.append(f"\\newcommand{{\\ResTotalValidRuns}}{{{macros['ResTotalValidRuns']}}}")
    lines.append(f"\\newcommand{{\\ResTotalExcludedRuns}}{{{macros['ResTotalExcludedRuns']}}}")

    mech_macro = {
        "M0": "ResMzero", "M1-kafka": "ResMkafka", "M1x": "ResMonex", "M2": "ResMtwo",
        "M3": "ResMthree", "M4": "ResMfour", "M2+M3": "ResMtwoPlusMthree",
        "M4+M3": "ResMfourPlusMthree", "M1x+M3": "ResMonexPlusMthree",
    }
    for mech, prefix in mech_macro.items():
        runs = [r for r in valid if r["mechanism"] == mech]
        k = sum(1 for r in runs if r["violating_run"])
        n = len(runs)
        macros[f"{prefix}ViolatingRuns"] = str(k)
        lines.append(f"\\newcommand{{\\{prefix}ViolatingRuns}}{{{k}}}")
        if prefix == "ResMonex":
            rate = (k / n) if n else None
            macros["ResMonexViolationRate"] = _fmt_pct(rate)[:-1] if rate is not None else "--"
            lines.append(f"\\newcommand{{\\ResMonexViolationRate}}"
                         f"{{{_fmt_pct(rate) if rate is not None else '--'}}}")
    early = [r for r in valid if r["ordering_variant"] == "early"
             and r["fault"] in ("K4", "F4b") and r["v4"]]
    macros["ResEarlyProbeLossRuns"] = str(len(early))
    lines.append(f"\\newcommand{{\\ResEarlyProbeLossRuns}}{{{macros['ResEarlyProbeLossRuns']}}}")
    lines.append("\\newcommand{\\ResSubstrateDeviationText}{native Windows processes instead of "
                 "containers}")
    lines.append("")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(lines))
    return macros


def _table_header(caption: str, label: str, colspec: str) -> list[str]:
    return [r"\begin{table}[htbp]", r"\centering", r"\small",
            f"\\caption{{{caption}}}", f"\\label{{{label}}}",
            f"\\begin{{tabular}}{{{colspec}}}", r"\toprule"]


def _table_footer() -> list[str]:
    return [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]


def write_tables(out_dir: str, *, gates: list[dict], cbmf: list[dict], counts: list[dict],
                 perf: list[dict], decision: list[dict], hypotheses: dict, recs: list[dict],
                 dataset: str, bootstrap_seed: str, formal_dir: str) -> dict:
    tables_dir = os.path.join(out_dir, "tables")
    os.makedirs(tables_dir, exist_ok=True)
    written = {}
    banner = ("% dataset: " + dataset
              + ("  (SYNTHETIC FIXTURE — NOT RESULTS)" if dataset != DATASET_CAMPAIGN else ""))

    # ---- T1 harness gates ------------------------------------------------------------
    lines = [banner, ""] + _table_header(
        "Harness validation gates G1–G8 (must pass before confirmatory analysis).",
        "tab:validation", "llp{0.56\\linewidth}")
    lines.append(r"\textbf{Gate} & \textbf{Status} & \textbf{Evidence} \\")
    lines.append(r"\midrule")
    for g in gates:
        status = g["status"]
        tex_status = {"PASS": r"\textbf{PASS}", "FAIL": r"\textbf{FAIL}"}.get(status, status)
        lines.append(f"{_tex_escape(g['gate'])} & {tex_status} & "
                     f"{_tex_escape(g['evidence'])} \\\\")
    lines += _table_footer()
    path = os.path.join(tables_dir, "t1-validation.tex")
    open(path, "w", encoding="utf-8", newline="").write("\n".join(lines))
    written["t1"] = path

    # ---- T2 correctness by mechanism × fault class ----------------------------------
    lines = [banner, ""] + _table_header(
        "Correctness by mechanism × fault class: violating runs / $N_{valid}$ with the exact "
        "one-sided 95~\\% upper bound (Clopper–Pearson). n/a cells are structurally unreachable "
        "and never counted as passes.",
        "tab:correctness", "lrrr")
    lines.append(r"\textbf{Mechanism} & \textbf{Condition} & \textbf{Violating / $N$} & "
                 r"\textbf{CP 95~\% upper} \\")
    lines.append(r"\midrule")
    current = None
    for row in sorted(cbmf, key=lambda r: (r["mechanism"], r["fault"])):
        if row["mechanism"] != current:
            current = row["mechanism"]
            lines.append(f"\\multicolumn{{4}}{{l}}{{\\textit{{{_tex_escape(current)}}}}} \\\\")
        lines.append(f" & {_tex_escape(row['fault'])} & "
                     f"{row['violating_runs']}/{row['n_valid']} & "
                     f"{row['cp_upper_one_sided']:.3f} \\\\")
    lines += _table_footer()
    path = os.path.join(tables_dir, "t2-correctness.tex")
    open(path, "w", encoding="utf-8", newline="").write("\n".join(lines))
    written["t2"] = path

    # ---- T3 duplicate/loss/invariant counts by mechanism ----------------------------
    lines = [banner, ""] + _table_header(
        "Violation count distributions by mechanism (median [IQR], runs with count $>0$).",
        "tab:counts", "lrrrrr")
    lines.append(r"\textbf{Mechanism} & \textbf{dup post.} & \textbf{lost post.} & "
                 r"\textbf{imbal. entries} & \textbf{mutated} & \textbf{spurious} \\")
    lines.append(r"\midrule")
    for row in sorted(counts, key=lambda r: r["mechanism"]):

        def cell(m):
            med = row.get(f"{m}_median")
            q1 = row.get(f"{m}_q1")
            q3 = row.get(f"{m}_q3")
            gt0 = row.get(f"{m}_runs_gt0")
            if med is None:
                return "—"
            return f"{med:.0f} [{q1:.0f}, {q3:.0f}] ({gt0})"

        lines.append(f"{_tex_escape(row['mechanism'])} & {cell('dup_postings')} & "
                     f"{cell('lost_postings')} & {cell('imbalanced_entries')} & "
                     f"{cell('mutated_postings')} & {cell('spurious_postings')} \\\\")
    lines += _table_footer()
    path = os.path.join(tables_dir, "t3-counts.tex")
    open(path, "w", encoding="utf-8", newline="").write("\n".join(lines))
    written["t3"] = path

    # ---- T4 performance by mechanism × load × partitions ----------------------------
    lines = [banner, ""] + _table_header(
        "Performance (fault-free): median throughput and median p99 per mechanism × load × "
        "partitions, with bootstrap 95~\\% CIs on the medians.",
        "tab:performance", "llrrrrr")
    lines.append(r"\textbf{Mechanism} & \textbf{Load / P} & \textbf{thr (ev/s)} & "
                 r"\textbf{p50 (ms)} & \textbf{p99 (ms)} & \textbf{p99.9 (ms)} & "
                 r"\textbf{$t_{cons}$ (ms)} \\")
    lines.append(r"\midrule")
    for row in sorted(perf, key=lambda r: (r["mechanism"], r["load_events"], r["partitions"])):
        if row["fault"] not in ("F0", "none"):
            continue

        def fmt_pair(med, lo, hi):
            if med is None:
                return "—"
            return f"{med:.1f} [{lo:.1f}, {hi:.1f}]" if lo is not None else f"{med:.1f}"

        lines.append(f"{_tex_escape(row['mechanism'])} & "
                     f"{load_events_key(row['load_events'])}/p{row['partitions']} & "
                     f"{fmt_pair(row['throughput_median'], row['throughput_lo'], row['throughput_hi'])} & "
                     f"{fmt_pair(row['p50_median'], row['p50_lo'], row['p50_hi'])} & "
                     f"{fmt_pair(row['p99_median'], row['p99_lo'], row['p99_hi'])} & "
                     f"{fmt_pair(row['p999_median'], row['p999_lo'], row['p999_hi'])} & "
                     f"{fmt_pair(row['t_consistent_median_ms'], row['t_consistent_lo_ms'], row['t_consistent_hi_ms'])} \\\\")
    lines += _table_footer()
    path = os.path.join(tables_dir, "t4-performance.tex")
    open(path, "w", encoding="utf-8", newline="").write("\n".join(lines))
    written["t4"] = path

    # ---- T5 TLC check summary (mirror of formal/tlc-results/tlc-summary.json) -------
    tlc_path = os.path.join(formal_dir, "tlc-results", "tlc-summary.json")
    tlc = None
    if os.path.exists(tlc_path):
        try:
            tlc = json.load(open(tlc_path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            tlc = None
    lines = [banner, ""] + _table_header(
        "TLA+/TLC bounded model-check summary (formal/ledger.tla).", "tab:tlc",
        "llp{0.45\\linewidth}")
    if tlc:
        lines.append(r"\textbf{Bound} & \textbf{Value} & \textbf{Result} \\")
        lines.append(r"\midrule")
        for key in sorted(tlc):
            lines.append(f"{_tex_escape(key)} & {_tex_escape(tlc[key])} & \\\\")
    else:
        lines.append(r"\multicolumn{3}{l}{TLC summary not available at analysis time "
                     r"(\texttt{formal/tlc-results/tlc-summary.json} missing).} \\")
    lines += _table_footer()
    path = os.path.join(tables_dir, "t5-tlc.tex")
    open(path, "w", encoding="utf-8", newline="").write("\n".join(lines))
    written["t5"] = path

    # ---- T6 practitioner decision table --------------------------------------------
    lines = [banner, ""] + _table_header(
        "Practitioner decision table: exposure class → minimal sufficient mechanism set, with "
        "measured corroboration from the pre-registered cell map (prereg §7.7). Values are "
        "filled only from results; ‘—’ means not measured.",
        "tab:decision", "lp{0.16\\linewidth}p{0.14\\linewidth}p{0.22\\linewidth}p{0.18\\linewidth}")
    lines.append(r"\textbf{Exposure} & \textbf{Model-minimal} & \textbf{Measured corroboration} "
                 r"& \textbf{CP 95~\% upper} & \textbf{Residual open risk} \\")
    lines.append(r"\midrule")
    for exposure in EXPOSURE_ORDER:
        model = MODEL_MINIMAL.get(exposure, "—")
        corroborated = [r["mechanism"] for r in decision
                        if r["exposure"] == exposure
                        and r["observed_consistent_with_sufficiency"]]
        upper = [r["cp_upper_one_sided"] for r in decision
                 if r["exposure"] == exposure and r["cp_upper_one_sided"] is not None]
        up = f"{max(upper):.3f}" if upper else "—"
        lines.append(f"{_tex_escape(exposure)} & {_tex_escape(model)} & "
                     f"{_tex_escape(', '.join(corroborated) if corroborated else '—')} & "
                     f"{up} & see method §11.1 \\\\")
    lines += _table_footer()
    path = os.path.join(tables_dir, "t6-decision.tex")
    open(path, "w", encoding="utf-8", newline="").write("\n".join(lines))
    written["t6"] = path
    return written


# --------------------------------------------------------------------------------------------
# CSV writer
# --------------------------------------------------------------------------------------------
def write_csv(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    if not rows:
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write("dataset\n")
        return
    fieldnames = list(rows[0].keys())
    for row in rows[1:]:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# --------------------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="WXEO frozen analysis pipeline")
    p.add_argument("--records", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results", "raw",
        "run-records"))
    p.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results", "analysis"))
    p.add_argument("--freeze", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs",
        "prereg-freeze.json"))
    p.add_argument("--harness-dir", default=None,
                   help="frozen harness-gate artefacts (default: <results>/harness)")
    p.add_argument("--formal-dir", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "formal"))
    p.add_argument("--bootstrap-seed", default=None)
    p.add_argument("--dataset", default=None, help="override dataset label (e.g. synthetic)")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    records_dir = os.path.abspath(args.records)
    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)
    freeze = load_freeze(args.freeze)
    raw_records, malformed = load_records(records_dir)
    all_recs = [extract(r) for r in raw_records]
    recs = confirmatory(all_recs)
    validation_recs = [r for r in all_recs if r["is_validation"]]
    reserved = load_reserved(records_dir)
    reserved_cells: dict[str, int] = {}
    for row in reserved:
        cell = row.get("cell_id")
        if cell:
            reserved_cells[cell] = reserved_cells.get(cell, 0) + 1
    dataset = args.dataset or dataset_label(raw_records, freeze)
    bootstrap_seed = (args.bootstrap_seed
                      or (freeze.get("seeds") or {}).get("bootstrap_seed")
                      or freeze.get("bootstrap_seed")  # tolerate a flattened freeze file
                      or "0000000000000000")
    harness_dir = args.harness_dir or os.path.join(os.path.dirname(records_dir), "harness")
    if not os.path.isdir(harness_dir):
        harness_dir = os.path.join(os.path.dirname(os.path.dirname(records_dir)), "harness")

    # estimator tables
    by_cell = correctness_by_cell(recs, dataset, reserved_cells)
    validation_by_cell = correctness_by_cell(validation_recs, dataset)
    cbmf = correctness_by_mechanism_fault(recs, dataset)
    counts = counts_by_mechanism(recs, dataset)
    perf = performance_by_cell(recs, dataset, bootstrap_seed)
    per_run = per_run_performance(recs, dataset)
    artifacts_dir = os.path.join(os.path.dirname(records_dir), "artifacts")
    gates = build_gate_table(all_recs, harness_dir, reserved=reserved,
                             faults_log=os.path.join(os.path.dirname(os.path.dirname(
                                 records_dir)), "faults.log"),
                             artifacts_dir=artifacts_dir)
    decision = build_decision_table(recs, dataset)
    zero_events = zero_event_table(recs, dataset)
    families = build_test_families(recs, bootstrap_seed)
    hypotheses = decide_hypotheses(recs, {"families": families}, dataset)

    # tests.json (all raw + adjusted p-values, effect sizes, CIs)
    input_digests = {}
    for name in sorted(os.listdir(records_dir)):
        if name.endswith(".json"):
            path = os.path.join(records_dir, name)
            input_digests[name] = sha256_file(path)
    tests = {
        "schema": "wxeo.tests.v1",
        "dataset": dataset,
        "synthetic": dataset != DATASET_CAMPAIGN,
        "warning": ("SYNTHETIC FIXTURE — harness/analysis demonstration only, NOT results"
                    if dataset != DATASET_CAMPAIGN else None),
        "alpha": ALPHA,
        "bootstrap": {"B": BOOTSTRAP_B, "seed": bootstrap_seed,
                      "method": "percentile primary, BCa sensitivity"},
        "cliff_thresholds": {"negligible": CLIFF_NEGLIGIBLE, "small": CLIFF_SMALL,
                             "medium": CLIFF_MEDIUM},
        "n_records": len(recs),
        "n_valid": sum(1 for r in recs if r["valid"] and not r["excluded"]),
        "n_excluded": sum(1 for r in recs if r["excluded"]),
        "malformed_records": malformed,
        "families": families,
        "hypotheses": hypotheses,
        "zero_event_statements": zero_events,
        "inputs": {"records_dir": _repo_rel(records_dir),
                   "reserved_runs": _repo_rel(os.path.join(os.path.dirname(records_dir),
                                                           "summaries", "reserved-runs.csv")),
                   "record_sha256": input_digests,
                   "freeze_file": _repo_rel(args.freeze),
                   "freeze_sha256": sha256_file(args.freeze) if os.path.exists(args.freeze)
                   else None},
        "analysis_script_sha256": sha256_file(os.path.abspath(__file__)),
    }
    try:
        from experiments import pathsafe as _pathsafe
        tests = _pathsafe.scrub(tests)
        pathsafe_ok = True
    except Exception:  # noqa: BLE001
        pathsafe_ok = False
    with open(os.path.join(out_dir, "tests.json"), "w", encoding="utf-8", newline="") as fh:
        json.dump(tests, fh, indent=2, sort_keys=False)
        fh.write("\n")
    with open(os.path.join(out_dir, "hypotheses.json"), "w", encoding="utf-8", newline="") as fh:
        json.dump({"dataset": dataset, "hypotheses": hypotheses}, fh, indent=2)
        fh.write("\n")
    write_csv(os.path.join(out_dir, "correctness-by-cell.csv"), by_cell)
    write_csv(os.path.join(out_dir, "validation-by-cell.csv"), validation_by_cell)
    write_csv(os.path.join(out_dir, "correctness-by-mechanism-fault.csv"), cbmf)
    write_csv(os.path.join(out_dir, "counts-by-mechanism.csv"), counts)
    write_csv(os.path.join(out_dir, "performance-by-cell.csv"), perf)
    write_csv(os.path.join(out_dir, "per-run-performance.csv"), per_run)
    write_csv(os.path.join(out_dir, "decision-table.csv"), decision)
    write_csv(os.path.join(out_dir, "gates.csv"), gates)
    write_csv(os.path.join(out_dir, "zero-events.csv"), zero_events)

    macros = write_macros(os.path.join(out_dir, "macros.tex"), hypotheses, recs, dataset)
    tables = write_tables(out_dir, gates=gates, cbmf=cbmf, counts=counts, perf=perf,
                          decision=decision, hypotheses=hypotheses, recs=recs,
                          dataset=dataset, bootstrap_seed=bootstrap_seed,
                          formal_dir=args.formal_dir)
    manifest = {
        "schema": "wxeo.analysis-manifest.v1",
        "dataset": dataset,
        "synthetic": dataset != DATASET_CAMPAIGN,
        "warning": ("SYNTHETIC FIXTURE — NOT RESULTS" if dataset != DATASET_CAMPAIGN else None),
        "outputs": {name: os.path.basename(path) for name, path in tables.items()},
        "macros": macros,
        "hypotheses": {k: v["verdict"] for k, v in hypotheses.items()
                       if isinstance(v, dict) and "verdict" in v},
        "n_valid": tests["n_valid"], "n_excluded": tests["n_excluded"],
        "analysis_script_sha256": tests["analysis_script_sha256"],
        "bootstrap_seed": bootstrap_seed,
    }
    with open(os.path.join(out_dir, "analysis-manifest.json"), "w", encoding="utf-8",
              newline="") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    print(json.dumps({
        "out": out_dir, "dataset": dataset, "synthetic": dataset != DATASET_CAMPAIGN,
        "n_valid": tests["n_valid"], "n_excluded": tests["n_excluded"],
        "hypotheses": manifest["hypotheses"],
        "tables": sorted(manifest["outputs"]), "bootstrap_seed": bootstrap_seed,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# =============================================================================================
# A2 analyzer (author-approved P2 amendment A2, 2026-11-24)
# AC-1 = H5-A, AC-3b = pooled stratified exact sign test, AC-4 = one of M3/M4,
# AC-5 = v5 is a violation, AC-6 = split I1a/I1b (P11 = v3 & not v1b), AC-8 = >20% censoring flag.
# =============================================================================================
A2_ALPHA = 0.05
A2_QUALITY = {"min_valid": 20, "min_attested": 0.80, "max_exclusion": 0.20,
              "max_censored": 0.20}
A2_C1_CELLS = (("A", "K2"), ("B", "F1"), ("B", "F2"), ("B", "F3"))
A2_H1_DIVERGENCE = (("A", "K2"), ("A", "K3"), ("B", "F1"), ("B", "F2"),
                    ("B", "F3"), ("B", "F4a"), ("B", "F6"))
A2_CANONICAL = {
    "M0": (("A", "K1"), ("A", "K2"), ("A", "K3"), ("A", "K5"),
           ("B", "F0"), ("B", "F1"), ("B", "F2"), ("B", "F3"), ("B", "F4a"),
           ("B", "F5"), ("B", "F6")),
    "M1-kafka": (("A", "K1"), ("A", "K2"), ("B", "F0"), ("B", "F1"), ("B", "F2"),
                 ("B", "F3")),
    "M1x": (("A", "K1"), ("A", "K2"), ("A", "K3"), ("A", "K5"),
            ("B", "F0"), ("B", "F1"), ("B", "F2"), ("B", "F3"), ("B", "F4a"),
            ("B", "F5"), ("B", "F6")),
    "M2": (("A", "K1"), ("A", "K2"), ("A", "K3"), ("A", "K5"), ("A", "K7"),
           ("B", "F0"), ("B", "F1"), ("B", "F2"), ("B", "F3"), ("B", "F4a"),
           ("B", "F5"), ("B", "F6")),
    "M3": (("A", "K1"), ("A", "K2"), ("A", "K3"), ("A", "K5"),
           ("B", "F0"), ("B", "F1"), ("B", "F2"), ("B", "F3"), ("B", "F4a"),
           ("B", "F5"), ("B", "F6")),
    "M4": (("A", "K1"), ("A", "K2"), ("A", "K5"),
           ("B", "F0"), ("B", "F1"), ("B", "F2"), ("B", "F3"), ("B", "F4a"),
           ("B", "F5"), ("B", "F6")),
    "M2+M3": (("A", "K1"), ("A", "K2"), ("A", "K3"), ("A", "K5"), ("A", "K7"),
              ("B", "F0"), ("B", "F1"), ("B", "F2"), ("B", "F3"), ("B", "F4a"),
              ("B", "F5"), ("B", "F6")),
    "M4+M3": (("A", "K1"), ("A", "K2"), ("A", "K5"),
              ("B", "F0"), ("B", "F1"), ("B", "F2"), ("B", "F3"), ("B", "F4a"),
              ("B", "F5"), ("B", "F6")),
    "M1x+M3": (("A", "K1"), ("A", "K2"), ("A", "K3"), ("A", "K5"),
               ("B", "F0"), ("B", "F1"), ("B", "F2"), ("B", "F3"), ("B", "F4a"),
               ("B", "F5"), ("B", "F6")),
}
A2_PROBE = {"M0": (("A", "K4"), ("B", "F4b")), "M1x": (("A", "K4"), ("B", "F4b")),
            "M2": (("A", "K4"), ("B", "F4b")), "M3": (("A", "K4"), ("B", "F4b")),
            "M4": (("A", "K4"),), "M2+M3": (("A", "K4"), ("B", "F4b")),
            "M4+M3": (("A", "K4"),), "M1x+M3": (("A", "K4"), ("B", "F4b"))}
A2_RESIDUAL = {"M0": ("E1", "E3", "E4", "E5"), "M1x": ("E1", "E3", "E4"),
               "M2": ("E5",), "M3": ("E5",), "M4": ("E3", "E5"),
               "M2+M3": ("E5",), "M4+M3": ("E5",), "M1x+M3": ()}
A2_DOMINANCE_DOMAIN = ("M0", "M1x", "M2", "M3", "M4", "M2+M3", "M4+M3", "M1x+M3")
A2_K6_NOTE = ("K6 is n/a for every mechanism (zombie writer not realisable within T_max); "
              "it is listed as n/a, never as missing evidence.")
A2_P3_CONTRASTS = (("M2", "M3"), ("M4", "M2"), ("M4+M3", "M2"))
A2_P3_FAULTS = ("F1", "F2", "F3", "F4a", "F5", "F6")


def a2_derive_v1(rec: dict) -> tuple[bool, bool]:
    """I1a/I1b derived from stored counts (AC-6); v1 itself is never recomputed."""
    counts = rec.get("counts") or {}
    v1a = int(counts.get("structurally_malformed_entries") or 0) > 0
    v1b = int(counts.get("imbalanced_sum_entries") or 0) > 0
    return v1a, v1b


def a2_clean_fail(rec: dict) -> bool:
    """outcome_clean_fail = violating_run OR censored (A2-2 §2.2)."""
    violations = rec.get("violations") or {}
    quiescence = rec.get("quiescence") or {}
    return bool(violations.get("violating_run")) or bool(quiescence.get("censored"))


def a2_cell(rec: dict) -> tuple[str, str]:
    return (str(rec.get("stage") or "").upper(), str(rec.get("fault") or "").upper())


def a2_cell_key(rec: dict) -> str:
    stage, fault = a2_cell(rec)
    return f"{stage}-{fault}"


def a2_attested(rec: dict) -> bool:
    """Single-source attested-receipt rule: injected classes need an attested receipt set."""
    fault = str(rec.get("fault") or "F0").upper()
    if fault in ("F0", "NONE", "CTRL"):
        return True
    try:
        from experiments import matrix_design as MD
        from experiments import receipts as RCPT
        owner = MD.injector_ownership(fault)
        failpoint = {"K1": "after_fetch", "K3": "after_effect_commit",
                     "K4": "before_effect_commit", "K7": "before_relay_mark",
                     "F4b": "before_effect_commit"}.get(fault)
        mode = "failpoint" if owner == "app" else "external"
        out = RCPT.validate_run_receipts(
            rec.get("fault_receipts") or [], run_id=rec.get("run_id") or "",
            injector_mode=mode, fault=fault, fault_level=rec.get("fault_level") or "L1",
            failpoint=failpoint if owner == "app" else None,
            action="crash" if owner == "app" else None)
        return bool(out.get("attested"))
    except Exception:  # noqa: BLE001
        return False


def a2_conf_quality(records: list[dict], mechanism: str | None = None) -> dict:
    """Per-cell quality bars (N_valid, attested rate, exclusion, censoring) for the given set."""
    by_cell: dict[str, list[dict]] = {}
    for rec in records:
        key = a2_cell_key(rec)
        if mechanism is not None and rec.get("mechanism") != mechanism:
            continue
        by_cell.setdefault(key, []).append(rec)
    table = {}
    for key, recs in sorted(by_cell.items()):
        valid = [r for r in recs if (r.get("validity") or {}).get("valid")]
        excluded = [r for r in recs if (r.get("validity") or {}).get("excluded")]
        censored = [r for r in valid if (r.get("quiescence") or {}).get("censored")]
        injected = [r for r in valid if str(r.get("fault") or "").upper() not in
                    ("F0", "NONE", "CTRL")]
        attested = [r for r in injected if a2_attested(r)] if injected else []
        n_valid = len(valid)
        rate = (len(attested) / len(injected)) if injected else 1.0
        table[key] = {
            "n_records": len(recs), "n_valid": n_valid, "n_excluded": len(excluded),
            "n_censored": len(censored),
            "attested_rate": rate,
            "exclusion_rate": len(excluded) / len(recs) if recs else 0.0,
            "censored_rate": len(censored) / n_valid if n_valid else 0.0,
            "usable": (n_valid >= A2_QUALITY["min_valid"]
                       and rate >= A2_QUALITY["min_attested"]
                       and (len(excluded) / len(recs) if recs else 0.0)
                       <= A2_QUALITY["max_exclusion"]),
            "fragile": (len(censored) / n_valid) > A2_QUALITY["max_censored"] if n_valid else False,
        }
    return table


def a2_pooled_sign_test(pairs_by_cell: dict[str, list[tuple[bool, bool]]]) -> dict:
    """AC-3b: pooled stratified exact one-sided sign test; cells unweighted, pairs weight 1."""
    b_total = c_total = 0
    contributing = 0
    per_cell = {}
    for cell, pairs in pairs_by_cell.items():
        b = sum(1 for treatment_fails, control_clean in pairs
                if treatment_fails and not control_clean)
        c = sum(1 for treatment_fails, control_clean in pairs
                if control_clean and not treatment_fails)
        if b + c > 0:
            contributing += 1
        per_cell[cell] = {"pairs": len(pairs), "b": b, "c": c}
        b_total += b
        c_total += c
    n = b_total + c_total
    p = 1.0 if n == 0 else sum(
        math.comb(n, k) for k in range(b_total, n + 1)) / (2 ** n)
    return {"b": b_total, "c": c_total, "n": n, "p_one_sided": p,
            "cells_contributing": contributing,
            "analysable": contributing >= 2,
            "significant": (contributing >= 2 and b_total > c_total and p < A2_ALPHA),
            "per_cell": per_cell}


def a2_h1(records: list[dict]) -> dict:
    """A2 §1.3 H1 under AC-3b (pooled sign test over C1; per-cell tests descriptive)."""
    m1x = [r for r in records if r.get("mechanism") == "M1x"]
    m1k = [r for r in records if r.get("mechanism") == "M1-kafka"]
    exposure_cells = [c for c in A2_H1_DIVERGENCE if c[0] == "A"]
    exposure = [r for r in m1x
                if a2_cell(r) in exposure_cells
                and bool((r.get("violations") or {}).get("violating_run"))
                and not bool((r.get("quiescence") or {}).get("censored"))]
    # pair by (stage, fault, condition, load, partitions, rep) — D15 shared seeds
    m1x_by_key = {_a2_pair_key(r): r for r in m1x}
    m1k_by_key = {_a2_pair_key(r): r for r in m1k}
    pairs_by_cell: dict[str, list[tuple[bool, bool]]] = {}
    for stage, fault in A2_C1_CELLS:
        cell = f"{stage}-{fault}"
        pairs = []
        for key, x in m1x_by_key.items():
            if a2_cell(x) != (stage, fault) or key not in m1k_by_key:
                continue
            k = m1k_by_key[key]
            pairs.append((a2_clean_fail(x), a2_clean_fail(k)))
        pairs_by_cell[cell] = pairs
    test = a2_pooled_sign_test(pairs_by_cell)
    descriptive = {}
    for cell, pairs in pairs_by_cell.items():
        mc = mcnemar_exact([(not x, not k) for x, k in pairs]) if pairs else None
        descriptive[cell] = mc
    m1k_clean_fail = [r for r in m1k if a2_clean_fail(r)]
    quality = a2_conf_quality(m1x)
    bars_ok = all(quality.get(f"A-{f}", {}).get("usable", False)
                  for f in ("K2", "K3"))
    if m1k_clean_fail:
        verdict = "refuted"
    elif not exposure:
        refuted_ok = bars_ok and test["analysable"] is False
        verdict = "refuted" if refuted_ok else "inconclusive"
    elif all((r.get("validity") or {}).get("valid") for r in exposure) and bars_ok:
        verdict = "confirmed" if test["significant"] else "inconclusive"
    else:
        verdict = "inconclusive"
    return {"hypothesis": "H1", "family": "C1 (AC-3b pooled stratified exact sign test)",
            "exposure_observed": len(exposure), "m1_kafka_clean_fail": len(m1k_clean_fail),
            "pooled": test, "per_cell_descriptive": descriptive,
            "quality": quality, "verdict": verdict,
            "deleted": ["old A-K3 McNemar", "C2", "C3"]}


def _a2_pair_key(rec: dict) -> tuple:
    return (rec.get("stage"), rec.get("condition") or rec.get("fault"),
            rec.get("load_events"), rec.get("partitions"), rec.get("rep"))


def a2_h2(records: list[dict], performance: dict | None = None) -> dict:
    m2 = [r for r in records if r.get("mechanism") == "M2"]
    canon = a2_conf_quality(m2)
    failing = [r for r in m2 if a2_cell(r) in A2_CANONICAL["M2"] and a2_clean_fail(r)]
    cost = _a2_cost_any_of(performance, "M2", ("M3", "M4")) if performance else None
    correctness = len(failing) == 0 and bool(m2) and all(
        canon.get(f"{s}-{f}", {}).get("usable", False)
        for s, f in A2_CANONICAL["M2"])
    if failing:
        verdict = "refuted"
    elif correctness and cost is not None:
        verdict = "confirmed" if cost["confirmed"] else "partially confirmed"
    elif correctness:
        verdict = "inconclusive"
    else:
        verdict = "inconclusive"
    return {"hypothesis": "H2", "correctness_confirmed": correctness,
            "clean_fail": len(failing), "cost": cost, "verdict": verdict,
            "probe_cells_excluded": True}


def a2_h3(records: list[dict]) -> dict:
    m3 = [r for r in records if r.get("mechanism") == "M3"]
    executed = [r for r in m3 if a2_cell(r) in A2_CANONICAL["M3"]
                or a2_cell(r) in A2_PROBE.get("M3", ())]
    dup = [r for r in executed if bool((r.get("violations") or {}).get("v3"))]
    probe = [r for r in m3 if a2_cell(r) in A2_PROBE.get("M3", ())
             and r.get("ordering_variant") == "early"
             and (r.get("validity") or {}).get("valid")
             and not (r.get("quiescence") or {}).get("censored")
             and bool((r.get("violations") or {}).get("v4"))]
    quality = a2_conf_quality(m3)
    probe_keys = [f"{s}-{f}" for s, f in A2_PROBE.get("M3", ())]
    probe_ok = all(quality.get(k, {}).get("usable", False) for k in probe_keys)
    if dup:
        verdict = "refuted"
    elif probe:
        verdict = "confirmed"
    elif m3 and probe_ok:
        verdict = "refuted"
    else:
        verdict = "inconclusive"
    return {"hypothesis": "H3", "duplicate_arm_fail": len(dup),
            "loss_arm_observed": len(probe), "probe_quality": probe_ok,
            "verdict": verdict, "deleted": ["family C2", "c2_sig"]}


def a2_h4(records: list[dict], performance: dict | None = None) -> dict:
    failing = []
    for mech in ("M4", "M4+M3"):
        failing.extend([r for r in records if r.get("mechanism") == mech
                        and a2_cell(r) in A2_CANONICAL[mech] and a2_clean_fail(r)])
    latency = _a2_latency(performance, "M4", "M2") if performance else None
    correctness = not failing
    if failing:
        verdict = "refuted"
    elif latency is not None:
        verdict = "confirmed" if latency["confirmed"] else "partially confirmed"
    else:
        verdict = "inconclusive"
    return {"hypothesis": "H4", "correctness_confirmed": correctness,
            "clean_fail": len(failing), "latency": latency, "verdict": verdict,
            "k6_clauses_removed": True, "k6_note": A2_K6_NOTE}


def a2_h5a(records: list[dict], performance: dict | None = None) -> dict:
    """AC-1 H5-A residual-closure dominance with non-vacuity guard."""
    frontier = []
    for mech in A2_DOMINANCE_DOMAIN:
        recs = [r for r in records if r.get("mechanism") == mech]
        canon_clean = (bool(recs) and not any(
            a2_cell(r) in A2_CANONICAL[mech] and a2_clean_fail(r) for r in recs))
        quality_ok = all(a2_conf_quality(recs).get(f"{s}-{f}", {}).get("usable", False)
                         for s, f in A2_CANONICAL[mech])
        perf_ok = _a2_non_inferior(performance, mech) if performance else None
        residual = list(A2_RESIDUAL[mech])
        dominates = bool(canon_clean and quality_ok and (perf_ok is True) and not residual)
        frontier.append({"mechanism": mech, "canonical_clean": canon_clean,
                         "quality_ok": quality_ok, "non_inferior": perf_ok,
                         "residual": residual, "dominates": dominates})
    adequate = [f for f in frontier if f["canonical_clean"] and f["quality_ok"]]
    dominating = [f for f in frontier if f["dominates"]]
    if dominating:
        verdict = "refuted"
    elif not adequate:
        verdict = "inconclusive"
    else:
        verdict = "confirmed"
    return {"hypothesis": "H5-A", "frontier": frontier, "adequate_power": len(adequate),
            "dominating": [f["mechanism"] for f in dominating], "verdict": verdict,
            "vacuity_guard": "confirmed requires >=1 canonical-clean mechanism with adequate "
                             "power; otherwise inconclusive"}


def _a2_cost_any_of(performance: dict, mech: str, others: tuple) -> dict:
    tests = performance.get("P1", {}) if performance else {}
    for other in others:
        key = f"{mech}|{other}"
        block = tests.get(key) or {}
        if block.get("delta_better") is not None and block.get("p_holm") is not None:
            if block["delta_better"] <= -0.33 and block["p_holm"] < A2_ALPHA:
                return {"confirmed": True, "pair": key, "delta_better": block["delta_better"],
                        "p_holm": block["p_holm"]}
    return {"confirmed": False, "reason": "no pre-registered pair with delta<=-0.33 and "
                                          "Holm-adjusted one-sided p<0.05"}


def _a2_latency(performance: dict, mech: str, other: str) -> dict:
    block = (performance.get("P2", {}) if performance else {}).get(f"{mech}|{other}") or {}
    if block.get("delta_better") is None:
        return None
    return {"delta_better": block["delta_better"], "p_holm": block.get("p_holm"),
            "blocks_present": block.get("blocks_present"),
            "confirmed": bool(block["delta_better"] >= 0.33
                              and (block.get("p_holm") or 1.0) < A2_ALPHA
                              and (block.get("blocks_present") or 0) >= 4)}


def _a2_non_inferior(performance: dict, mech: str) -> bool | None:
    entry = (performance.get("noninferiority", {}) if performance else {}).get(mech)
    if not entry:
        return None
    return bool(entry.get("throughput") and entry.get("p99"))


def a2_p3(records: list[dict]) -> dict:
    """A2 §1.8 P3: 3 pre-registered contrasts over canonical Stage-B fault cells, Fisher exact."""
    out = {}
    for a, b in A2_P3_CONTRASTS:
        ra = [r for r in records if r.get("mechanism") == a
              and a2_cell(r)[0] == "B" and a2_cell(r)[1] in A2_P3_FAULTS]
        rb = [r for r in records if r.get("mechanism") == b
              and a2_cell(r)[0] == "B" and a2_cell(r)[1] in A2_P3_FAULTS]
        if not ra or not rb:
            out[f"{a}|{b}"] = None
            continue
        qa = sum(1 for r in ra if (r.get("quiescence") or {}).get("consistent_within_tmax"))
        qb = sum(1 for r in rb if (r.get("quiescence") or {}).get("consistent_within_tmax"))
        out[f"{a}|{b}"] = {"consistent": [qa, qb], "n": [len(ra), len(rb)],
                           "fisher": fisher_test([[qa, len(ra) - qa],
                                                  [qb, len(rb) - qb]])}
    pvals = {k: (v["fisher"]["p"] if v else None) for k, v in out.items()}
    adjusted = holm(pvals)
    for k, v in out.items():
        if v:
            v["p_holm"] = adjusted.get(k)
    return {"family": "P3 (3 contrasts, Fisher exact two-sided, Holm m=3)",
            "contrasts": out}


def a2_p11(records: list[dict]) -> dict:
    """Restated P11 (AC-6): v3=1 & v1b=0 — sum-to-zero blindness to balanced duplication."""
    observed = []
    for rec in records:
        violations = rec.get("violations") or {}
        v1a, v1b = a2_derive_v1(rec)
        if bool(violations.get("v3")) and not v1b:
            observed.append({"run_id": rec.get("run_id"), "v1a": v1a, "v1b": v1b})
    return {"test": "v3=1 and v1b=0 (restated P11; AC-6)", "observations": len(observed),
            "examples": observed[:3],
            "confirmed": bool(observed)}


def a2_classify_absence(records: list[dict]) -> dict:
    """Evidence-class classifier: K6 has no rows -> n/a (not missing)."""
    by_cell = {a2_cell(r) for r in records}
    k6_rows = [r for r in records if a2_cell(r)[1] == "K6"]
    k6 = {"rows": len(k6_rows), "status": "n/a (structurally unreachable)",
          "reason": A2_K6_NOTE}
    missing = [c for c in (("A", k) for k in ("K1", "K2", "K3", "K4", "K5", "K7"))
               if c not in by_cell]
    return {"k6": k6, "absent_cell_tokens": missing,
            "pilot_validation_excluded": True}


def a2_validate_records(records: list[dict]) -> dict:
    """Re-validate every record against wxeo.run.v1 and the A2 cross-field equations."""
    try:
        from experiments import validate_record as VR
    except ImportError:
        VR = None
    invalid = []
    equations_bad = []
    for rec in records:
        errors = VR.validate_record(rec) if VR else []
        if errors:
            invalid.append({"run_id": rec.get("run_id"), "errors": errors[:3]})
        violations = rec.get("violations") or {}
        if violations:
            v1a, v1b = a2_derive_v1(rec)
            expected_v1 = v1a or v1b
            expected_violating = any(bool(violations.get(k))
                                     for k in ("v1", "v2", "v3", "v4", "v5"))
            if bool(violations.get("v1")) != expected_v1 or \
                    bool(violations.get("violating_run")) != expected_violating:
                equations_bad.append({"run_id": rec.get("run_id"),
                                      "v1_expected": expected_v1,
                                      "violating_expected": expected_violating})
    return {"n_records": len(records), "invalid": invalid,
            "equation_violations": equations_bad,
            "ok": not invalid and not equations_bad}


def a2_item14_fixture() -> tuple[list[dict], dict]:
    """A2 §6 item-14 acceptance fixture: six synthetic cases with N>=20 per cell."""
    APP = {"K1", "K3", "K4", "K7", "F4B"}
    APP_FP = {"K1": "after_fetch", "K3": "after_effect_commit",
              "K4": "before_effect_commit", "K7": "before_relay_mark",
              "F4B": "before_effect_commit"}
    S4 = {"K2", "K5", "F1", "F2", "F3", "F4A", "F5", "F6"}

    def rec(mech, stage, fault, rep=0, violating=False, v=None, counts=None,
            censored=False, valid=True, ordering="strict"):
        run_id = f"{stage}-{mech}-{fault}-r{rep:02d}"
        receipts = []
        fu = fault.upper()
        if fu in APP:
            receipts = [
                {"run_id": run_id, "fault": APP_FP[fu], "event": "fired",
                 "detail": "action=crash;instance=1"},
                {"run_id": run_id, "fault": fault, "event": "restored", "source": "s4"}]
        elif fu in S4:
            receipts = [
                {"run_id": run_id, "fault": fault, "event": "fired", "source": "s4",
                 "action_verified": True,
                 "phase": {"load_start_ms": 0, "load_end_ms": 1000, "fire_ms": 500,
                           "consumers_alive": True}},
                {"run_id": run_id, "fault": fault, "event": "restored", "source": "s4"}]
        return {
            "run_id": run_id, "stage": stage, "mechanism": mech, "fault": fault,
            "fault_level": "L1" if stage == "A" else "L2",
            "ordering_variant": ordering, "rep": rep,
            "condition": fault.lower(), "load_events": 1000, "partitions": 3,
            "validity": {"valid": valid, "excluded": not valid,
                         "exclude_reason": None if valid else "application_did_not_start"},
            "violations": {"v1": bool((counts or {}).get("v1", False)),
                           "v2": False, "v3": bool(violating and v == "v3"),
                           "v4": bool(violating and v == "v4"),
                           "v5": bool(violating and v == "v5"),
                           "violating_run": bool(violating)},
            "counts": {"structurally_malformed_entries":
                       int((counts or {}).get("v1a", 0)),
                       "imbalanced_sum_entries": int((counts or {}).get("v1b", 0)),
                       "spurious_postings": 1 if v == "v5" else 0},
            "quiescence": {"censored": bool(censored),
                           "consistent_within_tmax": not censored},
            "fault_receipts": receipts,
        }

    records = []
    # (i) divergence: A-K2 30 pairs (b=3), B-F1 30 pairs (b=2); M1-kafka clean throughout
    for rep in range(30):
        records.append(rec("M1x", "A", "K2", rep, violating=rep < 3, v="v3"))
        records.append(rec("M1-kafka", "A", "K2", rep))
        records.append(rec("M1x", "B", "F1", rep, violating=rep < 2, v="v3"))
        records.append(rec("M1-kafka", "B", "F1", rep))
        # M1x exposure cell A-K3 (quality bars): 1 spurious violation + clean rest
        records.append(rec("M1x", "A", "K3", rep, violating=rep == 0, v="v5"))
    # (ii) probe-only M3 loss: K4 early, v4, non-censored, attested (30 probe runs)
    for rep in range(30):
        records.append(rec("M3", "A", "K4", rep, violating=(rep == 0 and True) or False,
                           v="v4" if rep == 0 else None, ordering="early"))
    # (iii) canonical M2 clean with 2 censored runs (30 valid, censoring = clean fail)
    for rep in range(30):
        records.append(rec("M2", "A", "K1", rep, censored=(rep in (0, 1))))
    # (iv) spurious-only case: v5 violation, I1 structural clean
    records.append(rec("M1x", "A", "K3", 900, violating=True, v="v5"))
    # (v) duplicate case v1a=1, v1b=0 (balanced whole-entry duplication)
    records.append(rec("M0", "A", "K3", 0, violating=True, v="v3",
                       counts={"v1": True, "v1a": 1, "v1b": 0}))
    # (vi) K6 absent: no rows for K6 anywhere (classified n/a, never missing)
    expected = {
        "h1_verdict": "confirmed", "h1_pooled_b": 5, "h1_pooled_c": 0,
        "h3_verdict": "confirmed",
        "h2": {"censored_fails": 2},
        "p11_confirmed": True,
        "k6_status": "n/a (structurally unreachable)",
        "spurious_is_violation": True,
        "derived_v1a_v1b": (1, 0),
    }
    return records, expected


def a2_analyze(records: list[dict], performance: dict | None = None,
               matrix_pin_ok: bool = True) -> dict:
    """Full A2 analysis: re-validation, classes, H1-H5-A, P3, P11; fail-closed on matrix pin."""
    if not matrix_pin_ok:
        raise SystemExit("A2 analyzer: matrix hash does not match the pinned/frozen matrix; "
                         "refusing to classify evidence")
    validation = a2_validate_records(records)
    classes = a2_classify_absence(records)
    return {
        "amendment": "A2 (AC-1=H5-A, AC-3b, AC-4=one, AC-5=v5, AC-6=split, AC-8=20%)",
        "record_validation": validation,
        "evidence_classes": classes,
        "H1": a2_h1(records), "H2": a2_h2(records, performance),
        "H3": a2_h3(records), "H4": a2_h4(records, performance),
        "H5": a2_h5a(records, performance), "P3": a2_p3(records),
        "P11": a2_p11(records),
    }
