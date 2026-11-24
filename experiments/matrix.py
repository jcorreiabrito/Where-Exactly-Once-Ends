#!/usr/bin/env python3
"""Generate the frozen experiment matrix and the randomised run order.

Sources (frozen, binding):
  * ``docs/prereg.md`` §2 (exact matrices and counts), §4.3 (randomisation/interleaving),
    §4.4 (seed formulas), §5 (run-record identity fields).
  * ``docs/method.md`` §5.3 (mechanism × crash-class reachability), §5.4 (faults, ordering probe).
  * ``docs/P2-RULINGS.md`` R1–R4 (run_id encoding, saturation token, validation runs, pilot
    planned-vs-executable accounting) and ``docs/CONTRACTS.md`` §2, §4.

Nothing here changes a factor, a cell, or an N from the prereg.  The only accounting nuance is
R4: the pilot has 216 *planned* slots (9 mechanisms × 8 fault conditions × N=3) of which 15 are
structurally n/a by the documented reachability matrix (5 mechanism×fault cells × 3 reps) and are
kept in the matrix with ``executed: false`` + the reachability reason; the executable pilot is
201.  Totals are always reported as planned / executable / executed / excluded.

Outputs (committed and hashed before execution):
  * ``experiments/matrix.yaml``      — cells, counts, factors, reachability, seeds, totals
  * ``experiments/run_order.csv``    — one row per executable run, in executed order
  * ``experiments/matrix.sha256``    — SHA-256 of both files (prereg §10 freeze input)

Usage::

    python experiments/matrix.py            # write and verify
    python experiments/matrix.py --check    # verify existing files against the design
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import random
import sys
from datetime import datetime, timezone

try:  # allow both `python experiments/matrix.py` and `python -m experiments.matrix`
    from experiments import matrix_design as DES
except ImportError:  # pragma: no cover
    _REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, _REPO)
    from experiments import matrix_design as DES  # type: ignore

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MATRIX_PATH = os.path.join(REPO, "experiments", "matrix.yaml")
ORDER_PATH = os.path.join(REPO, "experiments", "run_order.csv")
HASH_PATH = os.path.join(REPO, "experiments", "matrix.sha256")

#: Campaign seeds: fixed constants generated once from the documented phrases below and recorded
#: in docs/prereg-freeze.json.  They are NOT chosen after seeing results and must not change
#: without a DEVIATIONS entry (a changed seed is a new campaign).
SEED_PHRASES = {
    "master_seed": "wxeo-campaign-v1|master|2026-11-24",
    "order_seed": "wxeo-campaign-v1|order|2026-11-24",
    "bootstrap_seed": "wxeo-campaign-v1|bootstrap|2026-11-24|B=10000",
}
MASTER_SEED = hashlib.sha256(SEED_PHRASES["master_seed"].encode()).hexdigest()[:32]
ORDER_SEED = hashlib.sha256(SEED_PHRASES["order_seed"].encode()).hexdigest()[:32]
BOOTSTRAP_SEED = hashlib.sha256(SEED_PHRASES["bootstrap_seed"].encode()).hexdigest()[:32]

ROW_KEYS = [
    "order_index", "stage", "stage_label", "run_id", "cell_id", "mechanism", "wire_mechanism",
    "fault", "fault_level", "load_events", "partitions", "consumer_instances",
    "ordering_variant", "load_mode", "injector_mode", "rep", "seed_key", "data_seed",
    "injection_seed", "executed", "day_block",
]


# --------------------------------------------------------------------------------------------
# seed formulas (prereg §4.4)
# --------------------------------------------------------------------------------------------
def _seed(kind: str, stage: str, condition: str, rep: int) -> str:
    """prereg §4.4: SHA-256(master_seed | "<kind>" | stage | condition | rep)[0:16] hex chars.

    The frozen formula writes concatenation without separators and a literal ``"data"``/``"inj"``
    marker; ``stage`` and ``condition`` are the lower-case tokens of docs/P2-RULINGS.md R2 and
    ``rep`` is the ASCII decimal repetition index.
    """
    text = MASTER_SEED + kind + stage + condition + str(rep)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------------------------
# matrix construction
# --------------------------------------------------------------------------------------------
def build_cells() -> list[dict]:
    cells: list[dict] = []

    def add(stage_label: str, stage_lower: str, mechanism: str, condition: str, load: str,
            events: int, partitions: int, reps: int, fault_level: str, ordering: str,
            injector_mode: str, load_mode: str = "fixed-rate", executed: bool = True,
            na_reason: str | None = None, block: str = "", extra: dict | None = None):
        cell = {
            "cell_id": f"{stage_label}.{mechanism}.{condition}.{load}.p{partitions}"
                       + (".sat" if load_mode == "saturation" else ""),
            "stage": stage_lower,
            "stage_label": stage_label,
            "mechanism": mechanism,
            "condition": condition,
            "fault": DES.record_fault(condition),
            "fault_level": fault_level,
            "load_token": load,
            "load_events": events,
            "partitions": partitions,
            "consumer_instances": DES.CONSUMER_INSTANCES,
            "ordering_variant": ordering,
            "load_mode": load_mode,
            "injector_mode": injector_mode,
            "reps": reps,
            "n_planned_slots": reps,
            "executed": executed,
            "na_reason": na_reason,
            "block": block,
        }
        if extra:
            cell.update(extra)
        cells.append(cell)

    # ---- Stage P — pilot: 9 mechanisms × 8 fault conditions × N=3 × 1k × P=3 -----------------
    for mech in DES.MECHANISMS:
        for fault in DES.FAULTS:
            na = fault in DES.PILOT_NA.get(mech, set())
            ordering = "early" if (fault == "F4b" and not na) else "strict"
            add("P", "pilot", mech, fault.lower(), "1k", 1_000, 3, 3,
                fault_level="L2" if fault != "F0" else "none",
                ordering=ordering, injector_mode="live" if fault != "F0" else "none",
                executed=not na,
                na_reason=(DES.NA_REASONS.get((mech, fault)) if na else None),
                block="pilot")

    # ---- Stage A — deterministic crash classes + controls: 1,830 runs ------------------------
    for mech in DES.MECHANISMS:
        for klass in DES.crash_classes(mech):
            ordering = "early" if klass == "K4" else "strict"
            # R3: single ownership table decides the injector mode
            mode = ("failpoint" if DES.injector_ownership(klass) == "app" else "external")
            add("A", "a", mech, klass.lower(), "1k", 1_000, 3, 30,
                fault_level="L1", ordering=ordering, injector_mode=mode,
                block="A-crash")
        add("A", "a", mech, "ctrl", "1k", 1_000, 3, 30,
            fault_level="none", ordering="strict", injector_mode="none", block="A-control")

    # ---- Stage B — stochastic faults at realistic load: 4,020 runs ---------------------------
    for mech in DES.MECHANISMS:
        for fault in DES.FAULTS:
            if fault in DES.STAGE_B_NA.get(mech, set()):
                continue
            ordering = "early" if fault == "F4b" else "strict"
            level = "L2" if fault != "F0" else "none"
            if fault == "F0":
                mode = "none"
            elif DES.injector_ownership(fault) == "app":
                mode = "failpoint"     # F4b deterministic ordering probe (app-owned)
            else:
                mode = "live"
            # core: 100k / P=6 / N=30
            add("B", "b", mech, fault.lower(), "100k", 100_000, 6, 30,
                fault_level=level, ordering=ordering, injector_mode=mode, block="B-core")
            # sensitivity S1 (load): 10k / P=6 / N=10 (D7 literal second block is S2)
            add("B", "b", mech, fault.lower(), "10k", 10_000, 6, 10,
                fault_level=level, ordering=ordering, injector_mode=mode, block="B-S1")
            # sensitivity S2 (partitions): 100k / P∈{3,12} / N=10
            for p in (3, 12):
                add("B", "b", mech, fault.lower(), "100k", 100_000, p, 10,
                    fault_level=level, ordering=ordering, injector_mode=mode, block="B-S2")

    # ---- Stage C — performance, fault-free: 1,800 runs --------------------------------------
    for mech in DES.MECHANISMS:
        for load, events in (("10k", 10_000), ("100k", 100_000)):
            for p in (3, 6, 12):
                add("C", "c", mech, "f0", load, events, p, 30,
                    fault_level="none", ordering="strict", injector_mode="none",
                    load_mode="fixed-rate", block="C-core")
        # 1M block: P=6, N=10
        add("C", "c", mech, "f0", "1m", 1_000_000, 6, 10,
            fault_level="none", ordering="strict", injector_mode="none",
            load_mode="fixed-rate", block="C-1m")
        # capacity block: saturation mode, 100k / P=6 / N=10 (uniquified by the `sat` token)
        add("C", "c", mech, "f0", "100k", 100_000, 6, 10,
            fault_level="none", ordering="strict", injector_mode="none",
            load_mode="saturation", block="C-capacity")
    # ---- M7 calibration block (author-approved at P2): 9 mechanisms x {10k,100k} x P6 x N=3
    # Fault-free capacity runs used ONLY to derive the fixed/saturation rates (R12).  It is a
    # separate stage token ("cal") that does not collide with P/A/B/C, is included in the
    # reserved list and run order, and is excluded from confirmatory hypothesis tables.
    for mech in DES.MECHANISMS:
        for load, events in (("10k", 10_000), ("100k", 100_000)):
            add("M7", "m7", mech, "cal", load, events, 6, 3,
                fault_level="none", ordering="strict", injector_mode="none",
                load_mode="fixed-rate", block="M7-cal", extra={"calibration": True})
    return cells


def build_validation_runs() -> list[dict]:
    """The 20 dedicated validation runs (prereg §2.3; categories per docs/method.md §7.6).

    Composition (recorded in matrix.yaml so the count is auditable):
      6 sham-injection runs  (M2 × F1,F2,F3,F4a,F5,F6 — injector armed, no action)
      2 C=1 robustness runs  (M2 F0 C=1; M4+M3 F0 C=1)
      6 injector self-tests  (M2 × F1,F2,F3,F4a,F5,F6 — fire + restore receipts)
      2 quiescence calibration runs (M2 F0, M3 F0 — T_quiet doubled, G7)
      4 oracle-fixture validations (clean + loss + duplicate + mutation defect classes, G3)
    """
    runs: list[dict] = []

    def add(kind: str, index: int, mechanism: str, fault: str, *, consumer_instances: int = 2,
            ordering: str = "strict", notes: str = ""):
        runs.append({
            "kind": kind,
            "index": index,
            "run_id": f"pilot-valid-{kind}-r{index:02d}",
            "stage": "pilot",
            "stage_label": "P",
            "mechanism": mechanism,
            "condition": f"valid-{kind}-{fault.lower()}",
            "fault": fault,
            "fault_level": "L2" if fault not in ("F0",) else "none",
            "load_token": "1k",
            "load_events": 1_000,
            "partitions": 3,
            "consumer_instances": consumer_instances,
            "ordering_variant": ordering,
            "load_mode": "fixed-rate",
            # g1 gate-control validation runs are app-owned failpoints (M0 K3/K5)
            # g1k3 = app-owned failpoint; g1k5 = S4-owned protocol replay (ownership table)
            "injector_mode": ("sham" if kind == "sham" else
                              "selftest" if kind == "selftest" else
                              ("failpoint" if fault in ("K3",) else "external")
                              if kind.startswith("g1") else
                              "none" if kind in ("c1", "quiesce") else "fixture"),
            "rep": index,
            "notes": notes,
        })

    i = 1
    # mechanisms are spread across the validation slate so that the prereg §4.3 interleaving
    # constraints also hold for the validation block (the categories are fixed, the mechanism
    # carrying each probe is not: a DB-sink mechanism exercises the same injector code path).
    sham_mechs = ["M2", "M3", "M4", "M2+M3", "M4+M3", "M1x"]
    for fault, mech in zip(("F1", "F2", "F3", "F4a", "F5", "F6"), sham_mechs):
        add("sham", i, mech, fault, notes="injector armed, no action (sham mode)"); i += 1
    add("c1", i, "M2", "F0", consumer_instances=1, notes="C=1 robustness"); i += 1
    add("c1", i, "M4+M3", "F0", consumer_instances=1, notes="C=1 robustness"); i += 1
    selftest_mechs = ["M2", "M3", "M4", "M2+M3", "M4+M3", "M1x"]
    for fault, mech in zip(("F1", "F2", "F3", "F4a", "F5", "F6"), selftest_mechs):
        add("selftest", i, mech, fault, notes="injector fire+restore receipt self-test"); i += 1
    add("quiesce", i, "M2", "F0", notes="T_quiet doubled, count stability (G7)"); i += 1
    add("quiesce", i, "M3", "F0", notes="T_quiet doubled, count stability (G7)"); i += 1
    for defect in ("clean", "loss", "duplicate", "mutation"):
        add("oracle", i, "M2", "F0",
            notes=f"oracle fixture integration: {defect} (synthetic; G3/G5 end-to-end)"); i += 1
    # G1 pre-Stage-A gate control (S5 F-E5): deterministic M0 K3 and K5 runs with attested
    # injections, so G1 can be decided before Stage-A data is analysed.
    for klass in ("K3", "K5"):
        for rep in range(3):
            add(f"g1{klass.lower()}", i, "M0", klass,
                notes=f"G1 gate control: M0 {klass} with attested injection (pre-Stage-A)")
            i += 1
    if len(runs) != DES.VALIDATION_RUNS:
        raise AssertionError(f"validation run composition must be {DES.VALIDATION_RUNS}, "
                             f"got {len(runs)}")
    return runs


def expand_runs(cells: list[dict], validation: list[dict]) -> list[dict]:
    runs: list[dict] = []
    for cell in cells:
        if not cell["executed"]:
            continue
        for rep in range(cell["reps"]):
            stage = cell["stage"]
            mech_token = DES.MECH_TOKEN[cell["mechanism"]]
            cond_token = cell["condition"]
            parts = [stage, mech_token, cond_token, cell["load_token"],
                     f"p{cell['partitions']}"]
            if cell["load_mode"] == "saturation":
                parts.append("sat")
            parts.append(f"r{rep:02d}")
            run_id = "-".join(parts)
            seed_condition = cond_token
            runs.append({
                "run_id": run_id,
                "cell_id": cell["cell_id"],
                "stage": stage,
                "stage_label": cell["stage_label"],
                "mechanism": cell["mechanism"],
                "wire_mechanism": cell["mechanism"],
                "fault": cell["fault"],
                "fault_level": cell["fault_level"],
                "load_events": cell["load_events"],
                "partitions": cell["partitions"],
                "consumer_instances": cell["consumer_instances"],
                "ordering_variant": cell["ordering_variant"],
                "load_mode": cell["load_mode"],
                "injector_mode": cell["injector_mode"],
                "rep": rep,
                "seed_key": f"{stage}|{seed_condition}|{rep}",
                "data_seed": _seed("data", stage, seed_condition, rep),
                "injection_seed": _seed("inj", stage, seed_condition, rep),
                "executed": True,
                "validation_kind": None,
            })
    for v in validation:
        stage = "pilot"
        seed_condition = v["condition"]
        runs.append({
            "run_id": v["run_id"],
            "cell_id": f"V.{v['kind']}.{v['mechanism']}.{v['fault']}.1k.p3",
            "stage": stage,
            "stage_label": "V",
            "mechanism": v["mechanism"],
            "wire_mechanism": v["mechanism"],
            "fault": v["fault"],
            "fault_level": v["fault_level"],
            "load_events": v["load_events"],
            "partitions": v["partitions"],
            "consumer_instances": v["consumer_instances"],
            "ordering_variant": v["ordering_variant"],
            "load_mode": v["load_mode"],
            "injector_mode": v["injector_mode"],
            "rep": v["rep"],
            "seed_key": f"{stage}|{seed_condition}|{v['rep']}",
            "data_seed": _seed("data", stage, seed_condition, v["rep"]),
            "injection_seed": _seed("inj", stage, seed_condition, v["rep"]),
            "executed": True,
            "validation_kind": v["kind"],
            "notes": v["notes"],
        })
    return runs


# --------------------------------------------------------------------------------------------
# randomised, interleaved run order (prereg §4.3)
# --------------------------------------------------------------------------------------------
def _violations(order: list[dict], max_run: int = 3, block: int = 60,
                max_share: float = 0.40) -> list[str]:
    problems: list[str] = []
    mechs = [r["mechanism"] for r in order]
    for i in range(len(mechs)):
        if i >= max_run and len(set(mechs[i - max_run: i + 1])) == 1:
            problems.append(f"run>={max_run + 1} identical mechanisms ending at index {i}")
            break
    n = len(mechs)
    cap_full = max(1, int(max_share * block))
    for start in range(0, n):
        window = mechs[start: start + block]
        if not window:
            break
        cap = max(1, int(max_share * len(window)))
        counts: dict[str, int] = {}
        for m in window:
            counts[m] = counts.get(m, 0) + 1
        if max(counts.values()) > cap:
            problems.append(f"block {start}..{start + len(window) - 1} share cap "
                            f"{cap} exceeded (max {max(counts.values())})")
            break
    return problems


def _shuffle_with_constraints(runs: list[dict], rng: random.Random,
                              max_attempts: int = 200) -> list[dict]:
    """Seeded Fisher–Yates shuffle + deterministic repair of the interleaving constraints."""
    if not runs:
        return []
    base = list(runs)
    for attempt in range(max_attempts):
        order = list(base)
        rng.shuffle(order)
        for _pass in range(60):
            changed = False
            for i in range(3, len(order)):
                if (order[i]["mechanism"] == order[i - 1]["mechanism"]
                        == order[i - 2]["mechanism"] == order[i - 3]["mechanism"]):
                    prev = order[i - 1]["mechanism"]
                    candidates = [j for j in range(i + 1, len(order))
                                  if order[j]["mechanism"] != prev]
                    if not candidates:
                        break
                    j = rng.choice(candidates)
                    order[i], order[j] = order[j], order[i]
                    changed = True
            if not changed:
                break
        problems = _violations(order)
        if not problems:
            return order
    raise RuntimeError(f"could not satisfy interleaving constraints after {max_attempts} "
                       f"seeded attempts: {problems[:3]}")


def build_run_order(runs: list[dict]) -> list[dict]:
    """Stage order pilot → A → B → C; within a stage, seeded interleaved shuffle.

    The prereg §4.3 constraints (≤3 consecutive runs of one mechanism; ≤40 % of a 60-run day
    block from one mechanism) are enforced on the *whole* realised sequence, including the
    pilot/validation and stage boundaries, by a deterministic repair pass.
    """
    pilot = [r for r in runs if r["validation_kind"] is None and r["stage"] == "pilot"]
    validation = [r for r in runs if r["validation_kind"] is not None]
    stage_a = [r for r in runs if r["stage"] == "a"]
    stage_b = [r for r in runs if r["stage"] == "b"]
    stage_c = [r for r in runs if r["stage"] == "c"]
    stage_m7 = [r for r in runs if r["stage"] == "m7"]
    groups = [("pilot", pilot + validation), ("A", stage_a), ("B", stage_b),
              ("C", stage_c), ("M7", stage_m7)]
    base_int = int(ORDER_SEED, 16)
    order: list[dict] = []
    section_ranges: list[tuple[int, int]] = []
    for idx, (label, group) in enumerate(groups):
        rng = random.Random(base_int ^ ((idx + 1) * 0x9E3779B97F4A7C15 & 0xFFFFFFFFFFFFFFFF))
        try:
            shuffled = _shuffle_with_constraints(group, rng)
        except RuntimeError:
            shuffled = _shuffle(group, rng)
        start = len(order)
        order.extend(shuffled)
        section_ranges.append((start, len(order)))
    repair_rng = random.Random((base_int ^ 0xC0FFEE) & 0xFFFFFFFFFFFFFFFF)
    order = _repair_order(order, section_ranges, repair_rng)
    problems = _violations(order)
    if problems:
        raise RuntimeError("realised run order violates prereg §4.3 constraints: "
                           + "; ".join(problems[:3]))
    for i, run in enumerate(order):
        run["order_index"] = i
        run["day_block"] = i // 60
    return order


def _section_of(index: int, section_ranges: list[tuple[int, int]]) -> tuple[int, int]:
    for start, end in section_ranges:
        if start <= index < end:
            return start, end
    raise IndexError(index)


def _repair_order(order: list[dict], section_ranges: list[tuple[int, int]],
                  rng: random.Random, max_passes: int = 400) -> list[dict]:
    """Deterministic local repair of consecutive-run and day-block-share violations."""
    max_run, block, max_share = 3, 60, 0.40
    cap = max(1, int(max_share * block))
    for _pass in range(max_passes):
        changed = False
        # (a) no more than `max_run` consecutive runs of the same mechanism
        for i in range(max_run, len(order)):
            if len({order[i - k]["mechanism"] for k in range(max_run + 1)}) != 1:
                continue
            mech = order[i]["mechanism"]
            start, end = _section_of(i, section_ranges)
            candidates = [j for j in range(start, end) if order[j]["mechanism"] != mech]
            if not candidates:
                continue
            j = rng.choice(candidates)
            order[i], order[j] = order[j], order[i]
            changed = True
            break
        if changed:
            continue
        # (b) no more than 40 % of any 60-run block from one mechanism
        for start in range(0, max(0, len(order) - block + 1)):
            window = order[start: start + block]
            counts: dict[str, int] = {}
            for run in window:
                counts[run["mechanism"]] = counts.get(run["mechanism"], 0) + 1
            mech, count = max(counts.items(), key=lambda kv: kv[1])
            if count <= cap:
                continue
            swap = None
            for i in range(start, start + block):
                if order[i]["mechanism"] != mech:
                    continue
                sec_start, sec_end = _section_of(i, section_ranges)
                outside = list(range(sec_start, start)) + list(range(start + block, sec_end))
                for j in outside:
                    if 0 <= j < len(order) and order[j]["mechanism"] != mech:
                        swap = (i, j)
                        break
                if swap:
                    break
            if swap:
                i, j = swap
                order[i], order[j] = order[j], order[i]
                changed = True
                break
        if not changed:
            return order
    return order


def _shuffle(runs: list[dict], rng: random.Random) -> list[dict]:
    out = list(runs)
    rng.shuffle(out)
    return out


# --------------------------------------------------------------------------------------------
# accounting + writing
# --------------------------------------------------------------------------------------------
def accounts(cells: list[dict], validation: list[dict]) -> dict:
    stages = {}
    for label, key in (("P", "pilot"), ("A", "A"), ("B", "B"), ("C", "C")):
        planned = sum(c["n_planned_slots"] for c in cells if c["stage_label"] == label)
        executable = sum(c["n_planned_slots"] for c in cells
                         if c["stage_label"] == label and c["executed"])
        stages[key] = {
            "cells_total": sum(1 for c in cells if c["stage_label"] == label),
            "cells_executed": sum(1 for c in cells
                                  if c["stage_label"] == label and c["executed"]),
            "cells_na": sum(1 for c in cells
                            if c["stage_label"] == label and not c["executed"]),
            "runs_planned": planned,
            "runs_executable": executable,
        }
    totals = {
        "runs_planned": sum(s["runs_planned"] for s in stages.values()),
        "runs_executable": sum(s["runs_executable"] for s in stages.values()),
        "validation_runs": len(validation),
        "nominal_total_planned": sum(s["runs_planned"] for s in stages.values()),
        "nominal_total_executable": sum(s["runs_executable"] for s in stages.values()),
        "nominal_total_executable_plus_validation":
            sum(s["runs_executable"] for s in stages.values()) + len(validation),
    }
    calibration = {
        "stage": "M7",
        "block": "M7-cal",
        "cells": sum(1 for c in cells if c["stage_label"] == "M7"),
        "runs": sum(c["n_planned_slots"] for c in cells
                    if c["stage_label"] == "M7" and c["executed"]),
        "note": "author-approved P2 calibration block; separate from the 7,506/7,488 "
                "confirmatory counts; used only to derive fixed/saturation rates (R12)",
    }
    return {"stages": stages, "totals": totals, "calibration": calibration}


def _matrix_document(cells: list[dict], validation: list[dict], acc: dict) -> dict:
    return {
        "schema": "wxeo.matrix.v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generator": "experiments/matrix.py",
        "normative_sources": {
            "matrices": "docs/prereg.md §2",
            "run_protocol": "docs/prereg.md §4",
            "reachability": "docs/method.md §5.3",
            "rulings": "docs/P2-RULINGS.md R1–R4",
            "interface": "docs/CONTRACTS.md §2, §4",
        },
        "prereg_version": "1.0-draft + P1 approval + P2 rulings R1–R4",
        "seeds": {
            "master_seed": MASTER_SEED,
            "order_seed": ORDER_SEED,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "generation": {k: f"sha256(UTF-8({v!r}))[:{32}]" for k, v in SEED_PHRASES.items()},
            "data_seed_formula": 'first 16 hex of sha256(UTF-8(master_seed + "data" + '
                                 'stage_token + condition_token + decimal(rep)))',
            "injection_seed_formula": 'first 16 hex of sha256(UTF-8(master_seed + "inj" + '
                                      'stage_token + condition_token + decimal(rep)))',
            "pairing": "all mechanisms in the same (stage, condition, rep) share data_seed "
                       "(docs/prereg.md §4.4, D15)",
        },
        "factors": {
            "mechanisms": DES.MECHANISMS,
            "mechanism_tokens": DES.MECH_TOKEN,
            "faults": DES.FAULTS,
            "crash_classes": list(DES.CRASH_CLASSES),
            "loads": {"1k": 1_000, "10k": 10_000, "100k": 100_000, "1m": 1_000_000},
            "partitions": [3, 6, 12],
            "consumer_instances": {"canonical": 2, "pilot_robustness": 1},
            "ordering_variants": ["strict", "early"],
            "load_modes": ["fixed-rate", "saturation"],
            "repetitions": {"pilot": 3, "stage_a": 30, "stage_b_core": 30,
                            "sensitivity": 10, "stage_c_core": 30, "stage_c_blocks": 10},
        },
        "reachability": {
            "stage_a_crash_classes": {m: DES.crash_classes(m) for m in DES.MECHANISMS},
            "stage_a_na_classes": {m: DES.STAGE_A_NA.get(m, {}) for m in DES.MECHANISMS},
            "stage_a_s4_realised": sorted(DES.S4_REALISED_CLASSES),
            "stage_a_app_failpoint": sorted(DES.APP_FAILPOINT_CLASSES),
            "k6_policy": "K6 (zombie writer C7) is structurally not realisable inside the frozen "
                         "max.poll.interval.ms=300 s / T_max=300 s configuration (S5 F-E2); it is "
                         "declared n/a for every mechanism and the M4/C7 claim is model-only in "
                         "the paper (recorded deviation).",
            "pilot_fault_na": {m: sorted(DES.PILOT_NA.get(m, set())) for m in DES.MECHANISMS},
            "stage_b_fault_na": {m: sorted(DES.STAGE_B_NA.get(m, set()))
                                 for m in DES.MECHANISMS},
            "na_policy": "n/a cells are recorded with executed=false and the reachability "
                         "reason; they are never counted as passes (docs/method.md §5.3)",
        },
        "accounting": acc,
        "cells": cells,
        "validation": validation,
    }


def _run_order_rows(order: list[dict]) -> list[list]:
    rows = []
    for run in order:
        rows.append([run.get(k) for k in ROW_KEYS])
    return rows


def write_outputs(out_dir: str) -> dict:
    import yaml

    cells = build_cells()
    validation = build_validation_runs()
    runs = expand_runs(cells, validation)
    order = build_run_order(runs)
    acc = accounts(cells, validation)

    # integrity assertions against the prereg arithmetic (fail loudly, never silently)
    expected = {
        "pilot": (DES.PILOT_PLANNED, DES.PILOT_EXECUTABLE),
        "A": (DES.STAGE_A_RUNS, DES.STAGE_A_RUNS),
        "B": (DES.STAGE_B_RUNS, DES.STAGE_B_RUNS),
        "C": (DES.STAGE_C_RUNS, DES.STAGE_C_RUNS),
    }
    for stage, (planned, executable) in expected.items():
        got = acc["stages"][stage]
        if (got["runs_planned"], got["runs_executable"]) != (planned, executable):
            raise AssertionError(
                f"stage {stage}: planned/executable {got['runs_planned']}/"
                f"{got['runs_executable']} != prereg {planned}/{executable}")
    if len(order) != (acc["totals"]["runs_executable"] + len(validation)
                      + acc["calibration"]["runs"]):
        raise AssertionError("run order length mismatch")
    # run ids must be unique, lower-case, Kafka-topic-legal
    import re
    seen = set()
    for run in order:
        rid = run["run_id"]
        if not re.fullmatch(r"[a-z0-9-]+", rid):
            raise AssertionError(f"run_id not topic-legal: {rid}")
        if rid in seen:
            raise AssertionError(f"duplicate run_id: {rid}")
        seen.add(rid)
    os.makedirs(out_dir, exist_ok=True)
    matrix_path = os.path.join(out_dir, "matrix.yaml")
    order_path = os.path.join(out_dir, "run_order.csv")
    doc = _matrix_document(cells, validation, acc)
    with open(matrix_path, "w", encoding="utf-8", newline="") as fh:
        yaml.safe_dump(doc, fh, sort_keys=False, allow_unicode=True, width=200)
    with open(order_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(ROW_KEYS)
        writer.writerows(_run_order_rows(order))
    matrix_sha = _sha256_file(matrix_path)
    order_sha = _sha256_file(order_path)
    with open(os.path.join(out_dir, "matrix.sha256"), "w", encoding="utf-8", newline="") as fh:
        fh.write(f"{matrix_sha}  matrix.yaml\n{order_sha}  run_order.csv\n")
    return {
        "matrix_path": matrix_path,
        "order_path": order_path,
        "matrix_sha256": matrix_sha,
        "order_sha256": order_sha,
        "accounting": acc,
        "order_rows": len(order),
        "seeds": {"master_seed": MASTER_SEED, "order_seed": ORDER_SEED,
                  "bootstrap_seed": BOOTSTRAP_SEED},
        "constraints": _order_constraint_report(order),
    }


def _order_constraint_report(order: list[dict]) -> dict:
    mechs = [r["mechanism"] for r in order]
    max_consecutive = 0
    cur = 0
    for i, m in enumerate(mechs):
        cur = cur + 1 if i and m == mechs[i - 1] else 1
        max_consecutive = max(max_consecutive, cur)
    worst_share = 0.0
    for start in range(0, max(0, len(mechs) - 59), 1):
        window = mechs[start:start + 60]
        if len(window) < 60:
            break
        counts: dict[str, int] = {}
        for m in window:
            counts[m] = counts.get(m, 0) + 1
        worst_share = max(worst_share, max(counts.values()) / 60.0)
    return {
        "max_consecutive_same_mechanism": max_consecutive,
        "worst_60_run_block_share": round(worst_share, 4),
        "constraints": {"max_consecutive": 3, "max_block_share": 0.40},
        "violations": _violations(order),
    }


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_outputs() -> dict:
    import yaml

    if not (os.path.exists(MATRIX_PATH) and os.path.exists(ORDER_PATH)):
        raise SystemExit("matrix files missing; run without --check first")
    with open(MATRIX_PATH, "r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    acc = doc["accounting"]
    expected = {
        "pilot": (DES.PILOT_PLANNED, DES.PILOT_EXECUTABLE),
        "A": (DES.STAGE_A_RUNS, DES.STAGE_A_RUNS),
        "B": (DES.STAGE_B_RUNS, DES.STAGE_B_RUNS),
        "C": (DES.STAGE_C_RUNS, DES.STAGE_C_RUNS),
    }
    problems = []
    for stage, (planned, executable) in expected.items():
        got = acc["stages"][stage]
        if (got["runs_planned"], got["runs_executable"]) != (planned, executable):
            problems.append(f"{stage}: {got['runs_planned']}/{got['runs_executable']} != "
                            f"{planned}/{executable}")
    with open(ORDER_PATH, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    calibration_runs = int((acc.get("calibration") or {}).get("runs") or 0)
    if len(rows) != (acc["totals"]["runs_executable"] + acc["totals"]["validation_runs"]
                     + calibration_runs):
        problems.append(f"run_order rows {len(rows)} != executable+validation+calibration")
    if calibration_runs != DES.M7_CAL_RUNS:
        problems.append(f"M7 calibration runs {calibration_runs} != {DES.M7_CAL_RUNS}")
    with open(os.path.join(REPO, "experiments", "matrix.sha256"), encoding="utf-8") as fh:
        recorded = {line.split()[1]: line.split()[0]
                    for line in fh if line.strip()}
    if recorded.get("matrix.yaml") != _sha256_file(MATRIX_PATH):
        problems.append("matrix.yaml hash changed")
    if recorded.get("run_order.csv") != _sha256_file(ORDER_PATH):
        problems.append("run_order.csv hash changed")
    return {"problems": problems, "ok": not problems, "rows": len(rows), "accounting": acc}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate WXEO matrix + run order")
    parser.add_argument("--out-dir", default=os.path.join(REPO, "experiments"))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    if args.check:
        report = check_outputs()
        print(json.dumps(report, indent=2))
        return 0 if report["ok"] else 1
    info = write_outputs(args.out_dir)
    print(json.dumps(info, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
