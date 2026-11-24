"""Single-owner fault-receipt protocol (v2) — S5 F-I1/F-I2/I3/I5 fix.

The red-team review found the receipt accounting structurally broken: external injectors never
wrote a ``fired`` receipt, L1 ``pause`` actions produced two ``restored`` receipts, and an
"attested" injection need not have changed the system.  This module is the single source of truth
for the protocol, used by ``experiments/injectors.py`` (write side),
``experiments/runner.py`` (exclusion decision) and ``tools/analyze.py`` (gate G4 evidence).

Ownership (exactly one writer per receipt event)
-----------------------------------------------
* **App-owned failpoint** (``injector_mode == "failpoint"`` / any L1 crash-class run, and F4b):
  the Java service writes ``fired`` at the failpoint.  For non-terminal actions
  (``pause``/``throw``/``abort_txn``/``none``) it also writes ``restored`` after rollback/pause;
  S4 writes **nothing**.  For the terminal ``crash`` action the service writes only ``fired``
  (it halts); S4 writes the matching ``restored`` receipt after the instance is restarted, with a
  distinct fault label (the class id, e.g. ``K3``) so the pair is unambiguous.
* **S4-owned injection** (external Level-2 F1/F2/F3/F4a/F5/F6; and the Level-1 classes K2/K5 that
  are realised by S4 protocol/network actions): S4 writes exactly one ``fired`` (after *verifying*
  the action took effect, with phase information) and exactly one ``restored``.
* **Sham** runs write ``armed`` + ``restored`` (source ``s4``) and no ``fired``.

Every receipt carries ``source`` (``"app"`` or ``"s4"``) and, for S4 ``fired`` receipts,
``action_verified: true`` plus a ``phase`` block with the load window and the realised fire time.
"""

from __future__ import annotations

import json
import os
from collections import Counter

APP_FAILPOINTS = frozenset({
    "after_fetch", "before_effect_commit", "after_effect_commit", "before_progress_commit",
    "after_progress_commit", "before_publish", "before_relay_mark",
})

#: faults S4 realises with its own external actions (own both receipts)
S4_FAULTS = frozenset({"F1", "F2", "F3", "F4a", "F5", "F6", "K2", "K5"})

#: faults realised by an in-process app failpoint (app owns fired; see ownership table)
APP_OWNED_FAULTS = frozenset({"K1", "K3", "K4", "K7", "F4b"})

#: actions that are terminal inside the service (fired-only; S4 restores)
TERMINAL_ACTIONS = frozenset({"crash"})

#: default failpoint/action per Level-1 class (see experiments/injectors.py for the table)
DEFAULT_ACTION = {
    "K1": "crash",
    "K3": "crash",
    "K4": "crash",
    "K5": None,   # S4 protocol replay
    "K7": "crash",
    "F4b": "crash",
}


try:  # single ownership table (S5 R3)
    from experiments.matrix_design import injector_ownership as _ownership
except ImportError:  # pragma: no cover
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from experiments.matrix_design import injector_ownership as _ownership


def fault_owner(fault: str) -> str:
    try:
        return _ownership(fault)
    except Exception:  # noqa: BLE001
        return "s4"


def _receipt_instance(receipt: dict) -> int | None:
    for part in str(receipt.get("detail") or "").replace(";", " ").split():
        if part.startswith("instance="):
            try:
                return int(part.split("=", 1)[1])
            except ValueError:
                return None
    return None


def classify(receipt: dict) -> str:
    """Return ``"app"`` or ``"s4"`` for one receipt (explicit ``source`` wins)."""
    source = receipt.get("source")
    if source in ("app", "s4"):
        return source
    if receipt.get("fault") in APP_FAILPOINTS:
        return "app"
    return "s4"


def _mine(receipts: list[dict], run_id: str) -> list[dict]:
    return [r for r in receipts if r.get("run_id") == run_id and "malformed" not in r]


def validate_run_receipts(
    receipts: list[dict],
    *,
    run_id: str,
    injector_mode: str,
    fault: str,
    fault_level: str,
    failpoint: str | None = None,
    action: str | None = None,
    failpoint_instance: int | None = None,
    phase_window: tuple[int, int] | None = None,
) -> dict:
    """Validate one run's receipts against the single-owner protocol.

    Returns ``{attested, reason, owner, fired, restored, armed, observed, ...}``.  ``attested`` is
    the G4/decisive predicate used by the runner's exclusion rule and the analysis gate.
    """
    mine = _mine(receipts, run_id)
    counts = Counter()
    for receipt in mine:
        counts[(classify(receipt), receipt.get("event"))] += 1
    summary = {
        "owner": None,
        "fired": sum(v for (src, ev), v in counts.items() if ev == "fired"),
        "restored": sum(v for (src, ev), v in counts.items() if ev == "restored"),
        "armed": sum(v for (src, ev), v in counts.items() if ev == "armed"),
        "app_fired": counts[("app", "fired")],
        "app_restored": counts[("app", "restored")],
        "s4_fired": counts[("s4", "fired")],
        "s4_restored": counts[("s4", "restored")],
        "receipts": len(mine),
    }

    if injector_mode in ("none", "fixture"):
        summary.update(attested=True, reason=None, owner=None)
        return summary

    if injector_mode == "sham":
        ok = (summary["app_fired"] == 0 and summary["s4_fired"] == 0
              and summary["armed"] == 1 and summary["restored"] == 1)
        summary.update(attested=ok, owner="s4",
                       reason=None if ok else
                       f"sham run needs exactly one armed + one restored, no fired; got "
                       f"armed={summary['armed']} restored={summary['restored']} "
                       f"fired={summary['fired']}")
        return summary

    fault = (fault or "").upper() if (fault or "").upper() in ("K1", "K2", "K3", "K4", "K5",
                                                                "K6", "K7") else fault
    app_owned = fault_owner(fault) == "app"
    if app_owned:
        summary["owner"] = "app"
        if not failpoint:
            summary.update(attested=False, reason="app-owned injection without a failpoint name")
            return summary
        designated = int(failpoint_instance) if failpoint_instance is not None else None
        app_fired_all = [r for r in mine if classify(r) == "app"
                         and r.get("event") == "fired" and r.get("fault") == failpoint]
        app_fired = [r for r in app_fired_all
                     if designated is None or _receipt_instance(r) == designated]
        off_designated = [r for r in app_fired_all
                          if designated is not None
                          and _receipt_instance(r) not in (None, designated)]
        app_restored = [r for r in mine if classify(r) == "app"
                        and r.get("event") == "restored" and r.get("fault") == failpoint]
        s4_restored = [r for r in mine if classify(r) == "s4" and r.get("event") == "restored"]
        problems: list[str] = []
        if off_designated:
            problems.append(f"{len(off_designated)} app fired receipt(s) from a non-designated "
                            f"instance (designated={designated}); C=2 arming bug (R5)")
        if len(app_fired) != 1:
            problems.append(f"expected exactly one app fired for {failpoint}, "
                            f"got {len(app_fired)}")
        terminal = (action in TERMINAL_ACTIONS) if action else (fault in ("K1", "K3", "K4", "K7",
                                                                          "F4b"))
        if terminal:
            if app_restored:
                problems.append(f"terminal crash failpoint must not write its own restored "
                                f"(got {len(app_restored)})")
            if len(s4_restored) != 1:
                problems.append(f"S4 must write exactly one restored after restart, "
                                f"got {len(s4_restored)}")
        else:
            if len(app_restored) != 1:
                problems.append(f"non-terminal failpoint must write exactly one app restored, "
                                f"got {len(app_restored)}")
            if s4_restored:
                problems.append(f"S4 must not duplicate the restored receipt "
                                f"(got {len(s4_restored)})")
        if summary["s4_fired"]:
            problems.append(f"S4 must not write fired for an app-owned injection "
                            f"(got {summary['s4_fired']})")
        summary.update(attested=not problems, reason="; ".join(problems) if problems else None)
        return summary

    # ---- S4-owned external injection ---------------------------------------------------------
    summary["owner"] = "s4"
    if fault not in S4_FAULTS and fault_level == "L2":
        # Level-2 external faults are F1..F6; anything else here is a wiring error
        if fault not in S4_FAULTS:
            summary.update(attested=False,
                           reason=f"fault {fault!r} is not an S4-owned external injection")
            return summary
    s4_fired = [r for r in mine if classify(r) == "s4" and r.get("event") == "fired"]
    s4_restored = [r for r in mine if classify(r) == "s4" and r.get("event") == "restored"]
    app_any = [r for r in mine if classify(r) == "app"]
    problems = []
    if len(s4_fired) != 1:
        problems.append(f"expected exactly one S4 fired, got {len(s4_fired)}")
    if len(s4_restored) != 1:
        problems.append(f"expected exactly one S4 restored, got {len(s4_restored)}")
    if app_any:
        problems.append(f"unexpected app failpoint receipts on an S4-owned injection "
                        f"({len(app_any)})")
    if len(s4_fired) == 1:
        receipt = s4_fired[0]
        if not receipt.get("action_verified"):
            problems.append("fired receipt has action_verified=false (action not proven)")
        phase = receipt.get("phase") or {}
        fire_ms = phase.get("fire_ms")
        load_start = phase.get("load_start_ms")
        load_end = phase.get("load_end_ms")
        if not (isinstance(fire_ms, int) and isinstance(load_start, int)
                and isinstance(load_end, int)):
            problems.append("fired receipt lacks a complete phase block "
                            "(fire_ms/load_start_ms/load_end_ms)")
        else:
            if phase.get("outside_window"):
                problems.append("fault fired outside the processing window "
                                "(uninformative_fire_outside_window)")
            if not (load_start <= fire_ms <= load_end):
                problems.append(f"fire time {fire_ms} outside load window "
                                f"[{load_start}, {load_end}]")
            if phase.get("consumers_alive") is False:
                problems.append("consumers were not alive at fire time")
    summary.update(attested=not problems, reason="; ".join(problems) if problems else None)
    return summary


def receipt_stats(receipts: list[dict], *, run_ids: list[str], faults: dict[str, str]) -> dict:
    """Fire-rate accounting over a set of reserved runs (F-I5).

    ``faults`` maps run_id -> expected fault token.  Returns per-run status and aggregate counts.
    """
    wanted = set(run_ids)
    by_run: dict[str, list[dict]] = {rid: [] for rid in run_ids}
    for receipt in receipts:
        rid = receipt.get("run_id")
        if rid in wanted:
            by_run[rid].append(receipt)
    summary = {"runs": len(run_ids), "with_fired": 0, "with_restored": 0,
               "attested": 0, "missing": [], "orphan_restored": []}
    for rid in run_ids:
        mine = by_run[rid]
        events = Counter(r.get("event") for r in mine)
        if events.get("fired"):
            summary["with_fired"] += 1
        if events.get("restored"):
            summary["with_restored"] += 1
        if events.get("fired") and events.get("restored"):
            summary["attested"] += 1
        elif events.get("restored") and not events.get("fired"):
            summary["orphan_restored"].append(rid)
        else:
            summary["missing"].append(rid)
    summary["fire_rate"] = (summary["with_fired"] / summary["runs"]) if summary["runs"] else None
    summary["restore_rate"] = ((summary["with_restored"] / summary["with_fired"])
                               if summary["with_fired"] else None)
    return summary


__all__ = [
    "APP_FAILPOINTS",
    "S4_FAULTS",
    "APP_OWNED_FAULTS",
    "TERMINAL_ACTIONS",
    "DEFAULT_ACTION",
    "classify",
    "validate_run_receipts",
    "receipt_stats",
]
