"""Frozen design constants for the WXEO experiment matrix.

Every value here is transcribed from the frozen documents and must not be tuned:

  * mechanisms and wire values — ``docs/CONTRACTS.md`` §7, ``docs/AMENDMENT-A1.md`` §1;
  * fault conditions — ``docs/prereg.md`` §2.1, ``docs/method.md`` §5.4;
  * crash classes K1–K7 and the mechanism × crash-class reachability — ``docs/method.md`` §5.3;
  * n/a cells — ``docs/prereg.md`` §2.2 (67 executed Stage-B fault cells of 72; 52 executed
    Stage-A crash cells of 63) and ``docs/method.md`` §5.3 footnotes;
  * repetitions, loads, partitions — ``docs/prereg.md`` §2.3–§2.6.

The generator (``experiments/matrix.py``) asserts the resulting counts against the prereg
arithmetic; if a constant here is wrong the generator fails loudly rather than silently
changing the design.
"""

from __future__ import annotations

MECHANISMS = [
    "M0", "M1-kafka", "M1x", "M2", "M3", "M4", "M2+M3", "M4+M3", "M1x+M3",
]

#: run_id mechanism tokens (docs/P2-RULINGS.md R2; no '+' because Kafka topic names allow
#: only [a-zA-Z0-9._-])
MECH_TOKEN = {
    "M0": "m0",
    "M1-kafka": "m1-kafka",
    "M1x": "m1x",
    "M2": "m2",
    "M3": "m3",
    "M4": "m4",
    "M2+M3": "m2m3",
    "M4+M3": "m4m3",
    "M1x+M3": "m1xm3",
}

MECHWIRE_TO_TOKEN = dict(MECH_TOKEN)

FAULTS = ["F0", "F1", "F2", "F3", "F4a", "F4b", "F5", "F6"]

CRASH_CLASSES = ["K1", "K2", "K3", "K4", "K5", "K6", "K7"]

CONSUMER_INSTANCES = 2

#: Stage A executed crash classes per mechanism (docs/method.md §5.3): R + probe(K4).
#: K4 is executed only under the ``ordering=early`` probe (D2); it is not "n/a" but "probe".
STAGE_A_REACHABLE = {
    "M0": ["K1", "K2", "K3", "K5", "K6"],           # K4 probe appended below
    "M1-kafka": ["K1", "K2", "K5", "K6"],           # K3, K4, K7 n/a (single-site atomic)
    "M1x": ["K1", "K2", "K3", "K5", "K6"],
    "M2": ["K1", "K2", "K3", "K5", "K6", "K7"],
    "M3": ["K1", "K2", "K3", "K5", "K6"],
    "M4": ["K1", "K2", "K5", "K6"],                 # K3, K7 n/a (atomic effect+progress)
    "M2+M3": ["K1", "K2", "K3", "K5", "K6", "K7"],
    "M4+M3": ["K1", "K2", "K5", "K6"],
    "M1x+M3": ["K1", "K2", "K3", "K5", "K6"],
}

#: mechanisms for which the K4 W2 probe is reachable (all except M1-kafka: 8 cells)
K4_PROBE = ["M0", "M1x", "M2", "M3", "M4", "M2+M3", "M4+M3", "M1x+M3"]

#: Stage B / pilot n/a fault cells (docs/prereg.md §2.2: 5 n/a cells / 67 executed)
PILOT_NA = {
    "M1-kafka": {"F4a", "F4b", "F5"},
    "M4": {"F4b"},
    "M4+M3": {"F4b"},
}
STAGE_B_NA = {m: set(v) for m, v in PILOT_NA.items()}

NA_REASONS = {
    ("M1-kafka", "F4a"): "n/a: no external DB sink, offset-commit/DB divergence unreachable "
                         "(docs/method.md §5.3 footnote 1)",
    ("M1-kafka", "F4b"): "n/a: ordering probe needs two distinct commits; M1-kafka has one "
                         "Kafka transaction (docs/method.md §5.3 footnote 1, D2)",
    ("M1-kafka", "F5"): "n/a: no app-DB link to partition (docs/prereg.md §2.2)",
    ("M4", "F4b"): "n/a: effect+progress in one PostgreSQL transaction; early-ordering probe "
                   "is not the canonical M4 mechanism (docs/method.md §5.3 footnote 2, D2)",
    ("M4+M3", "F4b"): "n/a: effect+progress in one PostgreSQL transaction "
                      "(docs/method.md §5.3 footnote 2, D2)",
}

#: Stage A n/a crash classes per mechanism (for reporting; never executed)
STAGE_A_NA = {
    "M0": ["K7"],
    "M1-kafka": ["K3", "K4", "K7"],
    "M1x": ["K7"],
    "M2": [],
    "M3": ["K7"],
    "M4": ["K3", "K7"],
    "M2+M3": [],
    "M4+M3": ["K3", "K7"],
    "M1x+M3": ["K7"],
}


def crash_classes(mechanism: str) -> list[str]:
    """Executed Stage-A crash classes for a mechanism: reachable R classes + K4 probe."""
    classes = list(STAGE_A_REACHABLE[mechanism])
    if mechanism in K4_PROBE:
        classes.append("K4")
    # keep canonical K1..K7 order
    return [k for k in CRASH_CLASSES if k in classes]


def condition_to_fault(condition: str) -> str:
    """Map a matrix condition token to the run-record ``fault`` value.

    Pilot/Stage-B conditions are lower-case fault tokens (``f1``), Stage-A conditions are crash
    classes (``k3``) or ``ctrl``.  Run-record ``fault`` is the upper-case fault/class or ``none``.
    """
    c = condition.lower()
    if c == "ctrl":
        return "none"
    if c.startswith("f") or c.startswith("k"):
        return c[0].upper() + c[1:]
    return condition


def validate_design() -> list[str]:
    """Self-check the frozen design against the prereg arithmetic; returns problems."""
    problems: list[str] = []
    if len(MECHANISMS) != 9:
        problems.append("expected 9 mechanisms")
    if len(FAULTS) != 8:
        problems.append("expected 8 fault conditions")
    # executed Stage-B cells: 8 per mechanism minus n/a
    b_cells = sum(len(FAULTS) - len(STAGE_B_NA.get(m, set())) for m in MECHANISMS)
    if b_cells != 67:
        problems.append(f"Stage-B executed fault cells {b_cells} != 67")
    # Stage-A executed crash cells: reachable + K4 probe
    a_cells = sum(len(crash_classes(m)) for m in MECHANISMS)
    if a_cells != 52:
        problems.append(f"Stage-A executed crash cells {a_cells} != 52")
    # pilot planned: 9*8*3=216; n/a slots 5*3=15
    na_cells = sum(len(PILOT_NA.get(m, set())) for m in MECHANISMS)
    if na_cells != 5:
        problems.append(f"pilot n/a cells {na_cells} != 5")
    return problems


if validate_design():
    raise AssertionError("frozen design constants inconsistent: " + "; ".join(validate_design()))
