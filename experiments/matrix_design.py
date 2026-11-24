"""Frozen design constants for the WXEO experiment matrix (post-S5 revision).

Transcribed from the frozen documents plus the S5 red-team fix pass rulings:

  * mechanisms and wire values — ``docs/CONTRACTS.md`` §7, ``docs/AMENDMENT-A1.md`` §1;
  * fault conditions — ``docs/prereg.md`` §2.1, ``docs/method.md`` §5.4;
  * crash classes K1–K7 and mechanism × crash-class reachability — ``docs/method.md`` §5.3 as
    amended by the S5 findings (K6 structurally unrealisable inside the frozen
    ``max.poll.interval.ms``/T_max window; M1-kafka has no external rewind without breaking the
    D5 positive control);
  * Stage-A tokens are canonical upper-case ``K1..K7`` in the run record; the lower-case form is
    used only inside ``run_id``/``cell_id`` tokens (Kafka-topic legality).

The generator (``experiments/matrix.py``) asserts the resulting counts against the prereg
arithmetic **as amended**; if a constant here is wrong the generator fails loudly rather than
silently changing the design.

S5-review accounting (recorded in results/DEVIATIONS.md and the P2 amendment):
  pilot 216 planned / 198 executable; Stage A 1,530; Stage B 3,960; Stage C 1,800;
  validation 26 (20 + 6 G1 gate-control runs).  Totals: 7,506 planned / 7,488 executable
  + 26 validation.

Calibration blocks (outside the confirmatory counts, docs/prereg-amendment-A5.md r2 + A6):
  M7-cal 54 (9 mechanisms x {10k,100k} x P6 x N=3); M7spot 54 (9 mechanisms x 6 spot blocks
  x N=1: five uncalibrated fixed-rate blocks + one saturation spot per mechanism); M7sat 9
  (Amendment A6: one 100k/P6 saturation-validation run per mechanism) = 117 runs.
  NOTE: the 1k/P3 block is deliberately NOT calibrated (A5.1: the frozen warm-up rule
  max(2000, 10 %) swallows all 1,000 events and Stage A/pilot run at max by design).
"""

from __future__ import annotations

MECHANISMS = [
    "M0", "M1-kafka", "M1x", "M2", "M3", "M4", "M2+M3", "M4+M3", "M1x+M3",
]

#: wire values passed to the service (CONTRACTS §7)
MECH_WIRE = {m: m for m in MECHANISMS}

#: run_id mechanism tokens (docs/P2-RULINGS.md R2; no '+' because Kafka topic names allow
#: only [a-zA-Z0-9._-])
MECH_TOKEN = {
    "M0": "m0", "M1-kafka": "m1-kafka", "M1x": "m1x", "M2": "m2", "M3": "m3", "M4": "m4",
    "M2+M3": "m2m3", "M4+M3": "m4m3", "M1x+M3": "m1xm3",
}

FAULTS = ["F0", "F1", "F2", "F3", "F4a", "F4b", "F5", "F6"]

CRASH_CLASSES = ["K1", "K2", "K3", "K4", "K5", "K6", "K7"]

CONSUMER_INSTANCES = 2

#: Stage A executed crash classes per mechanism (docs/method.md §5.3 as revised by S5):
#: K6 is not experimentally realisable inside the frozen configuration (a zombie writer needs the
#: owner paused beyond max.poll.interval.ms = 300 s, which exceeds T_max); it is declared n/a for
#: every mechanism with the reachability proof and reported as model-only in the paper.
#: M1-kafka K5 is n/a: duplicate delivery without an external rewind cannot occur when effect and
#: progress are one Kafka transaction, and the external rewind that would force it is excluded by
#: D5 (it would duplicate the positive control's own ledger).  K4 is executed only under the
#: ``ordering=early`` probe (D2), appended by :func:`crash_classes`.
STAGE_A_REACHABLE = {
    "M0": ["K1", "K2", "K3", "K5"],
    "M1-kafka": ["K1", "K2"],
    "M1x": ["K1", "K2", "K3", "K5"],
    "M2": ["K1", "K2", "K3", "K5", "K7"],
    "M3": ["K1", "K2", "K3", "K5"],
    "M4": ["K1", "K2", "K5"],
    "M2+M3": ["K1", "K2", "K3", "K5", "K7"],
    "M4+M3": ["K1", "K2", "K5"],
    "M1x+M3": ["K1", "K2", "K3", "K5"],
}

#: mechanisms for which the K4 W2 probe is reachable (all except M1-kafka: 8 probe cells)
K4_PROBE = ["M0", "M1x", "M2", "M3", "M4", "M2+M3", "M4+M3", "M1x+M3"]

#: pilot / Stage-B n/a fault cells (docs/prereg.md §2.2 + S5 F-E4: F6 is n/a for M1-kafka)
PILOT_NA = {
    "M1-kafka": {"F4a", "F4b", "F5", "F6"},
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
    ("M1-kafka", "F6"): "n/a (S5 F-E4 / D5): protocol-level replay rewinds the committed offset, "
                        "which for M1-kafka is its own ledger resume point; forcing reprocessing "
                        "duplicates the positive control's ledger. External rewind is excluded by "
                        "D5; reported as a threat, not measured.",
    ("M4", "F4b"): "n/a: effect+progress in one PostgreSQL transaction; early-ordering probe "
                   "is not the canonical M4 mechanism (docs/method.md §5.3 footnote 2, D2)",
    ("M4+M3", "F4b"): "n/a: effect+progress in one PostgreSQL transaction "
                      "(docs/method.md §5.3 footnote 2, D2)",
}

#: every mechanism × crash-class that is NOT executed, with the reachability reason
STAGE_A_NA = {
    "M0": {"K6": "K6 n/a: zombie writer not realisable within max.poll.interval.ms=300 s / "
                  "T_max=300 s (S5 F-E2); model-only",
           "K7": "K7 n/a: no publication relay for this mechanism"},
    "M1-kafka": {"K3": "M1-kafka n/a: single-site atomic transaction (method §5.3 fn 1)",
                 "K4": "M1-kafka n/a: one Kafka transaction, no two-commit ordering",
                 "K5": "M1-kafka n/a: no external rewind without breaking D5 (S5 F-E1)",
                 "K6": "K6 n/a: zombie writer not realisable (S5 F-E2)",
                 "K7": "K7 n/a: M1-kafka publication stream is its ledger, no relay"},
    "M1x": {"K6": "K6 n/a: zombie writer not realisable (S5 F-E2)",
            "K7": "K7 n/a: publication written inside the Kafka transaction, no relay"},
    "M2": {"K6": "K6 n/a: zombie writer not realisable (S5 F-E2)"},
    "M3": {"K6": "K6 n/a: zombie writer not realisable (S5 F-E2)",
           "K7": "K7 n/a: inline publication, no relay"},
    "M4": {"K3": "M4 n/a: effect+progress in one PostgreSQL transaction (method §5.3 fn 2)",
           "K6": "K6 n/a: zombie writer not realisable (S5 F-E2)",
           "K7": "K7 n/a: inline publication, no relay"},
    "M2+M3": {"K6": "K6 n/a: zombie writer not realisable (S5 F-E2)"},
    "M4+M3": {"K3": "M4+M3 n/a: effect+progress in one PostgreSQL transaction",
              "K6": "K6 n/a: zombie writer not realisable (S5 F-E2)",
              "K7": "K7 n/a: inline publication, no relay"},
    "M1x+M3": {"K6": "K6 n/a: zombie writer not realisable (S5 F-E2)",
               "K7": "K7 n/a: publication written inside the Kafka transaction, no relay"},
}

#: crash classes S4 realises itself (no in-process failpoint)
S4_REALISED_CLASSES = {"K2", "K5"}

#: classes realised by an in-process app failpoint
APP_FAILPOINT_CLASSES = {"K1", "K3", "K4", "K7"}


def crash_classes(mechanism: str) -> list[str]:
    """Executed Stage-A crash classes for a mechanism: reachable R classes + K4 probe."""
    classes = list(STAGE_A_REACHABLE[mechanism])
    if mechanism in K4_PROBE:
        classes.append("K4")
    return [k for k in CRASH_CLASSES if k in classes]


def n_stage_a_cells() -> int:
    return sum(len(crash_classes(m)) for m in MECHANISMS)


def n_stage_b_cells() -> int:
    return sum(len(FAULTS) - len(STAGE_B_NA.get(m, set())) for m in MECHANISMS)


#: Single ownership table for fault receipts (S5 R3).  Keyed by canonical fault token.
OWNERSHIP = {
    "K1": "app", "K3": "app", "K4": "app", "K7": "app",   # in-process failpoints
    "F4b": "app",                                          # deterministic ordering probe
    "K2": "s4", "K5": "s4", "K6": "s4",                    # S4 external interventions
    "F1": "s4", "F2": "s4", "F3": "s4", "F4a": "s4", "F5": "s4", "F6": "s4",
    "F0": "none", "none": "none", "ctrl": "none",
}


def normalize_fault(fault: str) -> str:
    token = str(fault or "").strip()
    upper = token.upper()
    if upper in ("F4A", "F4B"):
        return "F4a" if upper == "F4A" else "F4b"
    if upper in ("K1", "K2", "K3", "K4", "K5", "K6", "K7"):
        return upper
    return upper


def injector_ownership(fault: str, mechanism: str | None = None,
                       level: str | None = None) -> str:
    """Return ``app`` | ``s4`` | ``none`` for a fault token — the single source of truth."""
    token = normalize_fault(fault)
    if token in OWNERSHIP:
        return OWNERSHIP[token]
    raise ValueError(f"unknown fault for ownership: {fault!r}")


def record_stage(stage_label: str) -> str:
    """Canonical run-record ``stage`` value from a matrix stage label (S5 F-RN1)."""
    mapping = {"P": "pilot", "V": "pilot", "A": "A", "B": "B", "C": "C",
               "M7": "M7"}
    if stage_label not in mapping:
        raise ValueError(f"unknown stage label {stage_label!r}")
    return mapping[stage_label]


def record_fault(condition: str) -> str:
    """Canonical run-record ``fault`` value from a matrix condition token (S5 F-P4/F-RN1)."""
    token = str(condition)
    upper = token.upper()
    if upper == "CTRL":
        return "none"
    if upper == "CAL":
        return "none"
    if upper in ("F4A", "F4B"):
        return "F4a" if upper == "F4A" else "F4b"
    if upper in FAULTS:
        return upper
    if upper in CRASH_CLASSES:
        return upper
    if upper == "NONE":
        return "none"
    raise ValueError(f"unknown condition token {condition!r}")


def condition_to_fault(condition: str) -> str:
    """Deprecated alias kept for callers; canonical mapping is :func:`record_fault`."""
    return record_fault(condition)


def validate_design() -> list[str]:
    """Self-check the frozen design against the amended prereg arithmetic."""
    problems: list[str] = []
    if len(MECHANISMS) != 9:
        problems.append("expected 9 mechanisms")
    if len(FAULTS) != 8:
        problems.append("expected 8 fault conditions")
    if n_stage_b_cells() != 66:
        problems.append(f"Stage-B executed fault cells {n_stage_b_cells()} != 66")
    if n_stage_a_cells() != 42:
        problems.append(f"Stage-A executed crash cells {n_stage_a_cells()} != 42")
    na_cells = sum(len(PILOT_NA.get(m, set())) for m in MECHANISMS)
    if na_cells != 6:
        problems.append(f"pilot n/a cells {na_cells} != 6")
    # A5/A6 calibration arithmetic (fail loudly if a design constant drifts)
    if M7_CAL_RUNS != 54:
        problems.append(f"M7-cal runs {M7_CAL_RUNS} != 54")
    if (1_000, 3) in M7_SPOT_BLOCKS:
        problems.append("1k/P3 must not be a spot-check block (A5.1: not calibratable)")
    if M7_SPOT_RUNS != 54:
        problems.append(f"M7spot runs {M7_SPOT_RUNS} != 54")
    if M7_SAT_RUNS != 9:
        problems.append(f"M7sat runs {M7_SAT_RUNS} != 9")
    if M7_CALIBRATION_RUNS != 117:
        problems.append(f"calibration runs {M7_CALIBRATION_RUNS} != 117")
    if CONFIRMATORY_EXECUTABLE != 7_488 or ORDER_ROWS != 7_631:
        problems.append(f"confirmatory/order accounting {CONFIRMATORY_EXECUTABLE}/"
                        f"{ORDER_ROWS} != 7488/7631")
    for mechanism, classes in STAGE_A_NA.items():
        reachable = set(crash_classes(mechanism))
        if reachable & set(classes):
            problems.append(f"{mechanism}: class both reachable and n/a: "
                            f"{sorted(reachable & set(classes))}")
        if (reachable | set(classes)) != set(CRASH_CLASSES):
            problems.append(f"{mechanism}: crash classes not partitioned into R/n-a")
    return problems


#: amended campaign arithmetic (asserted by experiments/matrix.py)
PILOT_PLANNED = 216
PILOT_EXECUTABLE = 198
STAGE_A_RUNS = 42 * 30 + 9 * 30          # crash cells + controls = 1,530
STAGE_B_RUNS = 66 * 30 + 66 * 10 + 66 * 2 * 10   # core + S1 + S2 = 3,960
STAGE_C_RUNS = 1_800
VALIDATION_RUNS = 26
#: M7 calibration block (author-approved at P2): 9 mechanisms x {10k,100k} x P6 x N=3.
M7_CAL_RUNS = 9 * 2 * 3
#: A5.3 (revision r2) uncalibrated fixed-rate spot blocks: the 1k/P3 block was drafted
#: originally but is NOT calibratable (A5.1) and was dropped by revision r2.  The M7spot
#: block pairs these five (L,P) blocks with one saturation spot at (100k,P6) per mechanism:
#: 9 mechanisms x (5 + 1) spot blocks x N=1 = 54 runs.
M7_SPOT_BLOCKS = ((10_000, 3), (10_000, 12), (100_000, 3),
                  (100_000, 12), (1_000_000, 6))
M7_SPOT_RUNS = 9 * (len(M7_SPOT_BLOCKS) + 1)   # 5 fixed-rate + 1 saturation = 54
#: A6.1 post-rates saturation-validation block: 9 mechanisms x 1 run (100k/P6, saturation, N=1),
#: offered exactly rates.json.saturation_rate["100000|p6|sat"].rate (the candidate 2.5x max).
M7_SAT_RUNS = 9
#: A5/A6 calibration total: M7-cal 54 + M7spot 54 + M7sat 9 = 117 runs.
M7_CALIBRATION_RUNS = M7_CAL_RUNS + M7_SPOT_RUNS + M7_SAT_RUNS
#: Confirmatory accounting is unchanged by A5/A6 (planned/executable + validation runs).
CONFIRMATORY_PLANNED = PILOT_PLANNED + STAGE_A_RUNS + STAGE_B_RUNS + STAGE_C_RUNS
CONFIRMATORY_EXECUTABLE = PILOT_EXECUTABLE + STAGE_A_RUNS + STAGE_B_RUNS + STAGE_C_RUNS
ORDER_SLOTS = CONFIRMATORY_EXECUTABLE + VALIDATION_RUNS
#: Total run_order rows (7,514 confirmatory/validation slots + 117 calibration runs = 7,631).
ORDER_ROWS = ORDER_SLOTS + M7_CALIBRATION_RUNS


if validate_design():
    raise AssertionError("frozen design constants inconsistent: " + "; ".join(validate_design()))
