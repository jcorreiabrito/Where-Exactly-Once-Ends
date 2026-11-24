# `formal/` - bounded formal model (machine-checked at P2)

## What this is

`ledger.tla` is a small, bounded TLA+ model of the crash-point semantics of the Kafka → PostgreSQL ledger pipeline described in [`../docs/method.md`](../docs/method.md) §3–§6. It models the four structural properties that decide the paper's propositions:

| Boolean | Meaning | Mechanism it corresponds to |
|---|---|---|
| `DoDedup` | the ledger effect is idempotent under `(event_id, leg_no)` (unique index or inbox claim) | M3, M2, M2+M3, M4+M3, M1x+M3 |
| `DoAtomic` | the ledger effect and consumption progress become durable in **one** atomic commit | M4, M4+M3 |
| `DoTxn` | publication-stream records and progress become durable in **one** atomic commit (a Kafka transaction) | M1-kafka (`DoAtomic ∧ DoTxn`), M1x (`DoTxn` only) |
| `DoEarly` | the probe of `docs/method.md` §4.6/D2: progress may commit **before** the effect is durable | the K4/F4b probe, not a canonical mechanism |

Two snapshot facts are modelled explicitly because they are the paper's core: a crash **after the effect and before the progress** (W1, duplicate window) and a crash **after the progress and before the effect** (W2, loss window). The ledger is a per-`(event, leg)` multiplicity function, so duplicates are visible as multiplicity > 1.

## Status and honesty statement

**MACHINE-CHECKED at P2 (2026-11-24).** TLC 2.19 (tla2tools v1.7.4, SHA-256 `936A262061C914694DFD669A543BE24573C45D5AA0FF20A8B96B23D01E050E88`) on Java 21, bounds `N = 3`, `BatchSize = 2`, single partition, invariants `TypeOK`, `I1Balance`, `I3Unique`, `I4CompleteWhenDone`, `StrictProgressBound`. `tools/run-tlc.ps1` reproduces the check; logs and summary are in `formal/tlc-results/` (`tlc-summary.json` records UTC time, TLC version, SHA-256 and distinct-state counts). **All eight configurations matched the pre-registered expectations below**; no expectation was edited after checking. The bounds are small by construction: this is a bounded witness of the crash-point semantics, not an unbounded proof, and the paper must state the bounds alongside any TLC claim.

## Bounds checked (fixed in advance)

| Constant | Value | Rationale |
|---|---|---|
| `N` (events) | 3 | enough for ≥ 1 full batch plus a replay batch |
| `BatchSize` | 2 | batches of 2 → an event can be replayed and a partial batch exists in the state space |
| partitions | 1 (implicit) | per-event reasoning is partition-local; partitions only change multiplicity, not the invariant logic |
| state-space limit | `-simulate` fallback | a full BFS may still be large because of crash interleavings; if BFS exceeds memory, use `-simulate num=100000` and report it |

Reproduce the check with:

```
powershell -ExecutionPolicy Bypass -File tools/run-tlc.ps1
```

The script expects `tla2tools.jar` at `%LOCALAPPDATA%\eos-runtime\vendor\tla\tla2tools.jar` (version and SHA-256 in `tools/TOOLCHAIN-VERSIONS.txt`) and uses the eight committed configurations in `formal/cfg/` (bounds `N = 3`, `BatchSize = 2`).

## Outcome per configuration (machine-checked 2026-11-24)

The table states what the model **was expected** to demonstrate before checking, and what it **did** demonstrate (the checked outcome matched every row).

| Configuration | `DoDedup` | `DoAtomic` | `DoTxn` | `DoEarly` | Expected: invariant holds? | Corresponds to |
|---|---|---|---|---|---|---|
| naive | F | F | F | F | **I3Unique fails** (W1 duplicate); I4 holds | M0, M1x |
| dedup | T | F | F | F | I3Unique holds; I4 holds | M3, M2 |
| atomic | F | T | F | F | all hold (W1/W2 unreachable) | M4 |
| atomic+dedup | T | T | F | — | all hold | M4+M3, M2+M3 |
| kafka-txn | F | T | T | — | all checked ledger invariants hold (publication-stream completeness is **not** a TLC-checked property) | M1-kafka |
| txn-only | F | F | T | F | **I3Unique fails** (ledger duplicate; stream exactly-once) | M1x |
| early probe | F | F | F | T | **I4CompleteWhenDone fails** (W2 loss); I3Unique holds | K4/F4b probe, H3 |
| early+dedup | T | F | F | T | **I4CompleteWhenDone fails**, I3Unique holds → dedup cannot restore a never-written posting | M3-early, H3 |

`I1Balance` is expected to hold in every configuration: the model inserts both legs of an event together, which mirrors the one-record-one-entry design (`docs/method.md` §1.4). Its role is regression protection for the model itself, not a paper claim.

## Scope of the machine-checked claim

The checked claim is deliberately narrow. The model is a **bounded safety model** at
`N = 3`, `BatchSize = 2`, single partition, for **eight mechanism abstractions**. It checks
`I1Balance`, `I3Unique`, and `I4CompleteWhenDone` (a *state* invariant: "if `done` then every event
appears once"), plus `TypeOK`. The four booleans abstract real mechanism components as follows:
`DoDedup` = a sink-side idempotency claim (unique index or inbox); `DoAtomic` = ledger effect and
progress in one atomic commit (DB transaction for M4/M2-family); `DoTxn` = publication-stream
records and progress in one atomic commit (Kafka transaction); `DoEarly` = the degraded-variant
probe in which progress may commit before the effect is durable.

**Not modelled (and therefore not part of the checked claim):**
- the two-writer zombie/rebalance exposure (crash points C6/C7): requires a fencing epoch and a
  second writer; it remains a pen-and-paper argument and an experimental result (K6), **not** a TLC
  result;
- the append-only mutation invariant I2: the ledger state is a multiplicity function with no
  mutation action; I2 is tested experimentally by the oracle's mutation classes only;
- liveness under fair recovery (A1 §3): there is no fairness assumption and no temporal property;
  `I4CompleteWhenDone` is safety at termination;
- publication-stream exactly-once invariants: `StreamCompleteWhenDone` is defined but is **not** in
  any `.cfg`, so it is not a TLC-checked property (the stream half of M1x remains an argument);
- partial-batch durability and weakened durability (C11/C12): excluded by assumption (§5.2);
- `StrictProgressBound` is a derived consequence of the model's construction, not independent
  evidence: it cannot fail in any enumeration, so it is reported only as a sanity property of the
  model, never as a paper claim.

The paper must state exactly this scope next to any TLC sentence: a machine-checked **bounded
witness** of the duplicate (W1) and loss (W2) windows for the abstract transition systems, not an
unbounded proof and not a verification of the deployment.

## What is deliberately not modelled

- Multi-partition scheduling and rebalances (C6/C7); a zombie writer would need two writers and a
  fencing epoch — out of scope for the bounded model (see scope paragraph above).
- The relay/outbox publication path of M2 (trace X7); M2 is modelled by its dedup + atomic-ledger
  half, which is what its ledger invariants depend on.
- Partial-batch durability (C12) and weakened durability (C11): excluded by construction/assumption
  in `docs/method.md` §5.2.
- Latency, throughput and time: this is a safety model; performance claims rest on the experiment.

## How this maps to the paper

- `docs/method.md` §3 (EOS boundary and the two windows) — the model's `AtomicCommit`/`TxnCommit`/
  `ProgressCommit` actions are the transition-level statement of Lemma 1.
- `docs/method.md` §6.1 P2/P3/P8/P9 — only those parts within the scope above are machine-checked.
- `docs/method.md` §6.7 FO-6 — the author's decision on appendix material; if adopted, the paper
  reports the bounds, the TLC version and the scope limitation above, and does **not** claim C7/I2/
  liveness/stream verification.

## Reproducing the check

```powershell
powershell -ExecutionPolicy Bypass -File tools/run-tlc.ps1
```

The script vendors nothing at run time: it expects `tla2tools.jar` at
`%LOCALAPPDATA%\eos-runtime\vendor\tla\tla2tools.jar` (version and SHA-256 in
`tools/TOOLCHAIN-VERSIONS.txt`), runs the eight `formal/cfg/*.cfg` configurations with
`-metadir` on local scratch (never inside the cloud-synced workspace), writes
`formal/tlc-results/<config>.txt`, regenerates `tlc-summary.json`, and exits non-zero if any
configuration deviates from its pre-registered expectation. The authoritative per-configuration
distinct-state counts and match flags are in `formal/tlc-results/tlc-summary.json`; do not hard-code
them here (for the four configurations that exhibit an expected violation, TLC stops at the first
counterexample, so the explored-state count varies with worker scheduling, e.g. `06-txn-only` shows
36–39 across runs; the checked property is the violated invariant, not the count). If any row
contradicts the expectation, open a `results/DEVIATIONS.md` entry
and revise the model or proposition — never edit the expectation to match the output.
