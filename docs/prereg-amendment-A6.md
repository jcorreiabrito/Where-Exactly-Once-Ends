# Pre-registration Amendment A6 — post-rates saturation validation block (author-approved)

Status: normative addendum to `docs/prereg.md`, A2/R12, A5. Author decision of 2026-11-24 on the
S5 §12 B1 blocker: the derived saturation rate (2.5 × max measured capacity) must be **exercised**
before Stage C, not only computed. **Revision r2** (same day) implements S5 §14 Q1/Q2: the acceptance
criterion compares the achieved mechanism-committed throughput against the **measured per-mechanism
capacity** (a mechanism offered 2.5× its capacity physically cannot commit at the offered rate), and
the G7 capability claim is narrowed from the frozen "≥3×" text to the documented 2.5× saturation rate
(author-approved).

## A6.1 Block

A new calibration block `M7sat` measures the derived saturation rate end-to-end:

* 9 runs, one per mechanism, at 100k events / P6, `load_mode="saturation"`, N=1 each;
* the loader is offered exactly `rates.json.saturation_rate["100000|p6|sat"]["rate"]` (the candidate
  derived rate), recorded in the run record (`rate_mode="saturation-validation"`, `offered_rate`);
* these are calibration records (`stage: "m7"`, `stage_label: "M7"`, block `M7sat`), never
  confirmatory evidence and never ingested by the A2 analyzer.

Accounting: confirmatory counts stay 7,506 planned / 7,488 executable + 26 validation. Calibration
blocks: M7-cal 54, M7spot 54, M7sat 9 = 117 runs; run_order is 7,631 rows.

## A6.2 Two-step rates validation (r2 criterion)

1. **Derive (T1a):** after M7-cal + M7spot exist, `rates.py --derive` writes a **candidate**
   `rates.json` (`validated=false`, `saturation_validation.mode="post-rates"`) containing the frozen
   mapping-rule outputs, the 2.5×max saturation rate, and the **per-mechanism measured saturation
   capacity** at 100k/P6 (`saturation_capacity_by_mechanism`).
2. **Exercise (T1b):** the 9 M7sat runs are executed at that derived rate.
3. **Validate (T1b end):** `rates.py --write` passes a mechanism when the run is valid, non-censored,
   the load generator exited 0, generator-lag p99 ≤ 5 ms (G7), and the **achieved mechanism-committed
   throughput** satisfies, against that mechanism's measured capacity `C_m`:
   * `|achieved − C_m| / C_m ≤ 0.25` (A5 saturation tolerance), and
   * `achieved ≥ 0.75 × C_m` (the mechanism is actually saturated).
   The run entry records `{offered_rate, measured_capacity, achieved_throughput,
   achieved_vs_capacity_relative, saturated, generator_lag_p99_ms, valid, problems}`.
   `saturation_validation.validated=true` only if all nine mechanisms pass; the overall
   `validated=true` requires both the A5 block coverage/spot checks and the A6 validation; otherwise
   T1 fails closed and no B/C run may start. Producer acknowledgements are never accepted as the
   achieved-throughput evidence (P1: `committed_basis="app-committed"`).

## A6.3 Phase order (also in `docs/LINUX-CAMPAIGN-RUNBOOK.md`)

T0 smoke → T0b fault efficacy → T1a calibrate (M7-cal + M7spot + derive candidate rates) → T1b
saturation validation (M7sat + final validated rates) → T3 gate controls (under the provisional
bootstrap freeze) → T2 final freeze (`freeze.py --write --force --require-evidence`) → T4 pilot →
T5 A/B/C → T6 analysis → S5 re-confirmation.

## A6.4 G7 capability narrowing (r2)

`prereg.md` §6.2 asks for evidence that the generator can sustain ≥3× the maximum measured capacity.
The campaign only ever offers 2.5× (A6.1), and that rate is measured end-to-end by M7sat; the author
approved narrowing the G7 claim to the **documented 2.5× saturation rate** rather than maintaining an
unimplemented 3× assertion. `rates.json` records this explicitly
(`g7_capability={mode:"documented-saturation-rate", multiplier:2.5, author_approved:true,
amendment:"A6 r2"}`), and `rates.py --check` fails closed if that record or the A6 validation is
missing.

## A6.5 Rationale

S5 §12 B1: without this block the 2.5×max rate would never be exercised before Stage C. S5 §14 Q1:
the first implementation compared achieved throughput to the *offered* rate, which no mechanism can
reach; r2 validates against the measured capacity the rate was derived from, so a correct saturated
run passes and a slow sink fails. Q2 is handled by A6.4; the 0.5× (rather than 0.75×) fixed-rate
factor is declared in A5 r3.
