# Pre-registration Amendment A6 — post-rates saturation validation block (author-approved)

Status: normative addendum to `docs/prereg.md`, A2/R12, A5. Author decision of 2026-11-24 on the
S5 §12 B1 blocker: the derived saturation rate (2.5 × max measured capacity) must be **exercised**
before Stage C, not only computed.

## A6.1 Block

A new calibration block `M7sat` measures the derived saturation rate end-to-end:

* 9 runs, one per mechanism, at 100k events / P6, `load_mode="saturation"`, N=1 each;
* the loader is offered exactly `rates.json.saturation_rate["100000|p6|sat"]["rate"]` (the candidate
  derived rate), recorded in the run record (`rate_mode="saturation-validation"`, offered rate,
  committed-event throughput, generator lag);
* these are calibration records (`stage: "m7"`, `stage_label: "M7"`, block `M7sat`), never
  confirmatory evidence and never ingested by the A2 analyzer.

Accounting: confirmatory counts stay 7,506 planned / 7,488 executable + 26 validation. Calibration
blocks: M7-cal 54, M7spot 54, M7sat 9 = 117 calibration runs; run_order grows to 7,631 rows.

## A6.2 Two-step rates validation

1. **Derive (T1a):** after M7-cal + M7spot exist, `rates.py` writes a **candidate** `rates.json`
   (`validated=false`, `saturation_validation.mode="post-rates"`) containing the frozen mapping rule
   outputs and the 2.5×max saturation rate.
2. **Exercise (T1b):** the 9 M7sat runs are executed at that derived rate.
3. **Validate (T1b end):** `rates.py --write` sets `saturation_validation.validated=true` only if
   every M7sat run is valid and sustained the offered rate (G7 generator-lag bound, no load-generator
   failure, committed throughput within the frozen tolerance). The overall `validated=true` requires
   both the A5 block coverage/spot checks and the A6 saturation validation; otherwise T1 fails closed
   and no B/C run may start.

## A6.3 Phase order (also in `docs/LINUX-CAMPAIGN-RUNBOOK.md`)

T0 smoke → T0b fault efficacy → T1a calibrate (M7-cal + M7spot + derive candidate rates) → T1b
saturation validation (M7sat + final validated rates) → T3 gate controls (under the provisional
bootstrap freeze) → T2 final freeze (`freeze.py --write --force --require-evidence`) → T4 pilot →
T5 A/B/C → T6 analysis → S5 re-confirmation.

The final freeze binds the validated `rates.json`, the M7/M7spot/M7sat matrix hashes, the live
smoke/fault-efficacy/gate-control evidence and the container image digests.

## A6.4 Rationale

S5 §12 B1: without this block the 2.5×max saturation rate would never be exercised before Stage C,
so a derived rate could be systematically unattainable while `rates.json` claimed `validated`. A6
makes saturation validation an empirical, fail-closed gate at the same cost class as the other
calibration blocks (9 runs).
