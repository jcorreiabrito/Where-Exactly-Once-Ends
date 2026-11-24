# Pre-registration Amendment A5 — rate-calibration coverage for Stage B/C (author-approved)

Status: normative addendum to `docs/prereg.md`, A2 (R12 / AC-*), A4. Author decision of 2026-11-24
(option "mapping rule + spot-check runs" in the P2 follow-up). **Revision r2** (same day) aligns the
text with the implemented block set after S5 §12 B2: the (1k,P3) block is not calibratable, so the
spot-check block is 54 runs, not 63. No hypothesis, cell, N (confirmatory) or statistic changes; this
amendment only defines how the pre-registered fixed rates are derived from the approved M7 calibration
blocks and how that derivation is validated. The post-rates saturation *exercise* is Amendment A6.

## A5.1 Problem

The frozen rule (AC-2 / R12) is: fixed rate for a run type = 50 % of the minimum measured mechanism
capacity at that stage's load/partitions (capacity from **mechanism-committed events** in the
warm-up-excluded streaming window — method §7.2; never producer acknowledgements). Stage B/C use
**seven** measurable fixed-rate blocks: (10k,P3), (10k,P6), (10k,P12), (100k,P3), (100k,P6),
(100k,P12), (1M,P6), plus saturation at (100k,P6). The (1k,P3) block is **not calibrated**: the frozen
warm-up rule `max(2000, 10 %)` swallows all 1,000 events, and Stage A/pilot 1k runs execute at
`rate=max` by design (approved M7 proposal item iii / M6), so no 1k capacity sample exists or is
needed. The approved M7 core block measures capacity directly only at (10k,P6) and (100k,P6)
(9 mechanisms × 2 loads × N=3 = 54 runs). Direct measurement of all seven blocks would need 189 runs
plus saturation; the author rejected that cost and approved a recorded mapping rule with validation
(A5.2/A5.3).

## A5.2 Frozen mapping rule (rule id `A5.partition-scale.v1`)

For a mechanism m and load L with partitions P:

1. `capacity(m, L, 6)` is measured directly in the M7 core block at L ∈ {10k, 100k} from committed
   events in the warm-up-excluded window.
2. Partition scaling: `capacity(m, L, P) = capacity(m, L, 6) × (P / 6)` (Kafka partition parallelism is
   the dominant scaling factor; recorded as an explicit assumption in `rates.json`).
3. Load interpolation: for L = 10k and L = 100k use the direct measurement; for L = 1M use the
   (100k, P6) measurement (asymptotic throughput assumption). There is no 1k derivation: Stage A and
   pilot 1k runs always execute at `rate=max` (`rate_mode="stage-a-max"/"pilot-max"`) and are
   correctness-only for performance purposes.
4. Fixed rate for a block = `floor(0.5 × min_mechanism capacity(m, L, P))`, min over the **nine**
   frozen mechanisms, exactly as in R12; never below the 50 events/s floor used by A3.3. The nine
   include M1-kafka, although D13 excludes M1-kafka from *cross-mechanism performance comparisons*;
   including it in the minimum is conservative (it can only lower the derived rate) and is stated here
   for reviewers.
5. Saturation rate = `2.5 × max_mechanism measured capacity at (100k, P6)` (pre-registered
   "2.5× max capacity"), explicitly NOT a fraction of the minimum.
6. Every derived block records `{rule, basis_block, assumption_ids}` in `rates.json`; a block without a
   measured basis or a failed spot-check is an error, never a silent fallback.

## A5.3 Validation spot-check block (`M7spot`, 54 runs)

To test assumptions 2-3, the author approved **54** additional calibration runs (revision r2; the
originally drafted list included nine (1k,P3) rows, which are not calibratable — see A5.1):

* 9 mechanisms × 5 uncalibrated fixed-rate blocks ((10k,P3), (10k,P12), (100k,P3), (100k,P12),
  (1M,P6)) × N=1 = 45 runs;
* 9 mechanisms × 1 saturation-capacity spot at (100k,P6) = 9 runs (this measures the unthrottled
  saturation capacity that feeds the 2.5×max rule; the delivered 2.5×max rate itself is exercised in
  T1b/M7sat under Amendment A6, not here).

Acceptance is fail-closed:

* an uncalibrated fixed-rate block passes if every spot-checked mechanism's measured committed capacity
  is within ±25 % of the rule prediction; otherwise `rates.py` refuses to write `rates.json` and the
  block is escalated to the author (it would then need direct calibration);
* the saturation-capacity spots must be present and valid for every mechanism before the 2.5×max rate
  is derived.

Accounting: the confirmatory counts stay 7,506 planned / 7,488 executable + 26 validation. Calibration
blocks are M7-cal (54), M7spot (54) and M7sat (9, Amendment A6) = 117 runs; run_order grows to
**7,631 rows** (7,514 + 117). They are recorded as `stage: "m7"`, `stage_label: "M7"`, blocks `M7-cal`,
`M7spot` and `M7sat`, and are excluded from every confirmatory table by the analyzer's matrix pin
(A5.4).

## A5.4 Analysis consequences

* `tools/analyze.py` excludes all `stage in {"m7", "M7"}` records from confirmatory tables and gates;
  it consumes `experiments/rates.json` only for G7 (rate validation) and for the run-record rate
  provenance column.
* `rates.json` records the rule id, the measured basis (mechanism-committed events, app performance
  blocks), the derived blocks, spot-check outcomes, the A6 saturation validation and
  `validated: true/false`; Stage B/C preflight fails if `validated` is false.
* The final pre-campaign freeze binds `experiments/rates.py`, `experiments/rates.json`, the
  M7/M7spot/M7sat matrix hashes and the Linux fault-efficacy/live-smoke/gate-control/container-image
  evidence.
