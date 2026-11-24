# P2 INTERFACE RULINGS — lead decisions (2026-11-24)

Four S4/S3 interface ambiguities were resolved before the oracle freeze. None changes a cell, N,
factor or statistic; all are encoding/accounting clarifications. They are recorded in
`docs/CONTRACTS.md` (§2, §4) and `docs/prereg.md` (§2.3, §2.8, §4.4, §5), and S4 logs them in
`results/DEVIATIONS.md`.

| # | Question | Ruling |
|---|---|---|
| R1 | GT `line_no` base; seed text encoding; hex vs decimal seed passing | **0-based `line_no`; 1-based `leg_no`.** Derivation seed argument = per-repetition `data_seed` (prereg §4.4) as big-endian u64 rendered canonical decimal ASCII. Seeds recorded as 16-hex; loader/oracle convert to the same decimal text independently. Java↔Python digest equality (G5) must pass on hard-coded vectors. |
| R2 | `run_id` format vs prereg §5 schema; `+` illegal in Kafka topic names | **Adopt S4 encoding:** `<stage>-<mech>-<fault>-<load>-<partitions>[-sat]-rNN`, lower-case `[a-z0-9-]`; mechanism tokens `m0,m1-kafka,m1x,m2,m3,m4,m2m3,m4m3,m1xm3`; stages `pilot,a,b,c`; conditions `f0..f6,k1..k7,ctrl`; loads `1k,10k,100k,1m`; partitions `p3,p6,p12`. Examples: `a-m1x-k3-1k-p3-r07`, `c-m2-f0-100k-p6-sat-r05`, `c-m2-f0-1m-p6-r03`. |
| R3 | Stage C saturation block collides with core; validation runs need a stage | **`sat` token** for the saturation block (`c-m2-f0-100k-p6-sat-r05`); 1M block uses load token `1m`. Validation runs: stage `pilot`, run_id `pilot-valid-<kind>-rNN`, recorded separately from the 216/201 matrix slots. |
| R4 | Pilot 216 planned vs 201 executable (15 n/a slots) | **Report both.** `matrix.yaml` keeps 216 planned slots; n/a slots carry `executed=false` + reachability reason and are never passes; executable = 201; totals reported as 7,866 planned / 7,851 executable (+20 validation). No no-op runs are executed to inflate counts. |

## Authority
These rulings are within the lead's integration mandate; they are mechanical and do not alter the
author-approved methodology. They are surfaced to the author at the P2 gate (`docs/P2-GATE.md`, to
be produced after the pilot) together with the compute projection.
