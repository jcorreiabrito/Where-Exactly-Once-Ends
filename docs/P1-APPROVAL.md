# P1 AUTHOR APPROVAL — recorded decision

Date: 2026-11-24 (UTC-03). Author approved the P1 gate via explicit selection.
Status: **P1 CLOSED — P2 implementation authorised.** P2 has its own stop (pilot projection).

| Item | Author decision |
|---|---|
| P1 package (`docs/method.md` + `docs/prereg.md`, A1 revision, native-process deviation) | **APPROVED AS DRAFTED** |
| Decisions D1–D15 (`docs/method.md` §13) | **ALL DRAFT RECOMMENDATIONS ACCEPTED** |
| Wall-clock budget `H` | **8–24 h, unattended** (target the upper half; pilot projection + cut ladder at P2) |
| D2 ordering probe | **ADD `ordering ∈ {strict, early}` to all two-commit mechanisms**; C5/K4 cells test H3; `early` is a probe, canonical mechanisms remain `strict` |
| Formal model | **TLC-check at P2** (vendor `tla2tools.jar`, Java 21 available), report checked bounds, include as paper appendix; draft stays labelled unverified until then |

## Effective decisions (D1–D15, accepted)
| D | Accepted option |
|---|---|
| D1 | Campaign budget H = 8–24 h unattended; nominal A1 matrix; apply pre-registered cut ladder only if pilot projection exceeds H |
| D2 | `ordering ∈ {strict, early}` flag on every mechanism with both commits; O-a |
| D3 | M2 = inbox claim + outbox publication log + relay, ledger transaction atomic |
| D4 | Literal targeting in Stages B/C; semantic-role targeting in Stage A K-classes; n/a cells documented |
| D5 | F6 = protocol-level replay only; external rewind excluded (may be labelled exploratory) |
| D6 | Fresh logical state per run + full-stack restart every 50 runs |
| D7 | Both sensitivity blocks at 10k if budget allows, else cut first per ladder |
| D8 | I1 = structural well-formedness (I1a) ∧ signed sum zero (I1b) |
| D9 | I4 = end-state completeness at quiescence (primary), with formal liveness as argument |
| D10 | GT written+fsynced+hashed before produce; verified; keep digest/manifest, delete bulk file |
| D11 | Local non-synced scratch for runtime artefacts; curated copy + digest manifest into `results/raw/` |
| D12 | θ = 1 % per-run violation rate; "certified" reserved for model proof + deterministic adversarial cleanliness |
| D13 | M1-kafka = positive control only, excluded from cross-mechanism performance comparisons |
| D14 | Adopt Appendix C configuration/fairness tables as drafted |
| D15 | Paired seeds across mechanisms within a cell; McNemar + Fisher sensitivity |

## Consequences
1. S3 implementation starts now; S4 builds the independent oracle first, then runner/injectors.
2. No paper claim may use the word "certified" for a mechanism without the D12 condition.
3. The paper must state the native-process deviation, the 8–24 h campaign window, and every deviation
   recorded in `results/DEVIATIONS.md`.
4. The formal appendix may report a TLC result only after it is actually checked; bounds and TLC
   version recorded in `results/formal/` and `formal/README.md`.
