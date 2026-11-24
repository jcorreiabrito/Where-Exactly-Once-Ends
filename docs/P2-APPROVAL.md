# P2 approval record (author decision, 2026-11-24)

This is the author's formal P2 decision. It follows the P2 gate package in `docs/P2-GATE.md` §4,
the S5 verdict (`docs/redteam-rereview.md` §10, PILOT gate PASS-WITH-CONDITIONS; Stage A/B/C
BLOCKED), and the measured projection in `docs/P2-GATE.md` §3.

## 1. Decisions recorded

| # | Decision item | Author decision |
|---|---|---|
| 1 | **Matrix amendment M1** — 7,506 planned / 7,488 executable (pilot 216/198, A 1,530, B 3,960, C 1,800) + 26 validation; K6 (zombie) non-realisable; M1-kafka K5 absent; M1-kafka F6 n/a | **APPROVED as drafted** |
| 2 | **Amendment A2** items A2-1 … A2-8 with bounded choices | **APPROVED as drafted**, AC-1 = H5-A, AC-2 = no regeneration, AC-3 = pooled stratified exact sign test (AC-3b), AC-4 = at least one of M3/M4, AC-5 = `v5` violation, AC-6 = split I1a/I1b, AC-7 = narrow TLC claim, AC-8 = >20 % censoring flag |
| 3 | **K6 non-realisability** — E3/C7/P9 evidence is model-and-argument only; no K6 data will be claimed | **ACKNOWLEDGED / APPROVED** |
| 4 | **M7 calibration block** — 54 runs: 9 mechanisms × {10k, 100k} × P6 × N=3, then `rates.py` derives the fixed rates | **APPROVED** (separate measurement block, not folded into the confirmatory counts) |
| 5 | **Budget / scope** — measured infeasibility of the full 7,506-run programme within H = 8–24 h on the Windows host | **Move the campaign to a Linux/container host and execute the FULL frozen design there** |
| 6 | **Amendment A5** — rate-calibration coverage: frozen mapping rule `A5.partition-scale.v1` (partition scaling, load anchors, 1M from 100k/P6) + 63-run M7spot validation block, fail-closed ±25 %; saturation = 2.5 × max measured capacity | **APPROVED** (option "mapping rule + 63 spot-check runs") |
| 7 | **Amendment A6** — post-rates saturation validation: 9 calibration runs (one per mechanism at 100k/P6) offered exactly the derived 2.5×max saturation rate; `rates.json` is `validated=true` only if all sustain it (fail-closed); phase order T1a → T1b → T3 → T2 | **APPROVED** (option "9 post-rates saturation validation runs") |
| 9 | **Post-P2 clarifications approved by the author (2026-11-24):** A6 r2 saturation criterion (achieved vs measured capacity, ≥0.75× saturated), G7 narrowing to the documented 2.5× rate, and the 0.5× fixed-rate factor explicitly superseding prereg §6.2's 0.75× (A5 r3) | **APPROVED** |
| 8 | S5 Stage-A conditions (R10 analyzer, R12 `rates.py` + M7, evidence/provenance close-out, complete gate evidence K5 / post-freeze K3 / F1-F3-F5 live validation, re-freeze re-confirmation) | Accepted as **preconditions**; closed in the tree, host execution + final freeze re-confirmation pending |

Stage A/B/C are therefore **approved in principle** and remain blocked in practice until the S5
conditions are closed on the campaign host and the final re-freeze is re-confirmed. No confirmatory
run may execute on the retired Windows host; its pilot rows remain as measured calibration data and
are disclosed in `results/DEVIATIONS.md` (M14–M16).

## 2. Consequences recorded

- The approved substrate is `deploy/docker/docker-compose.yml` (Linux/container), driven by
  `deploy/linux/bootstrap.sh` and `deploy/linux/run-campaign.sh`; the complete operational contract is
  `docs/LINUX-CAMPAIGN-RUNBOOK.md`.
- The pilot's measured ~44 s/run on Windows is *not* a valid projection for the container host; the
  Linux pilot (T4 in the runbook) produces the authoritative projection from the frozen formula.
- The paper may claim B/C results only after the corresponding runs exist under the frozen analysis
  pipeline; until then the Results section stays placeholder-only.

## 3. Sign-off

- [x] Matrix amendment approved — author, 2026-11-24
- [x] Amendment A2 approved (item-by-item) — author, 2026-11-24
- [x] K6 limitation acknowledged — author, 2026-11-24
- [x] M7 calibration block approved — author, 2026-11-24
- [x] Amendment A5 (rates coverage: mapping rule + 63 spot checks) approved — author, 2026-11-24
- [x] Amendment A6 (9 post-rates saturation validation runs) approved — author, 2026-11-24
- [x] Budget/scope decided: Linux/container host, full design — author, 2026-11-24
- [ ] Final re-freeze aggregate recorded on the campaign host: `docs/prereg-freeze.json` → `<pending host run>`
- [ ] S5 re-confirmation of the final re-freeze → `<pending>`

