# Project status — final (2026-11-25, after the executed reduced campaign)

Single-author project: João Brito (UFABC). Operational snapshot only; not part of the paper.

## 1. Final state

| Item | Status | Evidence |
|---|---|---|
| Paper | **25 pp**, 0 undefined references/citations, 0 overfull hbox; abstract ≤200 words with finished results; results inserted from generated tables | `main.pdf`, `main.log`, `submission/CHECKLIST.md` |
| Anonymized review copy | built from the same source with author/institution/email removed | `submission/anonymized-review.pdf` |
| Executed campaign | **154 records, 152 valid**, on native-process substrate (Amendment A7): 40 correctness cells / 119 valid runs at 1k, 14 performance cells at 10k/100k, 2 excluded records | `results/raw/run-records/`, `results/raw/summaries/`, `results/RUNLOG.md` |
| Generated analysis | descriptive correctness (Clopper–Pearson) + mechanism-committed throughput; no hypothesis tests claimed ($N=3$) | `results/analysis-reduced/`, `tools/analyze_reduced.py` |
| Key findings | K4 early-ordering probe: **24/24 runs lost postings** (272–494/cell) across all 8 exposed mechanisms; K3 duplicates only in unguarded M0 (302) and M1x (462), guarded mechanisms clean; K1/K2/K7/fault-free clean | `results/analysis-reduced/correctness-by-*.csv`, paper §7 |
| Artifact | frozen service + injectors + oracle + runner + matrix; 158/158 harness tests; jar SHA-256 `6e8223751706d64e7b77752b42df5694d4231ba8bd3522034806bae25e8e50d3` | `artifact/`, `experiments/` |
| Provenance | run records carry the frozen git_sha; deviations A7/M23 and the re-freeze are recorded | `docs/prereg-amendment-A7.md`, `results/DEVIATIONS.md` |
| CI | green at the submission commit (4/4 jobs) | GitHub Actions run id recorded at submission/tag |
| Git tag | `v1.0-submission` on the submission commit | repository tag |

## 2. Known limitations (disclosed in the paper and A7)

Reduced $N=3$ design (no confirmatory hypothesis tests); no Stage B/C/D fixed-rate or saturation
claims; K5 protocol replay, K1 for the M1x family, and K2 for M1-kafka not realisable on the
native substrate; M2/M2+M3 performance censored at 10k/100k; one mid-campaign re-freeze after two
injector-realisation fixes. All are recorded in `docs/prereg-amendment-A7.md`,
`results/DEVIATIONS.md` M23 and paper §7/§9.

## 3. Submission package

`submission/final-paper.pdf`, `submission/anonymized-review.pdf`, `submission/COVER-LETTER.md`,
`submission/CHECKLIST.md`, `submission/VENUE-RULES.md`. Remaining author actions before upload:
similarity check, portal metadata, and confirming the venue's current template requirement; no
data or analysis action remains.
