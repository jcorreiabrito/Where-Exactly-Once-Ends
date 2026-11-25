# RITA SUBMISSION PREPARATION CHECKLIST — "Where Exactly-Once Ends"

**Manuscript:** *Where Exactly-Once Ends: Preserving Double-Entry Ledger Invariants When Apache Kafka Pipelines Write to External Stores*
**Venue:** RITA (UFRGS), Research Article · **Status: P2-AUTHOR-APPROVED • POST-COMPRESSION BUILD (data pending)**
**Audited artifact (this pass):** `main.pdf` built 2026-11-24 ~17:50 America/Sao_Paulo from working tree (HEAD `b444bf5` + this pass's manuscript edits) · **Build:** `powershell -ExecutionPolicy Bypass -File build.ps1 -Main main.tex -Clean` → `BUILD OK (24 pages, 423610 bytes)`, exit 0, **0 undefined references/citations**.
**Prior audit:** 2026-11-24 morning (33 pp; `docs/compliance-report.md` records the full audit and the compression plan executed here). Rules record: `submission/VENUE-RULES.md` (re-verified 2026-11-24, no drift).
**Status legend:** PASS = verified on the current build; FAIL = written rule violated now; PENDING = cannot be final until the frozen campaign's results are inserted / author or venue action.

**Current measured numbers:** **24 pp** (target ≤25 met; build log + `pdfinfo`; footer `p.X/24`), **1 overfull hbox / 0 overfull vbox / 26 underfull hbox / 7 underfull vbox** (underfulls cosmetic), **59 bibliography entries printed** (63 in `refs.bib`, all cited-in-print; 4 non-essential gray citations removed from the narrative only — entries retained so the verified-base gate still passes), abstract placeholder 111 words, 6 result-artifact **PENDING** blocks (4 tables + 2 figures) + decision-table placeholder, all clearly marked.

---

## A. Submission mechanics

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| A1 | Originality / not under consideration elsewhere (S2) | PENDING | No signed cover-letter statement yet. | Author signs cover letter (A13); SEER declaration. |
| A2 | Not under consideration elsewhere | PENDING | Author statement needed. | Cover letter. |
| A3 | Initial submission PDF formatted in LaTeX (S1/S2) | PASS | `main.pdf` Producer `MiKTeX pdfTeX-1.40.28`; built by `build.ps1`. | Keep this build path. |
| A4 | LaTeX source available at final version (S1/S2) | PASS | `main.tex`, `SelfArx.cls`, `refs.bib`, `sections/*.tex` complete and are the PDF's source. | Bundle for acceptance. |
| A5 | Submission package under `submission/` (BRIEF §7) | FAIL | `submission/` contains `VENUE-RULES.md` + this checklist; no PDF/source/cover letter yet (anonymized review PDF deliberately deferred until results insertion, per lead). | Copy final PDF + source + cover letter after the results pass; per VENUE-RULES §7.1 N1 also build `main-anonymized.pdf` for double-blind review. |
| A6 | Build green with documented command (BRIEF §9) | PASS | 2026-11-24 ~17:50: `build.ps1 … -Clean` → `BUILD OK (24 pages)`, exit 0; repeated passes stable; **0 undefined refs**, 0 undefined citations. | Re-run at final. |
| A7 | SEER submission metadata complete (S2 author info) | PENDING | Submission not created. | Enter names/affiliation/e-mail/title/abstract/keywords; keep title identical to PDF. |
| A8 | Release tag `v1.0-submission` (BRIEF §7) | PENDING | No tags. | Tag after final gate + guard-secrets pass. |
| A9 | Page-count reporting | PENDING | Current measured: **24** (`main.log`: `Output written on main.pdf (24 pages, 423610 bytes)`; `pdfinfo` Pages 24; footer p.X/24). Will change when frozen result tables/figures are inserted (see K2). | Record final measured count here + cover letter after results pass. |
| A10 | Confidentiality/secret scan passes (BRIEF §3) | PASS | `tools/guard-secrets.ps1` → `SECRET/CONFIDENTIALITY SCAN: CLEAN (368 text files scanned)`, exit 0 (after the morning's `docs/redteam-oracle.md` sanitization; re-run at submission). | Re-run before every push/upload. |
| A11 | No employer-confidential content in paper/metadata (BRIEF §3) | PASS | `pdftotext` scan clean (only the required author block); COI statement names only "the financial sector", no employer/internals. | Re-scan final PDF. |
| A12 | Plagiarism-free (S2) | PENDING | No similarity report; prose original with attributed citations. | Run similarity check before upload. |
| A13 | Cover letter (BRIEF §7) | PENDING | None. | Draft with article type, contribution, originality, COI, AI declaration pointer, page count. |

## B. Required structure (S1 sequence; S2 content)

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| B1 | Title → authors (affiliations/emails in footnote) → abstract+keywords → text → acknowledgments → references | PASS | PDF p.1 title block; sections §1–§10; `Acknowledgments`, `Conflict of Interest`, `Ethics Statement`, `Declaration of AI-Assisted Use in Writing` then `References` (pp. 22–24). | Keep order after results insertion. |
| B2 | Title concise/informative | PASS | `main.tex` title (16 words); matches `CITATION.cff`. | — |
| B3 | Author names with affiliations and emails in a footnote | PASS | p.1: João Brito¹·*; UFABC, Santo André, SP, Brazil; `*Corresponding author: correiadebrito.joao@gmail.com` via the official template's affiliation/footnote mechanism. | — |
| B4 | Abstract ≤200 words + keywords before text | PASS (count) / see C7 | abstract placeholder 111 words (≤200); 6 EN + 6 PT keywords. | Final abstract ≤200 with results/conclusion. |
| B5 | Text includes figures/tables | PENDING | Structures present; all result tables/figures are clearly marked `PENDING` placeholders (no fabricated content). | Insert generated artifacts from the frozen pipeline. |
| B6 | Acknowledgments before references | PASS | `main.tex`; no-funding statement. | — |
| B7 | References last | PASS | PDF pp. 22–24. | — |
| B8 | Mandatory section titled exactly `Declaration of AI-Assisted Use in Writing`, immediately before References | PASS | `main.tex`; PDF p.23; order verified. | Keep exact title/position. |

## C. Written formatting rules (S2; S1 figures/tables/references)

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| C1 | Single-spaced | PASS | `article` 12 pt/14.5 pt baseline (`size12.clo`); no `setspace`/`\linespread`. | — |
| C2 | 12-point font | PASS (body) | `\documentclass[12pt]{SelfArx}` → `size12.clo`; body text 12 pt. Tables `\footnotesize`/`\scriptsize` (wide result/assumption matrices) and bibliography `\footnotesize` are deliberate space measures; body stays 12 pt. | If the editor reads C2 literally, the documented fallback is in K2/K6 (restore 12 pt refs at +0.8–1 pp, compensate with reserve cuts). |
| C3 | Italics instead of underlining | PASS | no `\underline` in sources. | — |
| C4 | Figures/tables in text near citation | PASS | every remaining float and every PENDING block is cited; dangling/unreferenced-float audit → 0. | Re-audit after insertion. |
| C5 | Numbered consecutively | PASS | Tables/figures use LaTeX counters; `captionof` blocks continue the same sequence. | Re-check after insertion. |
| C6 | Titles in captions, not images | PASS | all titles are `\caption`/`\captionof`. | — |
| C7 | Abstract presents objectives, methodology, results, conclusions | FAIL | `PLACEHOLDER ABSTRACT (to be replaced after results)`; no results yet (campaign in progress; none may be invented). | Final ≤200-word abstract after the frozen analysis; mirror in Resumo. |
| C8 | Keywords relevant | PASS | 6 EN + 6 PT, consistent with mechanisms/faults. | — |
| C9 | Header/footer metadata | PASS | running head + footer p.X/24; `pdfinfo` title/author/keywords set (accented author fixed). | — |

## D. Formatting defects (was 43 overfull hboxes; now clean-up state)

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| D1 | No unresolved cross-references | PASS | `main.log`: 0 `undefined`; 0 `Reference undefined`; 0 `Citation undefined`; `bibtex` clean. | Re-run build gate. |
| D2 | Text must not overflow columns/pages | PASS | final log: **1 overfull hbox** (≤2 pt), 0 overfull vbox, 26 underfull hbox + 7 underfull vbox (cosmetic glue warnings only, no text overlap). Morning build had 43 overfull incl. an 81.8 pt collision; fixed with `\emergencystretch`, path-breaking, rewritten lines and tighter floats. | Keep 0 overfull >2 pt at final. |
| D3 | Reference labels legible (two-digit labels) | PASS | `main.tex` `\@biblabel` widened to 0.95 cm; PDF reference pages show `[59]` with spacing. | — |
| D4 | Grammar/spelling | PASS | typo scan clean (`progress-atomicy`, `re-redelivery`, `the authors expect` fixed; US `Acknowledgments`). | Full proofread after results. |
| D5 | No draft markers in the submitted PDF | PENDING | Intermediate build intentionally contains clearly marked placeholders (`PLACEHOLDER ABSTRACT`, `RESUMO PROVISÓRIO`, 6 × `PENDING` result blocks + `pending` verdicts, template DOI/date placeholders). Results are honestly marked as not yet generated; no numbers fabricated. | Remove all internal placeholders only when the frozen artifacts are inserted; template DOI/date/volume placeholders remain venue-assigned. |
| D6 | Bilingual title/Resumo accurate | PASS | PT title now "…Preservando Invariantes de Partidas Dobradas…" (idiomatic); Resumo mirrored (`partidas dobradas`, `livro-razão`). | Author final review (S3 unchanged). |
| D7 | Mechanism-count consistency | PASS | abstract/intro now "six mechanism variants (five mechanism designs) … three combinations"; §4 "six mechanisms and three combinations"; §6 "nine configurations"; §7/§8 reconcile. | — |

## E. References (S1/S2; verified base)

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| E1 | Numbered and cited in brackets | PASS | `abntex2cite` numeric; PDF `[1]…[59]`; all 59 printed entries cited. | — |
| E2 | URLs for all references | PASS | `main.bbl`: 59/59 `\bibitem`s contain URLs/DOIs; 49 peer entries have DOI URLs. | Re-run `verify-refs.py` after any bib edit. |
| E3 | Complete list; no uncited/undefined entries | PASS | printed list 59, all cited; 0 undefined citations; `bibtex` exit 0. | — |
| E4 | Verified base consistent (BRIEF §2 gate) | PASS | `python tools/verify-refs.py` → 63 verified entries; 49 peer (Crossref-verified DOI); 14 gray (22.2%); 228 checks; RESULT PASS (bib/verified.csv unchanged). | Re-run final. |
| E5 | No internal audit notes printed | PASS | morning defect fixed (lead removed `note` fields): bbl audit-note hits = **0**; 14 braced corporate authors render correctly (`Apache Software Foundation`, `PostgreSQL Global Development Group`), garbled forms = 0. | — |
| E6 | Corporate authors rendered correctly | PASS | bbl check: 0 `PROJECT)`/`PROJECT, H.` forms; braced corporate authors (14 refs). | — |
| E7 | Follows template examples | PASS | ABNT numeric, "Disponível em:" venue-style, template class. | — |
| E8 | Claim-bearing references retained | PASS | Compression removed **4 non-essential gray citations from the narrative** (Toxiproxy, HdrHistogram, AWS-idempotency, Kafka-Streams — all cited only supportively), leaving 59 printed. Entries remain in `refs.bib`/`refs/refs.bib` so the 228-check gate is intact. The requested "63→~52 by removing only non-essential gray refs" is **not reachable safely**: only 14 gray refs exist and most are load-bearing (KIP-98/447/429, Kafka design docs, PG isolation/ON CONFLICT/PREPARE, Debezium outbox, Demsar), so further reduction would remove DOI-verified peer sources or weaken claims — deliberately not done. | Lead decision if fewer refs are required. |

## F. AI policy (S3 — binding)

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| F1 | AI cannot be author | PASS | single human author. | — |
| F2 | Mandatory section exact title | PASS | `main.tex`: `\section*{Declaration of AI-Assisted Use in Writing}`. | — |
| F3 | Immediately before References | PASS | document order: Acknowledgments → COI → Ethics → Declaration → References. | Keep COI/ethics before the declaration. |
| F4 | AI limited to formatting/editing/translation/readability; not replace analytical work | PASS | declaration now states AI assisted with editing/formatting/translation/boilerplate code/search logistics and that AI did **not** design the study, derive the model/proofs, decide analyses, or interpret results; all analytical content is the author's. | — |
| F5 | Declaration accurate (no over/underclaim) | PASS | fixes applied: figures split into "data figures produced by deterministic scripts" vs "conceptual diagrams authored in LaTeX/TikZ"; reference claim softened to bibliographic record + supporting statement verified against source/abstract; no data/results generated by AI; AI not an author. | Author re-confirms after results insertion ("verified every number against the artifacts" must be true). |
| F6 | No AI-generated/alterated primary data (S3 §4) | PASS | all numbers from experiments/archived TLC check; no synthetic data in manuscript; results placeholders contain no numbers. | Spot-check final numbers. |
| F7 | No generative-AI result images (S3 §4) | PASS | data figures will be generated by `tools/make-figures.py`; the only current figure is a TikZ conceptual diagram (declared). | Keep script-only generation. |
| F8 | Author full responsibility incl. hallucination/plagiarism/bias | PASS | declaration retains the responsibility clause verbatim. | — |
| F9 | Mandatory human review of AI-assisted content | PASS | "directed and reviewed all AI-assisted output before submission"; author reviewed PT title/abstract. | — |

## G. Ethics, COI, funding (S2)

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| G1 | Conflict of interest declared | PASS | `\section*{Conflict of Interest}` on p.23: employment in financial sector disclosed generically, no employer/systems/information involved, no competing interests, no funding. | Keep (no employer-identifying details). |
| G2 | Ethical approval/statement | PASS | `\section*{Ethics Statement}`: no human participants/personal data/animal subjects; approval not required; synthetic data only. | — |
| G3 | Funding disclosure | PASS | "This work received no specific funding." | — |
| G4 | Data/artifact availability | PENDING | paper says artifact published alongside; no URL (double-blind). | Add availability statement or URL after acceptance (per VENUE-RULES §7.1 N1). |

## H. Length and page count

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| H1 | Length 12–25 pp (S1 suggested; S2 page limits) | PASS | **24 pp** measured (build log, `pdfinfo`, footer). Compression plan executed: 33 pp → 24 pp; 5 tables moved to prose, 2 more to prose (retry/stages), 5→3 algorithms, wide tables footnotesize/scriptsize, bibliography footnotesize, 4→2 result figure placeholders, float spacing tightened, prose −~2.6k words, section headings tightened. | **Caveat:** inserting the frozen result tables/figures replaces minimal PENDING blocks — projected **+1.5–2.5 pp** (see K2). Re-measure then; if >25, apply the documented reserve cuts. |
| H2 | Page count reported | PASS | 24 recorded here and in VENUE-RULES §7.2. | Update after results insertion. |

## I. Language, author information, anonymity

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| I1 | English acceptable (About page; S1 silent) | PASS | English manuscript; dual-language template block retained. | — |
| I2 | Complete author information (S2) | PASS | name, UFABC affiliation, e-mail in manuscript; COI/ethics complete. | Keep in sync with SEER. |
| I3 | Corresponding author marked | PASS | `^{1,*}` + `*Corresponding author:` line. | — |
| I4 | Double-blind review compatibility (About page N1; VENUE-RULES §7.1) | PENDING | Author-identified PDF required by S1; anonymized review copy deliberately not yet produced (per lead, after results insertion). | Build `submission/main-anonymized.pdf` after results: remove author block/affiliation/e-mail, clear PDF author metadata, neutralise acknowledgements that identify. |

## J. Project hygiene

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| J1 | Official template used; provenance recorded | PASS | `SelfArx.cls` SHA-256 `0582DA…DA6367` matches record; live template URL still 404 (Wayback authoritative). | — |
| J2 | Template-vs-guidelines conflicts documented | PASS | 12 pt written rule beats template 10 pt; letter paper/two columns/bilingual block/ABNT numeric retained (`VENUE-RULES.md` §6). | — |
| J3 | CI green (BRIEF §7) | PENDING | No CI run on this tree; CI's paper job (latexmk) does not enforce `build.ps1`'s undefined-ref gate; secret-scan gate now clean locally. | Run CI on final commit; align paper job with `build.ps1`. |
| J4 | Quantitative claims traceable; no invented/stale numbers | PASS | TLC table now matches archived `tlc-summary.json` (txn-only 39, early-probe 22, early+dedup 21; timestamp 2026-11-24T12:51:13Z) and notes count variance; no other measured numbers exist yet (results placeholders). | Re-verify each results number after insertion. |
| J5 | Deviations recorded | PASS | `results/DEVIATIONS.md` + native-process deviation stated in §6.1/§9.3; 8–24 h budget and cut ladder stated in §6.5. | Update after campaign. |
| J6 | Artifact acceptance tests (Maven) green | PENDING | not run in this pass (S3 domain). | `mvn -B -ntp test` green at final. |
| J7 | Final proofread | PASS (current text) | scripted typo/spelling scan clean on this build; results text is placeholder. | Proofread final artifact-inserted build. |
| J8 | Licensing/attribution consistent | PASS | MIT repo; template CC BY-NC-ND footer matches venue; `CITATION.cff` consistent. | Update version at tag. |

## K. Compression-pass records and residual risks

| ID | Requirement | Status | Evidence | Fix |
|---|---|---|---|---|
| K1 | Compression executed without fabricating results | PASS | Results section states data status and PENDING blocks; no numbers, no past-tense measured claims; discussion/conclusion phrases model-derived claims as predictions; TLC numbers remain the only quantitative table and are artifact-matched. | Keep until results pass. |
| K2 | Post-results page budget | PENDING | Current 24 pp uses compact non-floated PENDING blocks (≈1 block-line each). The real artifacts (5 tables + 2 two-panel figures) are estimated at **+1.5–2.5 pp** → projected **25.5–26.5 pp**, i.e. the 25-pp ceiling can be missed at insertion. Documented reserve cuts (from `docs/compliance-report.md` §a, still available): `tab:faults`/`tab:gates` → prose (−0.6), crashverdicts matrix flattened (−0.3), bibliography restore/trim trade, further prose tightening −700 words (−1.1), merge result figures (−0.3). | Re-measure after insertion; execute reserve cuts if >25. |
| K3 | Layout health of this build | PASS | 24 pp; 1 overfull hbox / 0 overfull vbox / 26 underfull hbox / 7 underfull vbox; warnings benign (title block `\twocolumn` height on p.1, template `fancyhdr` headheight, `\balance` in second column, caption hypcap option for `\captionof`); no text overlap. | Optional cosmetic cleanup at final. |
| K4 | Results section honest and compiles | PASS | §7 opens with a `Data status` paragraph; all tables/figures marked PENDING; hypothesis verdicts render `pending` from default macros and will be filled by `results/analysis/macros.tex` if present; section compiles with 0 undefined refs. | — |
| K5 | No invented campaign data | PASS | `results/analysis/` empty; no run data used; only TLC (machine-checked, archived) numbers appear. | — |
| K6 | What a reviewer might miss after compression (recorded for the lead) | PASS | Content moved, not deleted, except proof sketches shortened to one sentence each: (i) component table → §3.1 prose; (ii) ledger schema table → inline column list; (iii) closure table → Lemma 5.2 sentence; (iv) counterexample-trace table → §5.3 prose list (X1/X3/X4/X6 retained); (v) crash-point class table merged as a column into the crash-point table; (vi) retry-semantics table → §4.2 prose; (vii) stage table → §6.5 prose with all N/runs; (viii) design-space table → §4.2 prose; (ix) hypothesis verdict table → §7.5 prose list; (x) TLC table → §5.5 prose with all 8 outcomes and archived counts; (xi) M0/M3 pseudocode → prose (M1/M2/M4 algorithms retained); (xii) 4 result figures → 2 two-panel PENDING figures; (xiii) **proof sketches of the 12 propositions shortened to one sentence each** (propositions and counterexample traces intact); (xiv) 4 lightly-supportive gray citations dropped from the narrative (E8). All protected content (I1–I4, C1–C12, P1–P12, traces, gates G1–G8, statistics plan, controls, oracle independence, threats, AI/COI/ethics, decision-table structure) retained. | Reviewer-facing trade-off to be re-checked in S5 re-review. |

---

## Summary

| Status | Count (row-count by status marker; dual-status rows counted at worst) |
|---|---|
| PASS | 62 |
| FAIL | 2 (A5 package; C7 abstract content) |
| PENDING | 14 |
| **Total rows** | **78** |

**Top residual risks:** (1) K2 — the 25-pp ceiling can be missed when real result artifacts replace the compact PENDING blocks (projected 25.5–26.5); reserve cuts are documented. (2) C7/D5 — the abstract and results section are still placeholders; the submitted PDF cannot go out like this. (3) A5/I4 — no package and no anonymized review copy yet (deliberately deferred per lead). (4) Compressed proof sketches/removed supporting citations may draw reviewer questions (K6/E8). (5) G4 — artifact availability statement pending the double-blind/masked-copy decision.

## Sign-off (final pass, after results insertion)

- [ ] Build green (`build.ps1` exit 0), 0 undefined refs/citations, 0 overfull >2 pt, page count ≤25 measured and recorded.
- [ ] Results/abstract/resumo/pending blocks replaced from frozen artifacts; numbers traceable to run records; TLC numbers unchanged.
- [ ] `verify-refs.py` PASS; `guard-secrets.ps1` exit 0; CI green on tagged commit.
- [ ] Package (PDF + source + cover letter) and `main-anonymized.pdf` in `submission/`; SHA-256 recorded; `v1.0-submission` tag.
- [ ] Rules re-verified on submission day (record date here: ______).
- S6 compliance editor: ____________ (date) · Lead acceptance: ____________ (date)

---

## Final audited build — results inserted (2026-11-25)

**Audited artifact:** `submission/final-paper.pdf` (25 pp) and `submission/anonymized-review.pdf`
(25 pp), built from the working tree at the submission tag with
`powershell -ExecutionPolicy Bypass -File build.ps1 -Main main.tex`.
**Measured:** `BUILD OK (25 pages, 429698 bytes)`; **0 undefined references, 0 undefined citations,
0 overfull hbox** (see `main.log`); abstract ~190 words (EN) / ~190 (PT); results tables generated
from `results/analysis-reduced/` by `tools/analyze_reduced.py`, never transcribed.

**Status changes from the table above:** C7 PASS (abstract now reports objectives, method, results,
conclusions); H1/A9 PASS (**25 pp** measured); K2 PASS (page ceiling met by converting the
fault/gates/decision tables to prose and compressing results prose — reserve cuts executed);
K4 PASS (results section reports the executed reduced programme and descriptive results, no
`pending` macros remain); K5 PASS (every number traces to `results/raw/run-records/` +
`results/RUNLOG.md` + `results/analysis-reduced/`); J5 PASS (A7/M23 deviation recorded);
J6 PASS (Maven tests green on CI); A11 PASS (COI names only "the financial sector").

**Known non-blocking limitation to disclose in the portal (no data action remains):**
the empirical evidence is the reduced A7 programme ($N=3$ correctness cells, no Stage B/C/D); this
is stated in the abstract, §7, §9 and the decision summary. The paper's remaining PENDING markers
are author/venue actions only: similarity check before upload, portal metadata/DOI, and confirming
the venue template at submission time.

**Final counts:** 152 valid run records (2 excluded), 30 invariant-violating runs, 40 correctness
cells, 14 committed-throughput cells, 158/158 harness tests, matrix hashes unchanged
(`5d348efe…`, `f4e731a2…`, `aa9ad0b9…`, 7,631 rows).
