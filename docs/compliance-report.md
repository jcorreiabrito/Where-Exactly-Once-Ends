# COMPLIANCE REPORT — RITA submission readiness audit

**Manuscript:** *Where Exactly-Once Ends: Preserving Double-Entry Ledger Invariants When Apache Kafka Pipelines Write to External Stores*
**Author:** João Brito (UFABC) · **Article type:** Research Article · **Venue:** RITA (UFRGS)
**Audit pass:** P7 **AUDIT + PLAN** (final pass still required after results/analysis tables land)
**Date:** 2026-11-24, America/Sao_Paulo (UTC−3) · **Auditor:** S6 compliance editor
**Audited build:** git `030a4d5` (branch `main`) + working-tree edits in `sections/{mechanisms,method,system}.tex`
**Build command:** `powershell -ExecutionPolicy Bypass -File build.ps1 -Main main.tex -Clean`
**Build result:** **exit 1** — `BUILD PROBLEMS: There were undefined references`; PDF nevertheless produced: `Output written on main.pdf (33 pages, 467048 bytes)` (2026-11-24 10:11:54 local).
**Companion deliverable:** `submission/CHECKLIST.md` (72 item-by-item checks: 38 PASS / 20 FAIL / 14 PENDING / 0 N/A as of the final audit state; one PASS, A10, was a FAIL mid-audit and was fixed by the team during the audit — noted there).

---

## (a) Page-budget analysis

### a.1 Measured baseline (not an estimate)

| Metric | Value | Evidence |
|---|---|---|
| Page count | **33** | `main.log`: `Output written on main.pdf (33 pages, 467048 bytes)`; `pdfinfo main.pdf`: `Pages: 33`, `Page size: 612 x 792 pts (letter)`, PDF 1.5 |
| Last-page reference | p. 33/33 | footer `R. Inform. Teór. Apl. (Online) • … • p.{n}/33` (template `lastpage`) |
| Body pages (title+abstract+text) | 28 | references start p. 29 (`pdftotext -layout main.pdf`, page index) |
| Reference pages | 5 (p. 29–33) | 63 entries, 12 pt ABNT numeric, 63/63 with URLs |
| Body words | 16,419 (all text); **11,673 prose words** excluding float environments | `pdftotext` + word counts; tex scan of `sections/*.tex` |
| Float inventory | 16 tables, 5 figures, 5 algorithms | `grep -c '\\begin{table'` etc. |
| Section page spans (from `main.aux` labels) | front ≈0.7 [estimate: title/abstract block height]; intro 2.0; related 2.0; system 5.0; mechanisms 6.0; formal 3.0; method 4.0; results 2.0; discussion 1.0; threats 2.0; conclusion+ack+declaration 1.0; references 5.0 | `main.aux` `\newlabel{sec:…}{{…}{page}}`; front-matter height measured from the rendered page-1 block |

### a.2 Ablation measurements (real builds; what actually costs pages)

Method: exact copies of `main.tex` + `SelfArx.cls` + `sections/` + `refs.bib` were built in `%TEMP%\wxeo-abl\*` with the same four-pass sequence (`pdflatex → bibtex → pdflatex ×2`). No manuscript file was modified. One transformation per build; page count from `Output written on main.pdf (N pages…)`.

| Ablation | Pages | Δ vs 33 | Interpretation |
|---|---|---|---|
| Baseline copy | 33 | 0 | confirms the measured build is reproducible |
| **No tables** (all 16 `table`/`table*` envs removed) | **27** | **−6** | tables plus the float whitespace they force cost ≈ 6 pp (18 % of the paper) |
| **Remove the 5 candidate tables** (`tab:components`, `tab:posting`, `tab:closure`, `tab:traces`, `tab:kclasses`) | **31** | **−2** | the single biggest safe saving |
| **All remaining tables set `\footnotesize`** | **32** | **−1** | row text still 8–10 pt; booktabs kept |
| Remove 5 tables **and** footnotesize the rest | 31 | −2 | page quantisation absorbs the second effect |
| **No algorithms** (5 envs) | **32** | **−1** | ≈ 0.2 pp/algorithm incl. float whitespace |
| **No figures** (5 envs, incl. TikZ `fig:eos` + 4 placeholders) | 33 | 0 (≈ <1) | figures cost less than one page today; the 4 placeholders will grow with real plots |
| **No floats at all** (tables+figures+algorithms) | **25** | **−8** | prose + references + front matter = 25 pp |
| **Bibliography `\small`** | **32** | **−1** | 63 entries in 5 pp → ≈ 4 pp |
| Bibliography `\footnotesize` | 32 | −1 | no additional page over `\small` at this granularity |

Derived conversion factors used below (measured, not assumed):

- **Prose density ≈ 591–613 words per page; use 600 w/pp.** Two independent routes: (i) the no-floats build has 20 body pages (refs start p. 21 of 25), of which ≈0.25 is acknowledgments+declaration → 11,673 / 19.75 = **591** w/pp; (ii) 25 total − 5 refs − 0.7 front − 0.25 back matter = 19.05 prose pages → 11,673 / 19.05 = **613** w/pp. The 600 w/pp figure is the conservative cut factor (a 600-word cut ≈ 1 pp).
- **Table cost ≈ 0.375 pp/table** on average (6 pp / 16 tables incl. forced whitespace); the five candidate tables average 0.40 pp each.
- **Algorithm cost ≈ 0.20 pp/algorithm** (1 pp / 5).

### a.3 Projected final length once the pending artifacts land

CONTRACTS §11 requires 6 analysis tables (`t1-validation` … `t6-decision`) and 4 vector figures (`fig-correctness`, `fig-latency-ecdf`, `fig-cost-coverage`, `fig-throughput`). Current state: `results/analysis/` contains only `.gitkeep`; `figures/` is empty; `results/DEVIATIONS.md` says “No run records exist yet”; `results/RUNLOG.md` holds dry-run rows only. So the PDF today shows 4 placeholders (`Figure pending analysis`) and six literal `??` for the missing tables.

**Estimate method (explicit):** net new full tables = **5** (t5 mirrors the already-present `tab:tlc`, which it replaces), 5 × 0.375 pp/table = **+1.9 pp**; real figures replacing the 4 placeholder boxes (placeholder ≈ 1 line inside an `\fbox`; a real column-width figure ≈ 5 cm) ≈ **+0.3 pp**; float-placement whitespace risk **+0–0.5 pp**. Total **+2.2 to +2.7 pp**; cross-checked against the lead's briefed estimate for 6 tables + 4 figures of **+2.5–3.5 pp**. Planning figure used below: **+3.0 pp → projected final 36.0 pp (range 35.5–36.5)**.

### a.4 Required reduction

- Ceiling per RITA Research Article suggested length and the S2 checklist item "Length: within the journal's page limits": **25 pp**.
- Measured now: **33 pp**. Projected after required artifacts: **36.0 pp** (35.5–36.5).
- **Required reduction: 11.0 pp (range 10.5–11.5 pp)** to reach ≤ 25 pp without dropping required content.

### a.5 Compression plan (quantified, prioritized, honest)

Every line states its basis; `[M]` = measured ablation, `[D]` = measured density (600 w/pp), `[E]` = estimate with method named. "Weakens" states exactly what the reader loses.

**BASE cuts (safe; content preserved by re-presenting, not deleting):**

| # | Cut | Est. saving | Basis | Weakens |
|---|---|---|---|---|
| B1 | Remove 5 table environments: `tab:components` (→ 2 sentences in §3.1), `tab:posting` (→ inline schema list), `tab:closure` (→ 1 sentence in Lemma 5.2 proof), `tab:traces` (→ compact prose list of X1/X3/X4/X6), `tab:kclasses` (→ add a “class” column to `tab:crashpoints`) | **−2.0 pp** | [M] remove-5 build 33→31 | at-a-glance schema/trace tables lost; all content retained in prose |
| B2 | Set the remaining tables (11 existing + 5 new = 16) to `\footnotesize` (keep `\small` if the editor objects — then +0.2) | **−1.0 pp** | [M] all-tables build 33→32 | small type in wide tables (`t4`); readability |
| B3 | Tighten prose by **2,800 words** (−24 % of the 11,673 prose words) spread over §1–§10 (allocation below) | **−4.7 pp** | [D] 2,800/600 = 4.67 (591 w/pp ⇒ 4.74) | verbosity/justification depth; tightest in §3–§5 |
| B4 | Algorithms 5 → 3: keep M1x, M2, M4 pseudocode; describe M0 and M3 in 3–4 sentences each; `\footnotesize` the three remaining | **−0.6 pp** | [M] no-algorithms build 33→32 (1 pp for 5) + font | no pseudocode for the two controls; code pointer in artifact mitigates |
| B5 | Bibliography at `\small` (keep 12 pt single-spaced body) | **−1.0 pp** | [M] bib-small build 33→32 | possible literal “12-point” objection for the reference list; if the editor objects, restore 12 pt (+1 pp) and recover it with R2/R3 |
| B6 | Data figures 4 → 2 two-panel figures (correctness + cost–coverage as panels A/B; latency ECDF + throughput as C/D), width 0.95 | **−0.3 pp** | [E] measured figure cost <1 pp; halving envs | per-panel size shrinks; all four panels retained |
| | **Base subtotal** | **−9.6 pp** | | projected **26.4 pp** — still over the 25 ceiling |

**RESERVE cuts (needed to actually reach ≤ 25; run only if the post-artifact build is > 25.0):**

| # | Cut | Est. saving | Basis | Weakens |
|---|---|---|---|---|
| R1 | Reference base 63 → 52, removing 11 entries **not used to support any claim** (verify `supports_claim` column in `refs/verified.csv`; never remove a citation carrying a factual statement) | **−0.7 pp** | [E] 4 pp / 63 entries ≈ 0.064 pp × 11 | related-work breadth (still ≥ 50 refs, 49 Crossref-verified) |
| R2 | Drop `tab:retry` and `tab:hypotheses`; fold their content into prose (§4.2 retry paragraph; §7.5 verdict sentence per hypothesis) | **−0.7 pp** | [E] 0.35 pp/table | at-a-glance retry matrix/verdict table; content retained |
| R3 | Extra prose tightening **−500 words** | **−0.85 pp** | [D] 500/591 | more of B3's cost |
| | **Reserve subtotal** | **−2.25 pp** | | |
| | **FULL PLAN (B1–B6 + R1–R3)** | **−11.89 pp** | | **projected 36.0 − 11.89 = 24.1 pp** (range 23.6–24.6) |

**Plan verdict:** the full plan exceeds the required reduction (11.89 ≥ 11.0) with **0.89 pp contingency**. If the author refuses the reserve cuts, the realistic floor is **26.4 pp** (over limit) — so the reserve cuts are not optional unless something else is sacrificed. This is an aggressive plan (prose 3,300 words = −28 %, 11 references, 5 removed + 2 dropped tables, 2 algorithms); it lands at the edge of 25, so the **page count must be re-measured after the artifacts land and re-planned from that measurement** — one bad float placement can add a page.

**Section-level target ceilings (indicative; float whitespace makes per-section attribution fuzzy ±0.7 pp). Sum = 25.0, i.e. the acceptable worst case; the plan's point estimate is 24.1 pp:**

| Part | Current (pp) | Target ceiling (pp) | Main measures |
|---|---|---|---|
| Front matter (title/abstract block) | 0.7 | 0.7 | bilingual template block is mandatory → untouched |
| §1 Introduction | 2.0 | 1.4 | prose −240 w |
| §2 Related work | 2.0 | 1.2 | prose −480 w (biggest single prose cut; 7 subsections survive) |
| §3 System/failure model | 5.0 | 3.2 | prose −560 w; drop `tab:components`/`tab:posting`; merge `tab:kclasses`; footnotesize |
| §4 Mechanisms | 6.0 | 3.7 | prose −380 w; 2 algorithms out; drop `tab:retry` (R2); footnotesize `tab:distinct` |
| §5 Invariant preservation | 3.0 | 2.5 | prose −440 w; drop `tab:closure`/`tab:traces`; footnotesize `tab:crashverdicts` |
| §6 Experimental method | 4.0 | 2.9 | prose −440 w; footnotesize `tab:faults`/`tab:gates`/`tab:stages` |
| §7 Results | 2.0 | 3.2 | **+1.5** for `t1–t4`; **+0.8** real figures (2 panels); prose −130 w |
| §8 Discussion | 1.0 | 1.2 | **+0.35** `t6-decision`; prose −220 w |
| §9 Threats | 2.0 | 1.1 | prose −310 w (5 subsections stay) |
| §10 Conclusion + acknowledgments + AI declaration | 1.0 | 0.8 | prose −100 w; declarations untouched |
| References | 5.0 | 3.1 | `\small` + 11 entries trimmed (R1) |
| **Total** | **33.0** | **25.0 ceiling** | point estimate 24.1 pp |

**Prose-cut allocation (2,800 base + 500 reserve = 3,300 words; 11,673 → 8,373, −28 %), on text outside float environments (measured per file):** intro −240; related −480; system −560; mechanisms −380; formal −440; method −440; results −130; discussion −220; threats −310; conclusion −100. Note: the final abstract may grow from the 103-word placeholder toward the 200-word limit (≈ +0.2 pp); that growth is inside the +2.5–3.5 pp artifact projection, not a prose cut.

### a.6 What must NOT be cut (regardless of page pressure)

- **Invariants I1–I4** (I1a/I1b split), their executable predicates, and Eq. (consistency) — §3.3, `tab:invariants` content.
- **The EOS-boundary formal model**: Eq. (eos), windows W1/W2, Lemma 1 (single-site commit pair), Lemma 2 (closure), Corollary 1, and the crash-point model **C1–C12 plus classes K1–K7** (merging the two tables is fine; deleting points/classes is not).
- **The 12 propositions and 4 counterexample traces** (proof sketches may be condensed to 2–4 lines each, but every proposition must remain identifiable and falsifiable; traces X1/X3/X4/X6 are the evidence for the formal claims).
- **Bounded TLC results with bounds, version and scope qualification** (“bounded witness, not an unbounded proof”; N=3, BatchSize=2, one partition).
- **Harness-validation gates G1–G8** (compact table allowed; gates and their failure actions not).
- **Pre-registered statistics**: Clopper–Pearson, rule of three, McNemar/Fisher, Holm, Friedman/Nemenyi, Cliff's delta, α=0.05, bootstrap 10 000 resamples, warm-up rule, censoring/exclusion rules, N per cell.
- **The controls**: M0 negative control and M1-kafka positive control (their expected-failure/pass semantics are load-bearing).
- **Oracle independence and ground truth**: GT written and hashed before produce, digest chain, checker mutation testing 100 %.
- **All five threats-to-validity subsections**, including the native-process single-host deviation and version specificity.
- **The AI declaration** (exact title, immediately before References, accurate final wording), **COI and ethics statements** once added, acknowledgments/funding.
- **Deviation disclosure** (8–24 h campaign budget, cut ladder, `results/DEVIATIONS.md`), the **decision table output (RQ3)** and the **H1–H5 verdicts**.
- **References that support a claim**, and the URL for every reference.
- **Template furniture**: running head, footer with page numbers, RESEARCH ARTICLE badge, bilingual block.

### a.7 Moves that do NOT save pages (do not waste time / do not mislead the author)

- **Moving proofs, propositions or the TLC material to an appendix saves nothing.** The 12–25-page guidance applies to the manuscript as submitted; appendix pages still count, and an appendix heading/page break typically *adds* 0.2–0.5 pp. Do not propose it as a compression measure.
- Shrinking the body font below 12 pt, tightening margins below the official template's 2 cm, or switching to one column are **not acceptable compliance remedies** (they violate S2's written rules and/or the official template) — the 8 pp of measured float cost must be recovered editorially, not typographically.
- Removing the AI declaration, COI/ethics statements, acknowledgments, or the bilingual block does not help (required content, ~1 pp total).
- Increasing `\textheight`/`\textwidth` or reducing `\columnsep` beyond the template's values is a template deviation; do not count it in the plan.

---

## (b) Formatting defects found in the current build

### b.1 Build and cross-references

- `build.ps1` gate **fails**: 6 `LaTeX Warning: Reference 'tab:…' undefined` in `main.log`; six literal `??` in the PDF (p. 23–26) at: §7.1 "Table ?? summarises the gates"; §7.2 "Table ?? reports…" (twice); §7.3 "Table ?? reports throughput"; §7.3 "Table ?? decomposes"; §8.2 "Table ?? maps a failure exposure". Root cause: `results/analysis/tables/t1-validation.tex`, `t2-correctness.tex`, `t3-counts.tex`, `t4-performance.tex`, `t6-decision.tex` are not generated yet (`results/analysis/` holds only `.gitkeep`).
- BibTeX is clean: `bibtex` exit 0, `main.blg` 0 warnings, 0 undefined citations, 63/63 `\bibitem`s.
- Labels/refs audit: 94 `\label`, 99 `\ref`/`\eqref` uses; the only dangling refs are five distinct table labels (six reference sites) — `tab:validation`, `tab:correctness` ×2, `tab:performance`, `tab:counts`, `tab:decision`. **Two floats are never cited in the text**: `tab:closure` (`sections/formal.tex:53`) and `fig:correctness` (`sections/results.tex:56`) — an orphan float violates the "figures placed near their citation" rule and will look careless to a reviewer.

### b.2 Overfull boxes (visual defects)

`main.log`: **38 `Overfull \hbox`** (15 wider than 5 pt) and **1 `Overfull \vbox`**. The worst is a **visible cross-column collision**:

| Width | Location (source line) | What happens |
|---|---|---|
| **81.80 pt** | `sections/threats.tex:17-22` | the unbreakable `\code{results/faults.log}` overflows the left column on **PDF p. 27** and overlaps the right column's text ("…business rules…") — confirmed by rendering p. 27 to PNG (screenshot evidence in this audit's tool log). |
| 20.19 pt | `sections/related.tex:80-89` | double-entry-ledger paragraph overflows into the gutter |
| 19.45 pt | `sections/related.tex:53-62` | idempotency paragraph |
| 17.72 pt | `sections/related.tex:124-132` | statistics paragraph (`\cite{arcuri2011,arcuri2012}` cluster) |
| 14.80 pt | `sections/results.tex:24-38` | RQ1 paragraph |
| 14.15 pt | `sections/threats.tex:97-154` | reproducibility paragraph spanning file end |
| 12.39 pt | `sections/threats.tex:31-35` | order-drift paragraph |
| **12.25 pt** | `sections/related.tex:7` | **subsection heading** “Kafka exactly-once semantics and stream-processing fault tolerance” overflows its column |
| 10.30–10.83 pt | `related.tex:124-132`, `threats.tex:72-78`, `discussion.tex:18-26` | 3 more visible gutters |
| < 5 pt | 23 further spots (related 5, system 5, mechanisms 5, method 3, results 2, formal 2, discussion 1) | cosmetic hyphenation/spacing |

Fix priority: `threats.tex:17-22` (break the path: `results/\allowbreak faults.log` or `\path{…}`; the `url` package is already loaded), then the >10 pt lines, then the heading. Acceptance: 0 overfull > 2 pt in the final build.

### b.3 Reference-label collision (all two-digit labels)

`main.tex:175` sets `\renewcommand\@biblabel[1]{{\parbox{0.7cm}{[#1]}}}`. A two-digit `[NN]` is wider than 0.7 cm, so labels 10–63 run into the author string: `[10]AKIDAU`, `[11]ZAHARIA`, `[12]CAI`, `[16]FOUNDATION`, `[21]GARCIA-MOLINA`, `[46]PROJECT, H.` … (visible on PDF p. 30; 54 of 63 entries affected, all labels ≥ 10). Fix: widen the parbox to ≥ 0.95 cm or add explicit spacing; rebuild and inspect a two-digit page visually.

### b.4 Typos / singular–plural / spelling consistency

- `sections/conclusion.tex:25` — **“progress-atomicy”** → “progress-atomicity”.
- `sections/conclusion.tex:18` — **“re-redelivery”** → “redelivery”.
- `sections/threats.tex:8` — “the result **the authors expect**” in a single-author paper → “the author expects”.
- `main.tex:157` — heading `Acknowledgements` (UK) vs the venue's own wording “Acknowledgments” (S1/S2). Low risk; align to the venue spelling for a frictionless checklist read.
- No other spelling/doubled-word defect found by a scripted scan (`sections/*.tex` + `main.tex`), but the placeholder text blocks a real proofread.

### b.5 Draft placeholders visible in the PDF

| Placeholder | Location | Status |
|---|---|---|
| `PLACEHOLDER ABSTRACT (to be replaced after results)` | `main.tex:126`; PDF p.1 | must go before submission |
| `RESUMO PROVISÓRIO (a ser substituído após os resultados)` | `main.tex:128`; PDF p.1 | must go |
| `Figure pending analysis` × 4 | `sections/results.tex:48-105`; PDF p.24–25 | replaced by `figures/*.pdf` |
| Hypothesis verdicts `pending` × 5 | `sections/results.tex:134-138` via `\ResHypothesis*Verdict` defaults (`main.tex:51-70`) | replaced by `results/analysis/macros.tex` |
| Result macros rendering `--` | `main.tex:56-70`; §7.1–7.2 | replaced by macros |
| `Vol. XX, Num. XX (2026) 1-XX`, DOI `.XXXX`, `Received/Accepted dd/mm/yyyy` | template fields `main.tex:99-121` | **template-native placeholders** — identical in the official example (`paper-template/template/template.pdf`); acceptable for initial submission, venue assigns at typesetting. |

### b.6 Template-vs-guidelines conflicts (resolved, documented)

| Conflict | Evidence | Resolution in the manuscript | Verdict |
|---|---|---|---|
| Official template example is **10 pt**; S2 written rule says **12 pt** | `paper-template/template/template.tex:22` → `size10.clo` in `template.log`; S2 “12-point font” | compile official `SelfArx.cls` at 12 pt (`main.tex:8`) | compliant with the written rule; documented in `VENUE-RULES.md` §6 |
| Template example abstract is long; S1 says ≤ 200 words | official example abstract (lorem ipsum) > 200 words | manuscript abstract 103 words (placeholder) → final must stay ≤ 200 | compliant by manuscript, not by template example |
| Template ABNT style prints “Disponível em:” in an English paper | `main.bbl` 63 × “Disponível em” | inherited from the venue's `abntex2` style; the official template ships this style | acceptable (venue style); do not “fix” into APA |

### b.7 Table/figure/algorithm inventory

- Captions numbered consecutively: **Tables 1–16**, **Figures 1–5**, **Algorithms 1–5** (PDF text check).
- All figure titles are in captions, not inside the images (the only in-image items are axis labels in the TikZ `fig:eos`).
- Tables use `booktabs`, `\small`; **11 of 16 are full-width `table*`** (`tab:components`, `tab:invariants`, `tab:asm`, `tab:crashpoints`, `tab:distinct`, `tab:retry`, `tab:traces`, `tab:crashverdicts`, `tab:faults`, `tab:gates`, `tab:stages`) and 5 are single-column (`tab:posting`, `tab:kclasses`, `tab:closure`, `tab:tlc`, `tab:hypotheses`). The full-width ones are the main page-cost drivers.

---

## (c) AI-declaration wording audit against the mandatory policy

Current declaration (`main.tex:162-171`; PDF p.29) — sentence-by-sentence verdict against S3 (`https://seer.ufrgs.br/index.php/rita/iaguidelines`, read 2026-11-24):

| # | Sentence as written | Verdict | Reason |
|---|---|---|---|
| 1 | “Generative AI tools were used to assist with the preparation of this manuscript and its artifact: drafting and editing prose, translating the abstract and title into Portuguese, generating boilerplate experiment and analysis code, and assisting with literature retrieval.” | **RISK — partly outside the permitted uses; partly under-explained** | S3 §2 permits “formatting, editing, translation, and improvement of text readability and grammar”. “**Drafting** prose” is text creation, not editing; “**analysis** code” implies AI touched the analytical work (prohibited as a replacement for analytical work); “**literature retrieval**” is not an enumerated use (transparency is still the right call — keep it, but phrase it as logistics). Must also say the AI did not *replace* analytical work. |
| 2 | “The author directed, reviewed and verified all such output.” | **PASS** | Matches S3 §2 mandatory human review; keep. |
| 3 | “No research data, results, tables or figures were generated by AI:” | **PASS in intent** (data/results) — word “tables” is imprecise | All numbers must trace to run records; but authorship of LaTeX table *typesetting* is not what S3 prohibits. Reword to “No research data or results were generated, altered or fabricated by AI”. |
| 4 | “every number in this paper comes from experiments executed by the author with the artifact published alongside it,” | **PASS** (true by design) — keep; verify after results land | — |
| 5 | “and every figure is generated by a deterministic script from committed run records.” | **FALSE — overclaim** | `fig:eos` is a hand-authored TikZ schematic (`sections/system.tex:298-324`), not generated from run records; the statement is false as written and must be split into data figures (script-generated) vs conceptual diagrams (LaTeX/TikZ). |
| 6 | “The author opened and verified every cited reference” | **OVERCLAIM / accuracy risk** | `refs/verified.csv` + `refs/RETRIEVALS.md` show 49 peer-reviewed entries verified via Crossref DOIs and publisher **abstracts/metadata** (several full texts paywalled/403); only some were opened in full. True claim: “the bibliographic record and the supporting statement of every cited reference were verified against the retrieved source/abstract”. |
| 7 | “and assumes full responsibility for the content, including any error, omission or bias.” | **PASS** | Matches S3 §1; keep verbatim. |
| 8 | “AI tools are not authors of this work.” | **PASS** | Matches S3 §1. |
| — | **Missing**: explicit statement that AI did not design the study, derive the formal model/proofs, choose or run the statistical analyses, or interpret results; explicit statement that AI-generated images/charts of results are prohibited and none were used. | **UNDERCLAIM** | S3 §2/§4 put the analytical ownership on the author and restrict AI for result images; the declaration should say both explicitly. |

**Proposed replacement text (drop-in for `main.tex:163-171`, exact section title unchanged):**

> Generative AI tools were used to assist with the preparation of this manuscript and its artifact: editing, rephrasing and formatting prose; translating the title and abstract into Portuguese; generating boilerplate build, harness and plotting code; and assisting with literature-search logistics. AI tools did not design the study, derive the ledger/EOS model or the proofs, choose or execute the statistical analyses, interpret the results, or generate, alter or fabricate any research data or results; all analytical content and decisions are the author's. The author directed and reviewed all AI-assisted output before submission and verified every number against the artifacts released with this paper. No research data or results were generated, altered or fabricated by AI. Data tables and data figures are produced by deterministic scripts from committed run records; conceptual diagrams are authored in LaTeX/TikZ. The author verified the bibliographic record and the supporting statement of every cited reference against the retrieved source or its published abstract, and assumes full responsibility for the content, including any error, omission or bias. AI tools are not authors of this work.

The policy's exact-title rule (`Declaration of AI-Assisted Use in Writing`) and its placement immediately before References are already satisfied and must not change (`main.tex:162`, PDF p.29). S3's image restriction is satisfied in substance (no generative-AI result images), but the declaration must say so per the draft above.

---

## (d) Metadata audit

| Element | Current value / location | Verdict | Action |
|---|---|---|---|
| Full title | “Where Exactly-Once Ends: Preserving Double-Entry Ledger Invariants When Apache Kafka Pipelines Write to External Stores” (`main.tex:102`; PDF p.1) | PASS | keep; matches `CITATION.cff:3` |
| Short title / running head | “Where Exactly-Once Ends” (`main.tex:103`; header on every page) | PASS | — |
| Portuguese title `\Titulo` | “Onde o Exactly-Once Termina: Preservando Invariantes de Razão de Dupla Entrada…” (`main.tex:105`; PDF p.1) | PENDING — translation quality | “Razão de Dupla Entrada” is a calque (double-entry = *partidas dobradas*; livro-razão = ledger). Author review required by S3 §2; see CHECKLIST D6. |
| Authors / affiliation | João Brito¹·*, UFABC, Santo André, SP, Brazil (`main.tex:111-112`) | PASS | matches BRIEF §1 |
| Corresponding author + email | `*Corresponding author: correiadebrito.joao@gmail.com` (`main.tex:113`) | PASS | matches BRIEF §1; also set in SEER |
| Keywords (EN) | 6 terms: Exactly-once semantics; Apache Kafka; double-entry ledger; transactional outbox; idempotency; fault injection (`main.tex:115`) | PASS | keep in sync with final abstract |
| Palavras-chave (PT) | 6 equivalents (`main.tex:117`) | PENDING (same translation review as `\Titulo`) | — |
| Abstract | 103 words incl. label (96 excl.); ≤200 limit OK but placeholder | FAIL (content) / PASS (count) | rewrite final ≤200 with objectives/method/results/conclusion; recount |
| Resumo | 102 words; placeholder | FAIL (content) | rewrite after results; keep ≤200-ish |
| DOI / Received / Accepted | `10.22456/2175-2745.XXXX`, `dd/mm/yyyy` (`main.tex:119-121`); identical pattern to official template example | PASS (template placeholders) | leave unless the venue supplies values; never invent a DOI |
| Volume/number/year/page range | `Vol. XX, Num. XX (2026) 1-XX` (`main.tex:99`, `107-109`) | PASS (template placeholders) | venue typesets |
| Article-type badge | `RESEARCH ARTICLE` (`main.tex:100`) | PASS | matches the "Research Articles" submission category (S1) |
| PDF metadata `Title` | “Where Exactly-Once Ends” (short title, not full) | MINOR — recommend full title | set `pdftitle={Where Exactly-Once Ends: Preserving Double-Entry Ledger Invariants …}` in `\hypersetup` (`main.tex:93-94`) |
| PDF metadata `Author` | “Joao Brito” (accent lost in `pdfauthor`) | MINOR | `pdfauthor={João Brito}` (UTF-8 with inputenc works for pdfTeX) |
| PDF metadata `Keywords` | empty | MINOR (not required) | optionally set via `pdfkeywords={…}` |
| PDF page format | 612×792 pt (US Letter) | PASS | template `geometry` letterpaper, same as official example |
| PDF fonts | all embedded/subset (`pdffonts`: yes for every font) | PASS | — |
| Footer/date | “p.{n}/33 • 2026” via `lastpage`/`\PaperAno` | PASS (updates with final build) | verify p.N/N = measured page count |
| `CITATION.cff` | title/author/affiliation/email match; version 1.0.0, date-released 2026-11-24 | PASS (consistency) / PENDING (tag) | align version/date with `v1.0-submission` tag (BRIEF §7) |

---

## (e) Reference-style audit

| Check | Result | Evidence |
|---|---|---|
| Style/format | Numeric, bracketed, ABNT `abntex2-num` (template style; `\citebrackets[]`, `main.tex:29-30`) | PDF citations `[1, 2]`, `[3, 7]`; list `[1]…[63]` |
| Numbered and cited in brackets (S1) | PASS (citations) | all 63 `\cite` keys resolve, 0 `Citation undefined` in `main.log`, `bibtex` clean; the five dangling `\ref` labels are the not-yet-generated result tables (b.1), rendered as `??` this build |
| Uncited or undefined entries | PASS | 63 `refs.bib` = 63 `\bibitem` = 63 distinct cited keys (`main.aux`); `bibtex` includes only cited entries; no case of bib-not-cited or cited-not-bib |
| Verified-base consistency | PASS | `python tools/verify-refs.py` → 63 verified entries; 49 peer (Crossref-verified DOI); 14 gray (22.2%); 228 checks — RESULT: PASS; DOI set difference bib↔`refs/verified.csv` = 0; `refs/refs.bib` and root `refs.bib` byte-identical (SHA-256 `8659F88D…F034A`) |
| URLs for all references whenever available (S2) | PASS | 63/63 `\bibitem`s contain a URL; 63 “Disponível em” lines in `main.bbl`; peer entries use DOI URLs, gray entries their source URL (`urldate 2026-11-24`) |
| Internal audit notes leaking into the printed list | **FAIL** — 14 entries print `note` fields such as “Primary technical artifact, retrieved and cached.”, “Primary technical documentation; cached copy of official docs source.”, “Peer-reviewed, no publisher DOI; cached JMLR page.” | Positions in `main.bbl`: 1, 2, 7, 14, 15, 16, 24, 30, 31, 32, 33, 46, 52, 55; visible on PDF p.29–33 |
| Corporate/institutional authors | **FAIL** — 4 garbled (`kip98`, `kip447`, `kip429`: `PROJECT), A. S. F. A. K.`; `hdrhistogram`: `PROJECT, H.`), 9 merely inverted/acronymised (`kafka-design`, `kafka-streams`, `pg-isolation`, `pg-onconflict`, `pg-prepare`, `toxiproxy`, `stripe-idempotency`, `debezium-outbox`, `aws-idempotency`). Cause: parentheses/lower case inside `author = {…}` and no braced corporate names. | `.bbl` inspection + PDF p.29–32 |
| Two-digit label spacing | **FAIL** — 54/63 labels (all ≥ 10) collide with the following text due to the 0.7 cm `\parbox` label (`main.tex:175`) | PDF p.30 visual check |
| “Disponível em:” in an English paper | ACCEPTABLE — venue/template ABNT convention, not a defect | official template ships `abntex2`; RITA is a Brazilian journal |
| Numerical completeness vs `refs/verified.csv` | PASS | 63 rows, 63 unique keys, all peer entries `doi_verified_crossref=Y`, all entries have `url`, retrieval UTC and supporting quote |
| Delete-unverifiable rule (BRIEF §2) | PASS (maintenance) | all 63 have cached sources (`data/raw/lit/`, per P1 gate and verify-refs “every entry has a cached raw source”) |

**Fix order:** remove `note` fields from `refs.bib` (keep provenance in `refs/verified.csv`/`RETRIEVALS.md`) → brace corporate authors → widen the bib label → rebuild → visually inspect one 2-digit reference page and re-run `verify-refs.py`.

---

## (f) Rules verification table (every rule this submission relies on, with URL and read date)

Read dates: original rules record by the lead **2026-11-24** (`submission/VENUE-RULES.md`); **re-verification by this audit 2026-11-24 ~10:13–10:16 America/Sao_Paulo** (all three binding pages returned HTTP 200; the template URL was re-tested with an HTTP HEAD request). **No drift found; `submission/VENUE-RULES.md` requires no update.** One attribution nuance and three additional venue facts are noted at the bottom.

| # | Rule (verbatim/paraphrase) | Source (URL) | Read on | Live re-check result | Manuscript status |
|---|---|---|---|---|---|
| R1 | Research Articles: original findings; state research question, methodology, results; suggested length **12–25 pages** | https://seer.ufrgs.br/index.php/rita/AuthorGuidelines | 2026-11-24 (orig) / 2026-11-24 (re-read) | same text, HTTP 200 | **FAIL** (33 → 36 pp; plan §a) |
| R2 | Initial submission: **PDF formatted in LaTeX**; final version supplies **LaTeX source** | S1 + https://seer.ufrgs.br/index.php/rita/about/submissions | 2026-11-24 / 2026-11-24 | same text | PASS (PDF+source) / PENDING (package) |
| R3 | Required sequence: Title → author names (affiliations and emails **in a footnote**) → Abstract (≤200 words) + Keywords → Text (incl. figures/tables) → Acknowledgments → References | S1 | 2026-11-24 / 2026-11-24 | same text | PASS structure (B1–B8; B5 figure content pending) |
| R4 | Figures/tables **numbered consecutively**, placed **near their citation in the text**, **titles in the caption, not in the image** | S1 | 2026-11-24 / 2026-11-24 | same text | PASS numbering/captions; FAIL: 2 uncited floats; PENDING real figures |
| R5 | References **numbered and cited in brackets**, following the template's examples | S1 | 2026-11-24 / 2026-11-24 | same text | PASS numbering/brackets; FAIL label spacing and corporate-author/notes defects (§e) |
| R6 | **URLs included for all references whenever available** | S2 | 2026-11-24 / 2026-11-24 | same text | PASS 63/63 |
| R7 | Formatting: **single-spaced; 12-point font; italics instead of underlining (except URLs); figures and tables placed within the text** | S2 | 2026-11-24 / 2026-11-24 | same text | PASS C1–C3 (body 12 pt/single/italics); FAIL C4 (2 uncited floats; 4 figures pending) |
| R8 | Content checklist: title concise/informative; abstract presents objectives, methodology, results, conclusions; keywords relevant; clear methodology/results/discussion/conclusion; complete references/citations; figures/tables legible with informative captions; **length within page limits** | S2 | 2026-11-24 / 2026-11-24 | same text | PASS structure/captions; FAIL abstract content + length; PENDING figures |
| R9 | **Originality**: work is original and not under consideration elsewhere | S2 | 2026-11-24 / 2026-11-24 | same text | PENDING (cover letter/SEER attestation) |
| R10 | **Conflict of interest**: declared any potential conflicts | S2 | 2026-11-24 / 2026-11-24 | same text | **FAIL** — no statement; fix G1 |
| R11 | **Ethical approval**: obtained necessary approvals | S2 | 2026-11-24 / 2026-11-24 | same text | **FAIL** — no statement; add the N/A declaration (G2) |
| R12 | Language/style: clear, concise, objective; free of grammar/spelling/punctuation errors; plagiarism-free; **author information complete** | S2 | 2026-11-24 / 2026-11-24 | same text | author info PASS; language FAIL (b.4); plagiarism PENDING |
| R13 | AI **cannot be author/co-author** | https://seer.ufrgs.br/index.php/rita/iaguidelines | 2026-11-24 / 2026-11-24 | same text | PASS |
| R14 | Authors assume **full responsibility** incl. accuracy, data integrity, no plagiarism/hallucination/bias | S3 | 2026-11-24 / 2026-11-24 | same text | PASS (declaration sentence) |
| R15 | AI use permitted only for **formatting, editing, translation, readability/grammar**, not to replace theoretical/analytical work; **mandatory human review** | S3 | 2026-11-24 / 2026-11-24 | same text | **FAIL wording** (§c) |
| R16 | **Mandatory section titled exactly “Declaration of AI-Assisted Use in Writing”, immediately before References** | S3 | 2026-11-24 / 2026-11-24 | same text | PASS (title/position); FAIL accuracy/wording (§c) |
| R17 | **Prohibition: AI to generate, alter or fabricate primary research data** | S3 | 2026-11-24 / 2026-11-24 | same text | PASS by design (all data from experiments) |
| R18 | **Restriction: no generative AI for scientific images/charts/figures representing research results** | S3 | 2026-11-24 / 2026-11-24 | same text | PASS (data figures script-generated; TikZ diagram is conceptual) — declare |
| R19 | Official **LaTeX template** required | S1/S2 (link `http://www.inf.ufrgs.br/revista/RITA2018.zip`) | 2026-11-24 / **2026-11-24** | **still HTTP 404** (re-tested); Wayback copy remains authoritative; `SelfArx.cls` SHA-256 matches the record | PASS (VENUE-RULES §6, `submission/VENUE-RULES.md:15-21`) |
| R20 | Published works **CC BY-NC-ND 4.0**; immediate, permanent open access (the original rules record's “diamond OA / no APC” claim stands from the journal-site read) | https://seer.ufrgs.br/index.php/rita/about | 2026-11-24 (About page, new read) | licence + open-access confirmed | PASS (template footer carries the licence line) |
| R21 | **Submissions in Portuguese, Spanish and English are welcome** | https://seer.ufrgs.br/index.php/rita/about | 2026-11-24 (About page, new read) | confirmed | PASS (manuscript English) |
| R22 | **Double-blind peer review**; authors have 20 days to revise | https://seer.ufrgs.br/index.php/rita/about | 2026-11-24 (About page, **new fact not in the rules record**) | confirmed | PENDING — confirm SEER anonymisation or prepare blinded copy (CHECKLIST I4) |
| R23 | Venue handles final typesetting and page numbering | S1 | 2026-11-24 / 2026-11-24 | same text (not recorded in VENUE-RULES; no obligation on authors) | N/A — template footer already numbers pages |
| R24 | SEER submission system | S1/S2 | 2026-11-24 / 2026-11-24 | same text | PENDING (author action A7) |

**Re-verification notes (no rule drift):**

1. All three binding sources returned HTTP 200 with the same substantive text as the 2026-11-24 record (S1: categories, sequence, figures/tables, references; S2: full preparation checklist; S3: authorship/permitted uses/mandatory declaration/data and image prohibitions).
2. The template URL is still dead (**HTTP 404**, re-tested 2026-11-24); the Wayback-recovered class remains the correct source and its SHA-256 still matches the record — no action.
3. **Attribution nuance:** the rules record says “S1 accepts Portuguese/Spanish/English”; the live Author Guidelines page does not state a language policy. The language acceptance is on the **About** page (R21). Substance unchanged, but future edits should cite the About page.
4. **New venue facts discovered on the About page (not in `VENUE-RULES.md`):** double-blind peer review with a 20-day revision window (R22), CC BY-NC-ND 4.0 licensing (R20), explicit PT/ES/EN language welcome (R21), no APC. The lead should decide whether to append them to `submission/VENUE-RULES.md`; per instructions, the record was **not edited** because none of the recorded rules changed.

---

## (g) Remaining risks (ranked)

| # | Risk | Severity | Likelihood | Mitigation / owner |
|---|---|---|---|---|
| 1 | **Final length still > 25 pp** even after the plan (estimate 24.1, range 23.6–24.6; float placement can add 1 pp) | High | Medium | Re-measure after artifacts; hold the reserve cuts (R1–R3) back until needed; last-resort additional −0.5 pp by merging the two remaining result figures into one and trimming §2 by another 300 words (lead + S6, after S4 lands tables) |
| 2 | **AI declaration judged inaccurate** (overclaims about figures/references, “drafting/analysis code” wording) | High | Medium-High (the current text is demonstrably inaccurate) | Replace with §c draft; author must confirm every clause is true before submission; keep the exact title/position |
| 3 | **Results never land / land late** → placeholder PDF risk, undefined refs, no verdicts | High | Medium (campaign not started: `results/DEVIATIONS.md` says no run records) | Hard gate: no upload until `build.ps1` exit 0 and `results/analysis/tables/*` exist; this audit must be repeated |
| 4 | **TLC numbers not traceable to the archived artifact** (36 vs 39; 21 vs 22; timestamp mismatch) | High (integrity) | Certain (confirmed) | Regenerate the table/caption from `formal/tlc-results/tlc-summary.json` or drop volatile counts; never edit the artifact to match prose; re-check after `t5` lands |
| 5 | **Confidentiality/personal-path hits** in the tree (initially `docs/redteam-oracle.md`; earlier `results/dryrun` and `results/synthetic` paths) made guard-secrets red mid-audit | Medium-High (project firewall; public repo) | Failed at ~10:24, **clean at 10:30:39** (`CLEAN, 212 text files, exit 0`); the tree is still being edited, so it can regress | Sanitize on sight; re-run `tools/guard-secrets.ps1` until exit 0 before every push/upload; if git history contains paths, decide on history rewrite with the lead |
| 6 | **Reference list appearance** (notes, garbled corporate authors, label collision) — reviewers see it before the argument | Medium | Certain | Fix §e items; visual check of a two-digit reference page |
| 7 | **Severe overfull box** (`threats.tex:17-22`, 81.8 pt, visible overlap) | Medium | Certain | Break the path token; rebuild; 0 overfull > 2 pt |
| 8 | **S2 “12-point font” read literally** could object to `\small` bibliography / footnotesize tables | Low-Medium | Low | Keep body 12 pt; if objected, restore bibliography to 12 pt (+1 pp) and recover the page elsewhere (reserve cuts) |
| 9 | **Double-blind handling** (author identity in PDF + metadata vs About-page double-blind review) | Low-Medium | Low-Medium | Confirm with the editor; produce a blinded copy only if requested; do not strip the S1-required footnote from the submitted file |
| 10 | **Cover letter/SEER metadata inconsistency** (title/abstract/page count vs PDF) | Low | Medium | Single source of truth: copy from the final build and this checklist at upload time |

---

## (h) Integrity and repository-hygiene spot checks

1. **Claim-to-artifact traceability (one confirmed defect).** `sections/formal.tex:347-360` (TLC table) vs `formal/tlc-results/tlc-summary.json` (checked 2026-11-24T12:51:13Z): `06-txn-only` distinct states **39** in the artifact vs **36** in the paper; `07-early-probe` **22** vs **21**; caption timestamp **12:47:30Z** in the paper vs **12:51:13Z** in the artifact. `formal/README.md:118` says the count varies 36–39 and instructs “do not hard-code them here”. This violates BRIEF §2 (“every quantitative claim traces to a file”) and is the pattern the AI policy's accuracy clause prohibits. Fix by regenerating the table from the JSON or by removing the volatile counts.
2. **Results provenance.** `results/DEVIATIONS.md` (generated 2026-11-24T13:09:54Z) states no run records exist yet; `results/RUNLOG.md` contains only dry-run rows; `results/analysis/` and `figures/` are empty. Therefore **every results claim in §7–§8 is currently a placeholder and must not be read as a result**; the final compliance pass must re-verify each prose number against a run record (BRIEF §2).
3. **Verification gates re-run at the end of this audit (2026-11-24 10:30 −03):** `python tools/verify-refs.py` → PASS (228 checks, 63 entries); `build.ps1` → exit 1 (6 undefined refs); `tools/guard-secrets.ps1` → **CLEAN, exit 0** (212 text files scanned at that moment; it failed mid-audit at ~10:24 with 7 hits in `docs/redteam-oracle.md`, which the team sanitized during the audit, and the scanned-file count changes because the tree is being edited). The PDF text and metadata remain clean of employer/internal content.
4. **Git/package state:** branch `main`, HEAD `030a4d5`, no tags (`v1.0-submission` missing); `submission/` contains only `VENUE-RULES.md` **and this pass's `CHECKLIST.md`** (no PDF/source/cover letter yet); working tree is being actively edited by S3/S4/S5 agents (files changed during this audit) — final pass must start from a clean tree with a recorded commit SHA.
5. **Confidentiality of the PDF:** `pdftotext main.pdf` shows only the required author block; no employer/product/internal host names, no local paths. PDF metadata carries the author name (see (d)).

---

## Appendix — exact commands and outputs used as evidence

```
# build (documented command; BRIEF §9)
powershell -ExecutionPolicy Bypass -File build.ps1 -Main main.tex -Clean
  -> exit 1; "BUILD PROBLEMS: There were undefined references";
     "Output written on main.pdf (33 pages, 467048 bytes)"

# page count / metadata / fonts
pdfinfo main.pdf           -> Pages: 33; Page size: 612 x 792 pts (letter); PDF 1.5; Producer MiKTeX pdfTeX-1.40.28
pdffonts main.pdf          -> every font embedded (yes)

# text extraction and counts
pdftotext -layout main.pdf main.txt
  -> abstract 103 words (96 excl. placeholder label); resumo 102; 16,419 body words; 11,673 prose words (floats excluded)
  -> 6 x "??" in the PDF; 63 x "Disponível em" in the reference section

# log analysis
Select-String main.log -Pattern 'Overfull \\hbox'   -> 38 hits (15 > 5 pt; max 81.79974 pt)
Select-String main.log -Pattern 'undefined'        -> 6 undefined table references, 0 undefined citations
(Select-String main.bbl -Pattern '\\bibitem').Count -> 63; (Select-String refs.bib -Pattern '^@').Count -> 63

# reference base gate
python tools/verify-refs.py -> "63 verified entries | 49 peer (Crossref-verified DOI) | 14 gray (22.2%) | 228 checks"
                              "RESULT: PASS - reference base is internally consistent and DOI-verified"

# confidentiality gate (state changes with the tree; final at 10:30:39)
powershell -ExecutionPolicy Bypass -File tools/guard-secrets.ps1
  -> 10:24: exit 1; "SECRET/CONFIDENTIALITY SCAN: 7 HIT(S)" (docs/redteam-oracle.md)
  -> 10:30:39: exit 0; "SECRET/CONFIDENTIALITY SCAN: CLEAN (212 text files scanned)"

# page-budget ablations (temp copies only; manuscript untouched)
%TEMP%\wxeo-abl\{base,notables,remove5,tablesfoot,noalgos,nofigs,compound,bibsmall,bibfoot}
  base 33 | no tables 27 | remove 5 tables 31 | all tables footnotesize 32 | no algorithms 32
  no figures 33 | no floats 25 | bibliography \small 32 | bibliography \footnotesize 32

# live venue rules (re-read during this audit)
HTTP 200: https://seer.ufrgs.br/index.php/rita/AuthorGuidelines
HTTP 200: https://seer.ufrgs.br/index.php/rita/about/submissions
HTTP 200: https://seer.ufrgs.br/index.php/rita/iaguidelines
HTTP 200: https://seer.ufrgs.br/index.php/rita/about            (About page: double-blind, CC BY-NC-ND, PT/ES/EN)
HTTP 404: http://www.inf.ufrgs.br/revista/RITA2018.zip           (HEAD request; still dead -> Wayback copy stands)
```

*No manuscript file, section, or code file was modified by this audit. The two deliverables of this pass are `submission/CHECKLIST.md` and this report; the page-budget estimates are labelled with their measurement/estimation method, and no page number is invented.*
