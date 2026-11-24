# Literature search strategy — S1 (Literature Scout)

Project: *Where Exactly-Once Ends: Preserving Double-Entry Ledger Invariants When Apache Kafka
Pipelines Write to External Stores* (RITA, UFRGS).
Scope: complete, verified reference base for the article, including the Amendment A1 additions
(M0/M4 mechanisms, measurement methodology, statistics for mostly-zero correctness outcomes,
formal methods for ledger invariants, fault-injection methodology, reproducibility).
Search executed **2026-11-24, 12:00:52–12:27:34 UTC** (all timestamps in this repo are UTC).

## 1. Sources and APIs used

| Source | Endpoint(s) | Use |
|---|---|---|
| Crossref REST API | `https://api.crossref.org/works/{doi}?mailto=…`, `https://api.crossref.org/works?query.bibliographic=…&rows={5,6,8}&select=DOI,title,container-title,issued,type,author&mailto=…` | discovery (83 queries), exact title/venue/year for every peer entry, DOI resolution |
| OpenAlex | `https://api.openalex.org/works/doi:{doi}?mailto=…` | abstract recovery (`abstract_inverted_index`), open-access PDF location |
| Unpaywall | `https://api.unpaywall.org/v2/{doi}?email=…` | open-access PDF/landing lookup |
| Semantic Scholar Graph API | `https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=title,abstract,venue,year,authors,externalIds,openAccessPdf` | attempted abstract recovery (HTTP 200 but no abstracts returned in this run) |
| Europe PMC REST | `https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=…&format=json&resultType=core` | abstract recovery for older biomedical/statistics journals (Hanley 1983, Newcombe 1998) |
| Publisher / project pages | `kafka.apache.org`, `cwiki.apache.org`, `postgresql.org/docs`, `docs.stripe.com`, `debezium.io`, `aws.amazon.com/builders-library`, `jmlr.org`, `infoq.com` | primary technical artifacts (gray literature) |
| Official docs source | `raw.githubusercontent.com/apache/kafka/trunk/docs/**` | Apache Kafka design + Streams configuration documentation (the public site is a client-side redirect) |
| Author/repository PDFs | `arxiv.org`, `biblio.ugent.be`, `ir.cwi.nl`, `atlantis-press.com`, `projecteuclid.org` | open full text for quotes |
| Tool documentation | `raw.githubusercontent.com/Shopify/toxiproxy`, `raw.githubusercontent.com/HdrHistogram/HdrHistogram` | fault-injection and latency-measurement artifacts |

Politeness/access rules enforced by `tools/fetch-lit.py`: one request at a time per host,
≥1.1 s between requests to the same host, descriptive User-Agent containing the contact e-mail,
`robots.txt` fetched and honoured for every plain-HTTP host (a disallowed path is skipped and
logged), no bulk or parallel crawling. All raw responses are cached byte-for-byte under
`data/raw/lit/` with a `<key>.json` record of `{url, final_url, fetched_utc, http_status,
content_type, sha256, bytes}`; Crossref/Unpaywall records under `data/raw/lit/_crossref/`.
Every attempt — success or failure — is appended to `data/raw/lit/_fetchlog.jsonl` and tabulated
in `refs/RETRIEVALS.md`.

## 2. Exact query strings

Eighty-three Crossref `query.bibliographic` strings were run in four batches (results cached in
`lit/_discovery/_queries*.json`); the seed references identified from prior domain knowledge were
then verified individually by DOI (69 Crossref DOI lookups). **Appendix A lists every query string
verbatim.**

## 3. Screening criteria

A record was **included** if all of the following held:

1. it is peer-reviewed (journal/venue article with a Crossref-registered DOI) **or** a primary
   technical artifact (KIP, official product documentation, tool README, vendor API reference)
   needed to define a mechanism or a measurement instrument;
2. it addresses at least one of: exactly-once/delivery semantics, stream-processing fault
   tolerance, transaction commit/idempotency/consistency, microservice data-consistency patterns
   (outbox/inbox/saga/CDC), double-entry ledger or auditability, formal specification of
   concurrent/commit systems, benchmarking methodology, statistics for small-sample/zero-event
   outcomes, fault injection or reproducibility;
3. a verifiable text passage could be opened and cached (landing page, abstract record, open PDF,
   or official documentation), to which one exact supporting sentence is anchored;
4. for peer entries, the DOI resolves through Crossref to the same title (fuzzy ≥ 0.90, checked by
   `tools/verify-refs.py`).

A record was **excluded** if any of: non-peer-reviewed commentary/book chapter/encyclopedia entry
without primary-definition value; low-quality or predatory-adjacent venue with no methodologically
usable content; content mismatch discovered after reading the retrieved abstract (e.g. a paper
titled around transactions that in fact treats declarative query compilation); no retrievable text
within robots/paywall limits; duplicate of an already-included work (preprint vs. published
version — the published version is kept and, where the quote came from an author preprint, this is
stated in `refs/verified.csv` `notes`).

Gray literature is admitted **only** for primary definitions/artifacts and is capped at 30 % of
the final base; the achieved proportion is **14/63 = 22.2 %**.

## 4. PRISMA-style flow

| Stage | Count | Notes |
|---|---|---|
| Records identified — Crossref discovery queries | 470 | 83 queries × 5–6 rows (some queries returned fewer) |
| Records identified — seed references submitted directly | 69 | known classics + KIPs/docs verified by DOI/URL |
| Total identified | 539 | |
| Duplicates removed (DOI/title normalisation) | 104 | 470 discovery records collapse to 400 unique DOIs / 366 unique normalised titles; seeds overlap discovery |
| Unique records screened (title/abstract) | 435 | 366 + 69 |
| Excluded at title/abstract screening | 356 | irrelevant field, non-peer venue, book chapter/encyclopedia, superseded version, or duplicate |
| Works carried to full retrieval & record verification | 79 | 57 peer candidates + 16 gray candidates + 6 late exploratory candidates (statistics/methods) |
| Excluded after reading the retrieved source | 16 | 11 peer (content mismatch or no accessible text), 2 gray (unretrievable/unnecessary dup), 3 exploratory statistics candidates superseded |
| **Included in the verified base** | **63** | **49 peer-reviewed (all with Crossref-verified DOI) + 14 gray (22.2 %)** |

All exclusions are itemised with their reason in `refs/RETRIEVALS.md` (sections E and F).

## 5. Deduplication policy

Primary key: DOI lower-cased. If a record has no DOI, the key is the normalised title (HTML tags
and LaTeX removed, lower-cased, non-alphanumerics collapsed). Preprint/published pairs are merged
on title similarity ≥ 0.95 plus author+year agreement; the Crossref-registered published version is
retained, and when the anchored quote comes from an author preprint or repository copy this is
recorded in the `notes` column (e.g. `txncommit2006`, `bailis2014`, `georges2007`). Two versions of the
same work found in discovery (e.g. journal + proceedings DOIs) are reduced to one entry.

## 6. Reference-key convention and verification pipeline (acceptance)

BibTeX keys are unique, lower-case and stable: peer entries use `firstauthorYEAR`
(e.g. `chandy1985`, `flink2017`; a short slug is appended only where the same author-year occurs
twice — none in this base), gray entries use the artifact's canonical identifier (`kip98`,
`kafka-design`, `pg-onconflict`, …). The key is the join column across `refs/refs.bib`,
`refs/verified.csv`, `lit/matrix.csv` and the cached raw files `data/raw/lit/<key>.*`.

1. `tools/fetch-lit.py checkquotes --claims lit/_claims.json` — every one of the 63 quotes is
   checked as a literal substring (after Unicode/whitespace normalisation) of the cached source
   text: **63/63 verified**.
2. `tools/verify-refs.py` — structural + attribution + Crossref title-fuzzy + gray-URL + cached
   evidence checks: **PASS, 228 checks**, offline against cached Crossref records and live against
   `api.crossref.org` (`--live`).
3. Failures found during the work were fixed, not waived: missing years for two IEEE entries
   (eosmsg2001, eoscorba1999, Crossref `issued` empty) were filled from the proceedings record;
   an initial wrong KIP-447 URL (404) and a redirect-only Kafka documentation page were replaced
   with the correct/primary sources and re-fetched.

## 7. Coverage vs. the brief (and the Amendment A1 additions)

| Topic block | Included anchors |
|---|---|
| Kafka EOS primaries | `kip98`, `kip447`, `kip429`, `kafka-design`, `kafka-streams` (gray, primary) + `kafka2015`, `sax2018` (peer) |
| Stream-processing fault tolerance theory | `chandy1985`, `millwheel2013`, `flink2017`, `dstreams2013`, `naiad2013`, `dataflow2015` |
| Exactly-once / transactions / idempotency | `eosmsg2001`, `eosmid2012`, `eoscorba1999`, `txncommit2006`, `sagas1987`, `berenson1995`, `bailis2014`, `kung1981`, `vogels2009`, `pg-isolation`, `pg-prepare`, `pg-onconflict`, `stripe-idempotency`, `aws-idempotency` |
| Outbox/inbox/CDC, dual-write, M2/M4 | `debezium-outbox`, `laigner2021`, `micro2026`, `saga2019`, `saga2022`, `kafka-design` (external-sink / offset-coordination statement) |
| Ledger, auditability, append-only | `triple2019`, `triple2021`, `lsfs1992`, `lamport1978`, `eventsourcing2022` |
| Formal methods | `lamport1994` (TLA), `jackson2002` (Alloy), `verdi2015`, `txncommit2006` (TLA+ specs) |
| Measurement methodology | `georges2007`, `hoefler2015`, `raasveldt2018`, `mytkowicz2009`, `theodolite2021`, `dean2013`, `hdrhistogram` |
| Statistics (zero counts, comparisons, effect size) | `clopper`-lineage: `jsta2014`, `brown2001`, `rule3`; `mcnemar1947`, `holland1987`, `friedman`-lineage: `demsar2006`, `arcuri2011`, `arcuri2012`; `cliff1993`, `efron1979` |
| Fault injection / reproducibility | `basiri2016`, `elle2020`, `fdb2021`, `crashmonkey2019`, `collberg2016`, `toxiproxy` |

## 8. Gap evidence (what the searches did not find)

The discovery queries aimed explicitly at *empirical* evaluations of exactly-once boundaries and at
outbox/idempotency overhead — e.g. *“empirical evaluation exactly once semantics overhead stream
processing”*, *“end-to-end exactly once processing benchmark Kafka database”*, *“outbox pattern
evaluation performance overhead database”*, *“deduplication overhead event processing benchmark”*,
*“idempotency key API exactly once requests retries”*. Crossref returned **no peer-reviewed study
that (a) injects faults across the Kafka↔external-DB boundary, (b) compares EOS-boundary mechanisms
(M1x/M2/M3/M4) on double-entry ledger invariants, and (c) reports duplicate/lost postings together
with throughput and tail latency.** The relevant hits were framework/architecture descriptions
(`eosmsg2001`, `eosmid2012`, `eoscorba1999`), surveys (`laigner2021`, `micro2026`) and
language-specific claims in secondary venues. This absence is the empirical gap the article fills;
it is stated as a gap in the related-work matrix (`lit/matrix.csv`, `relation=gap`) and is *not*
claimed as proof that no such study exists anywhere (search limits: Crossref/OpenAlex metadata,
English, title/abstract screening — library full-text search of ACM/IEEE digital libraries was not
available in this environment).

## 9. Known limitations of this search

- No institutional access: ACM DL, IEEE Xplore, Elsevier and Taylor & Francis landing pages return
  HTTP 403 to automated clients. For those works the anchored quote is the **publisher abstract**
  recovered from the OpenAlex/Europe PMC record (cached), not the full text; this is stated in the
  `notes` column for each affected entry. OA copies were used wherever they existed (arXiv, VLDB,
  MDPI, repository copies).
- Semantic Scholar produced no abstracts in this run; OpenAlex was the abstract-recovery source.
- A small number of canonical works could not be opened within the rules and were therefore **not
  cited** (Helland 2012; Hanley & Lippman-Hand 1983; Friedman 1937; Clopper & Pearson 1934;
  Fisher 1922; ARIES 1992; Gil Tene's latency talk). Each is recorded in `refs/RETRIEVALS.md` with
  the reason, and the method/claim it would have supported is covered by an accessible
  peer-reviewed substitute where one exists.
- Google Scholar, Scopus and Web of Science were not queried (no subscriptions/API keys in this
  environment); Crossref + OpenAlex provide the DOI-level coverage of the same venues.

## Appendix A — all Crossref discovery query strings


**Batch 1 — topic/core** (lit/_discovery-queries.json)

| # | slug | query.bibliographic |
|---|---|---|
| 1 | `eos-stream` | exactly-once semantics distributed stream processing |
| 2 | `kafka-txn` | transactions Apache Kafka exactly once producer |
| 3 | `millwheel` | MillWheel fault-tolerant stream processing internet scale |
| 4 | `flink-state` | state management Apache Flink consistent snapshots |
| 5 | `chandy-lamport` | distributed snapshots determining global states distributed system |
| 6 | `dstreams` | discretized streams fault-tolerant streaming computation scale |
| 7 | `naiad` | Naiad timely dataflow system |
| 8 | `dataflow-model` | dataflow model balancing correctness latency completeness |
| 9 | `sagas` | sagas long lived transactions |
| 10 | `2pc` | consensus on transaction commit |
| 11 | `helland-idem` | idempotence is not a medical condition |
| 12 | `microservices-data` | data management in microservices state of the practice challenges research directions |
| 13 | `outbox` | transactional outbox pattern microservices reliable event publishing |
| 14 | `cdc` | change data capture database replication log |
| 15 | `event-sourcing` | event sourcing systematic literature review microservices |
| 16 | `double-entry` | double-entry bookkeeping accounting information systems |
| 17 | `triple-entry` | triple entry accounting blockchain |
| 18 | `sr-java-bench` | statistically rigorous Java performance evaluation |
| 19 | `se-stats` | practical guide using statistical tests assess randomized algorithms software engineering |
| 20 | `bench-sci` | scientific benchmarking of parallel computing systems |
| 21 | `fair-bench` | fair benchmarking considered difficult |
| 22 | `wrong-data` | producing wrong data without doing anything obviously wrong |
| 23 | `cliff` | dominance statistics ordinal analyses answer ordinal questions |
| 24 | `bootstrap` | bootstrap methods another look at the jackknife |
| 25 | `friedman` | use of ranks in one-criterion variance analysis |
| 26 | `demsar` | statistical comparisons of classifiers over multiple data sets |
| 27 | `ft-survey` | survey fault tolerance stream processing systems |
| 28 | `eos-survey` | survey exactly once semantics stream processing engines |
| 29 | `streams-tables` | streams and tables two sides of the same coin |
| 30 | `hat` | highly available transactions |
| 31 | `isolation` | critique of ANSI SQL isolation levels |
| 32 | `coord-avoid` | coordination avoidance in database systems |
| 33 | `micro-cons` | data consistency microservices architecture systematic literature review |
| 34 | `saga-survey` | saga pattern distributed transactions microservices survey |
| 35 | `stream-bench` | benchmarking stream processing engines evaluation |
| 36 | `idem-api` | idempotency key API exactly once requests retries |
| 37 | `audit-log` | tamper evident audit log append only |
| 38 | `ledger-db` | financial ledger database immutability transaction integrity |
| 39 | `kafka-vldb` | building a replicated logging system with Apache Kafka |
| 40 | `aries` | ARIES recovery method fine-grained locking |
| 41 | `dedup` | message deduplication exactly once middleware |
| 42 | `dual-write` | dual write problem microservices consistency event |
| 43 | `idempotent-consumer` | idempotent consumer pattern message processing |
| 44 | `kafka-bench` | Apache Kafka performance evaluation benchmarking |
| 45 | `flink-kafka-eos` | exactly once end-to-end Kafka Flink sink evaluation |
| 46 | `event-driven-cons` | event-driven microservices data consistency patterns |
| 47 | `durable-exec` | durable execution workflow engine exactly once |
| 48 | `ledger-formal` | formal verification accounting ledger invariants |
| 49 | `streaming-formal` | formal semantics stream processing exactly once |

**Batch 2 — gap probes (Elle, isolated failures, statistics, offsets)** (lit/_discovery-queries2.json)

| # | slug | query.bibliographic |
|---|---|---|
| 1 | `elle` | Elle inferring isolation anomalies experimental observations |
| 2 | `coord-omission` | coordinated omission latency measurement open loop load |
| 3 | `fisher` | on the interpretation of chi square from contingency tables test of goodness of fit |
| 4 | `holm` | simple sequentially rejective multiple test procedure |
| 5 | `eventsourcing2` | event sourcing pattern empirical evaluation event store |
| 6 | `offset-db` | consumer offset commit database transaction at-least-once idempotent processing |
| 7 | `txn-messaging` | transactional messaging exactly once delivery broker database |
| 8 | `nemenyi` | Nemenyi post hoc test multiple comparisons |
| 9 | `tail-latency` | tail at scale latency variability |
| 10 | `spanner` | Spanner Google globally distributed database |
| 11 | `rep-repro` | repeatability computer systems research |
| 12 | `jepsen` | inferring isolation anomalies observations distributed databases testing |
| 13 | `audit-continuous` | continuous auditing database event log financial reporting |
| 14 | `idem-kafka` | idempotent producer message deduplication broker sequence number |
| 15 | `eos-eval` | empirical evaluation exactly once semantics overhead stream processing |
| 16 | `dedup-overhead` | deduplication overhead event processing benchmark |
| 17 | `outbox-eval` | outbox pattern evaluation performance overhead database |
| 18 | `ledger-formal2` | formal specification accounting transaction double entry invariant |
| 19 | `crash-consistency` | crash consistency file system database storage |
| 20 | `tla-distributed` | specifying and verifying distributed systems temporal logic of actions |

**Batch 3 — outbox/inbox/event-sourcing/fault-injection probes** (lit/_discovery-queries3.json)

| # | slug | query.bibliographic |
|---|---|---|
| 1 | `event-sourcing-slr` | event sourcing architecture systematic mapping study |
| 2 | `outbox2` | transactional outbox reliable messaging database transaction event publication |
| 3 | `inbox-pattern` | inbox pattern message deduplication consumer database |
| 4 | `idempotency-study` | idempotent request retry deduplication key distributed systems evaluation |
| 5 | `eos-benchmark` | end-to-end exactly once processing benchmark Kafka database |
| 6 | `continuous-audit` | continuous auditing systematic literature review |
| 7 | `immutable-ledger` | append-only ledger immutable audit trail database design |
| 8 | `kafka-prod` | Apache Kafka production experience reliability messaging linkedin |
| 9 | `spanner2` | Spanner Google globally distributed database transactions externally consistent |
| 10 | `fault-injection-survey` | fault injection testing distributed systems survey chaos |
| 11 | `open-closed` | open versus closed system performance modeling cautionary tale |
| 12 | `percentile` | estimating tail percentiles performance measurement streaming histogram |

**Batch 4 — Clopper-Pearson / rule-of-three probes** (lit/_discovery-queries4.json)

| # | slug | query.bibliographic |
|---|---|---|
| 1 | `clopper` | Clopper-Pearson confidence interval binomial proportion exact |
| 2 | `ruleofthree` | interpreting zero numerators confidence interval 3/n rule of three |


## Appendix B — LaTeX/BibTeX health check

**LaTeX health check**: a throw-away document under `lit/_bibtest/` cites all 63 keys and
   was built with `pdflatex -> bibtex -> pdflatex x2` using natbib/plainnat + hyperref:
   all three passes exit 0, **bibtex reports no warnings or errors, no undefined citations**,
   and a 151 kB PDF is produced (see `lit/_bibtest/main.pdf`, `lit/_bibtest/bibtex.log`).
   This exercises the diacritics in author names, the escaped `\&` in a venue name and the
   percent-encoded KIP URLs.
