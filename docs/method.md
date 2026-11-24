# Method — Where Exactly-Once Ends

**Preserving Double-Entry Ledger Invariants When Kafka Pipelines Write to External Stores**

| Field | Value |
|---|---|
| Document | `docs/method.md` — method, formal model, fault model, propositions, metrics, design rationale, threats |
| Version | 2.0-draft (supersedes 1.0 skeleton; incorporates AMENDMENT A1) |
| Status | **DRAFT FOR AUTHOR APPROVAL — NOT FROZEN.** Binding only after the author signs the P1 gate (BRIEF §11, A1 §12). |
| Owner | S2 (methodologist). The author owns the formal model and proofs (A1 §0, §3); S2 drafts options and trade-offs. |
| Normative sources | `docs/BRIEF.md` (unchanged parts), `docs/AMENDMENT-A1.md` (binding, supersedes conflicts), this document and `docs/prereg.md` (execution contract). |
| Contains results? | **No.** This is a design document. No measurement, count of observed outcomes, or fabricated value appears here. Run counts are arithmetic functions of the design, not results. |
| Companion | `docs/prereg.md` (exact matrices, per-run record schema, frozen statistics, compute-projection method), `formal/ledger.tla` (optional bounded specification). |
| Precedence | If this document and `docs/prereg.md` disagree: model/claims sections here govern *what is asserted*; prereg governs *what is executed*. Neither may be changed after the analysis freeze except through `results/DEVIATIONS.md` (A1 §11, §9). |

**Revision log.**

| Rev | Date | Change |
|---|---|---|
| 1.0 | 2026-11-24 | Skeleton (mechanisms M1–M3, F1–F6, 21-cell table) — superseded. |
| 2.0 | 2026-11-24 | Rewritten after AMENDMENT A1: mechanisms M0/M1-kafka/M1x/M2/M3/M4 + combinations, H1–H5, I1–I4, crash-point model C1–C12, two-level injection, independent oracle, harness-validation gate, staged design A–D, expanded threats, decisions register. |

**How to read this document.** §1 fixes scope and the data model. §2 defines the ledger and the exact predicates that decide whether a run is consistent. §3 fixes the EOS boundary and the assumptions under which everything else is claimed. §4 gives implementable transition systems for every mechanism. §5 enumerates every crash point and maps the six scripted faults onto them. §6 states propositions, concrete counterexample traces, and the full prediction tables; it also derives the RQ3 decision table from the model (the author's requirement: experiments are evidence, the model answers RQ3). §7 defines metrics, the independent oracle, and the harness-validation gate. §8 and §9 summarise the design and statistics whose frozen detail lives in `docs/prereg.md`. §10 is the threats section. §11 is the practitioner decision table with *empty value cells* and the rules that will fill them. §12 lists open questions and the completeness checklist. §13 lists decisions requiring explicit author approval; §14 is the readiness verdict. Every proposition in §6 is paired with a specific planned measurement (Stage A or Stage B, named in §6.5).

---

## 1. Problem statement, scope, and system model

### 1.1 Problem and research questions

A Kafka-based ledger pipeline performs three durable actions per business event: (i) it reads a record from a Kafka source topic, (ii) it appends postings to a PostgreSQL ledger (the authoritative external store), and (iii) it advances consumption progress. Apache Kafka's transactions can make (i)-related log appends and (iii) atomic *inside Kafka*. They cannot include the PostgreSQL commit, because that commit is a different site with its own WAL and no shared coordinator. Exactly-once therefore ends at the external-store boundary. In a double-entry ledger the resulting failure is not a redundant row that can be de-duplicated later: a duplicated journal entry silently double-counts value, and a lost entry silently erases it; both are audit failures.

The article answers:

- **RQ1** Which failure modes make Kafka EOS fail to preserve double-entry ledger invariants when the sink is an external relational database?
- **RQ2** How do M0, M1-kafka, M1x, M2, M3, M4 and the combinations M2+M3, M4+M3, M1x+M3 compare on invariant violations, duplicates, losses, throughput, p50/p99/p99.9 end-to-end latency, and time-to-consistent-state?
- **RQ3** What is the *minimal sufficient mechanism set* for each failure exposure class?

### 1.2 Hypotheses (verbatim from A1 §2; operationalised here)

- **H1** M1x produces ≥ 1 invariant violation where the DB commit and the offset commit can diverge; M1-kafka produces none. The contribution is the mapping, the measured frequency, and the cost.
- **H2** M2 yields zero violations across the modelled failure classes, with a measurable throughput and latency penalty vs. M3 and M4.
- **H3** M3 alone yields zero duplicates but yields **lost postings** when the offset is committed before the DB write is durable (commit-ordering failure). De-duplication cannot restore a posting that was never written.
- **H4** M4 (or M4+M3) yields zero violations across F1–F6, with lower latency than M2.
- **H5** No single mechanism dominates on both correctness and performance across all failure classes.

Pre-registered decision rules (what would count as confirmed / refuted / inconclusive) are in `docs/prereg.md` §3 and summarised in §9.5. Throughout, "zero violations" is an empirical statement with an exact confidence bound, never a proof (A1 §9).

### 1.3 System architecture (as executed on this host)

The deployment is the native-process form of a typical containerised stack. There is no Docker and no WSL2 on the host (BRIEF §9); therefore every "service" is a directly started native process with recorded command line, environment, and configuration (§8.7, `docs/prereg.md` §11).

| Component | Instances | Role | Link |
|---|---|---|---|
| Kafka KRaft brokers | 3 processes (node ids 1–3) | source log, sink stream, `__consumer_offsets`, `__transaction_state` | `localhost:909x`; RF=3, `min.insync.replicas=2`, `unclean.leader.election.enable=false` |
| PostgreSQL 16 | 1 cluster (`pg_ctl`), one database per campaign | authoritative ledger for DB-sink mechanisms; progress/inbox/outbox tables | `localhost:543x` via Toxiproxy for F5 |
| Toxiproxy | 1 process | cuts/delays real TCP links (app↔Kafka, app↔PostgreSQL) | listeners in front of each upstream link |
| Load generator | 1 process (Java) | deterministic open-loop event production; writes ground truth *before* sending | Kafka producer |
| Consumer service | 1..C processes (Java 21, Spring Boot 3 / spring-kafka), C = 2 at the canonical point | reads source, executes the mechanism, writes the sink | Kafka consumer + JDBC + transactional producer |
| Oracle/checker | 1 process (Python), separate code base | reads ground truth + source dump + sink dump + progress store; computes I1–I4 | read-only DB connection, Kafka consumer |
| Runner/injector | 1 process (PowerShell + Python) | per-run orchestration, fault injection, fault log, run record | OS process control, Toxiproxy API, Kafka Admin API |

**Data flow (canonical).** Load generator → source topic `wxeo.src.<run_id>` (one Kafka record = one complete journal entry) → consumer group `wxeo.cg.<run_id>` → (a) ledger **sink** and (b) a **publication stream** `wxeo.sink.<run_id>`. The sink is PostgreSQL for every DB-sink mechanism (M0, M1x, M2, M3, M4 and the combinations) and the Kafka stream itself for M1-kafka. The publication stream receives one derived record per processed entry from every mechanism, by design, so that the mechanisms are not differentially burdened by an auxiliary workload; for M1-kafka this stream *is* the ledger, while for the others it is an auxiliary audit stream whose integrity is a **secondary** metric (§2.7, §6.6). This asymmetry is a construct-validity threat and a decision for the author (D13).

**Amount of concurrency.** C = 2 consumer instances in one consumer group at the canonical point, so that rebalance-related faults (F2, F6) are *real* rebalances between two live members rather than a single-member rejoin. Consumer instance count is a configuration, recorded per run; a C = 1 robustness check is part of the pilot.

### 1.4 Data model and schemas

**One record = one journal entry.** Each source record carries both legs of exactly one balanced entry. Consequence: no cross-record or cross-partition entry assembly is needed, and per-record processing is the atomic unit. This is a deliberate simplification (the real-world multi-leg/distributed-entry case is out of scope; construct threat T-C2).

#### 1.4.1 Event (source record value, JSON, version `wxeo.ledger-event.v1`)

```json
{
  "spec": "wxeo.ledger-event.v1",
  "event_id": "hex32",
  "seq": 0,
  "currency": "BRL",
  "legs": [
    {"account_id": "hex12", "side": "D", "amount_minor": 12345},
    {"account_id": "hex12", "side": "C", "amount_minor": 12345}
  ],
  "scheduled_at_ms": 1764000000000
}
```

Rules: `event_id` is unique; `seq` is the global production index (`0..N-1`); exactly two legs, one `D` and one `C`, same `currency`, equal `amount_minor`, different `account_id`; `amount_minor` is a positive integer (minor units, no floating point anywhere in the pipeline); no negative amounts; no reversals or claim adjustments (scope, T-C3). `scheduled_at_ms` is the producer's open-loop schedule time on the single host clock (§7.2).

**Deterministic derivation.** Events are a pure function of `(master_seed, seq)`, computed by both the Java generator and the Python oracle from the same written specification:

```
h1 = SHA-256("wxeo1|event|" + master_seed + "|" + decimal(seq))
event_id   = lowercase_hex(h1[0:16])            // 32 hex chars
account_d  = "acct:" + hex(h1[16:22])          // 12 hex chars
account_c  = "acct:" + hex(h1[22:28])
amount     = 1 + (uint64_be(h1[28:36]) mod 100000)
h2         = SHA-256("wxeo1|part|" + master_seed + "|" + decimal(seq))
partition  = uint64_be(h2[0:8]) mod P          // recorded in the produced manifest
```

Hash-derived determinism removes PRNG-implementation drift between the Java producer and the Python oracle, makes ground truth regenerable instead of stored (§7.4, D10), and lets the checker be written in a different language without shared code.

#### 1.4.2 Ledger sink schema (PostgreSQL)

DDL is **identical for all DB-sink mechanisms**; the *only* schema difference allowed between mechanisms is the presence of the de-duplication index (present in M3, M2+M3, M4+M3, M1x+M3) and the mechanism's auxiliary tables (§4). Identical DDL is part of the fairness contract (A1 §1: identical Kafka/PG settings; §4.11 audit list).

```sql
CREATE TABLE posting (
  posting_id    bigserial   PRIMARY KEY,
  event_id      text        NOT NULL,
  leg_no        smallint    NOT NULL CHECK (leg_no BETWEEN 1 AND 2),
  account_id    text        NOT NULL,
  side          char(1)     NOT NULL CHECK (side IN ('D','C')),
  amount_minor  bigint      NOT NULL CHECK (amount_minor > 0),
  currency      char(3)     NOT NULL,
  produced_at   timestamptz NOT NULL,           -- from event.scheduled_at_ms
  inserted_at   timestamptz NOT NULL DEFAULT clock_timestamp()  -- diagnostic only
);
-- Present only in M3, M2+M3, M4+M3, M1x+M3:
CREATE UNIQUE INDEX posting_dedup_idx ON posting (event_id, leg_no);

-- Append-only enforcement (all mechanisms): privilege + trigger, verified per run.
REVOKE UPDATE, DELETE ON posting FROM PUBLIC;
-- application role: GRANT INSERT, SELECT ON posting;  (no UPDATE/DELETE)
CREATE FUNCTION posting_deny_mutation() RETURNS trigger LANGUAGE plpgsql AS
$$ BEGIN RAISE EXCEPTION 'posting is append-only'; END $$;
CREATE TRIGGER posting_append_only BEFORE UPDATE OR DELETE ON posting
  FOR EACH ROW EXECUTE FUNCTION posting_deny_mutation();
```

`inserted_at` is statement time, not commit time; latency is measured from the application's observed commit acknowledgement or a WAL commit timestamp if enabled (§7.2). It is never used to decide a violation.

Auxiliary tables (only where the mechanism needs them; §4):

```sql
-- M4, M4+M3: progress in the same transaction as the postings
CREATE TABLE consumer_progress (
  partition    int     PRIMARY KEY,
  next_offset  bigint  NOT NULL,
  owner_epoch  bigint  NOT NULL,
  updated_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- M2: inbox de-duplication claim and outbox publication log
CREATE TABLE inbox  (event_id text PRIMARY KEY, processed_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE outbox (outbox_id bigserial PRIMARY KEY, event_id text NOT NULL UNIQUE,
                     payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                     published_at timestamptz NULL);
```

#### 1.4.3 Publication stream schema (Kafka topic `wxeo.sink.<run_id>`)

A derived record per journal entry: key = `event_id`, value = `{"spec":"wxeo.entry.v1","event_id":...,"seq":...,"legs":[...]}`, produced to the same partition index as the source record. Read back with `isolation.level=read_committed` when written in a transaction (M1-kafka, M1x, M1x+M3) and `read_uncommitted` otherwise; the reader records which isolation level applies. Same entry semantics as the DB sink; the invariant predicates of §2 apply to this stream when it is the ledger (M1-kafka).

#### 1.4.4 Instrument files (per run, outside the transactional path)

| File | Content | Written when |
|---|---|---|
| `gt_<run_id>.ndjson` | expected postings: one line per leg `{line_no,event_id,leg_no,account_id,side,amount_minor,currency}` | **before** the first produce (A1 §4), fsynced |
| `gt_<run_id>.sha256` | SHA-256 of the data lines | immediately after the GT file, fsynced |
| `produced_<run_id>.ndjson` | `{event_id,partition,offset,ack_ts_ms}` per acknowledged produce | after acks |
| `source_dump_<run_id>.ndjson` | independent dump of the source topic: `{partition,offset,event_id,value_sha256}` | during quiescence |
| `sink_dump_<run_id>.csv` | `COPY (SELECT event_id,leg_no,account_id,side,amount_minor,currency FROM posting ORDER BY event_id,leg_no,posting_id)` | after quiescence |
| `faults.log` | append-only NDJSON, one line per injected action (§5.6) | at injection time |
| `run_<run_id>.json` | complete run record (`docs/prereg.md` §5 schema) | at run end |
| `stdout/stderr` logs | app, brokers, PG, Toxiproxy | continuously |

### 1.5 Scope, unit of analysis, and non-goals

**Unit of analysis.** One *run* = one (mechanism configuration × fault condition × load × partitions × ordering variant × repetition) execution with a fresh logical state (§8.3). A run produces one outcome vector (§2.6) and one performance sample set.

**In scope.** Single-record balanced entries; one currency (BRL) per entry; append-only postings; the nine mechanism configurations; seven scripted fault classes plus control; two injection levels; crash-recovery faults (no Byzantine failures); one host; Kafka-internal exactly-once as a positive control.

**Out of scope (stated, not silently dropped).** Multi-leg/multi-currency entries; reversals, refunds, chargebacks; account state machines (overdrafts, holds); reconciliation jobs; 2PC/XA sinks; Kafka Connect; multiple data centres; disk corruption; operator error outside the scripted injectors; PostgreSQL HA/failover; TLS/SASL overhead; schema registries; real production load distributions. Each becomes a threat with a stated residual risk (§10).

### 1.6 Notation

| Symbol | Meaning |
|---|---|
| `E`, `e`, `eid` | set of produced events; an event; its `event_id` |
| `Ω` | expected posting multiset derived from the events that are durably in the source topic (the ground-truth oracle's reference set) |
| `π = (eid, leg_no)` | posting identity; `V*(π)` expected posting value `(account_id, side, amount_minor, currency)` |
| `D` | observed posting multiset in the ledger sink (DB rows, or committed records of the publication stream for M1-kafka) |
| `mult(D, x)` | multiplicity of key `x` in multiset `D` |
| `I1..I4` | invariants of A1 §3 (§2.3) |
| `v = (v1,v2,v3,v4)` | per-run violation flag vector; `v_i = 1` iff an I`i`-class violation was observed |
| `W1`, `W2` | the duplicate window and the loss window of the EOS gap (§3.3) |
| `C1..C12` | crash points (§5.1); `K1..K7` their proven equivalence classes (§5.2) |
| `F0..F6` | fault conditions: control plus the six scripted faults; F4 is split into F4a/F4b (§5.4) |
| `M0, M1-kafka, M1x, M2, M3, M4`, `M2+M3`… | mechanism configurations (§4) |
| `P` | source-topic partition count ∈ {3,6,12} |
| `C` | consumer instances in the group (2 at the canonical point) |
| `N` | independent repetitions per cell (30 nominal; 10 sensitivity; never below 20 for correctness cells after cuts, A1 §7) |

---

## 2. Formal ledger model

This section fixes the observation language: what a ledger is, what "consistent" means, and the exact predicates a script evaluates. It is deliberately stated so that the author can adopt it as the paper's formal section (A1 §3).

### 2.1 Events, entries, postings

An **event** is `e = (eid, curr, legs)` with `legs = ((a_D, D, amt), (a_C, C, amt))`, `amt ∈ ℤ⁺`, `a_D ≠ a_C`. A **journal entry** is the pair of legs of one event, identified by `eid`. A **posting** is one concrete sink row/record: a tuple `(eid, leg_no, account_id, side, amount_minor, currency)`, where `leg_no ∈ {1,2}` selects the leg. The **intended posting multiset** of `e` is

```
Π(e) = { (eid, 1, a_D, D, amt, curr) , (eid, 2, a_C, C, amt, curr) }.
```

**Signed amounts.** `σ(D, a) = +a`, `σ(C, a) = −a`. **Balancedness** of a set of postings for one entry means: exactly one `D` and one `C` leg, equal amounts, one currency, and signed sum zero. Note the algebraic fact that matters later: *a duplicated balanced entry is still balanced.* Balance is therefore necessary but not sufficient; duplicates are caught by I3, not I1 (§2.8).

### 2.2 Durable state and execution histories

For a run `r`:

- `K_r` — Kafka durable state (source log, publication stream, committed consumer offsets, transaction state);
- `S_r` — sink durable state (PostgreSQL tables; for M1-kafka, the publication stream);
- `A_r` — auxiliary durable state (inbox/outbox/progress tables, depending on mechanism).

An **execution history** `H = (σ_1, …, σ_m)` is a finite sequence of durable operations (each with a site and a total order per site) produced by a *schedule* `σ` = (process crash/restart events, network faults, message deliveries/duplications, injected reorderings) drawn from the fault model of §5. Recovery after a crash follows `fair recovery` (A3, §3.5): a restarted process resumes from the mechanism's own durable progress store and retries until quiescence.

**Observation.** The observation function `O` is applied only in a quiescent state (§7.5, A8) and returns `O(r) = (D_r, A_r, G_r, T_r)` where `D_r` is the sink posting multiset, `A_r` the publication-stream multiset, `G_r` the progress state (Kafka committed offsets and/or stored progress rows), and `T_r` the timing record.

**Admissible schedules.** `Sched(M, F, A)` is the set of schedules admissible for mechanism `M`, fault set `F`, and assumptions `A`. A schedule that the implementation cannot reach is *not* in the set; that is what makes some mechanism × crash-point cells structurally unreachable (§5.3) rather than merely untested.

### 2.3 Invariants I1–I4

For a run with expected multiset `Ω` (derived in §2.4 from the source topic) and observed multiset `D`:

- **I1 (balanced entries; per currency).** For every `eid` that occurs in `D`: the postings of `eid` in `D` consist of exactly one `D`-side and one `C`-side posting with equal `amount_minor` and a single `currency`, and the signed sum is zero.
  *Structural clause included.* A1 §3 states I1 as "every entry sums to zero per currency". The draft splits it into **I1a** (structural well-formedness: exactly two legs, one per side, equal amount, single currency) and **I1b** (signed sum zero), reported separately, with I1 = I1a ∧ I1b. Reason: I1b alone is invariant under a duplicated balanced entry and under a quadruplicated entry; the structural clause is what makes an imbalanced *or* mangled entry visible. Author's decision: D8.
- **I2 (append-only).** No posting that was durably inserted is later modified or deleted. Operationally: (a) enforcement attestation — the application role holds no `UPDATE`/`DELETE` privilege and the `BEFORE UPDATE OR DELETE` trigger exists (verified by the oracle each run); (b) content attestation — for every `π` present in both `D` and `Ω`, `V_D(π) = V*(π)`; a mismatch is an I2 violation (**mutation**). A row that is absent is an I4 violation (loss), not I2; the two are attributed separately so that loss cannot masquerade as "deletion".
- **I3 (at most one entry per business event).** For every `eid` in `D`: `mult(D, eid) ≤ 2` and `mult(D, (eid, leg_no)) ≤ 1`. Violations are **duplicates**. (A1 §3 states I3 as "at most one entry per business event id"; the posting-level clause is its operational form because the sink stores postings.)
- **I4 (every consumed event eventually has exactly one entry; liveness under fair recovery).** For every event `e ∈ E_source` (the events durably present in the source topic): after quiescence, exactly the two postings of `Π(e)` are present in `D`, each exactly once. Violations are **losses**. Note carefully: I4 is a *completeness* property. A mechanism that advances progress past an event without writing it still violates I4 — exactly H3's loss case — because the event is in the source topic and will never be redelivered. The "consumed" qualifier of A1 §3 is what makes the property a liveness property under fair recovery; the oracle measures its end-state instance (D9).

### 2.4 Observation and consistency

**Reference set.** `E_source` = events durably appended to the source topic for the run, obtained by an independent dump, cross-checked against the produced manifest and the pre-send ground-truth file (§7.4). `Ω = ⋃_{e ∈ E_source} Π(e)`.

**Consistency predicate.**

```
Consistent(D, Ω)  ⇔  I1(D) ∧ I2(D, Ω) ∧ I3(D) ∧ I4(D, Ω)
```

i.e. no imbalanced entries, no mutated postings, no duplicate extra postings, no lost postings, and no spurious postings. Spurious postings (`π ∉ Ω`) are counted under I3/I4 attribution: a spurious `π` with a key in `Ω` indicates mutation of the key (I2); a spurious `π` with a key not in `Ω` indicates a fabricated row (reported separately as `spurious`; in a correct pipeline it is impossible, so it is treated as an invalid-run signal for the harness, §7.6).

**Verdict.** `violating_run(r) = 1` iff `¬Consistent(O(r))`.

### 2.5 Executable predicates

The normative definitions are the predicates below; the checker implements them with `COPY`-based streaming in Python (memory-bounded) and the SQL forms are also executed directly for cross-checking. Both must agree; disagreement invalidates the run and is a harness defect. `expected` is `Ω` loaded as a temporary table (or streamed); `posting` is the sink table.

```sql
-- P1: observed postings
SELECT event_id, leg_no, account_id, side, amount_minor, currency
FROM posting;

-- P2: duplicate postings (I3, posting level): every copy beyond the first of (event_id, leg_no)
SELECT event_id, leg_no, count(*) - 1 AS extra
FROM posting GROUP BY event_id, leg_no HAVING count(*) > 1;

-- P3: duplicate entries (I3, entry level): events with more than two postings
SELECT event_id, count(*) AS n FROM posting GROUP BY event_id HAVING count(*) > 2;

-- P4: imbalanced / malformed entries (I1a + I1b)
SELECT event_id,
       count(*)                                                        AS legs,
       sum(CASE WHEN side = 'D' THEN amount_minor ELSE -amount_minor END) AS net,
       count(DISTINCT currency)                                         AS currencies,
       sum(CASE WHEN side = 'D' THEN 1 ELSE 0 END)                      AS debits,
       sum(CASE WHEN side = 'C' THEN 1 ELSE 0 END)                      AS credits
FROM posting
GROUP BY event_id
HAVING count(*) <> 2
    OR sum(CASE WHEN side = 'D' THEN amount_minor ELSE -amount_minor END) <> 0
    OR count(DISTINCT currency) <> 1
    OR sum(CASE WHEN side = 'D' THEN 1 ELSE 0 END) <> 1
    OR sum(CASE WHEN side = 'C' THEN 1 ELSE 0 END) <> 1;

-- P5: mutated postings (I2): key present, value differs from expected
SELECT p.event_id, p.leg_no, p.account_id, p.side, p.amount_minor, p.currency
FROM posting p JOIN expected x USING (event_id, leg_no)
WHERE (p.account_id, p.side, p.amount_minor, p.currency)
      IS DISTINCT FROM (x.account_id, x.side, x.amount_minor, x.currency);

-- P6: lost postings (I4): expected key absent from the sink
SELECT x.event_id, x.leg_no
FROM expected x LEFT JOIN posting p USING (event_id, leg_no)
WHERE p.event_id IS NULL;

-- P7: spurious postings (not in expected): fabricated keys
SELECT p.event_id, p.leg_no
FROM posting p LEFT JOIN expected x USING (event_id, leg_no)
WHERE x.event_id IS NULL;

-- P8: append-only attestation (must return false,false and one trigger row)
SELECT has_table_privilege(current_user, 'posting', 'UPDATE') AS can_update,
       has_table_privilege(current_user, 'posting', 'DELETE') AS can_delete;
SELECT count(*) FROM pg_trigger WHERE tgrelid = 'posting'::regclass AND NOT tgisinternal;
```

For M1-kafka the same predicates apply to the publication-stream multiset (the stream is the ledger); the oracle adapts by materialising the dump into a relation-shaped stream.

**Counts and flags.**

```
dup_postings   = Σ_π (mult(D,π) − 1)⁺
dup_entries    = Σ_eid (⌊mult(D,eid)/2⌋ − 1)⁺
imbalanced     = |{eid : I1a or I1b violated}|
mutated        = |{π ∈ keys(D) ∩ Ω : V_D(π) ≠ V*(π)}|
lost_postings  = |{π ∈ Ω : π ∉ keys(D)}|
spurious       = |{π ∈ keys(D) \ Ω}|
v1 = [imbalanced > 0]; v2 = [mutated > 0]; v3 = [dup_postings > 0]; v4 = [lost_postings > 0]
```

`v = (v1,v2,v3,v4)` is the per-run correctness outcome vector; `violating_run = ∨ v_i`. Performance outcomes (§7.2) are analysed separately from correctness.

### 2.6 Violation taxonomy used throughout §6

| Code | Meaning | First observable signature |
|---|---|---|
| `V1` | I1 violated: imbalanced/malformed entry | `P4` non-empty (e.g. a single leg); often secondary to I3 |
| `V2` | I2 violated: posting modified | `P5` non-empty, or append-only attestation fails |
| `V3` | I3 violated: duplicate posting/entry | `P2`/`P3` non-empty |
| `V4` | I4 violated: lost posting | `P6` non-empty |
| `P` | none of the above | all predicates empty |
| `C` | conditional: outcome depends on a stated sub-case (e.g. whether the effect commit landed) | sub-case recorded per run in the run record |
| `N/A` | structurally unreachable for that mechanism (proof reference in §5.3/§6) | not executed; counted in the matrix as not-applicable, never as "passed" |

### 2.7 Ledger vs. publication stream

For DB-sink mechanisms the ledger is PostgreSQL and the publication stream is auxiliary; violations there are reported as `pub_dup`, `pub_lost` (secondary outcomes, never mixed into I1–I4). For M1-kafka the publication stream *is* the ledger; its correctness is the positive control (A1 §5). Mixing the two would compare different storage media under one invariant label; the distinction is explicit and appears in every results table.

### 2.8 Why the invariant set is exactly I1–I4 (construct note)

The four invariants are orthogonal in their failure signatures — this is what makes the experiment interpretable:

| Failure | I1 | I2 | I3 | I4 |
|---|---|---|---|---|
| Effect applied twice (whole entry) | passes | passes | **fails** | passes |
| Effect applied twice (one leg) | **fails** | passes | **fails** | passes |
| Effect lost | passes | passes | passes | **fails** |
| Field corrupted after insert | maybe | **fails** | passes | maybe |
| Row physically deleted | passes | passes | passes | **fails** |

Two consequences are load-bearing for the paper. (i) **Balance checks do not detect duplication**: a duplicated balanced entry nets to zero; only I3 does. A practitioner who validates only "sum to zero" will not see M1x's duplicates. (ii) **De-duplication does not detect loss**: a duplicated *or* lost posting is invisible to a dedup index; only I4 does. This is the structural basis of H3 and of the claim in the title. If the sink assigned *surrogate* journal-entry ids instead of propagating the business `event_id`, I3 would be un-checkable without external evidence — a practitioner warning the paper will state explicitly (T-C4).

---

## 3. The EOS boundary

### 3.1 What Kafka's transactional guarantee covers

Write `H_K` for the sub-history of durable operations at the Kafka site, and `H_S` for the sink site. A Kafka transaction `T` is a set of operations at the Kafka site:

```
T = { AppendRecords(L) , CommitOffsets(G_p) } ,   G_p = consumer group offsets for partitions p ∈ B
```

with `TxnBegin(producer)` and `TxnCommit(producer, T)` as its boundary. Kafka's guarantee is **single-site atomic visibility**: `TxnCommit` makes all operations in `T` durable together, `TxnAbort` discards all of them, consumers at `isolation.level=read_committed` see either all or none, and the transaction coordinator fences stale producer epochs so that a suspended/zombie producer cannot commit after being fenced (AS5).

**Definition (EOS-covered set).** The set of transitions that Kafka EOS makes atomic is

```
EOS(T) = vis( AppendRecords(L) ∪ CommitOffsets(G) )    — all on the Kafka site.
```

Everything else in the pipeline is *outside* `EOS(·)`. In particular the PostgreSQL `COMMIT` of the ledger postings is outside it: different site, different durability mechanism, no shared coordinator, no prepared-transaction state, no presumed-abort recovery linking the two.

### 3.2 The external-store boundary

For a DB-sink mechanism the protocol contains (at least) two **distinct single-site atomic commit events** per batch:

- `Commit_S(b)` — the ledger effect becomes durable in PostgreSQL;
- `Commit_K(b)` — the mechanism's progress becomes durable (a Kafka offset commit, or a row in the DB progress table for M4).

A mechanism *closes* the boundary only if the pair `(Commit_S, Commit_K)` is atomic as a unit at one site (impossible across Kafka and PostgreSQL without a distributed commit protocol) **or** the mechanism makes replay harmless (`dedup`) *and* never advances progress ahead of durable effect (`ordering`). This classification is the formal core of the paper and is stated as proposition P1 (§6.1).

### 3.3 The EOS gap: the two failure windows

For an event `e` processed in a batch `b`, define two windows:

- **W1 (duplicate window).** A schedule in which `Commit_S(b)` is durable and `Commit_K(b)` is not, and `e` is redelivered: the effect is applied a second time unless the sink effect is idempotent under `e`'s business identity. Formally:

  ```
  W1(b)  =  { σ ∈ Sched : durable(Commit_S(b), σ) ∧ ¬durable(Commit_K(b), σ) }
  ```

- **W2 (loss window).** A schedule in which `Commit_K(b)` is durable and `Commit_S(b)` is not: `e` is never redelivered because progress passed it, so the posting is never written — and no amount of de-duplication can restore it. Formally:

  ```
  W2(b)  =  { σ ∈ Sched : durable(Commit_K(b), σ) ∧ ¬durable(Commit_S(b), σ) }
  ```

**Lemma 1 (single-site commit pair).** Let a mechanism execute `Commit_S` and `Commit_K` as two sequential durable operations with program order `o` and no cross-site recovery protocol. Then:

1. if `o = (Commit_S ≺ Commit_K)`, then `W1(b) ≠ ∅` and `W2(b) = ∅` **unless** the sink's durability is weaker than its acknowledgement (excluded here by AS3) or the ordering is an injected fault (C5, §5);
2. if `o = (Commit_K ≺ Commit_S)`, then `W2(b) ≠ ∅` and `W1(b) = ∅` for the same reason;
3. if the two commits are concurrent or the ordering is not enforced (`commitAsync`, pipelined writer pools), both windows may be non-empty.

*Proof sketch.* Crash-recovery semantics with prefix-consistent storage (AS1): a crash between two sequential durable operations leaves the first durable and the second not. Realise the crash point at the process boundary (a process kill is one admissible schedule). Clause 3 follows by interleaving. □

**Corollary (the EOS gap).** A pipeline that uses Kafka transactions for the Kafka-internal part and a plain `COMMIT` for the external ledger has `W1 ≠ ∅` and, under weak or unattractive ordering, possibly `W2 ≠ ∅`. Kafka's atomicity does not reach across §3.2's boundary. This is what "where exactly-once ends" means operationally, and it is what H1 measures.

**Lemma 2 (what closes the windows).**

| Closure mechanism | Closes W1? | Closes W2? | Mechanisms |
|---|---|---|---|
| Single-site atomic `(Commit_S, Commit_K)` | yes | yes | M4, M2 (progress in DB txn), M1-kafka (all Kafka) |
| Idempotent sink effect keyed by business identity (dedup) | yes | no | M3, M2 (inbox) |
| Strict ordering only | no | yes | M0, M1x, M3(strict), M2 |
| 2PC/XA across stores | yes | yes | (out of scope; discussed §3.6) |

*Proof sketch.* W1 is closed exactly when a second application of `e` is a no-op (dedup) or when `Commit_K` cannot be behind a durable `Commit_S` (same-site atomicity). W2 is closed exactly when progress cannot advance past a non-durable effect (ordering) or when progress and effect are atomic. Dedup is irrelevant to W2 by definition: the missing effect is never replayed, so there is nothing to de-duplicate. □

Lemma 2 yields the *prediction topology* of §6: the correctness difference between mechanisms is determined by which of {atomicity, dedup, ordering} they combine, not by Kafka configuration details. It is also the formal basis of H3: "dedup cannot restore a posting never written."

### 3.4 Visibility and the audit/publish channel

`EOS(T)` additionally gives exactly-once visibility to the *publication* of derived entries. M1-kafka uses this as its primary guarantee (positive control). M1x writes the same derived entries inside `T` while writing the ledger outside it; therefore under W1 **the stream records are aborted while the ledger rows persist**, and under a successful retry the ledger gets a second row while the stream gets one record. The predictable consequence — *the audit stream and the ledger diverge exactly when the gap opens* — is a secondary, observable signature of H1 (metric `divergence` in §7.1). It is also a practitioner-relevant warning: internal exactly-once is not evidence that the external store is consistent.

### 3.5 Assumptions

Every proposition in §6 is conditioned on AS1–AS13. Each is testable or explicitly excluded.

| ID | Assumption | How it is enforced or checked |
|---|---|---|
| **AS1** | Crash-recovery faults only: processes crash and restart; storage writes are prefix-consistent; no Byzantine corruption. | Fault injectors are process kills/restarts and TCP manipulations (§5.5); no random data corruption is injected. |
| **AS2** | Network faults are limited to loss, delay, partition and reset of TCP connections; Kafka/PG protocol checksums and retries are intact. | Toxiproxy toxics only (§5.5); no packet payload mutation. |
| **AS3** | PostgreSQL durability: `synchronous_commit=on`, `fsync=on`, `full_page_writes=on`, single instance, no replica. A `COMMIT` acknowledgement implies durability. | Config recorded per run and asserted by the oracle; no failover is injected (scope). |
| **AS4** | Kafka durability: RF=3, `min.insync.replicas=2`, producers `acks=all`, `unclean.leader.election.enable=false`; a broker kill does not lose committed records. | Broker configs pinned and recorded; single-host replication is a weakened-fidelity threat (T-E1). |
| **AS5** | Transactional producer idempotence and fencing behave as specified; `transaction.timeout.ms` is bounded and recorded; stale epochs cannot commit. | Producer/broker config table (§Appendix C); C7 exercises fencing. |
| **AS6** | Consumer group protocol: at most one owner per partition at a time, **except** inside the deliberately injected zombie window C7; `max.poll.interval.ms` and `session.timeout.ms` are pinned. | Config recorded; C7 exists precisely to test the exception. |
| **AS7** | Fair recovery: crashed processes restart within a bounded time and retry until quiescence; retries are not dropped. | Runner restart policy; quiescence criteria (§7.5); invalid-run detection (§7.6). |
| **AS8** | Clocks are used only for latency measurement, never for correctness; single host, single boot per campaign, no clock steps during a run. | Clock setup and offset checks logged before/after each run (§7.2); latency is a secondary metric. |
| **AS9** | Deterministic load: `(master_seed, seq)` fully determines an event; the generator is exactly-once into the source topic (idempotent producer, `acks=all`); the source-integrity gate validates it. | §7.4 gate; run invalidated on mismatch. |
| **AS10** | Fault injection acts at declared phase markers or declared wall-clock offsets; every firing is attested in `faults.log`. | §5.6 attestation; non-attested runs excluded and counted. |
| **AS11** | Equal tuning effort and identical Kafka/PG/JVM settings across mechanisms except the mechanism-intrinsic components listed in §4.11. | Config hash audit (§7.6, G6); author sign-off D14. |
| **AS12** | Single currency per entry; no reversals, adjustments, or multi-leg entries; `amount_minor` positive integers. | Generator schema enforcement; scope statement §1.5. |
| **AS13** | The oracle is independent: no shared code with the pipeline, read-only access, ground truth created before sending and cross-checked by an independent regeneration. | §7.4 architecture; G5 gate. |

### 3.6 Options for closing the gap (draft for author; no code implication at P1)

The paper's claim is diagnostic (EOS does not cover the external store) and prescriptive (which mechanism closes which window). The *formal* options for closing both windows are:

- **O-1: Distributed commit (2PC/XA)** — a coordinator linking `Commit_S` and `Commit_K`, with presumed-abort recovery. Closes W1 and W2; costs a coordinator, blocking on failure, and is unavailable between Kafka and PostgreSQL without an external transaction manager. Out of scope experimentally; discussed in related work.
- **O-2: Ordering + idempotence (dedup)** — commit effect before progress, make the effect idempotent under the business key, retry/redeliver freely. Closes W1 by dedup and W2 by ordering (Lemma 2). This is M3, and the paper's cheapest prescription.
- **O-3: Progress in the sink** — store progress in the same transaction as the effect; recovery seeks from the stored progress. Closes both by single-site atomicity. This is M4 (and the atomic half of M2).
- **O-4: Log-based relay (outbox)** — make the publish side recoverable from a DB-resident log; the ledger txn remains the atomic unit. This is M2; it buys publication recoverability at extra write cost.

The author may choose to include O-1 only as related work (recommended) or as a costed baseline (not recommended: no XA support in the pinned stack without additional dependencies).

---

## 4. Mechanisms as transition systems

### 4.1 Common protocol conventions

All mechanisms are implementations of the same *skeleton* with different commit protocols, and share every tunable that is not mechanism-intrinsic (§4.11):

```
skeleton(M):
  on assignment(p): resume_p ← M.resume_point(p)          # Kafka committed offset, or stored progress row
                    seek(p, resume_p)
  loop:
    B ← poll();  if B = ∅ then continue
    M.process(B)                                           # M-specific durable protocol
  on revoke/close: M.shutdown()
```

`B` is one poll batch: `max.poll.records = 500` for every mechanism (intrinsic batch-size tuning is forbidden; §4.11). `process(B)` for every mechanism consists of the same three *duties* in a mechanism-determined order:

- **D-effect:** append the postings of `B` to the ledger sink;
- **D-progress:** make progress for `B` durable;
- **D-publish:** emit derived entry records to the publication stream.

The mechanism is *defined* by (i) the ordering of these duties, (ii) whether D-effect and D-progress are the same atomic unit, (iii) whether the effect is idempotent, and (iv) how D-publish is made recoverable.

**Pseudocode conventions.** `db.begin/commit/rollback` are PostgreSQL transactions; `txn.*` are Kafka transactions; `kafka.commit_offsets(B)` is `commitSync` unless stated; `insert_plain` is a plain `INSERT`; `insert_idem` is `INSERT ... ON CONFLICT (event_id, leg_no) DO NOTHING`; every mechanism treats retriable SQL errors per §4.10.

### 4.2 M0 — naive at-least-once (negative control)

```
process(B):
  db.begin()
  for r in B: insert_plain(postings_of(parse(r)))        # no dedup, no conflict handling
  db.commit()                                            # D-effect durable
  publish_non_txn(B)                                     # D-publish, at-least-once, non-recoverable
  kafka.commit_offsets(B)                                # D-progress durable
```

Ordering: effect ≺ progress (strict). Dedup: none. Progress store: Kafka offsets. Publication: inline, non-recoverable. *Expected role:* negative control — must show duplicates wherever W1 is reachable; if it does not, the harness is broken (G1, §7.6).

### 4.3 M1-kafka — Kafka EOS with a Kafka sink (positive control)

```
process(B):
  txn.begin()
  for r in B: txn.send(sink_topic, key=event_id, value=entry(r))   # D-effect (in Kafka)
  txn.send_offsets_to_transaction(offsets(B), group)               # D-progress (in Kafka)
  txn.commit_transaction()                                         # atomic: D-effect ∧ D-progress
```

Ordering: single atomic commit (`EOS(T)`). Dedup: not needed. Progress store: Kafka transaction. Publication: the sink itself. *Expected role:* positive control — zero violations under the model, because both duties are at one site (A1 §5).

### 4.4 M1x — Kafka EOS plus a non-atomic external DB write (what practitioners deploy)

```
process(B):
  txn.begin()
  for r in B: txn.send(sink_topic, key=event_id, value=entry(r))   # stream records, inside T
  db.begin(); for r in B: insert_plain(postings_of(parse(r))); db.commit()   # D-effect, OUTSIDE T
  txn.send_offsets_to_transaction(offsets(B), group)
  txn.commit_transaction()                                         # D-progress + stream records, atomic in Kafka
```

Ordering: effect ≺ progress, with the effect outside the transaction. Dedup: none. Progress store: Kafka transaction. *This is the canonical gap.* If the process dies (or the transaction aborts) between the DB commit and `commit_transaction`, the batch is redelivered and re-inserted: duplicate postings (W1). Conversely, the stream records of an aborted `T` are discarded while the DB rows persist: stream/ledger divergence. There is no W2 under this ordering (Lemma 1 clause 1); the inverse ordering exists only as the C5 probe (§4.7, §5.4).

### 4.5 M2 — transactional outbox / inbox

M2 makes the *ledger transaction* the atomic unit and derives publication from a DB-resident log:

```
process(B):
  db.begin()
  for r in B:
    claimed ← INSERT INTO inbox(event_id) VALUES(:eid) ON CONFLICT (event_id) DO NOTHING RETURNING 1
    if claimed:
      insert_plain(postings_of(parse(r)))            # plain inserts: no posting-level index
      INSERT INTO outbox(event_id, payload) VALUES (:eid, :entry)
  db.commit()                                        # D-effect + dedup claim + publish intent, atomic
  publish_via_relay_only                              # D-publish deferred to relay
  kafka.commit_offsets(B)                             # D-progress (after effect)

relay thread (independent):
  loop: rows ← SELECT * FROM outbox WHERE published_at IS NULL ORDER BY outbox_id LIMIT K
        for row in rows:
          send_non_txn(sink_topic, key=event_id, value=payload)
          on ack: db.begin(); UPDATE outbox SET published_at = now()
                  WHERE outbox_id = :id AND published_at IS NULL; db.commit()
```

Ordering: effect ≺ progress (strict). Dedup: `inbox.event_id` primary key (W1 closed). Progress store: Kafka offsets; the inbox makes replay harmless. Publication: recoverable via the outbox + relay (at-least-once; relay crash between send and mark duplicates in the stream, ledger unaffected). Cost: three extra durable writes per event batch (inbox claim, outbox row, relay mark) and a background thread.

**Scope note (D3).** A "pure inbox" would differ from M3 only by table choice; a "pure outbox" without inbox would lose postings on ordinary redelivery (a straw man). Draft options: (a) inbox + outbox as above (recommended: it is the only realization that both closes W1 and provides recoverable publication, and it is the pattern practitioners deploy for a *dual* sink); (b) inbox-only (fails distinctness vs M3); (c) outbox-only (predictably violates I3 on redelivery, weakens H2's fairness). The author decides; the model's predictions in §6 assume (a).

### 4.6 M3 — idempotency key with a unique index

```
process(B):
  db.begin()
  for r in B, for leg in legs(r):
     insert_idem(event_id, leg_no, account, side, amount, currency)   # ON CONFLICT (event_id, leg_no) DO NOTHING
  db.commit()                                                        # D-effect durable
  publish_non_txn(B)                                                 # D-publish, at-least-once
  kafka.commit_offsets(B)                                            # D-progress durable
```

Ordering: effect ≺ progress (strict). Dedup: unique index on `(event_id, leg_no)` (W1 closed: replay hits conflicts and becomes a no-op). Progress store: Kafka offsets. Publication: inline, non-recoverable (redelivery may duplicate the stream; ledger unaffected).

**Ordering variants (D2 — required to make H3 falsifiable).** H3's loss scenario is *"the offset was committed before the DB write was durable"*. In M3 as written, program order makes that state unreachable (Lemma 1 clause 1). Three ways to test H3:

| Option | Realisation | Fidelity | Fairness | Cost |
|---|---|---|---|---|
| **O-a (recommended)** | an `ordering` flag on **every** mechanism with both commits: `strict` (effect≺progress) or `early` (progress≺effect). Level-1 probe for crash point C5; same code, one boolean; applied identically to M0, M1x, M2, M3, M4, and the combinations | high: this is a real deployment choice ("optimistic offset commit"), not a code mutation | high: the flag is available to all mechanisms, so it measures the *mechanism*, not an unfair variant of one | +1 factor level for C5 cells (§8.1) |
| **O-b (M3-eo only)** | a separate M3 variant that commits offsets first | medium | low: only M3 gets the "optimisation"; invites the unfair-tuning attack (S5/A1 §11) | extra variant to implement/test |
| **O-c (external progress advance)** | injector advances the group offset (or the M4 progress row) while the app is mid-batch, then kills it | low: models operator error/corruption rather than a code path | medium: applies uniformly but is an adversarial state injection, not a crash | no artifact change |

O-a is recommended and the §6 tables are written so that the `early` ordering is the witness for C5/W2; O-c may be kept as an exploratory robustness probe (clearly labelled). If the author prefers not to add the flag, H3's loss arm is *structurally* argued (Lemma 2) but **not experimentally tested** — the paper must then report H3's loss component as inconclusive, and say so.

### 4.7 M4 — offsets in the same DB transaction as the postings

```
on assignment(p):
  stored ← SELECT next_offset FROM consumer_progress WHERE partition = p
  seek(p, coalesce(stored, committed_offset(p)))

process(B):
  db.begin()
    for r in B: insert_plain(postings_of(parse(r)))                 # no dedup
    for p in partitions(B):
      UPDATE consumer_progress SET next_offset = end_offset(B,p), updated_at = now()
      WHERE partition = p AND next_offset < end_offset(B,p)          # monotone guard
  db.commit()                                                        # D-effect ∧ D-progress atomic
  publish_non_txn(B)                                                 # D-publish, at-least-once
  (kafka.commit_offsets(B) is optional; it is never used for resume)
```

Ordering: single atomic commit (effect and progress at the same site). Dedup: none. Progress store: `consumer_progress` in PostgreSQL; Kafka offsets may be committed for observability only, never consulted on assignment. Recovery: seek to the stored offset; on an unknown commit outcome, re-read `consumer_progress` — if it advanced to the batch end, the transaction committed, so skip; otherwise retry (§4.10). Cost: one progress update per partition per batch; no extra per-event write; no index. Exposure: W1 and W2 closed by atomicity; but with no dedup and no fencing, a **zombie writer** (C7) that resumes after being kicked can insert postings a second time. Hence the paper's assumption AS6 and the M4+M3 combination.

### 4.8 Combinations for RQ3

| Configuration | Construction | What the added component buys | Predicted extra cost |
|---|---|---|---|
| **M2+M3** | M2's DB transaction and relay, but the de-dup claim is the posting unique index instead of an `inbox` table: `{insert_idem postings, INSERT outbox ... ON CONFLICT (event_id) DO NOTHING}` then progress | removes one durable write per event (no inbox row); W1 closed by the index; publication still recoverable | index maintenance + outbox row |
| **M4+M3** | M4's atomic `{postings + progress}` transaction with `insert_idem` instead of `insert_plain` | C7 zombie duplicates closed by the index; W1/W2 closed by atomicity | index maintenance only; no relay, no inbox/outbox |
| **M1x+M3** | M1x plus `insert_idem` for the external write | W1 closed by the index while keeping Kafka-internal exactly-once for the publication stream (and the transaction's atomicity for the audit channel) | Kafka transaction overhead + index |

**Omitted combinations and justification (to be stated in the paper).**

| Omitted | Why |
|---|---|
| `M0+M3`, `M1-kafka+*` | M0 is a negative control; adding dedup turns it into M3. M1-kafka's sink is Kafka; any DB-side add-on is inapplicable. |
| `M2+M4` | Both close W1/W2 by progress atomicity; M4 is strictly stronger (no dedup needed). Adding M2's inbox/outbox would add cost without closing an additional exposure class in the model; the only unique feature (recoverable publication) is already covered by M2+M3. Kept as a *coverage gap* (not tested, stated). |
| `M2+M3+M4` | Predicted strictly dominated by M4+M3 (all of whose exposures are closed) plus M2's relay overhead; no additional exposure class is closed. |
| `M1x+M4` | M4's DB progress + M1x's Kafka transaction: the transaction's atomic progress is *ignored* by M4's resume logic, so the Kafka transaction buys only publication atomicity; this is close to M1x+M3 with a worse failure mode. Not tested; discussed. |

### 4.9 Distinctness matrix

| | Atomic effect+progress | Dedup key | Publication path | Recovery source of truth | Exposure closed |
|---|---|---|---|---|---|
| M0 | no | none | inline | Kafka offsets | — |
| M1-kafka | yes (all in Kafka) | implicit (txn) | in `T` | Kafka txns | W1, W2, publish |
| M1x | no | none | in `T` | Kafka txns | publish only |
| M2 | yes (DB txn) | inbox PK | outbox relay | Kafka offsets | W1, W2, publish (at-least-once) |
| M3 | no | posting index | inline | Kafka offsets | W1 |
| M4 | yes (DB txn) | none | inline | DB progress row | W1, W2 |
| M2+M3 | yes | posting index | outbox relay | Kafka offsets | W1, W2, publish |
| M4+M3 | yes | posting index | inline | DB progress row | W1, W2, zombie |
| M1x+M3 | no | posting index | in `T` | Kafka txns | W1, publish |

The mechanisms are genuinely distinct: no two rows share the same (atomicity, dedup, publication, recovery) vector, and the three combinations each add exactly one component to a row that lacks it.

### 4.10 Retry and failure-handling semantics (identical effort; mechanism-intrinsic differences)

| Situation | M0 | M1x | M2 | M3 | M4 | Rule applies to combinations too |
|---|---|---|---|---|---|---|
| SQLSTATE 40xxx (serialization/deadlock) | retry txn immediately (rolled back) | same | same | same | same | bounded at 5 attempts |
| SQLSTATE 08xxx (connection loss) | retry whole txn up to 3×, backoff 100/300/900 ms | same; abort Kafka txn on exhaustion | same (idempotent: inbox) | same (idempotent: index) | re-read progress; skip if advanced, else retry | exhaustion → no progress commit → redelivery |
| `COMMIT` sent, outcome unknown (client timeout / reset) | **no write retry**; do not commit progress → redelivery → duplicate possible (documented naive policy) | same | retry txn (inbox makes it idempotent) | retry txn (index makes it idempotent) | re-read `consumer_progress`; skip or retry | prevents "unknown-outcome by blind retry" from being mechanism-dependent |
| Kafka `commit_offsets` failure | retry with backoff; on exhaustion stop instance (leave group) | retry; `commit_transaction` failure → abort txn | retry (redelivery harmless) | retry (redelivery harmless) | retry best-effort (not used for resume) | progress stays behind → redelivery |
| `commit_transaction` outcome unknown / `ProducerFencedException` | n/a | abort, exit instance (restart) | n/a | n/a | n/a | transaction coordinator resolves; never retry a commit blindly |
| Poison record (parse/schema error) | skip + log + commit progress (documented) | same | same | same | same | counts as a validity event; golden tests eliminate malformed records in practice |

All retries are bounded and logged with attempt counters in the run record; retry storms are visible in the latency tail and are part of what the paper measures under F5.

### 4.11 Shared vs mechanism-intrinsic configuration (fairness contract)

**Shared (identical across all mechanisms):** consumer `max.poll.records=500`, `fetch.min.bytes=1`, `fetch.max.wait.ms=500`, `session.timeout.ms=10000`, `heartbeat.interval.ms=3000`, `max.poll.interval.ms=300000`, `isolation.level=read_committed`; producer `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, `linger.ms=0`, `batch.size=16384`; JDBC batch size 500, autocommit off, `prepareThreshold` pinned, Hikari pool size 8, `reWriteBatchedInserts=true`; Kafka broker/PG/JVM settings, memory limits, CPU affinity and process priority (§Appendix C); publication stream partitioning = source partitioning.

**Mechanism-intrinsic (declared, audited):** transactional producer + `transactional.id` (M1-kafka, M1x, M1x+M3); `inbox`/`outbox` tables and relay thread (M2, M2+M3); posting unique index (M3, M2+M3, M4+M3, M1x+M3); `consumer_progress` table and assignment-time seek (M4, M4+M3); retry policies exactly as §4.10.

**Equal tuning effort protocol.** No mechanism receives per-mechanism tuning. If a mechanism is found misconfigured during the pilot, the fix is applied to all mechanisms where applicable and the change is recorded in `results/DEVIATIONS.md` before Stage A (A1 §11). The config hash audit (G6) fails the campaign if a non-intrinsic key differs.

---

## 5. Crash-point model and fault injection

### 5.1 Reference protocol and crash points C1–C12

Every mechanism is an instance of the `process(B)` skeleton of §4.1 with three duties: **D-effect** (append postings to the ledger sink), **D-progress** (make progress durable), **D-publish** (emit derived entries to the publication stream). A crash point is a location in that skeleton at which a process may die, a message may be lost or duplicated, or two durable transitions may become visible in a non-program order. The enumeration below is exhaustive for the skeleton: every durable transition, every link between transitions, and every ordering ambiguity is listed.

| ID | Crash point (location) | Durable at the point | Window | Generic recovery |
|---|---|---|---|---|
| **C1** | after `poll()` returns `B`, before any durable operation | nothing of `B` | none | replay `B`; no effect applied twice |
| **C2** | inside the D-effect transaction, before `COMMIT` is sent | nothing of `B` (transaction aborts) | none | replay `B` |
| **C3** | after D-effect `COMMIT` is sent, before its acknowledgement is observed | `B`'s effect **unknown**; progress behind | W1 (conditional) | outcome is re-established by query (dedup) or by replay |
| **C4** | after D-effect is durable, before D-progress is durable | effect durable, progress behind | **W1** | replay of `B` re-applies the effect unless idempotent |
| **C5** | after D-progress is durable, before D-effect is durable | progress durable, effect absent | **W2** | `B` is never redelivered; effect lost permanently |
| **C6** | duplicate delivery of already-processed records **without** a crash: rebalance reassignment of uncommitted offsets, or a group offset rewind | unchanged | W1 (replay) | reprocessing; safe only for idempotent effects or atomic progress |
| **C7** | zombie writer: an owner paused beyond `max.poll.interval.ms` is expelled, its partition is reassigned, and the paused instance resumes and completes its write | the new owner's effect is durable | duplicate write | fencing at the sink (index) or at the transaction layer (producer epoch) |
| **C8** | publish window: D-publish acknowledged/sent, but the durable publication mark is not updated (relay), or the publish is lost while the ledger is committed | ledger durable; publication not durable | publication only | relay retries from the outbox; ledger unaffected |
| **C9** | sink-link loss during D-effect commit: TCP partition or reset while `COMMIT` is in flight | effect **unknown**; progress behind | W1 (conditional) | retry policy (§4.10) decides: unknown outcome is either re-checked or blindly retried |
| **C10** | Kafka-side disruption during D-progress: broker kill, leader change, coordinator failover, transaction abort | effect durable; progress not durable (or txn discarded) | **W1** | replay; for M1x the aborted transaction also discards the publication records → stream/ledger divergence |
| **C11** | acknowledged-before-durable at either store (the store acknowledges before the write is durable) | unknown | both W1 and W2 | **excluded by AS3/AS4**; listed for completeness, and as the reason M3's `early` ordering is a *real* hazard, not a theoretical one |
| **C12** | partial-batch durability: some records of `B` durable, others not (per-record autocommit, or a partially replayed retry set) | part of `B` | W1 (partial) | replay of the batch re-applies the durable part unless idempotent; **unreachable** when D-effect is one atomic DB transaction per poll batch (§4.11) |

**Ordering ambiguity points.** C3/C9 (unknown effect outcome), C4/C10 (effect before progress), C5 (progress before effect) and C11/C12 (durability/partiality) are the only ways the pair `(Commit_S, Commit_K)` can violate atomicity. Lemma 1 (§3.3) is stated over exactly these.

### 5.2 Equivalence classes K1–K7 (proof of equivalence for I1–I4)

Two crash points are *outcome-equivalent* for a mechanism if, for every admissible schedule at the one, there is a schedule at the other with the same projection onto (durable effect, durable progress, publication state) at the moment of recovery — hence identical I1–I4 outcome. Secondary metrics (publication integrity, latency) may still differ; the classes are used only for the correctness matrix of Stage A, and the paper reports the finer points inside a class where secondary differences are measured.

| Class | Members | Equivalence argument (sketch) |
|---|---|---|
| **K1** | C1, C2 | Both leave no durable trace of `B` and progress behind; recovery is identical (replay with no effect). |
| **K2** | C3, C9 | Both leave the effect *possibly* durable with progress behind; the recovery problem is identical (an unknown outcome must be resolved), and the set of admissible resolutions is the same (landed / not landed). |
| **K3** | C4, C10 | Both leave the effect durable and progress behind. For M1x, an aborted transaction (C10) additionally discards publication records; that is a secondary difference recorded per run, not an I1–I4 difference. |
| **K4** | C5 | Unique: progress durable and effect absent (W2). |
| **K5** | C6 | Unique: duplicate delivery with progress state indicating "not yet processed" for records whose effect may already be durable. |
| **K6** | C7 | Unique: two writers of the same partition alive concurrently, so the effect can be applied twice *independently of progress state*. |
| **K7** | C8 | Unique: ledger/progress durable; publication durability window. Ledger invariants are unaffected by construction, so the class exists only as a publication-integrity treatment. |
| **excluded** | C11, C12 | C11 is excluded by AS3/AS4 (durability is assumed stronger than acknowledgement). C12 is unreachable because D-effect is one atomic transaction per poll batch; a per-record-autocommit variant would make it reachable and is listed as an exploratory probe only (D5). |

### 5.3 Reachability matrix (mechanism × crash class)

`R` = reachable and executed in Stage A; `n/a` = structurally unreachable (not "passed"); `probe` = only reachable through the `ordering=early` probe (D2). Reasons in the footnote.

| Mechanism | K1 | K2 | K3 | K4 | K5 | K6 | K7 |
|---|---|---|---|---|---|---|---|
| M0 | R | R | R | probe | R | R | n/a |
| M1-kafka | R | R | n/a¹ | n/a¹ | R | R | n/a |
| M1x | R | R | R | probe | R | R | n/a |
| M2 | R | R | R | probe | R | R | R |
| M3 | R | R | R | probe | R | R | n/a |
| M4 | R | R | n/a² | probe | R | R | n/a |
| M2+M3 | R | R | R | probe | R | R | R |
| M4+M3 | R | R | n/a² | probe | R | R | n/a |
| M1x+M3 | R | R | R | probe | R | R | n/a |

¹ For M1-kafka the effect and progress are one Kafka transaction: a state with effect durable and progress lost (K3) or progress durable and effect lost (K4) is unreachable (`EOS(T)` is single-site atomic, §3.1).
² For M4 and M4+M3, D-effect and D-progress are one PostgreSQL transaction: K3 and K4 are unreachable. Under the `ordering=early` probe the same code commits the progress row in a separate transaction, which *is* the degraded variant the probe is meant to model; it is a probe, not the canonical mechanism.
Probe cells (K4) are executed for every mechanism that has two distinct commits (all except M1-kafka): 8 candidate cells. Count of executed Stage A cells: K1 9 + K2 9 + K3 6 + K4 8 + K5 9 + K6 9 + K7 2 = **52 cells**, plus **9 control cells** (one per mechanism, no injection; three of them are the gate controls of §7.6).

### 5.4 Fault-to-crash-point mapping, and the two levels

| Fault (A1 §7) | Definition | Crash points exercised | Level 1 (deterministic) realisation | Level 2 (stochastic) realisation |
|---|---|---|---|---|
| **F0** | control, no injection | none | — | — |
| **F1** | one Kafka broker killed mid-produce | K2, K3, K1 | kill the broker that leads ≥ 1 source partition and, for M1-family, hosts the transaction coordinator; kill at a phase marker (after D-effect durable, while D-progress is in flight) | kill at a uniformly random offset in the steady-state window (seed-derived, recorded) |
| **F2** | consumer instance killed during processing (rebalance) | K1, K2, K3, K5 | kill at a phase marker (inside D-effect, or after D-effect before D-progress) | kill at a random offset in the processing window |
| **F3** | produce timeout with unknown outcome | K2, K3 | Toxiproxy latency/timeout on the app↔Kafka link at a phase marker so the produce/commit times out while possibly durable | same toxic, applied at a random time for a bounded duration |
| **F4a** | DB commit succeeded + offset commit failed | K3 | after D-effect ack, cut the app↔Kafka link (offset commit cannot persist) → replay | random in the D-progress window |
| **F4b** | offset commit succeeded + DB commit failed (inverse ordering) | K4 | `ordering=early` probe (D2): progress committed first, then a link cut / failpoint aborts the DB transaction before it is durable | same, with random timing; only meaningful in the `early` variant |
| **F5** | network partition app↔DB | K2, K3 | Toxiproxy `disable`/`timeout` on the app↔PostgreSQL link during a commit | random time, bounded duration, restore without reset first, then with reset |
| **F6** | duplicate redelivery after rebalance | K5 | stop both consumers, rewind the mechanism's **own** resume point by `k` batches, restart, let it reprocess | same, with randomised `k` and timing |
| (exploratory) | external progress advance, per-record autocommit | K4, C12 | AdminClient offset commit on behalf of the app, or SQL progress-row update, then kill | not scheduled |

**Semantics of the two levels (important for interpreting every verdict).**

- **Level 1** is the *structural* experiment: a deterministic adversary places the fault exactly at a crash point (via code hooks / failpoints / injected state changes). Here a `P` verdict means the mechanism survived the fault *even when placed in its worst location* — the strongest evidence we can generate, and the one that speaks to H1/H3/H4.
- **Level 2** is the *frequency* experiment: the fault is injected at a random recorded time, so it lands in the exposed window with a probability determined by the mechanism's timing. Here a `V3`/`V4` verdict means "violations occur in some fraction of runs"; the fraction is a measured quantity, not a structural claim. A mechanism can absorb a Level-2 fault *by timing* while failing under Level 1; this is reported as such, never hidden.

### 5.5 Injector specifications

All injectors are native-process scripts driven by the runner; no simulation. Each injector must be able to (a) fire deterministically from a seed + phase marker, (b) log a receipt in `results/faults.log`, (c) restore the system to a running state so that quiescence can be established, (d) be provably non-firing when not armed (sham mode).

| Fault | Manipulated object | Action primitive | Timing | Determinism / repeatability | Restoration | Receipt fields |
|---|---|---|---|---|---|---|
| F1 | Kafka broker process | `taskkill /F /PID <pid>` (real process death; no graceful shutdown) | Level 1: phase marker; Level 2: seeded uniform in window | target broker = deterministic function of run seed and partition/coordinator state at `t_load_start`; fire time recorded to ±1 ms | runner restarts the broker (`kafka-server-start`), waits for broker count = 3 and ISR = 3 per partition, records restore latency; timeout T_restore = 120 s | node id, pid, leader set, coordinator id, offsets at kill, phase marker, actual fire offset |
| F2 | consumer JVM | `taskkill /F /PID` | Level 1: phase marker; Level 2: seeded uniform in processing window | victim instance = seeded choice among group members; assignment at kill recorded | killed instance restarted after the first completed rebalance; group stable before quiescence | pid, member id, assignment (partition→offset), phase marker |
| F3 | app↔Kafka **produce** link only (separate Toxiproxy proxy from the consumer link, so the fault hits the produce path and not the offset commit) | Toxiproxy toxic (`timeout` 5 000 ms or `latency` 5 000 ms with jitter 0) added for a bounded window | Level 1: phase marker; Level 2: seeded time | toxic JSON is the receipt; exact start/stop recorded | toxic removed; proxy health verified | toxic name/type/attributes, start/end, app exception classes |
| F4a | app↔Kafka **consumer** link (carries offset commits) during D-progress | Toxiproxy `disable` for T_hold = 3 s immediately after the D-effect-ack marker, then `enable` | phase-coupled (deterministic by construction) | δ between marker and disable ≤ 50 ms, measured and recorded | link re-enabled; offset commit retried or replayed per §4.10 | marker ts, toxic ts, phase log excerpt, offsets before/after |
| F4b | progress store (early-ordering variant) + app↔**DB** link | `ordering=early` failpoint: progress duty executed before effect duty; then a DB-link cut / failpoint abort before the effect transaction is durable | Level 1: deterministic; Level 2: seeded | flag is part of the cell definition (prereg), not an accident | restart; recovery per mechanism | flag value, hook ids, abort point, progress state |
| F5 | app↔PostgreSQL link | Toxiproxy `disable` (blackhole) or `reset_peer` for T_hold, then restore | Level 1: phase marker; Level 2: seeded | same | link restored; connection pool re-established; readiness probe | toxic JSON, SQLSTATEs observed, `pg_stat_activity` snapshot before/after |
| F6 | mechanism's resume point | Kafka Admin `alterConsumerGroupOffsets` (offset-store mechanisms) or `UPDATE consumer_progress` (M4-family); consumers stopped during the rewind | Level 1: fixed `k`; Level 2: seeded `k` and time | rewind amounts and resulting offsets recorded per partition | consumers resumed; group stable | offsets before/after per partition, rows updated, `k` |

**Non-events are not free.** A run in which the injector was armed but did not fire (target phase never occurred, target already dead, toxic rejected) is **invalid**, excluded from the analysis, counted, and its exclusion rate reported per cell (A1 §5). The gate is applied by the runner *without looking at outcome data* (§7.6, G4): validity is decided from `faults.log` and process state only.

### 5.6 Fault log, attestation, and exclusion policy

`results/faults.log` is append-only NDJSON, one line per *action* (including restoration):

```json
{"ts_utc":"2026-11-24T12:00:00.123Z","run_id":"A-M1x-K3-r07","fault":"F1","level":"L2",
 "target_role":"broker","target_id":"2","phase_marker":null,"scheduled_ms":3120,"actual_ms":3124,
 "action":"taskkill /F /PID 12345","result":"fired","before":{"leader_set":[0,1],"isr":3},
 "after":null,"seed":"hex","notes":"killed leader of p0"}
```

Rules: (1) an armed injection must produce exactly one `fired` receipt and one `restored` receipt; (2) the runner marks the run `invalid_fault_not_fired` if the receipt is missing at the expected step; (3) exclusion rate per cell = excluded runs / (excluded + valid runs), reported for every cell; (4) no run may be excluded for any outcome-based reason — only for provenance/validity reasons fixed before outcome inspection (G4, §7.6); (5) a cell with exclusion rate above 20 % is flagged in results and in the paper, and its inferences are labelled fragile.

---

## 6. Propositions, counterexample traces, and prediction tables

The propositions are drafts for the author, who owns the formal section (A1 §3). Each is stated so that a planned measurement can falsify it; the measurement is named. Proof sketches use Lemmas 1–2 and the crash-point model; counterexamples are concrete traces in §6.2.

### 6.1 Propositions

**P1 (EOS boundary).** Kafka's atomic commit covers exactly `EOS(T)` (§3.1, §3.2). For every mechanism except M1-kafka, the PostgreSQL `COMMIT` is outside the transaction; therefore under W1 the *internal stream* and the *external ledger* can disagree even though the internal guarantee holds.
*Proof.* §3.1–3.3 by construction; divergence trace X4. *Falsified if:* M1x shows zero violations in all K3/fault cells while the stream is exactly-once (no divergence) — i.e. if the gap does not open under adverse Level-1 injection.

**P2 (W1 closure condition).** Under strict ordering (D-effect durable before D-progress) and crash-recovery faults, a mechanism preserves I3 under every schedule in K2/K3 iff it is *either* idempotent on `(event_id, leg_no)` (dedup) *or* has D-effect and D-progress in one atomic commit. Formally: `I3 preserved ⇔ (dedup ∨ atomic)`.
*Proof.* (⇐) replay re-executes `B`: with dedup the second write is a no-op; with atomicity, a durable effect implies durable progress, so no replay occurs (Lemma 1 clause 1). (⇒) if neither holds, choose schedule σ ∈ K3 (effect durable, progress not); replay re-executes `B` and inserts a second copy — trace X1. *Falsified if:* a mechanism without dedup and without atomic progress yields zero duplicates in K3 while the injector demonstrably fired (M0 serves as the negative control for this test, G1).

**P3 (W2 closure condition).** Under any ordering, a mechanism preserves I4 against K4 iff D-effect and D-progress are one atomic commit; ordering alone cannot close W2 *when the fault itself is an ordering fault*; and de-duplication is irrelevant to W2.
*Proof.* In K4 progress is durable and effect is absent; no replay occurs (progress says done), so idempotence has nothing to act on; only atomicity prevents the state. Trace X3. *Falsified if:* a mechanism with non-atomic progress loses no posting under the `early` probe with a demonstrably fired K4 injection.

**P4 (M0 is a valid negative control).** M0 yields I3 violations in at least the K3 and K5 cells under Level-1 injection and in at least one fault condition under Level-2 injection.
*Proof.* P2 with no dedup/no atomicity; trace X1. *Falsified if:* M0 is clean in K3 with a fired injector → harness defect, gate G1 halts the campaign.

**P5 (M1-kafka positive control).** M1-kafka preserves I1–I4 in every executed cell under the model, because both duties are confined to `EOS(T)` (§3.1); its publication stream is exactly-once by construction.
*Proof.* Single-site atomicity; replay of uncommitted transactions produces no durable record; committed transactions carry their offsets. *Falsified if:* any M1-kafka run shows a violation (indicates a harness or configuration defect; gate G2).

**P6 (M1x fails exactly where the gap opens).** M1x violates I3 in K2/K3 and in the corresponding Level-2 fault conditions, and preserves I1–I4 in K1 and (under strict ordering) has no W2. Its stream is exactly-once while its ledger is not (P1).
*Proof.* P2 with neither dedup nor atomicity; the stream records are in `T`, the ledger rows are not (trace X1/X4). *Falsified if:* M1x clean in K3 with a fired injector (gate G1) or diverges structurally for another reason.

**P7 (M2 sufficiency).** M2 preserves I1–I4 in every cell in scope: K1–K7 are absorbed by the atomic `{postings, inbox claim}` transaction (K2/K3/K5), by strict ordering (K4), by the outbox relay (K7), and by the inbox under C7 (K6).
*Proof.* Each class: the inbox claim makes replay a no-op; the ledger transaction is atomic; publication is recoverable from the outbox. *Falsified if:* any M2 cell shows a violation under a fired injector. H2's cost component is a separate, pre-registered prediction tested by Stage C.

**P8 (M3: dedup closes W1, cannot close W2).** M3 preserves I1–I4 in K1, K2, K3, K5, K6 (index makes replay a no-op, including the zombie writer), and violates I4 in K4 under the `early` probe. I3 is preserved in every cell: M3 yields zero duplicates in all modelled conditions.
*Proof.* W1 closure by the unique index (P2); W2 is dedup-blind (P3, trace X3). *Falsified if:* M3 produces a duplicate under a fired injector (I3 claim) or shows no loss in K4 with the probe demonstrably fired (H3 claim).

**P9 (M4: atomicity closes W1/W2, leaves the zombie open).** M4 preserves I1–I4 in K1, K2, K3 (unreachable), K5, and in K4 (unreachable; atomic progress), but violates I3 in K6 (C7) because it has neither dedup nor epoch fencing at the sink. Its Kafka offset commits are irrelevant to correctness (they may be lost without consequence).
*Proof.* Atomic `{postings, progress}` closes both windows (Lemma 2); a zombie writer inserts a second copy with no constraint to stop it (trace X6). *Falsified if:* M4 duplicates in K5 with a fired injector, or is clean in K6 under a demonstrably fired zombie injection.

**P10 (combinations).** M2+M3, M4+M3 and M1x+M3 each add exactly one closure to their base mechanism: M2+M3 → same correctness as M2 at lower write cost (index instead of inbox); M4+M3 → M4 plus K6 closure (index); M1x+M3 → M1x plus K2/K3 closure (index) while retaining publication atomicity. Each preserves I1–I4 in every cell in scope.
*Proof.* Superposition of P6–P9; no new exposure is introduced because the added component is a constraint, not a new durable duty. *Falsified if:* a combination shows a violation in a cell where its base mechanism's own predictions say it is clean.

**P11 (balance checks are blind to duplication).** There exist runs in which I3 is violated while I1 is not (whole-entry duplication preserves the sum-to-zero property). Consequently a practitioner checking only `sum(signed)=0` cannot detect the EOS-gap duplicates.
*Proof.* §2.8 table; trace X1 (the second copy of a balanced entry is itself balanced). *Falsified if:* every duplicate run also shows I1 > 0 (i.e. duplicates are always structurally detectable) — measurable directly from the per-run vectors: `v3=1 ∧ v1=0` must occur in the duplicate cells.

**P12 (no dominance).** Under the model, no single mechanism is both maximally correct and unambiguously cheapest: M3 is the cheapest complete ledger-only mechanism under strict ordering but is blind to W2 (P8); M4 closes W1/W2 without an index but is open to C7 and does not preserve publication exactly-once; M1x+M3 is needed for ledger integrity *plus* Kafka-internal exactly-once publication and pays the transaction cost; M2 buys recoverable publication at the highest write cost. H5 is supported if the measured performance ordering shows at least one trade-off dimension consistent with this and no mechanism is simultaneously non-violating in every cell and non-inferior on all performance metrics.
*Proof.* Lemma 2's closure table + §4.9 distinctness matrix; traces X2/X5/X6/X7. *Falsified if:* some mechanism **both** shows zero violations in every cell **and** is non-inferior in median throughput and p99 latency (Cliff's δ within the negligible band) to the best competitor — then a single mechanism dominates.

### 6.2 Counterexample traces

Traces are written against the reference protocol; `B` is one poll batch of 500 records; "durable" lists durable state after the step.

**X1 — M0/M1x, W1 duplicate (C4, K3).**
1. `B` = offsets 1000..1499; D-effect `COMMIT` returns → durable: postings of `B`.
2. Process killed (F2 phase marker) before D-progress.
3. Restart; group offset still 1000; `B` redelivered.
4. Without dedup, step 1 is executed again → `mult(D,π) = 2` for every `π ∈ Π(B)` → **I3 violated**, `v3 = 1`; I1 clean (each copy balanced). This is trace for P4/P6 and the witness for P11.

**X2 — M3, same schedule (C4, K3).**
Steps 1–3 as X1; step 4 re-inserts with `ON CONFLICT (event_id, leg_no) DO NOTHING` → all rows conflict, no new row → `mult(D,π) = 1` → **P (I3 preserved)**. Shows W1 closed by dedup (P8).

**X3 — M3 with `early` ordering, W2 loss (C5, K4).**
1. `B` = offsets 1000..1499; D-progress committed first (the probe flag) → durable: progress = 1500.
2. Process killed before the D-effect transaction is durable.
3. Restart; resume at 1500; `B` is never redelivered.
4. The postings of `B` were never written; no dedup can create them → **I4 violated**, `v4 = 1`. Witness for P3/P8 and for H3 (trace X8 is the external-advance variant).

**X4 — M1x, stream/ledger divergence (C10, K3).**
1. Transaction `T` produces derived entries for `B` to the stream; DB `COMMIT` for `B` returns → ledger durable.
2. Broker kill (F1) rolls `T` back (no stream records, no offsets).
3. Replay re-writes the ledger and commits a new `T` with one stream record per entry.
4. Ledger has `2·|B|` postings; stream has `|B|` records → **ledger I3 violated** and `divergence = |B|` → witness for P1.

**X5 — M4, atomicity absorbs (C4, K3-unreachable; and C3).**
1. `B`'s transaction `{postings, progress}` commits atomically → durable: postings and progress = 1500.
2. Crash before any Kafka offset commit. Restart: resume from stored progress 1500 → no replay → **P**. If instead the crash happened before the DB commit (C2/K1), the transaction aborts with the progress row → replay → **P**. No schedule in between exists (single commit) → K3/K4 unreachable for M4 (P9).

**X6 — M4, zombie duplicate (C7, K6).**
1. Instance A owns partition 0 at offset 1000, reads `B`, begins the DB transaction, then is paused (failpoint sleep) beyond `max.poll.interval.ms`.
2. Group reassigns partition 0 to instance B, which processes `B` and later `B'` (1000..1999) → durable.
3. A resumes, its transaction is still open (no DB timeout), commits `{postings of B, progress 1500}` → postings of `B` written a second time → **I3 violated**. Shows M4 needs K6 fencing (P9) and motivates M4+M3.

**X7 — M2, publication duplicate with clean ledger (C8, K7).**
1. Ledger transaction `{postings, inbox, outbox}` commits.
2. Relay sends the outbox record to the stream and is killed before `UPDATE outbox SET published_at`.
3. Relay restarts, resends → stream has 2 records for the event, ledger has 2 postings once → **ledger P**, `pub_dup = 1`. Shows M2's publication is at-least-once (P7), not exactly-once — a distinction the paper must state.

**X8 — any dedup mechanism, external progress advance (C5 realised by injection, K4).**
1. `B`'s effect is not yet durable; the injector advances the mechanism's resume point past `B` (AdminClient offset commit, or SQL update of the progress row).
2. Process killed; restart resumes past `B`.
3. `B` is never written; dedup cannot restore it → **I4 violated**. This is the mechanism-independent witness of P3; it is the same failure as X3 but realised as an injected state change (D2 option O-c).

### 6.3 Prediction table — mechanism × fault condition (72 cells)

Verdicts are **model predictions under adverse realisation** of the fault (Level 1 semantics, §5.4). For Level-2 (random-timing) runs the same verdict is predicted but its *realisation frequency* is measured; a `P` cell means the mechanism has no exposure path, not that violations are statistically impossible. `J`-codes are one-line justifications below the table.

| Mechanism | F0 | F1 | F2 | F3 | F4a | F4b | F5 | F6 |
|---|---|---|---|---|---|---|---|---|
| **M0** | P (J1) | C3 (J2) | V3 (J3) | P (J4) | V3 (J5) | V4 (J6) | V3 (J7) | V3 (J8) |
| **M1-kafka** | P (J1) | P (J9) | P (J9) | P (J9) | n/a (J10) | n/a (J10) | n/a (J11) | P (J9) |
| **M1x** | P (J1) | V3 (J12) | V3 (J3) | V3 (J12) | V3 (J5) | V4 (J6) | V3 (J7) | V3 (J8) |
| **M2** | P (J1) | P (J13) | P (J13) | P (J13) | P (J13) | V4 (J14) | P (J13) | P (J13) |
| **M3** | P (J1) | P (J15) | P (J15) | P (J15) | P (J15) | V4 (J16) | P (J15) | P (J15) |
| **M4** | P (J1) | P (J17) | P (J17) | P (J17) | P (J17) | n/a (J18) | P (J17) | P (J17) |
| **M2+M3** | P (J1) | P (J19) | P (J19) | P (J19) | P (J19) | V4 (J14) | P (J19) | P (J19) |
| **M4+M3** | P (J1) | P (J20) | P (J20) | P (J20) | P (J20) | n/a (J18) | P (J20) | P (J20) |
| **M1x+M3** | P (J1) | P (J21) | P (J21) | P (J21) | P (J21) | V4 (J6) | P (J21) | P (J21) |

**Justifications (one line per code; the cell's verdict is the union of its invariants).**

- **J1** No fault: no window opens; every mechanism executes its protocol to completion (control; also the fault-free positive-control cells of G2).
- **J2** Broker kill: M0's ledger effect is already durable when its offset commit is disrupted; if the commit retries exhaust during coordinator failover the instance restarts and the batch is replayed → duplicates, otherwise progress is recovered and the ledger is preserved. Conditional; frequency measured (C3).
- **J3** Consumer death between a durable effect and durable progress (W1) → replay → duplicate postings; M0 and M1x have no dedup (P2, trace X1).
- **J4** A produce timeout on M0's publication path does not touch the ledger, and its progress is retried; ledger preserved (publication at-least-once, may duplicate or lose — secondary).
- **J5** F4a is exactly W1 realised deterministically: effect durable, offset/progress commit fails, redelivery re-applies → duplicates (P2; trace X1).
- **J6** F4b is W2 realised by the `early` ordering probe: progress durable, effect absent, no replay → lost postings; dedup is irrelevant (P3; trace X3/X8).
- **J7** A DB link partition makes the effect commit outcome unknown; M0's documented policy does not retry the write, so redelivery re-applies it when the commit had landed → duplicates in the affected runs (P2; §4.10).
- **J8** F6 replays already-processed batches: no dedup → duplicates (P2, trace X1); for M0 this is the cleanest duplicate demonstration.
- **J9** Both duties live inside one Kafka transaction; replay of an uncommitted transaction yields no durable record, and a committed transaction carries its offsets → I1–I4 preserved (P5).
- **J10** F4a/F4b require two distinct commits; M1-kafka has one (`EOS(T)`), so the state is unreachable (n/a, §5.3 footnote ¹).
- **J11** F5 targets the app↔DB link; M1-kafka has no DB link (role-substituted Kafka-link variant is predicted P, §5.5).
- **J12** M1x's effect is outside `T`: a broker kill aborts `T` (offsets not committed) while the DB rows persist → replay → duplicates, and the aborted stream diverges from the ledger (P1/P6, trace X4).
- **J13** M2's `{postings, inbox}` transaction is atomic and replay is absorbed by the inbox; W2 is closed by strict ordering (P7).
- **J14** Under the F4b `early` probe, M2's Kafka offsets can be durable while the ledger transaction is not; the inbox cannot restore a never-written posting → losses (P3/P7; trace X3).
- **J15** M3's unique index makes every replay a no-op; no dedup-independent loss path exists in F1/F3/F5/F6 (P8).
- **J16** H3's cell: with the offset committed before the DB write is durable, the unique index has nothing to deduplicate; postings are lost (P3/P8; trace X3).
- **J17** M4's `{postings, progress}` transaction is atomic, so both windows are closed; its Kafka offset state is not consulted on resume (P9).
- **J18** K3 and K4 are unreachable for M4's canonical protocol (single atomic commit; §5.3 footnote ²). The same code under the `early` probe is a degraded variant: its K4 outcome is predicted I4 (J6) and is reported as a *probe*, not as canonical M4.
- **J19** M2+M3: dedup by posting index replaces the inbox claim; all other M2 properties hold (P10).
- **J20** M4+M3: atomic progress closes W1/W2 and the posting index closes C7 (P10, trace X6).
- **J21** M1x+M3: Kafka transaction keeps publication exactly-once; the posting index closes the external W1 (P10).

**Counts.** 72 cells = 9 mechanisms × 8 conditions. Verdict distribution (model prediction, not measured): **P = 50, C3 = 1, V3 = 10, V4 = 6, n/a = 5**. The five n/a cells are: M1-kafka × {F4a, F4b} ("no external store commit to diverge from the Kafka transaction"), M1-kafka × F5 ("no app↔DB link to partition"), and M4/M4+M3 × F4b ("atomic progress makes W2 unreachable"). Each is justified individually and none is silently dropped (A1 §11). **F4b cells are executed in the `ordering=early` variant of the same code** (D2): F4b *is* the ordering fault, so it can only be realised where the mechanism has two distinct commits and where the flag is admitted; the degraded `early` probe on M4/M4+M3 is reported separately in Stage A K4, never as the canonical mechanism.

### 6.4 Prediction table — mechanism × crash class (63 cells)

Multiplicity `(I3, I4)` is the expected violation class; `P` = I1–I4 preserved.

| Mechanism | K1 | K2 | K3 | K4 (`early` probe) | K5 | K6 | K7 |
|---|---|---|---|---|---|---|---|
| **M0** | P | C3 (J22) | I3 (J3) | I4 (J6) | I3 (J8) | I3 (J23) | n/a |
| **M1-kafka** | P | P (J9) | n/a (J10) | n/a (J10) | P (J9) | P (J24) | n/a |
| **M1x** | P | I3 (J25) | I3 (J3) | I4 (J6) | I3 (J8) | I3 (J23) | n/a |
| **M2** | P | P (J13) | P (J13) | I4 (J14) | P (J13) | P (J26) | P (J27) |
| **M3** | P | P (J15) | P (J15) | I4 (J16) | P (J15) | P (J28) | n/a |
| **M4** | P | P (J29) | n/a (J18) | I4 (J6) | P (J29) | I3 (J30) | n/a |
| **M2+M3** | P | P (J19) | P (J19) | I4 (J14) | P (J19) | P (J31) | P (J27) |
| **M4+M3** | P | P (J32) | n/a (J18) | I4 (J6) | P (J32) | P (J33) | n/a |
| **M1x+M3** | P | P (J21) | P (J21) | I4 (J6) | P (J21) | P (J34) | n/a |

- **J22** Unknown effect outcome: if the commit landed, replay duplicates (I3); if not, no violation — outcome is conditional on the resolution, recorded per run.
- **J23** No dedup and no atomic progress: a resumed zombie writes a second copy (trace X6).
- **J24** Producer-epoch fencing prevents a fenced producer from committing; the aborted duplicate work leaves no durable record.
- **J25** M1x under unknown effect outcome: if the external `COMMIT` landed, replay re-applies it (I3); the Kafka transaction abort does not undo the DB write.
- **J26** M2's inbox absorbs the zombie's second claim (the claim conflicts) — preserved.
- **J27** Relay crash window affects only publication durability; ledger and progress are durable; the relay resumes from the outbox (P7, trace X7).
- **J28** The zombie's second insert conflicts on `(event_id, leg_no)` → no duplicate (P8).
- **J29** M4's atomic transaction: an unknown outcome is resolved by re-reading progress — advanced ⇒ skip, not advanced ⇒ retry; no duplicate, no loss.
- **J30** M4 with no dedup and no sink-side fencing: the resumed zombie's plain insert duplicates (trace X6). This is the single correctness exposure of M4 and the reason M4+M3 exists.
- **J31** Inbox claim (as M2) plus index: zombie and replay both absorbed.
- **J32** Atomic progress plus index: K2 absorbed by atomicity.
- **J33** Index closes the K6 exposure of M4.
- **J34** Index closes K6 and K2 for M1x+M3.

**Executed Stage A cells:** 52 (K1 9, K2 9, K3 6, K4 8, K5 9, K6 9, K7 2) + 9 control cells (one per mechanism). **Non-executed:** K3 for M1-kafka/M4/M4+M3 (3 cells), K4 for M1-kafka (1 cell), K7 for every mechanism without a relay (7 cells) — each with the reachability/structural proof above, recorded as n/a in the matrix and never as a pass. The K4 cells for M4/M4+M3 use the `early` probe and are labelled "degraded-variant probe" in the results, not "canonical M4".

### 6.5 Hypothesis → proposition → planned measurement

| Hypothesis | Propositions | Primary measurement (stage / cells) | Decision rule (frozen in `docs/prereg.md` §3) |
|---|---|---|---|
| H1 | P1, P5, P6 | Stage A K2/K3 for M1x vs M1-kafka; Stage B F1/F2/F3/F4a; divergence metric | M1x has ≥ 1 violating run in K3 (injector fired) and M1-kafka has none in all executed cells; else falsified/inconclusive |
| H2 | P7 | Stage A K1–K7 for M2; Stage B all faults; Stage C throughput/latency | M2 zero violating runs in all cells with exact upper bounds; performance penalty vs M3/M4 measured with pre-registered effect-size thresholds |
| H3 | P3, P8 | Stage A K4 `early` probe for M3 (trace X3); Stage B F4b | M3 shows zero I3 violations in all cells **and** ≥ 1 I4 violation in K4/F4b with the probe fired |
| H4 | P9 | Stage A K1–K7 for M4/M4+M3; Stage C latency vs M2 | M4/M4+M3 zero violations in all *reachable* cells; M4 latency lower than M2 with pre-registered threshold |
| H5 | P10, P12 | Stage A/B correctness + Stage C performance, combined | no mechanism is simultaneously fully non-violating and non-inferior on all performance metrics |

### 6.6 RQ3 answered by the model: minimal sufficient sets

RQ3 asks for the minimal mechanism set per failure exposure. The model answers **before** any measurement (the author's requirement: the experiments are evidence, not proof). Rows are exposure classes, defined so that each is a distinct *closure obligation* (Lemma 2).

| Exposure | Closure obligation | Minimal sufficient mechanism(s) | Why minimal | Combinations that also work |
|---|---|---|---|---|
| **E1** W1: duplicate effect after redo | idempotent effect *or* atomic effect+progress | **M3** (index, cheapest) or **M4** (atomic progress, no index) | one component closes it; no second mechanism needed | any of M2, M2+M3, M4+M3, M1x+M3 |
| **E2** W2: progress ahead of effect | atomic effect+progress, *or* enforced strict ordering | **M4** (internalises the ordering; no external obligation) | ordering alone is a code obligation, not a mechanism; M4 is the mechanism that discharges it structurally | M4+M3 |
| **E3** C7 duplicate writer (zombie) | dedup key at the sink *or* transactional fencing | **M3** (index) or **M1** family (fencing) | one of the two mechanisms; M4 alone is insufficient | M4+M3, M2, M2+M3, M1x+M3 |
| **E4** unknown-outcome retries (C3/C9) | idempotent effect *or* atomic progress | **M3** or **M4** | same as E1 | M2, combinations |
| **E5** Kafka-internal exactly-once publication | Kafka transaction *or* recoverable relay with downstream dedup | **M1 family** (only in-transaction publication is exactly-once) | the outbox relay is at-least-once (trace X7) | M1x+M3 (ledger + internal EOS), M2 (recoverable, not exactly-once) |
| **E1∧E2∧E3∧E4** (full ledger exposure) | all ledger closures | **M4+M3** | M4 discharges E2, M3 discharges E1/E3/E4; single mechanisms each miss one class | — |
| **E1..E4 ∧ E5** (full ledger + internal EOS) | as above + publication | **M1x+M3** | M1x supplies E5 and M3 supplies E1/E3/E4; E2 is discharged by M1x's strict ordering | — |

**Consequence (model answer to RQ3).** Under strict ordering, **M3 alone is the cheapest sufficient mechanism for ledger integrity** (E1, E3, E4) and needs the *ordering obligation* for E2; if the ordering obligation is not to be relied on (because early/optimistic offset commits are deployed), the minimal ledger-sufficient set becomes **M4+M3**; if the pipeline also requires exactly-once publication, the minimal set becomes **M1x+M3**. This is the paper's practitioner message, to be checked experimentally (H1–H5) and reported as a decision table (§11) whose value cells are filled only from measurements.

### 6.7 Formal-part options for the author (FO-1…FO-6)

A1 §3 assigns the formal part to the author. The following options are drafted so that a choice can be made at P1 without blocking the prereg.

- **FO-1 — Model form.** (a) Pen-and-paper state machine + lemmas (Lemmas 1–2, P1–P12) with counterexample traces [recommended: matches venue and page budget]; (b) TLA+ specification (`formal/ledger.tla`) with TLC bounds reported [adds rigour, costs tooling/page space]; (c) both. *Trade-off:* (b) needs a TLA+/TLC toolchain not yet vendored; (a) is sufficient for journal review if traces are concrete.
- **FO-2 — EOS-boundary formalisation.** (a) Set of covered transitions `EOS(T)` + two-window definitions [recommended, §3.1–3.3]; (b) a global-history atomicity predicate ("no interleaving observable"); (c) both. *Trade-off:* (b) is more elegant but requires more pages to define visibility; (a) is directly testable by the injectors.
- **FO-3 — I1 formalisation.** (a) structural + sum-zero (I1a ∧ I1b) [recommended]; (b) sum-zero only, literally as A1 §3 states. *Trade-off:* (a) detects mangled/partial entries; (b) is weaker and makes I1 blind to the duplicate case the paper cares about. Decide D8.
- **FO-4 — I4 formalisation.** (a) end-state completeness at quiescence (oracle-computable) [primary]; (b) temporal "eventually" under fair recovery (TLA-checkable); (c) both, (a) reported as the measured instance of (b). *Trade-off:* (a) is falsifiable experimentally; (b) needs a fairness assumption and cannot be measured directly. Decide D9.
- **FO-5 — Proof obligation set.** The claimed theorems are: Lemma 1 (two windows), Lemma 2 (closure table), P2, P3, P5, P7–P10, P12. Option (a) pen-and-paper with the traces of §6.2 [recommended]; (b) machine-checked with TLAPS/TLC for a bounded instance [expensive]; (c) pen-and-paper plus bounded TLA+ model as appendix material.
- **FO-6 — Bounds for the optional TLA+ model.** If checked: `MaxEvents = 3`, `MaxBatches = 2`, `Partitions = 2`, two sink models (Kafka-atomic vs DB), crash points at the four decisive locations (C3, C4, C5, C7). Report the bound sizes and the checked invariant set {I3, I4} in the paper. `formal/ledger.tla` is provided as a starting point (DRAFT, not yet TLC-checked; see `formal/README.md`).

---

## 7. Metrics, the independent oracle, and the harness-validation gate

### 7.1 Correctness metrics (operational definitions)

Each metric is computed by the oracle from `(Ω, D)` and is written verbatim into the run record; the analysis only reads run records.

| Metric | Definition | Unit | Aggregation |
|---|---|---|---|
| `dup_postings` | `Σ_π (mult(D,π) − 1)⁺` | postings | per run; median/IQR and proportion of runs > 0 per cell |
| `dup_entries` | `Σ_eid (⌊mult(D,eid)/2⌋ − 1)⁺` | entries | per run |
| `lost_postings` | `card({π ∈ Ω : π ∉ keys(D)})` | postings | per run |
| `lost_entries` | `card({eid : fewer than 2 expected legs present})` | entries | per run |
| `imbalanced_entries` | `card({eid : I1a or I1b violated})` | entries | per run |
| `mutated_postings` | `card({π ∈ keys(D) ∩ Ω : V_D(π) ≠ V*(π)})` | postings | per run |
| `spurious_postings` | `card({π ∈ keys(D) \ Ω})` | postings | per run; harness-invalid if > 0 unexpectedly |
| `v1..v4` | indicator of each class > 0 (I1..I4) | boolean | per run; proportions per cell |
| `violating_run` | `v1 ∨ v2 ∨ v3 ∨ v4` | boolean | per run; **primary correctness outcome** |
| `pub_dup`, `pub_lost` | same predicates applied to the publication stream for DB-sink mechanisms | records | secondary outcome only |
| `divergence` | `card(stream-records) − card(ledger-entries)` on matched events (M1x signature) | entries | secondary outcome |
| `exclusion_rate` | excluded runs / (excluded + valid) per cell | ratio | reported for every cell (§5.6) |

The **primary correctness outcome** for all hypotheses is `violating_run`; the per-invariant flags are secondary and are reported so that a reader can see *which* invariant failed (P11 depends on `v3=1 ∧ v1=0` occurring).

### 7.2 Performance metrics

**Open-loop load.** The generator emits each event at a pre-computed schedule time (`scheduled_at_ms`) at a fixed target rate `R_gen`, independent of service responses (no closed-loop feedback). The generator is validated in the pilot to sustain `R_gen ≥ 3 × max mechanism throughput`; if the generator cannot saturate, performance numbers measure the generator (validity gate G7, §7.6). Actual send time is recorded per event; generator lag (`actual_send − scheduled`) is a recorded covariate, and a run whose generator lag exceeds 5 ms p99 is flagged (but not excluded — it is a realism datapoint).

**End-to-end latency.** For an event `e`:

```
ℓ(e) = t_effect_commit_observed(e) − scheduled_at_ms(e)
```

where `t_effect_commit_observed` is the application's observation of the effect becoming durable for the batch containing `e` (for M1-kafka: the observation of `transaction.commitTransaction` returning; for DB mechanisms: the observation of `COMMIT` returning). One host clock (AS8). `ℓ` is recorded per event into an HdrHistogram with 3 significant digits, range 1 µs..60 s, and the run record stores p50/p90/p99/p99.9/max plus the histogram dump path. Batch structure means all events of a batch share one commit instant; the paper reports both per-event and per-batch percentiles (the per-event percentile is the primary definition; per-batch is a secondary diagnostic so a reader can see batching effects).

**Throughput.** `events_committed / steady_state_duration`, where `steady_state_duration` runs from the end of the warm-up to the last commit of the last intended event. Warm-up = the first `max(2 000, 10 % of N)` events, excluded from all performance metrics but *included* in correctness. `events_committed` is counted from durable commit observations, not from sends.

**Reported statistics.** Per cell: median and IQR of (throughput, p50, p99, p99.9); the analysis resamples runs (§9.3). Latency and throughput are never compared across *different sinks* (M1-kafka vs DB mechanisms) except where explicitly labelled construct-limited (D13).

### 7.3 Time-to-consistent-state

PostgreSQL-level "commit" is not the end of the story: retries, relay catch-up, and quiescence lag matter. Define, measured from `t_load_end` (last intended event acknowledged by the source producer):

- `t_quiesce` = first `t` such that, simultaneously: (i) consumer-group lag = 0 for all source partitions (AdminClient `listConsumerGroupOffsets` vs `listOffsets`), (ii) no new sink row for `T_quiet` = 10 s (poll `count(*)` at 1 Hz), (iii) no active application transaction (`pg_stat_activity`: no sessions in `idle in transaction` of the application), (iv) the mechanism exposes no pending retry (application status endpoint or, when the application is stopped for observation, the runner has already waited `T_drain` ≥ the maximum retry backoff). 
- `t_consistent` = `t_quiesce − t_load_end` **if** the final full observation passes (`Consistent`), else the run is **censored**: `t_consistent > T_max` (no consistent state observed within `T_max` = 300 s).

Censoring is an outcome, not a missing value: the proportion of censored runs per cell is reported with an exact confidence interval, and censored runs are included in the correctness primary outcome (they are violating runs). The polling cadence is 1 Hz using only cheap monotone indicators (`count(*)`, max `inserted_at`, lag); the expensive full predicate set is evaluated once at the observation snapshot (a `REPEATABLE READ` read-only transaction after the application is stopped), so `t_consistent` is a *quiescence-based* lower bound, stated as such.

### 7.4 The independent oracle

The oracle is built **before** the mechanisms (A1 §4) and shares no code with them: separate process, separate language (Python), read-only database role, no imports from the artifact, and no use of the pipeline's classes.

```
                ┌───────────────────────────────┐
 master_seed ──▶│ Ground-truth generator (Java) │──▶ gt_<run>.ndjson + .sha256   (BEFORE any produce)
                └───────────────────────────────┘
                                │  produce (acks=all, idempotent)
                                ▼
                        source topic  ── dump ──▶ source_dump_<run>.ndjson
                                │
                                ▼
                       pipeline under test ──▶ sink (DB or stream) ──▶ sink_dump_<run>.csv
                                                                             │
  (master_seed) ──▶ Oracle (Python, independent) ◀────────────────────────────┘
       regenerate expected set; verify sha256; verify source integrity;
       compare expected vs observed; write oracle_<run>.json
```

**Independence guarantees.**
1. Ground truth is written and fsynced before the first produce; its SHA-256 is stored.
2. The oracle regenerates the expected set from `(master_seed, seq)` using the *specification* (§1.4.1), not the Java code; the Java generator's digest must equal the Python digest (cross-implementation check, gate G5). A mismatch invalidates the run and indicates a specification bug.
3. `E_source` is obtained from an independent dump of the source topic; the source-integrity gate requires (a) per-partition offsets contiguous from 0 to the last committed offset, (b) the event-id multiset of the dump equal to the produced manifest, (c) the dump's postings digest equal to the regenerated digest. Only then is `Ω` well-defined.
4. The oracle reads the sink through a read-only role; it cannot modify it.
5. The oracle is mutation-tested (gate G3) against synthetic defects before the campaign.

**Oracle outputs.** `oracle_<run>.json`: the counts of §7.1, the verdict, the digests used, and the paths of the dumps; the raw dumps are retained (see the disk policy in `docs/prereg.md` §9) so any claim can be recomputed by a third party.

### 7.5 Quiescence

Quiescence is defined in §7.3 and is a *prerequisite of observation*: no predicate is evaluated, no latency is finalised, and no run is declared finished until §7.3 (i)–(iv) hold or `T_max` elapses. The runner enforces it; a run that reaches `T_max` is recorded as censored (and is a violating run if the observation then fails, or a *quiescence failure* if the application is still active). `T_quiet = 10 s` and `T_max = 300 s` are pre-registered constants; a pilot calibration doubles `T_quiet` and verifies that the counts do not change (gate G7).

### 7.6 Harness-validation gate (must pass before any measurement is analysed)

The gate is executed on the pilot's data and on dedicated validation runs; **no Stage A/B/C run is analysed before all gates pass** (A1 §5).

| Gate | Requirement | Failure action |
|---|---|---|
| **G1 negative control** | M0 shows I3 violations in the deterministic K3 and K5 cells (and in ≥ 1 Level-2 condition), with the injector attested | HALT: harness, injector, or oracle is broken |
| **G2 positive controls** | M1-kafka and M4+M3 show zero violations in all *fault-free* control runs (exact upper bound reported) | HALT |
| **G3 checker mutation testing** | For each synthetic defect class — drop a row, duplicate a row, flip a leg sign, update a row's value, insert a spurious row — the oracle detects it in **100 %** of ≥ 100 seeded mutations per class, attributed to the correct invariant | Fix oracle; repeat until 100 % |
| **G4 injector attestation** | Every armed injection has exactly one `fired` receipt and one `restored` receipt in `faults.log`; per-cell fire rate and exclusion rate computed from receipts only | Runs without receipts excluded and counted; cell flagged if exclusion > 20 % |
| **G5 ground-truth independence** | Java GT digest = Python regenerated digest for every pilot run; source-integrity gate passes | Invalidate runs; fix specification |
| **G6 configuration equality** | The shared config hash is identical across mechanisms; only the declared mechanism-intrinsic keys differ (§4.11) | HALT: unfair comparison |
| **G7 load-generator capability** | Generator sustains `≥ 3 ×` the maximum observed mechanism throughput at the canonical load; warm-up/quiescence calibration: doubling `T_quiet` does not change any count | Re-size generator / raise `T_quiet`; report |
| **G8 clock and host sanity** | Single boot per campaign; clock offset checks before/after each run; no sleep/thermal events recorded; process affinity/priority as configured; free disk above the pre-registered floor | Flag run; rerun if clock stepped |

### 7.7 Instrument configuration (recorded, pinned, audited)

| Instrument | Setting |
|---|---|
| Latency histograms | HdrHistogram, 3 significant digits, 1 µs..60 s, per run; raw histogram dump retained |
| Timestamps | `System.currentTimeMillis()` for schedule/commit (same host, single clock); monotonic `System.nanoTime()` for durations |
| JVM (all app instances) | identical flags recorded in the run record: fixed heap, fixed GC (`-XX:+UseG1GC`, fixed `MaxGCPauseMillis` omitted for realism), no JIT changes between mechanisms; fresh JVM per run for crash-fault runs |
| PostgreSQL | `synchronous_commit=on`, `fsync=on`, `full_page_writes=on`, `track_commit_timestamp=on` (optional diagnostic), fixed shared_buffers/work_mem, autovacuum settings recorded |
| Kafka | RF=3, `min.insync.replicas=2`, `unclean.leader.election.enable=false`, `transaction.state.log.replication.factor=3`, `offsets.topic.replication.factor=3`, `auto.create.topics.enable=false` |
| Process telemetry | per-run CPU time, peak working set, context switches for every app/broker/PG process (recorded, not analysed as a primary metric) |

---

## 8. Experiment design (rationale; the binding matrix and counts are in `docs/prereg.md`)

### 8.1 Stages

A1 §7 fixes the staged design; this section states its logic and leaves the exact matrices, N per cell and the compute projection to `docs/prereg.md` §2–§4.

| Stage | Purpose | Design | N |
|---|---|---|---|
| **P (pilot)** | calibrate timings, validate the oracle/injectors, measure per-run cost and mechanism throughput; produce the compute projection | all 9 mechanisms × 8 conditions (F0, F1, F2, F3, F4a, F4b, F5, F6) × 1k events × P=3 | 3 |
| **A** | **structural correctness** under deterministic crash points (Level 1) | 9 mechanisms × reachable K1–K7 classes (52 cells) + 9 control cells (one per mechanism; three of them are the gate controls of §7.6) × 1k events | 30 |
| **B** | **correctness under stochastic faults + realistic load** (Level 2) | 9 mechanisms × 8 conditions (67 executed cells; n/a cells with proof) × 100k events × P=6; sensitivity at 10k and at P∈{3,12} | 30 core, 10 sensitivity |
| **C** | **performance, fault-free** | 9 mechanisms × {10k,100k} × P∈{3,6,12}; 1M events at P=6; capacity block (saturation mode) at 100k/P=6 | 30; 1M and capacity at N=10 |
| **D** | **performance under faults + time-to-consistent-state** | reuses Stage B runs; no new runs | — |

**Never cut controls** (A1 §7): the G1/G2 control cells are outside the cut ladder. The cut order when the projected compute time exceeds the available budget: (1) drop the 1M load, (2) drop the partition-sensitivity blocks, (3) reduce repetitions to N = 20 for correctness cells — and only then, with explicit author approval, further coverage reductions (D1).

### 8.2 Randomisation, interleaving, seeds

- Runs are scheduled by a seeded random permutation of all cells, with the constraint that no more than `K_interleave = 3` consecutive runs share the same mechanism (interleaving across mechanisms), and that mechanisms are spread across days.
- `master_seed` is chosen once and recorded; per-run seed = `SHA-256(master_seed || stage || cell_id || rep)` → reproducible independent of order.
- The **same rep seed is used across mechanisms within a cell**, so correctness runs are *paired by experimental material* (identical event stream and identical injection schedule). This enables McNemar's exact test (§9) and reduces variance. It does not pair the wall-clock timing of injections, which depend on each mechanism's phase progress.
- `experiments/run_order.csv` records the realised order and is committed; the analysis checks for order effects (§9.6).

### 8.3 Reset protocol (fresh state per run)

Per run: stop consumer instances; create *fresh* topics (`wxeo.src.<run_id>`, `wxeo.sink.<run_id>`) and a fresh consumer group; create a fresh PostgreSQL schema and objects (`wxeo_run_<run_id>`); start consumer instances (fresh JVMs); start the injector supervisor; produce ground truth before the first event; run; quiesce; dump; stop; drop schema; delete topics; verify no residue (topic list, schema list, group list, PG sessions). Kafka and PostgreSQL *processes* are long-lived and periodically restarted every 50 runs (with a recorded restart record), because restarting the three broker JVMs and the database server per run would dominate wall-clock (A1 §7 note: "restarted components" becomes restarted native processes; the deviation is stated in `docs/prereg.md` §4 and §11, D6). A run is invalid if the residue check fails.

### 8.4 Per-run record

Every run writes one JSON record following the schema in `docs/prereg.md` §5 (identity, cell, seeds, config hashes, versions, phase timings, outcome counts, performance percentiles, quiescence, validity flags, artefact paths). Nothing in the paper is computed from anything else.

### 8.5 Compute projection and the cut ladder

The projection method is pre-registered in `docs/prereg.md` §9: the pilot measures `t_setup`, `t_load(R_gen, L)`, `t_drain`, `t_verify(L)` and restoration costs per fault class; the projected total is

```
T_total = Σ_cells N_cell · ( t_setup + t_load + t_fault_restore + t_drain + t_verify )
```

with the exact per-cell counts from the prereg matrices. The projection is written into `docs/prereg.md` **at P2** from measured pilot constants; no time value in this document is an estimate presented as a result. If `T_total` exceeds the author's available budget `H`, the cut ladder of §8.1 is applied mechanically and the deviations are logged (`results/DEVIATIONS.md`).

### 8.6 Deviations register

`results/DEVIATIONS.md` is generated, not hand-written: the runner diffs the committed planned matrix (`docs/prereg.md` tables / `experiments/matrix.yaml`) against the executed runs and emits one row per cell with `N_planned`, `N_valid`, `N_excluded`, `reason`, `date`. Policy: (i) no cell may be silently dropped — an absent cell becomes a row; (ii) every reduction cites a measured wall-clock fact from `results/RUNLOG.md`; (iii) the paper reports the deviations table and the truthful N per cell; (iv) exclusion-rate and non-fired-fault counts are part of the paper's results, not an appendix afterthought (A1 §11).

### 8.7 Native-process deviation (no containers) — stated, not hidden

The reported environment has no Docker and no WSL2 (BRIEF §9). Therefore: (i) "containers" become native Windows processes with recorded command lines, environment variables, working directories, process IDs, CPU affinity masks and priorities; (ii) "image digests" become SHA-256 digests of every vendored binary/archive and of every configuration file, recorded in the run record; (iii) process limits are recorded as (affinity mask, priority class, `-Xmx`, Hikari pool size, Kafka/PG config values), not cgroup limits; (iv) broker/DB restart fidelity is lower (JVM restart, no container kill semantics), a threat (T-I7); (v) all of this is recorded in `results/DEVIATIONS.md`, `docs/prereg.md` §11 and the paper's threats section.

---

## 9. Statistics plan (summary; the frozen, complete plan is `docs/prereg.md` §7)

Statistics are pre-registered and never chosen after seeing results (A1 §9). The analysis code is frozen and hashed before Stage A; anything not in the frozen plan is labelled **exploratory**.

### 9.1 Estimands and units

- **Correctness:** the proportion of *runs* with `violating_run = 1` per cell, and the per-invariant proportions. The run is the independent unit; pairs of runs sharing a rep seed across mechanisms form the paired unit for mechanism comparisons.
- **Performance:** per-run median throughput and per-run percentiles (p50, p99, p99.9 of per-event latency), with runs as the resampling unit. Pooled percentile estimates are descriptive only.

### 9.2 Correctness inference

- **Clopper–Pearson exact 95 % CI** for every proportion (violating-run rate, per-invariant rate). With 0 events in N runs the CI upper bound is reported from the exact binomial (CP) and, as a conservative summary, the **rule of three** (3/N) is also reported; the exact value is primary. "Zero observed" is never "proved" (A1 §9).
- **Exact tests**: McNemar's exact test for paired mechanism comparisons (same rep seed), Fisher's exact test as the unpaired sensitivity analysis. **Holm–Bonferroni** correction within pre-declared families; both unadjusted and adjusted p-values are reported.
- Pre-declared families (frozen in prereg §7): C1 = H1 comparisons; C2 = H3 comparisons; C3 = H4 correctness; C4 = zero-violation confirmation of M2/M3/M4/combinations; P1 = throughput, P2 = p99, P3 = time-to-consistent-state.
- **Decision rules**: pre-registered per hypothesis (`docs/prereg.md` §3), including what counts as *confirmed*, *refuted*, *inconclusive*. "Inconclusive" is a first-class outcome and is reported as such.

### 9.3 Performance inference

- Median + IQR per cell; **bootstrap 95 % CI**, B = 10,000 resamples, seed recorded, resampling unit = run (paired resampling for paired comparisons); percentile method primary, BCa as sensitivity.
- **Friedman test** across mechanisms, with **blocks = (load × partitions) within each stage, aggregated to block medians** (A1 §9): Stage C yields 6 blocks (2 loads × 3 partitions) × 9 mechanisms; the statistic, the block medians and the ranks are reported. A secondary family uses the three principal mechanisms (M1x, M2, M3, M4) over the same blocks.
- **Nemenyi post-hoc** with a critical-difference diagram at α = 0.05.
- **Cliff's delta** with bootstrap CI for every reported pair; thresholds fixed in advance: |δ| < 0.147 negligible, < 0.33 small, < 0.474 medium, otherwise large (Romano et al. conventions). These thresholds are pre-registered.
- Latency comparisons use run-level percentiles and Cliff's delta on paired or unpaired runs as appropriate; no comparison of different sinks (D13).

### 9.4 Zero-variance and all-zero cells

- Cells with all-zero counts are reported as "0 observed in N valid runs (excluded: E); exact CP 95 % upper bound U; rule-of-three bound 3/N; this bounds the rate, it does not prove absence". The pre-registered sentence template is in `docs/prereg.md` §7.6 so the paper cannot drift into stronger language.
- Cells whose confidence interval is too wide to separate the hypotheses are labelled inconclusive, not confirmed.
- Power statements: for N = 30 and a per-run violation probability p = 0.05, the probability of observing at least one violation is 1 − (1 − p)³⁰ ≈ 0.785; for p = 0.10 ≈ 0.958. This *limitation* is stated whenever a zero-event claim is made.

### 9.5 Hypothesis decision rules (summary)

- **H1** confirmed if M1x has ≥ 1 violating run in the deterministic K3 cells with attested injection and M1-kafka has 0 in every executed cell; refuted if M1x is clean in K3 with attested injection (or the injector cannot fire — then inconclusive).
- **H2** confirmed if (a) M2 has 0 violating runs in all Stage B fault cells (CP upper bounds reported) and (b) the performance penalty vs M3 and M4 is present in the pre-registered direction and magnitude (Cliff's |δ| ≥ small on throughput or p99, adjusted p < 0.05 in family P1/P2). If (a) holds and (b) does not, H2 is reported as *partially confirmed* (correctness component confirmed, cost component refuted). If (a) fails, refuted.
- **H3** confirmed if M3 has 0 I3 violations in all cells and ≥ 1 I4 violation in the K4/F4b probe cells with attested injection; refuted if M3 shows no loss with an attested probe (or the probe cannot be realised — then inconclusive and the ordering realisation decision D2 governs).
- **H4** confirmed if M4 and M4+M3 have 0 violating runs in every reachable Stage A/B cell and M4's latency is lower than M2's with the pre-registered effect size; partial if correctness holds and latency does not.
- **H5** confirmed if no mechanism is simultaneously (i) non-violating in every executed cell and (ii) non-inferior on all performance metrics; the formal dominance test is described in P12 and prereg §7.8.
- **Exploratory**: everything else (e.g., divergence frequency, publication-integrity rates, per-fault fire rates as explanatory variables) is labelled exploratory in the paper.

### 9.6 Independence, autocorrelation, and order effects

Runs are randomised and interleaved (§8.2). The analysis checks: (a) lag-1 autocorrelation of throughput residuals within a stage and Durbin–Watson for run-order drift; (b) Spearman correlation between the metric and the chronological run index; (c) day/block effects. If |ρ| is significant at α = 0.05, the exploratory analysis adds run order as a covariate and reports it; the pre-registered primary analysis is unchanged. Between-run contamination is prevented by the fresh-state protocol (§8.3) and checked by the residue check.

### 9.7 Analysis freeze

Before Stage A, the analysis and figure scripts are committed and hashed (`docs/prereg-freeze.json`): the oracle, the metric computation, the statistical tests, the figure generators. Changing any of them after freeze is allowed only through `results/DEVIATIONS.md` and the change is flagged in the paper. Data files are append-only; no run is deleted, only marked invalid with a reason.

---

## 10. Threats to validity

Each threat has a concrete mitigation and a residual risk that will be stated in the paper. Codes are referenced from `docs/prereg.md` §10 and from the paper's threats subsection.

### 10.1 Internal validity

| ID | Threat | Mitigation | Residual risk |
|---|---|---|---|
| **T-I1** | Harness defect produces or hides violations | G1/G2 controls; G3 mutation testing of the oracle (100 % detection); independent oracle; config-hash audit; residue checks | An undetected defect that is *consistent* across mechanisms would not be caught by controls; mitigated by the Oracle's independence and S5's review |
| **T-I2** | Fault did not fire, inflating apparent safety | `faults.log` attestation, per-cell fire rate and exclusion rate, invalid-run counting (G4) | A fault can fire but land in a harmless phase (especially Level 2 random timing); this is why structural claims rest on Level 1 |
| **T-I3** | Unequal tuning / configuration bias | §4.11 fairness contract; G6 config hash; equal tuning protocol; deviations log | Mechanism-intrinsic components (index, relay, progress table) are inherently unequal; the paper separates *intrinsic* cost from tuning |
| **T-I4** | Order drift (machine slowdown over days) confounds mechanism with time | randomised blocked order, interleaving, autocorrelation checks (§9.6) | Gradual thermal/IO drift within a stage; covariate analysis is exploratory only |
| **T-I5** | Single-host scheduling produces timing noise and CPU contention | fixed process affinity/priority, one host idle except the stack, per-run process telemetry, interleaving; performance claims based on medians and CIs, not single runs | Event-loop jitter from the Windows scheduler remains; the paper reports it as a fidelity limitation |
| **T-I6** | Observer/instrumentation effects (phase markers, failpoints, logging) | markers only at batch boundaries and buffered; failpoints compiled behind a flag and measured in a pilot A/B with the flag off; per-run overhead recorded | Small overhead may still bias short runs; 1k-event runs are the most exposed, so Stage A timing is not used for performance claims |
| **T-I7** | Native-process fault fidelity differs from container/VM kills | process kills are real (`taskkill /F`), Toxiproxy cuts are real TCP; all versions/configs pinned and recorded | A native kill leaves no cgroup/VM-level state; considered unlikely to matter for the modelled durability properties, but stated |
| **T-I8** | Cloud-synced working directory / background sync perturbs I/O | run artefacts written to a local non-synced scratch path and copied into the repository afterwards; sync paused during measurement; path policy recorded in prereg (D11) | Copy step must be verified; any missing artefact invalidates the run |

### 10.2 Construct validity

| ID | Threat | Mitigation | Residual risk |
|---|---|---|---|
| **T-C1** | Single-host Kafka replication is not a real cluster | 3 real broker processes with RF=3/min.insync=2; failure injector kills a real leader/coordinator | Shared CPU/memory/disk failure domain weakens "node failure" realism (T-E1) |
| **T-C2** | One record = one balanced entry; no multi-leg or cross-partition entries | scope statement; the pipeline processes one entry per record, which is a real pattern | Multi-leg entries change batching and atomicity; paper states the restriction and does not generalise |
| **T-C3** | Simplified ledger: one currency, no reversals/refunds, no balances, no account state machine | stated scope; invariants I1–I4 cover the studied properties | Practitioner reconciliation semantics (e.g., reversal-based corrections) are not exercised |
| **T-C4** | Oracle relies on the business `event_id` being propagated; a surrogate-key ledger would hide I3 | schema propagates `event_id`; the paper states the practitioner warning | A production ledger that assigns surrogate ids would need external evidence to detect duplicates |
| **T-C5** | M1-kafka's sink is Kafka while others' is PostgreSQL — comparing them on "invariant violations" compares different media | M1-kafka is used as a **positive control**, not as a performance competitor; cross-sink comparisons are labelled construct-limited (D13) | A reader may over-read the positive control as a mechanism ranking |
| **T-C6** | Publication stream treated as auxiliary for DB mechanisms but as the ledger for M1-kafka | explicit two-metric policy (§2.7); secondary publication metrics reported separately | Mechanism comparison on publication is asymmetric by construction and is stated |
| **T-C7** | Fault realisations (esp. F4b early ordering, external rewind) are our design choices, not natural faults | each realisation is declared, justified, and its reachability proved; alternatives are in §6.7/D2/D5 | Verdicts for F4b/K4 depend on a probe that a reviewer may call artificial; the paper reports both canonical (n/a) and probe results |

### 10.3 External validity

| ID | Threat | Mitigation | Residual risk |
|---|---|---|---|
| **T-E1** | Single host vs multi-AZ cluster; no real node/network failures | 3 broker processes, real TCP faults, real PG; threat stated | Node-failure timing, rack/AZ failure modes and clock/network partitions are not reproduced |
| **T-E2** | Single PostgreSQL instance; no HA, no failover, no replica lag | `synchronous_commit=on` and single-node durability stated; failover is out of scope | Real deployments with async replicas can lose committed transactions — which would *strengthen* the paper's warning but is not measured; stated as scope |
| **T-E3** | Version specificity (Kafka, PostgreSQL 16, JDK 21, Windows 11) | versions and digests pinned and recorded; claims scoped to the tested versions | Behaviour may differ on other versions/OSes (e.g. fsync semantics, JVM GC) |
| **T-E4** | Synthetic load (uniform random accounts/amounts, fixed rates, no skew, no bursts) | open-loop fixed rate, deterministic distribution, documented generator semantics | Real workloads have skew, hot accounts, bursts and day cycles; the paper states that the *violation mechanism* is load-independent in the model but the *frequency* is not |
| **T-E5** | Scale (≤ 1M events) vs production volumes | scaling study to 1M; the mechanisms' guarantees are scale-independent in the model | Long-run behaviours (compaction, retention, index bloat, GC over hours) are not observed |
| **T-E6** | No TLS/SASL, no schema registry, no connector framework | stated as out of scope; all mechanisms share the simplification | Practitioner stacks with those components have different failure surfaces |

### 10.4 Conclusion validity

| ID | Threat | Mitigation | Residual risk |
|---|---|---|---|
| **T-N1** | Multiple comparisons inflate false positives | pre-declared families + Holm; both raw and adjusted p reported; Nemenyi controls for Friedman | With many cells, some adjusted tests may be underpowered; reported as inconclusive |
| **T-N2** | Zero-event claims over-read as proof | CP upper bounds + rule-of-three + power statements; exact pre-registered language | A rare violation (p < 3/N) remains undetectable at this N; stated |
| **T-N3** | Non-independence of runs on one host | randomised interleaving; autocorrelation diagnostics; paired seeds | Correlation across runs can shrink CIs; the analysis reports the diagnostics |
| **T-N4** | Selection of exclusions | exclusion only for pre-declared validity causes (non-fired faults, residue, clock, oracle disagreement), decided without outcome data (G4) | Any exclusion can still bias; the exclusion rate is reported per cell and the paper states the bound |
| **T-N5** | Degrees of freedom in analysis / post-hoc changes | analysis code frozen and hashed; deviations logged; exploratory analyses labelled | A reviewer cannot fully verify the freeze; the commits and hashes make it auditable |

### 10.5 Reproducibility

| ID | Threat | Mitigation | Residual risk |
|---|---|---|---|
| **T-R1** | Seed/version drift | every run records seeds, versions, digests, config hashes; `results/RUNLOG.md` + raw records committed | Bit-identical re-execution on other hardware is not claimed |
| **T-R2** | Artefacts too large to commit | curated raw records + compressed dumps + digest-first policy (prereg §9); figures regenerate from committed data | Some raw dumps may be aggregated rather than stored whole; the aggregation script is part of the frozen analysis |
| **T-R3** | Windows and cloud-synced-directory path deviations | deviations documented; local scratch policy (D11); commands recorded verbatim | The native-process path is less reproducible than a container image; the run record's version/digest block is the mitigation |
| **T-R4** | TLA+ model not checked | if delivered, `formal/ledger.tla` is labelled unverified until TLC runs at P2; the paper reports bounds only if checked | An unchecked spec has no formal weight and is presented as an appendix artefact, not a result |

---

## 11. Practitioner decision table (schema and derivation rules; values are filled from results only)

The table answers RQ3 in the form a practitioner needs: *for a given failure exposure, what is the minimal sufficient mechanism set?* The **model answer** column is derived from §6.6 and is available now (the author's requirement: RQ3 is answered by the model). The **measured corroboration** and **residual-rate** columns are filled by the frozen analysis script from run records only; no value may be hand-entered, and empty cells must remain visibly empty ("—") until then.

### 11.1 Table

| Exposure class | Failure story | Model-minimal sufficient set (from §6.6) | Measured: violating-run rate per mechanism (CP 95 % CI) | Structural certification (deterministic adverse cells all clean, injectors attested) | Residual open risk (model) |
|---|---|---|---|---|---|
| **E1** W1 duplicates | crash/redelivery between effect and progress | M3 (index) or M4 (atomic progress) | — | — | none in model |
| **E2** W2 losses | progress durable before effect | M4 (internalised ordering) | — | — | mechanisms relying on the "strict ordering" obligation remain exposed if that obligation is broken |
| **E3** zombie writer | paused owner resumes after reassignment | M3 or M1 family (fencing) | — | — | M4 alone open |
| **E4** unknown-outcome retries | DB/Kafka commit acknowledgement lost | M3 or M4 | — | — | none in model |
| **E5** internal exactly-once publication | stream records must be exactly-once | M1 family (in-transaction) | — | — | outbox relay is at-least-once only (trace X7) |
| **E1∧E2∧E3∧E4** | full ledger exposure | **M4+M3** | — | — | — |
| **E1..E4 ∧ E5** | ledger + internal EOS | **M1x+M3** | — | — | — |
| **Cost-sensitive ledger only** | throughput matters more than ordering obligation | **M3** (cheapest) | — | — | E2 only via the ordering obligation |

### 11.2 Derivation rules (frozen)

- **D-R1 (model column).** Filled from §6.6 before measurement; it is a *model consequence*, not a measurement. If the experiments contradict it, the model or its assumptions are wrong and the paper must say which (that is a result, not a table edit).
- **D-R2 (measured corroboration).** For each exposure class `E`, the set of cells `Cells(E)` is pre-registered (prereg §7.7): E1 → K2/K3/K5 + F2/F4a/F6; E2 → K4 + F4b; E3 → K6; E4 → K2/C9-class cells + F5; E5 → publication metrics in M1x/M2 cells. A mechanism is "observed consistent with sufficiency" for `E` iff it has 0 violating runs in **all** `Cells(E)` with every injector attested and exclusion rate ≤ 20 %; the CP upper bound is reported next to the claim.
- **D-R3 (structural certification ≠ statistical proof).** "Certified" means: the model proof (P2/P3 and the corresponding closure in Lemma 2) plus zero violations under *deterministic adverse* injection in every cell of the class, with all injections attested. The statistical bound is reported alongside; it is never the basis for the word "certified" (a 0/30 CP bound is ≈ 11.6 %, which no practitioner should read as proof).
- **D-R4 (practical threshold θ).** θ is used **only** to decide whether a *residual* open exposure (one the model says is open) may be accepted in practice: if the model says the exposure is open **and** the measured upper bound is below θ, the table reports "empirically absorbed; model exposure remains" — never "closed". θ itself is an author decision (D12); the draft proposes θ = 1 % per-run violating rate and notes the design's resolution limit.
- **D-R5 (no invented cells).** The analysis script writes the table; every number traces to `results/analysis/decision-table.csv`, generated from run records by the frozen pipeline. Cells without a measured value stay "—".
- **D-R6 (scope statement).** The table is valid only for the assumptions AS1–AS13 and the tested versions; the paper's caption repeats this.

---

## 12. Open questions, self-review checklist, and completeness proof

### 12.1 Open questions for the author / lead

| ID | Question | Why it blocks | Proposed default |
|---|---|---|---|
| Q1 | What is the available wall-clock budget `H` for Stages A–C, and over how many calendar days? | fixes N and coverage via the cut ladder (§8.5, D1) | none — author must supply |
| Q2 | Is the optional TLA+/Alloy model required at P1, or may it be delivered (checked or unchecked) at P2? | FO-1/FO-6; toolchain not vendored | deliver `formal/ledger.tla` as an unverified draft at P1; check at P2 if tooling is added |
| Q3 | Which M2 realisation is adopted (inbox+outbox / inbox-only / outbox-only)? | D3; changes predictions for M2 and the combinations | inbox+outbox (§4.5) |
| Q4 | Is the `ordering` probe admitted to make H3's loss arm testable? | D2; without it H3's loss component is untestable | admit the flag, applied to all mechanisms |
| Q5 | Is F6 a protocol-level duplicate delivery or a semantic rewind of each mechanism's resume point? | D5; changes M4/M1-kafka predictions | protocol-level replay |
| Q6 | Are injectors targeted by semantic role (effect-store/progress-store) or literally per A1's wording (with n/a cells)? | D4; determines n/a coverage and exclusion rates | semantic role-based, with literal n/a cells documented |
| Q7 | Is I1 structural+sum or sum-only? | D8; affects what "I1 violation" means in the paper | structural+sum (I1a∧I1b) |
| Q8 | Ground-truth storage and local scratch path policy? | D10/D11; disk headroom is ≈ 14 GB | write-verify-delete with digest; local scratch + copy |
| Q9 | Is M1-kafka run through Stages B and C (full cost) or only Stage A + controls? | D1/D13; cost and construct comparability | Stage A + controls only is defensible; nominal plan keeps all stages for A1 compliance |
| Q10 | What is θ for the residual-exposure rule, and is "certified" language allowed? | D12; decision table wording | θ = 1 %; "certified" only per D-R3 |
| Q11 | Page budget for the formal section and appendices? | FO-1/FO-5; RITA page limit | pen-and-paper lemmas + traces; TLA+ only as appendix if checked |
| Q12 | Who signs P1 and P2, and on what date? | A1 §12 hard stops | author signs both; no code before P1 |

### 12.2 Self-review checklist (completeness proof for the P1 deliverable)

Each item names the section where it is discharged; a reviewer can verify the document is complete by checking each box.

- [x] **1. Problem, scope, system model** — §1 (architecture, schemas, determinism, scope, notation).
- [x] **2. Formal ledger model** — §2: state, entries/postings, I1–I4 with structural + liveness clauses, observation function, exact SQL predicates (P1–P8), counts, violation taxonomy, why I1–I4 are orthogonal.
- [x] **3. EOS boundary** — §3: covered transition set `EOS(T)`, external-store boundary, Lemma 1 (two windows W1/W2), Lemma 2 (closure table), assumptions AS1–AS13, closure options O-1…O-4.
- [x] **4. Mechanisms** — §4: M0, M1-kafka, M1x, M2, M3 (with ordering-variant options), M4, M2+M3, M4+M3, M1x+M3; pseudocode for each; commit ordering, retry semantics (§4.10), distinctness matrix (§4.9), omitted combinations justified (§4.8), fairness contract (§4.11).
- [x] **5. Crash-point model and injectors** — §5: C1–C12 enumerated with durable state and window; equivalence classes K1–K7 with proofs; reachability matrix (52 executed cells); F1–F6 mapping; two injection levels with semantics; injector specifications (object, primitive, timing, determinism, restoration, receipt); `faults.log` attestation and exclusion policy.
- [x] **6. Propositions and prediction tables** — §6: P1–P12 each falsifiable with a named measurement; eight concrete counterexample traces; **72-cell** mechanism × fault table with one-line justification codes (P = 50, C3 = 1, V3 = 10, V4 = 6, n/a = 5); **63-cell** mechanism × crash-class table (52 executed); hypothesis → proposition → measurement map; RQ3 minimal sets; formal-part options FO-1…FO-6.
- [x] **7. Metrics, oracle, quiescence, harness validation** — §7: exact metric definitions; open-loop latency/throughput with warm-up and steady state; time-to-consistent-state with censoring; independent oracle architecture (GT before send, source-integrity gate, cross-implementation digest); quiescence; gates G1–G8 including 100 % mutation-testing requirement; instrument configuration.
- [x] **8. Experiment design** — §8: stages P/A/B/C/D with purpose and N; randomisation/interleaving/paired seeds; reset protocol; per-run record pointer; compute-projection method; deviations register policy; native-process deviation.
- [x] **9. Statistics** — §9: Clopper–Pearson, rule of three, exact tests + Holm, bootstrap (B = 10,000, recorded seed), Friedman blocks = load×partitions, Nemenyi, Cliff's delta thresholds, power statements, zero-event reporting language, independence checks, analysis freeze. Full frozen detail in `docs/prereg.md` §7.
- [x] **10. Threats to validity** — §10: internal, construct, external, conclusion, reproducibility; each with mitigation and residual risk.
- [x] **11. Practitioner decision table** — §11: schema, model-minimal sets, frozen derivation rules D-R1…D-R6, empty measurement cells.
- [x] **12. Open questions and checklist** — §12 (this section).
- [x] **No fabricated numbers** — every number in this document is either an arithmetic count of planned runs (a design fact, e.g. 52/63/72 cells), a pre-registered constants (α, B, thresholds), or a symbol to be filled from the pilot; no measurement or estimate is presented as a result.
- [x] **Confidentiality firewall** — no employer/product/host/personal identifiers appear; the document is public-artefact safe (BRIEF §3).
- [x] **No code written** — only specifications, pseudocode, SQL predicates and formal notation; implementation belongs to S3 after P1.
- [x] **Author ownership of the formal part** — §6.1 and §6.7 are explicitly drafts; the author decides (A1 §3).
- [x] **Every decision requiring approval is labelled** — §13 (D1–D15) and repeated in the P1 report.

---

## 13. DECISIONS REQUIRING AUTHOR APPROVAL

Per A1 §0, every methodological decision below needs the author's explicit approval; nothing here takes effect by default. Each entry states the options, the trade-off, the draft recommendation, and the cost in planned runs or artefact work. Recommendations are drafts, not decisions.

### D1 — Campaign budget and coverage package
**Context:** the nominal A1 design implies several thousand runs across Stages A–C (counts in `docs/prereg.md` §2–§4); the projected wall-clock depends on pilot-measured per-run times. **Options:** (a) nominal A1 matrix, all 9 mechanisms, all stages; (b) apply the A1 cut ladder (1M load → partition sensitivity → N ≥ 20); (c) further reduce coverage (e.g. drop M1-kafka from Stages B/C, drop the 100k sensitivity blocks) with explicit deviation rows. **Recommendation:** (b) as the default; (c) only if the pilot projection still exceeds `H`. **Cost:** (a) is the most expensive; (b) reduces runs mechanically; (c) needs a deviation row per removed cell and weakens H2/H4 performance claims. **Author must supply:** `H` (available wall-clock) and the measurement window dates.

### D2 — Realising the W2/K4 fault (H3's loss arm)
**Context:** H3's loss arm requires a state where progress is durable and the effect is not. Strict-order mechanisms cannot reach it by environment faults alone (Lemma 1). **Options:** (a) `ordering ∈ {strict, early}` flag on every two-commit mechanism, K4/F4b executed in the `early` variant [recommended]; (b) a dedicated M3-eo variant only; (c) adversarial external progress advance (AdminClient offset commit / SQL progress-row update) then kill. **Trade-off:** (a) is uniform and testable but adds one variant dimension and a small implementation surface; (b) is minimal code but invites the unfair-tuning critique; (c) needs no code change but models operator error rather than a code path. **Cost:** (a) ~8 K4 cells × N and a flag in S3's code; (c) no code change, weaker construct validity.

### D3 — M2 realisation for a DB sink
**Context:** "transactional outbox" is conventionally a DB→Kafka pattern; for a Kafka→DB pipeline several realisations are defensible. **Options:** (a) inbox claim + outbox publication log + relay, ledger transaction atomic [recommended]; (b) inbox only (nearly indistinguishable from M4+M3 → confounded comparison); (c) outbox only (predicted I3 violations on ordinary redelivery → a straw man). **Trade-off:** (a) is the only one that is both distinct from the other mechanisms and sufficient for H2; it costs an extra table write and a relay thread (which is exactly the "cost" H2 predicts). **Cost:** relay implementation in the artifact (S3).

### D4 — Injector targeting: literal vs semantic-role
**Context:** F4/F5 are defined in A1 by external-store names ("DB commit", "app↔DB"). For M1-kafka there is no DB, and for M4 the Kafka offset is not the progress source. **Options:** (a) literal targets per A1 wording, with n/a cells for mechanisms that have no such target [fits A1 text; produces 5 n/a cells in the fault table]; (b) semantic-role targeting (effect store / progress store / publication link instantiated per mechanism) [uniform treatment; needs a role-instantiation table]; (c) both, with (b) as sensitivity. **Recommendation:** (a) for Stages B/C (literal, matches A1) plus the role-instantiation table as documentation; (b) used in Stage A K-classes where the class is semantic. **Trade-off:** (b) avoids meaningless injections but invites "you simulated a different fault" critiques; (a) leaves n/a cells that must be explained in the paper.

### D5 — F6 semantics and the partial-batch probe
**Context:** "duplicate redelivery after rebalance" can be realised as (i) protocol-level replay of uncommitted offsets, or (ii) a rewind of the mechanism's own resume point. (ii) would make M1-kafka duplicate in its own sink (EOS is not idempotent against offset rewinds) and would make M4 duplicate. **Options:** (a) protocol-level replay only [recommended; matches A1's wording; keeps M1-kafka's positive-control role intact]; (b) protocol-level + a clearly-labelled exploratory "external rewind" fault; (c) semantic rewind as the primary F6. **Trade-off:** (a) is the cleanest test of W1; (b) documents an important limitation of EOS; (c) would break the §7.6 positive-control gate. **Cost:** (b) adds one exploratory cell family; (a) none.

### D6 — Reset protocol
**Context:** A1 §7 says "fresh state per run (new topic, empty schema, restarted components)". Restarting three broker JVMs and PostgreSQL per run would dominate wall-clock. **Options:** (a) fresh logical state per run (new topics, new group, new PG schema, fresh consumer JVMs) + periodic full-stack restart every 50 runs, all recorded [recommended]; (b) full process restart per run (faithful to the wording, prohibitively expensive); (c) logical reset only, no periodic restart. **Trade-off:** (a) is the only feasible option; it is a declared deviation and must be stated in `results/DEVIATIONS.md`, prereg §11 and the paper. **Cost:** (a) ≈ one restart per 50 runs; (b) impossible within any plausible budget.

### D7 — Stage B sensitivity interpretation
**Context:** A1 §7 says "sensitivity at 10k events and 3/12 partitions, N=10" without specifying which factor is varied at which load. **Options:** (a) load-sensitivity block at 10k/P=6 and partition-sensitivity block at 100k/P∈{3,12} [literal reading, expensive]; (b) both sensitivity blocks at 10k (cheaper, isolates factor effects from load); (c) drop sensitivity if D1 cuts apply. **Recommendation:** (b) if the budget allows, (c) otherwise; (a) only if the projection is comfortable. **Cost:** 720 runs (a: 1,440) at N=10; (c) zero.

### D8 — I1 formalisation
**Options:** (a) I1 = structural well-formedness ∧ signed sum zero, reported as I1a/I1b [recommended]; (b) I1 = signed sum zero only, literally as A1 §3. **Trade-off:** (b) is exactly as written in A1 but is invariant under whole-entry duplication and under a 4-leg mangled entry; (a) detects the mangled/partial cases and makes the paper's duplicate-vs-balance distinction explicit. **Cost:** none (predicates are already written); affects the wording of the formal section.

### D9 — I4 formalisation
**Options:** (a) end-state completeness at quiescence, oracle-computable [primary, recommended]; (b) temporal "eventually, under fair recovery", TLA-checkable; (c) both, with (a) presented as the measurement of (b). **Trade-off:** (b) is needed for the formal liveness claim and matches A1's "liveness under fair recovery" wording; (a) is the only experimentally falsifiable form. **Cost:** (c) adds a fairness assumption and a TLA obligation; (a) is sufficient for the experiment.

### D10 — Ground-truth artefact storage
**Context:** ground truth must be written before sending (A1 §4), but storing it for every run would consume tens of GB (100k-event runs × ~2,000 runs) against ≈ 14 GB free disk. **Options:** (a) write and fsync the GT file, hash it, verify against it, keep only the digest + manifest + counts, delete the file after verification [recommended]; (b) retain all GT files (infeasible on this disk); (c) regenerate only, without writing a pre-send file (violates the A1 ordering requirement). **Trade-off:** (a) satisfies the ordering requirement and is auditable via the digest, but a third party cannot inspect the full GT without re-running the derivation (which is deterministic and documented). **Cost:** disk-bounded; no analytical cost.

### D11 — Local scratch path and copy policy
**Context:** the working directory is cloud-synced; background sync and quota can perturb I/O and consume space. **Options:** (a) run artefacts written to a local non-synced scratch directory, curated results copied into `results/raw/` after each run, with a digest manifest [recommended]; (b) all artefacts written into the synced working directory [I/O interference and quota risk]; (c) scratch only, no copy [results not in the repo → violates BRIEF §8]. **Trade-off:** (a) adds a copy/verify step (which must be gated: missing artefact → run invalid); (b) risks contaminated timing; (c) violates reproducibility. **Cost:** small harness work; deviation documented.

### D12 — Practical-adequacy threshold θ and certification language
**Context:** the decision table needs a rule for residual exposures; a 0/30 exact upper bound is ≈ 11.6 %, so statistics alone cannot certify a 1 % rate. **Options:** (a) θ = 1 % per-run violation rate, with "certified" reserved for model-proof + deterministic adversarial cleanliness (D-R3) [recommended]; (b) θ = 5 %; (c) no threshold — structural claims only, no residual acceptance language. **Trade-off:** (a) is honest and useful; (b) is weaker but easier to satisfy; (c) is the most conservative but less useful to practitioners. **Cost:** none; affects §11 wording.

### D13 — M1-kafka comparability
**Context:** M1-kafka's sink is Kafka, the others' is PostgreSQL; comparing them on performance or on "invariant violations" compares different media. **Options:** (a) M1-kafka as positive control only, excluded from cross-mechanism performance comparisons [recommended]; (b) include it in all comparisons with a construct-limit caveat; (c) include only in Stage A. **Trade-off:** (a) protects construct validity and saves runs (H1 still tested via M1kafka-vs-M1x correctness contrast); (b) gives a Kafka-sink performance datapoint at the cost of an apples-to-oranges comparison; (c) is cheapest. **Cost:** (a) saves the M1-kafka Stage B/C runs; (b) nominal cost.

### D14 — Configuration and fairness contract sign-off
**Context:** A1 requires equal tuning and identical Kafka/PG settings; the shared/intrinsic split in §4.11 and Appendix C must be approved before implementation. **Options:** (a) adopt the tables as drafted [recommended]; (b) author revises specific keys (e.g. batch size, pool size); (c) add a tuning-equalisation procedure (per-mechanism optimisation passes, logged) — expensive and hard to audit. **Trade-off:** (a) is auditable via config hash (G6); (b) may be needed if the author knows production-like settings; (c) is the hardest to defend. **Cost:** none for (a); (c) multiplies pilot work.

### D15 — Paired seeds across mechanisms
**Context:** using the same rep seed for all mechanisms within a cell enables McNemar's paired test and reduces variance, but the *timing* of stochastic faults still differs per mechanism. **Options:** (a) paired seeds design [recommended]; (b) independent seeds per mechanism (Fisher only). **Trade-off:** (a) gives more power for the same N and is reported with McNemar + Fisher sensitivity; (b) is simpler but less powerful and cannot control run-to-run material variance. **Cost:** none.

---

## Appendix A — SQL bundle (normative predicates and observation protocol)

```sql
-- A.1 Reference set Ω (loaded from the ground-truth dump / source dump; temporary, per run)
CREATE TEMP TABLE expected (
  event_id     text     NOT NULL,
  leg_no       smallint NOT NULL,
  account_id   text     NOT NULL,
  side         char(1)  NOT NULL,
  amount_minor bigint   NOT NULL,
  currency     char(3)  NOT NULL,
  PRIMARY KEY (event_id, leg_no)
);
-- Loaded with COPY (FORMAT csv) from the checker's regenerated expected set.
-- The load is verified against the pre-send SHA-256 (oracle_<run>.json records both digests).

-- A.2 Observation snapshot: application stopped, then
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
-- run P1..P8 of §2.5 in this transaction; capture counts and key lists
COMMIT;

-- A.3 Append-only enforcement attestation (per run, on the run schema)
SELECT has_table_privilege(current_user, 'posting', 'UPDATE') AS can_update,
       has_table_privilege(current_user, 'posting', 'DELETE') AS can_delete;
SELECT count(*) AS posting_triggers
FROM pg_trigger WHERE tgrelid = 'posting'::regclass AND NOT tgisinternal;

-- A.4 Durable-progress snapshot (mechanism-dependent)
-- M4/M4+M3:
SELECT partition, next_offset FROM consumer_progress ORDER BY partition;
-- Kafka-offset mechanisms (via AdminClient, recorded in the run record):
--   committed offsets per partition, log-end offsets per partition, lag per partition.

-- A.5 Publication-stream dump for M1-kafka / auxiliary audits (Kafka consumer, read_committed)
--   key = event_id, value = entry; one line per record; duplicates counted by key multiplicity.
```

## Appendix B — Injector command reference (native processes)

| Fault | Command / API (Windows native; exact paths pinned in the run record) |
|---|---|
| F1 broker kill | `Stop-Process -Id <broker_pid> -Force` (equivalently `taskkill /F /PID <pid>`); restart with the pinned `kafka-server-start.bat` invocation; poll AdminClient until broker count = 3 and ISR = 3 |
| F2 consumer kill | `Stop-Process -Id <app_pid> -Force`; restart the same instance with the pinned JVM command line |
| F3 produce timeout | `toxiproxy-cli toxic add kafka-produce --type timeout --attribute timeout=5000` … `toxiproxy-cli toxic remove kafka-produce --name <toxic>` (verify flag names against the vendored Toxiproxy version) |
| F4a offset-commit failure | `toxiproxy-cli toggle kafka-consume` (disable), wait `T_hold`, `toggle` again |
| F4b ordering probe | app flag `-Dwxeo.ordering=early` + DB-link toxic/cut; failpoint `hook:before_effect_commit` → `crash` or `abort` |
| F5 DB partition | `toxiproxy-cli toggle db-link` (blackhole) or `toxic add db-link --type reset_peer`; restore and verify readiness |
| F6 progress rewind | Kafka: `kafka-consumer-groups.bat --bootstrap-server … --group … --reset-offsets --shift-by -<k> --execute`; M4: `UPDATE consumer_progress SET next_offset = next_offset - <k> WHERE partition = :p;` (recorded, bounded, and only while consumers are stopped) |
| Level-1 hooks | app failpoints behind `-Dwxeo.failpoints.enabled=true`: `after_fetch`, `before_effect_commit`, `after_effect_commit`, `before_progress_commit`, `after_progress_commit`, `before_publish`, `before_relay_mark`; actions `crash`, `pause(ms)`, `abort_txn`, `throw` |

Every action writes its receipt to `results/faults.log` (§5.6) and never to the app's transactional path.

## Appendix C — Configuration tables (pinned; recorded in every run record)

**C.1 Kafka (3 combined KRaft nodes; identical except `node.id`, ports, log dir):** `process.roles=broker,controller`; `node.id∈{1,2,3}`; `controller.quorum.voters=1@127.0.0.1:9093,2@127.0.0.1:9193,3@127.0.0.1:9293`; `listeners=PLAINTEXT://:9092,CONTROLLER://:9093`; `log.dirs=<local-scratch>/kafka/node<id>`; `default.replication.factor=3`; `min.insync.replicas=2`; `unclean.leader.election.enable=false`; `transaction.state.log.replication.factor=3`; `transaction.state.log.min.isr=2`; `offsets.topic.replication.factor=3`; `auto.create.topics.enable=false`; `num.network.threads=3`; `num.io.threads=8`; remaining values at the distribution defaults; version and SHA-256 recorded.

**C.2 PostgreSQL 16:** data directory on local scratch; `shared_buffers=2GB`; `work_mem=32MB`; `maintenance_work_mem=512MB`; `max_connections=100`; `fsync=on`; `synchronous_commit=on`; `full_page_writes=on`; `wal_level=replica`; `max_wal_size=4GB`; `checkpoint_timeout=15min`; `autovacuum=on` (defaults); `track_commit_timestamp=on` (optional diagnostic, one restart); `random_page_cost=1.1`; `effective_cache_size=4GB`; session role for the app limited to `INSERT, SELECT` on `posting`.

**C.3 Application JVM (all instances):** vendored JDK 21; `-Xms2g -Xmx2g -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError`; identical for every mechanism; fresh JVM per run for crash-fault runs; exact command line and `java -version` recorded.

**C.4 Consumer / producer / JDBC:** as §4.11; every key recorded in the run record's `config` object, and its SHA-256 used by gate G6.

## Appendix D — Cross-reference map

| Proposition | Counterexample/trace | Primary cells | Gate |
|---|---|---|---|
| P1 EOS boundary | X4 | Stage A K3 for M1x; divergence metric | G1/G2 |
| P2 W1 closure | X1, X2 | Stage A K2/K3 all mechanisms | G1 |
| P3 W2 closure | X3, X8 | Stage A K4 (probe), Stage B F4b | G4 |
| P4 M0 negative control | X1 | K3, K5, one Level-2 cell | G1 |
| P5 M1-kafka positive control | — | all M1-kafka cells | G2 |
| P6 M1x failure | X1, X4 | K2/K3, F1/F2/F4a | G1 |
| P7 M2 sufficiency | X7 | all M2 cells incl. K7 | G2 (control cells) |
| P8 M3 dedup/loss | X2, X3 | K3/K5/K6 vs K4 | G1/G4 |
| P9 M4 atomicity/zombie | X5, X6 | K3 (n/a), K6 | G1 |
| P10 combinations | X6, X7 | Stage A/B combination cells | G1 |
| P11 balance blindness | X1 | `v3=1 ∧ v1=0` count | G3 (mutation test) |
| P12 no dominance | — | Stages B/C combined | analysis freeze |

---

## 14. READINESS VERDICT

**Verdict: the method document is complete for the P1 gate, but implementation is NOT ready to start.** Status is *P1-pending-author-approval*. The following are the concrete blockers, in order:

### 14.1 Blockers for P1 (author approval)

| # | Blocker | Owner | Resolution |
|---|---|---|---|
| **B1** | Decisions D1–D15 are unapproved. A1 requires explicit approval of every methodological decision. | author | approve/revise each D; the document is not binding until then |
| **B2** | Available wall-clock `H` (D1) is unknown, so N and coverage cannot be frozen; the cut ladder cannot be applied. | author | supply `H` and the measurement window |
| **B3** | H3's loss arm is untestable unless D2 (ordering probe) is approved; otherwise the paper must report H3's loss component as inconclusive. | author | decide D2 |
| **B4** | M2's realisation (D3) changes the mechanism's code and its predictions; H2 cannot be implemented before the choice. | author | decide D3 |
| **B5** | Injector targeting (D4/D5) determines the n/a cells and the exclusion policy; the prereg matrix depends on it. | author | decide D4/D5 |
| **B6** | Ground-truth storage and scratch-path policy (D10/D11) must be fixed before the oracle is built. | author | decide D10/D11 |
| **B7** | The formal part is the author's to own (A1 §3); §6.1 and §6.7 are drafts. | author | adopt/revise P1–P12 and FO choices |
| **B8** | θ and "certified" language (D12) and M1-kafka comparability (D13) affect the paper's claims and table wording. | author | decide D12/D13 |

### 14.2 Blockers for implementation (P2 and beyond, not P1)

| # | Blocker | Owner | Resolution |
|---|---|---|---|
| **B9** | Toolchain not yet bootstrapped: only a Maven archive is present under the vendor download area; Kafka KRaft, PostgreSQL 16, Toxiproxy and JDK 21 are not yet installed. No run can execute. | lead/S3 | complete the pinned bootstrap; record digests |
| **B10** | The artifact (nine mechanism configurations, failpoint hooks, phase markers) does not exist yet; S3 is correctly held until P1. | S3 | implement after approval |
| **B11** | The independent oracle and the ground-truth generator do not exist; they must be built before the mechanisms (A1 §4) and cross-validated (digest equality, G5). | S3/S4 | build oracle first; mutation-test it (G3) |
| **B12** | The TLA+ toolchain is absent; `formal/ledger.tla` is delivered as an unverified draft and must not be presented as a checked model. | lead | vendor TLC at P2 if FO-6 is adopted |
| **B13** | Disk headroom ≈ 14 GB: the 1M-event runs and retained dumps need a pre-registered retention/cleanup policy and a free-space floor check (G8). | lead/S4 | confirm D10/D11 and the disk budget in prereg §9 |
| **B14** | No pilot exists, so no projected compute time exists; A1 §12 requires the pilot and the projection at P2 before P3. | S4 | run the pilot after P1; write the projection into prereg |
| **B15** | The failure-injection phase markers/failpoints are instrumentation that could bias Stage A timings; the pilot must measure their overhead (T-I6). | S3/S4 | measure with flags off/on in the pilot |
| **B16** | Red-team review (S5) of oracle blindness, non-fired faults, and analysis fairness is scheduled after the draft; the oracle design should be reviewed before Stage A. | lead | schedule an early S5 review of §7 and the oracle |

### 14.3 What IS ready at P1

- The complete formal model draft (I1–I4, EOS boundary, Lemmas 1–2, C1–C12, K1–K7) with 12 propositions, 8 counterexample traces, a 72-cell fault prediction table and a 63-cell crash-class table, every cell justified or marked unreachable with a proof reference.
- The independent-oracle design, exact metric definitions, quiescence and time-to-consistency definitions.
- The harness-validation gate (G1–G8), including 100 % mutation-testing of the checker and injection attestation.
- The staged design logic and the exact, pre-registered matrices/counts/statistics in `docs/prereg.md`, including the compute-projection method and the mechanical cut ladder.
- The threats register with mitigations and residuals, and the decision-table schema with frozen derivation rules.
- The decisions register (D1–D15) that makes the author's remaining choices explicit and bounded.

**Bottom line.** Nothing in this document is frozen; every claim that the paper will make is either a model proposition (author-owned) or a pre-registered measurement (frozen in `docs/prereg.md` after approval). Implementation must not start until the author signs P1 with the decisions above resolved; the formal section must be re-owned and edited by the author before submission.

---

