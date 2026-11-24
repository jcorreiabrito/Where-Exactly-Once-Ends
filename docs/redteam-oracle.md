# Red-team early review — oracle, runner, matrix, gates, formal model, pre-registration

**Reviewer:** S5 (red team) — adversarial internal-validity review
**Date:** 2026-11-24 (UTC-03), pre-Stage-A
**Scope:** the P2 implementation as it exists **before** any confirmatory run: oracle + mutation harness,
runner + injectors + validity/exclusion pipeline, frozen matrix/run order/seeds, gates G1–G8,
formal model, and the pre-registered analysis plan.
**Authority for this memo:** A1 §11 (red team), BRIEF §2/§6, docs/method.md §7, docs/prereg.md §7–§10.
**Write scope:** this file only. No code, matrix, results or other docs were modified. Throwaway test
scripts live under `%TEMP%\wxeo-s5\`.

---

## 0. Verdict

> # **BLOCK**
> Stage A must not start. The present implementation cannot produce a valid confirmatory dataset:
> the oracle's live dump path crashes on every run, the Stage-A fault wiring cannot arm, the Level-2
> receipt protocol fails attestation for every external fault, quiescence never establishes, and
> the pre-registered hypothesis decision rules are not satisfiable on the frozen matrix. The
> pre-registered analysis then converts several of these failures into *vacuous passes or false
> clean runs* rather than failures. Fix + re-freeze + author-ratified prereg amendment required,
> then re-review **before** Stage A.
>
> None of this is a criticism of the research question or of the author-approved *method*; the
> model, derivation contract and matrix design are largely sound (§6). The blockers are P2
> implementation and pre-registration-execution mismatches, and they are fixable.

### Top 5 issues, ranked by severity (threat-to-claim × blast radius)

| # | Finding | Severity | Why it is top-5 |
|---|---|---|---|
| 1 | **F-P1**: H1/H2/H3/H4/H5 decision rules are not satisfiable on the frozen matrix (H1 requires McNemar on a cell that is n/a for M1-kafka; H2/H4 "every executed cell" includes the early-order probes the model itself predicts to violate; H4 also includes M4's K6 exposure; H3 has no M3-strict K4 control; H5 dominance is impossible-then-vacuous). The frozen analyzer (`tools/analyze.py`) implements these rules literally. | **critical** | The paper's five hypotheses become undecidable/refuted/confirmed-by-construction. This is a design defect that needs the author, not a code fix. |
| 2 | **F-E1**: Stage A cannot execute at all: run rows carry lower-case fault tokens (`k3`) while the injector maps use upper-case (`K3`) → `failpoint_config_for_run` returns `{"enabled": false}` for **all 1,560** L1 runs; and even with case fixed, `K4` is absent from the map, `K2≡K3`, `K5`/`K6` are no-ops, and M1-kafka's K2/K5/K6 hooks do not exist. | **critical** | The "deterministic structural experiment" (H1/H3/H4 evidence, G1) produces no valid data. |
| 3 | **F-I1**: Receipt attestation is structurally broken: external injectors never write a `fired` receipt (base class writes only `armed`/`restored`), so **all 3,654** Stage-B/pilot L2 fault runs fail `validate_receipts`; L1 `pause` actions produce two `restored` receipts, failing 540 K5/K6 runs. | **critical** | ~50 % of all planned runs become exclusions; per-cell exclusion-rate flags everywhere; frequency claims (H1/H2/H3/H5) impossible. |
| 4 | **F-O1**: `oracle.py` uses `time.time()` in the two live dump functions but never imports `time`. Every live run's source and publication dumps raise `NameError`; `source_integrity` is then false and the runner invalidates **every** live run. | **critical** | Total blocker. Cheap fix, but zero live data until fixed. |
| 5 | **F-G1**: Gates are largely incapable of failing: G4/G5/G8 can only ever PASS once a run is `valid` (the runner's validity rules already imply the gate predicates); G6 compares the wrong objects and empirically FAILS on a clean synthetic fixture; G1 is a union check over K3∪K5 instead of per class and needs Stage-A data to run "before Stage A is analysed"; G7 is hard-coded PENDING; `clock_check` is hard-coded True. | **critical** | The harness-validation gate — the paper's main internal-validity defence — is not a gate. G3 is genuine; the rest are not. |

Also critical (in the top 5's blast radius, detailed below): **F-P5** censored runs are scored clean by
the analysis and `n_planned` is silently redefined; **F-E4** F6 breaks the M1-kafka positive control
and cannot rewind M4; **F-RN1** the live run-record path cannot emit a schema-valid pilot/Stage-A
record and has no rerun path; **F-G2** quiescence never establishes (Kafka lag parser + no stream
definition for M1-kafka) which, combined with F-P5, hides 100 % censoring as clean data.

**Counts:** **9 critical, 19 major, 6 minor** findings (plus one positive finding, F-O8); plus **8
explicitly-pending items** (§13) that are "not yet built", not defects.

### Review snapshot (read this before acting)

The repository is a **moving target**: S4 was actively writing while this review ran (runner.py,
analyze.py and the freeze changed three times between 09:52 and 10:11). All file:line citations and
recomputations in this memo refer to this snapshot; **re-run the checks after any fix**:

| File | SHA-256 (first 16) | mtime | bytes |
|---|---|---|---|
| `experiments/oracle/oracle.py` | `AFA40CDDE1D8FDC4` | 09:52:42 | 61,666 |
| `experiments/oracle/derive.py` | `8146C76E4851E4D5` | 09:57:54 | 13,753 |
| `experiments/oracle/mutation_test.py` | `18BA9D82E0BC07CC` | 09:53:05 | 15,334 |
| `experiments/injectors.py` | `F1D74ABA65037872` | 09:57:38 | 34,798 |
| `experiments/runner.py` | `3C6DD6903E1FA6E4` | 10:08:15 | 68,090 |
| `experiments/matrix.py` / `matrix_design.py` | `A6A5D770C33FC18D` / `3A10884B90FD0AF0` | 09:56 | 32,028 / 5,580 |
| `experiments/matrix.yaml` / `run_order.csv` | `2964957D8EEA44FE` / `894D7B8EE20C3536` | 09:56:23 | 181,021 / 1,194,627 |
| `experiments/validate_record.py` | `A4E34CAB58C9BADF` | 09:58:44 | 12,384 |
| `experiments/deviations.py` | `78AF49BF17869DDF` | 10:00:21 | 21,445 |
| `tools/analyze.py` | `C6EA48ECD82A27A3` | 10:08:52 | 84,721 |
| `tools/make-figures.py` | `26172701623995FD` | 10:04:15 | 12,689 |
| `docs/prereg-freeze.json` | `216779DA6DB8152D` | 10:09:53 | 7,410 |
| `formal/ledger.tla` + `formal/tlc-results/tlc-summary.json` | (unchanged) | 09:51 | 9,234 / 3,680 |
| Java artifact jar used by runner contract | `54E185441712EDC2` | 10:01:50 | 34,462,466 |

Frozen prose hashes (from `docs/prereg-freeze.json`, verified there): `docs/prereg.md`
`7a6667a2…`, `docs/method.md` `e731750b…`, `docs/CONTRACTS.md` `ad86835a…`,
`docs/P2-RULINGS.md` `4dc0798e…`, `docs/derive-vectors.csv` `699518df…`.

---

## 1. Findings — oracle and oracle independence (attack surface 1–2)

### F-O1 (critical) — the oracle's live dump path cannot run: `time` is never imported

**Claim under attack:** "the oracle reads an independent source dump and publication dump for every
run" (method §7.4, prereg §4.1 step 10).

**Evidence:** `experiments/oracle/oracle.py` imports `argparse, csv, hashlib, json, os, shutil, sys,
tempfile, collections, datetime` (lines 60–69) and calls `time.time()` at lines 1129, 1130
(`dump_source_topic`) and 1185, 1186 (`dump_kafka_topic`). Command:

```
python -c "import experiments.oracle.oracle as O; print(hasattr(O,'time'))"
→ has time attr: False
```

**Failing scenario:** every live run (`runner.py:_dump`, both dumps unconditionally) raises
`NameError: name 'time' is not defined`; the exception is caught and only appended to `notes`
(`runner.py:_dump`), the dump files are absent, `verify_source_inputs` returns
`source_integrity=False`, and `_finish` marks the run invalid (`source_integrity_failed`). With
`source_integrity=False`, **100 % of live runs are invalid**. The synthetic-fixture/dry-run path
never calls the two live dumps, so the 30-record dry run and the 14 unit tests do not catch it
(verified: `python -m pytest experiments/tests -q` → 14 passed).

**Required fix:** `import time` (or replace with `time.monotonic()` consistently) and add a unit test
that exercises `dump_source_topic`/`dump_kafka_topic` against a fake/embedded client or at least
asserts the symbols resolve (`hasattr(O, "time")` is not enough; add a call with a monkeypatched
consumer). Add a smoke gate: the runner must fail the run if a required dump is absent rather than
degrade to `notes`.

**Disclosure:** artifact appendix (oracle contract); no paper claim is affected once fixed.

### F-O2 (major) — the I2 append-only *privilege* attestation is vacuous; a missing attestation scores a run clean

**Claim under attack:** method §2.3 I2(a) "the application role holds no UPDATE/DELETE privilege and
the trigger exists (verified by the oracle each run)"; method §2.6 V2 includes "append-only
attestation fails".

**Evidence:** `oracle.py:dump_pg` (lines 1030–1080) connects with the **read-only** DSN
(`user=wxeo_read`) and executes
`SELECT has_table_privilege(current_user, '<schema>.posting', 'UPDATE'|'DELETE')` (lines 1054–1057).
`current_user` is therefore the oracle's own role, which is granted `SELECT` only
(`SchemaDdl.java:93–97`); the query can only return `false`. The application role's privileges are
never queried. The app *does* self-attest correctly (`PostgresSink.java:114–139`, written into
`app_*.json`), but the oracle/runner never consume that file — the runner passes
`attestation_path=self.p.attestation` (oracle-generated, `runner.py:_dump`). `evaluate_attestation`
(`oracle.py:764–783`) returns `failed=None` when the file is absent, and `evaluate_sink` only adds a
**warning** (`oracle.py:853–854`): the run stays valid and clean.

**Demonstration:** clean fixture, no `--attestation`:

```
T4 clean sink, attestation file absent
  violations: all false, violating_run: false
  warnings: ["append-only attestation missing (I2 enforcement clause unverified)"]
```

Conversely `can_update=true` in the attestation does produce `v2`/`ATTRIBUTION` (T5, works).

**Failing scenario:** a mechanism/version that is accidentally granted UPDATE (or a run where the
attestation dump failed) is scored **clean** while the I2 enforcement clause is unverified. This is
the exact I2 (a) part the gate is supposed to attest.

**Required fix:** (i) query privileges for the **application role name** recorded in the run record
(`has_table_privilege('wxeo_app', …)`), or preferably consume the app's own `app_*.json` attestation
and *also* verify the role name; (ii) make `attestation.present == false` for a DB-sink run an
**invalid run** (or a v2 violation), never a warning; (iii) add an adversarial control to G3 where
`can_update=true` and one where the file is missing.

**Disclosure:** paper "harness validation" must say the oracle checks the app role's privileges, and
the threats section must note that update-then-revert is invisible at end state (see matrix §3).

### F-O3 (major) — P11 / §2.8 is contradicted by the normative I1 predicate; its pre-registered test can never fire

**Claim under attack:** method §2.8 table ("Effect applied twice (whole entry) → I1 **passes**, I3
fails") and proposition **P11** (lines 798–799: "There exist runs in which I3 is violated while I1 is
not … Falsified if: every duplicate run also shows I1 > 0"), with the pre-registered measurement
`v3=1 ∧ v1=0 must occur in the duplicate cells`.

**Evidence:** the *approved* I1 formalisation (D8; method §2.3, §2.5 P4 lines 272–285) includes the
structural clause I1a: per `event_id` there must be exactly two postings, one D and one C. A
duplicated whole entry yields 4 postings for that `event_id` → **`imbalanced_entries++`**. The oracle
implements P4 faithfully (`oracle.py:627–658`). Demonstration on a clean fixture (50 events), adding
an exact copy of both legs of one event:

```
T1 whole-entry duplication (balanced copy of both legs)
  counts: dup_postings=2, dup_entries=1, imbalanced_entries=1
  violations: v1=true, v3=true
  P11 test criterion v3==1 and v1==0 → criterion_satisfied: false
```

T9 (triplication) likewise gives v1+v3. Under D8, **v3=1 ⇒ v1=1 for every possible posting-level
duplicate**, so P11's measurable prediction is unsatisfiable; the paper would report P11 "falsified"
by construction.

**Failing scenario:** the headline practitioner message ("balance checks do not detect duplication")
is mis-stated for the approved I1. It is true for **I1b (signed-sum-to-zero) only**, which is why D8
split I1a/I1b — but the run record, oracle, analysis and P11 all use a single `v1`.

**Required fix:** add `v1a`/`v1b` (or an `imbalanced_sum` count separate from structural I1a) to the
oracle + run record; restate P11 in terms of I1b; change the P11 test to `v3=1 ∧ v1b=0`; and state in
the paper that the *structural* clause does catch duplication, i.e. the practitioner message is
"sum-to-zero checks are blind; ledger-shape checks are not". Requires prereg/method edit (author owns
the formal claim).

**Disclosure:** method §2.8/P11 text, prereg §5 (run-record fields), §6.1, §7.1 (per-invariant
reporting), and the paper's duplicate-vs-balance paragraph.

### F-O4 (major) — the spurious-posting verdict is an unratified extension; a fabricated *balanced* entry evades `v1..v4`

**Claim under attack:** prereg §6.1 "the oracle computes the metrics of method §7.1"; method §2.5
defines `violating_run = v1 ∨ v2 ∨ v3 ∨ v4`; method §2.4 says a spurious posting "is treated as an
invalid-run signal for the harness".

**Evidence:** the oracle adds `∨ (spurious_postings>0)` to the verdict (`oracle.py:37–45, 737, 754`,
`VERDICT_RULE`), recorded in `deviations.py` item S4-1 as a runner-level interpretation. Demonstration
with a fabricated, perfectly balanced new entry (both legs, new event id, sums to zero):

```
T2 balanced spurious entry
  counts: spurious_postings=2, all other counts 0
  violations: v1=false v2=false v3=false v4=false, violating_run=true (attribution: SPURIOUS)
```

So today the fabricated entry is caught **only** by the unratified extension; any consumer that
recomputes the pre-registered verdict `∨ v1..v4` from the run record (as method §2.5 literally
defines) scores this run **clean**. G3's `insert_spurious_row` class inserts one unbalanced row, so
the balanced-fabrication case is not even in the mutation corpus (see F-O6).

**Failing scenario:** a buggy/tampered pipeline (or a harness bug) that fabricates balanced entries is
scored clean by the published definition; or the extension is applied inconsistently between oracle,
analysis and paper.

**Required fix:** make the verdict rule explicit and author-ratified in the prereg *before* freeze
(either add `v5 = spurious>0` to the official violation vector and run-record schema, or declare
spurious ⇒ invalid run, per method §2.4); add a **balanced-spurious-pair** mutation class to G3 and
require the analysis to read `violations.violating_run` (never recompute it).

**Disclosure:** prereg §5 (violations schema), §6.1, §8.1 G3, §12/DEVIATIONS (S4-1 must become an
author-ratified amendment, not a runner decision); paper's oracle contract.

### F-O5 (major) — `divergence` is not the documented metric; publication-stream *content* is never checked for DB mechanisms

**Claim under attack:** method §7.1 `divergence = card(stream-records) − card(ledger-entries)` on
matched events; method §3.4 says stream/ledger divergence is the observable signature of the M1x gap
(trace X4); §7.1 `pub_dup`/`pub_lost`.

**Evidence:** `oracle.py:700–730` computes `pub_divergence` only as "published but ledger entry
incomplete". A missing stream record with a complete ledger gives `lost_records>0, divergence=0`.
Demonstration (clean fixture, one publication line deleted — the M1x abort-then-retry signature):

```
T6 publication record lost, ledger complete
  violations: all false
  publication: {pub_records: 49, lost_records: 1, divergence: 0}
```

The documented signed difference would be `−1` (or `|B|` in trace X4, where the *stream* is short).
Additionally, for DB-sink mechanisms the publication records are reduced to event ids before
comparison (`_shard_publication_eids`, lines 472–501): corrupted legs/amounts in the publication
stream are invisible. For M1-kafka the expansion path is content-aware, but that path is not
mutation-tested (F-O6).

**Failing scenario:** an M1x run with an aborted Kafka transaction and a durable ledger (exactly the
W1 signature the paper claims as secondary evidence) reports `divergence = 0`; the paper's
"divergence opens with the gap" claim would be reported as absent.

**Required fix:** implement the documented signed divergence (stream entries − complete ledger
entries, on matched events) plus directional `pub_dup`/`pub_lost`; validate publication *content*
against expected entries for DB mechanisms; add both directions as mutation classes on the
publication dump. State the metric definition in the run-record/oracle contract.

**Disclosure:** method §7.1, §3.4, trace X4; prereg §5 `publication` block; paper's secondary-metrics
paragraph.

### F-O6 (major) — G3 does not cover the paper's own duplicate signature, balanced fabrication, or the M1-kafka/oracle path

**Claim under attack:** A1 §5 "mutation testing of the checker … 100 % detection required"; method
G3 "for each synthetic defect class — drop a row, duplicate a row, flip a leg sign, update a row's
value, insert a spurious row".

**Evidence:** the six implemented classes (`make_fixtures.py:276–293`, `mutation_test.py`) are
`drop_row, delete_event, duplicate_row (one row), flip_leg_sign, update_row_value,
insert_spurious_row (one row)`. **Not covered:** whole-entry duplication (the M1x trace-X1 signature;
its expected attribution under method P11 would be I3-only, but the oracle gives I1+I3 — see F-O3);
duplicate-with-different-values; balanced fabricated entry (F-O4); currency mixing and negative
amounts; the append-only attestation failure; the **publication stream as ledger** (M1-kafka
`_expand_publication_to_postings`) — `mutation_test.py` always calls `sink_kind="db"` (line
131–136, 516 etc.). The report is honest that it is synthetic, and 720/720 detection is real for the
six classes; it is not the coverage the gate claims for the paper's core scenarios.

**Failing scenario:** a regression in the publication-expansion path (e.g. currency default, leg
ordering) would not be caught before the campaign and would corrupt the positive control G2/H1.

**Required fix:** extend DEFECT_CLASSES with `duplicate_entry` (both legs), `balanced_spurious_entry`,
`currency_mix`, `negative_amount`, `attestation_granted`, `attestation_missing`, and run the full
class set against both `sink_kind="db"` and a constructed M1-kafka publication dump; update
`results/harness/g3-mutation.json` and freeze it.

**Disclosure:** method §7.6 G3; prereg §8.1; paper T1/G3 row and oracle-contract appendix.

### F-O7 (minor) — malformed observed rows are silently dropped

`_shard_csv_dump`/`_parse_observed_shard` count a malformed CSV row and `evaluate_observed` skips it
(`oracle.py:386–414, 600–603`). Demonstration: append `deadbeef,not-a-number,acct:x,D,10,BRL` to a
clean sink → `observed_malformed_rows=1`, **verdict clean**. A corrupted/partial sink dump can
therefore hide a duplicate or mutation row. Required fix: any malformed observed row ⇒ invalid run
(provenance failure), or at minimum a hard oracle error; record the sink-dump digest/row count and
cross-check against the DB count. Also: the sink dump has no digest check against a DB-side count
(the runner archives its SHA-256 but nothing verifies the dump is the whole table).

### F-O8 (positive finding, no action) — oracle independence is genuine

- `experiments/oracle/*.py` imports only the standard library; psycopg2/confluent-kafka are lazy
  imports inside dump helpers; **no** import of `artifact/**` (verified by reading `oracle.py`,
  `derive.py`, `mutation_test.py`).
- `docs/derive-vectors.csv` (lead-derived from spec text) is reproduced exactly by the Python
  implementation (6/6 rows, command in §5) **and** by the Java implementation (JUnit
  `NormativeVectorsTest`, surefire `Tests run: 1, Failures: 0`).
- The Java `GroundTruthTest` golden digest for `seed=0123456789abcdef, 3 events`
  (`f4e127d98bddf1fba21f20b8b4cc9870995626acc8c152486118c8f822ea4f72`) is byte-identical to the
  Python regeneration (verified, §5). This is a real cross-language check.
- **Caveat:** `experiments/oracle/g5-vectors.json` is generated by the Python implementation itself
  (`generated_by` field); it is *not* independent evidence of Java↔Python equality. The independent
  link is `docs/derive-vectors.csv` (6 spot rows) + the golden-digest test. G5's per-run check
  (GT file digest == Python-regenerated digest) is implemented in `verify_source_inputs`
  (`oracle.py:234–239`), which is sound; only the gate-evidence aggregation is not (F-G1/G5).

---

## 2. Findings — fault attestation, exclusions, injectors (attack surface 3)

### F-I1 (critical) — no `fired` receipt is ever written for an external (Level-2) injection; all L2 fault runs fail G4

**Claim under attack:** method §5.6 / A1 §5: "exactly one `fired` and one `restored` per armed
injection; runs where the fault did not fire are excluded and counted"; CONTRACTS §9 same.

**Evidence:** `injectors.py:233–303` — `Injector.trigger_and_fire` performs `_fire()`, sets
`self._fired=True`, and **never appends a `fired` receipt**; the subclasses (`F1:329–368`,
`F2:380–416`, `F3:418–462`, `F4a:464–497`, `F5:550–581`, `F6:586–629`) also never call
`self._receipt("fired", …)`. Only the *Java* in-process failpoints write `fired` receipts
(`Failpoints.java:103–128`). The only S4 writes are `armed` (sham) and `restored`.

Consequently `validate_receipts` (`injectors.py:110–136`) requires `fired=1, restored=1` for
`injector_mode=live` and sees `fired=0, restored=1` → `attested=False` →
`runner.py:_collect_fault_receipts` appends `fault_receipts_invalid` → the run is excluded.

Command (direct function call):

```
L2 external receipts only S4(restored)
→ {'attested': False, 'reason': 'expected exactly one fired and one restored receipt, got
   fired=0 restored=1', 'fired': 0, 'restored': 1}
```

**Blast radius:** all Stage-B fault runs (3,480) + all pilot fault runs (174) = **3,654 runs**
excluded, i.e. every confirmatory frequency claim (H1's Stage-B arm, H2 clean claim, H3 H2, H5)
becomes an exclusion table. The 6 `selftest` validation runs (whose purpose is to prove injectors
work) fail the same way — they cannot serve as G4 evidence.

**Failing scenario:** Stage B executes; every F1–F6 run is marked excluded; `N_valid = 0` in every
fault cell (the F0 cells are still valid); the analysis can only report "inconclusive" and the
campaign's 8–24 h budget is burned on excluded runs.

**Required fix:** one `fired` receipt per run, from a single owner. Simplest consistent protocol:
`Injector.trigger_and_fire` writes the S4 `fired` receipt (with the realised scheduled/actual ms,
target id, before/after state) right after `_fire` returns; the app failpoint keeps writing its own
`fired` with `fault=<failpoint-name>` as the *in-process* evidence; `validate_receipts` must then
count receipts **per role** (S4 receipt vs app receipt), not a global count. Choose one and encode it
in CONTRACTS §9 + G4; update `deviations.py` S4-2 accordingly.
Also: assert `_fire` actually changed the system (F-I3) before writing `fired`.

**Disclosure:** prereg §8.1 G4, §8.2/§8.3; method §5.6; CONTRACTS §9; paper's injector-attestation
threat (T-I2) and the validation table.

### F-I2 (major) — L1 `pause` actions produce two `restored` receipts → K5/K6 runs fail attestation

The Java failpoint writes `fired` then `restored` for `pause` (`Failpoints.java:119–123`); S4's
`Injector.restore()` then writes a second `restored` for the same run. Command on synthetic receipts:

```
L1 pause receipts app(fired+restored)+S4(restored)
→ {'attested': False, 'reason': 'expected exactly one fired and one restored ..., got
   fired=1 restored=2'}
```

Blast radius: 540 K5/K6 Stage-A runs. (Even after case-fixing F-E1, these runs would be excluded.)
Fix: define the receipt protocol per action (`crash`: app fired + S4 restored; `pause`: app fired +
S4 restored, app must not write its own restored; or accept exactly one app restored) and validate
`fault` field = the expected failpoint name for L1 runs.

### F-I3 (major) — an "attested" injection need not have happened; L2 firing is not phase-checked

- `F1BrokerKill._fire` (injectors.py:345–357) and `F2ConsumerKill._fire` (395–404) run
  `taskkill` with `check=False`, poll ≤30 s, and return success **regardless of whether the process
  died**; `before/after` fields are recorded but never asserted. A wrong pid / already-dead target
  yields a "fired" receipt with no effect.
- For L2, `level2_trigger_time` (injectors.py:758–767) fires at 25–75 % of the *expected* load
  duration; `runner.py:_effective_rate` (current lines ~1036–1040) silently falls back to `"max"`
  when `experiments/rates.json` is missing (it is missing now), and `_expected_load_seconds`
  returns 60 s for `max`. A fault can therefore fire **after the load ended**, i.e. outside any
  processing window; the run is valid + clean by construction and enters the denominator as evidence
  of fault absorption.
- No injector records a phase marker (all L2 receipts have `phase_marker: null`), so the analysis
  cannot distinguish "fault fired during processing and was absorbed" from "fault fired while the
  applications were idle".

**Failing scenario:** F1 kills a broker whose pid is stale → ledger unaffected → clean F1 run counted
as "M1x/F1 absorbed"; or a fault fires after `t_load_end` → every mechanism appears to absorb it.
This biases **toward safety** in exactly the cells that decide H2/H4.

**Required fix:** assert the action result (pid gone / broker count < 3 then restored / toxic present
on the proxy / offsets rewound) *before* writing `fired`; for L2 require
`t_load_start + δ ≤ t_fault_fire ≤ t_load_end − δ` and consumers alive, else mark the run
`uninformative_fire_outside_window` (excluded, counted); record the container/process liveness at
fire time in the receipt. Add `rates.json` generation to the pilot and make Stage B/C **abort** if
the rate table is missing (fixed-rate mode is a prereg claim: §6.2, §4.5).

### F-I4 (major) — the exclusion pipeline has no rerun path; failed runs can vanish without a record

**Claim under attack:** prereg §8.3 (invalid runs rerun, same seeds, `R_max = 3`), §12 (no silent
drops), `results/DEVIATIONS.md` generated from records.

**Evidence:** `runner.py:my_live_one` (`_live_one`) catches **every** exception and returns
`{"run_id": …, "failed": …}`; `run_live` (current lines ~660–690) appends it to the results list but
writes no run record and applies no rerun. `RERUN_CAP = 3` exists as a constant only (line 75; no
use). Causes that reach this path today: Stage-A failpoint-arm failure (F-E1), injector restore
failure (e.g. F1 restart timeout), any validation error in `_finish` (the record is written first,
then `VR.validate_record` raises → the invalid record remains, but the run is also reported failed),
and any unexpected exception (e.g. F-O1 dump `NameError` is caught inside `_dump`, so that one
degrades to invalid instead). For pre-record failures there is **no invalid run record at all**, so
`results/DEVIATIONS.md` cannot distinguish "not executed" from "executed and failed" — and the
N_planned/N_valid/`N_missing` accounting hides them.

**Failing scenario:** a run whose post-processing failed *after* producing duplicates is dropped; the
per-cell violating-run rate is computed only over runs that happened to complete → survivorship bias
toward safety (this is the fatal direction the red team must prevent).

**Required fix:** wrap each attempt so that *any* outcome produces a `wxeo.run.v1` record, with
`validity.valid=false`, `excluded=true`, `exclude_reason`, and the partial artefacts; implement the
R_max=3 rerun loop with the same `(cell, rep)` seeds and log attempts; count `N_attempts` vs
`N_valid` in DEVIATIONS. No code path may omit a reserved run.

### F-I5 (minor) — injected-fault denominator/fire-rate computation is not implemented; G4 checks only valid runs

`deviations.py` computes exclusion counts/rates per cell (good) but there is no fire-rate
computation from receipts, and `tools/analyze.py:build_gate_table` G4 evaluates receipts only on
`valid` runs — but the runner's validity rules already require the receipts, so G4 is tautological
(F-G1). Required: a fire-rate table computed from `results/faults.log` per mechanism×fault (fired /
armed, restored / fired, exclusions), as prereg §8.2/§7.7 require, and the G4 gate must examine
**all reserved runs**, not just valid ones.

---

## 3. Findings — injector realizability and Stage A (attack surface 4, mechanism side)

### F-E1 (critical) — Stage A cannot arm: lower-case fault tokens + missing K4 map entry

**Claim under attack:** method §5.1–5.3 / prereg §2.4: 52 executed crash-class cells × N=30 = 1,560
deterministic runs; G1 negative control in M0's K3/K5 cells.

**Evidence chain:**

1. `matrix.py:137` constructs Stage-A cells with `condition=klass.lower()` and, at line 102, sets
   `"fault": … else condition` for Stage A → `matrix.yaml` and `run_order.csv` carry **lower-case**
   fault tokens. Verified: distinct Stage-A `fault` values in `run_order.csv` =
   `ctrl,k1,k2,k3,k4,k5,k6,k7`.
2. `injectors.CRASH_CLASS_FAILPOINT` keys are **upper-case** (`injectors.py:641–648`:
   `"K1"…"K7"`) and `failpoint_config_for_run` looks up `CRASH_CLASS_FAILPOINT.get(klass)`
   (`injectors.py:666–668`) with `klass = ctx.fault` (the run row). Direct call:

   ```
   k3 -> {'enabled': False}
   K3 -> {'enabled': True, 'name': 'after_effect_commit', 'action': 'crash', 'after': 1}
   ```

3. `make_injector` returns `FailpointWatch` for L1 (`injectors.py:750`); `FailpointWatch._arm`
   raises `RuntimeError("no L1 failpoint available for k3")` when the config is disabled
   (`injectors.py:691–694`). `runner.LiveSequence.execute` calls `self.injector.arm()` before the
   load (current runner line ~742); the exception aborts the run before any record is written
   (F-I4) → **all 1,560 Stage-A runs fail to execute**.
4. Even after case normalisation, **K4 is not in `CRASH_CLASS_FAILPOINT`** (`map keys ->
   ['K1','K2','K3','K5','K6','K7']`; direct call `K4 -> {'enabled': False}`), so the 8 K4 probe
   cells (240 runs, H3's loss arm) would still be unarmable. Note `K2` and `K3` map to the **same**
   failpoint (`after_effect_commit` crash): K2 ("unknown effect outcome", C3/C9) is realised as a
   known-outcome post-commit crash = K3. No run ever produces the C3 unknown-outcome state, so the
   K2 cells are mislabelled K3 repetitions.
5. M1-kafka has neither `effectTransaction` nor `BEFORE/AFTER_EFFECT_COMMIT` hooks
   (`M1Kafka.java:33–58` fires only `*_PROGRESS_COMMIT`), so `after_effect_commit`/`before_effect_commit`
   failpoints never fire for M1-kafka → K2, K5, K6 cells (90 runs) time out and are excluded even
   after the case fix. Separately, **K5 is structurally unreachable for M1-kafka** (a duplicate
   delivery without external rewind cannot occur when the effect and progress are one Kafka
   transaction), contradicting method §5.3's reachability matrix (`M1-kafka K5 = R`).

**Failing scenario:** the "deterministic structural experiment" produces zero data; H1's K2/K3 arm,
H3's K4 loss arm, H4's M4/M4+M3 reachable-cell claim and G1 have no valid evidence. If the case bug
were fixed only in `injectors.py` (mapping `k3→K3`), the run records would still carry lower-case
`fault` and `validate_record` would reject them (F-RN1), and the analysis filters on upper-case
`{"K2","K3"}` (`analyze.py:629, 1193`), so H1/G1 would silently see "no data".

**Required fix:** normalise Stage-A fault tokens to upper case in `matrix.py` (and regenerate +
re-hash `matrix.yaml`/`run_order.csv`; this is a pre-freeze change), add `K4` to the map
(`before_effect_commit` **crash** on the early-ordering path — see the intended realisation for
F4b), give each class a realisation that matches its method §5.1/§5.2 definition (F-E2/F-E3), and
provide a **role-instantiation table** for M1-kafka (D4: "semantic-role targeting in Stage A" —
currently not implemented). Re-derive Stage-A reachability for M1-kafka K5 (should be n/a) and
amend method §5.3/prereg §2.2 accordingly.

### F-E2 (critical) — K5 and K6 realisations are no-ops; K7/K1 are fine; G1 cannot pass as specified

Method definitions: K5 = C6 "duplicate delivery without a crash: rebalance reassignment of
uncommitted offsets, or a group offset rewind" (method §5.1/§5.2); K6 = C7 "zombie writer: owner
paused beyond `max.poll.interval.ms`, partition reassigned, paused instance resumes and completes its
write".

**Evidence:** `injectors.py:641–648` maps
`K5 → {"name": "before_effect_commit", "action": "pause:200"}` and
`K6 → {"name": "after_effect_commit", "action": "pause:200"}`. A 200 ms pause:
- does not create duplicate delivery (K5) — for M0 strict the batch is simply processed after 200 ms,
  the run is clean; **G1 explicitly requires M0 to show I3 violations in K5** (method §7.6,
  `analyze.py:1192–1203`) → G1 as specified cannot pass, or is passed vacuously by unioning K3∪K5
  (F-G1).
- cannot expel a consumer: `max.poll.interval.ms = 300,000` (`EffectiveConfig.java:204`,
  method §4.11) and `T_max = 300 s` (prereg §4.6) — a genuine zombie is impossible within a run
  window without violating the frozen shared config. So the M4 K6 exposure (method P9, RQ3 E3, the
  reason M4+M3 exists) has **no experimental realisation at all**.
- pause actions additionally fail receipt validation (F-I2), and the 540 K5/K6 runs are uninformative.

**Failing scenario:** M4's only correctness exposure (zombie) is asserted in the paper without
experiment; H4's "M4 zero violations in every reachable cell" cannot be tested; the G1 gate fails
(aside from the union shortcut) and the campaign halts.

**Required fix (decision needed by lead/author):** realise K5 via the F6-style protocol replay
without crash (stop consumers, rewind committed offsets by k *batches*, restart) on the cells where
that is semantically C6; realise K6 by a *declared probe* that lowers `max.poll.interval.ms` for the
probe cell only (a config deviation explicitly recorded — it is a probe, not canonical tuning) or by
a failpoint that pauses the owning instance past a shortened interval and lets the second instance
process; or (recommended, cheapest) declare K6 structurally **not experimentally realisable within
the frozen configuration**, mark the M4/M4+M3/C7 claim as model-only in the paper, and amend method
§5.3, the prediction tables and H4/RQ3/E3 wording. Do not leave 270 vacuous runs in the matrix.

### F-E3 (major) — K2 realisation is K3; Stage-A evidence will be mislabelled and P2's conditional claims untested

Covered in F-E1(4). Additionally, the K2 rows for M4 (post-commit crash with both effect and
progress durable) test nothing at all. Required: implement C3/C9-style unknown-outcome injection
(e.g. kill the process while the COMMIT acknowledgement is in flight, via a Toxiproxy cut on the
DB link *after* the commit was sent and before the ack, or a failpoint immediately after
`tx.commit()` returns but before the return value is used — a genuine "unknown outcome" state
requires acting at the protocol boundary, not after the ack), or narrow the K2 claim in the paper
and remove the cells.

### F-E4 (critical) — F6 realisation violates D5 and breaks the M1-kafka positive control; M4 rewind uses a read-only role

**Claim under attack:** D5 (protocol-level replay only; "external rewind excluded because it would
make M1-kafka duplicate in its own sink"); method §5.4 F6 realisation; G2 positive control
("M1-kafka shows zero violations in all fault-free control runs", and prereg §3.1/§8.1 require
zero violations in **every executed cell** for H1's clean arm — F6 is executed for M1-kafka: 63 runs
including pilot/Stage B).

**Evidence:** `injectors.py:F6DuplicateRedelivery` stops the consumers and, for non-M4 mechanisms,
runs `kafka.admin.ConsumerGroupCommand --reset-offsets --shift-by -k --execute` (lines 611–621).
For M1-kafka the resume point *is* the committed offset, so rewinding it forces reprocessing of
already-committed events; `M1Kafka.process` has no dedup (Kafka transactions are not idempotent
against offset rewinds) → every rewound event produces a second publication record = I3 violation in
the ledger. This is the exact external rewind D5 excludes to preserve the positive control. The
matrix executes F6 for M1-kafka (`run_order`: 63 F6 rows for M1-kafka), so if the receipt issue
(F-I1) is fixed, G2 fails and H1's clean arm fails — halt. The paper would then have to either drop
M1-kafka's F6 cells (making F6 n/a like F4a/F4b/F5, a matrix/prereg change) or admit the EOS
positive control is not immune to operator-induced replay.

Additionally:
- For M4/M4+M3 the rewind is `psql UPDATE consumer_progress …` executed with
  `db_dsn = host=127.0.0.1 port=5433 dbname=wxeo user=wxeo_read` (`runner.py:_db_dsn_direct`), and
  `SchemaDdl` grants the read role `SELECT` only (`SchemaDdl.java:93–97`) → the UPDATE is rejected,
  `_fire` raises, and all 126 M4/M4+M3 F6 runs become `fault_not_fired` exclusions. (Also it rewinds
  by `k` *offsets*, not `k` batches as the description and method §5.4 say.)
- F4b in Stage B is executed through `FailpointWatch` (deterministic `before_effect_commit` crash on
  the first hit), not the Level-2 stochastic realisation method §5.4 promises ("same, with random
  timing"). `F4bOrderingProbe` (injectors.py:501–544) is dead code (`make_injector` line 750 returns
  `FailpointWatch` whenever `fault=="F4b"`). Restore restarts only instance 1 even though the crash
  may have hit instance 2 (minor, F-M3).

**Required fix:** exclude F6 for M1-kafka (n/a with proof, D5-consistent: EOS is not idempotent
against an operator rewind — state it as a limitation/threat, or run it labelled exploratory outside
the positive-control gate); use a write-capable role for the M4 progress rewind (injector action,
recorded and logged; note this is an adversarial state injection); rewind by k*max.poll.records
offsets (or record the realised rewind in events); either implement the L2 F4b stochastic path or
record the deterministic substitution as a DEVIATIONS entry and correct the method/prereg text.

### F-E5 (major) — G1's evidence is produced only by Stage A, which the gate must precede

Method §7.6: "The gate is executed on the pilot's data and on dedicated validation runs; no
Stage A/B/C run is analysed before all gates pass." G1 requires deterministic M0 K3 and K5 cells —
those runs exist only in Stage A. The pilot contains no K classes (matrix design), and the 20
validation runs contain no M0 K3/K5 run. So G1 is unsatisfiable at its own gate point; either G1 is
assessed on Stage-A data (circular: "before any measurement is analysed" violated) or it can never
pass. Fix: add dedicated M0 K3/K5 validation runs to the pre-Stage-A slate (they are cheap: 2 cells
× 30 reps = 60 runs at 1k), or reword the gate and the pre-registration to place G1 at the start of
Stage A with a stop-on-fail rule; also require **per-class** evidence (K3 and K5 separately), not a
union.

---

## 4. Findings — gates G1–G8 (attack surface 4)

### F-G1 (critical) — G4, G5, G8 are tautological; G6 checks the wrong object and fails on clean data; G1 is a union shortcut; G7 is hard-coded PENDING

**Claims under attack:** method §7.6 G1–G8; prereg §8.1; A1 §5.

**Evidence (current implementation, `tools/analyze.py:build_gate_table`, lines 1166–1286):**

- **G4** (injector attestation): `injected = [r for r in valid …]; un = [r for r in injected if not
  r["attested"]]; PASS iff not un`. But the runner marks a run invalid when receipts are not
  attested (`runner.py:_collect_fault_receipts`), and `valid` excludes all invalid runs → every valid
  injected run is attested by definition → `un` is always empty → **G4 can only PASS or PENDING**.
- **G5** (GT independence): same structure — `valid` already requires `source_integrity` and runner
  invalidates on source-integrity failure; the gate re-checks the same predicates on valid runs →
  **tautological PASS**. The vector-assertion claim in its evidence string is not executed.
- **G8** (clock/host sanity): `clock_check` and `residue_check` are hard-coded `True` in the runner
  (`runner.py:_finish` line ~1207: `clock_check = True  # … checked by the runner clock probe` — no
  clock probe exists; `profile_limits` is hard-coded). G8 passes vacuously.
- **G6** (configuration equality): the analyzer hashes `rec["config"]` minus
  `{mechanism, ordering_variant, config_sha256}` **per mechanism across all runs** and requires one
  hash per mechanism. The runner's `config` block (`_config_block`) contains `load_events`,
  `partitions`, `consumer_instances`, `load_mode`, `rate` — which vary across cells — so each
  mechanism necessarily has several "shared" hashes, and the check also never compares hash values
  **across mechanisms**. Empirical proof on the synthetic 30-record fixture:

  ```
  python tools/analyze.py --records results/synthetic/run-fixture/run-records --out %TEMP%\… --harness-dir results/harness
  G6  FAIL  distinct shared-config hashes per mechanism: {"M0": 4, "M1x": 4, "M2": 4, "M3": 4, "M4": 4}
  ```

  On real data this fails spuriously (or, if reduced to constants, passes without ever comparing
  mechanisms). The app *does* emit the right artifact (`config_<run>_<instance>.json` with
  `shared_sha256` and `intrinsic_sha256`, `EffectiveConfig.java:128–140`), but the runner discards it
  (its `config` block is a six-field stub, `runner.py:_config_block`) and the analyzer never reads
  it.
- **G1**: checks `any(v3) over the union of M0's K3 and K5 runs` (`analyze.py:1192–1203`), not
  "M0 shows I3 in K3 **and** in K5"; with F-E2 this can pass on K3 alone while K5 was never realised.
- **G7**: `"status": "PENDING"` hard-coded (lines 1263–1268) — it can never pass, so the
  "all gates pass before Stage A" gate can never be satisfied. Wired evidence (generator lag, doubled
  T_quiet) is not collected anywhere (prereg §6.2/§7.2 generator-lag measurement is also missing —
  see F-M1).
- **G2** is implemented as intended but on pilot/controls only in practice; with 0/N it reports a CP
  bound — acceptable, but note the positive control is also the subject of F-E4.
- **G3** is real (720/720 on the synthetic corpus) and PASSes; its coverage gap is F-O6.

**Failing scenario:** a broken harness passes G4/G5/G8; a clean configuration FAILS G6; G7 blocks;
the paper presents a T1 "gates" table whose PASS cells are tautologies. This is the single most
dangerous class of defect for an internal-validity gate.

**Required fix:** each gate must be computed from **provenance data over all reserved runs**, not
from *valid* runs: G4 from `faults.log` receipts vs armed runs; G5 from GT/regenerated/source digests
independently of `validity`; G6 by reading the app's `config_<run>_<instance>.json` and comparing
`shared_sha256` **across mechanisms within the same cell**, while checking that the set of differing
keys per mechanism equals the declared intrinsic set; G1 per class (K3 and K5 separately) with
attested injections; G7 implemented (rate table, generator lag p99, T_quiet doubling); G8 implemented
(clock probe, process affinity/priority read-back). Add a synthetic **negative-control fixture** for
each gate (a run set engineered to fail that gate) and require the gate to FAIL on it — this is the
only way to prove a gate is falsifiable.

### F-G2 (critical) — quiescence never establishes for most/all runs; M1-kafka has no sink definition

**Claim under attack:** method §7.3 / prereg §4.6 (quiescence: lag = 0, no new sink rows for T_quiet,
no active app transaction, retry drain; T_max = 300 s; censored runs are outcomes, and per method
§7.3 censored runs are violating runs).

**Evidence (`runner.py:_quiesce`, current lines 1059–1132):**

- `_lag_per_partition` parses `kafka.admin.ConsumerGroupCommand --describe` output and only accepts
  lines whose **first** token is a digit (`parts[0].isdigit()`), then takes `parts[5]` as lag. Kafka
  3.x `--describe` output starts with the **GROUP** column (`GROUP TOPIC PARTITION CURRENT-OFFSET
  LOG-END-OFFSET LAG …`, or topic-first on old layouts); in neither layout is column 0 the partition
  number, and in the group-first layout the row never passes `.isdigit()`. The lag map is returned
  empty; `if (… count==last_count and lag and all(v==0 …))` can never be true → **every run is
  censored after T_max = 300 s** (7,851 runs × 300 s ≈ 654 h).
- For M1-kafka `db_schema_used = False` → `_sink_count()` returns `None` → `last_count is not None`
  never true → M1-kafka is **always** censored even if the lag parser were fixed. No stream-based
  quiescence definition exists for the Kafka-sink mechanism.
- Not checked: `pg_stat_activity` idle-in-transaction/active app transaction, retry-drain, outbox
  relay catch-up (M2's ledger is consistent before the relay has drained, so `pub_*` metrics are
  taken mid-flight and Stage-D time-to-consistency excludes relay cost), stream lag for M1-kafka.
- The dumps/oracle run **before** the consumers are stopped (`execute()`: quiesce → dump → oracle →
  `_finish` stops consumers), whereas prereg §4.6 and method §7.3 require the final observation after
  the application is stopped (and a `REPEATABLE READ READ ONLY` snapshot); a post-quiescence retry or
  relay write can race the dump.

**Failing scenario:** every run burns 300 s and is censored; the analyzer (F-P5) does not treat
censored as violating → censored runs are scored clean, so the campaign produces "clean" data for
all mechanisms while quiescence was never established. Even if the analyzer is fixed to count
censored as violating, the campaign produces 100 % violating runs and the paper dies. Both directions
are fatal; the runner must be fixed before any run.

**Required fix:** use the Kafka AdminClient (`listConsumerGroupOffsets` + `listOffsets`) instead of
parsing CLI text; for M1-kafka define sink quiescence on the publication stream (committed LSO =
log-end per partition and no open transactional state) or use transactional offsets + `__transaction_state`;
add `pg_stat_activity` and relay-drain checks; move the dumps after stopping the application (or
record the observation start and prove the app was idle — but the prereg wording says after stop);
add a **negative control** for the quiescence probe (inject a slow relay and verify T_max censoring
is detected). Add the M1-kafka quiescence definition to method §7.3/prereg §4.6.

---

## 5. Findings — pre-registration, matrix arithmetic, analysis plan (attack surface 5)

### F-P1 (critical) — the pre-registered hypothesis decision rules are not satisfiable on the frozen matrix

This is the highest-severity finding because it invalidates the *plan*, not just the code, and needs
the author (A1 §0: methodology decisions are the author's).

**(a) H1(iii) requires McNemar on a cell where M1-kafka is n/a.** prereg §3.1 line 140:
"the Stage A comparison M1x vs M1-kafka in **K3** … significant by McNemar at α=0.05 (Holm family
C1)". But M1-kafka's K3 is structurally unreachable (method §5.3 footnote 1; `matrix_design.py`
`STAGE_A_REACHABLE["M1-kafka"] = [K1,K2,K5,K6]`; prereg §2.2 row "M1-kafka 4 (K3,K4,K7 n/a)"). No
M1-kafka K3 run exists, so the McNemar test has zero pairs. `tools/analyze.py` implements it literally
(`build_test_families` line ~793: `_mcnemar_for(valid, "M1x", "M1-kafka", {"K3"}, {"A"})`) and
`decide_hypotheses` H1 requires `sig` from that test → **H1 can never be `confirmed`**; it will be
"inconclusive" even if M1x violates in every K2/K3 run and M1-kafka is clean. Note §3.6 C1 itself
says "per fault condition where both are executed (F0,F1,F2,F3,F6)" — inconsistent with §3.1(iii).
**Fix:** replace (iii) with a feasible paired contrast (e.g. Stage-B F1/F2/F4a/F6 family C1, or
M1x-K2 vs M1-kafka-K2 which are both executed) and amend prereg §3.1 before the freeze.

**(b) H2's correctness rule includes the early-order probe cells that the model predicts M2 to
violate.** prereg §3.2: "M2 has zero violating runs in all executed Stage A and Stage B cells".
Stage A K4 is executed for M2 under `ordering=early`; method §6.4 (J14) and §6.3 (F4b) predict M2
**loses postings** there by design. The analyzer implements the literal rule
(`m2_cells = M2 & stage in (A,B)`; `decide_hypotheses` H2 line ~670) → H2's correctness component is
**refuted by construction** whenever the probe works. Same for **H4** (line ~738: `reachable_zero`
includes Stage A/B; M4's K6 is a predicted I3 violation per P9 and F4b/K4 probes predict I4 for
M4+M3) → H4 **refuted by construction**. **Fix:** the decision rules must distinguish the
*canonical mechanism* cells from the declared *degraded-variant probe* cells (K4/F4b early). Amend
H2/H4 to "all executed **canonical** (strict-ordering) Stage A/B cells, excluding the ordering probe
cells", with the probe reported separately. Alternatively exclude probe cells from the hypothesis
tests by pre-registering that they are a separate evidence class — but that must be written before
the freeze.

**(c) H3(iii) has no control cell.** prereg §3.3/H3 line 150: "Stage A K4 comparison
M3-`early` vs M3-`strict` (control)". Stage A has no M3-strict K4 run: K4 is executed only in the
early variant, and the only strict M3 control is a **different condition** (`ctrl`, no injection) with
a different data seed (seed formula includes the condition token; `matrix.py:_seed`), so runs are not
paired by experimental material and a McNemar pairing by rep index is invalid. The analyzer
nevertheless fabricates pairs by rep (`analyze.py` C2 lines ~824–835) but **self-pairs M3 with M3**
in `_paired_field("M3","M3",…)`: `by_mech[mech_a]` and `by_mech[mech_b]` are the same record, so every
discordant count is 0 → p is 1/not applicable → H3 can never be confirmed. **Fix:** add a proper
M3-strict K4-like control (e.g. a Stage-A `k4s` control injected with a harmless pause at the same
phase marker and pairing seed) or use the existing `ctrl` cell as an **unpaired** Fisher comparison
and label it as such; amend prereg §3.3/H3 and family C2.

**(d) H5's dominance criterion is vacuous under (b).** prereg §3.5: dominance requires "zero
violating runs in **every** executed correctness cell (Stage A and B)". With the probe cells included,
no two-commit mechanism can be zero-violating (all lose in K4/F4b by model design), and M1-kafka's
own cells are partly n/a — so the "no mechanism dominates" conclusion is **confirmed by
construction**, not by measurement. The analyzer's H5 (`worst_correct` over all Stage A/B valid runs)
therefore reports `confirmed` whenever Stage-C blocks exist, as demonstrated on the synthetic fixture
(`H5: confirmed`). **Fix:** define dominance over canonical behavior cells only and state that probe
exclusions are part of the definition; or replace H5 with the pre-registered trade-off contrast
(M3 vs M4 vs M2 on the declared exposure classes) and report "no mechanism dominates within the
tested model" with the probes treated as separate evidence.

**Disclosure:** prereg §3.1–§3.6 (decision rules and families), §7.2/§7.7; method §6.5; paper's
hypothesis-results section and the decision table. These are author-level amendments; the lead should
bundle them into a single P2 amendment with the probe definitions and re-freeze.

### F-P2 (major) — prereg §2.2 row arithmetic is wrong (rows sum to 69, stated total 67)

The mechanism table's executed-fault-cell column lists `M4 8 (F4b n/a)` and `M4+M3 8 (F4b n/a)`:
8+5+8+8+8+8+8+8+8 = **69**, but the stated total is 67 (72−5). The 67 total is correct (matrix:
M4 and M4+M3 have 7 executed fault cells each; independently recomputed, §5). Fix the two row cells
to 7 and add "F4b n/a" to the total row; this is a documentation-only fix *if* done before freeze
(after freeze it is a DEVIATIONS entry).

### F-P3 (major) — seed formula encoding is not the frozen formula (separator ambiguity, unrecorded)

prereg §4.4: `data_seed(stage, condition, rep) = SHA-256(master_seed | "data" | stage | condition |
rep)[0:16]`. `matrix.py:_seed` implements **bare concatenation, with no separators and no "|"**
(line 81: `text = MASTER_SEED + kind + stage + condition + str(rep)`), and `matrix.yaml` records that
formula. P2-RULINGS R1 ruled the seed *text encoding* (hex→decimal) but not the formula separators;
`docs/prereg.md` still shows the `|` notation. This is harmless to the science (all runs share the
implementation), but it is an unrecorded interpretation against a frozen formula. Fix: record the
concatenation reading in `docs/P2-RULINGS.md`/prereg (P2 amendment) or implement literal `|`
separators and regenerate (would change every seed → unnecessary). Verified: all 7,871
`data_seed`/`injection_seed` values in `run_order.csv` recompute exactly under the implemented
formula (§5).

### F-P4 (major) — Stage-A fault tokens are lower-case in the matrix, violating the run-record schema and the analyzer

Covered mechanically in F-E1; recorded here as a **prereg/interface** defect: prereg §5 defines
`fault` as `F0..F6`, `K1..K7` or `none`; `validate_record.py` enforces upper case; `matrix.py` emits
`k1..k7`,`ctrl`. In addition to breaking the failpoint map, `validate_record` rejects any Stage-A
record with lower-case `fault`, and the analyzer's Stage-A filters (`{"K2","K3"}`,
`{"K3","K5"}`) silently match nothing. Fix in `matrix.py` (upper-case `fault` for Stage A, keep
lower-case `run_id` tokens) and regenerate/re-hash matrix + order before the freeze.

### F-P5 (major) — analyzer statistical/accounting defects (beyond F-P1)

1. **Censoring**: `extract()` copies `violating_run` from the record only
   (`analyze.py:407`); no path ORs `quiescence.censored` into the correctness outcome. Method §7.3
   says explicitly "censored runs are included in the correctness primary outcome (they are violating
   runs)". ⇒ a run that never quiesced but whose final dump happened to be clean is scored **clean**
   (a false-safety direction). Fix: `violating_run' = violating_run ∨ censored` for correctness
   (pre-registered wording allows it: method §7.3), or add a separate primary "censored or violating"
   outcome and use it in H2/H4/H5.
2. **`n_planned` is redefined**: `correctness_by_cell` sets `"n_planned": len(group)`
   (`analyze.py:464`) — the number of *records that exist*, not the frozen N. A cell with 5 surviving
   records reports `N_planned=5`, hiding the 25 missing/excluded runs in every downstream table. Fix:
   join with `matrix.yaml`/`prereg-freeze.json` for the frozen N and emit `N_planned`, `N_reserved`,
   `N_valid`, `N_excluded`, `N_missing` separately (as DEVIATIONS already does).
3. **Validation-run contamination**: `load_records` loads every `*.json` in the records directory;
   the 20 validation runs use stage `pilot` and share `mechanism×fault` keys with pilot cells;
   `counts_by_mechanism` groups only by mechanism across all stages, and `correctness_by_cell` does
   not filter `cell_id` starting `V.` → synthetic/probe validation runs and pilot runs are mixed into
   confirmatory tables (and H3's `m3_all` includes pilot runs). Fix: exclude `cell_id` matching
   `^V\.` from all tables/hypotheses and keep validation evidence separate (as prereg §2.3 says).
4. **Cost/latency direction is not checked**: `_cost_confirmed` (`analyze.py:908–921`) returns
   `ok = abs(delta) ≥ 0.33 and p_adj < 0.05`, never checking the sign. So "M2 is worse than M3"
   is confirmed if M2 is *better* but the difference is large; the doc note even says
   "pre-registered direction: M2 worse than M3" while the H4 call uses the same helper for
   "M4 **better** than M2" (note backwards). Fix: direction-aware comparisons per prereg §3.2/§3.4
   (one-sided in the pre-registered direction, with the two-sided test reported).
5. **P3 mixes fault/fault-free runs**: time-to-consistent-state pools all 100k/P6 valid runs
   (`analyze.py:950`) across fault conditions and stage C; prereg §7.4 is per cell. Report per
   mechanism×fault and restrict the confirmatory contrast to the pre-registered cells.
6. **H1's `mk_all` uses every M1-kafka valid run** (line 630) — including F6 (F-E4) and any future
   exploratory cell — which makes H1's clean arm depend on cells outside the pre-registered H1 set.
   Restrict to the cells named in the decision rule.
7. **`extract()`'s `attested` for L1/F4b** counts `len(fired)==1` for L1, ignoring restored, which
   disagrees with the runner's exclusion logic (F-I1/F-I2); after the receipt protocol is fixed,
   derive `attested` from the same frozen rule in both places (single source of truth).

### F-P6 (major) — the freeze and the deviation register are being maintained by hand-tuning; freeze must be re-generated *after* all fixes

`docs/prereg-freeze.json` currently matches the current file hashes (verified: runner
`3c6dd690…`, analyze `c6ea48ec…`, make-figures, validate_record, deviations, freeze all MATCH), i.e.
S4 has already re-frozen twice while the code was changing. That is fine before Stage A **only if**
the final freeze is regenerated after the last edit and no file changes afterwards. The freeze also
claims evidence ("S3 Java GroundTruthDigest reproduced all four committed digests byte-for-byte;
normative rows … asserted by experiments/tests/test_oracle.py (14 tests green)") — the 14 oracle
tests do pass (verified), and the Java golden-digest cross-check passes (verified), but the freeze
does not record *how* to re-verify, and it omits the artifact source hashes (only the jar is used by
the runner; the Java sources could change without the jar being rebuilt — the runner's preflight
checks only the jar's existence). Required: `experiments/freeze.py` gains a `--verify` mode run by
the runner preflight (refuse to start if the working tree hash ≠ freeze); freeze includes the Java
source tree hash and the built jar digest; the freeze is regenerated once after all P2 fixes and
recorded in DEVIATIONS. Also: `results/DEVIATIONS.md` does not exist yet, and the frozen
interpretation register (`deviations.py:INTERPRETATIONS`) already lists R1–R5 + S4-1..S4-4 but not
the many *behaviour-changing* deviations this memo identifies (Stage-A fault case, K4/K5/K6
realisations, F4b L2 substitution, F6/M1-kafka, rate-table fallback, censoring semantics). Every
behaviour change must be in that register (prereg §12) or fixed away.

### F-P7 (minor) — matrix/order compliance is otherwise correct (positive) — see §6 for the recomputation

---

## 6. Findings — measurement contract and fairness (attack surface 7–8)

### F-M1 (major) — the performance pipeline is not implemented; warm-up is not excluded; the generator-lag covariate does not exist

**Claims under attack:** prereg §4.5 (warm-up = first max(2000, 10 %) excluded from performance
metrics; steady state from end of warm-up to last commit), §5 `performance` block; method §7.2
(open-loop latency, `l(e)=t_effect_commit−scheduled_at`, generator lag recorded), §7.3 (throughput =
committed events / steady-state duration).

**Evidence:**

1. **Warm-up is not excluded.** `RunMetrics.recordEffectCommitMs` (lines 77–93) records *every*
   committed event into the HdrHistogram; no seq threshold exists anywhere in the Java artifact or
   runner. The run record's `performance.warmup_events_excluded = max(2000, 0.10·N)`
   (`runner.py:_finish`) is a **lie**: the histograms include the warm-up events. HdrHistogram dumps
   cannot retroactively remove the first W events (no event identity in the dump). Therefore p50/p99/
   p99.9 currently cannot be computed per the prereg at all.
   *Fix:* pass the warm-up count into the service (same shared config for all mechanisms), skip
   `recordValue` for `seq < W`, and record the skipped count plus both histograms (with/without
   warm-up) in `app_*.json`.
2. **Throughput is not computed.** `runner.py:_finish` reads
   `hist_<run>_1.json` and looks for top-level `throughput_eps_median`, `p50_ms`, … — but
   `RunMetrics.writeHistograms` writes `{"event": {p50_ms…}, "batch": {…}}` with **no**
   `throughput_eps_median` and no top-level percentiles (`RunMetrics.java:124–149`), and only
   instance 1 is read (C=2 instances exist; their samples are never merged). So
   `performance.throughput_eps_median`, p50/p99/p999 are all `null`; Stage C/P1/P2/H2/H4/H5 cost
   components cannot be computed. The app does not record a commit-timestamp series or a
   steady-state window; throughput cannot be reconstructed from the run record. *Fix:* compute
   per-run throughput in the runner from the loader's ack times + the app's committed-event counter /
   commit-observation window (fault-free runs), merge C instances' histograms correctly (sum counts;
   percentile merge per HdrHistogram merge API), and write exactly the prereg §5 keys; add a
   run-record validator cross-check that rejects a null performance block for Stage C (unless
   censored/failed).
3. **Generator lag / actual send time are not recorded.** method §7.2 requires per-event
   `actual_send − scheduled_at_ms` and a flag for p99 lag > 5 ms. The produced manifest has only
   `{event_id, partition, offset, ack_ts_ms, scheduled_at_ms}`
   (`LoadGenerator.java:183–188`; CONTRACTS §6 same), so the covariate cannot be computed. *Fix:* add
   `send_ts_ms` to the manifest and a per-run `generator_lag` block to the run record.
4. **`t_load_start` includes GT generation** (`runner.py:_start_load_generator` starts the JVM after
   recording `t_load_start`; the loader writes+hashes the whole GT first, `GroundTruth.write` +
   fsync path), and `t_load_end` is taken after the loader process exits. For 1M runs (GT = 2M lines)
   this inflates the window and deflates throughput. Use the loader-reported `load_start_ms` /
   `load_end_ms` (the loader already prints them in its result JSON, `LoadGenerator.java:204–206`;
   the runner currently discards the loader's stdout result).
5. **Latency clock resolution / clamping:** latency is `commitMs − scheduled_at_ms` using
   `System.currentTimeMillis()` (1 ms) recorded into a µs histogram (`*1000`), and clamped to
   `[1, 60 s]`; at target rates the distribution is coarse-grained. Fine as a recorded limitation,
   but the paper must state it; consider `nanoTime` for the commit instant relative to a
   boot-anchored clock.
6. **Instance aggregation / crash runs:** histograms are written only on graceful shutdown
   (`ServiceRunner.writeApplicationArtifacts`); a killed instance writes **no** histogram, so
   fault-run latency/throughput (Stage D) silently excludes the crashed instance's committed
   events. State it or reconstruct from the loader/offset data.
7. **M2 relay not drained before observation:** quiescence never checks the outbox relay
   (`F-G2`), so M2's `pub_*` metrics are taken mid-relay and Stage-D time-to-consistency excludes
   relay cost. Either include the relay in quiescence or state the definition explicitly.

### F-M2 (major) — "fsynced" files in the Java artifact are not fsynced (silent `instanceof` no-op)

**Claim under attack:** CONTRACTS §4 ("both fsynced"), A1 §4/D10, method §7.4 ("fsynced before the
first produce"), documented manifests.

**Evidence:** `IoUtil.writeFsync` (IoUtil.java:33–45) and `GroundTruth.write/Files.newOutputStream`
(GroundTruth.java:58–81), `LoadGenerator` produced-manifest writer (172–195) and the atomic-config
writer use:

```java
try (var fos = Files.newOutputStream(...)) { … if (fos instanceof java.io.FileOutputStream f)
  f.getFD().sync(); }
```

`Files.newOutputStream` returns a `ChannelOutputStream`, never a `FileOutputStream`, so the branch is
**never taken**: GT file, GT `.sha256`, manifest, `produced_*.ndjson`, config and app JSON are
flushed but **not** fsynced. (Process-crash safety is unaffected because the OS page cache survives;
OS-crash durability — excluded by AS1 — is what is lost.) The unit tests do not detect it (they test
`IoUtil.sha256Hex`, not fsync).

**Failing scenario:** a run that claims "GT written and fsynced before produce" is only
write-ordered, not durable; a system-level crash during the campaign could leave GT truncated while
the source topic has the events. Also the paper/contract's durability statement is false.

**Required fix:** use `FileChannel.force(true)` (or `new FileOutputStream(path).getFD().sync()`) and
add a test that opens the file from a second handle after a simulated kill (or at least asserts
`FileChannel.isOpen`/uses a `FileDescriptor`). Also fsync `faults.log` receipts in the Java
`FaultsLog` (CONTRACTS §6 claims the writer fsyncs; `FaultsLog.receipt`→`IoUtil.appendLine` does not)
and choose one writer for the cloud-synced `results/faults.log` (Java app + S4 injector both append;
Windows `O_APPEND` interleaving is not guaranteed for >4 KiB writes and the venue claims an
append-only log).

### F-M3 (minor) — fairness/measurement details

- `publishInline` (M0/M3/M4) blocks on futures (≤60 s) inside the batch path, while M2's relay does
  not; M1x/M1-kafka write in the Kafka transaction. This *is* a mechanism-intrinsic difference
  (declared), but the paper's fairness paragraph (method §1.3/§4.11) should state that M2's
  publication cost is off the critical path and that latency is measured at effect-commit for all
  mechanisms (so the publication path is excluded from latency for everyone).
- Java `appendLine` opens a new file handle per receipt (slow under faults; fine).
- `RunMetrics.usToMs` rounds to 0.1 ms (`Math.round(us/100)/10`), fine, but the record's p99 will be
  quantised; state it.
- `BoundedHistogram` p999 uses `getValueAtPercentile(99.9)` — correct HdrHistogram usage (positive).

---

## 7. Findings — formal model (attack surface 6)

### F-F1 (major) — the "machine-checked bounded model" claim has a narrower scope than method §6.7 FO-6 promises

**Claims under attack:** method §6.7 FO-6 ("checked … crash points at the four decisive locations
C3, C4, C5, **C7**; report bounds and invariant set {I3,I4}"), formal/README "machine-checked at P2",
and the paper's planned appendix claim.

**Evidence (`formal/ledger.tla`, `formal/README.md`, `formal/tlc-results/`):**

- **C7 (zombie) is not modelled at all** — no second writer, no fencing epoch, no rebalance; README
  §"What is deliberately not modelled" admits it (line 56) but method §6.7 FO-6 promised C7. So
  K6/M4's exposure (P9), RQ3/E3 and the M4+M3 rationale are **not** machine-checked; the paper must
  say so or the model must be extended.
- **I2 (append-only) is not modelled** — the ledger is a multiplicity function with no mutation
  operation; the checked invariant set is `TypeOK, I1Balance, I3Unique, I4CompleteWhenDone,
  StrictProgressBound`. I2/P11 (and spurious) have no formal counterpart.
- **The publication stream is not formally checked** except as a side-effect state: `StreamCompleteWhenDone`
  is defined but **not listed in any `.cfg`** (all eight configs list only the five invariants
  above), so "kafka-txn: all hold, stream complete" (README row 05) is not a TLC result. Config 06
  checks only `I3Unique` (the ledger duplicate); the exactly-once-stream half of M1x is a comment.
- **`StrictProgressBound` is a tautology by construction**: `EffectCommit` sets `effectMax =
  NextProg(progress)`, `ProgressCommit`/`AtomicCommit` advance both, and `DoEarly=TRUE` makes the
  implication vacuous; there is no configuration in which it can fail. Checking it adds no evidence;
  the paper should not count it as a checked property (or should replace it with a falsifiable bound,
  e.g. "progress ≥ effectMax + batch ⇒ fails").
- **I4 is a state invariant, not liveness**: `I4CompleteWhenDone == done ⇒ complete`, and `Finish`
  requires `progress = N`; "liveness under fair recovery" (A1 §3) is not checked (no fairness, no
  temporal property). The paper must not describe it as machine-checked liveness.
- **Model↔mechanism mapping is loose**: M2 is mapped to the `dedup` config (DoDedup only) even
  though its ledger transaction is atomic and its progress commit is separate; M4's atomic config
  (`03-atomic`) does not model its publication; M1-kafka is `DoAtomic ∧ DoTxn`, but the model's
  `AtomicCommit` adds ledger postings generically (same code path as M4), so the "Kafka sink" nature
  is only in the `stream` bookkeeping. This is defensible as an abstraction only if the paper states
  exactly what each boolean represents and which real mechanism component it abstracts.
- **Evidence-quality nits:** README says "Distinct states found: 20, 18, 8, 8, 8, 8, 21, 21 for
  configurations 01–08", but `tlc-summary.json` records `20, 18, 8, 8, 8, 8, 22, 21` (07 = 22, 06 =
  39). The README's own variance note explains why the count moves for the violating configs, but the
  hard-coded sequence is inconsistent with the committed summary; fix the README to cite the summary
  rather than a stale list.
- `formal/README` line 18 says "All eight configurations matched the pre-registered expectations
  below; no expectation was edited after checking" — `tlc-summary.json` "Expected/Observed/Match"
  records are consistent with the README table; this is fine as a bounded-witness claim.

**Required fix / disclosure:** add a "scope of the machine-checked claim" paragraph to
`formal/README.md`, method §6.7 and the paper: *bounded safety model (N=3, BatchSize=2, single
partition) of I1/I3 and I4-at-termination for eight mechanism abstractions; not modelled: C6/C7,
I2, liveness, publication-stream invariants, partial batches*. Either extend the model with a
two-writer zombie fragment (recommended if FO-6/C7 is kept) or remove the FO-6 C7 promise and state
that RQ3/E3 evidence for the zombie rests on method/argument, not TLC. Mirror the TLC summary into
`t5-tlc.tex` only after these edits.

---

## 8. Findings — runner validity/record pipeline (attack surface 3/5)

### F-RN1 (critical) — the live path cannot emit a schema-valid pilot or Stage-A record; invalid/failed runs are not recorded per contract

**Evidence (current `runner.py`):**

1. `LiveSequence._finish` writes `"stage": self.stage if self.stage != "V" else "pilot"`
   (line ~1222). `self.stage` is `run["stage_label"]`, i.e. `"P"`/`"A"`/`"B"`/`"C"`. `"P"` is **not**
   in `validate_record.STAGES` `{pilot,A,B,C,D}` → every pilot matrix run record fails validation;
   `"A"`/`"B"`/`"C"` are fine. (`"V"`→`"pilot"` is handled.)
2. `"fault": self.run["fault"]` (line ~1224): Stage-A rows carry `k1..k7`,`ctrl`; `FAULTS`
   requires `K1..K7`/`none`/`F0..F6` → every Stage-A record fails validation. (`ctrl` is also not in
   `FAULTS`.)
3. `_finish` writes the record, then `if errors: raise RuntimeError(...)` (line ~1251). The caller
   `_live_one` catches **all** exceptions and returns `{"failed": …}` with no `invalid` record for
   pre-record failures; the invalid record that *was* written remains on disk but is not flagged
   `excluded` (its `validity.valid` may be True). Nothing in the pipeline validates records before
   analysis (`tools/analyze.load_records` loads any JSON and does not run the validator).
4. Consequences: Stage-A/pilot data are either absent (validation raise → reported failed) or present
   as "valid" records with out-of-schema `stage`/`fault` that the analysis silently mis-buckets
   (`{"K2","K3"}` filters never match `k2`/`k3`; `stage=="P"` runs are excluded from H2/H4 by the
   stage filter but included in H3's `m3_all` and in `counts_by_mechanism`).
5. Also missing from the live record: full config (the `_config_block` stub; prereg §5 requires the
   full shared+intrinsic configuration and `config_sha256`), host/versions blocks are taken from the
   preflight (OK), `performance` values are null (F-M1), `t_fault_fire` only set for L2 (L1 fails
   before it), `fault_receipts` include app receipts plus S4 restorations (mix of names), and
   `validity.notes` claims `"live run"` even when dumps failed.

**Required fix:** map stage labels and fault tokens at record-write time (single canonical mapping
function used by both matrix and record: `P→pilot`, `V→pilot`, `k3→K3`, `ctrl→none`); write a record
*always* (including invalidity, with `excluded=true` and an exhaustive `exclude_reason` from prereg
§8.2's closed list); run `validate_record` **before** writing and treat a validation error as an
internal bug that fails the campaign (not a "failed run" to skip); implement R_max reruns for
invalid runs (F-I4); populate the §5 config block from the app's `config_<run>_<instance>.json`
(including `shared_sha256`/`intrinsic_sha256`) and the full effective config; have the analysis
re-validate every record on load (or verify `oracle.result_sha256` and record schema version).

### F-RN2 (major) — residue/readiness checks are weaker than the protocol they claim to enforce

- `_residue_check` only asserts no foreign `run_%` schemas exist (`runner.py`); prereg §4.1 step 2
  also requires topic list, consumer group list and PG session checks. Topics/groups are deleted with
  `check=False` and their absence is not verified; a failed delete silently carries residue into the
  next run (fresh run_ids make collisions unlikely, but the prerequisite is not met).
- `_wait_ready` accepts a `ready_*.json` with status `subscribed`/`assigned`; prereg/method require
  group stable, lag = 0 and no errors for 5 s before load. The load generator can start before the
  group has partitions assigned (the readiness file is written on `subscribe`, before assignment),
  contaminating early-run latency and F-fault phase targeting.
- The periodic full-stack restart (`_full_stack_restart`) invokes a placeholder
  `python -c pass` log line then the native-stack verb; the `fault_restart.log` record is written
  before/after? (it logs, but the run record's environment block does not capture the restart epoch;
  method T-I4 order-drift diagnostics will not see restarts).
Fix: implement the exact residue/readiness assertions from prereg §4.1, record their results in the
run record (`validity.residue_check` must be evidence-backed), and record restart epochs.

### F-RN3 (minor) — clock/host sanity check is hard-coded

`clock_check = True` (F-G1). Implement the prereg §8.2 item 4 clock probe (compare
`Get-Date`/WMIC against a monotonic reference before/after each run, record offsets; flag
`clock_step`), and set `residue_check` from the actual assertions. Same for `process_limits`: the
record currently hard-codes `hikari_pool: 10` while the frozen config is 8 (`EffectiveConfig.java:224`
`pool_size=8`; method Appendix C says 8) and `affinity_mask: None` — the fairness audit cannot
verify pinned limits from the record.

---

## 9. Findings — matrix/order/seeds and G5 recomputation (attack surface 5, verification)

### F-R1 (minor, documentation) — recomputed counts are correct; three small documentation/interface defects

Independently recomputed from `experiments/matrix.yaml` + `run_order.csv` (commands §10):

| Quantity | Expected (prereg) | Recomputed from committed files | Verdict |
|---|---|---|---|
| Pilot planned / executable | 216 / 201 | 216 / 201 | ✔ |
| Pilot n/a cells | 5 cells (15 slots) | 5 cells, all `executed=false` | ✔ |
| Stage A cells/runs | 52 + 9 controls / 1,830 | 61 cells, 1,830 runs | ✔ |
| Stage B cells/runs | 67 / 4,020 (2,010+670+1,340) | 268 cells, 4,020 runs | ✔ |
| Stage C cells/runs | 72 / 1,800 (1,620+90+90) | 72 cells, 1,800 runs | ✔ |
| Totals | 7,866 planned / 7,851 executable (+20 validation) | 7,866 / 7,851 / +20 = 7,871 order rows | ✔ |
| Stage-A executed crash classes per mechanism | 6,4,6,7,6,5,7,5,6 = 52 | same | ✔ |
| Stage-B executed fault cells per mechanism | 8,5,8,8,8,7,8,7,8 = 67 | same (matrix correct; prereg §2.2 rows for M4/M4+M3 say 8 — F-P2) | matrix ✔ / prose ✘ |
| Controls | 270 | 9 control cells × 30 = 270 | ✔ |
| Ordering=early | only K4 (Stage A) and F4b cells | 618 early runs = 240 K4 + 378 F4b; no others | ✔ |
| Pairing | same `data_seed` per (stage,condition,rep) across mechanisms | 0 violations of the pairing property | ✔ |
| Seeds per §4.4 | reproducible | 7,871/7,871 recompute exactly (implemented concatenation; F-P3) | formula wording ✘ |
| Fisher–Yates constraints | ≤3 consecutive same mechanism; ≤40 %/60-run block; stages P→A→B→C | max consecutive = 3; worst sliding 60-window share = 28.33 %; no day-block/share violations; P/V then A,B,C contiguous | ✔ |
| Run ids | unique, lower-case, topic-legal | 7,871 unique, `[a-z0-9-]+`; `sat` token on 90 capacity runs | ✔ |
| `matrix.sha256` | matches files | matches `matrix.yaml` + `run_order.csv` | ✔ |
| Stage A fault tokens | `K1..K7` per §5 schema | emitted lower-case (`k1..k7`) | ✘ (F-P4) |

The ordering seed (`order_seed`) is recorded in `matrix.yaml`/`prereg-freeze.json`; the order is a
seeded Fisher–Yates shuffle followed by deterministic constraint repair (allowed by prereg §4.3's
"subject to"), and the realised constraints hold. `bootstrap_seed` derivation is recorded. This part
of the design is solid.

Additional small defects:
- `matrix.yaml`/`run_order.csv` were generated at 09:56; `matrix.py` (09:56:20) and
  `matrix_design.py` (09:55:48) are consistent with them; no post-hoc cells exist (verified by set
  equality of order rows vs matrix cells×reps + validation, 0 missing / 0 extra).
- `matrix.py` writes a `rate`/`rates.json`-independent matrix; the missing rate table is referenced
  by the runner (`RunnerOptions.rate_table = experiments/rates.json`), which does not exist yet
  (pilot output; pending, not a defect).
- `run_order.csv` includes the 20 validation runs interleaved in the pilot block (allowed; prereg
  records them separately).

---

## 10. Oracle attack matrix — enumerated scenarios and current detection

Legend: ✔ detected; ◐ detected only via an unratified/partial rule; ✘ not detected / would be scored
clean. "T#" = demonstration in §11.

| # | Scenario (concrete input) | Current oracle verdict | Invariant / rule | Evidence |
|---|---|---|---|---|
| 1 | Partial batch at crash: some events of a batch committed, rest lost | ✔ lost_postings>0 → v4 | I4/P6 | code review; loss class T-drop |
| 2 | Duplicate `(event_id, leg_no)` with **different** legs/account | ✔ v2 (mutated) and v3 if multiplicity>1 | I2/P5, I3/P2 | T3 |
| 3 | Same event id re-inserted under a different account (single row) | ✔ v2 mutated | I2/P5 | T3 |
| 4 | Amount overflow / negative amount on both legs (sum still zero) | ✔ v1 (amounts>0 structural) + v2 mutated; DB CHECK also blocks | I1a, I2 | T7 |
| 5 | Currency mix (D in BRL, C in USD) | ✔ v1 + v2 | I1a, I2 | T8 |
| 6 | **Balanced spurious entry** (fabricated event id, both legs, sums zero) | ◐ `v1..v4` false; caught **only** by the unratified `spurious>0` verdict extension | method §2.4 (harness-invalid) vs oracle extension | T2 |
| 7 | Whole-entry duplication (both legs, balanced) | ✔ v3 + **v1**; method §2.8/P11 claim that I1 passes is wrong | I3, I1a | T1 |
| 8 | Leg duplication (one row copied) | ✔ v3 + v1 | I3, I1a | mutation class / T9 analogous |
| 9 | Append-only mutation via trigger bypass (UPDATE), **persistent** | ✔ v2 content mismatch (if key still present) | I2/P5 | code review |
| 10 | Append-only mutation, update-then-revert before observation | ✘ invisible at end state (no WAL/logical decoding); oracle-side privilege attestation vacuous | residual (AS1 excludes Byzantine/operator) | F-O2 |
| 11 | Posting rows inserted by a non-app role | ✔ end-state as spurious/dup/mutation **if** persisted; privilege-attestation path vacuous | I2/I3/P7 | F-O2 |
| 12 | Source topic contains events never in GT | ✔ source_integrity=false → run invalid (not a clean pass) | G5 gate | T11 |
| 13 | GT file truncated / replaced | ✔ GT digest mismatch → invalid | G5 gate | T10 |
| 14 | Digest spoofing of the oracle result | ✔ GT digest vs Python regeneration is strong; `result_sha256` is self-signed only; sink dump has no independent digest/row-count check | G5 / retention | code review |
| 15 | Malformed sink-dump row (e.g. truncated CSV line) | ✘ silently dropped (`observed_malformed_rows` only) — can hide a dup/mutation row | provenance | T12 |
| 16 | Publication record lost while ledger complete (M1x abort signature) | ◐ `lost_records>0`, `divergence=0` (documented metric would be non-zero) | method §7.1 / §3.4 | T6 |
| 17 | Publication **content** corrupted (DB mechanisms) | ✘ only event ids extracted; legs/amounts unchecked | secondary metric | F-O5 |
| 18 | M1-kafka publication stream as ledger: duplicate/corrupt entry | ✔ (expansion + compare) but **not mutation-tested** | I3/I2 | F-O6 |
| 19 | Read-committed vs read-uncommitted publication dump | ◐ isolation recorded per mechanism; M2 relay / aborted txn timing can inflate `pub_lost` (no drain) | secondary | F-M1/F-G2 |
| 20 | Censored run whose final dump is clean | ✘ analysis scores it clean (oracle itself is fine) — method says censored ⇒ violating | method §7.3 | F-P5 |

---

## 11. Independent recomputations (commands and outputs)

All read-only against the committed files; throwaway scripts under `%TEMP%\wxeo-s5\`.
(Windows PowerShell; `REPO` = working directory.)

### 11.1 Matrix / order / seeds / constraints

```
python %TEMP%\wxeo-s5\check_matrix.py
```

Selected output (full output retained in the script):

```
pilot: cells_total 72, cells_executed 67, cells_na 5, runs_planned 216, runs_executable 201
A:     cells 61/61/0, runs 1830/1830
B:     cells 268/268/0, runs 4020/4020
C:     cells 72/72/0, runs 1800/1800
totals: 7866 planned / 7851 executable / 20 validation / 7871
order_rows 7871, unique_run_ids 7871, no missing/extra runs vs matrix (0/0)
stage blocks: P/V (pilot+validation) then A 1830, B 4020, C 1800
max_consecutive_same_mech 3; worst 60-window share 0.2833 (at index 5557, M1x 17/60)
day_block share violations: {}; n_day_blocks 132
seed_recompute_mismatches: 0 / 7871
multi-seed (pairing) violations: 0
all-digit 16-hex data seeds: 0 (the decimal/hex ambiguity in derive.py did not materialise)
unexpected early runs: 0 (after correcting the case-sensitive self-check)
saturation rows 90, all with "-sat-" token, stage c only
cells_na: P.M1-kafka.f4a/f4b/f5, P.M4.f4b, P.M4+M3.f4b (5 × 3 slots)
per-mechanism Stage-A cells: 6,4,6,7,6,5,7,5,6 = 52; Stage-B: 8,5,8,8,8,7,8,7,8 = 67
```

Additional checks: `matrix.sha256` matches both files; run ids unique/topic-legal; 1,830/4,020/1,800
as above; 270 control runs (9×30); `matrix_design.validate_design()` passes.

### 11.2 Derivation, normative vectors and Java↔Python cross-check

```
python -c "… from experiments.oracle import derive as D; …"
```

- `docs/derive-vectors.csv` (6 lead-derived rows): **6/6 match** (event_id, account_d, account_c,
  amount_minor, partition_p3, partition_p6), Python implementation.
- `experiments/oracle/g5-vectors.json` (4 vectors × 100 events): all 4 `gt_sha256` digests reproduce
  from `derive.py` (self-consistency; noted non-independent).
- Java golden digest check: Python regeneration of seed `0123456789abcdef`, 3 events =
  `f4e127d98bddf1fba21f20b8b4cc9870995626acc8c152486118c8f822ea4f72`, identical to the Java
  `GroundTruthTest` golden constant → real cross-language agreement.
- Java `NormativeVectorsTest` surefire: `Tests run: 1, Failures: 0, Errors: 0`; the Java unit test
  total from `artifact/target/surefire-reports` = 45 tests, all passing (11 suites, 0 failures).
- Oracle unit tests: `python -m pytest experiments/tests -q` → **14 passed in 3.09 s**.

### 11.3 Oracle adversarial fixtures (exact synthetic inputs)

```
python %TEMP%\wxeo-s5\attack_oracle.py
```

Uses the committed synthetic-fixture builder, 50 events, seed `0f0e0d0c0b0a0908`. The exact mutated
inputs are the ones in the script (e.g. T1 = append a copy of both rows of `eid0`; T2 = append
`[<new 32-hex eid>,1,acct:000000000001,D,500,BRL]` + `[…,2,acct:000000000002,C,500,BRL]`;
T3 = rewrite account of `(eid1,leg1)` to `acct:deadbeefcafe`; T4 = drop the attestation file;
T6 = delete the first publication line; T10 = truncate the GT file to half; T11 = source dump plus a
record with event id `e*32`; T12 = append `deadbeef,not-a-number,acct:x,D,10,BRL`). Results are
quoted in F-O3–F-O7 and the attack matrix (§10).

### 11.4 Gates / analysis dry-run (synthetic fixture, current frozen analyzer)

```
python tools/analyze.py --records results/synthetic/run-fixture/run-records \
  --out %TEMP%\wxeo-s5\analysis-synthetic --harness-dir results/harness
```

Output `gates.csv` (30 synthetic records, `n_valid=30`):

```
G3 PASS   g3-mutation.json: 720/720 detected
G1 PASS   M0 K3/K5 runs=1 I3-violating=1          ← union shortcut, N=1
G2 PENDING no fault-free control runs yet
G4 PASS   armed injections=10 missing receipts=0  ← tautological on valid records
G5 PASS   runs=30 source-integrity/digest failures=0 ← tautological
G6 FAIL   distinct shared-config hashes per mechanism: {"M0":4,"M1x":4,"M2":4,"M3":4,"M4":4}
G7 PENDING (hard-coded)
G8 PASS   runs=30 clock/residue failures=0        ← both fields hard-coded True
hypotheses: H1 inconclusive, H2 partially confirmed, H3 inconclusive, H4 partially confirmed,
            H5 confirmed
```

This is the clearest single artefact for the gate audit: G6 fails on a *clean* configuration, G4/G5/G8
pass without independent evidence, and H5 is "confirmed" vacuously.

### 11.5 Interface enum checks

```
python -c "… from experiments.validate_record import STAGES, FAULTS; …"
→ 'k3' in FAULTS: False | 'K3' in FAULTS: True | 'P' in STAGES: False | 'pilot' in STAGES: True
```

plus `failpoint_config_for_run` direct calls: `k3 → {'enabled': False}`, `K3 → {'enabled': True,
'name': 'after_effect_commit', …}`, `K4 → {'enabled': False}` (both cases);
`validate_receipts` on synthetic L2 receipts → `attested=False (fired=0, restored=1)`; on L1 pause
receipts → `attested=False (restored=2)`.

### 11.6 Live-dump symbol check

```
python -c "import experiments.oracle.oracle as O; print(hasattr(O,'time')); print('time.time()' in
inspect.getsource(O.dump_source_topic))"
→ has time attr: False
→ uses time: True
```

---

## 12. Genuinely solid (do not over-correct)

1. **Research design and factor structure.** The 9-mechanism × {F0–F6, K1–K7} × {1k,10k,100k,1M} ×
   {3,6,12} × N staged design is coherent, the n/a reachability proofs are documented per cell, and
   the matrix/order/counts recompute exactly (F-R1). Controls (M0 negative, M1-kafka positive) and
   the 20 validation runs are correctly separated from confirmatory cells.
2. **Determinism contract.** The event derivation (hash domains, seed decimal encoding, GT line_no /
   ordering / canonical JSON / digest) is implemented independently in Java and Python, and the
   cross-language evidence is real: all 6 lead-derived normative vectors match both implementations,
   the Java golden GT digest matches Python byte-for-byte, and the per-run oracle check
   (GT file digest == Python regenerated digest) is sound. This key independence claim survives
   adversarial review.
3. **Oracle predicate content checks for persisted state.** The I1a/I1b, I2 content, I3 posting/entry,
   I4 loss and spurious-key computations are correct for every case I could construct where the
   mutation persists in the sink dump (T3, T7, T8, T9; negative/currency/duplicate/mutation all
   detected). The sharded/memory-bounded design is appropriate for 1M-postings and its canonical
   ordering argument (eid-prefix shards preserve global order) is correct.
4. **Matrix randomisation.** The seeded Fisher–Yates + constraint repair satisfies prereg §4.3
   (max 3 consecutive, ≤40 %/60-block, stage-contiguous) on the *realised* order, with 7,871 unique
   topic-legal run ids and exact seed pairing for McNemar.
5. **Freeze discipline exists.** `experiments/freeze.py`, `docs/prereg-freeze.json` (file hashes,
   seeds, provenance hashes, harness evidence) and `experiments/deviations.py` (generated
   planned-vs-executed diff with N_planned/N_valid/N_excluded/missing/reasons, exclusion-rate flags,
   n/a register) are the right mechanisms; they need to add a `--verify` preflight mode and be
   regenerated after the fixes (F-P6), not be redesigned.
6. **TLC model is real and reproducible.** Eight configurations, committed logs, `tlc-summary.json`
   with version/UTC/SHA-256/distinct states, pre-registered expectations matched; the bounded-witness
   scope is honestly described in `formal/README.md` §"What is deliberately not modelled" (the
   problem is the mismatch with method §6.7 FO-6, F-F1).
7. **Analysis plan (frozen script) is a serious implementation** of the pre-registered statistics:
   Clopper–Pearson (beta.ppf) and one-sided upper bounds are correct, exact McNemar via binomial,
   Fisher, Holm, seeded bootstrap (sha256(bootstrap_seed|label), B=10,000), Cliff's delta with
   thresholds, Friedman blocks = load×partitions, Nemenyi CD, zero-event template and macros/tables.
   The defects are the *decisions fed into it* (F-P1/F-P5), not the statistics code.
8. **Good-faith hygiene:** synthetic artifacts are labelled `synthetic` in metadata and the analysis
   propagates a `dataset`/warning into every artefact; the G3 mutation report is honest that it is a
   synthetic corpus; the oracle JSON is self-describing (`VERDICT_RULE`, digests, warnings).

---

## 13. Pending (not defects) — must not be padded as findings

These were absent/incomplete because S3/S4 were still working at review time; they need delivery but
are not evidence of a wrong design:

1. `experiments/rates.json` (pilot-measured fixed/saturation rates; prereg §6.2). Runner currently
   falls back to `"max"` — this is a *must-fix* in F-I3 but the rate table itself is pilot output.
2. G7 evidence (generator capability ≥3× max throughput; T_quiet doubling) — the 2 quiescence
   calibration runs exist in the matrix but no measurement/report path.
3. `results/DEVIATIONS.md`, `results/RUNLOG.md` campaign rows beyond the dry run, `results/faults.log`
   (no campaign runs yet).
4. `docs/P2-GATE.md` (P2 stop document), the filled projection worksheet in `docs/prereg.md` §9.2, the
   author's P2 budget approval.
5. Stage-C performance post-processing (throughput aggregation, merged histograms) and the
   Stage-D/quiescence analysis path.
6. The Java integration tests (`*IT.java`, PostgreSQL/Kafka) — built, not yet run against the native
   stack; the artifact unit tests (45) pass.
7. The 20 validation runs + the two gate-control cells for G2 at Stage-A scale (F-E5 adds M0 K3/K5
   validation runs).
8. `artifact/**` source-to-jar provenance (jar exists and is used; a rebuild digest check is pending).

---

## 14. Must-fix list before Stage A (ordered; owner)

**P0 — do not run anything until these land (they invalidate runs or destroy the data):**
1. `oracle.py`: import `time`; add live-dump smoke test (F-O1).
2. Receipt protocol: single-owner `fired`+`restored` per run; fix L1 pause double-restored; assert
   the action happened (pid dead / toxic present / offsets moved) before writing `fired` (F-I1/I2/I3).
3. Stage-A wiring: normalise `fault` to upper case in `matrix.py` → regenerate + re-hash matrix/order;
   add `K4`; give K2/K5/K6 realisable, semantics-faithful interventions or mark them n/a with proofs;
   role-instantiation for M1-kafka (F-E1/E2/E3, F-P4).
4. F6: exclude M1-kafka (or label exploratory), write-capable rewind role for M4, k-batch rewind,
   L2-F4b either stochastic or documented as deterministic (F-E4).
5. Quiescence: AdminClient lag; M1-kafka stream quiescence; `pg_stat_activity`; relay drain; dumps
   after stopping the app (F-G2).
6. Runner records/validity: canonical stage/fault mapping; always write an invalid record; R_max=3
   reruns; record-level validator gate before analysis (F-RN1, F-I4).
7. Warm-up exclusion + throughput + generator-lag + timing source + instance merge (F-M1).
8. Gates: implement the falsifiable versions (all reserved runs, app config files for G6, per-class
   G1, implemented G7/G8, negative-control fixtures per gate) (F-G1).

**P1 — author-level prereg amendment (needed before freeze; lead drafts, author signs):**
9. Replace H1(iii), exclude probe cells from H2/H4/H5 correctness clauses (or declare them a separate
   evidence class), add a valid H3 control, and restate H5's dominance definition (F-P1).
10. Ratify the spurious-posting verdict extension (F-O4), I1a/I1b split (F-O3), censored⇒violating
    (F-P5), F6/M1-kafka limitation (F-E4), and the §2.2 row arithmetic fix (F-P2).
11. Declare the TLC model's exact scope (or extend it for C7/I2/liveness) (F-F1).

**P2 — re-verify and re-freeze:**
12. Re-run `python experiments/freeze.py`, add `--verify` preflight, freeze after the last edit;
    include Java source/jar digests; record every behaviour change in `results/DEVIATIONS.md`
    (F-P6).
13. Re-run this memo's §11 scripts; require: G1 per-class PASS with attested M0 K3/K5; G6
    cross-mechanism PASS; G2/G4/G5/G7/G8 PASS with negative controls; zero `fired`-missing exclusions
    in a pilot smoke set; a 3-run mini-pilot whose run records all pass `validate_record` and whose
    oracle `source_integrity=true`.

---

## 15. Required statements for paper/prereg (so the fixes are not cosmetic)

1. **Oracle scope:** exact verdict rule incl. spurious; I1a vs I1b distinction; read-only-role
   attestation limitation; update-then-revert invisibility; sink-dump provenance model.
2. **Probe semantics:** K4/F4b early ordering is a *degraded-variant probe*; which hypotheses use it
   and which exclude it; M4 K6 not experimentally realised (or its probe definition).
3. **Fault attestation:** what "fired" means per fault, phase/window validation, fire-rate and
   exclusion-rate tables per cell; censored-run handling as violating outcomes.
4. **Realisation table:** each K1–K7 class → failpoint/action → which mechanisms execute it → which
   are n/a with the reachability proof (replacing the current silent mismatches).
5. **TLC scope:** bounds, configs, invariant set, what is *not* modelled (C6/C7, I2, liveness, stream)
   and which claims therefore remain pen-and-paper/experimental.
6. **Measurement:** warm-up definition as implemented; throughput numerator/denominator; latency
   clock resolution; generator-lag flag; per-instance aggregation; M2 relay-drain definition.
7. **Environment:** native-process deviations (already documented), plus the source of fsync/durability
   claims (fixed implementation) and the config-hash audit procedure.

---

## 16. Appendix — commands run (reproducible; all read-only w.r.t. repository)

```powershell
# snapshot / hashes
Get-FileHash experiments/*.py tools/*.py docs/prereg-freeze.json -Algorithm SHA256
# matrix/order/seeds/constraints
python C:\Users\jvtri\AppData\Local\Temp\wxeo-s5\check_matrix.py
python C:\Users\jvtri\AppData\Local\Temp\wxeo-s5\blast_radius.py
# oracle adversarial tests (T1–T14; script text contains every synthetic input)
python C:\Users\jvtri\AppData\Local\Temp\wxeo-s5\attack_oracle.py
# oracle unit tests
python -m pytest experiments/tests -q                      # → 14 passed
# dry-run records validate
python experiments/validate_record.py <dryrun record json files>   # → 2/2 records valid
# enums / failpoint map / receipt validator (one-liners)
python -c "... from experiments.validate_record import STAGES, FAULTS ..."
python -c "... injectors.failpoint_config_for_run(...) for k3/K3/k4/K4 ..."
python -c "... injectors.validate_receipts(...) ..."
# live-dump symbol check
python -c "import experiments.oracle.oracle as O, inspect; print(hasattr(O,'time'), 'time.time()' in inspect.getsource(O.dump_source_topic))"
# frozen analysis dry-run on the committed 30-record synthetic fixture
python tools/analyze.py --records results/synthetic/run-fixture/run-records `
  --out $env:TEMP\wxeo-s5\analysis-synthetic --harness-dir results/harness
# Java unit-test evidence (built artifact, surefire reports)
Get-Content artifact/target/surefire-reports/*.txt | Select-String 'Tests run'
# TLC evidence
Get-Content formal/tlc-results/tlc-summary.json
```

No repository file was created or modified except this memo. No pilot/confirmatory experiment, no
Kafka/PostgreSQL process, and no `git` command was run.

---

*End of memo. Re-run §11 after any fix; the repository has changed repeatedly during this review and
the freeze must be regenerated after the final edit.*
