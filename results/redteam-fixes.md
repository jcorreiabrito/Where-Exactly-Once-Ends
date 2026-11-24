# S4 red-team fix pass — finding-by-finding status (post-S5 review)

Source memo: `docs/redteam-oracle.md` (verdict BLOCK, 9 critical / 19 major / 6 minor).
This file maps **every finding id** to its status, the code that addresses it and the evidence
that can be checked without trusting this report.

Legend: **FIXED** = code + regression test/evidence in place; **PARTIAL** = S4-side fixed, part
of the fix is owned elsewhere (stated); **NOT-S4** = owner outside S4 per the lead's instruction;
**PENDING** = requires another agent's artefact; **DOC** = documentation/derivation only.

Status snapshot: `python -m pytest experiments/tests -q` → see the command log appended to
`results/RUNLOG.md`; `results/harness/g3-mutation.json`; `results/harness/gate-negative-controls.json`.

## P0 — oracle and independence

| ID | Status | Fix | Evidence |
|---|---|---|---|
| F-O1 live dump `time` NameError | **FIXED** | `experiments/oracle/oracle.py` imports `time`; `dump_source_topic`/`dump_kafka_topic` gained an injectable `consumer_factory` so the real code path is exercised by a test | `experiments/tests/test_redteam_regressions.py::test_f_O1_*` (2 tests); live smoke exercises the real dumps |
| F-O2 vacuous privilege attestation | **FIXED** | `dump_pg` queries the **application role** (`wxeo_app`) privileges via `information_schema.role_table_grants`; `evaluate_attestation(..., required=True)` makes a missing attestation an I2 violation, never a warning | `test_f_O2_*` (2 tests); G3 `attestation_granted`/`attestation_missing` classes |
| F-O3 I1a/I1b collapse vs P11 | **PARTIAL (rule change pending A2)** | Oracle now reports `structurally_malformed_entries` (I1a) and `imbalanced_sum_entries` (I1b) separately in counts + attribution; run-record validator accepts them. The P11 *test rule* rewrite (`v3=1 ∧ v1b=0`) is an author-level decision in amendment A2 — S4 did not edit hypothesis rules | `test_f_O3`; G3 `duplicate_entry` class; `results/DEVIATIONS.md` S5-3 |
| F-O4 unratified spurious verdict | **FIXED (ratification pending A2)** | Explicit clause `v5 = spurious_postings>0` added to the violation vector + run-record validator; balanced fabricated entries are detected; analysis reads `violations.violating_run` verbatim | `test_f_O4`; G3 `insert_spurious_row` + `balanced_spurious_entry` classes |
| F-O5 divergence/contents | **FIXED** | Publication content sharded and validated; `pub_mutated_records`; `divergence` is the documented signed difference (stream-matched records − ledger entries incl. duplicates) | `test_f_O5`; G3 `pub_record_lost` (requires `lost_records>=1` **and** `divergence<0`) and `pub_content_corrupt` classes |
| F-O6 G3 coverage | **FIXED** | Corpora extended to 4 groups / 18 classes: DB posting defects (10), attestation (2), publication metrics (2), M1-kafka ledger via the publication-expansion path (4) | `results/harness/g3-mutation.json` (`corpora`, per-class 100 % detection) |
| F-O7 malformed rows silently dropped | **FIXED** | Malformed observed row ⇒ hard `ValueError` (invalid run); `dump_pg` records the independent DB row count and the evaluator cross-checks dump row count | `test_f_O7`; S5-3 deviation entry |

## P0 — receipts, injectors, Stage A

| ID | Status | Fix | Evidence |
|---|---|---|---|
| F-I1 no external `fired` | **FIXED** | Single-owner protocol (`experiments/receipts.py`): S4 external injectors write one verified `fired` + one `restored`; app-owned failpoints own their receipts | `test_f_I1`; all live runs validate via `receipts.validate_run_receipts` |
| F-I2 L1 pause double-restored | **FIXED** | Crash failpoints: app fired + S4 restored; non-terminal: app fired + app restored and S4 writes nothing; sham: armed+restored only | `test_f_I2_*` (2 tests) |
| F-I3 unverified action / L2 window | **FIXED** | Each S4 injector verifies its action (pid dead, toxic present, proxy disabled, offsets moved) before writing `fired`; receipt carries `action_verified` + `phase` (load window, fire time, liveness); runner excludes `uninformative_fire_outside_window`; rate-table fallback removed (Stage B/C abort if `experiments/rates.json` missing) | `test_f_I3`; runner `_collect_fault_receipts`; `results/DEVIATIONS.md` S5-2 |
| F-I4 rerun/exclusion pipeline | **FIXED** | Runner attempt loop (R_max = 3, same cell/rep seeds) and `_write_invalid_record` writes a schema-valid excluded record for **every** failed attempt; `results/raw/summaries/attempts.csv` | `test_f_I4` |
| F-I5 fire-rate denominator | **FIXED** | `deviations.compute_fire_rates` over the frozen reserved-run list + `results/faults.log`; generated `## 3b` table in `results/DEVIATIONS.md` | `test_f_I5` |

## P0 — Stage-A realisation and faults

| ID | Status | Fix | Evidence |
|---|---|---|---|
| F-E1 case tokens / missing K4 / M1-kafka roles | **FIXED** | Canonical upper-case tokens end-to-end (`matrix_design.record_fault`, validated); K4 in the failpoint map (early-ordering probe); M1-kafka semantic-role table | `test_f_E1_*`; `experiments/matrix.yaml` regenerated |
| F-E2 K5/K6 no-ops, G1 impossible | **FIXED (K6 declared n/a)** | K5 realised as protocol-level duplicate delivery (stop → rewind k batches → restart); K6 declared structurally unreachable with the documented proof (cannot pause an owner > 300 s inside T_max) and removed from the matrix; M1-kafka K5 n/a | `test_f_E2`; `matrix.yaml` reachability + `results/DEVIATIONS.md` S5-1 |
| F-E3 K2 ≡ K3 | **FIXED** | K2 is an unknown-outcome injection: Toxiproxy `timeout` toxic on the effect link at a seeded committed-progress marker (DB link for DB sinks, produce link for M1-kafka) | `experiments/injectors.py::K2UnknownOutcome`; matrix |
| F-E4 F6 breaks M1-kafka / M4 rewind role / L2 F4b | **FIXED** | F6 is n/a for M1-kafka (D5, matrix); M4-family rewind uses a write-capable admin role (explicitly an adversarial state injection, logged in the receipt); rewind is k×batch offsets; F4b Level-2 uses a seeded `--wxeo.failpoint.after=N` | `test_f_E4_*`; matrix Stage-B n/a list |
| F-E5 G1 evidence timing | **FIXED** | 6 dedicated pre-Stage-A gate-control validation runs (M0 K3 and K5 × 3) added; G1 evaluated per class | `experiments/run_order.csv` validation rows `pilot-valid-g1k3/g1k5-*`; `matrix.yaml` validation section |

## P0 — gates, quiescence

| ID | Status | Fix | Evidence |
|---|---|---|---|
| F-G1 tautological gates | **FIXED** | Gates rewritten to be falsifiable: G1 per class; G4/G5/G8 from all executed runs + raw provenance; G6 compares app `shared_sha256` across mechanisms within a cell; G7 implemented (rate table, generator-lag, T_quiet calibration); G8 real clock/affinity probes. Each gate can only PASS if a purpose-built negative fixture fails it | `results/harness/gate-negative-controls.json` (all 8 gates FAIL as required, `all_passed=true`); `tools/analyze.py::build_gate_table` |
| F-G2 quiescence never established / M1-kafka sink | **FIXED** | `experiments/kafka_admin.py` uses the AdminClient (`list_consumer_group_offsets` + `list_offsets`) for real lag; `wait_consumer_quiescence` adds PG idle-in-transaction, M2 relay-drain; M1-kafka uses `wait_publication_quiescence` (read-committed LSO stable); consumers are stopped **before** the dumps/oracle | `test_f_G2`; runner `_quiesce`/`_dump` ordering |

## P0 — runner/records

| ID | Status | Fix | Evidence |
|---|---|---|---|
| F-RN1 live record invalid | **FIXED** | Canonical stage/fault mapping (`matrix_design.record_stage/record_fault`); a record is written for every attempt (valid or excluded); validator runs before write and a validation error is an internal bug; config block read from the app's `config_<run>_<inst>.json` (with `shared_sha256`/`intrinsic_sha256`); R_max reruns | `test_f_RN1`; live smoke record (see RUNLOG) |
| F-RN2 weak residue/readiness | **FIXED** | Pre-run assertions: topics absent, group absent, schema absent (then created), exact partition count; readiness = `assigned` **and** lag = 0 for 5 s; post-run drop verifies topic absence; restart epochs recorded in `fault_restart.log` | runner `_assert_fresh_state`, `_wait_ready`, `_drop_state` |
| F-RN3 hard-coded clock/limits | **FIXED** | `clock_sanity_probe()` measures wall-vs-monotonic drift and reads the OS boot time before/after each run; process limits/affinity/priority read from the live processes and app config | `test_f_RN3`; runner `_process_limits` |

## P1/P2 — prereg, analysis plan, matrix

| ID | Status | Fix | Evidence |
|---|---|---|---|
| F-P1 unsatisfiable hypothesis rules | **NOT-S4 (A2)** | S2 is drafting `docs/prereg-amendment-A2.md`; S4 changed no decision-rule constant | `results/DEVIATIONS.md` S5-5 |
| F-P2 §2.2 row arithmetic | **DOC (lead/S2)** | Matrix arithmetic already correct (7/7 for M4/M4+M3; totals verified); the prose rows are the lead's edit | `experiments/matrix.py` assertions; `matrix.yaml` accounting |
| F-P3 seed formula wording | **DOC (lead)** | Implemented formula is recorded verbatim in `matrix.yaml`/`prereg-freeze.json`; the separator reading needs a P2-rulings/prereg note (S4 cannot edit docs) | `matrix.yaml` `seeds` block |
| F-P4 lower-case Stage-A tokens | **FIXED** | Canonical fault tokens in matrix/records; lower-case only inside run_id/cell_id | `test_f_P4`; regenerated `run_order.csv` |
| F-P5 analyzer accounting | **FIXED (mechanics)** | Censored runs count as violating (method §7.3); `n_planned` from the frozen reserved-run list + `n_missing`; validation runs (`V.*`) excluded from confirmatory tables; direction-aware cost/latency tests; P3 restricted to the pre-registered Stage-C cell; H1 clean arm restricted; H5 requires ≥4 complete blocks and non-vacuous comparisons | `test_f_P5`; `tools/analyze.py`; synthetic dry-run `results/synthetic/analysis/` |
| F-P6 freeze hygiene | **FIXED** | `experiments/freeze.py --check` run by the runner preflight (refuses to run on drift); freeze now includes the artifact jar + Java source-tree digests; regenerated after the last fix | `docs/prereg-freeze.json`; `results/DEVIATIONS.md` S5-8 |
| F-M1 performance pipeline | **PARTIAL** | Runner-side implemented: warm-up-excluded throughput from committed/acked events, generator-lag covariate when `send_ts_ms` is present, per-instance histogram percentile merge, time-to-consistent from quiescence. Service-side warm-up skip + `send_ts_ms` manifest field are artifact changes requested from S3 (recorded S5-6); until delivered those fields report `null`/PENDING, never fabricated | runner `_performance_block`, `_merge_hist_percentiles`; `results/DEVIATIONS.md` S5-6 |
| F-M2 fsync no-op | **NOT-S4 (S3)** | artifact-side (Java `IoUtil`/`GroundTruth`/manifests + faults writers); reported to lead, not modified by S4 | memo item; S3 owner |
| F-M3 fairness details | **DOC (paper/S2)** | S4 records the publication-path/latency definitions in the run record; prose statement belongs to the paper | RUNLOG/record config |
| F-F1 TLC scope | **NOT-S4 (lead)** | formal-scope wording is the lead's; `tools/analyze.py` mirrors `formal/tlc-results/tlc-summary.json` only when present | `t5-tlc.tex` |
| F-R1 documentation nits | **DOC** | matrix/order/seeds recomputed correct; the three documentation nits are lead/S2 edits | `matrix.py --check` |

## M5 (lead directive) — Windows-KRaft topic lifecycle, no per-run topic deletion

**Status: IMPLEMENTED (unit/dry-run verified; live verification still queued behind the stack
single-owner hold).** Evidence: `python -m pytest experiments/tests -q` → **48 passed**
(8 new lifecycle tests in `experiments/tests/test_runner_lifecycle.py`); dry-run record still
schema-valid; no stack start/stop performed for this change.

What changed in `experiments/runner.py`:
1. **No per-run topic deletion anywhere.** `_drop_state` drops only the PostgreSQL schema;
   source/sink topics are retained and reclaimed by periodic full-stack wipes. The lifecycle
   test asserts the runner source contains no `.delete_topic(`, `delete_topics(` or `--delete`.
2. **Attempt-scoped names** (so retries never rely on deletion): attempt 1 keeps the
   CONTRACTS-exact `wxeo.src/sink.<run_id>` / `wxeo_run_<run_id>`; attempt N≥2 appends `.aN` /
   `_aN`. Fresh state = unique names + fresh consumer group + fresh schema.
3. **Variable reset cadence by load (M5):** 1k → every 50 runs, 10k → 25, 100k → 20,
   saturation-100k → 20, 1M → every run; hard cap `MAX_LIVE_TOPICS = 400` non-internal topics
   → reset before the next run (`should_periodic_reset` returns reason `cadence`/`topic_cap`).
4. **`reset-kafka` replaces `restart-all`:** `_perform_reset` calls
   `native-stack.ps1 reset-kafka` (wipe+reformat) then `start-pg`/`start-toxiproxy`, waits for
   liveness, increments `reset_epoch`/`block_id`, records the reset in `results/fault_restart.log`
   and in the run record at `config.runner.{restart_before_run,block_id,reset_epoch,
   runs_since_reset_before,reset_cadence,reset_reason,topic_count_before/after,
   disk_free_start/end_gb,scratch_bytes}`.
5. **Broker safety preflight (live only):** each of the 3 `server.properties` must contain
   `delete.topic.enable=false` and `log.retention.check.interval.ms=3600000`; otherwise the live
   run refuses to start with a loud M5 error. (Current S3 configs do not yet carry these keys —
   the check will fail until S3 adds them, by design.)
6. **Broker-liveness guard before every run:** 3 brokers + PostgreSQL + Toxiproxy must respond;
   if not, exactly one `reset-kafka` attempt is made and the run is recorded invalid with the
   closed-list reason `environment_stack_down` (never a silent continue).
7. **Disk/topic accounting for the pilot:** every run appends to
   `results/raw/summaries/lifecycle.csv` (cadence, block, epoch, live topics before/after) and
   `results/raw/summaries/disk-footprint.csv` (free disk start/end, per-run scratch bytes); the
   cadence may be adjusted upward only if the 3 GB floor holds, and the measured values will be
   reported in the pilot projection.

Artifact binding: the freeze now records the shipped jar SHA-256 + a deterministic artifact
source-tree digest (66 files) and the runner preflight verifies both; a rebuilt jar shows up as
freeze drift and must be re-frozen before a live run (the current frozen jar is
`3fd8572311860ef7…`, which predates S3's service-start fix and will be replaced by the rebuilt
jar at the smoke step).

DEVIATIONS register: **M5** recorded (reason, cadence, no-deletion policy, attempt-scoped names,
preflight verification, liveness guard); disclosed at the P2 gate.

## Path hygiene (lead P0 addendum)

**FIXED.** `experiments/pathsafe.py` provides scrubbing and violation detection; the runner,
oracle, analyzer and figure generator record repo-relative paths / `<scratch>` placeholders; the
run-record validator rejects absolute drive-letter/UNC/home paths and the local username;
`results/synthetic/**` and `results/dryrun/**` were regenerated and contain **0** violations
(checked recursively). Test: `test_fixture_and_analysis_outputs_are_path_clean`.

## Live end-to-end smoke (required by the lead) — QUEUED (stack single-owner hold)

**Status: not yet executed to completion; currently blocked by stack ownership, not by code.**
The lead ordered a hold on all native-stack work while S3 runs its self-contained acceptance
suite. Facts established from the failed attempts (all real, all fixed/understood):
1. Over-scrubbed executable paths in `check_env` (exit 127) — fixed; paths are raw internally and
   scrubbed only in committed output.
2. S3's `SchemaTool` rejects `-` in schema identifiers → runner now uses the deterministic
   `wxeo_run_<run_id with '-'→'_'>` mapping (unique across all 7,514 reserved runs; DEVIATIONS
   S5-9). S3 confirmed the runner owns the final schema string (artifact README §8.11).
3. Concurrent Kafka start/stop/reformat by two owners corrupted the KRaft dirs (S3 acceptance
   suite vs the smoke). S3 added race guards (`start-kafka` idempotent, `reset-kafka` refuses
   while ports listen) and the lead granted a single-owner protocol.
4. K2/K5 failures traced to wrong links/roles: runner now routes F3/F4a/F5/K2 links through the
   documented Toxiproxy proxies, sends the required Toxiproxy User-Agent, uses the `wxeo_fault`
   role for M4-family rewinds, and obtains the I2 attestation from `SchemaTool --mode=verify` as
   the application role (artifact README §8.2/§8.8/§8.9; DEVIATIONS S5-10).

**Exact runbook once the lead releases the stack** (single owner, no other process touching
Kafka/PG/Toxiproxy):

```powershell
# 0) after S3 rebuilt the service jar and the lead releases the stack: bind the NEW jar first
python experiments/freeze.py --write --force      # records jar SHA-256 + source-tree digest
python experiments/freeze.py --check              # must be ok (no artifact drift)
# 1) stack (single owner; S3 may have left it stopped)
powershell -File artifact/harness/native-stack.ps1 reset-kafka   # only if KafkaStorageException
powershell -File artifact/harness/native-stack.ps1 start-pg
powershell -File artifact/harness/native-stack.ps1 start-toxiproxy
powershell -File artifact/harness/native-stack.ps1 status
# 2) the live smoke itself (real run, not a fixture)
python experiments/runner.py --stage pilot --run-id pilot-m2-f0-1k-p3-r00 --no-resume
python experiments/validate_record.py results/raw/run-records/run_pilot-m2-f0-1k-p3-r00.json
# 3) record the evidence bundle (jar SHA, freeze aggregate, run/oracle digests, checks)
python experiments/live_smoke_evidence.py --run-id pilot-m2-f0-1k-p3-r00
#    -> results/harness/live-smoke-evidence.json, exit 0 iff source_integrity=true,
#       schema-valid record, oracle result present, artifact matches the freeze
# 4) update this section with the evidence, then message S3 "stack free"
```

Success criteria: `source_integrity=true`, `validity.valid=true`, schema-valid record, oracle JSON
present with counts, quiescence not censored (or a censored outcome recorded honestly), app/hist/
config artefacts archived. Then send S3 "stack free" and let S3 run its acceptance window.

## Live end-to-end smoke — first attempt (superseded, kept for the record)

Attempted against the running native stack via `experiments/runner.py` (not a fixture). The first
attempt exposed a real integration bug that fixtures could not catch: `check_env()` had been
over-scrubbed and returned `<scratch>/…` placeholders as **executable paths**, so psql/java/
SchemaTool all failed with exit 127 (RUNLOG rows `pilot-m2-f0-1k-p3-r00`, 13:31Z). The bug is
fixed (`check_env` now returns real internal execution paths; RUNLOG scrubs them on record), but a
full live smoke run was **not completed before this fix-pass report was filed** — it must be run
and its record + oracle output attached before Stage A. This is the one acceptance item still
open for this pass.

## G3 extended corpus — GREEN

`python experiments/oracle/mutation_test.py --mutations 100 --events 100 --partitions 3` →
**passed=true**, exit 0. Overview: 18 defect classes / 4 corpora (DB-sink 10, attestation 2,
publication metrics 2, M1-kafka ledger 4), **1,800/1,800 mutations detected (100 %) and
1,800/1,800 correctly attributed (100 %)**, clean controls pass (DB sink, M1-kafka ledger),
input-tamper controls pass. Evidence: `results/harness/g3-mutation.json`.
