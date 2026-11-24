# Pre-registration Amendment A3 — Platform-deviation addendum (post-P1)

**Status:** normative addendum to `docs/prereg.md`, effective before Stage A. **Scope:** platform
realisation and instrumentation only. No hypothesis, mechanism, fault class, cell, replication
count, ordering variant, statistic, decision rule, or frozen analysis artefact changes. Every
deviation below is identical for all nine mechanisms (M0, M1-kafka, M1x, M2, M3, M4, M2+M3,
M4+M3, M1x+M3) and therefore cannot change relative comparisons — it can only change absolute
feasibility (throughput/disk) and, where stated, the *measured* fire position of an injected
fault class, which is recorded per run and checked against the load window.
**Authority:** P1 platform findings (2026-11-24); P2 gate must acknowledge A3 (integrity
statement); S2 re-pins A2; the freeze is re-issued after the pilot (pilot is an evidence/
feasibility phase, not a confirmatory sample).

## A3.1 Service and load-generator JVM heap

**Change.** The service runs with `-Xms512m -Xmx1g` (prereg Appendix C: `-Xms2g -Xmx2g`); the
load generator runs with `-Xmx1g` (Appendix C: `-Xmx1g` unchanged; documented for completeness).
**Rationale.** The host commit limit could not satisfy the 2 g/2 g configuration; the JVM failed
to start (Windows `DOS 1455`, commit limit exceeded) and runs could not execute at 1k/P3 with two
consumer instances. The reduced heap preserves the service's working set at the registered loads.
**Integrity impact.** Fairness unaffected and identical across mechanisms: the same heap is used
for every cell, mechanism and load. No statistic depends on heap size; no run is excluded by
heap, and GC pressure is not a measured variable. If the service cannot sustain a registered load
with this heap, the run is a platform failure (recorded offline), not a mechanism effect.
**Reviewer note.** Compare measured throughput only against the pilot-derived fixed rates
(A3.3/R12), never against a hardware-free expectation.

## A3.2 Broker index-segment sizing

**Change.** Kafka brokers run with `log.index.size.max.bytes=1048576` and
`log.index.interval.bytes=1048576` (Kafka defaults: 10 MiB / 4096 B).
**Rationale.** Kafka preallocates a full 10 MiB index file per segment; with 3 topics × 3
partitions × 3 replicas and the default settings the pilot exhausted 12.5 GB after ~20 topics.
The 1 MiB preallocation and sparse index interval keep disk use bounded and, together with the
M5 reset cadence, make the campaign fit the host. The index is an implementation detail of the
log; it does not change record bytes, offsets, ordering or commit semantics.
**Integrity impact.** Fairness unaffected and identical across mechanisms; per-run disk footprint
is measured and reported (M5). No change to the delivery/visibility guarantees under test; the
index interval only trades index density for space at read time (log reads remain exact).
**Reviewer note.** Disk is an operational constraint, not a measured factor; the 3 GB floor +
periodic full-stack wipe (M5) guarantee headroom.

## A3.3 Marker-driven S4 injections and the `rates.json`-absent path

**Change.** When `experiments/rates.json` is absent (pilot/Stage A; R12 generator deferred until
pilot outputs exist), marker-driven S4-owned injections (K2, K5, F2, F4a, F6) run at a **fixed
rate `max(50, load_events/5)` events/s** — i.e. a ~5 s load window — instead of the
"rate=max" default. At 1k/max the load ends in <1 s and the injection fires after the window
(observed: K2 fire 232 ms *late*); after the fix the same K2 fired 2.36 s into a 30 s planned
bound, **inside** the load window (phase.fire_ms within [load_start, load_end], verified).
**Rationale.** The prereg says injections fire at random recorded times inside the load window;
with the window shorter than the fault setup time the fault cannot be informative. Marker-driven
firing (durable-progress markers for K5, progress-based seeded fractions for F1–F6, M6) plus a
minimum window makes the registered *fraction* observable. The pilot records both the intended
marker and the observed fire position.
**Integrity impact.** Fairness unaffected and identical across mechanisms: the same rate floor
applies to every S4-owned fault class in every mechanism, and the fault class affects all
mechanisms that can experience it. The confirmatory claim does not depend on ambient rate;
however the pilot-period fire positions are recorded and reported, and any run firing outside
[load_start, load_end] is excluded with the frozen reason `uninformative_fire_outside_window`
(prereg §8.2 item 4), so the deviation cannot silently inflate an effect.
**Reviewer note.** A3.3 is superseded by R12 `rates.py` once the pilot yields the fixed/saturation
rate table; Stage B/C use rates.json (runner refuses otherwise).

## A3.4 Pilot runs with `WXEO_DISABLE_PERIODIC_RESET=1`

**Change.** The pilot executes with `WXEO_DISABLE_PERIODIC_RESET=1`: the M5 periodic
`reset-kafka` is disabled; resets between pilot batches are performed manually by the lead.
**Rationale.** With the 1 MiB index cap the pilot fits the disk, and the harness reset path was
the least reliable component during P1 (state-file races, topic-dir rename hazards). Manual,
batch-level resets keep the evidence loop deterministic while the pilot is running.
**Integrity impact.** Fairness unaffected: reset boundaries are batch-level and never
run-level; no run is attributed to a different mechanism because of a reset. Fresh state per run
is still guaranteed by unique session/attempt-scoped topic, group and schema names (M5), and
free space is monitored with the frozen 3 GB floor. Confirmatory campaigns (Stage A/B/C) use the
automatic M5 cadence unless a further amendment says otherwise.
**Reviewer note.** Pilot environment deviation only; it does not touch the confirmatory reset
policy and is recorded per run in `config.runner` (session tag/block/reset epoch).

## A3.5 Receipt attribution hygiene (retries)

**Change.** Receipt validation considers only the **final attempt's wall-clock window**, and
**identical app receipts are deduplicated** (retries share `run_id`; a crash-restart can append
the same `fired` line twice).
**Rationale.** The single-owner protocol requires exactly one app `fired` from the designated
failpoint instance and one matching `restored`; without windowing/dedup, retries and restart
re-arm attempts produce false double-fired conclusions. Duplicates are the *same* receipt
event, not two independent injections.
**Integrity impact.** Fairness unaffected (all mechanisms, all fault classes); the rule never
creates or hides a fired event — it removes retry/re-arm artifacts. Any real second fire from a
non-designated instance remains a hard failure (R5).
**Reviewer note.** Receipts remain append-only on disk; the analysis window/dedup is in the
validator, and both raw and filtered receipt sets are archived.

## A3.6 Platform/API realisations required by confluent-kafka 2.16

**Change.** The live harness adapts to the installed client: (a) `AdminClient.list_offsets` takes
a **dict `{TopicPartition: OffsetSpec}`** with `IsolationLevel.READ_COMMITTED` (not a list/string);
(b) `ListOffsetsResultInfo` exposes **`.offset`**, not `.high`; (c)
`ConsumerGroupTopicPartitions` lives at **`confluent_kafka` top-level**, and
`topic_partitions=None` means "all partitions"; (d) the oracle live dump assigns offset 0 in the
dump record instead of `seek()` (librdkafka local `_STATE` makes seek unreliable in the dump
consumer); (e) service readiness accepts **`status=subscribed`** with process-alive + zero lag
(S3 does not rewrite `ready_*.json` to `assigned`); a crashed failpoint instance is restarted
with `--wxeo.failpoint.enabled=false` (re-arming caused crash loops) and only the restarted
instance is waited for.
**Rationale.** These are client/harness/library realities, not experimental variables; without
them the live measurement path cannot produce a schema-valid record (observed failures:
`topic_partition_offsets` TypeError, `.high` AttributeError, `isolation_level` TypeError,
dump-consumer seek state, readiness never reaching `assigned`, restart crash loop).
**Integrity impact.** Fairness unaffected and identical across mechanisms. (a)/(b)/(c) are
API-shape fixes around the frozen quiescence/offset checks — the checks themselves (stable
read-committed LSO for T_quiet, exact sink counts) are unchanged. (d) is dump-only and cannot
alter the committed ledger or the oracle's expected data (the oracle recomputes from the frozen
seed; the dump carries full key/value/headers). (e) changes only the *readiness predicate* from
`assigned` to `subscribed+alive+zero-lag`, which is a superset check on the same observable
fact (the consumers are consuming the registered topic), applied uniformly. Restart with
failpoints disabled affects only recovery after a terminal crash and is applied identically in
every mechanism; it prevents an artificial crash loop, not a real duplicate/deduplication
effect.
**Reviewer note.** The measured guarantees (I1a/I1b, I2–I5) and the oracle are unchanged; the
live evidence records show schema-valid records with `source_integrity=true` and the expected
app-owned (K3) and S4-owned (K2) receipt counts.

## A3.7 What a reviewer needs to know (summary)

1. A3 changes feasibility/instrumentation only; all nine mechanisms see identical settings.
2. The only measured quantity affected is the observed fire *position* of injected faults; runs
   outside the load window are excluded by the frozen rule.
3. Confirmatory inputs (cells, N, seeds, order, statistics, analysis) are unchanged and frozen
   after approval.
4. R12 (`experiments/rates.py`) will replace the A3.3 rate floor for Stage B/C; R10
   (A2-analyzer) remains a post-P2 item.
5. Evidence for P1: live records `run_pilot-m2-f0-1k-p3-r00` (fault-free, 2000/2000, dup=0),
   `run_pilot-valid-g1k3-r22` (app-owned K3: 2000 expected/2044 observed, dup=44,
   violating_run=true, exactly 1 app fired + 1 S4 restored), `run_a-m3-k2-1k-p3-r02` (S4-owned
   K2: one S4 fired in-window, action_verified=true, 1 restored), consolidated in
   `results/harness/live-smoke-evidence.json` (`passed=true`).
