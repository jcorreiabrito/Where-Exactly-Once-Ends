#!/usr/bin/env python3
"""Generate ``results/DEVIATIONS.md`` — planned-vs-executed diff + interpretation register.

prereg §12: the deviations register is **generated** by the runner from the committed matrix and
the executed run records, never hand-written.  Two kinds of content are produced:

1. **Executed diff (generated).** For every matrix cell: planned N, valid N, excluded N, missing
   N, exclusion rate, and the exclusion reasons.  Cells with no executed runs are listed
   explicitly (no silent drops).  Run-record based; updates automatically as runs complete.
2. **Interpretation/erratum register (frozen list).** Mechanical decisions that the frozen
   documents left open or that required an erratum, all with lead rulings recorded in
   ``docs/P2-RULINGS.md``.  These are not data-driven and never change after freeze; additions
   require a lead ruling first.

Usage::

    python experiments/deviations.py [--matrix experiments/matrix.yaml]
                                     [--records results/raw/run-records]
                                     [--out results/DEVIATIONS.md]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from collections import Counter, defaultdict

try:  # package import (runner --write-deviations)
    from experiments.matrix_design import NA_REASONS as _MD_NA_REASONS
except ImportError:  # direct script execution (`python experiments/deviations.py`)
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from experiments.matrix_design import NA_REASONS as _MD_NA_REASONS
from datetime import datetime, timezone

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: Frozen interpretation / erratum register (additions need a lead ruling first).
INTERPRETATIONS = [
    {
        "id": "R1",
        "title": "GT line numbering, seed encoding and seed passing",
        "decision": "GT `line_no` is 0-based (0..2N-1); `leg_no` is 1-based (1..2). The "
                    "derivation seed argument is the per-repetition `data_seed` interpreted as "
                    "a big-endian u64 and rendered as canonical decimal ASCII; seeds are "
                    "recorded as 16-hex strings and converted independently by the runner/oracle "
                    "and by the Java loader. The campaign `master_seed`, `order_seed` and "
                    "`bootstrap_seed` are 32-hex constants generated from documented phrases "
                    "(recorded in experiments/matrix.yaml and docs/prereg-freeze.json).",
        "reason": "docs/method.md §1.4.1 fixes the hash domains but not the textual encoding of "
                  "the seed or the GT index base; CONTRACTS §4 left both open.",
        "effect": "Byte-level encoding only; no N, cell, factor or statistic changes. Gate G5 "
                  "compares Java and Python on docs/derive-vectors.csv and "
                  "experiments/oracle/g5-vectors.json.",
        "paper": "Artifact appendix (reproduction contract).",
    },
    {
        "id": "R2",
        "title": "run_id encoding (Kafka-topic-legal)",
        "decision": "`<stage>-<mech>-<fault>-<load>-<partitions>[-sat]-rNN`, lower-case "
                    "`[a-z0-9-]`; mechanism tokens m0,m1-kafka,m1x,m2,m3,m4,m2m3,m4m3,m1xm3. "
                    "The pre-registered schema field keeps its semantic form "
                    "`<stage>-<mech>-<condition>-r<rep>`; this is its exact encoding.",
        "reason": "Mechanism names contain '+' (M2+M3), which is illegal in Kafka topic names; "
                  "the prereg §5 shorthand is ambiguous across loads/partitions.",
        "effect": "Naming only; no design change.",
        "paper": "Artifact appendix.",
    },
    {
        "id": "R3",
        "title": "Stage C saturation token; 1M load token; validation-run stage",
        "decision": "Capacity-block runs get the `sat` token (`c-m2-f0-100k-p6-sat-r05`); the 1M "
                    "block uses load token `1m`; the 20 dedicated validation runs use stage "
                    "`pilot` and run_id `pilot-valid-<kind>-rNN`, recorded separately from the "
                    "216/201 matrix slots.",
        "reason": "Both Stage C blocks share 100k/P=6 and would otherwise collide; the "
                  "prereg §5 stage enum has no validation value.",
        "effect": "Identity only; no design change.",
        "paper": "Artifact appendix.",
    },
    {
        "id": "R4",
        "title": "Pilot planned vs executable accounting",
        "decision": "matrix.yaml keeps 216 planned pilot slots; the 15 structurally n/a slots "
                    "(M1-kafka F4a/F4b/F5; M4 F4b; M4+M3 F4b × N=3) carry executed=false + the "
                    "reachability reason and are never counted as passes. Executable pilot = "
                    "201. All reporting separates planned / executable / executed / excluded: "
                    "7,866 planned, 7,851 executable (+20 validation).",
        "reason": "docs/prereg.md §2.3's 216 arithmetic assumes all 72 fault cells are runnable, "
                  "while §2.2 and docs/method.md §5.3 mark 5 cells n/a.",
        "effect": "Accounting only; n/a cells have documented unreachability proofs.",
        "paper": "Results table + limitations.",
    },
    {
        "id": "R5",
        "title": "SPEC ERRATUM — amount derivation byte range",
        "decision": "docs/method.md §1.4.1 wrote `uint64_be(h1[28:36])`, but SHA-256 is 32 bytes. "
                    "Frozen effective form: `amount_minor = 1 + (uint32_be(h1[28:32]) mod "
                    "100000)`. Verified by lead re-derivation against "
                    "docs/derive-vectors.csv (6 vectors). Implementations keep the literal "
                    "in-bounds slice.",
        "reason": "The frozen formula's index range is out of bounds for a 32-byte digest; the "
                  "literal slice semantics (4 available bytes) is the only in-bounds reading.",
        "effect": "Amount values change relative to a hypothetical last-8-byte reading; both "
                  "implementations align on R5-A. No N/cell/statistic changes. GT digest values "
                  "in experiments/oracle/g5-vectors.json are R5-A-consistent.",
        "paper": "Artifact appendix (reproduction contract) + method footnote if the author "
                 "keeps §1.4.1 verbatim.",
    },
    {
        "id": "S4-1",
        "title": "Oracle verdict rule: spurious postings and attestation",
        "decision": "`violating_run = v1∨v2∨v3∨v4∨(spurious_postings>0)`; `v2` is set by "
                    "mutation OR by failure of the append-only attestation. A fabricated posting "
                    "with an unknown key is reported as `SPURIOUS` attribution and is not folded "
                    "into `dup_postings`.",
        "reason": "docs/method.md §2.4 includes 'no spurious postings' in the consistency "
                  "predicate while §2.5 defines v1..v4 only from the four count classes; §2.6 "
                  "says V2 also covers attestation failure.",
        "effect": "Verdict/attribution only; counts keep the exact §2.5 definitions. G3 mutation "
                  "testing exercises the rule (insert-spurious-row class).",
        "paper": "Artifact appendix (oracle contract).",
    },
    {
        "id": "S4-2",
        "title": "G4 receipt protocol for terminal crash failpoints and sham runs",
        "decision": "A terminal `crash` failpoint writes only a `fired` receipt inside the "
                    "service (Runtime.halt(99)); S4 writes the matching `restored` receipt after "
                    "the instance is restarted, so the run still has exactly one fired + one "
                    "restored receipt. Sham validation runs write `armed` + `restored` and no "
                    "`fired`; the G4 validator accepts that only for injector_mode=sham.",
        "reason": "S3's failpoint cannot append after Runtime.halt; a crash is by design not "
                  "restorable from inside the process.",
        "effect": "Attestation accounting only; exclusion policy unchanged.",
        "paper": "Threats (injector attestation).",
    },
    {
        "id": "S4-3",
        "title": "Validation-run composition (20 runs)",
        "decision": "6 sham-injection + 2 C=1 robustness + 6 injector self-tests + 2 quiescence "
                    "calibration (T_quiet doubled, G7) + 4 oracle-fixture validations "
                    "(clean/loss/duplicate/mutation). Mechanisms are spread across the validation "
                    "slate so that the §4.3 interleaving constraints hold for the whole sequence.",
        "reason": "docs/prereg.md §2.3 fixes the categories and the total (20) but not the split.",
        "effect": "Validation only; not part of the matrix counts.",
        "paper": "Artifact appendix (harness validation).",
    },
    {
        "id": "S4-4",
        "title": "Analysis freeze scope and issuance",
        "decision": "The freeze hashes the oracle, run-record validator, matrix generator, "
                    "runner, injectors, deviations generator, analysis and figure scripts, plus "
                    "the matrix/run-order hashes, the seeds generation procedure, the normative "
                    "derivation vectors and the pre-freeze harness evidence. The oracle reads "
                    "only results/raw inputs plus frozen vector files. The freeze is issued "
                    "once during P2 implementation; files whose hashes changed while the "
                    "pipeline was still being completed were re-frozen before the first run — "
                    "this is not a post-freeze analysis change because no run existed and no "
                    "analysis output was ever produced from the earlier revision.",
        "reason": "prereg §10 requires a committed frozen analysis pipeline before Stage A.",
        "effect": "Provenance only; any change after the first Stage A run becomes a deviation.",
        "paper": "Reproducibility.",
    },
    {
        "id": "S5-1",
        "title": "Stage-A realisation + matrix amendment (S5 F-E1/E2/E3/E4/P4/F-RN1)",
        "decision": "Stage-A fault tokens are canonical upper-case in run records (K1..K7, none), "
                    "lower-case only inside run_id/cell_id tokens. Realisations: K1/K3/K4/K7 via "
                    "app failpoints (K4 = ordering=early probe + before_effect_commit crash); "
                    "K2 = unknown outcome (Toxiproxy timeout toxic on the effect link at a seeded "
                    "committed-progress marker); K5 = protocol-level duplicate delivery (stop, "
                    "rewind k batches, restart). K6 (zombie) is structurally n/a for every "
                    "mechanism (cannot pause an owner beyond max.poll.interval.ms=300 s within "
                    "T_max=300 s; model-only claim). M1-kafka: K5 n/a (no external rewind without "
                    "breaking D5); F6 n/a (rewinding its committed offset duplicates the positive "
                    "control's ledger). Amended counts: pilot 216/198, Stage A 1,530, Stage B "
                    "3,960, Stage C 1,800, validation 26 (20 + 6 G1 gate controls).",
        "reason": "The review showed Stage A could not arm (case mismatch, missing K4), K2 "
                  "duplicated K3, K5/K6 were pause no-ops, and F6 broke the M1-kafka control and "
                  "the M4 write path.",
        "effect": "matrix.yaml/run_order.csv/matrix.sha256 regenerated pre-freeze; N changes are "
                  "logged here and in the P2 amendment; no hypothesis rule rewritten by S4.",
        "paper": "Realisation table (S5 §15 item 4); threats.",
    },
    {
        "id": "S5-2",
        "title": "Fault-receipt protocol v2 (S5 F-I1/I2/I3)",
        "decision": "Single-owner receipts: S4 external injections write exactly one verified "
                    "fired + one restored (action_verified + phase block); app failpoints own "
                    "their fired (and their restored when non-terminal); terminal crash failpoints "
                    "get the S4 restored after restart; sham = armed + restored, no fired. One "
                    "validation module (experiments/receipts.py) is used by runner, analysis and "
                    "deviations.",
        "reason": "External injectors never wrote fired; L1 pause double-wrote restored; an "
                  "unverified action could count as attested.",
        "effect": "G4 and all validity decisions use the protocol.",
        "paper": "Fault attestation (S5 §15 item 3).",
    },
    {
        "id": "S5-3",
        "title": "Oracle contract additions (S5 F-O2/O3/O4/O5/O7)",
        "decision": "The oracle reports v5 = spurious postings as an explicit clause, the I1a "
                    "structural / I1b signed-sum counts and attribution labels, queries the "
                    "*application* role privileges for I2(a) (missing attestation on a DB sink is "
                    "an I2 violation), validates publication content and reports signed divergence "
                    "(stream records - ledger entries) plus pub_mutated_records, raises hard on "
                    "malformed sink rows and cross-checks the dump row count against the DB. The "
                    "v5/I1 split and attestation semantics are submitted for author ratification "
                    "in amendment A2; the analysis reads violations.violating_run verbatim.",
        "reason": "Privilege attestation was vacuous, balanced fabrications evaded v1..v4, "
                  "divergence did not match the documented metric, malformed rows were dropped.",
        "effect": "Additive oracle/run-record extension; G3 corpus extended.",
        "paper": "Oracle contract appendix (S5 §15 item 1).",
    },
    {
        "id": "S5-4",
        "title": "Falsifiable gates and negative controls (S5 F-G1/F-G2/F-E5)",
        "decision": "G1 per class (K3 and K5 separately, M0, attested) on 6 dedicated pre-Stage-A "
                    "gate-control validation runs; G4/G5/G8 computed from all *reserved* runs and "
                    "raw provenance (receipts/digests), never from 'valid' records; G6 compares the "
                    "app-emitted shared_sha256 across mechanisms within a cell; G7 implemented "
                    "(rate table, generator-lag p99, T_quiet doubling); G8 uses a real clock probe "
                    "(wall-vs-monotonic drift + boot time) and real affinity/priority read-back; a "
                    "purpose-built negative-control set (results/harness/gate-negative-controls.json) "
                    "proves each gate can FAIL.",
        "reason": "G4/G5/G8 were tautological, G6 failed on clean data, G7 was hard-coded PENDING, "
                  "and G1's evidence could only come from Stage A.",
        "effect": "The gate table becomes a real internal-validity test; +6 validation runs.",
        "paper": "Harness-validation table T1.",
    },
    {
        "id": "S5-5",
        "title": "Analysis accounting fixes (S5 F-P5 infrastructure)",
        "decision": "Censored runs count as violating outcomes (method §7.3); N_planned comes from "
                    "the frozen reserved-run list; validation runs (cell_id V.*) are excluded from "
                    "confirmatory tables; cost/latency comparisons are direction-aware; P3 is per "
                    "cell; H1's clean arm is restricted to its pre-registered cells; H5's "
                    "non-inferiority machinery rejects vacuous comparisons. No hypothesis "
                    "decision-rule constant changed by S4 — those await amendment A2.",
        "reason": "Censoring scored as clean, N_planned silently redefined, validation "
                  "contamination, direction-blind cost tests.",
        "effect": "Accounting/mechanics only; decision rules pending A2.",
        "paper": "Statistics/analysis sections.",
    },
    {
        "id": "S5-6",
        "title": "Performance pipeline + measurement hooks (S5 F-M1)",
        "decision": "Runner computes throughput from committed events in the warm-up-excluded "
                    "steady-state window, records generator-lag from the loader manifest, merges "
                    "per-instance HdrHistogram percentiles and records time-to-consistent from the "
                    "quiescence probe. Service-side warm-up exclusion and send timestamps are "
                    "artifact changes requested from S3 and recorded here until delivered.",
        "reason": "Performance block was never computed; warm-up was not excluded.",
        "effect": "Stage C/D performance claims become computable from run records.",
        "paper": "Measurement section (S5 §15 item 6).",
    },
    {
        "id": "S5-7",
        "title": "Committed-artefact path hygiene (lead P0 addendum)",
        "decision": "Every committed JSON/NDJSON/CSV/TeX artefact carries repo-relative paths or "
                    "<scratch>/<name> placeholders; runner/oracle/analysis scrub paths and the "
                    "run-record validator rejects absolute drive-letter/UNC/home paths and the "
                    "local username.",
        "reason": "The first push was blocked by absolute local paths in dry-run artefacts.",
        "effect": "No host path leaves a committed artefact; enforced by tests.",
        "paper": "Reproducibility/artifact appendix.",
    },
    {
        "id": "M5",
        "title": "Windows-KRaft topic lifecycle: no per-run topic deletion; variable reset cadence",
        "decision": "Topic deletion is FATAL on this Windows KRaft setup (S3 live acceptance "
                    "evidence: `Error while renaming dir for <topic>` -> KafkaStorageException -> "
                    "brokers shut down). Therefore the runner NEVER deletes topics. Fresh logical "
                    "state comes from unique attempt-scoped topic names (`wxeo.src/sink.<run_id>` "
                    "for attempt 1, `...<run_id>.aN` for retries), a fresh consumer group and a "
                    "fresh PostgreSQL schema. Storage is reclaimed only by periodic full-stack "
                    "`reset-kafka` (wipe + reformat), at a load-dependent cadence: 1k events every "
                    "50 runs, 10k every 25, 100k every 20, saturation-100k every 20, 1M every run; "
                    "plus a hard cap of 400 live non-internal topics after which the runner "
                    "resets before the next run. `restart-all` is replaced by `reset-kafka` so "
                    "segments are actually reclaimed. Resets are recorded in `fault_restart.log` "
                    "and in every run record (`config.runner.restart_before_run`/`block_id`/"
                    "`reset_epoch`). Per-run disk footprint and live-topic counts are measured "
                    "into `results/raw/summaries/{disk-footprint,lifecycle}.csv` and adjusted "
                    "only upward if the 3 GB floor holds. Preflight verifies the broker safety "
                    "properties `delete.topic.enable=false` and "
                    "`log.retention.check.interval.ms=3600000` and fails loudly otherwise; any "
                    "topic delete would be a runner bug. A pre-run liveness guard (3 brokers + "
                    "PG + Toxiproxy) resets once on failure and records the run invalid with "
                    "reason `environment_stack_down`.",
        "reason": "Platform constraint found by S3's live acceptance run; per-run deletion and "
                  "stop/start restarts cannot work safely on this KRaft/Windows combination.",
        "effect": "Operational only: matrix counts, mechanisms, statistics unchanged. Disclosed "
                  "at the P2 gate; storage/topic-count measured in the pilot.",
        "paper": "Threats (environment) + artifact appendix (fresh-state protocol).",
    },
    {
        "id": "M6",
        "title": "Level-2 fault fire timing is progress-based, not wall-clock",
        "decision": "For S4-owned external faults (F1-F6) the Level-2 fire trigger is a seeded "
                    "fraction (25-75 %, derived from injection_seed) of the INTENDED event "
                    "count: the runner polls durable committed progress and fires when that "
                    "marker is reached, then stamps the receipt with the actual observed window "
                    "(load_start, planned upper bound, fire time, progress at fire) and the "
                    "runner post-checks the fire against the REAL load window. Wall-clock "
                    "scheduling (the prereg's 'random recorded times') is replaced because the "
                    "pilot runs at rate=max where the load duration is unknown, and because a "
                    "fixed 60 s delay would fire after a real 1k-event load has ended.",
        "reason": "R4 (S5 re-review): with rates.json absent in the pilot, time-based triggers "
                  "fired outside the load window and were rejected by the F-I3 window check.",
        "effect": "Operational only: the fault still fires at a pre-registered random position "
                  "in the stream (seeded, recorded, reproducible); no cell/N/statistic changes. "
                  "Disclosed to the author at the P2 gate. (M7 = topic lifecycle/reset cadence "
                  "is already registered as M5.)",
        "paper": "Threats (fault injection timing) + artifact appendix.",
    },
    {
        "id": "S5-11",
        "title": "Matrix injector-mode correction (S5 R3) and re-issued hashes",
        "decision": "Stage-A `injector_mode` is corrected to match the injector semantics: "
                    "K1/K3/K4/K7 are app-owned in-process failpoints (`failpoint`), K2 "
                    "(unknown-outcome Toxiproxy intervention) and K5 (protocol replay) are "
                    "S4-owned (`external`), F1-F6 are `live`, controls are `none`; the 6 G1 "
                    "gate-control validation runs are app-owned failpoints. matrix.yaml/"
                    "run_order.csv/matrix.sha256 were regenerated: sha256 "
                    "matrix.yaml=bce2568a59406499c100ec2fd90c136f00071be0b4fa187fd638a24006e69894, "
                    "run_order.csv=520b119053e3fc3e5b0878dcdd8ff3f2f998287b4773101371c4006e62f06af7. "
                    "Counts are unchanged (7,506 planned / 7,488 executable / +26 validation; "
                    "§4.3 constraints verified).",
        "reason": "The S5 re-review found Stage-A K2/K5 rows classified as `failpoint`, making "
                  "the receipt validator treat S4-owned interventions as app-owned and fail "
                  "their attestation (R3).",
        "effect": "Matrix metadata only; no cell, N, factor or statistic changes. S2 re-pins A2 "
                  "and the freeze is re-issued with these hashes.",
        "paper": "Artifact appendix (injector ownership table).",
    },
    {
        "id": "S5-9",
        "title": "PostgreSQL schema identifier sanitisation",
        "decision": "The run schema identifier is `wxeo_run_<run_id>` with every '-' replaced by "
                    "'_' (e.g. `wxeo_run_pilot_m2_f0_1k_p3_r00`), because S3's SchemaTool "
                    "enforces the PostgreSQL identifier pattern [A-Za-z0-9_]{1,63} while run_ids "
                    "are hyphenated for Kafka-topic legality. The mapping is deterministic, "
                    "verified unique across all reserved runs, and recorded in every run's notes; "
                    "the service receives the same sanitised `--wxeo.db-schema`.",
        "reason": "Discovered by the live end-to-end smoke: the first real run failed with "
                  "`schema must match [A-Za-z0-9_]{1,63}` (exit 127/1), a defect no fixture "
                  "exercise could reveal.",
        "effect": "Naming only; no design change. Schema create/drop/observe all use the same "
                  "sanitised identifier.",
        "paper": "Artifact appendix (fresh-state protocol).",
    },
    {
        "id": "S5-10",
        "title": "Per-fault endpoints, split bootstraps and fault role (artifact README §8.2/§8.8)",
        "decision": "The runner selects service endpoints per cell from S3's hand-off contract: "
                    "direct bootstrap/DB by default; F3 → `--wxeo.producer-bootstrap=19093` "
                    "(kafka-produce); F4a → `--wxeo.consumer-bootstrap=19094` (kafka-consume); "
                    "F5 → db through the `db` proxy (15433); K2 unknown-outcome → db proxy for DB "
                    "sinks / produce proxy for M1-kafka. Toxiproxy HTTP requests send an explicit "
                    "`User-Agent: wxeo-harness/1.0` (2.9.0 rejects the PowerShell default with "
                    "403). M4/M4+M3 progress rewinds and the K7 outbox resend use the dedicated "
                    "`wxeo_fault` role (SELECT all + UPDATE on outbox/consumer_progress only, "
                    "never on posting); the runner passes `--fault-role=wxeo_fault` to SchemaTool "
                    "and reads the I2(a) attestation via `SchemaTool --mode=verify --db-user="
                    "wxeo_app`.",
        "reason": "CONTRACTS §7 predates S3's additive split-bootstrap extension and the fault "
                  "role; using the wrong link would make F3/F4a toxics no-ops and a superuser "
                  "rewind would violate the posting-safe design.",
        "effect": "Endpoint routing only; the measured mechanism configuration is unchanged. "
                  "Recorded in every run's command log.",
        "paper": "Artifact appendix (injector realisation).",
    },
    {
        "id": "S5-8",
        "title": "Freeze re-issued after the S5 fix pass (S5 F-P6)",
        "decision": "docs/prereg-freeze.json regenerated once after the last fix; the runner "
                    "preflight runs a freeze --check and refuses to start on drift; the freeze "
                    "records the artifact jar and Java source-tree digests in addition to the "
                    "analysis pipeline. Intermediate re-freezes during the fix pass are not "
                    "post-freeze analysis changes (no run existed, no analysis output produced).",
        "reason": "Freeze must be regenerated after all fixes and verifiable at preflight.",
        "effect": "Provenance only; the freeze binds the exact data-producing pipeline.",
        "paper": "Reproducibility.",
    },
]


#: Frozen ORIGINAL author-approved arithmetic (prereg v1.0 as approved at P1).  This is the
#: baseline for the generated planned-vs-executed delta table; it must never be edited.
ORIGINAL_PREREG_ARITHMETIC = {
    "stages": {
        "pilot": {"runs_planned": 216, "runs_executable": 201,
                  "cells_total": 72, "cells_executed": 67, "cells_na": 5},
        "A": {"runs_planned": 1830, "runs_executable": 1830,
              "cells_total": 61, "cells_executed": 61, "cells_na": 0},
        "B": {"runs_planned": 4020, "runs_executable": 4020,
              "cells_total": 268, "cells_executed": 268, "cells_na": 0},
        "C": {"runs_planned": 1800, "runs_executable": 1800,
              "cells_total": 72, "cells_executed": 72, "cells_na": 0},
    },
    "validation_runs": 20,
    "totals": {"runs_planned": 7866, "runs_executable": 7851,
               "runs_executable_plus_validation": 7871},
}


def _amendment_delta(matrix: dict) -> list[str]:
    """Generated planned-vs-executed delta against the ORIGINAL author-approved arithmetic.

    Required by the lead so the material matrix amendment (S5 fix pass) is explicit at the P2
    gate: per stage old count -> new count, and every removed/added cell with its reason.
    """
    lines: list[str] = []
    stages = (matrix.get("accounting") or {}).get("stages", {})
    totals = (matrix.get("accounting") or {}).get("totals", {})
    lines.append("## 1b. PLANNED-vs-EXECUTED DELTA against the original author-approved "
                 "prereg (P2 amendment)")
    lines.append("")
    lines.append("The S5 fix pass required Stage-A realisation changes. They are a **material "
                 "amendment** to the P1-approved matrix (presented to the author at the P2 gate "
                 "together with S2's amendment A2); `docs/prereg.md` itself was not edited by S4.")
    lines.append("")
    lines.append("| Stage | planned old → new | executable old → new | Δ executable | cells "
                 "(total/executed/n-a) | reason |")
    lines.append("|---|---|---|---|---|---|")
    reasons = {
        "pilot": "M1-kafka F6 declared n/a (D5: external rewind duplicates the positive "
                 "control's own ledger)",
        "A": "K6 (zombie C7) structurally n/a for all 9 mechanisms (−270 runs); M1-kafka K5 n/a "
             "(−30 runs: no rewind without breaking D5)",
        "B": "M1-kafka F6 n/a across core/S1/S2 (−60 runs)",
        "C": "unchanged",
    }
    for key, label in (("pilot", "Pilot"), ("A", "A"), ("B", "B"), ("C", "C")):
        old = ORIGINAL_PREREG_ARITHMETIC["stages"][key]
        new = stages.get(key, {})
        d_exec = (new.get("runs_executable", 0) - old["runs_executable"])
        lines.append(
            f"| {label} | {old['runs_planned']} → {new.get('runs_planned', 0)} | "
            f"{old['runs_executable']} → {new.get('runs_executable', 0)} | {d_exec:+d} | "
            f"{new.get('cells_total', 0)}/{new.get('cells_executed', 0)}/"
            f"{new.get('cells_na', 0)} | {reasons[key]} |")
    old_t = ORIGINAL_PREREG_ARITHMETIC["totals"]
    lines.append(
        f"| **Total** | **{old_t['runs_planned']} → "
        f"{totals.get('runs_planned', 0)}** | **{old_t['runs_executable']} → "
        f"{totals.get('runs_executable', 0)}** | "
        f"{totals.get('runs_executable', 0) - old_t['runs_executable']:+d} | | |")
    lines.append("")
    lines.append(f"Validated runs: {ORIGINAL_PREREG_ARITHMETIC['validation_runs']} → "
                 f"{totals.get('validation_runs', 0)} (+6 dedicated G1 gate-control runs: M0 K3 "
                 f"and M0 K5 × 3 reps, so G1 can be decided per class before Stage A data is "
                 f"analysed).")
    lines.append("")
    lines.append("### Removed cells (n/a, with the reachability reason; never counted as passes)")
    lines.append("")
    removed_a = 0
    for mechanism, classes in (matrix.get("reachability", {})
                               .get("stage_a_na_classes", {}) or {}).items():
        for klass, reason in classes.items():
            # only the cells this amendment removed: K6 for every mechanism and M1-kafka K5
            if klass == "K6" or (mechanism == "M1-kafka" and klass == "K5"):
                removed_a += 1
                lines.append(f"- Stage A `{mechanism}` × `{klass}` (×30 runs): {reason}")
    removed_b = 0
    for mechanism, faults in (matrix.get("reachability", {})
                              .get("pilot_fault_na", {}) or {}).items():
        for fault in faults:
            if (mechanism, fault) == ("M1-kafka", "F6"):
                removed_b += 1
                lines.append(
                    f"- pilot/Stage-B `{mechanism}` × `{fault}`: "
                    f"{_MD_NA_REASONS.get((mechanism, fault), 'n/a per reachability')} "
                    f"(pilot 3 + B core 30 + S1 10 + S2 20 = 63 runs)")
    lines.append("")
    lines.append(f"Stage-A cells removed: {removed_a} (K6 ×9 = 270 runs; M1-kafka K5 = 30 runs); "
                 f"Stage-B/pilot removed cells: {removed_b} (M1-kafka F6, 63 runs). "
                 f"Added: 6 validation gate-control runs. Nothing else in the matrix changed: "
                 f"every other cell keeps its prereg N, load, partitions, ordering variant and "
                 f"seed. The analyzer never hard-codes these counts — it reads the reserved-run "
                 f"list emitted by the runner and matrix.yaml.")
    lines.append("")
    return lines


DES_NA_REASON: dict = {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_plan(matrix_path: str) -> dict:
    try:
        import yaml

        with open(matrix_path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh)
    except Exception:  # noqa: BLE001 - matrix may be absent early
        return {}


def _load_records(records_dir: str) -> list[dict]:
    records = []
    if not os.path.isdir(records_dir):
        return records
    for name in sorted(os.listdir(records_dir)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(records_dir, name)
        try:
            with open(path, "r", encoding="utf-8") as fh:
                records.append(json.load(fh))
        except Exception:  # noqa: BLE001
            continue
    return records


def compute_fire_rates(faults_log_path: str, records: list[dict],
                       reserved: list[dict] | None = None) -> dict:
    """F-I5: fire-rate / exclusion accounting per mechanism × fault (S5 review).

    Denominators are **reserved** runs (the frozen matrix plan), never the surviving records:
    a fault that never fired must stay visible as a missing receipt, not vanish.  Returns
    ``{"by_cell": [...], "totals": {...}}``; rows are suitable for a CSV table.
    """
    receipts = []
    if faults_log_path and os.path.exists(faults_log_path):
        with open(faults_log_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        receipts.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    by_run: dict[str, dict] = defaultdict(lambda: {"fired": 0, "restored": 0,
                                                   "armed": 0, "app": 0, "s4": 0})
    for receipt in receipts:
        run_id = receipt.get("run_id")
        if not run_id:
            continue
        entry = by_run[run_id]
        event = receipt.get("event")
        if event in ("fired", "restored", "armed"):
            entry[event] = entry.get(event, 0) + 1
        source = receipt.get("source") or ("app" if receipt.get("fault") in {
            "after_fetch", "before_effect_commit", "after_effect_commit",
            "before_progress_commit", "after_progress_commit", "before_publish",
            "before_relay_mark"} else "s4")
        entry[source] = entry.get(source, 0) + 1
    # reserved denominators from the frozen plan (fallback: existing records)
    plan_index = {}
    for row in (reserved or []):
        plan_index.setdefault((row.get("mechanism"), row.get("fault"), row.get("stage_label")),
                              0)
        plan_index[(row.get("mechanism"), row.get("fault"), row.get("stage_label"))] += 1
    cells: dict[tuple, dict] = {}
    for rec in records:
        key = (rec.get("mechanism"), rec.get("fault"),
               {"pilot": "P", "A": "A", "B": "B", "C": "C"}.get(rec.get("stage"),
                                                                  rec.get("stage")))
        cell = cells.setdefault(key, {"reserved": plan_index.get(key, 0),
                                      "valid": 0, "excluded": 0, "fired": 0,
                                      "restored": 0, "armed": 0,
                                      "no_receipt": 0, "reasons": Counter()})
        validity = rec.get("validity") or {}
        if validity.get("excluded"):
            cell["excluded"] += 1
            cell["reasons"][str(validity.get("exclude_reason") or "unknown")] += 1
        elif validity.get("valid"):
            cell["valid"] += 1
        mine = by_run.get(rec.get("run_id"), {})
        cell["fired"] += int(mine.get("fired", 0) > 0)
        cell["restored"] += int(mine.get("restored", 0) > 0)
        cell["armed"] += int(mine.get("armed", 0) > 0)
        if not mine.get("fired") and rec.get("fault_level") in ("L1", "L2") \
                and rec.get("fault") not in ("F0", "none", "ctrl"):
            cell["no_receipt"] += 1
    rows = []
    for (mechanism, fault, stage), cell in sorted(cells.items()):
        executed = cell["valid"] + cell["excluded"]
        rows.append({
            "stage": stage, "mechanism": mechanism, "fault": fault,
            "reserved": cell["reserved"], "executed": executed,
            "valid": cell["valid"], "excluded": cell["excluded"],
            "fired": cell["fired"], "restored": cell["restored"], "armed": cell["armed"],
            "missing_receipt": cell["no_receipt"],
            "fire_rate": (cell["fired"] / executed) if executed else None,
            "restore_rate": (cell["restored"] / cell["fired"]) if cell["fired"] else None,
            "exclusion_rate": (cell["excluded"] / executed) if executed else None,
            "exclude_reasons": "; ".join(f"{k} x{v}" for k, v in
                                         sorted(cell["reasons"].items())),
        })
    injected = [r for r in rows if r["fault"] not in ("F0", "none", "ctrl")]
    totals = {
        "injected_cell_reports": len(injected),
        "executed_injected": sum(r["executed"] for r in injected),
        "fired": sum(r["fired"] for r in injected),
        "missing_receipt": sum(r["missing_receipt"] for r in injected),
        "excluded": sum(r["excluded"] for r in injected),
        "fire_rate": (sum(r["fired"] for r in injected)
                      / max(1, sum(r["executed"] for r in injected))),
        "exclusion_rate": (sum(r["excluded"] for r in injected)
                           / max(1, sum(r["executed"] for r in injected))),
    }
    return {"by_cell": rows, "totals": totals}


def _load_reserved(summaries_dir: str) -> list[dict]:
    path = os.path.join(summaries_dir, "reserved-runs.csv")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _cell_plan(matrix: dict) -> dict[str, dict]:
    """cell_id -> {stage, mechanism, condition, rep count, n/a} from matrix.yaml."""
    out: dict[str, dict] = {}
    for cell in matrix.get("cells", []):
        out[cell["cell_id"]] = {
            "stage": cell["stage_label"],
            "mechanism": cell["mechanism"],
            "condition": cell["condition"],
            "planned": cell["n_planned_slots"] if cell.get("executed") else 0,
            "planned_slots": cell["n_planned_slots"],
            "executed": bool(cell.get("executed")),
            "na_reason": cell.get("na_reason"),
        }
    for v in matrix.get("validation", []):
        key = f"V.{v['kind']}.{v['mechanism']}.{v['fault']}.1k.p3"
        out[key] = {"stage": "V", "mechanism": v["mechanism"], "condition": v["fault"],
                    "planned": 1, "planned_slots": 1, "executed": True, "na_reason": None}
    return out


def _executed_by_cell(records: list[dict]) -> dict[str, dict]:
    agg: dict[str, dict] = defaultdict(lambda: {
        "valid": 0, "excluded": 0, "censored": 0, "violating": 0,
        "reasons": Counter(), "reps": set(),
    })
    for rec in records:
        cell = rec.get("cell_id", "?")
        entry = agg[cell]
        validity = rec.get("validity") or {}
        if validity.get("excluded"):
            entry["excluded"] += 1
            entry["reasons"][str(validity.get("exclude_reason") or "unknown")] += 1
        elif validity.get("valid"):
            entry["valid"] += 1
        else:
            entry["excluded"] += 1
            entry["reasons"]["invalid_unspecified"] += 1
        if (rec.get("quiescence") or {}).get("censored"):
            entry["censored"] += 1
        if (rec.get("violations") or {}).get("violating_run"):
            entry["violating"] += 1
        entry["reps"].add(rec.get("rep"))
    return agg


def render_deviations(*, matrix: dict, records: list[dict], matrix_path: str,
                      faults_log: str = "", reserved: list[dict] | None = None) -> str:
    lines: list[str] = []
    plan = _cell_plan(matrix)
    executed = _executed_by_cell(records)
    stages = {k: v for k, v in (matrix.get("accounting") or {}).get("stages", {}).items()}
    totals = (matrix.get("accounting") or {}).get("totals", {})
    lines.append("# DEVIATIONS — planned vs executed (+ interpretation register)")
    lines.append("")
    lines.append("Generated by `experiments/deviations.py` (prereg §12). This file is a "
                 "diff between the frozen matrix and the run records that actually exist, plus "
                 "a frozen register of mechanical interpretations/errata ruled by the lead before "
                 "the freeze. Nothing here changes a pre-registered N, cell, factor or statistic.")
    lines.append("")
    lines.append(f"- generated UTC: `{_utc_now()}`")
    if os.path.exists(matrix_path):
        lines.append(f"- matrix: `experiments/matrix.yaml` "
                     f"(sha256 `{_sha256_file(matrix_path)}`)")
    lines.append(f"- run records scanned: {len(records)}")
    lines.append("")
    lines.append("## 1. Campaign status")
    lines.append("")
    if not records:
        lines.append("No run records exist yet. The matrix and run order are frozen and hashed; "
                     "the pilot has not started (P2 integration gate pending S3's artifact "
                     "acceptance tests and the lead's go-ahead). No cell has been dropped.")
    else:
        total_valid = sum(v["valid"] for v in executed.values())
        total_excl = sum(v["excluded"] for v in executed.values())
        lines.append(f"- valid runs: {total_valid}; excluded runs: {total_excl}; "
                     f"violating runs: {sum(v['violating'] for v in executed.values())}")
        lines.append("")
    lines.append("### Frozen accounting (prereg §2, docs/P2-RULINGS.md R4)")
    lines.append("")
    lines.append("| Stage | cells (total/executed/n-a) | runs planned | runs executable |")
    lines.append("|---|---|---|---|")
    for key, label in (("pilot", "Pilot"), ("A", "A"), ("B", "B"), ("C", "C")):
        s = stages.get(key, {})
        lines.append(f"| {label} | {s.get('cells_total', '—')} / "
                     f"{s.get('cells_executed', '—')} / {s.get('cells_na', '—')} | "
                     f"{s.get('runs_planned', '—')} | {s.get('runs_executable', '—')} |")
    lines.append(f"| **Total** | | **{totals.get('runs_planned', '—')}** planned | "
                 f"**{totals.get('runs_executable', '—')}** executable (+"
                 f"{totals.get('validation_runs', 20)} validation) |")
    lines.append("")
    lines.extend(_amendment_delta(matrix))
    lines.append("## 2. Executed diff by cell")
    lines.append("")
    lines.append("Only cells whose valid count differs from the plan (including cells with zero "
                 "executed runs) are listed. `N_planned` is the frozen repetition count; excluded "
                 "runs are counted with their provenance-based reason (prereg §8.2).")
    lines.append("")
    if not records:
        lines.append("_No executed cells yet — the diff will populate as runs complete._")
    else:
        lines.append("| cell | stage | mechanism | condition | N_planned | N_valid | N_excluded "
                     "| N_missing | exclusion rate | reasons |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for cell_id, info in sorted(plan.items()):
            if info["stage"] == "V":
                continue
            got = executed.get(cell_id, {"valid": 0, "excluded": 0, "reasons": Counter()})
            planned = info["planned"]
            missing = max(0, planned - got["valid"] - got["excluded"])
            if got["valid"] == planned and got["excluded"] == 0:
                continue
            denom = got["valid"] + got["excluded"]
            rate = f"{got['excluded'] / denom:.1%}" if denom else "—"
            reasons = "; ".join(f"{k} x{v}" for k, v in
                                sorted(got["reasons"].items())) or "—"
            lines.append(f"| `{cell_id}` | {info['stage']} | {info['mechanism']} | "
                         f"{info['condition']} | {planned} | {got['valid']} | "
                         f"{got['excluded']} | {missing} | {rate} | {reasons} |")
        # n/a cells (never counted as passes)
        na_cells = [(cid, info) for cid, info in plan.items()
                    if info["stage"] == "P" and not info["executed"]]
        if na_cells:
            lines.append("")
            lines.append("### Structurally n/a pilot slots (never counted as passes, R4)")
            lines.append("")
            for cid, info in sorted(na_cells):
                lines.append(f"- `{cid}` — planned slot {info['planned_slots']}×; n/a: "
                             f"{info['na_reason']}")
        # cells planned but absent entirely
        absent = [cid for cid, info in plan.items()
                  if info["planned"] > 0 and cid not in executed]
        if absent:
            lines.append("")
            lines.append(f"### Cells with zero executed runs ({len(absent)})")
            lines.append("")
            lines.append(", ".join(f"`{cid}`" for cid in sorted(absent)))
    lines.append("")
    lines.append("## 3. Reruns, exclusions and censoring")
    lines.append("")
    if not records:
        lines.append("_No runs yet._")
    else:
        excl_total = sum(v["excluded"] for v in executed.values())
        cens = sum(v["censored"] for v in executed.values())
        lines.append(f"- excluded runs: {excl_total} (exclusion rate "
                     f"{excl_total / max(1, len(records)):.1%} overall); rerun cap R_max = 3 "
                     "attempts per (cell, rep) (prereg §8.3).")
        lines.append(f"- censored (quiescence-timeout) runs: {cens}; censored runs are outcomes, "
                     "not missing values (prereg §8.3, method §7.3).")
        cell_excl = {cid: v for cid, v in executed.items()
                     if (v["valid"] + v["excluded"]) and v["excluded"] / (v["valid"] + v["excluded"]) > 0.20}
        if cell_excl:
            lines.append("- cells with exclusion rate > 20 % (inference labelled fragile per "
                         "method §5.6): " + ", ".join(f"`{c}`" for c in sorted(cell_excl)))
        else:
            lines.append("- no cell exceeds the 20 % exclusion flag.")
    # ---- 3b. fault fire-rate / attestation table (S5 F-I5) --------------------------------
    try:
        fire = compute_fire_rates(faults_log, records, reserved)
    except Exception:  # noqa: BLE001 - the register must still render early in the campaign
        fire = {"by_cell": [], "totals": {}}
    injected_rows = [r for r in fire["by_cell"]
                     if r["fault"] not in ("F0", "none", "ctrl")]
    lines.append("")
    lines.append("## 3b. Fault fire-rate and attestation per cell (generated from "
                 "`results/faults.log`)")
    lines.append("")
    if not injected_rows:
        lines.append("_No injected runs recorded yet._")
    else:
        lines.append(f"- injected cells: {len(injected_rows)}; executed injected runs: "
                     f"{fire['totals'].get('executed_injected')}; fired: "
                     f"{fire['totals'].get('fired')}; missing receipts: "
                     f"{fire['totals'].get('missing_receipt')}; fire rate "
                     f"{fire['totals'].get('fire_rate', 0):.3f}; exclusion rate "
                     f"{fire['totals'].get('exclusion_rate', 0):.3f}.")
        lines.append("")
        lines.append("| stage | mechanism | fault | reserved | executed | valid | excluded | "
                     "fired | restored | missing receipt | fire rate | exclusion rate |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
        for row in injected_rows:
            lines.append(
                f"| {row['stage']} | {row['mechanism']} | {row['fault']} | {row['reserved']} | "
                f"{row['executed']} | {row['valid']} | {row['excluded']} | {row['fired']} | "
                f"{row['restored']} | {row['missing_receipt']} | "
                f"{(row['fire_rate'] if row['fire_rate'] is not None else 0):.2f} | "
                f"{(row['exclusion_rate'] if row['exclusion_rate'] is not None else 0):.2f} |")
    lines.append("")
    lines.append("## 4. Interpretation / erratum register (frozen)")
    lines.append("")
    lines.append("These are the mechanical decisions the frozen documents left open or that "
                 "required an erratum. They were ruled by the lead before the analysis freeze "
                 "(`docs/P2-RULINGS.md`); none alters a cell, N, factor or statistic.")
    lines.append("")
    lines.append("| ID | Topic | Decision | Reason | Effect | Paper location |")
    lines.append("|---|---|---|---|---|---|")
    for item in INTERPRETATIONS:
        lines.append(f"| {item['id']} | {item['title']} | {item['decision']} | "
                     f"{item['reason']} | {item['effect']} | {item['paper']} |")
    lines.append("")
    lines.append("## 5. Environment deviations (docs/prereg.md §11)")
    lines.append("")
    lines.append("The host has no Docker, no WSL2 and no administrator rights "
                 "(`docs/SUBSTRATE-VALIDATION.md`). Therefore: isolation = native Windows "
                 "processes with recorded command lines/PIDs/affinity; 'image digests' = SHA-256 "
                 "of vendored archives + configuration hashes; resource limits = affinity mask, "
                 "priority class, `-Xmx`, Hikari pool, broker/PG config; restart cadence = fresh "
                 "topics/group/schema + fresh consumer JVMs per run, broker/PG processes "
                 "restarted every 50 runs (D6). Broker failure is a process kill, not a machine "
                 "loss (external-validity threat). All are reported in the paper's threats "
                 "section and are present in every run record (`host`, `versions`, "
                 "`process_limits`).")
    lines.append("")
    return "\n".join(lines) + "\n"


def write_deviations(matrix_path: str, records_dir: str, out_path: str) -> str:
    matrix = _load_plan(matrix_path)
    records = _load_records(records_dir)
    summaries_dir = os.path.join(os.path.dirname(records_dir), "summaries")
    reserved = _load_reserved(summaries_dir)
    results_root = os.path.dirname(os.path.dirname(records_dir))
    faults_log = os.path.join(results_root, "faults.log")
    text = render_deviations(matrix=matrix, records=records, matrix_path=matrix_path,
                             faults_log=faults_log, reserved=reserved)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)
    return os.path.abspath(out_path)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate results/DEVIATIONS.md")
    parser.add_argument("--matrix", default=os.path.join(_REPO, "experiments", "matrix.yaml"))
    parser.add_argument("--records", default=os.path.join(_REPO, "results", "raw",
                                                          "run-records"))
    parser.add_argument("--out", default=os.path.join(_REPO, "results", "DEVIATIONS.md"))
    args = parser.parse_args(argv)
    path = write_deviations(args.matrix, args.records, args.out)
    print(json.dumps({"deviations": path}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
