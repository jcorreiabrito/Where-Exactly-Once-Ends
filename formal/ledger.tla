------------------------------------------------------------------------------
-- ledger.tla  --  DRAFT, NOT YET MACHINE-CHECKED
--
-- Bounded TLA+ model of the crash-point semantics of a Kafka -> PostgreSQL
-- double-entry ledger pipeline.  Companion to docs/method.md sections 3-6.
--
-- STATUS (P1):  This module is a DRAFT delivered for author review.  A TLA+
-- toolchain is not installed on the reported host at P1, so the module has
-- NOT been checked with TLC.  Do not present any TLC result until
-- formal/README.md records the checked bounds and the exact TLC version.
--
-- INTENDED BOUNDS (see formal/README.md):
--     MaxEvents = 3, BatchSize = 2, single partition.
--
-- WHAT IS MODELLED
--   * durable ledger postings as per-(event,leg) multiplicities
--   * durable consumption progress
--   * durable publication-stream records (only where they share an atomic
--     commit with progress, i.e. the Kafka-transaction family: this is where
--     the stream/ledger divergence of docs/method.md trace X4 appears)
--   * process crash at any point, recovery from the mechanism's own progress
--   * mechanisms as four booleans (see CONFIGURATIONS in README):
--       DoDedup  - unique index on (event_id, leg_no) / inbox claim
--       DoAtomic - ledger effect and progress in one atomic commit (M4, M2)
--       DoTxn    - publication stream and progress in one atomic commit
--                  (Kafka transaction: M1-kafka, M1x)
--       DoEarly  - progress may commit before the effect (W2 probe)
--
-- Invariants checked: TypeOK, I1Balance, I3Unique, I4CompleteWhenDone,
-- StrictProgressBound.  The expected outcome per configuration (to be
-- confirmed by TLC at P2) is stated in formal/README.md; it is a design
-- expectation, not a result.
------------------------------------------------------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS N,          \* number of events; charge with N = 3
          BatchSize,  \* records per poll batch; charge with BatchSize = 2
          DoDedup,    \* TRUE: idempotent effect keyed by (event, leg)
          DoAtomic,   \* TRUE: ledger effect and progress commit atomically
          DoTxn,      \* TRUE: stream records and progress commit atomically
          DoEarly     \* TRUE: progress may commit before the effect (probe)

ASSUME N \in Nat /\ BatchSize \in Nat /\ BatchSize >= 1 /\ N >= 1
ASSUME N <= 3 /\ BatchSize <= 2      \* documented model bound

Legs     == {1, 2}
Events   == 1..N
Keys     == { <<e, l>> : e \in Events, l \in Legs }
MaxOff   == N

VARIABLES ledger,      \* [Keys -> 0..N]   durable postings (multiplicity)
          stream,      \* [Events -> 0..N] durable publication records
          progress,    \* 0..N  durable next offset
          effectMax,   \* 0..N  highest offset whose ledger effect is durable
          phase,       \* "idle" | "effectPending" | "effectDurable"
          crashed,     \* BOOLEAN
          done         \* BOOLEAN

vars == <<ledger, stream, progress, effectMax, phase, crashed, done>>

------------------------------------------------------------------------------
-- Pure state functions and helper operators
------------------------------------------------------------------------------

BatchFrom(p) == (p + 1) .. Min(p + BatchSize, N)

PostsOf(p) == { <<e, l>> : e \in BatchFrom(p), l \in Legs }

AddsOf(p) == IF DoDedup THEN { k \in PostsOf(p) : ledger[k] = 0 } ELSE PostsOf(p)

NewLedger(p) ==
  [ k \in Keys |-> IF k \in AddsOf(p) THEN ledger[k] + 1 ELSE ledger[k] ]

NewStream(p) ==
  [ e \in Events |-> IF e \in BatchFrom(p) THEN stream[e] + 1 ELSE stream[e] ]

AtEnd == progress = N

------------------------------------------------------------------------------
-- Initial state
------------------------------------------------------------------------------

Init ==
  /\ ledger    = [ k \in Keys   |-> 0 ]
  /\ stream    = [ e \in Events |-> 0 ]
  /\ progress  = 0
  /\ effectMax = 0
  /\ phase     = "idle"
  /\ crashed   = FALSE
  /\ done      = FALSE

------------------------------------------------------------------------------
-- Transitions
--
-- Effect-request: the batch has been read and processing has started but
-- nothing is durable yet (crash points C1/C2 of docs/method.md).
--------------------------------------------------------------------------------
RequestEffect ==
  /\ ~crashed /\ ~done /\ ~DoAtomic
  /\ phase = "idle"
  /\ ~AtEnd
  /\ phase' = "effectPending"
  /\ UNCHANGED <<ledger, stream, progress, effectMax, crashed, done>>

------------------------------------------------------------------------------
-- Effect commit: the ledger postings of the batch become durable.
-- Crash between the effect and the progress is crash point C4 (W1).
--------------------------------------------------------------------------------
EffectCommit ==
  /\ ~crashed /\ ~done /\ ~DoAtomic
  /\ phase = "effectPending"
  /\ LET p == progress IN
     /\ ledger'    = NewLedger(p)
     /\ effectMax' = Min(p + BatchSize, N)
     /\ phase'     = "effectDurable"
     /\ UNCHANGED <<stream, progress, crashed, done>>

------------------------------------------------------------------------------
-- Progress commit (non-atomic mechanisms): after the effect (strict ordering)
-- or, in the DoEarly probe, before it (crash point C5 / W2).
-- Only ledger-atomic mechanisms advance effectMax with the progress.
--------------------------------------------------------------------------------
ProgressCommit ==
  /\ ~crashed /\ ~done /\ ~DoAtomic
  /\ ~AtEnd
  /\ ( phase = "effectDurable" \/ (DoEarly /\ phase = "idle") )
  /\ LET p == progress IN
     /\ progress'  = Min(p + BatchSize, N)
     /\ phase'     = "idle"
     /\ UNCHANGED <<ledger, stream, effectMax, crashed, done>>

------------------------------------------------------------------------------
-- Kafka transaction commit (M1x): stream records and progress become durable
-- together, atomically, after the external ledger effect was made durable.
-- A crash before this point discards the stream records but keeps the ledger
-- rows -> the stream/ledger divergence of trace X4.
--------------------------------------------------------------------------------
TxnCommit ==
  /\ ~crashed /\ ~done /\ DoTxn /\ ~DoAtomic
  /\ phase = "effectDurable"
  /\ LET p == progress IN
     /\ stream'    = NewStream(p)
     /\ progress'  = Min(p + BatchSize, N)
     /\ phase'     = "idle"
     /\ UNCHANGED <<ledger, effectMax, crashed, done>>

------------------------------------------------------------------------------
-- Atomic commit (M1-kafka, M4): ledger effect and progress (and, when DoTxn,
-- the stream records) become durable in one step.  No intermediate state
-- exists, so the W1/W2 windows are unreachable for this configuration.
--------------------------------------------------------------------------------
AtomicCommit ==
  /\ ~crashed /\ ~done /\ DoAtomic
  /\ phase = "idle"
  /\ ~AtEnd
  /\ LET p == progress IN
     /\ ledger'    = NewLedger(p)
     /\ progress'  = Min(p + BatchSize, N)
     /\ effectMax' = Min(p + BatchSize, N)
     /\ stream'    = IF DoTxn THEN NewStream(p) ELSE stream
     /\ UNCHANGED <<phase, crashed, done>>

------------------------------------------------------------------------------
-- Crash at any point (crash points C1-C12; the durable variables persist).
-- Recovery resumes from the durable progress and in-flight work is lost.
--------------------------------------------------------------------------------
Crash ==
  /\ ~crashed
  /\ crashed' = TRUE
  /\ phase'   = "idle"
  /\ UNCHANGED <<ledger, stream, progress, effectMax, done>>

Recover ==
  /\ crashed
  /\ crashed' = FALSE
  /\ UNCHANGED <<ledger, stream, progress, effectMax, phase, done>>

Finish ==
  /\ ~crashed /\ ~done
  /\ phase = "idle" /\ AtEnd
  /\ done' = TRUE
  /\ UNCHANGED <<ledger, stream, progress, effectMax, phase, crashed>>

Next ==
  \/ RequestEffect \/ EffectCommit \/ ProgressCommit \/ TxnCommit
  \/ AtomicCommit \/ Crash \/ Recover \/ Finish

Spec == Init /\ [][Next]_vars

------------------------------------------------------------------------------
-- Invariants (docs/method.md section 2.3)
------------------------------------------------------------------------------

TypeOK ==
  /\ ledger    \in [ Keys   -> 0..N ]
  /\ stream    \in [ Events -> 0..N ]
  /\ progress  \in 0..N
  /\ effectMax \in 0..N
  /\ phase     \in {"idle", "effectPending", "effectDurable"}
  /\ crashed   \in BOOLEAN
  /\ done      \in BOOLEAN

I1Balance == \A e \in Events : ledger[<<e,1>>] = ledger[<<e,2>>]

I3Unique == \A k \in Keys : ledger[k] <= 1

I4CompleteWhenDone ==
  done => (\A e \in Events : ledger[<<e,1>>] = 1 /\ ledger[<<e,2>>] = 1)

StreamCompleteWhenDone ==
  done => (\A e \in Events : stream[e] = 1)

StrictProgressBound ==
  ~DoEarly => progress <= effectMax

=============================================================================
