# P1 GATE — package submitted for author approval

Date: 2026-11-24 (UTC-03). Status: **AWAITING AUTHOR APPROVAL. Implementation is held.**

## 1. P1 deliverables

| Deliverable | Path | Status | Acceptance evidence |
|---|---|---|---|
| Method document (run 2, A1 revision) | `docs/method.md` (24.6k words, 1,468 lines) | DRAFT v2.0 — awaiting approval | 12 required sections + §13 decisions + §14 readiness verdict; 72-cell fault table; 63-cell crash-class table; C1–C12 crash points; 12 propositions; 8 counterexample traces |
| Pre-registration | `docs/prereg.md` (6.5k words) | DRAFT — awaiting approval | exact matrices A–D, N per cell, run protocol, per-run JSON schema, frozen statistics, compute-projection method, cut ladder, environment deviations, freeze checklist |
| Formal model draft | `formal/ledger.tla` (216 lines) + `formal/README.md` | DRAFT, **not machine-checked** (no TLC on host) | explicitly labelled unverified; TLC bounds/commands documented |
| Literature base | `refs/refs.bib`, `refs/verified.csv`, `lit/matrix.csv`, `lit/search-strategy.md`, `refs/RETRIEVALS.md`, `tools/verify-refs.py` | COMPLETE | `verify-refs.py --live`: 63 entries, 49 peer-reviewed Crossref-verified DOIs, 14 gray (22.2 %), 228/228 checks PASS, 195 cached sources |
| Toolchain | `tools/bootstrap-toolchain.ps1` (+ `tools/TOOLCHAIN-VERSIONS.txt`) | COMPLETE (native-process substrate) | JDK 21.0.13.1, Maven 3.9.9, Kafka 3.7.2 (SHA-512 verified), PostgreSQL 16.11, Toxiproxy 2.9.0 |
| Substrate validation | `docs/SUBSTRATE-VALIDATION.md` | COMPLETE | 3-node KRaft cluster started on Windows (ports 9091/9092/9093; all brokers `STARTED`), PostgreSQL 16 initdb+start+TCP verified, RITA template + manuscript build green |

## 2. Material deviations from the original brief (require author awareness)
1. **No Docker / no admin / WSL2 disabled** on this host. The stack runs as native Windows processes
   (3 Kafka KRaft brokers, PostgreSQL via `pg_ctl`, Toxiproxy). This is written into `docs/prereg.md`
   §11 (environment deviations) and will be a limitation in the paper. Reproduction in CI uses
   containers (GitHub Actions), which keeps the public artifact container-reproducible.
2. **Official RITA template URL is 404** (`inf.ufrgs.br/revista/RITA2018.zip`); template recovered
   from the Wayback Machine snapshot of the official archive (SHA-256 recorded in
   `submission/VENUE-RULES.md`). The class is committed.
3. **Template 10 pt vs written checklist 12 pt**: written guidelines win per the author's instruction;
   the official class is compiled at 12 pt, single-spaced.
4. **Kafka pinned to 3.7.2** (last widely-deployed 3.x line; KRaft, Java 21-compatible). Verified
   SHA-512 against the official Apache checksum. Version recorded in every run record.

## 3. Decisions D1–D15 requiring explicit approval (`docs/method.md` §13)
All fifteen have drafted options, trade-offs and a recommended default. Summary:

| D | Topic | Draft recommendation | Impact if not approved |
|---|---|---|---|
| D1 | Campaign budget `H` + coverage package | cut ladder by default; need `H` | N and coverage cannot be frozen; P3 cannot start |
| D2 | `ordering ∈ {strict, early}` probe for H3's loss arm | approve probe | H3 loss component reportable only as inconclusive |
| D3 | M2 realisation | inbox + outbox log + relay | H2 untestable as specified |
| D4 | Injector targeting literal vs semantic-role | literal in B/C, semantic in A Stage | n/a cells and exclusion policy change |
| D5 | F6 semantics (protocol replay vs rewind) | protocol-level replay only | breaks M1-kafka positive control if rewind |
| D6 | Reset protocol (full restart vs logical + periodic) | logical per run + full restart every 50 runs | per-run restarts would dominate wall-clock |
| D7 | Stage B sensitivity interpretation | both sensitivity blocks at 10k | 720–1,440 runs difference |
| D8 | I1 formalisation | structural well-formedness ∧ signed sum zero (I1a/I1b) | balance blindness to duplicate/mangled entries |
| D9 | I4 formalisation | end-state completeness at quiescence | liveness claim unmeasurable |
| D10 | Ground-truth storage | write+fsync+digest, verify, delete | 10s of GB needed (> disk) |
| D11 | Scratch path | local non-synced scratch + curated copy | I/O interference and quota risk |
| D12 | Practical-adequacy θ | θ = 1 %, "certified" only with model proof + D-R3 | decision table wording |
| D13 | M1-kafka comparability | positive control only, excluded from performance comparisons | apples-to-oranges bias |
| D14 | Config/fairness contract | adopt Appendix C tables | implementation cannot start |
| D15 | Paired seeds | paired seeds + Fisher sensitivity | loses McNemar power |

## 4. Compute envelope (no fabricated projection)
Nominal design: **7,866 matrix runs + 20 validation runs** (pilot 216, Stage A 1,830, Stage B 4,020,
Stage C 1,800). Per-run cost is unknown until the pilot (A1 §12); the projection worksheet in
`docs/prereg.md` §9.2 is committed with blanks that may only be filled from pilot measurements.
Cut ladder (fixed): 1M block → partition sensitivity → N≥20 correctness → escalate to author.
Known host constraints: 12 logical CPUs, 16 GB RAM, ~11.7 GB free disk at P1 time, cloud-synced
workspace (scratch on local disk per D11).

## 5. What starts immediately after approval
1. S3: independent oracle first (ground truth before mechanisms), then nine mechanism configurations,
   Level-1 failpoints, Level-2 injectors, load generator; harness-validation gate G1–G8.
2. S4: pilot (N=3) → projection worksheet → **P2 gate (author re-approval of compute budget)**.
3. S5: early red-team review of oracle design (before Stage A).

## 6. Sign-off requested
- [ ] Approve `docs/method.md` as the binding method (with amendments noted).
- [ ] Approve `docs/prereg.md` as frozen pre-registration once the P2 projection is filled.
- [ ] Resolve D1–D15 (or accept all draft recommendations).
- [ ] Adopt or edit the formal-model draft (`formal/ledger.tla`), which the author owns (A1 §3).
- [ ] Confirm available wall-clock `H` and preferred measurement window.
