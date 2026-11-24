# results/harness/ — harness-validation evidence (gate G3, and G5 support)

These files are produced by the S4 harness-validation scripts and are referenced by analysis
table T1. They contain **harness evidence only**, never mechanism measurements.

## `g3-mutation.json`

Produced by:

```
python experiments/oracle/mutation_test.py --mutations 120
```

Gate G3 requirement (`docs/method.md` §7.6, `docs/prereg.md` §8.1): for each synthetic defect
class — drop a row, duplicate a row, flip a leg sign, update a row value, insert a spurious row,
delete a whole event — the oracle must detect **100 %** of ≥100 seeded mutations **and attribute
the defect to the correct invariant**.

The report contains, per class: mutations, detected, detected %, correctly attributed, attributed
%, expected primary attribution, allowed co-attribution (a partial entry is also structurally
malformed, so `I1` may co-fire), the attribution histogram, metric ranges, and any failures. It
also contains:

* `clean_control` — a clean synthetic fixture must produce **zero** violations and no
  attribution (false-positive guard);
* `input_integrity_controls` — tampered GT / source-dump gap / duplicate manifest record must each
  fail the source-integrity gate (the detection side of G5);
* `oracle.oracle_script_sha256` — the hashed oracle implementation used for the run.

Current result: **720/720 detected and correctly attributed, clean control clean, tamper
controls caught, passed=true** (120 mutations × 6 classes). The corpus is fully seeded
(`mutation_base_seed` recorded) and re-runnable; a failure exits non-zero.

The G5 Java↔Python contract vectors are `experiments/oracle/g5-vectors.json` (S3's Java
`GroundTruthDigest` reproduced all four digests byte-for-byte) and the lead's normative rows
`docs/derive-vectors.csv` (asserted by `experiments/tests/test_oracle.py`).
