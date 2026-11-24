"""Independent oracle package for the Where-Exactly-Once-Ends experiments.

Modules
-------
``derive``  re-implementation of the deterministic event derivation and canonical GT digest
            (docs/method.md §1.4.1, docs/CONTRACTS.md §4).
``oracle``  input verification (GT/source integrity), the I1a/I1b/I2/I3/I4 predicates
            (docs/method.md §2.5), publication metrics, and the ``oracle_<run>.json`` writer.

The package is intentionally dependency-free in its predicate path (standard library only) so a
third party can re-run the checker without the experiment toolchain.  The live *dump* subcommands
import psycopg2 / confluent-kafka lazily and are never involved in computing a verdict.

Independence: no module here imports or reads anything under ``artifact/**``.
"""

__all__ = ["derive", "oracle"]
