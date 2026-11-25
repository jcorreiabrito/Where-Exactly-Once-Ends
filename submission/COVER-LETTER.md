# Cover letter — Revista de Informática Teórica e Aplicada (RITA)

**Manuscript:** *Where Exactly-Once Ends: Preserving Double-Entry Ledger Invariants When Apache Kafka
Pipelines Write to External Stores*

**Article type:** Research article

**Corresponding author:** João Brito, Universidade Federal do ABC (UFABC), Santo André, SP, Brazil —
correiadebrito.joao@gmail.com

Dear Editors,

Please consider the enclosed manuscript for publication in RITA. The paper studies a practical but
under-modelled boundary: Kafka's exactly-once semantics guarantees Kafka's own durable state, not an
external relational ledger. We give a formal ledger state machine, an enumeration of crash-point
classes and the two windows ($W_1$ duplicate, $W_2$ permanent loss) that separate effect durability
from progress durability; we analyse nine mechanism configurations (naive at-least-once, Kafka
transactions with internal and external sinks, transactional outbox, idempotency key,
in-transaction offsets and three combinations) with invariant-preservation propositions and a
bounded machine-checked model, and we score them with an independent, mutation-tested oracle.

The empirical part is reported with explicit limits: the pre-registered full campaign required a
dedicated Linux/container host that was not available, so the paper reports the reduced but fully
instrumented programme of Amendment A7 — 152 valid runs at 1k events over 40 (mechanism, crash-class)
cells with $N\le3$, plus descriptive committed-event throughput. The results are descriptive (no
confirmatory hypothesis tests are claimed): the duplicate window is closable (only unguarded
mechanisms duplicated; guarded ones stayed clean), while the lost-effect window was fatal in all
24/24 runs, including the in-transaction-offset design. We believe this honest reduced study, the
formal boundary analysis, and the public artifact are still a useful contribution, and we state the
power limitation prominently rather than overclaiming.

The artifact (service, fault injectors, independent oracle, run records and the scripts that
regenerate every table) is public at https://github.com/jcorreiabrito/Where-Exactly-Once-Ends under
an open license; the manuscript contains the required Declaration of AI-Assisted Use in Writing and
the Conflict of Interest / Ethics statements. The author is employed in the financial sector but
conducted the work independently using only public technology and synthetic data; no employer
systems, data or infrastructure were involved and no employer is identified.

The manuscript is original, is not under consideration elsewhere, and all authors (one) approved
this submission. We confirm the anonymized review copy is free of author identification.

Sincerely,
João Brito
