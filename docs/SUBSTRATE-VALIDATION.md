# SUBSTRATE VALIDATION — native Windows experiment stack

Date: 2026-11-24. Purpose: prove that the measured experiments can run on this host despite the
absence of Docker, admin rights and WSL2, and record the evidence.

## 1. Toolchain (portable, no administrator rights)

| Component | Version | Provenance / integrity |
|---|---|---|
| JDK | Eclipse Temurin **21.0.13.1** (2026-11-17 LTS) | Adoptium API, extracted to local scratch |
| Maven | **3.9.9** | Apache archive |
| Kafka | **3.7.2** (Scala 2.13, KRaft) | Huawei Cloud Apache mirror; **SHA-512 verified against the official Apache checksum** `65C09FBE…3CDC` |
| PostgreSQL | **16.11** x64 portable binaries | EnterpriseDB binaries |
| Toxiproxy | **2.9.0** (server + CLI, windows-amd64) | GitHub release |
| TeX | MiKTeX 25.12; pdflatex 4.23; bibtex 4.2; biber 2.21 | on-the-fly package installation enabled |
| Python | 3.12 (pip 26.0.1) | for oracle/analysis |

Exact archive SHA-256 values and the pinned download URLs are in `tools/bootstrap-toolchain.ps1`
and are recorded by it into `VERSIONS.txt`; the digest block is copied to
`tools/TOOLCHAIN-VERSIONS.txt` for the public repository.

## 2. Kafka: 3-node KRaft cluster on Windows

- Invocation path: the distribution's Windows `.bat` wrappers fail with *"input line too long"*
  (119 jars exceed the 8191-character `cmd.exe` limit). The harness therefore invokes the Kafka
  main classes directly with a wildcard classpath
  (`java -cp "<kafka>\libs\*" kafka.Kafka server.properties`), which is equivalent and robust.
- Cluster: 3 combined `broker,controller` nodes, static quorum
  `1@127.0.0.1:9192,2@127.0.0.1:9193,3@127.0.0.1:9194`, data-plane listeners 9091/9092/9093,
  `default.replication.factor=3`, `min.insync.replicas=2`, transaction-state replication 3,
  auto topic creation off.
- Evidence: all three brokers logged `Kafka Server started (kafka.server.KafkaRaftServer)` and
  `Transition from STARTING to STARTED`; listening sockets present for 9091/9092/9093 and
  9192/9193/9194. Topic creation and broker-API queries executed through
  `kafka.tools.TopicCommand` / `kafka.admin.BrokerApiVersionsCommand`.
- The console producer/consumer `.bat` pipe test was abandoned as a test vehicle (stdin-pipe
  behaviour on Windows); the artifact uses the Kafka Java client API, which is the same code path
  the paper measures. No result in the paper will depend on console tools.

## 3. PostgreSQL 16.11

- `initdb` (UTF-8, trust auth for the local test) followed by `pg_ctl start` on 127.0.0.1:55432;
  server logged `database system is ready to accept connections`; TCP connections and SQL executed;
  a unique index on `(event_id, leg_no)` was created and exercised in the smoke schema.
- Production runs use a dedicated data directory and role with `INSERT, SELECT` only on `posting`
  (append-only attestation in `docs/method.md` Appendix A).

## 4. Manuscript pipeline

- Official RITA class (`SelfArx.cls`) compiled at 12 pt with the full package set used by the
  manuscript (babel, hyperref, amsmath, tikz, tcolorbox, algorithm/algpseudocode, booktabs,
  abntex2cite numeric citations).
- `build.ps1` (pdflatex → bibtex → pdflatex ×2, log analysis, non-zero exit on error/undefined)
  builds `main.pdf` successfully; template example builds 5 pages and the manuscript skeleton
  builds clean.
- `latexmk` is unusable on this host (no Perl) and is not used.

## 5. Residual risks carried into the paper's threats section

1. Single-host deployment (3 broker processes, one OS) differs from a multi-AZ cluster; broker
   failure is a process kill, not a machine loss. Declared as external-validity threat.
2. Shared 12-core/16-GB host means mechanisms can contend with each other's leftovers; reset
   protocol and periodic full restarts are pre-registered (D6).
3. Windows NTFS + Java 21 scheduling adds noise vs a Linux server; all mechanisms run under the
   identical environment and paired seeds (D15).
4. Cloud-synced working directory is avoided for runtime data (D11); results are copied in.

## 6. Reproduction of this validation

```powershell
powershell -ExecutionPolicy Bypass -File tools/bootstrap-toolchain.ps1   # portable toolchain
# Kafka 3-node KRaft smoke (harness version arrives with S3; this P1 record documents the manual run)
# PostgreSQL: initdb + pg_ctl start + psql (commands in this file)
powershell -ExecutionPolicy Bypass -File build.ps1 -Main main.tex -Clean
```
