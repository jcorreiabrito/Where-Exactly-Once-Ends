# RETRIEVALS — every retrieval attempt (success and failure)

Generated: 2026-11-24T12:33:03Z from `data/raw/lit/_fetchlog.jsonl` (476 logged events). Raw responses are cached under `data/raw/lit/`; metadata lookups under `data/raw/lit/_crossref/`; discovery query results under `lit/_discovery/`.

## Summary

- Full-text / landing-page fetches: **108 attempts, 43 succeeded**.
- Crossref DOI resolutions: **69 attempts, 67 HTTP 200**.
- OA/abstract metadata API calls: **178 attempts** (Unpaywall, Semantic Scholar, OpenAlex).
- robots.txt fetches: **34 hosts**; any disallowed path was skipped and is shown below.
- Crossref discovery queries: **83 distinct query strings** (87 logged executions, one pass aborted on a console-encoding error; results in `lit/_discovery/`).

## A. Full-text / landing-page retrieval attempts

| key | requested URL | HTTP | outcome | cache |
|---|---|---|---|---|
| `arcuri2011` | - | None | no_oa_url | `-` |
| `arcuri2012` | https://orbilu.uni.lu/bitstream/10993/1071/1/paper_stvr_icse_2012.pdf | 0 | failed | `-` |
| `aries1992` | https://dl.acm.org/doi/pdf/10.1145/128765.128770 | 403 | failed | `-` |
| `aws-idempotency` | https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/ | 200 | ok | `data/raw/lit/aws-idempotency.html` |
| `bailis2014` | https://arxiv.org/pdf/1402.2237 | 200 | ok | `data/raw/lit/bailis2014.pdf` |
| `basiri2016` | https://arxiv.org/pdf/1702.05843 | 200 | ok | `data/raw/lit/basiri2016.pdf` |
| `berenson1995` | https://dl.acm.org/doi/pdf/10.1145/568271.223785 | 403 | failed | `-` |
| `brown2001` | - | None | no_oa_url | `-` |
| `chandy1985` | https://dl.acm.org/doi/pdf/10.1145/214451.214456 | 403 | failed | `-` |
| `cliff1993` | - | None | no_oa_url | `-` |
| `clopper1934` | https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=TITLE:%22The%20use%20of%20confidence%20or%20fiducial%20limits%20illustrated%20in%20the%20case%20of%20the%20binomial%22&format=json&resultType=core | 200 | ok | `data/raw/lit/clopper1934.txt` |
| `clopper1934` | - | None | no_oa_url | `-` |
| `collberg2016` | - | None | no_oa_url | `-` |
| `cp2015` | - | None | no_oa_url | `-` |
| `cpa2024` | https://doi.org/10.1016/j.cpa.2024.102702 | 403 | failed | `-` |
| `cpa2024` | - | None | no_oa_url | `-` |
| `crashmonkey2019` | https://dl.acm.org/doi/pdf/10.1145/3320275 | 403 | failed | `-` |
| `dataflow2015` | - | None | no_oa_url | `-` |
| `dean2013` | - | None | no_oa_url | `-` |
| `debezium-outbox` | https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html | 200 | ok | `data/raw/lit/debezium-outbox.html` |
| `demsar2006` | https://www.jmlr.org/papers/v7/demsar06a.html | 200 | ok | `data/raw/lit/demsar2006.html` |
| `dstreams2013` | http://dl.acm.org/ft_gateway.cfm?id=2522737&type=pdf | 403 | failed | `-` |
| `efron1979` | https://projecteuclid.org/journals/annals-of-statistics/volume-7/issue-1/Bootstrap-Methods-Another-Look-at-the-Jackknife/10.1214/aos/1176344552.pdf | 200 | failed | `-` |
| `elle2020` | https://arxiv.org/pdf/2003.10554 | 200 | ok | `data/raw/lit/elle2020.pdf` |
| `eoscorba1999` | - | None | no_oa_url | `-` |
| `eosmid2012` | - | None | no_oa_url | `-` |
| `eosmsg2001` | - | None | no_oa_url | `-` |
| `eventsourcing2022` | https://dl.acm.org/doi/pdf/10.1145/3575879.3576006 | 403 | failed | `-` |
| `expose2021` | https://doi.org/10.1007/978-3-030-84924-5_2 | 200 | ok | `data/raw/lit/expose2021.bin` |
| `expose2021` | - | None | no_oa_url | `-` |
| `fdb2021` | - | None | no_oa_url | `-` |
| `fisher1922` | https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=TITLE:%22On%20the%20interpretation%20of%20chi%20square%20from%20contingency%20tables%22&format=json&resultType=core | 200 | ok | `data/raw/lit/fisher1922.txt` |
| `fisher1922` | - | None | no_oa_url | `-` |
| `flink2017` | - | None | no_oa_url | `-` |
| `friedman1937` | - | None | no_oa_url | `-` |
| `georges2007` | https://biblio.ugent.be/publication/417084/file/448219.pdf | 200 | ok | `data/raw/lit/georges2007.pdf` |
| `hanley1983` | https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:%2210.1001/jama.1983.03330370053031%22&format=json&resultType=core | 200 | ok | `data/raw/lit/hanley1983.txt` |
| `hanley1983` | - | None | no_oa_url | `-` |
| `hdrhistogram` | https://raw.githubusercontent.com/HdrHistogram/HdrHistogram/master/README.md | 200 | ok | `data/raw/lit/hdrhistogram.html` |
| `hdrhistogram` | https://raw.githubusercontent.com/HdrHistogram/HdrHistogram/master/README.md | 0 | failed | `-` |
| `helland-msr` | https://www.microsoft.com/en-us/research/publication/idempotence-is-not-a-medical-condition/ | 404 | failed | `-` |
| `helland2012` | https://queue.acm.org/detail.cfm?id=2187821 | None | skipped_robots | `-` |
| `helland2012` | - | None | no_oa_url | `-` |
| `hoefler2015` | - | None | no_oa_url | `-` |
| `holland1987` | - | None | no_oa_url | `-` |
| `jackson2002` | - | None | no_oa_url | `-` |
| `kafka-design` | https://kafka.apache.org/documentation/#semantics | 200 | ok | `data/raw/lit/kafka-design.html` |
| `kafka-design` | https://kafka.apache.org/_print/documentation/ | 0 | failed | `-` |
| `kafka-design` | https://raw.githubusercontent.com/apache/kafka/trunk/docs/design/design.md | 200 | ok | `data/raw/lit/kafka-design.txt` |
| `kafka-producer` | https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence | 200 | ok | `data/raw/lit/kafka-producer.html` |
| `kafka-producer` | https://kafka.apache.org/_print/documentation/ | 200 | ok | `data/raw/lit/kafka-producer.html` |
| `kafka-producer` | https://raw.githubusercontent.com/apache/kafka/trunk/docs/configuration/producer_configs.md | 404 | failed | `-` |
| `kafka-streams` | https://kafka.apache.org/documentation/streams/developer-guide/config-streams | 200 | ok | `data/raw/lit/kafka-streams.html` |
| `kafka-streams` | https://kafka.apache.org/39/documentation/streams/developer-guide/config-streams | 0 | failed | `-` |
| `kafka-streams` | https://raw.githubusercontent.com/apache/kafka/trunk/docs/streams/developer-guide/config-streams.md | 200 | ok | `data/raw/lit/kafka-streams.txt` |
| `kafka2015` | - | None | no_oa_url | `-` |
| `kip429` | https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol | 200 | ok | `data/raw/lit/kip429.html` |
| `kip447` | https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+Scalability+for+Transactional+Producers | 404 | failed | `-` |
| `kip447` | https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics | 200 | ok | `data/raw/lit/kip447.html` |
| `kip98` | https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging | 200 | ok | `data/raw/lit/kip98.html` |
| `kung1981` | https://dl.acm.org/doi/pdf/10.1145/319566.319567 | 403 | failed | `-` |
| `laigner2021` | - | None | no_oa_url | `-` |
| `lamport1978` | https://dl.acm.org/doi/pdf/10.1145/359545.359563 | 403 | failed | `-` |
| `lamport1994` | - | None | no_oa_url | `-` |
| `lsfs1992` | https://dl.acm.org/doi/pdf/10.1145/146941.146943 | 403 | failed | `-` |
| `mcnemar1947` | - | None | no_oa_url | `-` |
| `micro2026` | https://doi.org/10.1007/s00607-026-01701-5 | 200 | ok | `data/raw/lit/micro2026.bin` |
| `micro2026` | https://link.springer.com/content/pdf/10.1007/s00607-026-01701-5.pdf | 200 | failed | `-` |
| `millwheel2013` | - | None | no_oa_url | `-` |
| `mytkowicz2009` | - | None | no_oa_url | `-` |
| `naiad2013` | http://dl.acm.org/ft_gateway.cfm?id=2522738&type=pdf | 403 | failed | `-` |
| `newcombe1998` | https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=TITLE:%22Two-sided%20confidence%20intervals%20for%20the%20single%20proportion%22&format=json&resultType=core | 200 | ok | `data/raw/lit/newcombe1998.txt` |
| `pg-isolation` | https://www.postgresql.org/docs/current/transaction-iso.html | 200 | ok | `data/raw/lit/pg-isolation.html` |
| `pg-onconflict` | https://www.postgresql.org/docs/current/sql-insert.html | 200 | ok | `data/raw/lit/pg-onconflict.html` |
| `pg-prepare` | https://www.postgresql.org/docs/current/sql-prepare-transaction.html | 200 | ok | `data/raw/lit/pg-prepare.html` |
| `pipeflow2015` | - | None | no_oa_url | `-` |
| `raasveldt2018` | https://ir.cwi.nl/pub/27862/27862.pdf | 200 | ok | `data/raw/lit/raasveldt2018.pdf` |
| `reliablemsg2015` | - | None | no_oa_url | `-` |
| `repro2024` | https://doi.org/10.1016/j.cosrev.2024.100655 | 403 | failed | `-` |
| `repro2024` | https://www.sciencedirect.com/science/article/am/pii/S157401372400039X | 403 | failed | `-` |
| `rule3` | - | None | no_oa_url | `-` |
| `saga2019` | - | None | no_oa_url | `-` |
| `saga2022` | https://www.mdpi.com/2076-3417/12/12/6242 | 403 | failed | `-` |
| `saga2022` | https://www.mdpi.com/2076-3417/12/12/6242/pdf?version=1655804594 | 403 | failed | `-` |
| `sagas1987` | https://dl.acm.org/doi/pdf/10.1145/38714.38742 | 403 | failed | `-` |
| `sax2018` | https://dl.acm.org/doi/pdf/10.1145/3242153.3242155 | 403 | failed | `-` |
| `stripe-idempotency` | https://docs.stripe.com/api/idempotent_requests | 200 | ok | `data/raw/lit/stripe-idempotency.html` |
| `tene-latency` | https://www.infoq.com/presentations/latency-response-time/ | 200 | ok | `data/raw/lit/tene-latency.html` |
| `theodolite2021` | https://arxiv.org/pdf/2009.00304 | 200 | ok | `data/raw/lit/theodolite2021.pdf` |
| `toxiproxy` | https://raw.githubusercontent.com/Shopify/toxiproxy/master/README.md | 200 | ok | `data/raw/lit/toxiproxy.txt` |
| `triple2019` | https://doi.org/10.1111/acfi.12556 | 403 | failed | `-` |
| `triple2019` | - | None | no_oa_url | `-` |
| `triple2021` | https://doi.org/10.1016/j.bcra.2021.100037 | 403 | failed | `-` |
| `triple2021` | https://www.sciencedirect.com/science/article/pii/S2096720921000324/pdf | 403 | failed | `-` |
| `txncommit2006` | https://arxiv.org/pdf/cs/0408036 | 200 | ok | `data/raw/lit/txncommit2006.pdf` |
| `verdi2015` | https://dl.acm.org/doi/pdf/10.1145/2737924.2737958 | 403 | failed | `-` |
| `vogels2009` | - | None | no_oa_url | `-` |

## B. Crossref DOI resolution attempts

| key | DOI | HTTP | cache |
|---|---|---|---|
| `chandy1985` | 10.1145%2F214451.214456?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/chandy1985.json` |
| `millwheel2013` | 10.14778%2F2536222.2536229?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/millwheel2013.json` |
| `flink2017` | 10.14778%2F3137765.3137777?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/flink2017.json` |
| `dstreams2013` | 10.1145%2F2517349.2522737?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/dstreams2013.json` |
| `naiad2013` | 10.1145%2F2517349.2522738?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/naiad2013.json` |
| `dataflow2015` | 10.14778%2F2824032.2824076?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/dataflow2015.json` |
| `txncommit2006` | 10.1145%2F1132863.1132867?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/txncommit2006.json` |
| `sagas1987` | 10.1145%2F38713.38742?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/sagas1987.json` |
| `helland2012` | 10.1145%2F2160718.2160734?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/helland2012.json` |
| `eosmsg2001` | 10.1109%2Ficde.2001.914808?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/eosmsg2001.json` |
| `eosmid2012` | 10.1109%2Fprdc.2012.23?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/eosmid2012.json` |
| `eoscorba1999` | 10.1109%2Freldis.1999.805108?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/eoscorba1999.json` |
| `pipeflow2015` | 10.1145%2F2675743.2776774?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/pipeflow2015.json` |
| `laigner2021` | 10.14778%2F3484224.3484232?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/laigner2021.json` |
| `sax2018` | 10.1145%2F3242153.3242155?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/sax2018.json` |
| `kafka2015` | 10.14778%2F2824032.2824063?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/kafka2015.json` |
| `micro2026` | 10.1007%2Fs00607-026-01701-5?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/micro2026.json` |
| `saga2022` | 10.3390%2Fapp12126242?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/saga2022.json` |
| `saga2019` | 10.5220%2F0007918704830490?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/saga2019.json` |
| `theodolite2021` | 10.1016%2Fj.bdr.2021.100209?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/theodolite2021.json` |
| `expose2021` | 10.1007%2F978-3-030-84924-5_2?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/expose2021.json` |
| `hat2014` | 10.14778%2F2735461.2735462?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/hat2014.json` |
| `berenson1995` | 10.1145%2F223784.223785?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/berenson1995.json` |
| `bailis2014` | 10.14778%2F2735508.2735509?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/bailis2014.json` |
| `aries1992` | 10.1145%2F128765.128770?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/aries1992.json` |
| `cpa2024` | 10.1016%2Fj.cpa.2024.102702?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/cpa2024.json` |
| `triple2021` | 10.1016%2Fj.bcra.2021.100037?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/triple2021.json` |
| `triple2019` | 10.1111%2Facfi.12556?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/triple2019.json` |
| `georges2007` | 10.1145%2F1297027.1297033?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/georges2007.json` |
| `arcuri2011` | 10.1145%2F1985793.1985795?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/arcuri2011.json` |
| `arcuri2012` | 10.1002%2Fstvr.1486?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/arcuri2012.json` |
| `hoefler2015` | 10.1145%2F2807591.2807644?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/hoefler2015.json` |
| `raasveldt2018` | 10.1145%2F3209950.3209955?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/raasveldt2018.json` |
| `mytkowicz2009` | 10.1145%2F1508244.1508275?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/mytkowicz2009.json` |
| `cliff1993` | 10.1037%2F0033-2909.114.3.494?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/cliff1993.json` |
| `efron1979` | 10.1214%2Faos%2F1176344552?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/efron1979.json` |
| `friedman1952` | 10.1080%2F01621459.1952.10483441?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/friedman1952.json` |
| `clopper1934` | 10.1093%2Fbiomet%2F26.4.404?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/clopper1934.json` |
| `hanley1983` | 10.1001%2Fjama.1983.03330370053031?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/hanley1983.json` |
| `mcnemar1947` | 10.1007%2FBF02295996?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/mcnemar1947.json` |
| `holm1979` | 10.2307%2F4615733?mailto=correiadebrito.joao%40gmail.com | 404 | `data/raw/lit/_crossref/holm1979.json` |
| `fisher1922` | 10.2307%2F2340521?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/fisher1922.json` |
| `lamport1994` | 10.1145%2F177492.177726?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/lamport1994.json` |
| `jackson2002` | 10.1145%2F505145.505149?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/jackson2002.json` |
| `dean2013` | 10.1145%2F2408776.2408794?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/dean2013.json` |
| `basiri2016` | 10.1109%2FMS.2016.60?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/basiri2016.json` |
| `ironfleet2015` | 10.1145%2F2815400.2815402?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/ironfleet2015.json` |
| `verdi2015` | 10.1145%2F2737924.2737958?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/verdi2015.json` |
| `fdb2021` | 10.1145%2F3448016.3457559?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/fdb2021.json` |
| `collberg2016` | 10.1145%2F2812803?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/collberg2016.json` |
| `kung1981` | 10.1145%2F319566.319567?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/kung1981.json` |
| `lamport1978` | 10.1145%2F359545.359563?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/lamport1978.json` |
| `spanner2013` | 10.1145%2F2491245.2491248?mailto=correiadebrito.joao%40gmail.com | 404 | `data/raw/lit/_crossref/spanner2013.json` |
| `friedman1937` | 10.1080%2F01621459.1937.10503522?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/friedman1937.json` |
| `elle2020` | 10.14778%2F3430915.3430918?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/elle2020.json` |
| `crashmonkey2019` | 10.1145%2F3320275?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/crashmonkey2019.json` |
| `lsfs1992` | 10.1145%2F146941.146943?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/lsfs1992.json` |
| `vogels2009` | 10.1145%2F1435417.1435432?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/vogels2009.json` |
| `repro2024` | 10.1016%2Fj.cosrev.2024.100655?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/repro2024.json` |
| `holland1987` | 10.2307%2F2531823?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/holland1987.json` |
| `spanner2013b` | 10.1145%2F2485732.2485756?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/spanner2013b.json` |
| `reliablemsg2015` | 10.1145%2F2675743.2771878?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/reliablemsg2015.json` |
| `eventsourcing2022` | 10.1145%2F3575879.3576006?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/eventsourcing2022.json` |
| `brown2001` | 10.1214%2Fss%2F1009213286?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/brown2001.json` |
| `cp2015` | 10.1080%2F00031305.2015.1019646?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/cp2015.json` |
| `rule3` | 10.1515%2Fijb-2022-0061?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/rule3.json` |
| `jsta2014` | 10.2991%2Fjsta.2014.13.4.3?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/jsta2014.json` |
| `shortcp2009` | 10.1080%2F03610910903391270?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/shortcp2009.json` |
| `gencp2006` | 10.1080%2F10629360500107527?mailto=correiadebrito.joao%40gmail.com | 200 | `data/raw/lit/_crossref/gencp2006.json` |

## C. Metadata / open-access discovery APIs

| API | attempts | HTTP 200 | purpose | cache pattern |
|---|---|---|---|---|
| unpaywall | 58 | 58 | open-access location lookup per DOI | `data/raw/lit/_crossref/<key>.unpaywall.json` |
| openalex | 63 | 63 | abstract recovery + OA PDF location | `data/raw/lit/<key>.openalex.json` |
| s2 | 57 | 14 | Semantic Scholar metadata/abstract attempt | `data/raw/lit/<key>.s2.json` |

Semantic Scholar returned HTTP 200 but no abstract for the DOI batch; OpenAlex abstracts were used as the abstract-recovery source and every quote is checked against the cached record by `tools/fetch-lit.py checkquotes` and `tools/verify-refs.py`.

## D. robots.txt decisions

| host | robots.txt HTTP | User-agent:* rules | cached copy |
|---|---|---|---|
| cwiki.apache.org | 404 | 0 | `data/raw/lit/_robots/cwiki.apache.org.txt` |
| kafka.apache.org | 200 | 0 | `data/raw/lit/_robots/kafka.apache.org.txt` |
| www.postgresql.org | 200 | 9 | `data/raw/lit/_robots/www.postgresql.org.txt` |
| raw.githubusercontent.com | 404 | 0 | `data/raw/lit/_robots/raw.githubusercontent.com.txt` |
| docs.stripe.com | 200 | 3 | `data/raw/lit/_robots/docs.stripe.com.txt` |
| debezium.io | 200 | 2 | `data/raw/lit/_robots/debezium.io.txt` |
| aws.amazon.com | 200 | 273 | `data/raw/lit/_robots/aws.amazon.com.txt` |
| www.infoq.com | 200 | 13 | `data/raw/lit/_robots/www.infoq.com.txt` |
| www.jmlr.org | 404 | 0 | `data/raw/lit/_robots/www.jmlr.org.txt` |
| cwiki.apache.org | 404 | 0 | `data/raw/lit/_robots/cwiki.apache.org.txt` |
| kafka.apache.org | 0 | 0 | `data/raw/lit/_robots/kafka.apache.org.txt` |
| www.postgresql.org | 200 | 9 | `data/raw/lit/_robots/www.postgresql.org.txt` |
| raw.githubusercontent.com | 0 | 0 | `data/raw/lit/_robots/raw.githubusercontent.com.txt` |
| docs.stripe.com | 200 | 3 | `data/raw/lit/_robots/docs.stripe.com.txt` |
| debezium.io | 200 | 2 | `data/raw/lit/_robots/debezium.io.txt` |
| aws.amazon.com | 200 | 273 | `data/raw/lit/_robots/aws.amazon.com.txt` |
| www.infoq.com | 200 | 13 | `data/raw/lit/_robots/www.infoq.com.txt` |
| www.jmlr.org | 404 | 0 | `data/raw/lit/_robots/www.jmlr.org.txt` |
| www.ebi.ac.uk | 200 | 170 | `data/raw/lit/_robots/www.ebi.ac.uk.txt` |
| dl.acm.org | 200 | 20 | `data/raw/lit/_robots/dl.acm.org.txt` |
| arxiv.org | 200 | 35 | `data/raw/lit/_robots/arxiv.org.txt` |
| link.springer.com | 200 | 0 | `data/raw/lit/_robots/link.springer.com.txt` |
| www.mdpi.com | 403 | 0 | `data/raw/lit/_robots/www.mdpi.com.txt` |
| www.sciencedirect.com | 403 | 0 | `data/raw/lit/_robots/www.sciencedirect.com.txt` |
| biblio.ugent.be | 200 | 4 | `data/raw/lit/_robots/biblio.ugent.be.txt` |
| orbilu.uni.lu | 0 | 0 | `data/raw/lit/_robots/orbilu.uni.lu.txt` |
| ir.cwi.nl | 200 | 4 | `data/raw/lit/_robots/ir.cwi.nl.txt` |
| projecteuclid.org | 200 | 0 | `data/raw/lit/_robots/projecteuclid.org.txt` |
| doi.org | 404 | 0 | `data/raw/lit/_robots/doi.org.txt` |
| www.mdpi.com | 403 | 0 | `data/raw/lit/_robots/www.mdpi.com.txt` |
| raw.githubusercontent.com | 0 | 0 | `data/raw/lit/_robots/raw.githubusercontent.com.txt` |
| queue.acm.org | 200 | 20 | `data/raw/lit/_robots/queue.acm.org.txt` |
| www.ebi.ac.uk | 200 | 170 | `data/raw/lit/_robots/www.ebi.ac.uk.txt` |
| www.microsoft.com | 200 | 201 | `data/raw/lit/_robots/www.microsoft.com.txt` |

## E. Failed or excluded sources (reasons)

| identifier | source attempted | status | reason / impact |
|---|---|---|---|
| KIP-447 (first URL) | https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+Scalability+for+Transactional+Producers | HTTP 404 | Wrong page title in URL; retried successfully at .../KIP-447%3A+Producer+scalability+for+exactly+once+semantics (HTTP 200, cached). No loss. |
| kafka.apache.org/documentation/ | Apache Kafka documentation | HTTP 200 but redirect shell (20 kB) | Current site is a client-side redirect; the real content was retrieved from the official docs markdown source in apache/kafka trunk (docs/design/design.md, docs/streams/developer-guide/config-streams.md). Cached. |
| kafka.apache.org/_print/documentation/ | Apache Kafka print documentation | transport failure (status 0) | Connection did not complete; superseded by the apache/kafka docs-markdown source (cached). |
| kafka producer configs doc (docs/configuration/producer_configs.md) | apache/kafka GitHub raw | HTTP 404 | Path does not exist in trunk; producer semantics are covered by KIP-98 + docs/design/design.md, and idempotence is documented there. Entry dropped from the base rather than cited from an unverified path. |
| helland2012 (CACM) | ACM DL / DOI landing, then ACM Queue reprint https://queue.acm.org/detail.cfm?id=2187821 | ACM DL HTTP 403; ACM Queue skipped by robots.txt (Disallow) | Could not open any copy of 'Idempotence is not a medical condition' under robots.txt and paywall rules. Excluded (not cited). Idempotency is covered by eosmsg2001/eosmid2012/eoscorba1999, Stripe/AWS docs and the M3 unique-index argument. |
| hanley1983 (JAMA) | DOI landing + Europe PMC REST record | HTTP 403 (publisher); Europe PMC record has no abstractText | No accessible text for the rule-of-three paper. Excluded; zero-event bounds are instead supported by the peer-reviewed modified-rule-of-three paper (rule3) and Clopper-Pearson sources (jsta2014/brown2001). |
| friedman1937 | Taylor & Francis DOI landing | HTTP 403 | Original Friedman test paper paywalled and no OA copy was found; excluded. The Friedman+Nemenyi protocol is cited via Demsar (2006, gray per DOI rule) and Arcuri & Briand (2012, peer, DOI-verified). |
| clopper1934 | Oxford University Press DOI landing | HTTP 403 / no OA copy | Original Clopper-Pearson paper paywalled; excluded. The Clopper-Pearson method is cited via peer-reviewed descriptions (jsta2014 DOI-verified; brown2001 for Wald-interval failure). |
| fisher1922 | JSTOR DOI 10.2307/2340521 | HTTP 403; Europe PMC search returned 0 hits | Original Fisher exact-test paper unopened; excluded. Paired exact testing is supported by McNemar (1947, DOI-verified) and the methods references. |
| aries1992 | ACM DL DOI landing | HTTP 403; OpenAlex abstract truncated mid-sentence | No quote could be anchored in a verifiable passage; excluded. Recovery/immutability foundations are covered by lsfs1992, fdb2021 and crashmonkey2019. |
| cpa2024 (Critical Perspectives on Accounting) | Elsevier DOI landing | HTTP 403; no OpenAlex abstract | Excluded. Double-entry framing is covered by triple2019 and triple2021. |
| expose2021 (Springer LNCS) | Springer DOI landing | HTTP 200 but JS-only shell (3 kB); no OpenAlex abstract | No readable text obtained; excluded. Benchmarking methodology covered by theodolite2021, hoefler2015, raasveldt2018. |
| repro2024 (Computer Science Review) | Elsevier DOI landing / OA PDF | HTTP 403; no OpenAlex abstract | Excluded. Reproducibility covered by collberg2016 and crashmonkey2019. |
| tene-latency (Gil Tene, 'How NOT to Measure Latency') | InfoQ presentation page | HTTP 200 but page contains only talk metadata/bio; no transcript text | Could not quote a talk that has no textual artifact; excluded. Coordinated-omission mitigation is cited via the HdrHistogram documentation (gray, primary artifact), which implements expectedIntervalBetweenValueSamples. |
| cp2015 (The American Statistician, Easterling) | OpenAlex abstract | abstract is only a comment header | No usable sentence; excluded in favour of jsta2014 for the Clopper-Pearson definition. |
| shortcp2009 / gencp2006 | OpenAlex abstracts | abstracts do not state the Clopper-Pearson construction cleanly | Suppressed candidates; jsta2014 selected instead. |
| spanner/TOCS 2013 | Crossref | DOI guess 404; Crossref query returns a SYSTOR abstract, not the TOCS article | Not carried forward; distributed-transaction coverage rests on txncommit2006, fdb2021, bailis2014. |
| n/a — global | Semantic Scholar Graph API | HTTP 200 but abstract field empty for all 57 DOI lookups | OpenAlex used as abstract-recovery source; every quote verified against cached evidence before inclusion. |

## F. Screening exclusions after reading the retrieved source

| key | source found | exclusion reason |
|---|---|---|
| `pipeflow2015` | OpenAlex abstract (PEER, DEBS 2015) | After opening: paper is about a declarative multi-engine stream language, not transactional exactly-once; content mismatch with the topic, excluded. |
| `reliablemsg2015` | OpenAlex abstract (PEER, DEBS 2015) | Producer-centric messaging architecture for big-data enterprises; no reliability/atomicity evaluation; peripheral, excluded. |
| `kafka-producer` | Apache docs (GRAY) | Canonical docs path not retrievable (404); KIP-98 and design.md already provide the producer-side semantics. Excluded to avoid citing an unverified path. |

## G. Discovery query log

| file | queries | records |
|---|---|---|
| `lit/_discovery/_queries.txt` | 49 | 294 |
| `lit/_discovery/_queries2.txt` | 20 | 100 |
| `lit/_discovery/_queries3.txt` | 12 | 60 |
| `lit/_discovery/_queries4.txt` | 2 | 16 |
