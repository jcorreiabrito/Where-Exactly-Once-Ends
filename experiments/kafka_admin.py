"""Kafka AdminClient wrapper for the runner's quiescence, residue and progress checks.

S5 finding F-G2: the old runner parsed ``kafka.admin.ConsumerGroupCommand --describe`` text and
could never establish quiescence (column layout mismatch) and had no sink definition for M1-kafka
(Kafka publication stream as ledger).  This module uses librdkafka's AdminClient directly
(``confluent-kafka``, installed in the S4 tooling environment) and provides:

* real per-partition consumer lag (``list_consumer_group_offsets`` + ``list_offsets``);
* topic/group existence for residue checks;
* committed-progress markers (phase targeting);
* a consumer-group quiescence predicate (lag = 0, monotone sink count stable for T_quiet);
* a publication-stream quiescence predicate for M1-kafka (read_committed LSO/high-watermark
  stable for T_quiet, i.e. no open transactions and no new committed records).
"""

from __future__ import annotations

import os
import time

DEFAULT_BOOTSTRAP = "127.0.0.1:9091"


def _admin(bootstrap: str):
    from confluent_kafka.admin import AdminClient  # lazy: only live paths need it

    return AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 10000})


def _consumer(bootstrap: str, group: str, isolation: str):
    from confluent_kafka import Consumer  # lazy

    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": isolation,
        "enable.partition.eof": False,
        "session.timeout.ms": 10000,
    })


class KafkaAdmin:
    """Thin wrapper around the librdkafka AdminClient used by the runner."""

    def __init__(self, bootstrap: str = DEFAULT_BOOTSTRAP):
        self.bootstrap = bootstrap
        self._client = _admin(bootstrap)

    # -- metadata ---------------------------------------------------------------------------
    def list_topics(self) -> set[str]:
        return set(self._client.list_topics(timeout=20).topics.keys())

    def topic_exists(self, topic: str) -> bool:
        return topic in self.list_topics()

    def partitions(self, topic: str) -> list[int]:
        md = self._client.list_topics(topic, timeout=20)
        return sorted(md.topics[topic].partitions.keys())

    def list_groups(self) -> set[str]:
        from confluent_kafka.admin import ConsumerGroupListing  # noqa: F401

        result = self._client.list_consumer_groups(request_timeout=20.0).result()
        return {g.group_id for g in result.valid}

    def group_exists(self, group: str) -> bool:
        return group in self.list_groups()

    # -- offsets ------------------------------------------------------------------------------
    def topic_offsets(self, topic: str) -> dict[int, int]:
        """High watermark (log end offset) per partition."""
        from confluent_kafka import TopicPartition

        tps = [TopicPartition(topic, p) for p in self.partitions(topic)]
        futures = self._client.list_offsets(tps, request_timeout=20.0,
                                            isolation_level="read_committed")
        out: dict[int, int] = {}
        for tp, future in futures.items():
            result = future.result()
            out[tp.partition] = int(result.high)
        return out

    def group_offsets(self, group: str, topic: str) -> dict[int, int]:
        from confluent_kafka.admin import ConsumerGroupTopicPartitions

        request = ConsumerGroupTopicPartitions(group, [])
        futures = self._client.list_consumer_group_offsets([request], request_timeout=20.0)
        result = futures[group].result()
        out: dict[int, int] = {}
        for tp in result.topic_partitions or []:
            if tp.topic == topic and tp.offset >= 0:
                out[tp.partition] = int(tp.offset)
        return out

    def lag(self, group: str, topic: str) -> dict[int, int]:
        """Per-partition lag = high watermark - committed offset (missing commit => full lag)."""
        high = self.topic_offsets(topic)
        committed = self.group_offsets(group, topic)
        return {p: int(high[p] - committed.get(p, 0)) for p in sorted(high)}

    def progress_sum(self, group: str, topic: str) -> int:
        """Sum of committed offsets: a monotone progress marker for phase targeting."""
        return int(sum(self.group_offsets(group, topic).values()))

    def stream_record_count(self, topic: str) -> int:
        """Sum of high watermarks: monotone record count of the publication/sink stream."""
        return int(sum(self.topic_offsets(topic).values()))

    # -- topic lifecycle ----------------------------------------------------------------------
    def create_topic(self, topic: str, partitions: int, replication_factor: int = 3,
                     configs: dict | None = None) -> None:
        from confluent_kafka.admin import NewTopic

        topic_cfg = {"min.insync.replicas": "2", "cleanup.policy": "delete",
                     "retention.ms": str(24 * 3600 * 1000)}
        if configs:
            topic_cfg.update(configs)
        futures = self._client.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor,
                      config=topic_cfg)], request_timeout=60.0)
        future = futures[topic]
        try:
            future.result()
        except Exception as exc:  # noqa: BLE001 - "topic already exists" is tolerated
            if "already exists" not in str(exc).lower():
                raise

    def delete_topic(self, topic: str) -> None:
        futures = self._client.delete_topics([topic], request_timeout=60.0)
        future = futures.get(topic)
        if future is None:
            return
        try:
            future.result()
        except Exception as exc:  # noqa: BLE001 - "unknown topic" is tolerated
            if "unknown topic" not in str(exc).lower() and "does not exist" not in str(exc).lower():
                raise

    def wait_topic_absent(self, topic: str, timeout: float = 120.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.topic_exists(topic):
                return True
            time.sleep(1.0)
        return False

    def wait_topic_present(self, topic: str, partitions: int, timeout: float = 120.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if self.topic_exists(topic) and len(self.partitions(topic)) == partitions:
                    return True
            except Exception:  # noqa: BLE001
                pass
            time.sleep(1.0)
        return False

    def wait_group_stable(self, group: str, topic: str, timeout: float = 120.0) -> bool:
        """Group exists and every partition has a committed offset (assignment happened)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if self.group_exists(group):
                    committed = self.group_offsets(group, topic)
                    if committed and len(committed) >= len(self.partitions(topic)):
                        return True
            except Exception:  # noqa: BLE001
                pass
            time.sleep(1.0)
        return False

    def close(self) -> None:
        return None


def wait_consumer_quiescence(
    admin: KafkaAdmin,
    *,
    group: str,
    source_topic: str,
    quiet_period_ms: int = 10_000,
    timeout_ms: int = 300_000,
    sink_count_fn=None,
    pg_idle_fn=None,
    relay_drained_fn=None,
    poll_ms: int = 1_000,
) -> dict:
    """method §7.3 quiescence: lag=0, sink count stable for T_quiet, app idle, relay drained.

    ``sink_count_fn()`` must return a monotone integer (DB row count for DB sinks, stream record
    count for M1-kafka); ``pg_idle_fn()`` returns True when no application transaction is active;
    ``relay_drained_fn()`` returns True when the mechanism's pending retry/relay is empty.
    A run that exceeds ``timeout_ms`` is **censored** (an outcome, never a clean pass).
    """
    started = int(time.time() * 1000)
    stable_since: int | None = None
    last_count = None
    last_lag: dict[int, int] = {}
    checks = {"lag_zero": False, "sink_stable": False, "app_idle": False,
              "relay_drained": False}
    while int(time.time() * 1000) - started <= timeout_ms:
        lag = admin.lag(group, source_topic) if admin.topic_exists(group and source_topic) else {}
        lag_zero = bool(lag) and all(v == 0 for v in lag.values())
        count = sink_count_fn() if sink_count_fn else None
        app_idle = bool(pg_idle_fn()) if pg_idle_fn else True
        relay_drained = bool(relay_drained_fn()) if relay_drained_fn else True
        checks = {"lag_zero": lag_zero, "sink_stable": False, "app_idle": app_idle,
                  "relay_drained": relay_drained}
        if lag_zero and app_idle and relay_drained and count is not None:
            if last_count is not None and count == last_count:
                if stable_since is None:
                    stable_since = int(time.time() * 1000)
                if int(time.time() * 1000) - stable_since >= quiet_period_ms:
                    checks["sink_stable"] = True
                    return {
                        "censored": False,
                        "lag_per_partition": lag,
                        "quiet_period_ms": quiet_period_ms,
                        "t_consistent_ms": stable_since - started,
                        "checks": checks,
                        "sink_count": count,
                    }
            else:
                stable_since = None
        else:
            stable_since = None
        last_count = count
        last_lag = lag
        time.sleep(poll_ms / 1000.0)
    return {
        "censored": True,
        "lag_per_partition": last_lag,
        "quiet_period_ms": quiet_period_ms,
        "t_consistent_ms": None,
        "checks": checks,
        "timeout_ms": timeout_ms,
        "note": "quiescence not established within T_max: censored (method §7.3)",
    }


def wait_publication_quiescence(
    *,
    bootstrap: str,
    sink_topic: str,
    quiet_period_ms: int = 10_000,
    timeout_ms: int = 300_000,
    poll_ms: int = 1_000,
) -> dict:
    """M1-kafka ledger quiescence on the publication stream (S5 F-G2).

    The publication stream *is* the ledger for M1-kafka (method §2.7), so the sink-count stability
    check is its committed record count.  A read-committed consumer's high watermark is the last
    stable offset (LSO): while a transaction is open, the LSO does not advance, so a stable
    ``read_committed`` high watermark for T_quiet means no transaction is in flight and no new
    committed records are arriving.
    """
    from confluent_kafka import Consumer, TopicPartition

    consumer = _consumer(bootstrap, group=f"wxeo.quiesce.{os.getpid()}.{int(time.time())}",
                         isolation="read_committed")
    started = int(time.time() * 1000)
    stable_since: int | None = None
    last_total = None
    last_hw: dict[int, int] = {}
    try:
        partitions = sorted(consumer.list_topics(sink_topic, timeout=30).topics[sink_topic]
                            .partitions.keys())
        tps = [TopicPartition(sink_topic, p) for p in partitions]
        while int(time.time() * 1000) - started <= timeout_ms:
            high: dict[int, int] = {}
            for tp in tps:
                try:
                    _low, h = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
                    high[tp.partition] = int(h)
                except Exception:  # noqa: BLE001
                    high[tp.partition] = -1
            last_hw = high
            total = sum(v for v in high.values() if v >= 0)
            if last_total is not None and total == last_total and total >= 0:
                if stable_since is None:
                    stable_since = int(time.time() * 1000)
                if int(time.time() * 1000) - stable_since >= quiet_period_ms:
                    return {
                        "censored": False,
                        "lag_per_partition": {str(k): 0 for k in sorted(high)},
                        "quiet_period_ms": quiet_period_ms,
                        "t_consistent_ms": stable_since - started,
                        "sink_count": total,
                        "checks": {"read_committed_lso_stable": True},
                    }
            else:
                stable_since = None
            last_total = total
            time.sleep(poll_ms / 1000.0)
        return {
            "censored": True,
            "lag_per_partition": {str(k): -1 for k in sorted(last_hw)},
            "quiet_period_ms": quiet_period_ms,
            "t_consistent_ms": None,
            "timeout_ms": timeout_ms,
            "note": "publication-stream quiescence not established within T_max: censored",
        }
    finally:
        consumer.close()


__all__ = [
    "KafkaAdmin",
    "wait_consumer_quiescence",
    "wait_publication_quiescence",
]
