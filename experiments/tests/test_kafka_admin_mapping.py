"""KafkaAdmin endpoint mapping test (mocked AdminClient, no live broker)."""
from __future__ import annotations
import os, sys, types
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)
from experiments import kafka_admin as KA


class _Fut:
    def __init__(self, high): self._high = high
    def result(self): return types.SimpleNamespace(offset=self._high)


class _FakeAdmin:
    def __init__(self): self.calls = []
    def list_topics(self, topic=None, timeout=20):
        return types.SimpleNamespace(topics={topic: types.SimpleNamespace(
            partitions={0: object(), 1: object(), 2: object()})})
    def list_offsets(self, topic_partition_offsets, request_timeout=None,
                     isolation_level=None):
        assert isinstance(topic_partition_offsets, dict), type(topic_partition_offsets)
        self.calls.append(topic_partition_offsets)
        return {tp: _Fut(10 + tp.partition) for tp in topic_partition_offsets}


def test_topic_offsets_uses_dict_of_offsetspecs():
    admin = KA.KafkaAdmin.__new__(KA.KafkaAdmin)
    admin.bootstrap = "fake:9092"
    admin._client = _FakeAdmin()
    admin.partitions = lambda topic: [0, 1, 2]
    assert admin.topic_offsets("wxeo.src.x") == {0: 10, 1: 11, 2: 12}
    from confluent_kafka.admin import OffsetSpec
    queries = admin._client.calls[0]
    assert len(queries) == 3 and all(isinstance(v, OffsetSpec) for v in queries.values())
