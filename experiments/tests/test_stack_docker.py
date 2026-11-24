"""Container/stack portability and injector-fix regression tests (Linux campaign substrate).

These tests are host-independent: they validate the committed deployment description, the stack
mode selection and the two injector defects found during the Windows pilot (M16) without starting
any service.
"""

from __future__ import annotations

import json
import os

import pytest
import yaml

from experiments import runner
from experiments import stack


def test_compose_defines_the_required_services_and_ports():
    path = os.path.join(stack.REPO, "deploy", "docker", "docker-compose.yml")
    assert os.path.exists(path)
    doc = yaml.safe_load(open(path, encoding="utf-8"))
    services = doc["services"]
    assert {"kafka-1", "kafka-2", "kafka-3", "postgres", "toxiproxy"} <= set(services)
    for name in ("kafka-1", "kafka-2", "kafka-3"):
        ports = services[name]["ports"]
        assert any("909" in p for p in ports)
        env = services[name]["environment"]
        assert env["KAFKA_PROCESS_ROLES"] == "broker,controller"
        assert env["KAFKA_MIN_INSYNC_REPLICAS"] == "2"
        assert env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] == "3"
        assert env["KAFKA_DELETE_TOPIC_ENABLE"] == "false"
        # disk-accounting parity with the Windows run (A3/M9)
        assert env["KAFKA_LOG_INDEX_SIZE_MAX_BYTES"] == "1048576"
    assert services["postgres"]["ports"] == ["5433:5432"]
    toxi_ports = set(services["toxiproxy"]["ports"])
    assert {"8474:8474", "19092:19092", "19093:19093", "19094:19094", "15433:15433"} <= toxi_ports


def test_toxiproxy_config_covers_the_four_proxies_used_by_the_fault_endpoints():
    path = os.path.join(stack.REPO, "deploy", "docker", "toxiproxy.json")
    proxies = json.load(open(path, encoding="utf-8"))["proxies"]
    by_name = {p["name"]: p for p in proxies}
    assert set(by_name) == {"kafka", "kafka-produce", "kafka-consume", "db"}
    assert by_name["kafka"]["listen"].endswith("19092")
    assert by_name["kafka-produce"]["listen"].endswith("19093")
    assert by_name["kafka-consume"]["listen"].endswith("19094")
    assert by_name["db"]["listen"].endswith("15433")


def test_stack_mode_env_override(monkeypatch):
    monkeypatch.setenv("WXEO_STACK_MODE", "docker")
    assert stack.stack_mode() == "docker"
    assert stack.is_docker() is True
    monkeypatch.setenv("WXEO_STACK_MODE", "native")
    assert stack.stack_mode() == "native"
    assert stack.is_docker() is False
    monkeypatch.setenv("WXEO_STACK_MODE", "bogus")
    # explicit but invalid values fall back to host inference, never to an unchecked mode
    assert stack.stack_mode() in ("docker", "native")


def test_all_external_fault_classes_are_marker_driven():
    # M16 defect: F1/F3/F5 fired after the ~1 s 1k/max load and were excluded; all external classes
    # (S4-owned) must get the fixed-rate window when rates.json is absent.
    external = {"F1", "F2", "F3", "F4a", "F5", "F6", "K2", "K5"}
    assert external <= set(runner.LiveSequence.MARKER_DRIVEN_FAULTS)
    # app-owned classes must NOT be marker driven (they own their in-process trigger)
    assert not ({"K1", "K3", "K4", "K7", "F4b"} & set(runner.LiveSequence.MARKER_DRIVEN_FAULTS))


def test_f4b_failpoint_threshold_scales_with_effect_transactions():
    # The app failpoint counts effect transactions, not events.  With 1k events / 500-record
    # batches / C=2 there is exactly one effect transaction per instance, so the threshold MUST be
    # 1 (a seeded 50-150 could never fire: the observed 300 s F4b timeout, M16).  Larger loads
    # scale to enough transactions for a seeded mid-stream trigger.
    assert runner.expected_effect_transactions(1_000, 2) == 1
    assert runner.expected_effect_transactions(10_000, 2) == 10
    assert runner.expected_effect_transactions(100_000, 2) == 100
    assert runner.expected_effect_transactions(100_000, 1) == 200
    assert runner.expected_effect_transactions(500, 2) == 1
