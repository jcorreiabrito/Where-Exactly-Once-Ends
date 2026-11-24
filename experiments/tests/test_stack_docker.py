"""Container/stack portability + fault-routing regression tests (Linux campaign substrate).

Host-independent: these tests validate the committed deployment description (compose listeners,
loopback bindings, Toxiproxy proxies/groups, container names), the stack adapter's pure fault-routing
API, and the M16 injector fixes -- without starting any service.
"""

from __future__ import annotations

import json
import os

import pytest
import yaml

from experiments import runner
from experiments import stack


def _compose():
    path = os.path.join(stack.REPO, "deploy", "docker", "docker-compose.yml")
    assert os.path.exists(path)
    return yaml.safe_load(open(path, encoding="utf-8"))


def _toxiproxy():
    path = os.path.join(stack.REPO, "deploy", "docker", "toxiproxy.json")
    proxies = json.load(open(path, encoding="utf-8"))
    assert isinstance(proxies, list), "toxiproxy-server -config expects a JSON array"
    return {p["name"]: p for p in proxies}


def test_compose_defines_the_required_services_and_pinned_container_names():
    doc = _compose()
    services = doc["services"]
    assert {"kafka-1", "kafka-2", "kafka-3", "postgres", "toxiproxy"} <= set(services)
    assert services["kafka-1"]["container_name"] == "wxeo-campaign-kafka-1"
    assert services["kafka-2"]["container_name"] == "wxeo-campaign-kafka-2"
    assert services["kafka-3"]["container_name"] == "wxeo-campaign-kafka-3"
    assert services["postgres"]["container_name"] == "wxeo-campaign-postgres"
    assert services["toxiproxy"]["container_name"] == "wxeo-campaign-toxiproxy"


def test_compose_loopback_binding_is_universal():
    # Red-team L6: A4/runbook claim "only reached on 127.0.0.1"; every published port must say so.
    doc = _compose()
    published = []
    for service in doc["services"].values():
        published.extend(service.get("ports") or [])
    assert published, "compose must publish the frozen contract ports"
    bad = [p for p in published if not str(p).startswith("127.0.0.1:")]
    assert bad == [], "every published port must bind 127.0.0.1 (found %r)" % (bad,)


def test_kafka_listeners_route_every_client_connection_through_a_proxy():
    # External listener per broker (29091/29092/29093), advertised as the three Toxiproxy host ports;
    # a host client that bootstraps anywhere still receives proxy addresses after metadata.
    doc = _compose()
    expected = {1: (29091, 19091), 2: (29092, 19092), 3: (29093, 19093)}
    for node_id, (listen_port, advertised_port) in expected.items():
        env = doc["services"]["kafka-%d" % node_id]["environment"]
        listeners = env["KAFKA_LISTENERS"]
        advertised = env["KAFKA_ADVERTISED_LISTENERS"]
        assert "EXTERNAL://0.0.0.0:%d" % listen_port in listeners
        assert "EXTERNAL://127.0.0.1:%d" % advertised_port in advertised
        # INTERNAL listener stays broker-to-broker only (distinct port; EXTERNAL 29092 would collide
        # with the old INTERNAL 29092 inside broker-2's network namespace).
        assert "INTERNAL://0.0.0.0:29095" in listeners
        assert "INTERNAL://kafka-%d:29095" % node_id in advertised
        assert env["KAFKA_INTER_BROKER_LISTENER_NAME"] == "INTERNAL"
        assert env["KAFKA_CONTROLLER_QUORUM_VOTERS"] == (
            "1@kafka-1:9192,2@kafka-2:9193,3@kafka-3:9194")
        # direct compatibility/admin port published on loopback only
        assert "127.0.0.1:909%d:2909%d" % (node_id, node_id) in doc["services"]["kafka-%d" % node_id]["ports"]


def test_toxiproxy_proxies_and_contract_compat_bootstrap_alias():
    proxies = _toxiproxy()
    assert set(proxies) == {"kafka-1", "kafka-2", "kafka-3", "kafka-bootstrap", "db"}
    for node_id in (1, 2, 3):
        name = "kafka-%d" % node_id
        assert proxies[name]["listen"] == "0.0.0.0:1909%d" % node_id
        assert proxies[name]["upstream"] == "kafka-%d:2909%d" % (node_id, node_id)
        assert proxies[name]["enabled"] is True
    # Frozen runner consumer-bootstrap endpoint 127.0.0.1:19094 must stay usable; it is a bootstrap
    # alias only (never in a toxic group, since traffic follows the advertised kafka-N proxies).
    assert proxies["kafka-bootstrap"]["listen"] == "0.0.0.0:19094"
    assert proxies["kafka-bootstrap"]["upstream"] == "kafka-1:29091"
    assert proxies["db"]["listen"] == "0.0.0.0:15433"
    assert proxies["db"]["upstream"] == "postgres:5432"
    assert all(p["enabled"] is True for p in proxies.values())


def test_toxiproxy_service_has_a_healthcheck_and_loopback_ports():
    doc = _compose()
    toxi = doc["services"]["toxiproxy"]
    assert "healthcheck" in toxi
    ports = set(toxi["ports"])
    assert {"127.0.0.1:8474:8474", "127.0.0.1:19091:19091", "127.0.0.1:19092:19092",
            "127.0.0.1:19093:19093", "127.0.0.1:19094:19094",
            "127.0.0.1:15433:15433"} <= ports


def test_proxy_groups_cover_the_logical_fault_vocabulary():
    path = os.path.join(stack.REPO, "deploy", "docker", "proxy-groups.json")
    assert os.path.exists(path)
    doc = json.load(open(path, encoding="utf-8"))
    groups = doc["groups"]
    kafka = ["kafka-1", "kafka-2", "kafka-3"]
    assert groups["kafka"] == kafka
    assert groups["kafka-produce"] == kafka
    assert groups["kafka-consume"] == kafka
    assert groups["db"] == ["db"]
    proxies = _toxiproxy()
    for names in groups.values():
        for name in names:
            assert name in proxies, "proxy-group name %r is not a configured proxy" % (name,)


def test_stack_container_names_and_broker_instances_are_deterministic():
    assert stack.container_name("kafka-1") == "wxeo-campaign-kafka-1"
    assert stack.container_name("postgres") == "wxeo-campaign-postgres"
    assert stack.container_name("toxiproxy") == "wxeo-campaign-toxiproxy"
    assert stack.broker_container_name(1) == "wxeo-campaign-kafka-1"
    assert stack.broker_container_name(3) == "wxeo-campaign-kafka-3"
    assert stack.broker_container_names() == [
        "wxeo-campaign-kafka-1", "wxeo-campaign-kafka-2", "wxeo-campaign-kafka-3"]
    for bad in (0, 4, -1, True, "1", None):
        with pytest.raises(ValueError):
            stack.broker_container_name(bad)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        stack.container_name("not-a-service")


def test_stack_proxy_groups_and_host_ports_match_the_deployment():
    groups = stack.proxy_groups()
    assert groups["kafka"] == ["kafka-1", "kafka-2", "kafka-3"]
    assert groups["db"] == ["db"]
    assert stack.proxy_host_port("kafka-1") == 19091
    assert stack.proxy_host_port("kafka-2") == 19092
    assert stack.proxy_host_port("kafka-3") == 19093
    assert stack.proxy_host_port("kafka-bootstrap") == 19094
    assert stack.proxy_host_port("db") == 15433
    with pytest.raises(KeyError):
        stack.proxy_host_port("does-not-exist")


def test_stack_fault_helpers_fail_closed_on_bad_input():
    # Pure validation paths: no docker/signal is reached for invalid arguments.
    for bad in (True, 0, -3, "2", None):
        with pytest.raises(ValueError):
            stack.pid_kill_linux(bad)  # type: ignore[arg-type]
    # A well-formed pid on a non-POSIX host must fail closed (never silently "succeed"),
    # while on the Linux campaign host it reaches the SIGKILL path (tested live by
    # deploy/linux/fault-efficacy.py).
    if os.name != "posix":
        with pytest.raises(RuntimeError):
            stack.pid_kill_linux(99)


def test_stack_mode_env_override(monkeypatch):
    monkeypatch.setenv("WXEO_STACK_MODE", "docker")
    assert stack.stack_mode() == "docker"
    assert stack.is_docker() is True
    monkeypatch.setenv("WXEO_STACK_MODE", "native")
    assert stack.stack_mode() == "native"
    assert stack.is_docker() is False
    monkeypatch.setenv("WXEO_STACK_MODE", "bogus")
    assert stack.stack_mode() in ("docker", "native")


def test_external_fault_classes_are_marker_driven():
    # M16 defect: F1/F3/F5 fired after the ~1 s 1k/max load and were excluded; all external classes
    # (S4-owned) must get the fixed-rate window when rates.json is absent.
    external = {"F1", "F2", "F3", "F4a", "F5", "F6", "K2", "K5"}
    assert external <= set(runner.LiveSequence.MARKER_DRIVEN_FAULTS)
    assert not ({"K1", "K3", "K4", "K7", "F4b"} & set(runner.LiveSequence.MARKER_DRIVEN_FAULTS))


def test_f4b_failpoint_threshold_scales_with_effect_transactions():
    assert runner.expected_effect_transactions(1_000, 2) == 1
    assert runner.expected_effect_transactions(10_000, 2) == 10
    assert runner.expected_effect_transactions(100_000, 2) == 100
    assert runner.expected_effect_transactions(100_000, 1) == 200
    assert runner.expected_effect_transactions(500, 2) == 1
