"""Linux/docker injector tests with a fake STACK + fake Toxiproxy (no network)."""
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments import injectors as INJ  # noqa: E402


class FakeToxiproxy:
    def __init__(self, fail_on=()):
        self.toxics_by_proxy = {}
        self.enabled = {}
        self.fail_on = set(fail_on)
        self.removed = []

    def add_toxic(self, proxy, toxic):
        if proxy in self.fail_on:
            raise RuntimeError(f"fake toxic failure on {proxy}")
        self.toxics_by_proxy.setdefault(proxy, []).append(toxic["name"])
        return {"ok": True}

    def remove_toxic(self, proxy, name):
        self.removed.append((proxy, name))
        if name in self.toxics_by_proxy.get(proxy, []):
            self.toxics_by_proxy[proxy].remove(name)

    def toxic_present(self, proxy, name):
        return name in self.toxics_by_proxy.get(proxy, [])

    def toxics(self, proxy):
        return [{"name": n} for n in self.toxics_by_proxy.get(proxy, [])]

    def set_enabled(self, proxy, enabled):
        if proxy in self.fail_on:
            raise RuntimeError(f"fake enable failure on {proxy}")
        self.enabled[proxy] = enabled

    def is_enabled(self, proxy):
        return self.enabled.get(proxy, True)

    def wait_healthy(self, proxy, timeout_s=30):
        return True


class FakeStack:
    def __init__(self):
        self.calls = []

    def is_docker(self):
        return True

    def proxy_groups(self):
        return {"kafka-produce": ["kafka-1", "kafka-2", "kafka-3"],
                "kafka-consume": ["kafka-c1", "kafka-c2"],
                "db": ["db"]}

    def broker_kill(self, broker_id):
        self.calls.append(("broker_kill", broker_id))
        return {"killed": broker_id}

    def broker_start(self, broker_id):
        self.calls.append(("broker_start", broker_id))
        return {"started": broker_id}

    def pid_kill_linux(self, pid):
        self.calls.append(("pid_kill_linux", pid))
        return {"killed": pid}


def _ctx(monkeypatch, fake_stack=None, **extra):
    monkeypatch.setattr(INJ, "_stack_module", lambda: fake_stack or FakeStack())
    return INJ.InjectorContext(
        run_id="rid", mechanism="M0", ordering_variant="strict", fault="F1",
        fault_level="L2", injector_mode="live", seed_hex="0102030405060708",
        scratch_dir=REPO, faults_log=os.path.join(REPO, "results", "faults.log"),
        bootstrap="127.0.0.1:9091",
        broker_count_probe=lambda: 2,
        extra={"load_events": 1000, **extra})


def test_proxy_members_falls_back_to_legacy_single_proxy(monkeypatch):
    ctx = INJ.InjectorContext(run_id="r", mechanism="M0", ordering_variant="strict",
                              fault="F3", fault_level="L2", injector_mode="live",
                              seed_hex="00", scratch_dir=REPO,
                              bootstrap="127.0.0.1:9091",
                              faults_log=os.path.join(REPO, "x.log"))
    monkeypatch.setattr(INJ, "_stack_module", lambda: None)
    assert INJ._proxy_members("kafka-produce", "kafka-produce", ctx) == ["kafka-produce"]


def test_f3_group_toxic_applies_to_all_and_records_per_proxy(monkeypatch):
    fake = FakeToxiproxy()
    monkeypatch.setattr(INJ, "Toxiproxy", lambda api: fake)
    ctx = _ctx(monkeypatch, extra={"produce_proxy": "legacy"})
    inj = INJ.F3ProduceTimeout(ctx)
    inj.arm()
    assert inj.state["proxies"] == ["kafka-1", "kafka-2", "kafka-3"]
    out = inj.fire()
    assert fake.toxics_by_proxy["kafka-3"]
    assert set(ctx.extra["toxic_group_results"]) == {"kafka-1", "kafka-2", "kafka-3"}
    verified, detail = inj._verify({}, {})
    assert verified is True
    assert all(detail["per_proxy_toxic_present"].values())
    inj._restore()
    assert len(fake.removed) == 3


def test_f3_group_toxic_fails_closed_on_member_failure(monkeypatch):
    fake = FakeToxiproxy(fail_on={"kafka-2"})
    monkeypatch.setattr(INJ, "Toxiproxy", lambda api: fake)
    ctx = _ctx(monkeypatch, extra={"produce_proxy": "legacy"})
    inj = INJ.F3ProduceTimeout(ctx)
    inj.arm()
    with pytest.raises(INJ.InjectionNotVerified):
        inj.fire()
    assert ctx.extra["toxic_group_results"]["kafka-2"]["ok"] is False


def test_f1_docker_uses_stack_broker_kill_and_start(monkeypatch):
    fake_stack = FakeStack()
    ctx = _ctx(monkeypatch, fake_stack, extra={"broker_id": 2})
    state = {"brokers": 3}

    def probe():
        return state["brokers"]

    # model the substrate: kill drops a broker, start brings it back
    orig_kill, orig_start = fake_stack.broker_kill, fake_stack.broker_start

    def kill(broker_id):
        state["brokers"] = 2
        return orig_kill(broker_id)

    def start(broker_id):
        state["brokers"] = 3
        return orig_start(broker_id)

    monkeypatch.setattr(fake_stack, "broker_kill", kill)
    monkeypatch.setattr(fake_stack, "broker_start", start)
    ctx.broker_count_probe = probe
    inj = INJ.F1BrokerKill(ctx)
    inj.arm()
    inj.fire()
    inj._restore()
    assert ("broker_kill", 2) in fake_stack.calls
    assert ("broker_start", 2) in fake_stack.calls
    assert inj.state["mode"] == "docker"


def test_f2_posix_uses_pid_kill_linux(monkeypatch):
    fake_stack = FakeStack()
    monkeypatch.setattr(INJ.os, "name", "posix")
    ctx = _ctx(monkeypatch, fake_stack)
    ctx.consumer_pids = {1: 4242}
    inj = INJ.F2ConsumerKill(ctx)
    inj.arm()
    inj.fire()
    assert ("pid_kill_linux", 4242) in fake_stack.calls


def test_k2_group_toxics_target_all_produce_proxies_for_m1kafka(monkeypatch):
    fake = FakeToxiproxy()
    monkeypatch.setattr(INJ, "Toxiproxy", lambda api: fake)
    ctx = _ctx(monkeypatch)
    ctx.mechanism = "M1-kafka"
    ctx.extra.update({"produce_proxy": "legacy", "load_events": 1000,
                      "marker_timeout_s": 0.2})
    ctx.progress_probe = lambda: 10_000    # marker reached immediately
    inj = INJ.K2UnknownOutcome(ctx)
    inj.arm()
    assert inj.state["proxies"] == ["kafka-1", "kafka-2", "kafka-3"]
    inj.fire()
    assert set(fake.toxics_by_proxy) == {"kafka-1", "kafka-2", "kafka-3"}
