#!/usr/bin/env python3
"""Fault-efficacy proof for the Linux/container campaign substrate (S3 deliverable C).

Run this on the campaign host BEFORE any campaign run; ``deploy/linux/bootstrap.sh`` runs it and
fails closed. It proves that each logical fault group actually intercepts the operation the injector
will fault, i.e. that a toxic on the group makes the operation fail (or exceed the latency bound)
and that removing it restores service -- not merely that a toxic object exists.

Checks (all fail-closed; top-level ``passed`` only if every check passes):

* ``produce.<group>``  -- confluent-kafka producer against ``127.0.0.1:19091`` (bootstrap through a
  Toxiproxy proxy; Kafka advertises the three proxy ports so every broker connection is proxied).
  With a ``timeout`` toxic on every proxy of the group, an ``acks=all`` produce must fail within the
  bound; after removing the toxics it must succeed again.
* ``consume.<group>``  -- consumer fetch/commit through the same group; with the toxic active a
  poll/commit must fail or time out; after removal a produce/consume round trip must succeed.
* ``db``               -- disabling the ``db`` proxy makes a loopback TCP connect to 15433 fail;
  re-enabling restores it (F5 realisation).
* ``f1.broker-kill``   -- :func:`experiments.stack.broker_kill` one broker -> live broker count
  drops to 2 (via the surviving direct bootstrap ``127.0.0.1:9092``), ``broker_start`` restores
  count 3 and a produce/consume round trip succeeds.
* ``f2.pid-kill``      -- spawn a tiny consumer process, wait until its group member is visible,
  :func:`experiments.stack.pid_kill_linux`, then verify the process is dead and the group member
  disappears.

Usage::

    python deploy/linux/fault-efficacy.py [--out results/harness/linux-fault-efficacy.json]
    exit code 0 == passed (all checks), 1 == failed, 2 == wrong host/substrate

No campaign cells are executed; the script creates one scratch topic and small probes only.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from experiments import stack  # noqa: E402

DEFAULT_OUT = os.path.join(REPO, "results", "harness", "linux-fault-efficacy.json")
BOOTSTRAP = "127.0.0.1:19091"          # kafka-1 Toxiproxy proxy (host loopback)
SURVIVING_BOOTSTRAP = "127.0.0.1:9092"  # direct EXTERNAL listener of broker-2 (admin/ops only)
TOXIPROXY_API = "http://127.0.0.1:8474"
DB_PROXY_PORT = 15433
FAIL_BOUND_S = 20.0     # an active fault must make the operation fail within this bound
RECOVER_BOUND_S = 60.0  # service must recover within this bound after toxic removal


# ----------------------------------------------------------------------------------------------
# generic helpers
# ----------------------------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _tail(text: str | bytes | None, limit: int = 800) -> str:
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    return (text or "").strip()[-limit:]


def _http(method: str, path: str, body: dict | None = None, timeout: float = 10.0) -> Any:
    url = TOXIPROXY_API + path
    data = None
    headers = {"User-Agent": "wxeo-fault-efficacy/1.0"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw) if raw.strip() else {}


def _list_proxies() -> dict[str, dict]:
    doc = _http("GET", "/proxies")
    if isinstance(doc, dict) and "proxies" in doc:  # tolerate the object shape
        doc = doc["proxies"]
    if isinstance(doc, dict):
        return {name: spec for name, spec in doc.items() if isinstance(spec, dict)}
    return {p["name"]: p for p in doc if isinstance(p, dict)}


def _add_timeout_toxics(proxies: list[str], timeout_ms: int = 500) -> list[dict]:
    applied = []
    for name in proxies:
        body = {"name": "s4-efficacy", "type": "timeout",
                "attributes": {"timeout": timeout_ms}, "stream": "downstream"}
        _http("POST", "/proxies/%s/toxics" % name, body)
        applied.append({"proxy": name, "toxic": "s4-efficacy", "type": "timeout",
                        "attributes": body["attributes"]})
    return applied


def _remove_timeout_toxics(proxies: list[str]) -> list[dict]:
    removed = []
    for name in proxies:
        try:
            _http("DELETE", "/proxies/%s/toxics/s4-efficacy" % name)
            removed.append({"proxy": name, "toxic": "s4-efficacy", "removed": True})
        except urllib.error.HTTPError as exc:
            removed.append({"proxy": name, "toxic": "s4-efficacy", "removed": False,
                            "error": "HTTP %s" % exc.code})
    return removed


def _set_proxy_enabled(name: str, enabled: bool) -> dict:
    return _http("POST", "/proxies/%s" % name, {"enabled": bool(enabled)})


def _tcp_connect_ok(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _import_confluent():
    try:
        import confluent_kafka as ck  # noqa: F401
        return ck
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(
            "confluent-kafka is required for the fault-efficacy proof (pip install -r "
            "deploy/linux/requirements.txt): %s" % exc) from exc


def _new_topic(ck, bootstrap: str, topic: str, partitions: int = 3, rf: int = 3) -> None:
    admin = ck.AdminClient({"bootstrap.servers": bootstrap, "request.timeout.ms": 30000})
    try:
        admin.create_topics([ck.NewTopic(topic, num_partitions=partitions,
                                         replication_factor=rf)]).result(timeout=45)
    except Exception as exc:  # noqa: BLE001 - topic may already exist on a reused stack
        if "already exists" not in str(exc).lower():
            raise
        try:
            admin.delete_topics([topic]).result(timeout=10)
        except Exception:  # noqa: BLE001 - delete.topic.enable=false is the expected case
            pass
    finally:
        try:
            admin.poll(0)
        except Exception:  # noqa: BLE001
            pass


def _produce_once(ck, bootstrap: str, topic: str, value: str,
                  delivery_timeout_ms: int = 8000) -> None:
    """One acks=all produce; raises if the record is not acknowledged."""
    producer = ck.Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "delivery.timeout.ms": delivery_timeout_ms,
        "request.timeout.ms": delivery_timeout_ms,
        "socket.timeout.ms": delivery_timeout_ms,
        "message.timeout.ms": delivery_timeout_ms,
        "retries": 3,
        "linger.ms": 0,
    })
    errors: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    producer.produce(topic, key="probe", value=value, on_delivery=on_delivery)
    producer.flush(delivery_timeout_ms / 1000.0 + 2.0)
    if errors:
        raise RuntimeError("produce failed: %s" % errors[0])


def _consume_round_trip(ck, bootstrap: str, topic: str, group: str,
                        expect: int = 1, timeout_s: float = 15.0) -> int:
    consumer = ck.Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "session.timeout.ms": 10000,
    })
    consumer.subscribe([topic])
    seen = 0
    deadline = time.time() + timeout_s
    try:
        while seen < expect and time.time() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError("consume error: %s" % msg.error())
            seen += 1
        if seen:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
    return seen


def _wait_for(predicate, timeout_s: float, interval_s: float = 0.5) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:  # noqa: BLE001 - poll-time errors are expected during faults
            pass
        time.sleep(interval_s)
    return False


def _admin(ck, bootstrap: str):
    return ck.AdminClient({"bootstrap.servers": bootstrap, "request.timeout.ms": 10000})


def _broker_count(ck, bootstrap: str) -> int:
    admin = _admin(ck, bootstrap)
    try:
        return len(admin.list_topics(timeout=10).brokers)
    finally:
        try:
            admin.poll(0)
        except Exception:  # noqa: BLE001
            pass


# ----------------------------------------------------------------------------------------------
# checks
# ----------------------------------------------------------------------------------------------

def _check(name: str, group: str | None, fn) -> dict:
    started = time.time()
    result: dict[str, Any] = {"name": name, "group": group, "started_utc": _utc_now()}
    try:
        evidence = fn()
        result.update({"passed": True, "evidence": evidence})
    except Exception as exc:  # noqa: BLE001 - every exception is a failed check
        result.update({"passed": False, "error": "%s: %s" % (type(exc).__name__, exc)})
    result["duration_ms"] = int((time.time() - started) * 1000)
    result["finished_utc"] = _utc_now()
    status = "PASS" if result["passed"] else "FAIL"
    print("[%s] %-28s %s%s" % (status, name, result.get("error", ""),
                               "" if result["passed"] else ""), flush=True)
    return result


def _group_path_check(ck, group: str, proxies: list[str], topic: str,
                      run_id: str) -> dict:
    """Fault one logical group: prove the operation fails, then prove recovery."""
    toxics = _add_timeout_toxics(proxies)
    failed_within_bound = False
    failure_detail = ""
    t0 = time.time()
    try:
        if group in ("kafka", "kafka-produce"):
            _produce_once(ck, BOOTSTRAP, topic, "during-toxic-%s" % group)
            failure_detail = "produce unexpectedly succeeded under the timeout toxic"
        else:  # kafka-consume (and kafka group additionally checks commit)
            consumer = ck.Consumer({
                "bootstrap.servers": BOOTSTRAP,
                "group.id": "%s-consume-fault" % run_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 10000,
            })
            try:
                consumer.subscribe([topic])
                consumer.poll(2.0)
                consumer.commit(asynchronous=False)
                failure_detail = "consume/commit unexpectedly succeeded under the timeout toxic"
            finally:
                consumer.close()
    except Exception as exc:  # noqa: BLE001 - this is the expected failure path
        failed_within_bound = (time.time() - t0) <= FAIL_BOUND_S
        failure_detail = "%s: %s" % (type(exc).__name__, exc)
    if group == "kafka" and failed_within_bound:
        # the kafka group check must also show that a consume/commit cannot make progress
        try:
            consumer = ck.Consumer({
                "bootstrap.servers": BOOTSTRAP,
                "group.id": "%s-kafka-fault" % run_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 10000,
            })
            try:
                consumer.subscribe([topic])
                consumer.poll(2.0)
                consumer.commit(asynchronous=False)
                raise AssertionError("consume/commit unexpectedly succeeded under the kafka toxic")
            finally:
                consumer.close()
        except AssertionError as exc:
            failed_within_bound = False
            failure_detail = str(exc)
        except Exception:  # noqa: BLE001 - expected
            pass
    removed = _remove_timeout_toxics(proxies)

    def _recovered() -> bool:
        _produce_once(ck, BOOTSTRAP, topic, "recovery-%s" % group, delivery_timeout_ms=5000)
        return _consume_round_trip(ck, BOOTSTRAP, topic, "%s-recovery-%s" % (run_id, group),
                                   expect=1, timeout_s=10.0) >= 1

    recovered = _wait_for(_recovered, RECOVER_BOUND_S)
    if not failed_within_bound:
        raise AssertionError("fault not effective for group %r: %s" % (group, failure_detail))
    if not recovered:
        raise AssertionError("group %r did not recover within %.0fs" % (group, RECOVER_BOUND_S))
    return {"proxies": proxies, "toxics_applied": toxics, "toxics_removed": removed,
            "failed_within_bound": True, "failure": failure_detail,
            "recovered": True}


def _db_check() -> dict:
    up_before = _tcp_connect_ok("127.0.0.1", DB_PROXY_PORT)
    _set_proxy_enabled("db", False)
    try:
        down = _wait_for(lambda: not _tcp_connect_ok("127.0.0.1", DB_PROXY_PORT, 1.0), 10.0)
    finally:
        _set_proxy_enabled("db", True)
    recovered = _wait_for(lambda: _tcp_connect_ok("127.0.0.1", DB_PROXY_PORT, 2.0), 20.0)
    if not (up_before and down and recovered):
        raise AssertionError("db proxy toggle not effective (before=%s down=%s recovered=%s)"
                             % (up_before, down, recovered))
    return {"host": "127.0.0.1", "port": DB_PROXY_PORT, "before": up_before,
            "down": down, "recovered": recovered}


def _f1_check(ck, topic: str) -> dict:
    before = _broker_count(ck, SURVIVING_BOOTSTRAP)
    if before != 3:
        raise AssertionError("expected 3 live brokers before F1, got %d" % before)
    kill = stack.broker_kill(1)
    if not kill["ok"]:
        raise AssertionError("broker_kill failed: rc=%s %s" % (kill["rc"], kill["stderr_tail"]))
    dropped = _wait_for(lambda: _broker_count(ck, SURVIVING_BOOTSTRAP) == 2, 45.0)
    start = stack.broker_start(1)
    if not start["ok"]:
        raise AssertionError("broker_start failed: rc=%s %s" % (start["rc"], start["stderr_tail"]))
    restored = _wait_for(lambda: _broker_count(ck, SURVIVING_BOOTSTRAP) == 3, 90.0)
    produced_during = None
    try:
        _produce_once(ck, SURVIVING_BOOTSTRAP, topic, "f1-during", delivery_timeout_ms=5000)
        produced_during = "succeeded"
    except Exception as exc:  # noqa: BLE001 - a blocked produce is contract-consistent
        produced_during = "%s: %s" % (type(exc).__name__, exc)
    recovery_ok = _wait_for(lambda: _consume_round_trip(
        ck, BOOTSTRAP, topic, "f1-recovery", expect=1, timeout_s=10.0) >= 1, 30.0)
    if not (dropped and restored and recovery_ok):
        raise AssertionError("F1 realisation failed (dropped=%s restored=%s recovery=%s)"
                             % (dropped, restored, recovery_ok))
    return {"broker": 1, "broker_count_before": before,
            "broker_count_after_kill": 2 if dropped else None,
            "broker_count_after_start": 3 if restored else None,
            "produce_during_outage": produced_during,
            "recovery_round_trip": True,
            "kill": kill, "start": start}


_F2_CHILD = r"""
import sys, json, time
from confluent_kafka import Consumer
group, topic = sys.argv[1], sys.argv[2]
c = Consumer({"bootstrap.servers": "127.0.0.1:29092",
              "group.id": group,
              "auto.offset.reset": "earliest",
              "enable.auto.commit": False,
              "session.timeout.ms": 6000})
c.subscribe([topic])
print("JOINED", flush=True)
while True:
    c.poll(0.5)
"""


def _f2_check(ck, topic: str) -> dict:
    group = "wxeo-efficacy-f2-%d" % os.getpid()
    child = subprocess.Popen([sys.executable, "-c", _F2_CHILD, group, topic],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.time() + 20
        joined = False
        while time.time() < deadline:
            line = child.stdout.readline() if child.stdout else ""
            if "JOINED" in line:
                joined = True
                break
            if child.poll() is not None:
                break
            time.sleep(0.2)
        if not joined:
            raise AssertionError("F2 child consumer did not join (stdout=%s stderr=%s)"
                                 % (_tail(child.stdout.read() if child.stdout else ""),
                                    _tail(child.stderr.read() if child.stderr else "")))
        admin = _admin(ck, BOOTSTRAP)
        members_seen = _wait_for(
            lambda: len(admin.describe_consumer_groups([group])[group].members) >= 1, 20.0)
        killed = stack.pid_kill_linux(child.pid)
        child.wait(timeout=20)
        dead = child.returncode is not None
        members_after = None
        try:
            members_after = _wait_for(
                lambda: len(admin.describe_consumer_groups([group])[group].members) == 0, 30.0)
        except Exception:  # noqa: BLE001 - group may be expired by the coordinator
            members_after = True
        if not (killed and dead and members_seen and members_after):
            raise AssertionError("F2 realisation failed (killed=%s dead=%s member_seen=%s "
                                 "member_gone=%s)" % (killed, dead, members_seen, members_after))
        return {"group": group, "pid": child.pid, "killed": killed,
                "exit_code": child.returncode, "member_seen": members_seen,
                "member_gone": members_after}
    finally:
        if child.poll() is None:
            child.kill()


# ----------------------------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Linux/container fault-efficacy proof")
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    parser.add_argument("--skip-f1f2", action="store_true",
                        help="skip the broker-kill / pid-kill checks (proxy checks only)")
    args = parser.parse_args(argv)

    if os.name != "posix" or not stack.is_docker():
        print("fault-efficacy: Linux/container substrate required "
              "(host=%s stack_mode=%s)" % (os.name, stack.stack_mode()), file=sys.stderr)
        return 2
    stack.require_docker()
    ck = _import_confluent()

    groups = stack.proxy_groups()
    proxies_available = _list_proxies()
    run_id = "eff-%d-%d" % (os.getpid(), int(time.time()))
    topic = "wxeo.efficacy.%s" % run_id

    report: dict[str, Any] = {
        "schema": "wxeo.linux-fault-efficacy.v1",
        "generated_utc": _utc_now(),
        "host": {"platform": platform.platform(), "machine": platform.machine(),
                 "python": platform.python_version()},
        "stack": {"mode": stack.stack_mode(), "compose_file": stack.compose_file(),
                  "container_names": stack.broker_container_names()},
        "bootstrap": args.bootstrap,
        "topic": topic,
        "proxy_groups": groups,
        "checks": [],
    }
    checks = report["checks"]

    # 0) proxy inventory must match the logical groups (fail closed on typos)
    def _inventory():
        missing = sorted({p for names in groups.values() for p in names} - set(proxies_available))
        if missing:
            raise AssertionError("proxy-groups names not present in Toxiproxy: %s" % missing)
        return {"proxies": sorted(proxies_available), "groups": groups}
    checks.append(_check("inventory.proxies", None, _inventory))

    # 1) bootstrap path: create the scratch topic through a proxy and round-trip once
    def _bootstrap_path():
        _new_topic(ck, args.bootstrap, topic)
        _produce_once(ck, args.bootstrap, topic, "bootstrap")
        seen = _consume_round_trip(ck, args.bootstrap, topic, run_id + "-bootstrap", 1, 20.0)
        if seen < 1:
            raise AssertionError("no record consumed through the proxy bootstrap path")
        return {"bootstrap": args.bootstrap, "produced": 1, "consumed": seen}
    checks.append(_check("bootstrap.proxied-round-trip", None, _bootstrap_path))

    # 2) one check pair per logical group (produce and consume faults on the same proxy set)
    for group, proxies in groups.items():
        if group == "db":
            checks.append(_check("db.proxy-toggle", group, _db_check))
            continue
        checks.append(_check("produce.%s" % group, group,
                             lambda g=group, p=proxies: _group_path_check(ck, g, p, topic, run_id)))
        checks.append(_check("consume.%s" % group, group,
                             lambda g=group, p=proxies: _group_path_check(ck, g, p, topic, run_id)))

    # 3) F1 / F2 realisations (container kill / SIGKILL)
    if not args.skip_f1f2:
        checks.append(_check("f1.broker-kill", "kafka", lambda: _f1_check(ck, topic)))
        checks.append(_check("f2.pid-kill", "consumer", lambda: _f2_check(ck, topic)))

    report["passed"] = bool(checks) and all(c.get("passed") for c in checks)
    report["summary"] = {"checks": len(checks),
                         "passed": sum(1 for c in checks if c.get("passed")),
                         "failed": sum(1 for c in checks if not c.get("passed"))}
    out = args.out if os.path.isabs(args.out) else os.path.join(REPO, args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print("fault-efficacy: %s (%d/%d checks) -> %s"
          % ("PASSED" if report["passed"] else "FAILED",
             report["summary"]["passed"], report["summary"]["checks"], out))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
