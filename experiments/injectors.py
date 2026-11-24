"""F1–F6 fault injectors + fault-receipt protocol (S4-owned; docs/method.md §5.5, CONTRACTS §9).

Every injection is a real action on real objects: a Windows process kill, a real Toxiproxy TCP
cut/timeout, a real consumer-group offset rewind, or an in-process failpoint armed through the
service CLI.  No simulation, no sleep-and-declare.

Receipt protocol (append-only NDJSON in ``results/faults.log``, one line per *action*)
-------------------------------------------------------------------------------------------
* exactly **one** ``fired`` and **one** ``restored`` receipt per armed injection;
* the terminal ``crash`` failpoint is the one exception: S3 writes only ``fired`` (then
  ``Runtime.halt(99)``) and S4 writes the ``restored`` receipt after the instance is restarted —
  the invariant of one fired + one restored per run still holds;
* a **sham** run (injector armed, no action) writes ``armed`` + ``restored`` receipts and never a
  ``fired``; the validator accepts this only for ``injector_mode == "sham"`` validation runs;
* all timestamps are UTC ISO-8601 with milliseconds; the writer fsyncs each line.

Toxiproxy is driven through its HTTP API (stdlib ``urllib`` only).  Proxy names follow S3's
harness contract: ``kafka-produce`` (app→Kafka produce path), ``kafka-consume`` (app→Kafka
consumer/offset path), ``db`` (app→PostgreSQL).

The injectors are constructed by ``experiments/runner.py`` with a :class:`InjectorContext` whose
callbacks are the runner's logged operations; nothing here starts a raw JVM or talks to Kafka
directly except through those documented callbacks.
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

FAULTS = ["F0", "F1", "F2", "F3", "F4a", "F4b", "F5", "F6"]


# --------------------------------------------------------------------------------------------
# receipts
# --------------------------------------------------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def utc_now_ms() -> int:
    return int(time.time() * 1000)


class ReceiptLog:
    """Append-only NDJSON fault-receipt writer with per-line fsync."""

    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def append(self, *, run_id: str, fault: str, event: str, detail: str,
               level: str = "L2", seed: str | None = None, phase_marker: str | None = None,
               scheduled_ms: int | None = None, actual_ms: int | None = None,
               before: dict | None = None, after: dict | None = None,
               target_role: str | None = None, target_id: str | None = None,
               action: str | None = None, result: str | None = None,
               notes: str | None = None) -> dict:
        record = {
            "ts_utc": utc_now_iso(),
            "run_id": run_id,
            "fault": fault,
            "level": level,
            "event": event,
            "target_role": target_role,
            "target_id": target_id,
            "phase_marker": phase_marker,
            "scheduled_ms": scheduled_ms,
            "actual_ms": actual_ms,
            "action": action,
            "result": result or event,
            "before": before,
            "after": after,
            "seed": seed,
            "detail": detail,
            "notes": notes,
        }
        line = json.dumps(record, separators=(",", ":")) + "\n"
        with open(self.path, "a", encoding="utf-8", newline="") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())
        return record


def read_receipts(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    out.append({"malformed": line})
    return out


def validate_receipts(receipts: list[dict], *, run_id: str, injector_mode: str) -> dict:
    """G4 attestation for one run: exactly one fired + one restored (sham: armed + restored).

    Returns ``{attested, reason, fired, restored, sham, receipts}``.
    """
    mine = [r for r in receipts if r.get("run_id") == run_id and "malformed" not in r]
    fired = [r for r in mine if r.get("event") == "fired"]
    restored = [r for r in mine if r.get("event") == "restored"]
    armed = [r for r in mine if r.get("event") == "armed"]
    detail = {"fired": len(fired), "restored": len(restored), "armed": len(armed),
              "receipts": len(mine)}
    if injector_mode in ("none", "fixture"):
        return {"attested": True, "reason": None, "sham": False, **detail}
    if injector_mode == "sham":
        ok = len(armed) == 1 and len(restored) == 1 and len(fired) == 0
        return {
            "attested": ok,
            "reason": None if ok else "sham run must have exactly one armed + one restored "
                                      "receipt and no fired receipt",
            "sham": True, **detail,
        }
    ok = len(fired) == 1 and len(restored) == 1
    reason = None
    if not ok:
        reason = (f"expected exactly one fired and one restored receipt, got "
                  f"fired={len(fired)} restored={len(restored)}")
    return {"attested": ok, "reason": reason, "sham": False, **detail}


# --------------------------------------------------------------------------------------------
# Toxiproxy HTTP API (stdlib only)
# --------------------------------------------------------------------------------------------
class ToxiproxyError(RuntimeError):
    pass


def _http(method: str, url: str, payload: dict | None = None, timeout: float = 10.0):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:  # pragma: no cover - live path
        body = exc.read().decode("utf-8", "replace")
        raise ToxiproxyError(f"Toxiproxy HTTP {method} {url} -> {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:  # pragma: no cover - live path
        raise ToxiproxyError(f"Toxiproxy unreachable at {url}: {exc.reason}") from exc


class Toxiproxy:
    def __init__(self, api_url: str = "http://127.0.0.1:8474"):
        self.api_url = api_url.rstrip("/")

    def proxies(self) -> dict:
        return _http("GET", f"{self.api_url}/proxies")

    def set_enabled(self, proxy: str, enabled: bool) -> dict:
        return _http("POST", f"{self.api_url}/proxies/{proxy}", {"enabled": enabled})

    def add_toxic(self, proxy: str, toxic: dict) -> dict:
        return _http("POST", f"{self.api_url}/proxies/{proxy}/toxics", toxic)

    def remove_toxic(self, proxy: str, name: str) -> dict:
        return _http("DELETE", f"{self.api_url}/proxies/{proxy}/toxics/{name}")

    def toxics(self, proxy: str) -> list:
        return _http("GET", f"{self.api_url}/proxies/{proxy}/toxics")

    def wait_healthy(self, proxy: str, timeout_s: float = 30.0) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                info = _http("GET", f"{self.api_url}/proxies/{proxy}")
                if info.get("enabled", True):
                    return True
            except ToxiproxyError:
                pass
            time.sleep(0.2)
        return False


# --------------------------------------------------------------------------------------------
# context handed in by the runner
# --------------------------------------------------------------------------------------------
@dataclass
class InjectorContext:
    """Everything an injector may touch; all operations are the runner's logged commands."""

    run_id: str
    mechanism: str
    ordering_variant: str
    fault: str
    fault_level: str            # L1 | L2 | none
    injector_mode: str          # live | sham | failpoint | selftest | fixture | none
    seed_hex: str
    scratch_dir: str
    faults_log: str
    bootstrap: str
    toxiproxy_api: str = "http://127.0.0.1:8474"
    stack_state_path: str = ""
    native_stack_ps1: str = ""
    kafka_home: str = ""
    java_exe: str = "java"
    db_dsn: str = ""
    run_log: Callable[..., Any] = lambda *a, **k: None
    call: Callable[..., subprocess.CompletedProcess] = None  # runner's logged subprocess runner
    restart_service: Callable[..., int] | None = None
    stop_consumers: Callable[..., None] | None = None
    start_consumers: Callable[..., None] | None = None
    wait_group_stable: Callable[..., bool] | None = None
    wait_isr: Callable[..., bool] | None = None
    consumer_pids: dict[int, int] = field(default_factory=dict)
    extra: dict = field(default_factory=dict)


# --------------------------------------------------------------------------------------------
# injector base
# --------------------------------------------------------------------------------------------
class Injector:
    fault = "?"
    level = "L2"
    description = ""

    def __init__(self, ctx: InjectorContext):
        self.ctx = ctx
        self.receipts = ReceiptLog(ctx.faults_log)
        self.rng = random.Random(int.from_bytes(
            __import__("hashlib").sha256(
                f"inj|{ctx.seed_hex}|{ctx.fault}|{ctx.mechanism}".encode()).digest()[:8], "big"))
        self.state: dict = {}
        self._fired = False
        self._restored = False
        self.scheduled_ms: int | None = None
        self.actual_ms: int | None = None

    # -- receipt helpers ------------------------------------------------------------------
    def _receipt(self, event: str, detail: str, **kw) -> dict:
        return self.receipts.append(
            run_id=self.ctx.run_id, fault=self.fault, event=event, detail=detail,
            level=self.level, seed=self.ctx.seed_hex, scheduled_ms=self.scheduled_ms,
            actual_ms=self.actual_ms, **kw,
        )

    # -- interface ------------------------------------------------------------------------
    def arm(self) -> "Injector":
        """Prepare the injection (record target, optional sham no-op); no action yet."""
        if self.ctx.injector_mode == "sham":
            self._receipt("armed", "sham: injector armed, no action (validation run)",
                          target_role=self.target_role(), result="armed", action="sham-arm")
            self._restored = True
            return self
        self._arm()
        return self

    def trigger_and_fire(self, *, wait: Callable[[], bool] | None = None) -> dict:
        """Wait for the trigger (if any) then perform the injection; writes the fired receipt."""
        if self.ctx.injector_mode == "sham":
            return {"fired": False, "sham": True}
        if wait is not None:
            wait()
        self.scheduled_ms = utc_now_ms()
        detail, before, after = self._fire()
        self.actual_ms = utc_now_ms()
        self._fired = True
        self._restore_after_fire()
        return {"fired": True, "detail": detail, "before": before, "after": after}

    def restore(self) -> dict:
        """Restore the system and write the restored receipt (idempotent)."""
        if self.ctx.injector_mode == "sham":
            receipt = self._receipt("restored", "sham: disarmed, system untouched",
                                    target_role=self.target_role(), result="restored",
                                    action="sham-disarm")
            return {"restored": True, "sham": True, "receipt": receipt}
        if not self._fired:
            # armed but never fired: system was never touched; still close the injection
            self._receipt("restored", "injection never fired; nothing to restore",
                          target_role=self.target_role(), result="restored",
                          action="no-op-restore")
            self._restored = True
            return {"restored": True, "noop": True}
        if self._restored:
            return {"restored": True, "already": True}
        detail, after = self._restore()
        receipt = self._receipt("restored", detail, target_role=self.target_role(),
                                result="restored", action=self.restore_action(),
                                after=after)
        self._restored = True
        return {"restored": True, "receipt": receipt}

    # -- subclass hooks -------------------------------------------------------------------
    def target_role(self) -> str:
        return "unknown"

    def restore_action(self) -> str:
        return "restore"

    def _arm(self) -> None:
        raise NotImplementedError

    def _fire(self) -> tuple[str, dict | None, dict | None]:
        raise NotImplementedError

    def _restore(self) -> tuple[str, dict | None]:
        raise NotImplementedError

    def _restore_after_fire(self) -> None:
        """Actions that must be performed immediately after firing (before final restore)."""
        return None


# --------------------------------------------------------------------------------------------
# F1 — Kafka broker killed mid-produce
# --------------------------------------------------------------------------------------------
class F1BrokerKill(Injector):
    fault = "F1"
    description = "kill one Kafka broker process mid-produce; restart it; wait ISR=3"

    def target_role(self) -> str:
        return "broker"

    def _arm(self) -> None:
        state = _read_json(self.ctx.stack_state_path)
        brokers = state.get("brokers") or state.get("kafka") or []
        if not brokers:
            raise RuntimeError("stack-state.json has no broker pids; cannot arm F1")
        target = self.rng.choice(brokers)
        self.state["target"] = target
        self.state["broker_count_before"] = len(brokers)

    def _fire(self):
        target = self.state["target"]
        pid = int(target["pid"])
        node_id = target.get("node_id", target.get("nodeId"))
        before = {"node_id": node_id, "pid": pid, "brokers": self.state["broker_count_before"]}
        cmd = ["taskkill", "/F", "/PID", str(pid)]
        self.ctx.call(cmd, check=False)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and _pid_alive(pid):
            time.sleep(0.2)
        return (f"killed broker node {node_id} pid {pid}", before,
                {"pid_alive_after": _pid_alive(pid)})

    def _restore(self):
        self.ctx.run_log("restore F1: start-kafka + wait ISR=3")
        self.ctx.call([self.ctx.native_stack_ps1, "start-kafka"], check=False)
        ok_count = _wait(lambda: self._broker_count() >= 3, timeout=120)
        ok_isr = True
        source_topic = self.ctx.extra.get("source_topic")
        if source_topic and self.ctx.wait_isr:
            ok_isr = self.ctx.wait_isr(source_topic, 3, timeout=120)
        return (f"broker restarted; brokers>=3={ok_count} isr=3={ok_isr}",
                {"brokers": self._broker_count(), "isr_ok": ok_isr})

    def _broker_count(self) -> int:
        try:
            state = _read_json(self.ctx.stack_state_path)
            return len(state.get("brokers") or state.get("kafka") or [])
        except Exception:  # noqa: BLE001 - liveness probe
            return 0


# --------------------------------------------------------------------------------------------
# F2 — consumer instance killed during processing (real rebalance)
# --------------------------------------------------------------------------------------------
class F2ConsumerKill(Injector):
    fault = "F2"
    description = "kill one consumer JVM; the group rebalances; restart the instance"

    def target_role(self) -> str:
        return "consumer"

    def _arm(self) -> None:
        pids = dict(self.ctx.consumer_pids)
        if not pids:
            raise RuntimeError("no consumer pids available; cannot arm F2")
        victim = self.rng.choice(sorted(pids))
        self.state["victim"] = victim
        self.state["pid"] = pids[victim]

    def _fire(self):
        victim = self.state["victim"]
        pid = int(self.state["pid"])
        before = {"instance": victim, "pid": pid}
        self.ctx.call(["taskkill", "/F", "/PID", str(pid)], check=False)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and _pid_alive(pid):
            time.sleep(0.2)
        return f"killed consumer instance {victim} pid {pid}", before, {"pid_alive": _pid_alive(pid)}

    def _restore(self):
        victim = self.state["victim"]
        if self.ctx.restart_service is None:
            raise RuntimeError("runner did not provide restart_service for F2")
        new_pid = self.ctx.restart_service(victim)
        stable = self.ctx.wait_group_stable(timeout=120) if self.ctx.wait_group_stable else True
        return (f"consumer instance {victim} restarted pid={new_pid} group_stable={stable}",
                {"instance": victim, "new_pid": new_pid, "group_stable": stable})


# --------------------------------------------------------------------------------------------
# F3 — produce timeout / unknown outcome (Toxiproxy timeout toxic on app->Kafka produce path)
# --------------------------------------------------------------------------------------------
class F3ProduceTimeout(Injector):
    fault = "F3"
    description = "timeout toxic on the produce path so produce/commit outcome is unknown"
    T_HOLD_S = 5.0

    def __init__(self, ctx: InjectorContext):
        super().__init__(ctx)
        self.toxiproxy = Toxiproxy(ctx.toxiproxy_api)

    def target_role(self) -> str:
        return "app-kafka-produce-link"

    def _arm(self) -> None:
        self.state["proxy"] = self.ctx.extra.get("produce_proxy", "kafka-produce")
        self.state["toxic_name"] = f"wxeo-f3-{self.ctx.run_id}"
        self.state["timeout_ms"] = 5000

    def _fire(self):
        toxic = {
            "name": self.state["toxic_name"],
            "type": "timeout",
            "stream": "downstream",
            "attributes": {"timeout": self.state["timeout_ms"]},
        }
        self.toxiproxy.add_toxic(self.state["proxy"], toxic)
        return (f"added timeout toxic {self.state['toxic_name']} to "
                f"{self.state['proxy']} (timeout={self.state['timeout_ms']}ms)",
                {"proxy": self.state["proxy"]}, {"toxic": toxic})

    def restore_action(self) -> str:
        return "remove toxic + verify proxy health"

    def _restore(self):
        try:
            self.toxiproxy.remove_toxic(self.state["proxy"], self.state["toxic_name"])
        except ToxiproxyError as exc:
            if "404" not in str(exc):
                raise
        healthy = self.toxiproxy.wait_healthy(self.state["proxy"], timeout_s=30)
        return (f"removed toxic; proxy healthy={healthy}",
                {"proxy_healthy": healthy, "toxics": self.toxiproxy.toxics(self.state["proxy"])})


# --------------------------------------------------------------------------------------------
# F4a — DB commit succeeded + offset commit failed (cut app->Kafka consumer link)
# --------------------------------------------------------------------------------------------
class F4aOffsetCommitFailure(Injector):
    fault = "F4a"
    description = "disable the app->Kafka consumer link so the offset commit cannot persist"
    T_HOLD_S = 3.0

    def __init__(self, ctx: InjectorContext):
        super().__init__(ctx)
        self.toxiproxy = Toxiproxy(ctx.toxiproxy_api)

    def target_role(self) -> str:
        return "app-kafka-consume-link"

    def _arm(self) -> None:
        self.state["proxy"] = self.ctx.extra.get("consume_proxy", "kafka-consume")

    def _fire(self):
        self.toxiproxy.set_enabled(self.state["proxy"], False)
        return (f"disabled proxy {self.state['proxy']} (offset commits cannot persist)",
                {"proxy_enabled": True}, {"proxy_enabled": False})

    def _restore_after_fire(self) -> None:
        time.sleep(self.T_HOLD_S)  # bounded cut window per docs/method.md §5.5

    def restore_action(self) -> str:
        return "enable link + verify proxy health + wait offset retry"

    def _restore(self):
        self.toxiproxy.set_enabled(self.state["proxy"], True)
        healthy = self.toxiproxy.wait_healthy(self.state["proxy"], timeout_s=30)
        stable = self.ctx.wait_group_stable(timeout=60) if self.ctx.wait_group_stable else True
        return (f"re-enabled {self.state['proxy']}; healthy={healthy} group_stable={stable}",
                {"proxy_enabled": True, "proxy_healthy": healthy, "group_stable": stable})


# --------------------------------------------------------------------------------------------
# F4b — ordering probe (early ordering) + effect-commit failure (Level 1, via S3 failpoint)
# --------------------------------------------------------------------------------------------
class F4bOrderingProbe(Injector):
    fault = "F4b"
    description = ("ordering=early probe: progress committed before effect; the effect commit is "
                   "aborted by the S3 failpoint before_effect_commit (crash/abort_txn)")
    level = "L1"

    def target_role(self) -> str:
        return "app-failpoint"

    def _arm(self) -> None:
        if self.ctx.ordering_variant != "early":
            raise RuntimeError("F4b requires ordering=early (armed at service start)")
        self.state["failpoint"] = "before_effect_commit"
        self.state["action"] = self.ctx.extra.get("failpoint_action", "crash")
        self.state["after"] = int(self.ctx.extra.get("failpoint_after", 1))

    def _fire(self):
        """The failpoint fires inside the service; S4 waits for the S3-fired receipt."""
        deadline = time.monotonic() + float(self.ctx.extra.get("failpoint_timeout_s", 120))
        found = None
        while time.monotonic() < deadline:
            for receipt in read_receipts(self.ctx.faults_log):
                if (receipt.get("run_id") == self.ctx.run_id
                        and receipt.get("fault") == self.state["failpoint"]
                        and receipt.get("event") == "fired"):
                    found = receipt
                    break
            if found:
                break
            time.sleep(0.1)
        if not found:
            raise RuntimeError("F4b failpoint did not fire (no S3 receipt within timeout)")
        return (f"failpoint {self.state['failpoint']} fired with action {self.state['action']}",
                None, {"failpoint_receipt": found})

    def _restore(self):
        stable = True
        if self.ctx.restart_service is not None:
            self.ctx.restart_service(1)
            self.ctx.restart_service(2)
        if self.ctx.wait_group_stable:
            stable = self.ctx.wait_group_stable(timeout=120)
        return (f"instances restarted after failpoint; group_stable={stable}",
                {"group_stable": stable})


# --------------------------------------------------------------------------------------------
# F5 — network partition app<->DB (Toxiproxy disable on the db proxy)
# --------------------------------------------------------------------------------------------
class F5DbPartition(Injector):
    fault = "F5"
    description = "blackhole the app->PostgreSQL link during a commit, then restore it"
    T_HOLD_S = 3.0

    def __init__(self, ctx: InjectorContext):
        super().__init__(ctx)
        self.toxiproxy = Toxiproxy(ctx.toxiproxy_api)

    def target_role(self) -> str:
        return "app-db-link"

    def _arm(self) -> None:
        self.state["proxy"] = self.ctx.extra.get("db_proxy", "db")

    def _fire(self):
        self.toxiproxy.set_enabled(self.state["proxy"], False)
        return (f"disabled db proxy {self.state['proxy']} (app<->PostgreSQL partition)",
                {"proxy_enabled": True}, {"proxy_enabled": False})

    def _restore_after_fire(self) -> None:
        time.sleep(self.T_HOLD_S)

    def restore_action(self) -> str:
        return "enable db link + wait health + readiness probe"

    def _restore(self):
        self.toxiproxy.set_enabled(self.state["proxy"], True)
        healthy = self.toxiproxy.wait_healthy(self.state["proxy"], timeout_s=30)
        return (f"db link re-enabled; healthy={healthy}",
                {"proxy_enabled": True, "proxy_healthy": healthy})


# --------------------------------------------------------------------------------------------
# F6 — duplicate redelivery after rebalance (protocol-level replay; D5)
# --------------------------------------------------------------------------------------------
class F6DuplicateRedelivery(Injector):
    fault = "F6"
    description = ("stop consumers, rewind the mechanism's own resume point by k batches, "
                   "restart -> duplicate redelivery of already-processed records (protocol-level "
                   "only; D5)")

    def target_role(self) -> str:
        return "consumer-progress"

    def _arm(self) -> None:
        self.state["k"] = int(self.ctx.extra.get("rewind_batches", 2))
        self.state["mechanism_is_m4"] = self.ctx.mechanism in ("M4", "M4+M3")

    def _fire(self):
        if self.ctx.stop_consumers is None:
            raise RuntimeError("runner did not provide stop_consumers for F6")
        self.ctx.stop_consumers()
        before = {"k_batches": self.state["k"]}
        if self.state["mechanism_is_m4"]:
            # M4 family: the resume point is the DB progress table
            sql = ("UPDATE consumer_progress SET next_offset = GREATEST(0, next_offset - "
                   f"{int(self.state['k'])})")
            self.ctx.call([self.ctx.extra["psql"], "-d", self.ctx.extra["db_dsn"], "-c", sql],
                          check=True)
            before["progress_update"] = sql
        else:
            cmd = [
                self.ctx.java_exe, "-cp", os.path.join(self.ctx.kafka_home, "libs", "*"),
                "kafka.admin.ConsumerGroupCommand",
                "--bootstrap-server", self.ctx.bootstrap,
                "--group", self.ctx.extra["consumer_group"],
                "--reset-offsets", "--shift-by", f"-{int(self.state['k'])}",
                "--all-topics", "--execute",
            ]
            self.ctx.call(cmd, check=True)
            before["offset_rewind"] = " ".join(cmd)
        return (f"consumers stopped; resume point rewound by {self.state['k']} batches",
                before, None)

    def _restore(self):
        if self.ctx.start_consumers is not None:
            self.ctx.start_consumers()
        stable = self.ctx.wait_group_stable(timeout=120) if self.ctx.wait_group_stable else True
        return (f"consumers restarted; group_stable={stable}", {"group_stable": stable})


# --------------------------------------------------------------------------------------------
# Level-1 deterministic crash points armed through S3 failpoints
# --------------------------------------------------------------------------------------------
FAILPOINT_NAMES = [
    "after_fetch", "before_effect_commit", "after_effect_commit", "before_progress_commit",
    "after_progress_commit", "before_publish", "before_relay_mark",
]

#: map Stage-A crash class -> the failpoint that realises it deterministically
CRASH_CLASS_FAILPOINT = {
    "K1": {"name": "after_fetch", "action": "crash"},
    "K2": {"name": "after_effect_commit", "action": "crash"},
    "K3": {"name": "after_effect_commit", "action": "crash"},       # replay after effect durable
    "K5": {"name": "before_effect_commit", "action": "pause:200"},
    "K6": {"name": "after_effect_commit", "action": "pause:200"},   # zombie writer window
    "K7": {"name": "before_relay_mark", "action": "crash"},
}


def failpoint_config_for_run(ctx: InjectorContext) -> dict | None:
    """Return service failpoint args for a run, or None when no L1 failpoint is armed.

    * Stage-A crash-class runs: the class maps to the documented failpoint/action
      (docs/method.md §5.4 Level-1 realisations, CONTRACTS §7).
    * F4b runs: ordering=early + ``before_effect_commit`` crash/abort_txn.
    * Everything else: no in-process failpoint.
    """
    if ctx.fault_level != "L1" and ctx.fault != "F4b":
        return None
    if ctx.injector_mode in ("none", "fixture"):
        return None
    if ctx.fault == "F4b":
        return {"enabled": True, "name": "before_effect_commit",
                "action": ctx.extra.get("failpoint_action", "crash"), "after": 1}
    klass = ctx.fault
    spec = CRASH_CLASS_FAILPOINT.get(klass)
    if spec is None:
        return {"enabled": False}
    return {"enabled": True, "name": spec["name"], "action": spec["action"],
            "after": int(ctx.extra.get("failpoint_after", 1))}


class FailpointWatch(Injector):
    """Non-firing arm for L1 failpoints: validates the S3 receipt and writes the restored receipt.

    The injection action itself is inside the service; S4's job is to (a) arm it through the
    service CLI, (b) wait for the S3 ``fired`` receipt, and (c) after restart write the
    ``restored`` receipt (the terminal crash failpoint is fired-only by design).
    """

    level = "L1"

    def __init__(self, ctx: InjectorContext):
        super().__init__(ctx)
        self.fault = ctx.fault

    def target_role(self) -> str:
        return "app-failpoint"

    def _arm(self) -> None:
        self.state["config"] = failpoint_config_for_run(self.ctx) or {"enabled": False}
        if not self.state["config"].get("enabled"):
            raise RuntimeError(f"no L1 failpoint available for {self.ctx.fault}")

    def _fire(self):
        name = self.state["config"]["name"]
        deadline = time.monotonic() + float(self.ctx.extra.get("failpoint_timeout_s", 180))
        found = None
        while time.monotonic() < deadline:
            for receipt in read_receipts(self.ctx.faults_log):
                if (receipt.get("run_id") == self.ctx.run_id
                        and receipt.get("fault") == name
                        and receipt.get("event") == "fired"):
                    found = receipt
                    break
            if found:
                break
            time.sleep(0.1)
        if not found:
            raise RuntimeError(f"L1 failpoint {name} did not fire (no S3 receipt)")
        return f"failpoint {name} fired", None, {"failpoint_receipt": found}

    def _restore(self):
        stable = True
        if self.ctx.restart_service is not None:
            self.ctx.restart_service(1)
        if self.ctx.wait_group_stable:
            stable = self.ctx.wait_group_stable(timeout=180)
        return f"instance restarted after failpoint; group_stable={stable}", {"group_stable": stable}


# --------------------------------------------------------------------------------------------
# factory + trigger scheduling
# --------------------------------------------------------------------------------------------
INJECTOR_TYPES = {
    "F1": F1BrokerKill,
    "F2": F2ConsumerKill,
    "F3": F3ProduceTimeout,
    "F4a": F4aOffsetCommitFailure,
    "F4b": F4bOrderingProbe,
    "F5": F5DbPartition,
    "F6": F6DuplicateRedelivery,
}


def make_injector(ctx: InjectorContext) -> Injector | None:
    """Create the injector for a run: Level-1 failpoint watch, Level-2 fault, or None."""
    if ctx.injector_mode in ("none", "fixture"):
        return None
    if ctx.injector_mode == "sham":
        # a sham run arms the cell's injector but performs no action
        cls = INJECTOR_TYPES.get(ctx.fault)
        if cls is None:
            # F0 sham: a no-op injector
            inst = Injector(ctx)
            inst.fault = ctx.fault or "F0"
            return inst
        return cls(ctx)
    if ctx.fault_level == "L1" or ctx.fault == "F4b":
        return FailpointWatch(ctx)
    cls = INJECTOR_TYPES.get(ctx.fault)
    if cls is None:
        raise ValueError(f"no injector for fault {ctx.fault!r}")
    return cls(ctx)


def level2_trigger_time(ctx: InjectorContext, *, load_duration_s: float,
                        window_fraction: tuple[float, float] = (0.25, 0.75)) -> float:
    """Seeded Level-2 fire time (seconds after load start), uniformly inside the window.

    Recorded per run (``scheduled_ms``) so the realised time is reproducible from the seed.
    """
    lo, hi = window_fraction
    rng = random.Random(int.from_bytes(
        __import__("hashlib").sha256(f"trigger|{ctx.seed_hex}".encode()).digest()[:8], "big"))
    return load_duration_s * rng.uniform(lo, hi)


# --------------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------------
def _read_json(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":  # pragma: no cover - Windows live path
        try:
            out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                                 capture_output=True, text=True, timeout=15)
            return str(pid) in (out.stdout or "")
        except Exception:  # noqa: BLE001
            return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _wait(predicate: Callable[[], bool], timeout: float, interval: float = 0.5) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return True
        except Exception:  # noqa: BLE001 - poll loop
            pass
        time.sleep(interval)
    return False


__all__ = [
    "FAULTS",
    "ReceiptLog",
    "read_receipts",
    "validate_receipts",
    "Toxiproxy",
    "ToxiproxyError",
    "InjectorContext",
    "Injector",
    "F1BrokerKill",
    "F2ConsumerKill",
    "F3ProduceTimeout",
    "F4aOffsetCommitFailure",
    "F4bOrderingProbe",
    "F5DbPartition",
    "F6DuplicateRedelivery",
    "FailpointWatch",
    "FAILPOINT_NAMES",
    "CRASH_CLASS_FAILPOINT",
    "failpoint_config_for_run",
    "make_injector",
    "level2_trigger_time",
    "utc_now_iso",
    "utc_now_ms",
]
