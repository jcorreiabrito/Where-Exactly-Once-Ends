"""F1–F6 and Level-1 fault injectors (v2, post-S5 review).

Every injection is a real action on real objects: a Windows process kill, a real Toxiproxy TCP
cut/timeout, a real consumer-group offset rewind, or an in-process failpoint armed through the
service CLI. No simulation.

Receipt protocol v2 — see :mod:`experiments.receipts` for the single source of truth
-------------------------------------------------------------------------------------------
* **S4-owned** injections (external Level-2 F1/F2/F3/F4a/F5/F6 and the Level-1 classes K2/K5 that
  are realised by S4 protocol/network actions): S4 writes exactly one ``fired`` **after verifying
  that the action took effect** (with an ``action_verified`` flag and a ``phase`` block carrying
  the load window and realised fire time) and exactly one ``restored``.
* **App-owned** failpoints (K1/K3/K4/K7 and F4b): the Java service writes ``fired`` (and its own
  ``restored`` for non-terminal actions). For the terminal ``crash`` action S4 writes the
  matching ``restored`` receipt after the restart; S4 never duplicates a non-terminal restored.
* **Sham** runs write ``armed`` + ``restored`` and no ``fired``.

Realisation table (S5 fix pass; recorded in results/DEVIATIONS.md and the P2 amendment):

| class | realisation | owner |
|---|---|---|
| K1 | failpoint ``after_fetch`` crash (no durable trace) | app |
| K2 | **unknown outcome** (C3/C9): seeded progress marker -> Toxiproxy ``timeout`` toxic on the effect link (DB for DB sinks, ``kafka-produce`` for M1-kafka); the in-flight commit times out with unknown outcome | S4 |
| K3 | failpoint ``after_effect_commit`` crash (effect durable, progress behind) | app |
| K4 | ``ordering=early`` probe + failpoint ``before_effect_commit`` crash (progress durable, effect absent) | app |
| K5 | protocol-level duplicate delivery: stop consumers, rewind the mechanism's own resume point by k batches, restart (C6 without crash) | S4 |
| K6 | **structurally not realisable** inside the frozen max.poll.interval.ms=300 s / T_max window -> declared n/a with proof, never executed (matrix never emits it) | — |
| K7 | failpoint ``before_relay_mark`` crash (publication window) | app |

M1-kafka role instantiation (D4, semantic-role targeting): M1-kafka has no separate effect hook;
for K1 it uses ``after_fetch``; K2 is realised on its producer/transaction commit path with the
``kafka-produce`` proxy. K3/K4/K5/K6/K7 are structurally n/a for M1-kafka (matrix).
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

try:
    from experiments import receipts as RCPT
    from experiments import pathsafe
except ImportError:  # script execution
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from experiments import receipts as RCPT  # type: ignore
    from experiments import pathsafe  # type: ignore

FAULTS = ["F0", "F1", "F2", "F3", "F4a", "F4b", "F5", "F6"]

FAILPOINT_NAMES = [
    "after_fetch", "before_effect_commit", "after_effect_commit", "before_progress_commit",
    "after_progress_commit", "before_publish", "before_relay_mark",
]

#: Level-1 class -> app failpoint (canonical upper-case keys; tokens are normalised on input)
CRASH_CLASS_FAILPOINT = {
    "K1": {"name": "after_fetch", "action": "crash"},
    "K3": {"name": "after_effect_commit", "action": "crash"},
    "K4": {"name": "before_effect_commit", "action": "crash"},
    "K7": {"name": "before_relay_mark", "action": "crash"},
}

#: classes realised by S4 external actions (no app failpoint)
S4_REALISED_CLASSES = {"K2", "K5"}

#: declared structurally unreachable crash classes with the reachability proof (S5 F-E2)
STRUCTURALLY_UNREACHABLE = {
    "K6": "zombie writer C7 requires the owner to be paused beyond max.poll.interval.ms = 300 s, "
          "which exceeds T_max = 300 s and would violate the frozen shared configuration; "
          "declared n/a with the reachability proof and reported as model-only in the paper",
}

#: M1-kafka semantic-role instantiation for Level-1 failpoints (D4)
M1KAFKA_FAILPOINT = {
    "K1": {"name": "after_fetch", "action": "crash"},
}

#: terminal failpoint actions
TERMINAL_ACTIONS = RCPT.TERMINAL_ACTIONS

TOXIPROXY_DEFAULT = "http://127.0.0.1:8474"
MAX_POLL_RECORDS = 500   # docs/method.md §4.11 batch size (k-batch rewind unit)


class InjectionNotVerified(RuntimeError):
    """The injector action did not demonstrably take effect; no ``fired`` receipt is written."""


# --------------------------------------------------------------------------------------------
# receipts
# --------------------------------------------------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def utc_now_ms() -> int:
    return int(time.time() * 1000)


class ReceiptLog:
    """Append-only NDJSON fault-receipt writer with per-line fsync (source-tagged)."""

    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def append(self, *, run_id: str, fault: str, event: str, detail: str,
               source: str = "s4", level: str = "L2", seed: str | None = None,
               phase_marker: str | None = None, scheduled_ms: int | None = None,
               actual_ms: int | None = None, before: dict | None = None,
               after: dict | None = None, target_role: str | None = None,
               target_id: str | None = None, action: str | None = None,
               action_verified: bool | None = None, phase: dict | None = None,
               result: str | None = None, notes: str | None = None) -> dict:
        record = {
            "ts_utc": utc_now_iso(),
            "run_id": run_id,
            "fault": fault,
            "level": level,
            "event": event,
            "source": source,
            "target_role": target_role,
            "target_id": target_id,
            "phase_marker": phase_marker,
            "scheduled_ms": scheduled_ms,
            "actual_ms": actual_ms,
            "action": action,
            "action_verified": action_verified,
            "phase": phase,
            "before": before,
            "after": after,
            "seed": seed,
            "detail": detail,
            "result": result or event,
            "notes": notes,
        }
        line = json.dumps(pathsafe.scrub(record), separators=(",", ":")) + "\n"
        with open(self.path, "a", encoding="utf-8", newline="") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())
        return record


def read_receipts(path: str) -> list[dict]:
    if not path or not os.path.exists(path):
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


def validate_receipts(receipts: list[dict], *, run_id: str, injector_mode: str,
                      fault: str = "", fault_level: str = "", failpoint: str | None = None,
                      action: str | None = None) -> dict:
    """Delegate to the single-source-of-truth protocol module."""
    return RCPT.validate_run_receipts(
        receipts, run_id=run_id, injector_mode=injector_mode, fault=fault,
        fault_level=fault_level, failpoint=failpoint, action=action)


# --------------------------------------------------------------------------------------------
# Toxiproxy HTTP API (stdlib only)
# --------------------------------------------------------------------------------------------
class ToxiproxyError(RuntimeError):
    pass


def _http(method: str, url: str, payload: dict | None = None, timeout: float = 10.0):
    data = None
    # Toxiproxy 2.9.0 rejects PowerShell's default user agent with HTTP 403; send an explicit one.
    headers = {"User-Agent": "wxeo-harness/1.0"}
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
    def __init__(self, api_url: str = TOXIPROXY_DEFAULT):
        self.api_url = api_url.rstrip("/")

    def proxies(self) -> dict:
        return _http("GET", f"{self.api_url}/proxies")

    def get(self, proxy: str) -> dict:
        return _http("GET", f"{self.api_url}/proxies/{proxy}")

    def set_enabled(self, proxy: str, enabled: bool) -> dict:
        return _http("POST", f"{self.api_url}/proxies/{proxy}", {"enabled": enabled})

    def add_toxic(self, proxy: str, toxic: dict) -> dict:
        return _http("POST", f"{self.api_url}/proxies/{proxy}/toxics", toxic)

    def remove_toxic(self, proxy: str, name: str) -> dict:
        return _http("DELETE", f"{self.api_url}/proxies/{proxy}/toxics/{name}")

    def toxics(self, proxy: str) -> list:
        return _http("GET", f"{self.api_url}/proxies/{proxy}/toxics")

    def toxic_present(self, proxy: str, name: str) -> bool:
        try:
            return any(t.get("name") == name for t in self.toxics(proxy))
        except ToxiproxyError:
            return False

    def is_enabled(self, proxy: str) -> bool:
        return bool(self.get(proxy).get("enabled", True))

    def wait_healthy(self, proxy: str, timeout_s: float = 30.0) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                if self.is_enabled(proxy):
                    return True
            except ToxiproxyError:
                pass
            time.sleep(0.2)
        return False


# --------------------------------------------------------------------------------------------
# runner-provided context
# --------------------------------------------------------------------------------------------
@dataclass
class InjectorContext:
    """Everything an injector may touch; all stack operations are runner-provided callables."""

    run_id: str
    mechanism: str
    ordering_variant: str
    fault: str                    # canonical token (K3, F1, ...) or lower-case matrix token
    fault_level: str              # L1 | L2 | none
    injector_mode: str            # live | sham | failpoint | selftest | fixture | none
    seed_hex: str
    scratch_dir: str
    faults_log: str
    bootstrap: str
    toxiproxy_api: str = TOXIPROXY_DEFAULT
    stack_state_path: str = ""
    native_stack_ps1: str = ""
    kafka_home: str = ""
    java_exe: str = "java"
    db_dsn: str = ""
    admin_dsn: str = ""
    run_log: Callable[..., Any] = lambda *a, **k: None
    call: Callable[..., subprocess.CompletedProcess] = None  # type: ignore[assignment]
    restart_service: Callable[..., int] | None = None
    stop_consumers: Callable[..., None] | None = None
    start_consumers: Callable[..., None] | None = None
    wait_group_stable: Callable[..., bool] | None = None
    wait_isr: Callable[..., bool] | None = None
    progress_probe: Callable[[], int] | None = None        # committed progress marker
    rewind_progress: Callable[[int], dict] | None = None   # k-batch rewind for K5/F6
    verify_broker_count: Callable[[], int] | None = None
    broker_count_probe: Callable[[], int] | None = None
    failpoint_instance: int | None = None
    consumer_pids: dict[int, int] = field(default_factory=dict)
    # phase bookkeeping set by the runner while the load generator runs
    load_start_ms: int | None = None
    load_end_ms: int | None = None
    extra: dict = field(default_factory=dict)


# --------------------------------------------------------------------------------------------
# injector base
# --------------------------------------------------------------------------------------------
class Injector:
    fault = "?"
    level = "L2"
    description = ""
    owns_receipts = True     # False for app-failpoint watchers (app owns fired)

    def __init__(self, ctx: InjectorContext):
        self.ctx = ctx
        self.receipts = ReceiptLog(ctx.faults_log)
        self.rng = random.Random(int.from_bytes(
            hashlib.sha256(f"inj|{ctx.seed_hex}|{ctx.fault}|{ctx.mechanism}".encode()).digest()[:8],
            "big"))
        self.state: dict = {}
        self._fired = False
        self._restored = False
        self.scheduled_ms: int | None = None
        self.actual_ms: int | None = None
        self.canonical_fault = _canonical_fault(ctx.fault)

    # -- receipt helpers ------------------------------------------------------------------
    def _receipt(self, event: str, detail: str, **kw) -> dict:
        return self.receipts.append(
            run_id=self.ctx.run_id, fault=kw.pop("fault", self.canonical_fault),
            event=event, detail=detail, level=self.level, seed=self.ctx.seed_hex,
            scheduled_ms=kw.pop("scheduled_ms", self.scheduled_ms),
            actual_ms=kw.pop("actual_ms", self.actual_ms), **kw)

    def _phase(self) -> dict:
        fire = utc_now_ms()
        start = self.ctx.load_start_ms
        end = self.ctx.load_end_ms
        outside = None
        if isinstance(start, int) and isinstance(end, int):
            outside = not (start <= fire <= end)
        return {
            "load_start_ms": start,
            "load_end_ms": end,
            "fire_ms": fire,
            "consumers_alive": self.ctx.extra.get("consumers_alive", True),
            "progress_marker": self.ctx.extra.get("progress_marker"),
            "outside_window": outside,
        }

    # -- interface ------------------------------------------------------------------------
    def arm(self) -> "Injector":
        if self.ctx.injector_mode == "sham":
            self._receipt("armed", "sham: injector armed, no action (validation run)",
                          target_role=self.target_role(), result="armed", action="sham-arm")
            self._restored = True
            return self
        self._arm()
        return self

    def fire(self) -> dict:
        """Perform and verify the injection, then write exactly one ``fired`` receipt."""
        if self.ctx.injector_mode == "sham":
            return {"fired": False, "sham": True}
        self.scheduled_ms = utc_now_ms()
        detail, before, after = self._fire()
        self.actual_ms = utc_now_ms()
        verified, verify_detail = self._verify(before, after)
        if not verified:
            raise InjectionNotVerified(
                f"{self.canonical_fault}: action not verified ({verify_detail}); "
                f"no fired receipt written")
        phase = self._phase()
        self._fired = True
        receipt = self._receipt(
            "fired", detail, target_role=self.target_role(), target_id=self.state.get("target_id"),
            before=before, after=after, action=self.action_name(),
            action_verified=True, phase=phase, result="fired",
            notes=(None if phase.get("outside_window") in (None, False)
                   else "fire outside the load window: uninformative_fire_outside_window"),
        )
        self._after_fire()
        return {"fired": True, "receipt": receipt, "phase": phase, "verified": verify_detail}

    def wait_for_app_fired(self, timeout_s: float = 180.0) -> dict:
        """Wait for the app-owned ``fired`` receipt (used by failpoint watchers)."""
        raise NotImplementedError

    def restore(self) -> dict:
        if self.ctx.injector_mode == "sham":
            receipt = self._receipt("restored", "sham: disarmed, system untouched",
                                    target_role=self.target_role(), result="restored",
                                    action="sham-disarm")
            return {"restored": True, "sham": True, "receipt": receipt}
        if not self._fired:
            self._receipt("restored", "injection never fired; nothing to restore",
                          target_role=self.target_role(), result="restored",
                          action="no-op-restore")
            self._restored = True
            return {"restored": True, "noop": True}
        if self._restored:
            return {"restored": True, "already": True}
        detail, after = self._restore()
        receipt = self._receipt("restored", detail, target_role=self.target_role(),
                                after=after, result="restored", action=self.restore_action())
        self._restored = True
        return {"restored": True, "receipt": receipt}

    # -- subclass hooks -------------------------------------------------------------------
    def target_role(self) -> str:
        return "unknown"

    def action_name(self) -> str:
        return self.fault

    def restore_action(self) -> str:
        return "restore"

    def _arm(self) -> None:
        raise NotImplementedError

    def _fire(self) -> tuple[str, dict | None, dict | None]:
        raise NotImplementedError

    def _verify(self, before, after) -> tuple[bool, dict]:
        return True, {"note": "no explicit verification for this action"}

    def _after_fire(self) -> None:
        return None

    def _restore(self) -> tuple[str, dict | None]:
        raise NotImplementedError


def _canonical_fault(fault: str) -> str:
    token = str(fault or "").strip()
    upper = token.upper()
    for prefix in ("F", "K"):
        if upper.startswith(prefix) and upper not in ("",):
            # F4a/F4b keep their letter suffix
            if upper in ("F0", "F1", "F2", "F3", "F4A", "F4B", "F5", "F6"):
                return upper.replace("F4A", "F4a").replace("F4B", "F4b")
            if upper in ("K1", "K2", "K3", "K4", "K5", "K6", "K7"):
                return upper
    return token


# --------------------------------------------------------------------------------------------
# Linux/docker substrate helpers (S3's experiments/stack.py API; no edits to stack.py here)
# --------------------------------------------------------------------------------------------
#: Logical proxy groups the Kafka toxics must target (S3 ships deploy/docker/proxy-groups.json;
#: ``STACK.proxy_groups()`` returns it, e.g. {"kafka-produce": ["kafka-1","kafka-2","kafka-3"],
#: "kafka-consume": [...], "db": ["db"]}).  When unavailable we fall back to the legacy single
#: proxy name (native Windows path, unchanged).
PROXY_GROUP_CANDIDATES = {
    "kafka-produce": ("kafka-produce", "produce", "kafka_produce", "kafka-produce-group"),
    "kafka-consume": ("kafka-consume", "consume", "kafka_consume", "kafka-consume-group"),
    "kafka": ("kafka", "kafka-group"),
    "db": ("db", "database", "postgres", "db-group"),
}


def _stack_module():
    """S3's stack adapter, imported lazily (absent on the native Windows path)."""
    try:
        from experiments import stack as _stack
        return _stack
    except Exception:  # noqa: BLE001
        return None


def _docker_mode() -> bool:
    if os.environ.get("WXEO_STACK_MODE") == "docker":
        return True
    stack = _stack_module()
    try:
        return bool(stack is not None and getattr(stack, "is_docker", lambda: False)())
    except Exception:  # noqa: BLE001
        return False


def _proxy_members(kind: str, fallback: str, ctx) -> list[str]:
    """All proxies of a logical group (docker) or the single legacy proxy (native)."""
    explicit = ctx.extra.get(f"{kind}_group")
    if explicit:
        return list(explicit)
    stack = _stack_module()
    if stack is not None and hasattr(stack, "proxy_groups"):
        try:
            groups = stack.proxy_groups() or {}
            for candidate in PROXY_GROUP_CANDIDATES.get(kind, (kind,)):
                members = groups.get(candidate)
                if members:
                    return list(members)
        except Exception:  # noqa: BLE001
            pass
    return [ctx.extra.get(f"{kind.replace('-', '_')}_proxy", fallback)]


def _apply_group_toxic(toxiproxy, members: list[str], toxic: dict,
                       ctx) -> dict:
    """Apply a toxic to every group member; fail closed if ANY member fails (S5 §11.5)."""
    results = {}
    for proxy in members:
        try:
            toxiproxy.add_toxic(proxy, toxic)
            results[proxy] = {"ok": True}
        except Exception as exc:  # noqa: BLE001
            results[proxy] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:200]}
    failed = [p for p, r in results.items() if not r["ok"]]
    ctx.extra["toxic_group_results"] = results
    if failed:
        raise InjectionNotVerified(
            f"toxic {toxic.get('name')} could not be applied to group members {failed}; "
            f"per-proxy results: {results}")
    return results


def _set_group_enabled(toxiproxy, members: list[str], enabled: bool, ctx) -> dict:
    results = {}
    for proxy in members:
        try:
            toxiproxy.set_enabled(proxy, enabled)
            results[proxy] = {"ok": True}
        except Exception as exc:  # noqa: BLE001
            results[proxy] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:200]}
    ctx.extra["proxy_group_results"] = results
    failed = [p for p, r in results.items() if not r["ok"]]
    if failed:
        raise InjectionNotVerified(
            f"could not set enabled={enabled} on group members {failed}: {results}")
    return results


def _group_all_healthy(toxiproxy, members: list[str]) -> bool:
    return all(toxiproxy.wait_healthy(proxy, timeout_s=30) for proxy in members)


# --------------------------------------------------------------------------------------------
# F1 — broker killed mid-produce (verified dead; restore waits brokers=3 and ISR=3)
# --------------------------------------------------------------------------------------------
class F1BrokerKill(Injector):
    fault = "F1"
    description = "kill one Kafka broker process mid-produce; restart it; wait brokers=3, ISR=3"

    def target_role(self) -> str:
        return "broker"

    def action_name(self) -> str:
        return "taskkill /F (broker)"

    def _arm(self) -> None:
        if _docker_mode():
            # S3 stack.py API: broker ids 1..3 (container mode has no Windows stack-state pids)
            broker_id = self.ctx.extra.get("broker_id") or self.rng.choice([1, 2, 3])
            self.state.update(target={"id": broker_id, "pid": None},
                              target_id=str(broker_id), mode="docker")
            return
        state = _read_json(self.ctx.stack_state_path) or {}
        brokers = ((state.get("components") or {}).get("kafka") or {}).get("nodes") or []
        if not brokers:
            raise RuntimeError("stack-state.json has no running Kafka nodes; cannot arm F1")
        target = self.rng.choice(brokers)
        self.state.update(target=target, target_id=str(target.get("id")), mode="native")

    def _fire(self):
        target = self.state["target"]
        before = {"node_id": target.get("id"), "pid": target.get("pid"),
                  "broker_count": self._broker_count(),
                  "mode": self.state.get("mode", "native")}
        if self.state.get("mode") == "docker":
            stack = _stack_module()
            if stack is None or not hasattr(stack, "broker_kill"):
                raise InjectionNotVerified(
                    "F1 docker mode requires STACK.broker_kill(broker_id) from "
                    "experiments/stack.py (S3 API); refusing to fall back to taskkill")
            result = stack.broker_kill(int(self.state["target_id"]))
            self.state["stack_result"] = str(result)[:300]
        else:
            pid = int(target["pid"])
            self.ctx.call(["taskkill", "/F", "/PID", str(pid)], check=False)
        self.state["after_kill_count"] = self._broker_count()
        pid_repr = target.get("pid")
        return (f"killed broker node {target.get('id')} pid {pid_repr}", before,
                {"broker_count": self.state["after_kill_count"]})

    def _broker_count(self) -> int:
        if self.ctx.broker_count_probe is not None:
            try:
                return int(self.ctx.broker_count_probe())
            except Exception:  # noqa: BLE001
                pass
        try:
            from experiments.kafka_admin import KafkaAdmin
            return int(KafkaAdmin(self.ctx.bootstrap).broker_count())
        except Exception:  # noqa: BLE001
            return 0

    def _verify(self, before, after):
        base = int(before.get("broker_count") or 3)
        if self.state.get("mode") == "docker":
            reduced = _wait(lambda: self._broker_count() < base, timeout=180)
            return reduced, {"brokers_before": base, "brokers_after": self._broker_count(),
                             "stack_result": self.state.get("stack_result")}
        pid = int(self.state["target"]["pid"])
        dead = _wait(lambda: not _pid_alive(pid), timeout=60)
        reduced = _wait(lambda: self._broker_count() < base, timeout=120)
        return (dead and reduced), {"pid_dead": dead, "brokers_before": base,
                                    "brokers_after": self._broker_count()}

    def _restore(self):
        if self.state.get("mode") == "docker":
            self.ctx.run_log("restore F1: STACK.broker_start(broker_id) + brokers=3 + ISR=3")
            stack = _stack_module()
            if stack is None or not hasattr(stack, "broker_start"):
                raise RuntimeError("F1 docker restore requires STACK.broker_start(broker_id)")
            stack.broker_start(int(self.state["target_id"]))
        else:
            self.ctx.run_log("restore F1: start-kafka (PowerShell host) + brokers=3 + ISR=3")
            import shutil as _shutil
            host = _shutil.which("pwsh") or _shutil.which("powershell") or "powershell"
            self.ctx.call([host, "-NoProfile", "-ExecutionPolicy", "Bypass",
                           "-File", self.ctx.native_stack_ps1, "start-kafka"], check=False)
        ok_count = _wait(lambda: self._broker_count() >= 3, timeout=120)
        ok_isr = True
        source_topic = self.ctx.extra.get("source_topic")
        if source_topic and self.ctx.wait_isr:
            ok_isr = self.ctx.wait_isr(source_topic, 3, timeout=120)
        if not (ok_count and ok_isr):
            raise RuntimeError(f"F1 restore failed: brokers>=3={ok_count} isr3={ok_isr}")
        return (f"broker restarted; brokers=3={ok_count} isr=3={ok_isr}",
                {"brokers": self._broker_count(), "isr_ok": ok_isr})


# --------------------------------------------------------------------------------------------
# F2 — consumer instance killed during processing (real rebalance)
# --------------------------------------------------------------------------------------------
class F2ConsumerKill(Injector):
    fault = "F2"
    description = "kill one consumer JVM; the group rebalances; restart the instance"

    def target_role(self) -> str:
        return "consumer"

    def action_name(self) -> str:
        return "SIGKILL (linux pid)" if os.name != "nt" else "taskkill /F (consumer)"

    def _arm(self) -> None:
        pids = dict(self.ctx.consumer_pids)
        if not pids:
            raise RuntimeError("no consumer pids available; cannot arm F2")
        victim = self.rng.choice(sorted(pids))
        self.state.update(victim=victim, pid=int(pids[victim]), target_id=str(victim))

    def _fire(self):
        victim = self.state["victim"]
        pid = int(self.state["pid"])
        if os.name != "nt":
            stack = _stack_module()
            if stack is not None and hasattr(stack, "pid_kill_linux"):
                stack.pid_kill_linux(pid)      # S3 API (SIGKILL on the container/POSIX pid)
            else:
                os.kill(pid, 9)               # POSIX fallback, same semantics
        else:
            self.ctx.call(["taskkill", "/F", "/PID", str(pid)], check=False)
        return f"killed consumer instance {victim} pid {pid}", {"instance": victim, "pid": pid}, {
            "pid_alive": _pid_alive(pid)}

    def _verify(self, before, after):
        dead = not _pid_alive(int(self.state["pid"]))
        return dead, {"pid_dead": dead, "instance": self.state["victim"]}

    def _restore(self):
        victim = self.state["victim"]
        if self.ctx.restart_service is None:
            raise RuntimeError("runner did not provide restart_service for F2")
        new_pid = self.ctx.restart_service(victim)
        stable = self.ctx.wait_group_stable(timeout=120) if self.ctx.wait_group_stable else True
        if not stable:
            raise RuntimeError("F2 restore: consumer group did not stabilise")
        return (f"consumer instance {victim} restarted pid={new_pid} group_stable={stable}",
                {"instance": victim, "new_pid": new_pid, "group_stable": stable})


# --------------------------------------------------------------------------------------------
# F3 — produce timeout with unknown outcome (timeout toxic on the produce path)
# --------------------------------------------------------------------------------------------
class F3ProduceTimeout(Injector):
    fault = "F3"
    description = "timeout toxic on the produce path so the produce/commit outcome is unknown"
    T_HOLD_S = 3.0

    def __init__(self, ctx: InjectorContext):
        super().__init__(ctx)
        self.toxiproxy = Toxiproxy(ctx.toxiproxy_api)

    def target_role(self) -> str:
        return "app-kafka-produce-link"

    def action_name(self) -> str:
        return "toxiproxy timeout toxic (produce)"

    def _arm(self) -> None:
        members = _proxy_members("kafka-produce",
                                 self.ctx.extra.get("produce_proxy", "kafka-produce"),
                                 self.ctx)
        self.state.update(proxies=members, proxy=members[0],
                          toxic_name=f"wxeo-f3-{self.ctx.run_id}", timeout_ms=5000,
                          target_id=",".join(members))

    def _fire(self):
        toxic = {"name": self.state["toxic_name"], "type": "timeout", "stream": "downstream",
                 "attributes": {"timeout": self.state["timeout_ms"]}}
        results = _apply_group_toxic(self.toxiproxy, self.state["proxies"], toxic, self.ctx)
        return (f"added timeout toxic {self.state['toxic_name']} on group "
                f"{self.state['proxies']} ({len(results)} proxies)",
                {"proxies": self.state["proxies"]},
                {"toxic": toxic, "per_proxy": results})

    def _verify(self, before, after):
        checks = {p: self.toxiproxy.toxic_present(p, self.state["toxic_name"])
                  for p in self.state["proxies"]}
        return all(checks.values()), {"per_proxy_toxic_present": checks}

    def _after_fire(self) -> None:
        time.sleep(self.T_HOLD_S)

    def restore_action(self) -> str:
        return "remove toxic + verify proxy health"

    def _restore(self):
        for proxy in self.state["proxies"]:
            try:
                self.toxiproxy.remove_toxic(proxy, self.state["toxic_name"])
            except ToxiproxyError as exc:  # pragma: no cover
                if "404" not in str(exc):
                    raise
        healthy = _group_all_healthy(self.toxiproxy, self.state["proxies"])
        if not healthy:
            raise RuntimeError("F3 restore: at least one group proxy did not become healthy")
        return (f"removed toxic; group healthy={healthy}",
                {"proxy_healthy": healthy,
                 "per_proxy_toxics": {p: self.toxiproxy.toxics(p)
                                      for p in self.state["proxies"]}})


# --------------------------------------------------------------------------------------------
# F4a — effect durable + offset commit cannot persist (cut app->Kafka consumer link)
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

    def action_name(self) -> str:
        return "toxiproxy disable (consumer link)"

    def _arm(self) -> None:
        members = _proxy_members("kafka-consume",
                                 self.ctx.extra.get("consume_proxy", "kafka-consume"),
                                 self.ctx)
        self.state.update(proxies=members, proxy=members[0],
                          target_id=",".join(members))

    def _fire(self):
        results = _set_group_enabled(self.toxiproxy, self.state["proxies"], False, self.ctx)
        return (f"disabled proxies {self.state['proxies']}", {"enabled": True},
                {"enabled": False, "per_proxy": results})

    def _verify(self, before, after):
        checks = {p: not self.toxiproxy.is_enabled(p) for p in self.state["proxies"]}
        return all(checks.values()), {"per_proxy_disabled": checks}

    def _after_fire(self) -> None:
        time.sleep(self.T_HOLD_S)

    def restore_action(self) -> str:
        return "enable link + verify health + wait group stable"

    def _restore(self):
        _set_group_enabled(self.toxiproxy, self.state["proxies"], True, self.ctx)
        healthy = _group_all_healthy(self.toxiproxy, self.state["proxies"])
        stable = self.ctx.wait_group_stable(timeout=60) if self.ctx.wait_group_stable else True
        if not healthy:
            raise RuntimeError("F4a restore: proxy did not become healthy")
        return (f"re-enabled {self.state['proxy']}; healthy={healthy} group_stable={stable}",
                {"proxy_enabled": True, "proxy_healthy": healthy, "group_stable": stable})


# --------------------------------------------------------------------------------------------
# F5 — network partition app<->DB
# --------------------------------------------------------------------------------------------
class F5DbPartition(Injector):
    fault = "F5"
    description = "blackhole the app->PostgreSQL link, then restore it"
    T_HOLD_S = 3.0

    def __init__(self, ctx: InjectorContext):
        super().__init__(ctx)
        self.toxiproxy = Toxiproxy(ctx.toxiproxy_api)

    def target_role(self) -> str:
        return "app-db-link"

    def action_name(self) -> str:
        return "toxiproxy disable (db link)"

    def _arm(self) -> None:
        self.state.update(proxy=self.ctx.extra.get("db_proxy", "db"),
                          target_id=self.ctx.extra.get("db_proxy", "db"))

    def _fire(self):
        self.toxiproxy.set_enabled(self.state["proxy"], False)
        return (f"disabled db proxy {self.state['proxy']}", {"enabled": True},
                {"enabled": False})

    def _verify(self, before, after):
        disabled = not self.toxiproxy.is_enabled(self.state["proxy"])
        return disabled, {"proxy_enabled": not disabled}

    def _after_fire(self) -> None:
        time.sleep(self.T_HOLD_S)

    def restore_action(self) -> str:
        return "enable db link + wait health"

    def _restore(self):
        self.toxiproxy.set_enabled(self.state["proxy"], True)
        healthy = self.toxiproxy.wait_healthy(self.state["proxy"], timeout_s=30)
        if not healthy:
            raise RuntimeError("F5 restore: db proxy did not become healthy")
        return (f"db link re-enabled; healthy={healthy}",
                {"proxy_enabled": True, "proxy_healthy": healthy})


# --------------------------------------------------------------------------------------------
# F6 — duplicate redelivery after rebalance (protocol-level replay; D5)
# --------------------------------------------------------------------------------------------
class F6DuplicateRedelivery(Injector):
    fault = "F6"
    description = ("stop consumers, rewind the mechanism's own resume point by k batches, "
                   "restart -> duplicate redelivery of already-processed records (D5, "
                   "protocol-level only; n/a for M1-kafka)")

    def target_role(self) -> str:
        return "consumer-progress"

    def action_name(self) -> str:
        return "k-batch progress rewind (write-capable admin role)"

    def _arm(self) -> None:
        if self.ctx.mechanism == "M1-kafka":
            raise RuntimeError("F6 is n/a for M1-kafka (D5): external rewind would duplicate the "
                               "positive control's own ledger; the matrix never emits this cell")
        self.state["k_batches"] = int(self.ctx.extra.get("rewind_batches", 2))
        self.state["target_id"] = self.ctx.mechanism

    def _fire(self):
        if self.ctx.stop_consumers is None or self.ctx.rewind_progress is None:
            raise RuntimeError("runner did not provide stop_consumers/rewind_progress for F6")
        self.ctx.stop_consumers()
        before = {"next_offsets_or_progress": self._progress_snapshot()}
        rewind = self.ctx.rewind_progress(int(self.state["k_batches"]))
        after = self._progress_snapshot()
        self.state["rewind"] = rewind
        return (f"consumers stopped; resume point rewound by {self.state['k_batches']} batches "
                f"({int(self.state['k_batches']) * MAX_POLL_RECORDS} events)", before, after)

    def _progress_snapshot(self):
        if self.ctx.progress_probe is None:
            return None
        try:
            return int(self.ctx.progress_probe())
        except Exception:  # noqa: BLE001
            return None

    def _verify(self, before, after):
        ok = bool(self.state.get("rewind", {}).get("moved"))
        return ok, {"rewind": self.state.get("rewind"), "after": after}

    def _restore(self):
        if self.ctx.start_consumers is not None:
            self.ctx.start_consumers()
        stable = self.ctx.wait_group_stable(timeout=120) if self.ctx.wait_group_stable else True
        if not stable:
            raise RuntimeError("F6 restore: consumer group did not stabilise")
        return f"consumers restarted; group_stable={stable}", {"group_stable": stable}


# --------------------------------------------------------------------------------------------
# K2 — unknown outcome (C3/C9): timeout toxic on the effect path after a progress marker
# --------------------------------------------------------------------------------------------
class K2UnknownOutcome(Injector):
    fault = "K2"
    level = "L1"
    description = ("unknown-outcome injection: at a seeded committed-progress marker, a Toxiproxy "
                   "timeout toxic makes the in-flight effect commit time out with an unknown "
                   "outcome (DB link for DB sinks, produce/transaction link for M1-kafka)")
    T_HOLD_S = 2.0

    def __init__(self, ctx: InjectorContext):
        super().__init__(ctx)
        self.toxiproxy = Toxiproxy(ctx.toxiproxy_api)

    def target_role(self) -> str:
        return "app-effect-commit-link"

    def action_name(self) -> str:
        return "toxiproxy timeout toxic (effect commit)"

    def _arm(self) -> None:
        if self.ctx.mechanism == "M1-kafka":
            members = _proxy_members("kafka-produce",
                                     self.ctx.extra.get("produce_proxy", "kafka-produce"),
                                     self.ctx)
        else:
            members = _proxy_members("db", self.ctx.extra.get("db_proxy", "db"), self.ctx)
        events = int(self.ctx.extra.get("load_events") or 1000)
        lo, hi = int(events * 0.25), int(events * 0.60)
        marker_rng = random.Random(int.from_bytes(
            hashlib.sha256(f"k2|{self.ctx.seed_hex}".encode()).digest()[:8], "big"))
        marker = marker_rng.randint(max(1, lo), max(lo + 1, hi))
        self.state.update(proxies=members, proxy=members[0],
                          toxic_name=f"wxeo-k2-{self.ctx.run_id}",
                          marker=marker, timeout_ms=5000, target_id=",".join(members))
        self.ctx.extra["progress_marker"] = marker

    def _wait_for_marker(self) -> None:
        if self.ctx.progress_probe is None:
            raise RuntimeError("runner did not provide progress_probe for K2")
        deadline = time.monotonic() + float(self.ctx.extra.get("marker_timeout_s", 180))
        while time.monotonic() < deadline:
            progress = int(self.ctx.progress_probe())
            if progress >= int(self.state["marker"]):
                self.ctx.extra["progress_at_fire"] = progress
                return
            time.sleep(0.05)
        raise InjectionNotVerified(
            f"K2 progress marker {self.state['marker']} never reached; no unknown-outcome "
            f"state created")

    def _fire(self):
        self._wait_for_marker()
        toxic = {"name": self.state["toxic_name"], "type": "timeout", "stream": "downstream",
                 "attributes": {"timeout": self.state["timeout_ms"]}}
        results = _apply_group_toxic(self.toxiproxy, self.state["proxies"], toxic, self.ctx)
        before = {"marker": self.state["marker"],
                  "progress_at_fire": self.ctx.extra.get("progress_at_fire")}
        return (f"added timeout toxic {self.state['toxic_name']} on group "
                f"{self.state['proxies']} at progress {before['progress_at_fire']} "
                f"(marker {self.state['marker']})",
                before, {"toxic": toxic, "per_proxy": results})

    def _verify(self, before, after):
        checks = {p: self.toxiproxy.toxic_present(p, self.state["toxic_name"])
                  for p in self.state["proxies"]}
        return all(checks.values()), {"per_proxy_toxic_present": checks,
                                      "marker": self.state["marker"],
                                      "progress_at_fire": before.get("progress_at_fire")}

    def _after_fire(self) -> None:
        time.sleep(self.T_HOLD_S)

    def restore_action(self) -> str:
        return "remove timeout toxic + verify proxy health"

    def _restore(self):
        for proxy in self.state["proxies"]:
            try:
                self.toxiproxy.remove_toxic(proxy, self.state["toxic_name"])
            except ToxiproxyError as exc:  # pragma: no cover
                if "404" not in str(exc):
                    raise
        healthy = _group_all_healthy(self.toxiproxy, self.state["proxies"])
        if not healthy:
            raise RuntimeError("K2 restore: at least one group proxy did not become healthy")
        return (f"removed K2 toxic; proxy healthy={healthy}",
                {"proxy_healthy": healthy})


# --------------------------------------------------------------------------------------------
# K5 — duplicate delivery without a crash (protocol-level replay, C6)
# --------------------------------------------------------------------------------------------
class K5DuplicateDelivery(Injector):
    fault = "K5"
    level = "L1"
    description = ("duplicate delivery without a crash (C6): stop consumers, rewind the "
                   "mechanism's own resume point by k batches, restart")

    def target_role(self) -> str:
        return "consumer-progress"

    def action_name(self) -> str:
        return "k-batch progress rewind (no crash)"

    def _arm(self) -> None:
        if self.ctx.mechanism == "M1-kafka":
            raise RuntimeError("K5 is n/a for M1-kafka (no external rewind, D5)")
        self.state["k_batches"] = int(self.ctx.extra.get("rewind_batches", 2))
        self.state["target_id"] = self.ctx.mechanism

    def _wait_marker(self) -> int:
        if self.ctx.progress_probe is None:
            raise RuntimeError("runner did not provide progress_probe for K5")
        events = int(self.ctx.extra.get("load_events") or 1000)
        k = max(1, int(self.state.get("k_batches", 2)))
        marker = min(k * MAX_POLL_RECORDS, max(1, events - 1))
        self.state["k_effective"] = k if marker == k * MAX_POLL_RECORDS else 1
        self.state["progress_marker"] = marker
        self.ctx.extra["progress_marker"] = marker
        deadline = time.monotonic() + float(self.ctx.extra.get("marker_timeout_s", 300))
        while time.monotonic() < deadline:
            progress = int(self.ctx.progress_probe())
            if progress >= marker:
                self.ctx.extra["progress_at_fire"] = progress
                return progress
            time.sleep(0.05)
        raise InjectionNotVerified(
            f"K5 progress marker {marker} never reached; rewind would move 0 offsets")

    def _fire(self):
        if self.ctx.stop_consumers is None or self.ctx.rewind_progress is None:
            raise RuntimeError("runner did not provide stop_consumers/rewind_progress for K5")
        progress = self._wait_marker()
        self.ctx.stop_consumers()
        before = {"progress": self._snapshot(), "marker": progress,
                  "progress_at_fire": self.ctx.extra.get("progress_at_fire")}
        k = int(self.state.get("k_effective", self.state["k_batches"]))
        rewind = self.ctx.rewind_progress(k)
        self.state["rewind"] = rewind
        after = {"progress": self._snapshot()}
        return (f"consumers stopped; rewound {k} batches after durable marker {progress}",
                before, after)

    def _snapshot(self):
        if self.ctx.progress_probe is None:
            return None
        try:
            return int(self.ctx.progress_probe())
        except Exception:  # noqa: BLE001
            return None

    def _verify(self, before, after):
        return bool(self.state.get("rewind", {}).get("moved")), {
            "rewind": self.state.get("rewind")}

    def _restore(self):
        if self.ctx.start_consumers is not None:
            self.ctx.start_consumers()
        stable = self.ctx.wait_group_stable(timeout=120) if self.ctx.wait_group_stable else True
        if not stable:
            raise RuntimeError("K5 restore: consumer group did not stabilise")
        return f"consumers restarted; group_stable={stable}", {"group_stable": stable}


# --------------------------------------------------------------------------------------------
# app-failpoint watcher (K1/K3/K4/K7 and F4b)
# --------------------------------------------------------------------------------------------
FAILPOINT_ROLE_MAP = {
    # M1-kafka has no separate effect hook; only progress hooks exist (D4 role instantiation)
    "M1-kafka": M1KAFKA_FAILPOINT,
}


def failpoint_config_for_run(ctx: InjectorContext) -> dict | None:
    """Service failpoint args for a run, or None when no in-process failpoint is armed."""
    fault = _canonical_fault(ctx.fault)
    if ctx.injector_mode in ("none", "fixture"):
        return None
    if fault in ("F0", "F1", "F2", "F3", "F4a", "F5", "F6"):
        return None  # external S4 injections (F4b handled below before this branch)
    if fault == "F4b":
        return {"enabled": True, "name": "before_effect_commit",
                "action": ctx.extra.get("failpoint_action", "crash"),
                "after": int(ctx.extra.get("failpoint_after", 1))}
    if fault in S4_REALISED_CLASSES:
        return None
    if fault in STRUCTURALLY_UNREACHABLE:
        raise RuntimeError(f"{fault} is declared structurally unreachable: "
                           f"{STRUCTURALLY_UNREACHABLE[fault]}")
    role_map = FAILPOINT_ROLE_MAP.get(ctx.mechanism, CRASH_CLASS_FAILPOINT)
    spec = role_map.get(fault)
    if spec is None:
        raise RuntimeError(f"no failpoint realisation for {ctx.mechanism}/{fault}")
    return {"enabled": True, "name": spec["name"], "action": spec["action"],
            "after": int(ctx.extra.get("failpoint_after", 1))}


class FailpointWatch(Injector):
    """App-owned failpoint: wait for the S3 ``fired`` receipt, verify the action, restore."""

    level = "L1"
    owns_receipts = False

    def __init__(self, ctx: InjectorContext):
        super().__init__(ctx)
        self.fault = _canonical_fault(ctx.fault)

    def target_role(self) -> str:
        return "app-failpoint"

    def action_name(self) -> str:
        cfg = self.state.get("config") or {}
        return f"failpoint {cfg.get('name')} ({cfg.get('action')})"

    def _arm(self) -> None:
        self.state["config"] = failpoint_config_for_run(self.ctx)
        if not self.state["config"] or not self.state["config"].get("enabled"):
            raise RuntimeError(f"no in-process failpoint available for "
                               f"{self.ctx.mechanism}/{self.fault}")

    def _fire(self):  # pragma: no cover - overridden by wait_for_app_fired
        raise NotImplementedError("app-owned failpoints are driven by the service receipt")

    def wait_for_app_fired(self, timeout_s: float = 180.0) -> dict:
        name = self.state["config"]["name"]
        expected_action = self.state["config"]["action"]
        deadline = time.monotonic() + timeout_s
        found = None
        while time.monotonic() < deadline:
            for receipt in read_receipts(self.ctx.faults_log):
                if (receipt.get("run_id") == self.ctx.run_id
                        and receipt.get("fault") == name
                        and receipt.get("event") == "fired"
                        and RCPT.classify(receipt) == "app"):
                    found = receipt
                    break
            if found:
                break
            time.sleep(0.1)
        if not found:
            raise InjectionNotVerified(
                f"app failpoint {name} did not fire within {timeout_s}s (no service receipt); "
                f"run is uninformative")
        phase = self._phase()
        verified = self._verify_app_action(expected_action)
        if not verified:
            raise InjectionNotVerified(
                f"app failpoint {name} fired but action {expected_action!r} not verified")
        self._fired = True
        self.actual_ms = utc_now_ms()
        # R2 fix: the APPLICATION is the sole writer of the `fired` receipt for app-owned
        # failpoints (S3's Failpoints/FaultsLog, frozen).  S4 records only its own verification
        # in the run notes and writes the `restored` receipt (crash action) after restart.
        self.state["app_fired_receipt"] = found
        self.state["phase"] = phase
        self.state["action"] = expected_action
        return {"fired": True, "app_receipt": found, "phase": phase}

    def _verify_app_action(self, action: str) -> bool:
        if action in TERMINAL_ACTIONS:
            pid = self.ctx.consumer_pids.get(1)
            if pid is not None:
                return _wait(lambda: not _pid_alive(int(pid)), timeout=60)
            return True
        return True

    def restore(self) -> dict:
        if not self._fired:
            self.receipts.append(run_id=self.ctx.run_id, fault=self.fault, event="restored",
                                 source="s4", level=self.level, seed=self.ctx.seed_hex,
                                 detail="injection never fired; nothing to restore",
                                 target_role="app-failpoint", result="restored",
                                 action="no-op-restore")
            self._restored = True
            return {"restored": True, "noop": True}
        if self._restored:
            return {"restored": True, "already": True}
        action = self.state.get("action") or self.state.get("config", {}).get("action")
        if action in TERMINAL_ACTIONS:
            # crash failpoint: the app halted (fired-only); S4 owns the restored receipt and
            # restarts the instance(s)
            stable = True
            if self.ctx.restart_service is not None:
                for instance in (1, 2):
                    self.ctx.restart_service(instance)
            if self.ctx.wait_group_stable:
                stable = self.ctx.wait_group_stable(timeout=180)
            if not stable:
                raise RuntimeError("failpoint restore: group did not stabilise")
            receipt = self.receipts.append(
                run_id=self.ctx.run_id, fault=self.fault, event="restored", source="s4",
                level=self.level, seed=self.ctx.seed_hex,
                detail=f"instance restarted after terminal crash failpoint; "
                       f"group_stable={stable}",
                target_role="app-failpoint", result="restored", action="restart+stable")
            self._restored = True
            return {"restored": True, "receipt": receipt}
        # non-terminal (pause/throw/abort_txn/none): the app wrote its own restored receipt;
        # S4 must not duplicate it. Wait for the app receipt and record nothing more.
        name = self.state.get("config", {}).get("name")
        deadline = time.monotonic() + float(self.ctx.extra.get("app_restore_timeout_s", 60))
        while time.monotonic() < deadline:
            for receipt in read_receipts(self.ctx.faults_log):
                if (receipt.get("run_id") == self.ctx.run_id
                        and receipt.get("fault") == name
                        and receipt.get("event") == "restored"
                        and RCPT.classify(receipt) == "app"):
                    self._restored = True
                    return {"restored": True, "app_receipt": receipt}
            time.sleep(0.1)
        raise RuntimeError(
            f"app-owned non-terminal failpoint {name} did not write its restored receipt")


# --------------------------------------------------------------------------------------------
# factory
# --------------------------------------------------------------------------------------------
INJECTOR_TYPES = {
    "F1": F1BrokerKill,
    "F2": F2ConsumerKill,
    "F3": F3ProduceTimeout,
    "F4a": F4aOffsetCommitFailure,
    "F5": F5DbPartition,
    "F6": F6DuplicateRedelivery,
    "K2": K2UnknownOutcome,
    "K5": K5DuplicateDelivery,
}


def make_injector(ctx: InjectorContext) -> Injector | None:
    """Create the injector for a run (app failpoint watcher, S4 injector, sham, or None)."""
    if ctx.injector_mode in ("none", "fixture") or _canonical_fault(ctx.fault) in ("F0", "none"):
        return None
    fault = _canonical_fault(ctx.fault)
    if ctx.injector_mode == "sham":
        cls = INJECTOR_TYPES.get(fault)
        if cls is None:
            inst = Injector(ctx)
            inst.canonical_fault = fault
            return inst
        return cls(ctx)
    if fault == "F4b":
        # ordering probe: app-owned failpoint (ordering=early + before_effect_commit crash)
        return FailpointWatch(ctx)
    # R1 fix: live external faults (F1-F6) and the S4-realised Level-1 classes (K2/K5) dispatch
    # to their external injector classes; injected_mode "live"/"selftest"/"external" all map
    # here (the matrix declares K2/K5 as "external" and F1-F6 as "live").
    if fault in INJECTOR_TYPES and ctx.injector_mode in ("live", "selftest", "external"):
        return INJECTOR_TYPES[fault](ctx)
    if fault in S4_REALISED_CLASSES:
        return INJECTOR_TYPES[fault](ctx)
    if fault in CRASH_CLASS_FAILPOINT:
        return FailpointWatch(ctx)
    raise ValueError(f"no injector for fault {ctx.fault!r} (mode={ctx.injector_mode!r})")


def seeded_progress_marker(seed_hex: str, events: int,
                           window_fraction: tuple[float, float] = (0.25, 0.75)) -> int:
    lo, hi = window_fraction
    rng = random.Random(int.from_bytes(
        hashlib.sha256(f"progress|{seed_hex}".encode()).digest()[:8], "big"))
    marker = int(int(events) * rng.uniform(lo, hi))
    return max(1, min(int(events) - 1, marker))


def level2_trigger_time(ctx: InjectorContext, *, load_duration_s: float,
                        window_fraction: tuple[float, float] = (0.25, 0.75)) -> float:
    """Seeded Level-2 fire time (seconds after load start) inside the processing window."""
    lo, hi = window_fraction
    rng = random.Random(int.from_bytes(
        hashlib.sha256(f"trigger|{ctx.seed_hex}".encode()).digest()[:8], "big"))
    return load_duration_s * rng.uniform(lo, hi)


def seeded_failpoint_after(ctx: InjectorContext, *, expected_hits: int = 200,
                           window_fraction: tuple[float, float] = (0.25, 0.75)) -> int:
    """Seeded ``--wxeo.failpoint.after=N`` for a stochastic (Level-2) app failpoint."""
    lo = max(1, int(expected_hits * window_fraction[0]))
    hi = max(lo + 1, int(expected_hits * window_fraction[1]))
    rng = random.Random(int.from_bytes(
        hashlib.sha256(f"failpoint-after|{ctx.seed_hex}".encode()).digest()[:8], "big"))
    return rng.randint(lo, hi)


# --------------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------------
def _read_json(path: str):
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
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


def _wait(predicate: Callable[[], bool], timeout: float, interval: float = 0.2) -> bool:
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
    "FAILPOINT_NAMES",
    "CRASH_CLASS_FAILPOINT",
    "S4_REALISED_CLASSES",
    "STRUCTURALLY_UNREACHABLE",
    "M1KAFKA_FAILPOINT",
    "InjectionNotVerified",
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
    "F5DbPartition",
    "F6DuplicateRedelivery",
    "K2UnknownOutcome",
    "K5DuplicateDelivery",
    "FailpointWatch",
    "failpoint_config_for_run",
    "make_injector",
    "level2_trigger_time",
    "seeded_failpoint_after",
    "utc_now_iso",
    "utc_now_ms",
]
