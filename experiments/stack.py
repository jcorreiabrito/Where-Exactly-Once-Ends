"""Campaign stack adapter: the approved substrate is now a Linux/container host.

Two realisations are supported and recorded in every run record:

* ``native`` -- the original Windows process stack managed by ``artifact/harness/native-stack.ps1``
  (kept for reproducibility of the Windows evidence and for the frozen pilot rows);
* ``docker`` -- the container stack in ``deploy/docker/docker-compose.yml`` (the approved substrate
  for the full campaign).  A reset is ``docker compose down -v`` + ``up -d --wait``: it recreates
  broker log dirs and the PostgreSQL data volume, which is the clean equivalent of the Windows
  ``reset-kafka`` verb and removes the M5 no-topic-deletion constraint's storage effect.

Fault-routing API (S4 injectors consume this; stable signatures, no docker needed for the pure
parts):

* :func:`container_name` / :func:`broker_container_name` -- deterministic container names
  (``wxeo-campaign-kafka-1`` ... );
* :func:`proxy_groups` -- logical fault groups read from ``deploy/docker/proxy-groups.json``;
* :func:`proxy_host_port` -- host port a client must connect to for a named Toxiproxy proxy;
* :func:`broker_kill` / :func:`broker_start` / :func:`broker_restart` -- container lifecycle for
  F1, returning a record dict with the executed command, rc and output tail;
* :func:`pid_kill_linux` -- SIGKILL a PID (F2 realisation, ``os.kill``).

The Kafka topology advertises ``127.0.0.1:19091..19093`` (the three per-broker Toxiproxy proxies),
so a host client that bootstraps through any endpoint still sends all subsequent traffic through
Toxiproxy.  See ``docs/LINUX-CAMPAIGN-RUNBOOK.md`` section 3 and ``deploy/docker/proxy-groups.json``.

Environment:

* ``WXEO_STACK_MODE=native|docker`` (default: docker when ``/proc`` exists and the compose file is
  present, else native only if ``native-stack.ps1`` exists -- i.e. the Windows host keeps working
  unchanged);
* ``WXEO_COMPOSE_FILE`` optional override for the compose file path;
* ``WXEO_PROXY_GROUPS_FILE`` optional override for the logical fault-group map;
* ``EOS_RUNTIME_ROOT`` scratch root (defaults to ``~/eos-runtime`` on POSIX, ``%LOCALAPPDATA%`` on
  Windows);
* ``WXEO_DISABLE_PERIODIC_RESET=1`` keeps the mid-campaign reset disabled (pilot only, deviation
  M11; confirmatory campaigns keep the automatic M5 cadence, which on docker is cheap and safe).
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_COMPOSE = os.path.join(REPO, "deploy", "docker", "docker-compose.yml")
DEFAULT_TOXIPROXY = os.path.join(REPO, "deploy", "docker", "toxiproxy.json")
DEFAULT_PROXY_GROUPS = os.path.join(REPO, "deploy", "docker", "proxy-groups.json")

COMPOSE_PROJECT = "wxeo-campaign"
CONTAINER_PREFIX = COMPOSE_PROJECT + "-"
SERVICES = ("kafka-1", "kafka-2", "kafka-3", "postgres", "toxiproxy")


def compose_file() -> str:
    return os.environ.get("WXEO_COMPOSE_FILE") or DEFAULT_COMPOSE


def toxiproxy_config_file() -> str:
    return os.environ.get("WXEO_TOXIPROXY_FILE") or DEFAULT_TOXIPROXY


def proxy_groups_file() -> str:
    return os.environ.get("WXEO_PROXY_GROUPS_FILE") or DEFAULT_PROXY_GROUPS


def _native_harness_present() -> bool:
    return os.path.exists(os.path.join(REPO, "artifact", "harness", "native-stack.ps1"))


def stack_mode() -> str:
    """Return ``docker`` or ``native`` (explicit env wins; otherwise infer from the host)."""
    explicit = (os.environ.get("WXEO_STACK_MODE") or "").strip().lower()
    if explicit in ("docker", "native"):
        return explicit
    if os.name != "nt" and os.path.exists(compose_file()):
        return "docker"
    if os.name == "nt" and _native_harness_present():
        return "native"
    if os.path.exists(compose_file()):
        return "docker"
    return "native"


def is_docker() -> bool:
    return stack_mode() == "docker"


def docker_available() -> bool:
    return shutil.which("docker") is not None and os.path.exists(compose_file())


def require_docker() -> None:
    """Fail closed with an actionable message when the container substrate is unavailable."""
    if shutil.which("docker") is None:
        raise RuntimeError(
            "docker CLI not found: install docker engine + compose v2 and add the user to the "
            "docker group (see docs/LINUX-CAMPAIGN-RUNBOOK.md section 1)")
    if not os.path.exists(compose_file()):
        raise RuntimeError("compose file not found: %s" % compose_file())


def _compose(args: list[str], timeout: float = 900.0) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "-f", compose_file(), "-p", COMPOSE_PROJECT, *args]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def compose_up(timeout: float = 900.0) -> subprocess.CompletedProcess:
    """Start the stack and wait for every healthcheck (``--wait``)."""
    return _compose(["up", "-d", "--wait", "--remove-orphans"], timeout=timeout)


def compose_down(timeout: float = 600.0) -> subprocess.CompletedProcess:
    return _compose(["down", "-v", "--remove-orphans", "--timeout", "60"], timeout=timeout)


def compose_status() -> str:
    proc = _compose(["ps", "--format", "{{.Service}} {{.State}} {{.Status}}"], timeout=120)
    return (proc.stdout or proc.stderr or "").strip()


def docker_reset() -> dict:
    """Wipe and recreate the container stack; returns the recorded command results."""
    down = compose_down()
    up = compose_up()
    return {
        "action": "docker-compose-reset",
        "compose_file": "deploy/docker/docker-compose.yml",
        "rc_down": int(down.returncode or 0),
        "rc_up": int(up.returncode or 0),
        "down_tail": (down.stdout or down.stderr or "")[-400:],
        "up_tail": (up.stdout or up.stderr or "")[-400:],
        "status": compose_status(),
    }


# ---------------------------------------------------------------------------------------------
# Fault-routing helpers (S4-facing API; pure parts never touch docker)
# ---------------------------------------------------------------------------------------------

def container_name(service: str) -> str:
    """Deterministic compose container name for a service (pinned in docker-compose.yml)."""
    if service not in SERVICES:
        raise ValueError("unknown stack service %r (expected one of %s)" % (service, ", ".join(SERVICES)))
    return CONTAINER_PREFIX + service


def broker_container_name(instance: int) -> str:
    """Container name for Kafka broker ``instance`` (1..3)."""
    if isinstance(instance, bool) or not isinstance(instance, int) or not 1 <= instance <= 3:
        raise ValueError("broker instance must be an int in 1..3, got %r" % (instance,))
    return container_name("kafka-%d" % instance)


def broker_container_names() -> list[str]:
    return [broker_container_name(i) for i in (1, 2, 3)]


def _tail(text: str | None, limit: int = 600) -> str:
    return (text or "").strip()[-limit:]


def _record(action: str, cmd: list[str], proc: subprocess.CompletedProcess,
            container: str | None = None) -> dict:
    """Uniform injector-receipt record: action, exact command, rc, output tails."""
    record = {
        "action": action,
        "cmd": list(cmd),
        "rc": int(proc.returncode or 0),
        "ok": int(proc.returncode or 0) == 0,
        "stdout_tail": _tail(proc.stdout),
        "stderr_tail": _tail(proc.stderr),
    }
    if container is not None:
        record["container"] = container
    return record


def _docker(args: list[str], action: str, timeout: float = 120.0,
            container: str | None = None) -> dict:
    require_docker()
    cmd = ["docker", *args]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return _record(action, cmd, proc, container=container)


def broker_kill(instance: int) -> dict:
    """F1 realisation: ``docker kill`` one broker container (no graceful shutdown)."""
    container = broker_container_name(instance)
    record = _docker(["kill", container], action="broker-kill", container=container)
    record["instance"] = instance
    return record


def broker_start(instance: int) -> dict:
    """F1 restoration: ``docker start`` the killed broker container."""
    container = broker_container_name(instance)
    record = _docker(["start", container], action="broker-start", container=container)
    record["instance"] = instance
    return record


def broker_restart(instance: int) -> dict:
    """``docker restart`` one broker container (clean broker JVM restart)."""
    container = broker_container_name(instance)
    record = _docker(["restart", container], action="broker-restart", timeout=180.0,
                     container=container)
    record["instance"] = instance
    return record


def container_status(service: str) -> dict:
    """Current container state (``docker inspect``); used by F1/F2 verification."""
    container = container_name(service)
    record = _docker(["inspect", "-f", "{{.State.Status}}", container],
                     action="container-status", container=container)
    record["service"] = service
    record["status"] = record["stdout_tail"].strip()
    return record


def pid_kill_linux(pid: int) -> bool:
    """F2 realisation: SIGKILL a PID on Linux. Returns True iff the process was signalled.

    Returns False when the process does not exist (already dead); raises RuntimeError on a
    permission problem and on non-POSIX hosts (the Linux campaign host only).
    """
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 1:
        raise ValueError("refusing to kill pid %r (must be an int > 1)" % (pid,))
    if os.name != "posix":
        raise RuntimeError("pid_kill_linux is only available on the Linux campaign host")
    try:
        os.kill(pid, signal.SIGKILL)
        return True
    except ProcessLookupError:
        return False
    except PermissionError as exc:  # pragma: no cover - host-specific
        raise RuntimeError("permission denied killing pid %d: %s" % (pid, exc)) from exc


def _load_json(path: str, what: str):
    if not os.path.exists(path):
        raise FileNotFoundError("%s not found: %s" % (what, path))
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def proxy_groups() -> dict[str, list[str]]:
    """Logical fault groups -> Toxiproxy proxy names (reads deploy/docker/proxy-groups.json).

    Fail closed: a missing/malformed map is an error, never an empty mapping (an injector that
    silently targets zero proxies would attest a fault that never touches the path).
    """
    doc = _load_json(proxy_groups_file(), "proxy-groups.json")
    groups = doc.get("groups") if isinstance(doc, dict) and "groups" in doc else doc
    if not isinstance(groups, dict) or not groups:
        raise ValueError("proxy-groups map must be a non-empty object of name -> [proxy, ...]")
    clean: dict[str, list[str]] = {}
    for name, proxies in groups.items():
        if not isinstance(name, str) or not name:
            raise ValueError("proxy-group names must be non-empty strings")
        if not isinstance(proxies, list) or not proxies:
            raise ValueError("proxy-group %r must list at least one proxy" % (name,))
        if not all(isinstance(p, str) and p for p in proxies):
            raise ValueError("proxy-group %r entries must be non-empty strings" % (name,))
        clean[name] = list(proxies)
    return clean


def proxy_host_port(name: str) -> int:
    """Host port a client must connect to for Toxiproxy proxy ``name``.

    The compose file publishes every proxy listener on the same loopback port (``127.0.0.1:<p>:<p>``);
    the port is read from ``deploy/docker/toxiproxy.json`` so there is exactly one source of truth.
    """
    doc = _load_json(toxiproxy_config_file(), "toxiproxy.json")
    proxies = doc.get("proxies") if isinstance(doc, dict) and "proxies" in doc else doc
    if not isinstance(proxies, list):
        raise ValueError("toxiproxy.json must be an array of proxy objects")
    for proxy in proxies:
        if isinstance(proxy, dict) and proxy.get("name") == name:
            listen = str(proxy.get("listen") or "")
            if ":" not in listen:
                raise ValueError("proxy %r has malformed listen %r" % (name, listen))
            try:
                return int(listen.rsplit(":", 1)[1])
            except ValueError as exc:
                raise ValueError("proxy %r listen port is not numeric: %r" % (name, listen)) from exc
    raise KeyError("unknown Toxiproxy proxy %r" % (name,))


__all__ = [
    "REPO", "compose_file", "toxiproxy_config_file", "proxy_groups_file",
    "stack_mode", "is_docker", "docker_available", "require_docker",
    "compose_up", "compose_down", "compose_status", "docker_reset",
    "COMPOSE_PROJECT", "CONTAINER_PREFIX", "SERVICES",
    "container_name", "broker_container_name", "broker_container_names", "container_status",
    "broker_kill", "broker_start", "broker_restart", "pid_kill_linux",
    "proxy_groups", "proxy_host_port",
]


def _main(argv: list[str] | None = None) -> int:
    import sys

    args = list(argv if argv is not None else sys.argv[1:])
    action = args[0] if args else "status"
    if action == "status":
        print(json_status())
        return 0
    if action == "up":
        proc = compose_up()
    elif action == "down":
        proc = compose_down()
    elif action == "reset":
        result = docker_reset()
        print(json.dumps(result, indent=2))
        return 0 if result["rc_down"] == 0 and result["rc_up"] == 0 else 1
    elif action == "proxy-groups":
        print(json.dumps(proxy_groups(), indent=2))
        return 0
    elif action == "proxy-host-port" and len(args) == 2:
        print(proxy_host_port(args[1]))
        return 0
    elif action == "broker-kill" and len(args) == 2:
        print(json.dumps(broker_kill(int(args[1])), indent=2))
        return 0
    elif action == "broker-start" and len(args) == 2:
        print(json.dumps(broker_start(int(args[1])), indent=2))
        return 0
    elif action == "pid-kill" and len(args) == 2:
        print(json.dumps({"pid": int(args[1]), "killed": pid_kill_linux(int(args[1]))}))
        return 0
    else:
        print("usage: python -m experiments.stack "
              "[status|up|down|reset|proxy-groups|proxy-host-port NAME|"
              "broker-kill N|broker-start N|pid-kill PID]", file=sys.stderr)
        return 2
    print((proc.stdout or "")[-2000:])
    print((proc.stderr or "")[-2000:], file=sys.stderr)
    return int(proc.returncode or 0)


def json_status() -> str:
    return json.dumps({"mode": stack_mode(), "compose_file": compose_file(),
                       "docker_available": docker_available(),
                       "status": compose_status()}, indent=2)


if __name__ == "__main__":  # pragma: no cover - host CLI
    raise SystemExit(_main())
