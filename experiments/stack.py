"""Campaign stack adapter: the approved substrate is now a Linux/container host.

Two realisations are supported and recorded in every run record:

* ``native`` -- the original Windows process stack managed by ``artifact/harness/native-stack.ps1``
  (kept for reproducibility of the Windows evidence and for the frozen pilot rows);
* ``docker`` -- the container stack in ``deploy/docker/docker-compose.yml`` (the approved substrate
  for the full campaign).  A reset is ``docker compose down -v`` + ``up -d --wait``: it recreates
  broker log dirs and the PostgreSQL data volume, which is the clean equivalent of the Windows
  ``reset-kafka`` verb and removes the M5 no-topic-deletion constraint's storage effect.

Environment:

* ``WXEO_STACK_MODE=native|docker`` (default: docker when ``/proc`` exists and the compose file is
  present, else native only if ``native-stack.ps1`` exists -- i.e. the Windows host keeps working
  unchanged);
* ``WXEO_COMPOSE_FILE`` optional override for the compose file path;
* ``EOS_RUNTIME_ROOT`` scratch root (defaults to ``~/eos-runtime`` on POSIX, ``%LOCALAPPDATA%`` on
  Windows);
* ``WXEO_DISABLE_PERIODIC_RESET=1`` keeps the mid-campaign reset disabled (pilot only, deviation
  M11; confirmatory campaigns keep the automatic M5 cadence, which on docker is cheap and safe).
"""

from __future__ import annotations

import os
import shutil
import subprocess

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_COMPOSE = os.path.join(REPO, "deploy", "docker", "docker-compose.yml")


def compose_file() -> str:
    return os.environ.get("WXEO_COMPOSE_FILE") or DEFAULT_COMPOSE


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


def _compose(args: list[str], timeout: float = 900.0) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "-f", compose_file(), "-p", "wxeo-campaign", *args]
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


__all__ = ["REPO", "compose_file", "stack_mode", "is_docker", "docker_available",
           "compose_up", "compose_down", "compose_status", "docker_reset"]


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
    else:
        print(f"usage: python -m experiments.stack [status|up|down|reset]", file=sys.stderr)
        return 2
    print((proc.stdout or "")[-2000:])
    print((proc.stderr or "")[-2000:], file=sys.stderr)
    return int(proc.returncode or 0)


def json_status() -> str:
    import json
    return json.dumps({"mode": stack_mode(), "compose_file": compose_file(),
                       "docker_available": docker_available(),
                       "status": compose_status()}, indent=2)


if __name__ == "__main__":  # pragma: no cover - host CLI
    raise SystemExit(_main())
