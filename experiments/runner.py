#!/usr/bin/env python3
"""Experiment runner: exact per-run sequence of docs/prereg.md §4.1 (S4-owned, post-S5 v2).

Two modes
---------
``--dry-run --fixture DIR``
    Executes the complete orchestration path against a synthetic fixture bundle: matrix row ->
    oracle -> run record -> artefact archive + digest manifest -> RUNLOG entry.  No
    Kafka/PostgreSQL/Toxiproxy is touched.  Records are schema-valid but labelled ``DRY-RUN`` in
    ``validity.notes`` and written outside ``results/raw/``.

``--stage pilot|A|B|C|M7cal|M7sat`` (live)
    Real native stack, exactly as frozen.  Post-S5 fix-pass changes:

    * **Calibration stages** (A5/A6): ``M7cal`` executes the M7 core + M7spot runs at max (T1a);
      ``M7sat`` executes the 9 A6 saturation-validation runs at exactly the candidate
      ``rates.json`` 2.5x-max rate (T1b; fail-closed if the candidate table/key is missing).

    * **Fresh state with assertions** (F-RN2): topics/groups/schema are created fresh and their
      absence is *asserted* before the run; readiness requires ``assigned`` status and zero lag
      for 5 s; residue checks cover schemas, topics, groups and PG sessions.
    * **Receipts v2** (F-I1/I2/I3): single-owner receipt protocol (``experiments/receipts.py``);
      an injection only counts as fired after its action is verified, with a phase-window record.
    * **Quiescence via AdminClient** (F-G2): real per-partition lag + sink-count stability for
      T_quiet=10 s, PG idle-in-transaction check, M2 relay drain, and a publication-stream
      definition for M1-kafka (read-committed LSO stable).  Censored runs are outcomes.
    * **Observation after stop** (F-G2): consumers are stopped gracefully *before* the dumps and
      the final read-only snapshot.
    * **Always a record** (F-I4/F-RN1): every reserved run attempt writes a schema-valid
      ``wxeo.run.v1`` record; invalid attempts are marked excluded with a closed-list reason, and
      invalid runs are rerun up to R_max = 3 with the same seeds.  Attempts are logged.
    * **Performance pipeline** (F-M1): throughput from committed events in the steady-state window,
      warm-up exclusion, generator-lag covariate, p50/p99/p99.9 from the per-instance
      HdrHistogram percentile files (merged), time-to-consistent from the quiescence probe.
    * **Path hygiene** (lead P0): committed artefacts carry repo-relative paths or
      ``<scratch>`` placeholders; the record validator rejects absolute local paths.

Usage examples::

    python experiments/runner.py --dry-run --fixture results/dryrun/fixtures/<run> \
        --stage pilot --limit 2 --results-dir results/dryrun
    python experiments/runner.py --stage pilot --rate max
    python experiments/runner.py --check-env
    python experiments/runner.py --write-deviations
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from glob import glob

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from experiments import validate_record as VR  # noqa: E402
from experiments import injectors as INJ  # noqa: E402
from experiments import kafka_admin as KA  # noqa: E402
from experiments import matrix_design as MD  # noqa: E402
from experiments import pathsafe  # noqa: E402
from experiments import receipts as RCPT  # noqa: E402
from experiments import stack as STACK  # noqa: E402
from experiments.fixtures import make_fixtures as FX  # noqa: E402
from experiments.oracle import derive as D  # noqa: E402
from experiments.oracle import oracle as O  # noqa: E402

QUIET_PERIOD_MS = 10_000          # prereg §4.6 / method §7.3 (T_quiet)

# ---- Windows-KRaft topic lifecycle (lead directive M5) --------------------------------------
# Topic deletion is FATAL on this Windows KRaft setup ("Error while renaming dir for <topic>" ->
# KafkaStorageException -> brokers shut down).  Therefore:
#  * the runner NEVER deletes topics; fresh state comes from unique attempt-scoped topic names,
#    a fresh consumer group and a fresh schema;
#  * storage is reclaimed by periodic full-stack `reset-kafka` (wipe + reformat), never per run;
#  * the reset cadence depends on the run load and is recorded in results/DEVIATIONS.md (M5).
RESET_CADENCE_BY_LOAD = {1_000: 50, 10_000: 25, 100_000: 20, 1_000_000: 1}
RESET_CADENCE_SATURATION = 20
MAX_LIVE_TOPICS = 400             # hard topic-count cap: abort -> reset-kafka
BROKER_REQUIRED_PROPERTIES = {
    "delete.topic.enable": "false",
    "log.retention.check.interval.ms": "3600000",
}


def expected_effect_transactions(load_events: int, consumer_instances: int = 1,
                                 max_poll_records: int | None = None) -> int:
    """Number of app effect transactions a run can produce (failpoint trigger geometry).

    The app failpoint counter advances once per effect transaction, i.e. once per polled batch of
    ``MAX_POLL_RECORDS`` events per consumer instance -- not once per event. Seeded thresholds must
    be scaled by this number or they can never fire (the M16/F4b 300 s timeout).
    """
    batch = int(max_poll_records or INJ.MAX_POLL_RECORDS)
    per_instance = max(1, int(load_events) // max(1, int(consumer_instances) * batch))
    return per_instance


def reset_cadence_for(load_events: int, load_mode: str = "fixed-rate") -> int:
    """Runs between full-stack `reset-kafka` wipes for a run type (lead M5 directive)."""
    if load_mode == "saturation":
        return RESET_CADENCE_SATURATION
    return int(RESET_CADENCE_BY_LOAD.get(int(load_events), 50))


def should_periodic_reset(runs_since_reset: int, load_events: int,
                          load_mode: str = "fixed-rate",
                          live_topics: int = 0) -> tuple[bool, str | None]:
    """Pure cadence decision: (should_reset, reason). reason in {cadence, topic_cap}."""
    if live_topics > MAX_LIVE_TOPICS:
        return True, "topic_cap"
    cadence = reset_cadence_for(load_events, load_mode)
    # cadence == 1 means "reset before every run" (1M block, M5)
    if cadence <= 1 or int(runs_since_reset) >= cadence:
        return True, "cadence"
    return False, None


def check_broker_properties(text: str) -> dict:
    """Verify the broker safety properties required by the M5 directive are in effect."""
    values: dict[str, str | None] = {k: None for k in BROKER_REQUIRED_PROPERTIES}
    for line in (text or "").splitlines():
        line = line.strip()
        for key in BROKER_REQUIRED_PROPERTIES:
            if line.startswith(key + "="):
                values[key] = line.split("=", 1)[1].strip()
    missing = [k for k, v in values.items() if v is None]
    wrong = [f"{k}={values[k]!r} (required {BROKER_REQUIRED_PROPERTIES[k]!r})"
             for k in BROKER_REQUIRED_PROPERTIES
             if values[k] is not None and values[k] != BROKER_REQUIRED_PROPERTIES[k]]
    return {"ok": not missing and not wrong, "values": values, "missing": missing,
            "wrong": wrong}


def broker_property_files() -> list[str]:
    """Broker server.properties files (persist on disk even when Kafka is stopped)."""
    root = os.path.join(eos_runtime_root(), "run", "kafka")
    return sorted(glob(os.path.join(root, "node*", "server.properties")))
QUIESCE_TIMEOUT_MS = 300_000      # T_max = 300 s
READY_STABLE_MS = 5_000           # readiness: group stable, lag 0 for 5 s (prereg §4.1 step 3)
FREE_SPACE_FLOOR_GB = 3.0         # prereg §9.3 (G8)
RESTART_EVERY_N_RUNS = 50         # D6
RERUN_CAP = 3                     # prereg §8.3 (R_max): total attempts per reserved run
FULL_STACK_RESTART_VERB = "restart-all"
PHASE_MARGIN_MS = 2_000           # fire must be inside [load_start+δ, load_end−δ] (F-I3)

#: closed exclusion-reason list (prereg §8.2). Anything else is an internal bug, not an exclusion.
EXCLUDE_RECEIPTS = "fault_receipts_invalid"
EXCLUDE_NOT_FIRED = "fault_not_fired"
EXCLUDE_SOURCE = "source_integrity_failed"
EXCLUDE_RESIDUE = "residue_check_failed"
EXCLUDE_CLOCK = "clock_step"
EXCLUDE_DISK = "disk_below_floor"
EXCLUDE_CONFIG = "config_mismatch"
EXCLUDE_START = "application_did_not_start"
EXCLUDE_OUTCOME = "outcome_obscured_by_invalidity"
EXCLUDE_STACK = "environment_stack_down"
EXCLUDE_WINDOW = "uninformative_fire_outside_window"

CLOSED_EXCLUSIONS = {
    EXCLUDE_RECEIPTS, EXCLUDE_NOT_FIRED, EXCLUDE_SOURCE, EXCLUDE_RESIDUE, EXCLUDE_CLOCK,
    EXCLUDE_DISK, EXCLUDE_CONFIG, EXCLUDE_START, EXCLUDE_OUTCOME, EXCLUDE_WINDOW,
    EXCLUDE_STACK,
}


# --------------------------------------------------------------------------------------------
# environment/paths
# --------------------------------------------------------------------------------------------
def repo_root() -> str:
    return _REPO


def eos_runtime_root() -> str:
    return os.environ.get("EOS_RUNTIME_ROOT") or os.path.join(
        os.environ.get("LOCALAPPDATA", os.path.expanduser("~")), "eos-runtime")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: datetime | None = None) -> str:
    return (dt or utc_now()).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def utc_ms(dt: datetime | None = None) -> int:
    return int((dt or utc_now()).timestamp() * 1000)


def read_json(path: str, default=None):
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return default


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------------------------
# endpoint contract (S5 B7 / A6): deploy/docker/endpoints.json is the single shared source of
# truth for the runner, the S4 injectors and the Linux fault-efficacy proof.  The Kafka fault
# paths (F3, F4a and K2 for M1-kafka) must use the contract's group-member Toxiproxy bootstrap
# (``kafka.fault_bootstrap``); the old bootstrap-only alias on 19094 is removed.  Any missing or
# malformed contract fails closed before a fault run can touch the stack.
# --------------------------------------------------------------------------------------------
ENDPOINTS_CONTRACT_PATH = os.path.join(_REPO, "deploy", "docker", "endpoints.json")
ENDPOINTS_SCHEMA = "wxeo.endpoints.v1"
KAFKA_FAULT_BOOTSTRAP_KEY = "kafka.fault_bootstrap"
#: faults whose Kafka client paths must traverse the injected Toxiproxy group
KAFKA_FAULT_PATH_FAULTS = frozenset({"F3", "F4a"})


def endpoints_contract_path() -> str:
    """Path of the endpoint contract (``WXEO_ENDPOINTS_FILE`` override for tests/ops)."""
    return os.environ.get("WXEO_ENDPOINTS_FILE") or ENDPOINTS_CONTRACT_PATH


def load_endpoints_contract(path: str | None = None) -> dict:
    """Load ``deploy/docker/endpoints.json`` (S5 B7); fail closed on missing/malformed input.

    Returns ``{path, sha256, fault_bootstrap, doc}`` where ``path`` is repo-relative (path
    hygiene: run records must never carry absolute host paths).
    """
    ep_path = path or endpoints_contract_path()
    if not os.path.exists(ep_path):
        raise RuntimeError(
            f"endpoint contract missing: {ep_path} (S5 B7); the Kafka fault paths cannot be "
            f"routed through a group-member Toxiproxy bootstrap -- refusing to run")
    try:
        with open(ep_path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"endpoint contract unreadable: {ep_path}: {exc}") from exc
    if not isinstance(doc, dict) or doc.get("schema") != ENDPOINTS_SCHEMA:
        raise RuntimeError(
            f"endpoint contract must be a {ENDPOINTS_SCHEMA} object: {ep_path}")
    kafka = doc.get("kafka")
    if not isinstance(kafka, dict):
        raise RuntimeError(f"endpoint contract has no 'kafka' object: {ep_path}")
    fault_bootstrap = kafka.get("fault_bootstrap")
    if not isinstance(fault_bootstrap, str) or not fault_bootstrap.strip():
        raise RuntimeError(
            f"endpoint contract has no {KAFKA_FAULT_BOOTSTRAP_KEY!r} string: {ep_path} "
            f"(S5 B7 fail-closed)")
    fault_bootstrap = fault_bootstrap.strip()
    members = kafka.get("group_member_proxies")
    if not isinstance(members, list) or fault_bootstrap not in members:
        raise RuntimeError(
            f"endpoint contract {KAFKA_FAULT_BOOTSTRAP_KEY}={fault_bootstrap!r} is not a member "
            f"of kafka.group_member_proxies={members!r} (B7: the bootstrap connection itself "
            f"must be inside the injected proxy group)")
    return {"path": pathsafe.repo_rel(ep_path), "sha256": sha256_file(ep_path),
            "fault_bootstrap": fault_bootstrap, "doc": doc}


def run_block(run: dict) -> str:
    """Calibration block token of a run-order row (A5/A6 stage split, S5-B7 follow-up).

    ``matrix.yaml`` cells carry the ``block`` token; ``run_order.csv`` is the frozen runner plan
    and does not add a column for it, so the block is recovered from the frozen run-id prefix
    (``m7sat-`` = A6 T1b, ``m7sopt-`` = A5 T1a spot checks, ``m7-`` = A5 T1a core block).
    """
    block = str(run.get("block") or "")
    if block:
        return block
    run_id = str(run.get("run_id") or "")
    if run_id.startswith("m7sat-"):
        return "M7sat"
    if run_id.startswith("m7sopt-"):
        return "M7spot"
    if run_id.startswith("m7-"):
        return "M7-cal"
    return ""


def is_kafka_fault_path(run: dict) -> bool:
    """True for the runs whose Kafka client path must go through the fault Toxiproxy group."""
    fault = str(run.get("fault") or "")
    if fault in KAFKA_FAULT_PATH_FAULTS:
        return True
    return fault == "K2" and str(run.get("mechanism") or "") == "M1-kafka"


def endpoints_for_run(run: dict, fault_bootstrap: str | None) -> dict:
    """Service endpoints for a run: direct by default, proxied only for the fault link.

    B7/A6: the Kafka fault paths (F3 producer, F4a consumer, K2 for M1-kafka producer) use the
    contract's ``kafka.fault_bootstrap`` (a group-member Toxiproxy proxy); non-fault/native paths
    keep the pre-registered direct endpoints (bootstrap 127.0.0.1:9091, DB 5433).
    """
    ep = {"bootstrap": "127.0.0.1:9091",
          "producer": None, "consumer": None,
          "db_url": "jdbc:postgresql://127.0.0.1:5433/wxeo"}
    if is_kafka_fault_path(run):
        if not fault_bootstrap:
            raise RuntimeError(
                "Kafka fault path requires the endpoint-contract group-member fault bootstrap "
                "(S5 B7); refusing to route a fault through a direct/bootstrap alias")
        fault = str(run.get("fault") or "")
        if fault == "F4a":
            ep["consumer"] = fault_bootstrap
        else:  # F3 producer path, K2 unknown-outcome producer path for M1-kafka
            ep["producer"] = fault_bootstrap
    elif str(run.get("fault") or "") == "F5":
        ep["db_url"] = "jdbc:postgresql://127.0.0.1:15433/wxeo"
    elif str(run.get("fault") or "") == "K2":
        # K2 for DB-sink mechanisms: DB-link proxy (unchanged, A4 fault realisation)
        ep["db_url"] = "jdbc:postgresql://127.0.0.1:15433/wxeo"
    return ep


def free_space_gb(path: str) -> float:
    try:
        target = path
        while not os.path.exists(target):
            parent = os.path.dirname(target)
            if parent == target:
                break
            target = parent
        return round(shutil.disk_usage(target).free / (1 << 30), 3)
    except OSError:
        return -1.0


def git_sha() -> str:
    """Read the commit id without invoking git (constraint: do not run git)."""
    head_path = os.path.join(_REPO, ".git", "HEAD")
    try:
        head = open(head_path, encoding="utf-8").read().strip()
    except OSError:
        return "0000000"
    if head.startswith("ref: "):
        ref = head[5:].strip()
        ref_path = os.path.join(_REPO, ".git", *ref.split("/"))
        if os.path.exists(ref_path):
            return open(ref_path, encoding="utf-8").read().strip()
        packed = os.path.join(_REPO, ".git", "packed-refs")
        if os.path.exists(packed):
            for line in open(packed, encoding="utf-8"):
                parts = line.split()
                if len(parts) == 2 and parts[1] == ref:
                    return parts[0]
        return "0000000"
    return head if re.fullmatch(r"[0-9a-f]{40}", head) else "0000000"


def host_block() -> dict:
    ram = None
    try:
        if os.name == "nt":
            import ctypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong),
                            ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong),
                            ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong),
                            ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]

            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            ram = round(stat.ullTotalPhys / (1 << 30), 1)
        else:
            meminfo = "/proc/meminfo"
            if os.path.exists(meminfo):
                with open(meminfo, encoding="utf-8") as fh:
                    for line in fh:
                        if line.startswith("MemTotal:"):
                            ram = round(int(line.split()[1]) / (1024 * 1024), 1)
                            break
    except Exception:  # noqa: BLE001 - best effort host record
        ram = None
    cpu = os.environ.get("PROCESSOR_IDENTIFIER", platform.processor() or "unknown")
    if cpu in ("", "unknown") and os.path.exists("/proc/cpuinfo"):
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as fh:
                for line in fh:
                    if line.lower().startswith("model name"):
                        cpu = line.split(":", 1)[1].strip()
                        break
        except OSError:
            pass
    return {
        "os_build": f"{platform.system()} {platform.release()} ({platform.version()})",
        "cpu_model": cpu,
        "logical_cpus": os.cpu_count(),
        "ram_gb": ram,
        "disk_free_start_gb": free_space_gb(eos_runtime_root()),
    }


def version_block() -> dict:
    versions = {
        "java": None, "kafka": None, "postgresql": None, "toxiproxy": None,
        "powershell": None, "python": sys.version.split()[0],
        "python_executable": pathsafe.repo_rel(sys.executable),
        "pinned_toolchain": None,
        "sha256": {},
    }
    pinned = os.path.join(_REPO, "tools", "TOOLCHAIN-VERSIONS.txt")
    if os.path.exists(pinned):
        text = open(pinned, encoding="utf-8").read()
        versions["pinned_toolchain"] = "tools/TOOLCHAIN-VERSIONS.txt"
        for line in text.splitlines():
            m = re.match(r"(\w+)=(.+)", line.strip())
            if m:
                versions[m.group(1)] = m.group(2)
            m2 = re.match(r"([\w.-]+\.(?:zip|tgz|exe))\s+sha256=([0-9A-Fa-f]+)", line.strip())
            if m2:
                versions["sha256"][m2.group(1)] = m2.group(2).lower()
    java_exe = discover_java()
    if java_exe:
        try:
            out = subprocess.run([java_exe, "-version"], capture_output=True, text=True,
                                 timeout=30)
            versions["java_runtime"] = (out.stderr or out.stdout).strip().splitlines()[0]
        except Exception:  # noqa: BLE001
            pass
    return versions


def discover_java() -> str | None:
    env = os.environ.get("JAVA_HOME")
    if env:
        cand = os.path.join(env, "bin", "java.exe" if os.name == "nt" else "java")
        if os.path.exists(cand):
            return cand
    vendor = os.path.join(eos_runtime_root(), "vendor")
    for pattern in ("jdk*/bin/java.exe", "jdk*/bin/java", "**/jdk*/bin/java.exe"):
        hits = glob(os.path.join(vendor, pattern), recursive=True)
        if hits:
            return hits[0]
    return shutil.which("java")


def discover_tool(patterns: list[str]) -> str | None:
    vendor = os.path.join(eos_runtime_root(), "vendor")
    for pattern in patterns:
        hits = sorted(glob(os.path.join(vendor, "**", pattern), recursive=True))
        if hits:
            return hits[0]
    # Container/Linux hosts use system tools (postgresql-client, openjdk) from PATH instead of the
    # vendored Windows runtime; the frozen runner must not require the vendor tree there.
    for pattern in patterns:
        name = pattern.replace("\\", "/").split("/")[-1].replace(".exe", "")
        if name and not any(ch in name for ch in "*?["):
            found = shutil.which(name) or shutil.which(name + ".exe")
            if found:
                return found
    return None


def discover_pwsh() -> str:
    return shutil.which("pwsh") or shutil.which("powershell") or "pwsh"


def clock_sanity_probe() -> dict:
    """Real clock/host sanity probe (F-RN3): detect wall-clock steps and multiple boots.

    Compares the wall-clock delta with the monotonic delta across the call and reads the OS boot
    time.  A clock step during a run (|wall_delta - monotonic_delta| > tolerance) invalidates the
    run (prereg §8.2 item 4).
    """
    wall0 = time.time()
    mono0 = time.monotonic()
    time.sleep(0.05)
    wall1 = time.time()
    mono1 = time.monotonic()
    drift_ms = abs((wall1 - wall0) - (mono1 - mono0)) * 1000.0
    boot = None
    try:
        if os.name == "nt":
            proc = subprocess.run(
                [discover_pwsh(), "-NoProfile", "-Command",
                 "(Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime()"
                 ".ToString('o')"],
                capture_output=True, text=True, timeout=30)
            boot = (proc.stdout or "").strip() or None
        elif os.path.exists("/proc/stat"):
            with open("/proc/stat", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("btime "):
                        boot = datetime.fromtimestamp(
                            int(line.split()[1]), tz=timezone.utc).isoformat().replace(
                                "+00:00", "Z")
                        break
    except Exception:  # noqa: BLE001
        boot = None
    return {"wall_utc": utc_iso(), "monotonic_s": mono1, "drift_ms": round(drift_ms, 3),
            "boot_utc": boot, "ok": drift_ms <= 1000.0}


# --------------------------------------------------------------------------------------------
# RUNLOG
# --------------------------------------------------------------------------------------------
RUNLOG_HEADER = """# RUNLOG — every experiment command with provenance

Protocol: docs/BRIEF.md §8. One row per experiment-affecting command: UTC timestamp, git SHA,
host, stage/run, seed(s), exact command, output paths, exit status. Rows are append-only; this
file (and `results/faults.log`) are the provenance chain for every number in the paper.

| UTC | git SHA | host | stage/run | seed | exit | command | outputs |
|---|---|---|---|---|---|---|---|
"""


class RunLog:
    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            with open(self.path, "w", encoding="utf-8", newline="") as fh:
                fh.write(RUNLOG_HEADER)
        self.sha = git_sha()
        self.host = host_block()

    def append(self, *, stage: str, run_id: str, seed: str, command: str,
               exit_status: int | str, outputs: str = "", notes: str = "") -> None:
        def esc(text: str) -> str:
            # path hygiene: commands contain absolute vendor paths; committed log must not
            return (pathsafe.scrub_string(text or "")).replace("|", "\\|").replace("\n", " ")[:600]

        row = (f"| {utc_iso()} | `{self.sha[:12]}` | {esc(self.host['os_build'])} "
               f"| {esc(stage)}/{esc(run_id)} | `{seed}` | {exit_status} "
               f"| `{esc(command)}` | {esc(outputs)} |")
        if notes:
            row = row[:-1] + f" {esc(notes)} |"
        with open(self.path, "a", encoding="utf-8", newline="") as fh:
            fh.write(row + "\n")
            fh.flush()
            os.fsync(fh.fileno())


# --------------------------------------------------------------------------------------------
# runner
# --------------------------------------------------------------------------------------------
@dataclass
class RunnerOptions:
    stage: str = "pilot"
    limit: int | None = None
    run_id: str | None = None
    dry_run: bool = False
    fixture: str | None = None
    results_dir: str = os.path.join(_REPO, "results")
    scratch_dir: str = ""
    rate: str = "max"
    resume: bool = True
    quiet_period_ms: int = QUIET_PERIOD_MS
    quiesce_timeout_ms: int = QUIESCE_TIMEOUT_MS
    rate_table: str = os.path.join(_REPO, "experiments", "rates.json")
    failpoint_action: str = "crash"
    runlog_path: str | None = None
    warmup_events: int | None = None
    verify_freeze: bool = True


class Runner:
    def __init__(self, opts: RunnerOptions):
        self.opts = opts
        self.repo = repo_root()
        self.matrix_path = os.path.join(self.repo, "experiments", "matrix.yaml")
        self.order_path = os.path.join(self.repo, "experiments", "run_order.csv")
        self.results_dir = os.path.abspath(opts.results_dir)
        self.scratch_root = opts.scratch_dir or os.path.join(eos_runtime_root(), "runs")
        runlog_path = opts.runlog_path or os.path.join(self.results_dir, "RUNLOG.md")
        self.runlog = RunLog(runlog_path)
        self.faults_log = os.path.join(self.results_dir, "faults.log")
        self.raw_dir = os.path.join(self.results_dir, "raw")
        self.records_dir = os.path.join(self.raw_dir, "run-records")
        self.oracle_dir = os.path.join(self.raw_dir, "oracle")
        self.artifacts_dir = os.path.join(self.raw_dir, "artifacts")
        self.summaries_dir = os.path.join(self.raw_dir, "summaries")
        for d in (self.records_dir, self.oracle_dir, self.artifacts_dir, self.summaries_dir):
            os.makedirs(d, exist_ok=True)
        self.plan = self._load_plan()
        self._started_iso = utc_iso()
        self._commands: list[dict] = []
        self._restart_epoch = 0
        self._reset_epoch = 0
        self._block_id = 0
        self._runs_since_reset = 0
        self._last_reset: dict | None = None
        # M5: topics/schemas are NEVER deleted, so every runner invocation gets a unique
        # session tag; combined with the attempt number this guarantees fresh names even when
        # earlier smoke attempts left topics behind (reclaimed by periodic reset-kafka).
        self._session_tag = hashlib.sha256(
            (utc_iso() + str(os.getpid())).encode()).hexdigest()[:8]
        self._reserved_written = False

    # -- plan -----------------------------------------------------------------------------
    def _load_plan(self) -> list[dict]:
        if not os.path.exists(self.order_path):
            raise SystemExit("experiments/run_order.csv missing; run experiments/matrix.py first")
        rows = []
        with open(self.order_path, "r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                rows.append(row)
        return rows

    def selected_runs(self) -> list[dict]:
        runs = self.plan
        if self.opts.run_id:
            runs = [r for r in runs if r["run_id"] == self.opts.run_id]
            if not runs:
                raise SystemExit(f"run_id {self.opts.run_id!r} not found in run_order.csv")
        else:
            if self.opts.stage == "pilot":
                runs = [r for r in runs if r["stage"] == "pilot"]
            elif self.opts.stage == "M7cal":
                # Amendment A5 (r2) T1a: M7 core + M7spot only (54 + 54 = 108 runs).  The A6
                # M7sat saturation-validation block is T1b and must NOT run before the candidate
                # rates.json exists (--stage M7sat).
                runs = [r for r in runs if r.get("stage") == "m7"
                        and run_block(r) in ("M7-cal", "M7spot")]
            elif self.opts.stage == "M7sat":
                # Amendment A6 T1b: the 9 post-rates saturation-validation runs (100k/P6,
                # one per mechanism), offered exactly the candidate 2.5x-max saturation rate.
                runs = [r for r in runs if r.get("stage") == "m7"
                        and run_block(r) == "M7sat"]
            elif self.opts.stage in ("A", "B", "C"):
                runs = [r for r in runs if r["stage_label"] == self.opts.stage]
            elif self.opts.stage != "all":
                raise SystemExit(f"unknown stage {self.opts.stage!r}")
            if self.opts.limit:
                runs = runs[: self.opts.limit]
        return runs

    # -- command execution + logging -------------------------------------------------------
    def call(self, command: list[str], *, stage: str, run_id: str, seed: str,
             purpose: str = "", check: bool = False, timeout: float | None = None,
             cwd: str | None = None, outputs: str = "") -> subprocess.CompletedProcess:
        printable = " ".join(command)
        self.runlog.append(stage=stage, run_id=run_id, seed=seed,
                           command=f"[{purpose}] {printable}", exit_status="RUNNING")
        try:
            proc = subprocess.run(command, capture_output=True, text=True, timeout=timeout,
                                  cwd=cwd)
            rc = proc.returncode
        except subprocess.TimeoutExpired:
            proc = subprocess.CompletedProcess(command, 124, "", "timeout")
            rc = 124
        except FileNotFoundError as exc:
            proc = subprocess.CompletedProcess(command, 127, "", str(exc))
            rc = 127
        self._commands.append({"command": pathsafe.scrub_string(printable), "purpose": purpose,
                               "exit": rc, "when": utc_iso()})
        self.runlog.append(stage=stage, run_id=run_id, seed=seed,
                           command=f"[{purpose}] exit={rc}", exit_status=rc, outputs=outputs,
                           notes=(proc.stderr or "")[:200])
        if check and rc != 0:
            raise RuntimeError(f"command failed ({rc}): {pathsafe.scrub_string(printable)}\n"
                               f"{(proc.stderr or '')[-1000:]}")
        return proc

    # -- preflight -------------------------------------------------------------------------
    def preflight(self, *, require_broker_configs: bool = False) -> dict:
        free = free_space_gb(eos_runtime_root())
        if free < FREE_SPACE_FLOOR_GB:
            raise SystemExit(f"free-space floor violated: {free:.2f} GB < "
                             f"{FREE_SPACE_FLOOR_GB} GB (prereg §9.3); aborting")
        freeze_report = self.verify_freeze() if self.opts.verify_freeze else {"ok": None}
        if self.opts.verify_freeze and not freeze_report.get("ok"):
            raise SystemExit("ANALYSIS FREEZE DRIFT: refusing to run (prereg §10.3). "
                             f"Run experiments/freeze.py: {freeze_report}")
        interface = self.check_env()
        clock = clock_sanity_probe()
        if not clock["ok"]:
            raise SystemExit(f"clock sanity check failed: {clock}")
        broker_configs = None
        if require_broker_configs:
            broker_configs = self.verify_broker_safety()
        self._write_reserved_summary()
        return {"free_gb": free, "interface": interface, "host": host_block(),
                "versions": version_block(), "git_sha": self.runlog.sha,
                "freeze": freeze_report, "clock": clock,
                "broker_safety": broker_configs}

    def verify_broker_safety(self) -> dict:
        """M5 requirement: `delete.topic.enable=false` + `log.retention.check.interval.ms=3600000`.

        Topic deletion is fatal on this Windows KRaft setup (S3 acceptance-run evidence); the
        runner refuses to start if the broker safety properties are not in effect.
        """
        files = broker_property_files()
        # Lead R3-robustness: harness start/reset/start-toxiproxy can be clobbered by a
        # concurrent state write; poll up to 30 s for the 3 configs AND an all-running
        # stack-state before refusing (still fail loudly if it never converges).
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            files = broker_property_files()
            try:
                state = read_json(os.path.join(eos_runtime_root(), "run", "stack-state.json"),
                                  {})
                comps = state.get("components") or {}
                all_running = all(
                    (comps.get(name) or {}).get("status") == "running"
                    for name in ("kafka", "postgres", "toxiproxy"))
            except Exception:  # noqa: BLE001
                all_running = False
            if len(files) >= 3 and all_running:
                break
            time.sleep(1.0)
        if len(files) < 3:
            raise SystemExit(
                f"BROKER SAFETY CHECK FAILED: expected >=3 broker server.properties under "
                f"<scratch>/run/kafka/node*/server.properties, found {len(files)}. The stack "
                f"must be started by its single owner (S3 harness) before a live run.")
        problems = []
        values = {}
        for path in files:
            try:
                text = open(path, "r", encoding="utf-8", errors="replace").read()
            except OSError as exc:
                problems.append(f"{os.path.basename(os.path.dirname(path))}: unreadable ({exc})")
                continue
            result = check_broker_properties(text)
            values[os.path.basename(os.path.dirname(path))] = result["values"]
            if not result["ok"]:
                problems.append(f"{os.path.basename(os.path.dirname(path))}: "
                                f"missing={result['missing']} wrong={result['wrong']}")
        if problems:
            raise SystemExit(
                "BROKER SAFETY CHECK FAILED (M5): topic deletion is fatal on this Windows KRaft "
                "setup, so the broker configuration must contain "
                f"{BROKER_REQUIRED_PROPERTIES}. Not in effect: " + "; ".join(problems)
                + ". S3 must add these to the broker configs; refusing to run (a topic delete "
                  "would be a runner bug).")
        self.runlog.append(stage="preflight", run_id="-", seed="-",
                           command="[preflight] broker safety configs verified "
                                   "(delete.topic.enable=false, retention check 1h)",
                           exit_status=0, outputs="; ".join(
                               pathsafe.repo_rel(f) or "" for f in files))
        return {"ok": True, "files": len(files), "values": values}

    def endpoint_contract(self, *, strict: bool = False) -> dict | None:
        """Endpoint contract summary for ``config.runner.endpoint_contract`` (S5 B7).

        ``strict=True`` fails closed (Kafka fault paths must never run without the contract);
        the record writer uses the non-strict form so a missing contract cannot suppress a
        run record (which would itself be an evidence loss).
        """
        try:
            contract = load_endpoints_contract()
        except RuntimeError:
            if strict:
                raise
            return None
        return {"path": contract["path"], "sha256": contract["sha256"],
                "fault_bootstrap": contract["fault_bootstrap"]}

    def verify_freeze(self) -> dict:
        freeze_path = os.path.join(self.repo, "docs", "prereg-freeze.json")
        if not os.path.exists(freeze_path):
            return {"ok": False, "problem": "docs/prereg-freeze.json missing"}
        frozen = read_json(freeze_path, {})
        drift = []
        for rel, info in (frozen.get("files") or {}).items():
            path = os.path.join(self.repo, rel)
            if not os.path.exists(path):
                drift.append({"file": rel, "problem": "missing"})
                continue
            if sha256_file(path) != info.get("sha256"):
                drift.append({"file": rel, "problem": "hash changed"})
        # artifact binding: a rebuilt jar must be re-frozen before a live run (lead order)
        try:
            from experiments.freeze import artifact_binding
            frozen_artifact = frozen.get("artifact") or {}
            current = artifact_binding()
            if frozen_artifact:
                frozen_sha = (frozen_artifact.get("jar") or {}).get("sha256")
                if frozen_sha != current["jar"]["sha256"]:
                    drift.append({"file": "artifact/target/wxeo-service.jar",
                                  "problem": "jar hash differs from the freeze (rebuilt?)",
                                  "frozen": frozen_sha, "current": current["jar"]["sha256"]})
                if (frozen_artifact.get("source_tree_sha256")
                        != current["source_tree_sha256"]):
                    drift.append({"file": "artifact source tree",
                                  "problem": "artifact sources changed after the freeze"})
            else:
                drift.append({"file": "artifact binding",
                              "problem": "freeze has no artifact block (re-freeze required)"})
            artifact = current["jar"]
        except Exception as exc:  # noqa: BLE001
            drift.append({"file": "artifact binding", "problem": str(exc)[:200]})
            artifact = None
        return {"ok": not drift, "drift": drift, "artifact": artifact,
                "aggregate": frozen.get("aggregate_pipeline_sha256")}

    def _write_reserved_summary(self) -> None:
        path = os.path.join(self.summaries_dir, "reserved-runs.csv")
        if os.path.exists(path) and self._reserved_written:
            return
        keys = list(self.plan[0].keys()) if self.plan else []
        with open(path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=keys)
            writer.writeheader()
            for row in self.plan:
                writer.writerow(row)
        self._reserved_written = True
        self.runlog.append(stage="preflight", run_id="-", seed="-",
                           command="[preflight] froze reserved-run list", exit_status=0,
                           outputs=pathsafe.repo_rel(path))

    def check_env(self) -> dict:
        jar = os.path.join(self.repo, "artifact", "target", "wxeo-service.jar")
        harness = os.path.join(self.repo, "artifact", "harness", "native-stack.ps1")
        stack_state = os.path.join(eos_runtime_root(), "run", "stack-state.json")
        docker = STACK.is_docker()
        kafka_home = discover_tool(["kafka_2.*/libs", "kafka/libs"]) or ""
        if kafka_home:
            kafka_home = os.path.dirname(kafka_home.rstrip("\\/"))
        pg_bin = discover_tool(["pgsql/bin/psql.exe", "**/psql.exe", "psql"])
        toxiproxy = discover_tool(["toxiproxy-server*.exe"])
        if docker:
            return {
                "stack_mode": "docker",
                "service_jar": {"path": "artifact/target/wxeo-service.jar",
                                "present": os.path.exists(jar)},
                "compose": {"path": "deploy/docker/docker-compose.yml",
                            "present": os.path.exists(STACK.compose_file())},
                "docker_cli": {"present": STACK.docker_available()},
                "kafka_home": kafka_home or None,
                "psql": pg_bin,
                "java": discover_java(),
                "pwsh": discover_pwsh(),
                "ready": bool(os.path.exists(jar) and os.path.exists(STACK.compose_file())
                              and STACK.docker_available()),
            }
        return {
            "stack_mode": "native",
            "service_jar": {"path": "artifact/target/wxeo-service.jar",
                            "present": os.path.exists(jar)},
            "native_stack_ps1": {"path": "artifact/harness/native-stack.ps1",
                                 "present": os.path.exists(harness)},
            "stack_state": {"path": "<scratch>/run/stack-state.json",
                            "present": os.path.exists(stack_state)},
            # internal execution paths (raw on purpose); RUNLOG/artifact blocks are scrubbed
            "kafka_home": kafka_home or None,
            "psql": pg_bin,
            "toxiproxy": toxiproxy,
            "java": discover_java(),
            "pwsh": discover_pwsh(),
            "ready": bool(os.path.exists(jar) and os.path.exists(harness)
                          and os.path.exists(stack_state)),
        }

    # -- artefact helpers ------------------------------------------------------------------
    def _archive_artefact(self, run_id: str, src: str | None, name: str,
                          copy: bool = True) -> dict | None:
        if not src or not os.path.exists(src):
            return None
        info = {"sha256": sha256_file(src), "bytes": os.path.getsize(src),
                "path": pathsafe.repo_rel(src), "retained": bool(copy)}
        if not copy:
            return info
        dest_dir = os.path.join(self.artifacts_dir, run_id)
        os.makedirs(dest_dir, exist_ok=True)
        dest = os.path.join(dest_dir, name)
        shutil.copy2(src, dest)
        info["path"] = pathsafe.repo_rel(dest)
        return info

    def _archive_run(self, run_id: str, artefacts: dict[str, str | None],
                     *, retain_map: dict[str, bool] | None = None) -> tuple[dict, dict]:
        retain_map = retain_map or {}
        archived: dict[str, dict | None] = {}
        for name, src in artefacts.items():
            archived[name] = self._archive_artefact(run_id, src, name,
                                                    copy=retain_map.get(name, True))
        manifest = {
            "run_id": run_id,
            "generated_at_utc": utc_iso(),
            "sha256": {k: (v or {}).get("sha256") for k, v in archived.items()},
            "bytes": {k: (v or {}).get("bytes") for k, v in archived.items()},
            "paths": {k: (v or {}).get("path") for k, v in archived.items()},
            "retained": {k: bool((v or {}).get("retained")) for k, v in archived.items()},
            "policy": "full dumps retained for violating/control/sample runs (prereg §5); for "
                      "other runs only digests are kept — bulk data stays in local non-synced "
                      "scratch and is deterministically re-generatable (D10/D11)",
        }
        manifest = pathsafe.scrub(manifest)
        manifest_path = os.path.join(self.artifacts_dir, run_id, "digest-manifest.json")
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        with open(manifest_path, "w", encoding="utf-8", newline="") as fh:
            json.dump(manifest, fh, indent=2)
            fh.write("\n")
        archived_paths = {k: (v or {}).get("path") if v else None for k, v in archived.items()}
        archived_paths["digest_manifest"] = pathsafe.repo_rel(manifest_path)
        return archived_paths, manifest

    # -- records ---------------------------------------------------------------------------
    def _campaign_master_seed(self) -> str:
        try:
            import yaml

            doc = yaml.safe_load(open(self.matrix_path, encoding="utf-8"))
            return doc["seeds"]["master_seed"]
        except Exception:  # noqa: BLE001
            return "0" * 32

    def _base_record(self, run: dict, preflight: dict) -> dict:
        """Common skeleton for both valid and invalid records (F-I4: a record always exists)."""
        run_id = run["run_id"]
        stage_record = MD.record_stage(run["stage_label"])
        try:
            fault_record = MD.record_fault(run["fault"])
        except ValueError:
            fault_record = run["fault"]
        cfg_stub = {
            "mechanism": run["mechanism"],
            "ordering_variant": run["ordering_variant"],
            "load_events": int(run["load_events"]),
            "partitions": int(run["partitions"]),
            "consumer_instances": int(run["consumer_instances"]),
            "load_mode": run.get("load_mode"),
            "note": "config block unavailable (run failed before the app config was written)",
        }
        cfg_stub["config_sha256"] = hashlib.sha256(
            json.dumps(cfg_stub, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return {
            "schema": VR.SCHEMA_VERSION,
            "run_id": run_id,
            "stage": stage_record,
            "cell_id": run["cell_id"],
            "mechanism": run["mechanism"],
            "ordering_variant": run["ordering_variant"],
            "fault": fault_record,
            "fault_level": run["fault_level"],
            "load_events": int(run["load_events"]),
            "partitions": int(run["partitions"]),
            "consumer_instances": int(run["consumer_instances"]),
            "rep": int(run["rep"]),
            "data_seed": run["data_seed"],
            "injection_seed": run["injection_seed"],
            "master_seed": self._campaign_master_seed(),
            "git_sha": self.runlog.sha,
            "host": preflight["host"],
            "versions": preflight["versions"],
            "config": cfg_stub,
            "process_limits": {"note": "run failed before process telemetry was collected"},
            "timings": {k: None for k in VR.TIMING_KEYS},
            "quiescence": {"lag_per_partition": {}, "quiet_period_ms": self.opts.quiet_period_ms,
                           "censored": False, "t_consistent_ms": None},
            "performance": {**{k: None for k in VR.PERF_KEYS},
                            "perf_window": {"note": "no performance sample (invalid run)"}},
            "counts": {k: None for k in VR.COUNTS_KEYS},
            "violations": {"v1": False, "v2": False, "v3": False, "v4": False, "v5": False,
                           "violating_run": False},
            "publication": {"pub_records": None, "dup_records": None, "lost_records": None,
                            "divergence": None, "notes": "invalid run: no oracle result"},
            "oracle": {"gt_sha256": None, "expected_sha256": None, "source_integrity": False,
                       "oracle_script_sha256": None, "result_sha256": None},
            "fault_receipts": [r for r in INJ.read_receipts(self.faults_log)
                               if r.get("run_id") == run_id],
            "validity": {"valid": False, "excluded": True, "exclude_reason": EXCLUDE_OUTCOME,
                         "residue_check": True, "clock_check": True,
                         "disk_free_start_gb": preflight["free_gb"], "notes": []},
            "artifacts": {},
            "stdout_path": None,
            "stderr_path": None,
            "exit_status": {"consumer": None, "load_generator": None},
        }

    def _write_invalid_record(self, run: dict, preflight: dict, *, reason: str,
                              error: str, notes: list[str] | None = None) -> str:
        """F-I4: any outcome produces a schema-valid record, even a failed run."""
        record = self._base_record(run, preflight)
        record["config"]["runner"] = self._lifecycle_block(None, {})
        reason = reason if reason in CLOSED_EXCLUSIONS else EXCLUDE_OUTCOME
        record["validity"]["exclude_reason"] = reason
        record["validity"]["valid"] = False
        record["validity"]["excluded"] = True
        record["validity"]["notes"] = (notes or []) + [f"invalid: {error}"[:500]]
        if reason == EXCLUDE_RESIDUE:
            record["validity"]["residue_check"] = False
        if reason == EXCLUDE_CLOCK:
            record["validity"]["clock_check"] = False
        record["artifacts"] = {}
        record = pathsafe.scrub(record)
        pathsafe.assert_clean(record, f"invalid record {run['run_id']}")
        errors = VR.validate_record(record)
        if errors:
            raise RuntimeError("internal bug: invalid-record skeleton fails validation:\n"
                               + "\n".join(errors))
        path = os.path.join(self.records_dir, f"run_{run['run_id']}.json")
        with open(path, "w", encoding="utf-8", newline="") as fh:
            json.dump(record, fh, indent=2)
            fh.write("\n")
        self.runlog.append(stage=run["stage_label"], run_id=run["run_id"],
                           seed=run["data_seed"],
                           command=f"[record] invalid run record ({reason})", exit_status=0,
                           outputs=pathsafe.repo_rel(path))
        return path

    def _attempt_loop(self, run: dict, preflight: dict,
                      reset_info: dict | None = None) -> dict:
        """prereg §8.3: invalid attempts are rerun up to R_max with the same (cell, rep) seeds."""
        attempts = []
        final = None
        for attempt in range(1, RERUN_CAP + 1):
            self._restart_epoch = self._restart_epoch if hasattr(self, "_restart_epoch") else 0
            outcome = self._attempt_once(run, preflight, attempt, reset_info or {})
            outcome["attempt"] = attempt
            attempts.append({"attempt": attempt, "status": outcome.get("status"),
                             "record": outcome.get("record"),
                             "reason": outcome.get("reason")})
            final = outcome
            if outcome.get("status") in ("valid", "censored", "excluded_terminal"):
                break
            if outcome.get("status") == "invalid":
                self.runlog.append(stage=run["stage_label"], run_id=run["run_id"],
                                   seed=run["data_seed"],
                                   command=f"[rerun] attempt {attempt}/{RERUN_CAP} invalid: "
                                           f"{outcome.get('reason')}", exit_status="RETRY")
        self._write_attempt_summary(attempts)
        return {"run_id": run["run_id"], "attempts": attempts, "final": final}

    def _attempt_once(self, run: dict, preflight: dict, attempt: int,
                      reset_info: dict | None = None) -> dict:
        try:
            seq = LiveSequence(self, run, preflight, attempt, reset_info or {})
            result = seq.execute()
            if result.get("status") == "invalid":
                # LiveSequence already wrote the invalid record
                return result
            return result
        except Exception as exc:  # noqa: BLE001 - a failed attempt must still be recorded
            traceback.print_exc()
            reason = getattr(exc, "exclude_reason", None) or EXCLUDE_START
            if not isinstance(reason, str) or reason not in CLOSED_EXCLUSIONS:
                reason = EXCLUDE_OUTCOME
            path = self._write_invalid_record(run, preflight, reason=reason,
                                              error=f"{type(exc).__name__}: {exc}")
            self.runlog.append(stage=run["stage_label"], run_id=run["run_id"],
                               seed=run["data_seed"],
                               command=f"[live] attempt {attempt} failed: "
                                       f"{type(exc).__name__}", exit_status="FAIL")
            return {"status": "invalid", "reason": str(exc)[:300],
                    "record": pathsafe.repo_rel(path)}

    def _write_attempt_summary(self, attempts: list[dict]) -> None:
        path = os.path.join(self.summaries_dir, "attempts.csv")
        exists = os.path.exists(path)
        with open(path, "a", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            if not exists:
                writer.writerow(["utc", "run_id", "attempt", "status", "record", "reason"])
            for item in attempts:
                writer.writerow([utc_iso(), item.get("run_id", ""), item["attempt"],
                                 item["status"], item.get("record"), item.get("reason")])

    def run_live(self) -> dict:
        env = self.check_env()
        should_start = any(r.get("injector_mode") not in ("none", "fixture", "sham")
                           for r in self.selected_runs())
        if should_start and not env["ready"]:
            raise SystemExit(
                "S3 interface not ready — missing:\n" + json.dumps(
                    {k: v for k, v in env.items()
                     if isinstance(v, dict) and v.get("present") is False}, indent=2)
                + "\nThe pilot must not start before S3's artifact acceptance tests pass and "
                  "the lead confirms (P2 gate).")
        runs = self.selected_runs()
        preflight = self.preflight(require_broker_configs=True)
        results = []
        for run in runs:
            record_path = os.path.join(self.records_dir, f"run_{run['run_id']}.json")
            if self.opts.resume and os.path.exists(record_path):
                existing = read_json(record_path, {})
                if (existing.get("validity") or {}).get("valid"):
                    results.append({"run_id": run["run_id"], "skipped": "valid record exists"})
                    continue
            # broker/PG/Toxiproxy liveness guard before every run (M5 item 5). If the stack is
            # unexpectedly down: reset once, record the run invalid with an explicit reason,
            # never silently continue.
            alive, live_state = self._ensure_stack_alive(run)
            if not alive:
                path = self._write_invalid_record(
                    run, preflight, reason=EXCLUDE_STACK,
                    error=f"stack not alive after one reset attempt: {live_state}",
                    notes=["environment_stack_down: brokers/PG/Toxiproxy did not respond; "
                           "one reset-kafka attempt made; run recorded invalid for rerun"])
                results.append({"run_id": run["run_id"], "status": "invalid",
                                "reason": EXCLUDE_STACK,
                                "record": pathsafe.repo_rel(path)})
                self._runs_since_reset += 1
                continue
            reset_info = self._maybe_periodic_reset(run)
            results.append(self._attempt_loop(run, preflight, reset_info))
            self._runs_since_reset += 1
        return {"mode": "live", "runs": results, "preflight": preflight,
                "reset_epoch": self._reset_epoch, "block_id": self._block_id,
                "attempts_summary": pathsafe.repo_rel(
                    os.path.join(self.summaries_dir, "attempts.csv")),
                "lifecycle_summary": pathsafe.repo_rel(
                    os.path.join(self.summaries_dir, "lifecycle.csv"))}

    # -- Windows-KRaft topic lifecycle (M5) -------------------------------------------------
    def _live_topic_count(self) -> int:
        """Non-internal topics currently in the cluster (topic-cap accounting, M5 item 2)."""
        try:
            topics = self._admin_client().list_topics()
            return len([t for t in topics if not t.startswith("__")])
        except Exception:  # noqa: BLE001 - liveness guard reports the failure
            return -1

    def _admin_client(self):
        if getattr(self, "_admin", None) is None:
            self._admin = KA.KafkaAdmin()
        return self._admin

    def _stack_liveness(self) -> dict:
        """Brokers + PostgreSQL + Toxiproxy respond (M5 item 5)."""
        state: dict = {"kafka": False, "brokers": 0, "pg": False, "toxiproxy": False}
        try:
            md = self._admin_client()._client.list_topics(timeout=20)
            state["brokers"] = len(getattr(md, "brokers", {}) or {})
            state["kafka"] = state["brokers"] >= 3
        except Exception as exc:  # noqa: BLE001
            state["kafka_error"] = f"{type(exc).__name__}: {exc}"[:200]
        env = self.check_env()
        psql = env.get("psql")
        if psql:
            try:
                proc = subprocess.run([psql, "-h", "127.0.0.1", "-p", "5433", "-U", "wxeo_read",
                                       "-d", "wxeo", "-tAc", "SELECT 1"],
                                      capture_output=True, text=True, timeout=20)
                state["pg"] = proc.returncode == 0 and (proc.stdout or "").strip() == "1"
            except Exception as exc:  # noqa: BLE001
                state["pg_error"] = f"{type(exc).__name__}: {exc}"[:200]
        try:
            info = INJ.Toxiproxy().proxies()
            state["toxiproxy"] = isinstance(info, dict) and len(info) >= 2
        except Exception as exc:  # noqa: BLE001
            state["toxiproxy_error"] = f"{type(exc).__name__}: {exc}"[:200]
        state["ok"] = bool(state["kafka"] and state["pg"] and state["toxiproxy"])
        return state

    def _harness_verb(self, verb: str, run: dict, purpose: str, timeout: float = 600) -> int:
        harness = os.path.join(self.repo, "artifact", "harness", "native-stack.ps1")
        proc = self.call([discover_pwsh(), "-NoProfile", "-ExecutionPolicy", "Bypass",
                          "-File", harness, verb],
                         stage=run["stage_label"], run_id=run["run_id"],
                         seed=run["data_seed"], purpose=purpose, check=False, timeout=timeout)
        return int(proc.returncode or 0)

    def _perform_reset(self, reason: str, run: dict) -> dict:
        """Full-stack storage reclaim via `reset-kafka` (wipe + reformat), then restart PG/Toxi.

        Plain stop/start would not reclaim the accumulated topic segments, and per-run topic
        deletion is fatal on this Windows KRaft setup (M5 directive).
        """
        topics_before = self._live_topic_count()
        start_utc = utc_iso()
        if STACK.is_docker():
            self.runlog.append(stage=run["stage_label"], run_id=run["run_id"],
                               seed=run["data_seed"],
                               command=f"[reset] docker compose down -v && up -d --wait ({reason})",
                               exit_status="START")
            result = STACK.docker_reset()
            rc_reset = result["rc_down"]
            rc_pg = result["rc_up"]
            rc_toxi = 0
            reset_action = "docker-compose-reset"
        else:
            self.runlog.append(stage=run["stage_label"], run_id=run["run_id"],
                               seed=run["data_seed"],
                               command=f"[reset] reset-kafka ({reason})", exit_status="START")
            rc_reset = self._harness_verb("reset-kafka", run, f"reset-kafka ({reason})", 900)
            rc_pg = self._harness_verb("start-pg", run, "start-pg (after reset)", 300)
            rc_toxi = self._harness_verb("start-toxiproxy", run,
                                         "start-toxiproxy (after reset)", 300)
            reset_action = "reset-kafka"
        # verify liveness (retry up to 240 s)
        deadline = time.monotonic() + 240
        live = {"ok": False}
        while time.monotonic() < deadline:
            live = self._stack_liveness()
            if live["ok"]:
                break
            time.sleep(2.0)
        self._reset_epoch += 1
        self._block_id += 1
        info = {
            "ts_utc": start_utc, "action": reset_action, "stack_mode": STACK.stack_mode(),
            "reason": reason,
            "epoch": self._reset_epoch, "block_id": self._block_id,
            "runs_since_last_reset": self._runs_since_reset,
            "cadence": reset_cadence_for(int(run["load_events"]),
                                         run.get("load_mode", "fixed-rate")),
            "topics_before": topics_before, "rc_reset_kafka": rc_reset, "rc_start_pg": rc_pg,
            "rc_start_toxiproxy": rc_toxi, "alive_after": bool(live.get("ok")),
            "brokers": live.get("brokers"), "git_sha": self.runlog.sha,
        }
        self._last_reset = info
        restart_log = os.path.join(self.results_dir, "fault_restart.log")
        with open(restart_log, "a", encoding="utf-8", newline="") as fh:
            fh.write(json.dumps(pathsafe.scrub(info), separators=(",", ":")) + "\n")
        self.runlog.append(stage=run["stage_label"], run_id=run["run_id"],
                           seed=run["data_seed"],
                           command=f"[reset] {reset_action} done (alive={info['alive_after']})",
                           exit_status=0,
                           outputs=f"epoch={self._reset_epoch} block={self._block_id}")
        self._runs_since_reset = 0
        if not info["alive_after"]:
            raise SystemExit(
                "ABORT: full-stack reset (-reason " + reason + ") did not restore a live stack "
                f"({live}); refusing to continue the campaign. Live state: {live}")
        return info

    def _ensure_stack_alive(self, run: dict) -> tuple[bool, dict]:
        """M5 item 5: liveness guard; one reset attempt on failure, then record invalid."""
        live = self._stack_liveness()
        if live.get("ok"):
            return True, live
        self.runlog.append(stage=run["stage_label"], run_id=run["run_id"],
                           seed=run["data_seed"],
                           command="[guard] stack not alive before run; reset-kafka once",
                           exit_status="WARN", notes=json.dumps(pathsafe.scrub(live))[:400])
        try:
            self._perform_reset("stack_down", run)
        except SystemExit:
            raise
        except Exception as exc:  # noqa: BLE001
            return False, {"first_state": live, "reset_error":
                           f"{type(exc).__name__}: {exc}"[:300]}
        live_after = self._stack_liveness()
        return bool(live_after.get("ok")), {"first_state": live, "after_reset": live_after}

    def _maybe_periodic_reset(self, run: dict) -> dict:
        """M5 item 2: reset cadence by load + hard live-topic cap."""
        load = int(run["load_events"])
        mode = run.get("load_mode") or "fixed-rate"
        cadence = reset_cadence_for(load, mode)
        live_topics = self._live_topic_count()
        due, reason = should_periodic_reset(self._runs_since_reset, load, mode, live_topics)
        if os.environ.get("WXEO_DISABLE_PERIODIC_RESET") == "1" and due:
            # Documented pilot deviation: the pilot set fits in the disk budget with the 1 MB
            # broker index cap, and the harness reset path is the least reliable component of the
            # stack; resets are therefore done between batches by the lead, never mid-pilot.
            due, reason = False, None
        info = {"restart_before_run": due, "block_id": self._block_id,
                "reset_epoch": self._reset_epoch,
                "runs_since_reset_before": self._runs_since_reset,
                "cadence": cadence, "topic_count_before": live_topics}
        if due:
            reset = self._perform_reset(reason or "cadence", run)
            info.update({"reset": reset, "restart_before_run": True,
                         "block_id": self._block_id, "reset_epoch": self._reset_epoch,
                         "topic_count_after": self._live_topic_count()})
        self._append_summary("lifecycle", {
            "utc": utc_iso(), "run_id": run["run_id"], "run_type": run.get("cell_id"),
            "load_events": load, "load_mode": mode, "cadence": cadence,
            "runs_since_reset": self._runs_since_reset,
            "reset_before_run": bool(info["restart_before_run"]),
            "reset_reason": reason if due else None,
            "block_id": self._block_id, "reset_epoch": self._reset_epoch,
            "topics_live_before": live_topics,
            "topics_live_after": info.get("topic_count_after", live_topics),
            "git_sha": self.runlog.sha})
        return info

    def _append_summary(self, name: str, row: dict) -> None:
        path = os.path.join(self.summaries_dir, f"{name}.csv")
        exists = os.path.exists(path)
        with open(path, "a", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(row.keys()))
            if not exists:
                writer.writeheader()
            writer.writerow(pathsafe.scrub(row))

    @staticmethod
    def _dir_bytes(path: str) -> int:
        total = 0
        for dirpath, _dirs, files in os.walk(path):
            for name in files:
                try:
                    total += os.path.getsize(os.path.join(dirpath, name))
                except OSError:
                    continue
        return total

    # -- dry run ---------------------------------------------------------------------------
    def dry_run_plan(self, fixture_dir: str) -> dict:
        descriptor = read_json(os.path.join(fixture_dir, "fixture.json"))
        if not descriptor:
            raise SystemExit(f"{fixture_dir} is not a fixture bundle (fixture.json missing)")
        if not descriptor.get("synthetic"):
            raise SystemExit("refusing: fixture is not labelled synthetic")
        return descriptor

    def run_dry(self) -> dict:
        if not self.opts.fixture:
            raise SystemExit("--dry-run requires --fixture")
        provided_dir = os.path.abspath(self.opts.fixture)
        provided = self.dry_run_plan(provided_dir)
        runs = self.selected_runs()
        preflight = self.preflight()
        results = []
        for run in runs:
            fixture_dir, descriptor = self._dry_fixture_for(run, provided_dir, provided)
            results.append(self._dry_one(run, fixture_dir, descriptor, preflight))
        return {"mode": "dry-run", "synthetic": True, "runs": results,
                "preflight": preflight,
                "note": "SYNTHETIC DRY-RUN — not measurement data; no confirmatory analysis may "
                        "use results/dryrun/"}

    def _dry_fixture_for(self, run: dict, provided_dir: str,
                         provided: dict) -> tuple[str, dict]:
        if provided.get("run_id") == run["run_id"] and provided.get("seed_hex") == run["data_seed"]:
            return provided_dir, provided
        fixture_dir = os.path.join(self.results_dir, "fixtures", run["run_id"])
        fixture_path = os.path.join(fixture_dir, "fixture.json")
        existing = read_json(fixture_path)
        if existing and existing.get("run_id") == run["run_id"] and \
                existing.get("seed_hex") == run["data_seed"]:
            return fixture_dir, existing
        descriptor = FX.write_clean_run(
            fixture_dir, run_id=run["run_id"], mechanism=run["mechanism"], sink_kind="db",
            seed_hex=run["data_seed"], n_events=int(run["load_events"]),
            partitions=int(run["partitions"]), label="synthetic-dryrun",
        )
        return fixture_dir, descriptor

    def _dry_one(self, run: dict, fixture_dir: str, descriptor: dict, preflight: dict) -> dict:
        run_id = run["run_id"]
        seed_hex = run["data_seed"]
        seed_u64 = D.seed_hex_to_u64(seed_hex)
        stage = run["stage_label"]
        paths = {k: os.path.join(fixture_dir, v) for k, v in descriptor["paths"].items()}
        if descriptor.get("seed_hex") != seed_hex:
            raise SystemExit(
                f"fixture seed {descriptor.get('seed_hex')!r} != run data_seed {seed_hex!r}; "
                f"rebuild with: python experiments/fixtures/build_dryrun_fixture.py "
                f"--run-id {run_id}")
        if int(descriptor["events"]) != int(run["load_events"]):
            raise SystemExit(
                f"fixture has {descriptor['events']} events but run {run_id} needs "
                f"{run['load_events']}; generate a matching fixture")
        t0 = utc_now()
        self.runlog.append(stage=stage, run_id=run_id, seed=seed_hex,
                           command=f"[dry-run] oracle over fixture "
                                   f"{pathsafe.repo_rel(fixture_dir)}", exit_status="START")
        oracle_out = os.path.join(self.oracle_dir, f"oracle_{run_id}.json")
        result = O.oracle_run(
            run_id=run_id, mechanism=run["mechanism"], sink_kind="db",
            master_seed_u64=seed_u64, n_events=int(run["load_events"]),
            partitions=int(run["partitions"]),
            gt_path=paths["gt"], source_dump_path=paths["source_dump"],
            produced_manifest_path=paths["produced"], sink_dump_path=paths["sink_dump"],
            publication_dump_path=paths["publication_dump"],
            attestation_path=paths["attestation"], out_path=oracle_out, shards=16,
        )
        t1 = utc_now()
        self.runlog.append(stage=stage, run_id=run_id, seed=seed_hex,
                           command="[dry-run] oracle", exit_status=0,
                           outputs=pathsafe.repo_rel(oracle_out))
        record = self._base_record(run, preflight)
        record.update({
            "timings": {"t_start": utc_ms(t0), "t_load_start": utc_ms(t0) + 5,
                        "t_load_end": utc_ms(t0) + 105, "t_fault_fire": None, "t_restore": None,
                        "t_quiesce": utc_ms(t1), "t_observation": utc_ms(t1) + 1,
                        "t_end": utc_ms(t1) + 2},
            "performance": {**{k: None for k in VR.PERF_KEYS},
                            "warmup_events_excluded": 0,
                            "perf_window": {"note": "dry-run: no performance measurement"}},
            "counts": result["counts"],
            "violations": result["violations"],
            "publication": result["publication"],
            "oracle": result["oracle"],
            "fault_receipts": [],
            "validity": {"valid": True, "excluded": False, "exclude_reason": None,
                         "residue_check": True, "clock_check": True,
                         "disk_free_start_gb": preflight["free_gb"],
                         "notes": ["DRY-RUN synthetic fixture: orchestration/schema/oracle/"
                                   "archive exercise only; NOT a measurement and outside "
                                   "results/raw"]},
            "exit_status": {"consumer": None, "load_generator": None,
                            "note": "dry-run: no processes started"},
        })
        archived, manifest = self._archive_run(run_id, {
            "gt": paths["gt"],
            "gt_sha256_sidecar": os.path.join(fixture_dir,
                                              f"gt_{descriptor['run_id']}.sha256"),
            "source_dump": paths["source_dump"], "produced_manifest": paths["produced"],
            "sink_dump": paths["sink_dump"], "publication_dump": paths["publication_dump"],
            "attestation": paths["attestation"], "manifest": paths["manifest"],
            "oracle": oracle_out,
        })
        record["artifacts"] = archived
        record = pathsafe.scrub(record)
        pathsafe.assert_clean(record, f"dry-run record {run_id}")
        errors = VR.validate_record(record)
        record_path = os.path.join(self.records_dir, f"run_{run_id}.json")
        with open(record_path, "w", encoding="utf-8", newline="") as fh:
            json.dump(record, fh, indent=2)
            fh.write("\n")
        self.runlog.append(stage="dry-run", run_id=run_id, seed=seed_hex,
                           command="[dry-run] write run record + digest manifest", exit_status=0,
                           outputs=pathsafe.repo_rel(record_path))
        if errors:
            raise RuntimeError("dry-run produced an invalid run record:\n" + "\n".join(errors))
        return {"run_id": run_id, "record": pathsafe.repo_rel(record_path),
                "oracle": pathsafe.repo_rel(oracle_out), "valid": True,
                "violating_run": record["violations"]["violating_run"],
                "digest_manifest": manifest}

    def _disk_footprint(self) -> dict:
        return {"free_gb": free_space_gb(eos_runtime_root()),
                "scratch_free_gb": free_space_gb(self.scratch_root)}

    def _lifecycle_block(self, seq, reset_info: dict | None = None) -> dict:
        """Lifecycle metadata recorded in the run record (prereg §5 config object, additive)."""
        reset_info = reset_info or {}
        return {
            "attempt": getattr(seq, "attempt", 1),
            "topic_suffix": getattr(seq, "topic_suffix", ""),
            "source_topic": getattr(seq, "source_topic", None),
            "sink_topic": getattr(seq, "sink_topic", None),
            "db_schema": getattr(seq, "schema", None),
            "restart_before_run": bool(reset_info.get("restart_before_run")),
            "block_id": int(reset_info.get("block_id", self._block_id)),
            "reset_epoch": int(reset_info.get("reset_epoch", self._reset_epoch)),
            "runs_since_reset_before": reset_info.get("runs_since_reset_before"),
            "reset_cadence": reset_info.get("cadence"),
            "reset_reason": (reset_info.get("reset") or {}).get("reason"),
            "topic_count_before": reset_info.get("topic_count_before"),
            "rate_mode": getattr(seq, "rate_mode", None),
            # A6.1/A6.2: the saturation-validation runs record the EXACT rate offered to the
            # load generator, so rates.py --write can prove the derived rate was exercised.
            "offered_rate": getattr(seq, "offered_rate", None),
            # S5 B7: endpoint contract path + sha (repo-relative) for provenance/fail-closed audit.
            "endpoint_contract": self.endpoint_contract(),
            "failpoint_instance": getattr(seq, "_failpoint_instance", 1),
            "l2_trigger": "progress-based (M6)",
            "topic_count_after": reset_info.get("topic_count_after"),
            "topic_lifecycle": "no per-run deletion (M5); periodic reset-kafka wipe",
            "max_live_topics": MAX_LIVE_TOPICS,
            "disk_free_start_gb": getattr(seq, "disk_start", {}).get("free_gb"),
            "disk_free_end_gb": getattr(seq, "disk_end", {}).get("free_gb"),
            "scratch_bytes": getattr(seq, "scratch_bytes", None),
        }

    # -- deviations ------------------------------------------------------------------------
    def write_deviations(self) -> str:
        from experiments import deviations as DEV

        return DEV.write_deviations(self.matrix_path, self.records_dir,
                                    os.path.join(self.results_dir, "DEVIATIONS.md"))


# --------------------------------------------------------------------------------------------
# live sequence — docs/prereg.md §4.1 steps 1-13
# --------------------------------------------------------------------------------------------
class LiveSequence:
    """One run attempt against the real native stack."""

    def __init__(self, runner: Runner, run: dict, preflight: dict, attempt: int,
                 reset_info: dict | None = None):
        self.r = runner
        self.run = run
        self.preflight = preflight
        self.attempt = attempt
        self.run_id = run["run_id"]
        self.stage = run["stage_label"]
        self.seed_hex = run["data_seed"]
        self.seed_u64 = D.seed_hex_to_u64(self.seed_hex)
        self.p = RunnerPaths(self.run_id, self.r.scratch_root, attempt)
        self.env = preflight["interface"]
        self.timings = {k: None for k in VR.TIMING_KEYS}
        self.processes: dict[int, subprocess.Popen] = {}
        self.consumer_pids: dict[int, int] = {}
        # Attempt/session-scoped names: topics/schemas are NEVER deleted per run (Windows KRaft
        # bug), so every invocation uses a unique session tag + attempt number; fresh state does
        # not depend on deletion (reclaimed later by periodic reset-kafka).
        session = getattr(runner, "_session_tag", "s0")
        self.topic_suffix = f".s{session}.a{attempt}"
        self.source_topic = f"wxeo.src.{self.run_id}{self.topic_suffix}"
        self.sink_topic = f"wxeo.sink.{self.run_id}{self.topic_suffix}"
        self.group = f"wxeo.cg.{self.run_id}{self.topic_suffix}"
        # PostgreSQL identifiers must match [A-Za-z0-9_]{1,63} (S3 SchemaTool contract);
        # run_ids are lower-case hyphenated for Kafka-topic legality, so map '-' -> '_' for the
        # schema identifier only. The mapping is deterministic and recorded in the run notes.
        suffix_schema = "" if False else f"_s{getattr(runner, '_session_tag', 's0')}_a{attempt}"
        self.schema = ("wxeo_run_" + self.run_id.replace("-", "_") + suffix_schema)[:63]
        self.reset_info = reset_info or {}
        self._failpoint_instance = 1
        # rate provenance (config.runner.rate_mode / config.runner.offered_rate): the offered
        # rate is only set by the A6 saturation-validation path so its absence is meaningful.
        self.rate_mode: str | None = None
        self.offered_rate: int | None = None
        self._consumer_exit: dict[int, int | None] = {}
        self.disk_start = runner._disk_footprint()
        self.attempt = attempt
        self.db_schema_used = self.run["mechanism"] != "M1-kafka"
        self.injector = None
        self._inj_ctx = None
        self.oracle_result = None
        self.exclusions: list[str] = []
        self.notes: list[str] = [f"db schema identifier: {self.schema} "
                                 f"(run_id hyphens mapped to underscores per SchemaTool pattern)"]
        self.loader_result: dict | None = None
        self.load_proc: subprocess.Popen | None = None
        self.admin = None
        self.quiescence: dict = {}
        self.dumps: dict = {}
        self.clock_before: dict | None = None
        self.clock_after: dict | None = None

    # -- injector context ------------------------------------------------------------------
    def _injector_context(self) -> INJ.InjectorContext:
        kafka_home = self.env.get("kafka_home") or ""
        failpoint_after = 1
        if self.run.get("fault_level") == "L2" and self.run.get("fault") in ("F4b",):
            # The app failpoint counts EFFECT-TRANSACTION hits, not events: one batch is
            # MAX_POLL_RECORDS events and the load is split over the consumer instances.  At 1k
            # events this leaves a single effect transaction per instance, so any seeded threshold
            # >=2 can never fire (observed: 300 s timeout, zero receipts, M16).  Scale the seeded
            # window by the number of effect transactions and fall back to the deterministic first
            # hit when only one exists (disclosed in amendment A4).
            instances = max(1, int(self.run.get("consumer_instances") or 1))
            expected_hits = expected_effect_transactions(int(self.run["load_events"]), instances)
            failpoint_after = (1 if expected_hits <= 1 else
                               INJ.seeded_failpoint_after(self._empty_ctx(),
                                                          expected_hits=expected_hits))
        return INJ.InjectorContext(
            run_id=self.run_id,
            mechanism=self.run["mechanism"],
            ordering_variant=self.run["ordering_variant"],
            fault=self.run["fault"],
            fault_level=self.run["fault_level"],
            injector_mode=self.run.get("injector_mode", "none"),
            seed_hex=self.seed_hex,
            scratch_dir=self.p.scratch,
            faults_log=self.r.faults_log,
            bootstrap="127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093",
            stack_state_path=os.path.join(eos_runtime_root(), "run", "stack-state.json"),
            native_stack_ps1=os.path.join(self.r.repo, "artifact", "harness",
                                          "native-stack.ps1"),
            kafka_home=kafka_home,
            java_exe=self.env.get("java") or "java",
            db_dsn=self._db_dsn_read(),
            admin_dsn=self._db_dsn_admin(),
            run_log=self.r.runlog.append,
            call=lambda cmd, **kw: self.r.call(cmd, stage=self.stage, run_id=self.run_id,
                                               seed=self.seed_hex, **kw),
            restart_service=self._restart_service,
            stop_consumers=self._stop_consumers,
            start_consumers=self._start_services,
            wait_group_stable=self._wait_group_stable,
            wait_isr=self._wait_isr,
            progress_probe=self._progress_probe,
            rewind_progress=self._rewind_progress,
            verify_broker_count=self._broker_count,
            consumer_pids=self.consumer_pids,
            extra={
                "source_topic": self.source_topic,
                "consumer_group": self.group,
                "psql": self.env.get("psql") or "psql",
                "db_dsn": self._db_dsn_read(),
                "load_events": int(self.run["load_events"]),
                "failpoint_action": self.r.opts.failpoint_action,
                "failpoint_after": failpoint_after,
                "failpoint_timeout_s": 240,
                "rewind_batches": 2,
                "produce_proxy": "kafka-produce",
                "consume_proxy": "kafka-consume",
                "db_proxy": "db",
            },
        )

    def _empty_ctx(self):
        return INJ.InjectorContext(
            run_id=self.run_id, mechanism=self.run["mechanism"],
            ordering_variant=self.run["ordering_variant"], fault=self.run["fault"],
            fault_level=self.run["fault_level"],
            injector_mode=self.run.get("injector_mode", "none"), seed_hex=self.seed_hex,
            scratch_dir=self.p.scratch, faults_log=self.r.faults_log,
            bootstrap="127.0.0.1:9091")

    # -- execute ---------------------------------------------------------------------------
    def execute(self) -> dict:
        self.clock_before = clock_sanity_probe()
        self.admin = KA.KafkaAdmin()
        # R4 fix: one context instance whose load window is filled in around the real loader
        self._inj_ctx = self._injector_context()
        self.injector = INJ.make_injector(self._inj_ctx)
        self.timings["t_start"] = utc_ms()
        try:
            self._assert_fresh_state()
            self._create_state()
            self._start_services()
            if self.injector is not None:
                self.injector.arm()
            # 7. produce (load generator writes+fsyncs GT+manifest before the first produce)
            self._start_load_generator()
            fire_thread = self._start_injector_thread()
            self._wait_load_generator()
            if fire_thread is not None:
                fire_thread.join(timeout=600)
            if self.timings["t_load_start"] is None or self.timings["t_load_end"] is None:
                raise RuntimeError("load generator did not report its load window")
            # 8. restore the injection (fired receipt already written by the injector)
            if self.injector is not None:
                self.injector.restore()
                self._collect_fault_receipts()
            # 9. quiescence (app still running)
            self.quiescence = self._quiesce()
            # 10. observation: stop the app gracefully, THEN dump (prereg §4.6/method §7.3)
            self._stop_consumers()
            self.timings["t_observation"] = utc_ms()
            self.dumps = self._dump()
            # 11. oracle
            self._run_oracle()
            # 12/13. finish, residue check, archive, drop
            return self._finish()
        except Exception:
            self._emergency_cleanup()
            raise

    # -- fresh state -----------------------------------------------------------------------
    def _assert_fresh_state(self) -> None:
        problems = []
        if self.admin.topic_exists(self.source_topic):
            problems.append(f"source topic {self.source_topic} already exists")
        if self.admin.topic_exists(self.sink_topic):
            problems.append(f"sink topic {self.sink_topic} already exists")
        if self.admin.group_exists(self.group):
            problems.append(f"consumer group {self.group} already exists")
        if self.db_schema_used:
            proc = self._psql("SELECT count(*) FROM information_schema.schemata WHERE "
                              f"schema_name = '{self.schema}'", "residue: schema",
                              user="wxeo_read", check=False)
            if (proc.stdout or "").strip() not in ("", "0"):
                problems.append(f"schema {self.schema} already exists")
        if problems:
            raise RuntimeError("residue before run: " + "; ".join(problems))

    def _create_state(self) -> None:
        self.r.runlog.append(stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                             command="[reset] fresh topics/group/schema", exit_status="START")
        self.admin.create_topic(self.source_topic, int(self.run["partitions"]), 3)
        self.admin.create_topic(self.sink_topic, int(self.run["partitions"]), 3)
        for topic in (self.source_topic, self.sink_topic):
            if not self.admin.wait_topic_present(topic, int(self.run["partitions"]), 120):
                raise RuntimeError(f"topic {topic} did not appear with "
                                   f"{self.run['partitions']} partitions")
        if self.db_schema_used:
            jar = os.path.join(self.r.repo, "artifact", "target", "wxeo-service.jar")
            self.r.call([self.env.get("java") or "java", "-cp", jar,
                         "com.wxeo.sink.SchemaTool", "--mode=init",
                         "--db-url=jdbc:postgresql://127.0.0.1:5433/wxeo",
                         "--db-user=postgres", f"--schema={self.schema}",
                         f"--mechanism={self.run['mechanism']}", "--app-role=wxeo_app",
                         "--read-role=wxeo_read", "--fault-role=wxeo_fault"],
                        stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                        purpose="schema-init", check=True, timeout=180)

    # -- services --------------------------------------------------------------------------
    def _service_args(self, instance: int, disable_failpoint: bool = False) -> list[str]:
        jar = os.path.join(self.r.repo, "artifact", "target", "wxeo-service.jar")
        java = self.env.get("java") or "java"
        warmup = self._warmup_events()
        ep = self._endpoints()
        args = [
            java, "-Xms512m", "-Xmx1g", "-XX:+UseG1GC", "-XX:+ExitOnOutOfMemoryError",
            "-jar", jar,
            f"--wxeo.mechanism={self.run['mechanism']}",
            f"--wxeo.ordering={self.run['ordering_variant']}",
            f"--wxeo.run-id={self.run_id}",
            f"--wxeo.instance-id={instance}",
            f"--wxeo.bootstrap={ep['bootstrap']}",
            f"--wxeo.db-url={ep['db_url']}",
            "--wxeo.db-user=wxeo_app",
            f"--wxeo.db-schema={self.schema}",
            f"--wxeo.source-topic={self.source_topic}",
            f"--wxeo.sink-topic={self.sink_topic}",
            f"--wxeo.group={self.group}",
            f"--wxeo.output-dir={self.p.outputs}",
            f"--wxeo.faults-log={self.r.faults_log}",
            f"--wxeo.warmup-events={warmup}",
            "--wxeo.require-append-only-attestation=true",
        ]
        fp = INJ.failpoint_config_for_run(self._inj_ctx or self._injector_context())
        if fp and fp.get("enabled") and int(instance) == int(self._failpoint_instance) \
                and not disable_failpoint:
            args += ["--wxeo.failpoint.enabled=true", f"--wxeo.failpoint.name={fp['name']}",
                     f"--wxeo.failpoint.action={fp['action']}",
                     f"--wxeo.failpoint.after={fp['after']}"]
        else:
            args.append("--wxeo.failpoint.enabled=false")
        if fp and fp.get("enabled"):
            args.append(f"--wxeo.failpoint.instance={self._failpoint_instance}")
        if ep["producer"]:
            args.append(f"--wxeo.producer-bootstrap={ep['producer']}")
        if ep["consumer"]:
            args.append(f"--wxeo.consumer-bootstrap={ep['consumer']}")
        return args

    def _warmup_events(self) -> int:
        if self.r.opts.warmup_events is not None:
            return int(self.r.opts.warmup_events)
        return max(2_000, int(int(self.run["load_events"]) * 0.10))

    def _start_services(self) -> None:
        n = int(self.run["consumer_instances"])
        # Only (re)start instances that are not currently alive. Cleanup is per-instance so a
        # single-instance restart (K-class failpoint restore) does not delete a healthy peer's
        # ready marker (observed live: restart of instance 1 made readiness fail 1/2).
        import glob
        to_start = []
        for instance in range(1, n + 1):
            proc = self.processes.get(instance)
            if instance in self.consumer_pids and proc is not None and proc.poll() is None:
                continue
            to_start.append(instance)
        for instance in to_start:
            for stale in glob.glob(os.path.join(self.p.outputs,
                                                f"stop_{self.run_id}_{instance}.flag")):
                try:
                    os.remove(stale)
                except OSError:
                    pass
            for stale in glob.glob(os.path.join(self.p.outputs,
                                                f"ready_{self.run_id}_{instance}.json")):
                try:
                    os.remove(stale)
                except OSError:
                    pass
        for instance in to_start:
            args = self._service_args(instance)
            stdout_path = os.path.join(self.p.logs, f"consumer{instance}.stdout.log")
            stderr_path = os.path.join(self.p.logs, f"consumer{instance}.stderr.log")
            stdout = open(stdout_path, "wb")
            stderr = open(stderr_path, "wb")
            proc = subprocess.Popen(args, stdout=stdout, stderr=stderr)
            self.processes[instance] = proc
            self.consumer_pids[instance] = proc.pid
            self.r.runlog.append(stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                                 command=f"[start] consumer instance {instance} "
                                         f"pid={proc.pid}", exit_status="STARTED")
        self._wait_ready(timeout=180)

    def _wait_ready(self, timeout: float = 180) -> None:
        """Readiness = all instances registered and alive, with zero lag for 5 s (prereg §4.1).

        S3's service writes ``status=subscribed`` when it joins the group and does not
        necessarily rewrite the file to ``assigned`` afterwards; a later assignment is therefore
        verified through process liveness and the lag probe rather than only the ready-file label.
        """
        deadline = time.monotonic() + timeout
        n = int(self.run["consumer_instances"])
        lag_zero_since: float | None = None
        while time.monotonic() < deadline:
            assigned = 0
            for instance in range(1, n + 1):
                info = read_json(os.path.join(self.p.outputs,
                                              f"ready_{self.run_id}_{instance}.json"))
                proc = self.processes.get(instance)
                alive = proc is not None and proc.poll() is None
                if info and info.get("status") in ("subscribed", "assigned") and alive:
                    assigned += 1
            lag = self.admin.lag(self.group, self.source_topic)
            lag_zero = bool(lag) and all(v == 0 for v in lag.values())
            if assigned == n and lag_zero:
                lag_zero_since = lag_zero_since or time.monotonic()
                if (time.monotonic() - lag_zero_since) * 1000 >= READY_STABLE_MS:
                    self.r.runlog.append(stage=self.stage, run_id=self.run_id,
                                         seed=self.seed_hex,
                                         command="[ready] instances registered+alive, lag=0 for 5s",
                                         exit_status=0)
                    return
            else:
                lag_zero_since = None
            time.sleep(0.5)
        raise RuntimeError(f"consumer readiness timeout: registered={assigned}/{n} lag={lag}")

    def _restart_service(self, instance: int) -> int:
        """Restart the crashed injection target WITHOUT re-arming the failpoint.

        Re-arming the same failpoint would crash the fresh JVM again (observed live: K3 loop).
        Only this instance is waited for; its peer is already healthy.
        """
        proc = self.processes.get(instance)
        if proc is not None:
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                pass
        self.consumer_pids.pop(instance, None)
        import glob
        for stale in glob.glob(os.path.join(self.p.outputs,
                                            f"stop_{self.run_id}_{instance}.flag")):
            try:
                os.remove(stale)
            except OSError:
                pass
        for stale in glob.glob(os.path.join(self.p.outputs,
                                            f"ready_{self.run_id}_{instance}.json")):
            try:
                os.remove(stale)
            except OSError:
                pass
        args = self._service_args(instance, disable_failpoint=True)
        stdout_path = os.path.join(self.p.logs, f"consumer{instance}.stdout.log")
        stderr_path = os.path.join(self.p.logs, f"consumer{instance}.stderr.log")
        stdout = open(stdout_path, "wb")
        stderr = open(stderr_path, "wb")
        new_proc = subprocess.Popen(args, stdout=stdout, stderr=stderr)
        self.processes[instance] = new_proc
        self.consumer_pids[instance] = new_proc.pid
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            info = read_json(os.path.join(self.p.outputs,
                                          f"ready_{self.run_id}_{instance}.json"))
            alive = new_proc.poll() is None
            lag = self.admin.lag(self.group, self.source_topic)
            lag_zero = bool(lag) and all(v == 0 for v in lag.values())
            if info and info.get("status") in ("subscribed", "assigned") and alive and lag_zero:
                return new_proc.pid
            time.sleep(0.5)
        raise RuntimeError(f"restart readiness timeout for instance {instance}")

    def _stop_consumers(self) -> None:
        for instance in list(self.processes):
            flag = os.path.join(self.p.outputs, f"stop_{self.run_id}_{instance}.flag")
            with open(flag, "w", encoding="utf-8") as fh:
                fh.write("stop\n")
        deadline = time.monotonic() + 120
        for instance, proc in list(self.processes.items()):
            try:
                proc.wait(timeout=max(1.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                proc.kill()
                raise RuntimeError(f"consumer instance {instance} did not stop gracefully")
        self._consumer_exit = {inst: proc.returncode for inst, proc in self.processes.items()}
        self.processes.clear()
        self.consumer_pids.clear()

    # -- kafka helpers ---------------------------------------------------------------------
    def _wait_group_stable(self, timeout: float = 120) -> bool:
        return self.admin.wait_group_stable(self.group, self.source_topic, timeout=timeout)

    def _wait_isr(self, topic: str, rf: int, timeout: float = 120) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                proc = self.r.call(self._kafka_cmd(
                    "kafka.tools.TopicCommand", "--bootstrap-server", "127.0.0.1:9091",
                    "--describe", "--topic", topic), stage=self.stage, run_id=self.run_id,
                    seed=self.seed_hex, purpose="topic describe ISR", check=False, timeout=60)
                text = proc.stdout or ""
                if f"Isr: {rf}" in text and text.count("Isr:") >= int(self.run["partitions"]):
                    return True
            except Exception:  # noqa: BLE001
                pass
            time.sleep(1.0)
        return False

    def _kafka_cmd(self, main_class: str, *args: str) -> list[str]:
        kafka_home = self.env.get("kafka_home") or ""
        return [self.env.get("java") or "java", "-cp",
                os.path.join(kafka_home, "libs", "*"), main_class, *args]

    def _broker_count(self) -> int:
        state = read_json(os.path.join(eos_runtime_root(), "run", "stack-state.json"), {})
        nodes = ((state.get("components") or {}).get("kafka") or {}).get("nodes") or []
        return len([n for n in nodes if n.get("pid")])

    def _progress_probe(self) -> int:
        if self.run["mechanism"] in ("M4", "M4+M3"):
            proc = self._psql(f"SELECT coalesce(sum(next_offset),0) FROM "
                              f"{self.schema}.consumer_progress", "progress marker",
                              user="wxeo_read", check=False)
        else:
            return self.admin.progress_sum(self.group, self.source_topic)
        try:
            return int((proc.stdout or "0").strip() or 0)
        except ValueError:
            return 0

    def _rewind_progress(self, k_batches: int) -> dict:
        """Rewind the mechanism's own resume point by k batches (write-capable admin role)."""
        k = int(k_batches) * INJ.MAX_POLL_RECORDS
        before = self._progress_snapshot()
        if self.run["mechanism"] in ("M4", "M4+M3"):
            proc = self._psql(
                "UPDATE consumer_progress SET next_offset = GREATEST(0, next_offset - "
                f"{k}) RETURNING partition, next_offset", "rewind consumer_progress",
                user="wxeo_fault", check=False)
            if (proc.returncode or 0) != 0:
                raise RuntimeError(f"progress rewind failed: {proc.stderr}")
        else:
            from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
            offsets = []
            current = self.admin.group_offsets(self.group, self.source_topic)
            for partition, offset in current.items():
                offsets.append(TopicPartition(self.source_topic, partition,
                                              max(0, int(offset) - k)))
            request = ConsumerGroupTopicPartitions(self.group, offsets)
            futures = self.admin._client.alter_consumer_group_offsets(
                [request], request_timeout=30.0)
            futures[self.group].result()
        after = self._progress_snapshot()
        moved = before is not None and after is not None and after < before
        return {"k_batches": k_batches, "rewound_offsets": k,
                "progress_before": before, "progress_after": after, "moved": bool(moved)}

    def _progress_snapshot(self) -> int | None:
        try:
            return int(self._progress_probe())
        except Exception:  # noqa: BLE001
            return None

    # -- load generator --------------------------------------------------------------------
    #: S4-owned injections whose fire time is marker-driven: they need a load window long enough
    #: for the progress probe (a few admin round-trips) to observe the marker mid-load.
    MARKER_DRIVEN_FAULTS = frozenset({"F1", "F2", "F3", "F4a", "F5", "F6", "K2", "K5"})

    def _effective_rate(self) -> str:
        mode = self.run.get("load_mode")
        stage_label = str(self.run.get("stage_label") or "")
        stage_lower = str(self.run.get("stage") or "")
        # Amendment A6 (T1b): the M7sat block EXERCISES the derived saturation rate.  It carries
        # stage_label "M7" (calibration record) but must NOT run at max: it is offered exactly
        # rates.json.saturation_rate["100000|p6|sat"].rate (the candidate 2.5x max), fail-closed.
        if stage_label == "M7" and run_block(self.run) == "M7sat":
            return self._saturation_validation_rate()
        # B1/B2 (S5 §12): calibration rows are ALWAYS at max, before any rates.json guard;
        # pilot and Stage-A 1k rows also run at max (frozen M6/A3.3 semantics).
        if stage_label == "M7":
            self.rate_mode = ("calibration-saturation" if mode == "saturation"
                              else "calibration-max")
            return "max"
        if stage_lower == "pilot":
            self.rate_mode = "pilot-max"
            return "max"
        if stage_lower == "a" and int(self.run["load_events"]) == 1000:
            self.rate_mode = "stage-a-max"
            return "max"
        # Pilot calibration: at 1k events and rate=max the load finishes in <1 s, leaving no window
        # in which a marker-driven S4 injection can fire mid-processing (observed live for K2:
        # fire 232 ms after load end). Pilot marker-driven cells therefore use a fixed rate that
        # stretches the load to ~5 s; the rate is recorded in the run config and is identical
        # across mechanisms, so it cannot bias the mechanism comparison.
        if self.opts_rate_table_missing() and self.run.get("fault") in self.MARKER_DRIVEN_FAULTS:
            return str(max(50, int(int(self.run["load_events"]) / 5)))
        if self.opts_rate_table_missing():
            if mode == "saturation" or self.stage in ("B", "C"):
                raise RuntimeError(
                    "experiments/rates.json is missing: fixed-rate/saturation modes are "
                    "pre-registered (prereg §6.2) and may not fall back to 'max' (S5 F-I3)")
            return "max"
        stage = str(self.run.get("stage_label") or "")
        if stage == "M7":
            self.rate_mode = "calibration-max"   # capacity measurement, never derived
            return "max"
        table = read_json(self.r.opts.rate_table, {}) or {}
        if mode == "saturation":
            entry = table.get("saturation_rate")
            if not entry:
                raise RuntimeError(
                    "rates.json has no saturation_rate entry: the saturation block cannot "
                    "run (S5 \u00a711.4(iv)); refusing the max fallback")
            self.rate_mode = "saturation"
            return str(entry.get("rate") if isinstance(entry, dict) else entry)
        key = f"{self.run['load_events']}|p{self.run['partitions']}"
        entry = (table.get("fixed_rate") or {}).get(key)
        if not entry:
            raise RuntimeError(
                f"rates.json exists but has no fixed_rate entry for block {key!r}; "
                f"refusing the --rate/max fallback (S5 \u00a711.4(iii)); calibrated "
                f"blocks: {sorted((table.get('fixed_rate') or {}).keys())}")
        self.rate_mode = "fixed-derived"
        return str(entry.get("rate") if isinstance(entry, dict) else entry)

    def opts_rate_table_missing(self) -> bool:
        return not os.path.exists(self.r.opts.rate_table)

    def _saturation_validation_rate(self) -> str:
        """A6.1: offer EXACTLY the candidate rates.json 2.5x-max saturation rate.

        T1b runs after T1a has written the CANDIDATE table (``rates.py --derive``).  The runner
        must never fall back to ``max`` here: a missing candidate file/key would silently turn
        the saturation-validation block into a capacity measurement and let the derived rate go
        unexercised, which is exactly the S5 §12 B1 blocker A6 closes (fail closed).
        """
        path = self.r.opts.rate_table
        if not os.path.exists(path):
            raise RuntimeError(
                "A6 saturation validation: candidate experiments/rates.json is missing; run "
                "`python experiments/rates.py --derive` after --stage M7cal (T1a) before "
                "--stage M7sat (T1b); refusing the max fallback (S5 §12 B1 / A6.2)")
        table = read_json(path, {}) or {}
        entry = (table.get("saturation_rate") or {}).get("100000|p6|sat")
        rate = entry.get("rate") if isinstance(entry, dict) else entry
        if isinstance(rate, bool) or not isinstance(rate, int) or rate < 1:
            raise RuntimeError(
                "A6 saturation validation: candidate rates.json has no integer "
                "saturation_rate['100000|p6|sat'].rate; refusing the max fallback "
                "(S5 §12 B1 / A6.2)")
        self.rate_mode = "saturation-validation"
        self.offered_rate = int(rate)
        return str(int(rate))

    def _start_load_generator(self) -> None:
        jar = os.path.join(self.r.repo, "artifact", "target", "wxeo-service.jar")
        java = self.env.get("java") or "java"
        rate = self._effective_rate()
        args = [
            java, "-Xms64m", "-Xmx1g", "-cp", jar, "com.wxeo.load.LoadGenerator",
            f"--run-id={self.run_id}",
            "--bootstrap=127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093",
            f"--topic={self.source_topic}",
            f"--events={self.run['load_events']}",
            f"--partitions={self.run['partitions']}",
            f"--rate={rate}",
            f"--data-seed-hex={self.seed_hex}",
            f"--injection-seed-hex={self.run['injection_seed']}",
            f"--gt-out={self.p.gt}",
            f"--manifest-out={self.p.manifest}",
            f"--produced-out={self.p.produced}",
            "--ack-timeout-ms=120000",
        ]
        self.loader_log = os.path.join(self.p.logs, "loadgenerator.stdout.log")
        stderr_path = os.path.join(self.p.logs, "loadgenerator.stderr.log")
        self.r.runlog.append(stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                             command="[load] " + " ".join(args), exit_status="RUNNING")
        self.load_proc = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=open(stderr_path, "wb"), text=True)
        self._loader_stdout_lines: list[str] = []

        def _reader():
            try:
                with open(self.loader_log, "w", encoding="utf-8", newline="") as fh:
                    for line in self.load_proc.stdout:
                        fh.write(line)
                        self._loader_stdout_lines.append(line)
            except Exception:  # noqa: BLE001
                pass

        self._loader_reader = threading.Thread(target=_reader, daemon=True)
        self._loader_reader.start()
        self.timings["t_load_start"] = utc_ms()
        # R4: wire the REAL phase window for the injector receipts before any fire can happen
        if self._inj_ctx is not None:
            self._inj_ctx.load_start_ms = self.timings["t_load_start"]
            # S4-owned injectors fire DURING the load, when the real end is not yet known; the
            # receipt carries the planned upper bound (tight for fixed-rate runs, a wide bound
            # for rate=max) and the runner enforces the REAL end post-run against the recorded
            # timings (see _collect_fault_receipts).
            expected_ms = int(self._expected_load_seconds() * 1000)
            upper_bound = max(int(expected_ms * 1.5), 30_000)
            self._inj_ctx.load_end_ms = int(self.timings["t_load_start"] + upper_bound)
            self._inj_ctx.extra["window_kind"] = "planned_upper_bound"
            self._inj_ctx.extra["consumers_alive"] = self._consumers_alive()

    def _wait_load_generator(self) -> None:
        if self.load_proc is None:
            raise RuntimeError("load generator not started")
        try:
            rc = self.load_proc.wait(timeout=7200)
        except subprocess.TimeoutExpired:
            self.load_proc.kill()
            raise RuntimeError("load generator timeout")
        self.timings["t_load_end"] = utc_ms()
        if hasattr(self, "_loader_reader"):
            self._loader_reader.join(timeout=30)
        self.loader_result = _parse_loader_result("".join(getattr(self, "_loader_stdout_lines",
                                                               [])))
        if self.loader_result:
            if isinstance(self.loader_result.get("load_start_ms"), int):
                self.timings["t_load_start"] = int(self.loader_result["load_start_ms"])
            if isinstance(self.loader_result.get("load_end_ms"), int):
                self.timings["t_load_end"] = int(self.loader_result["load_end_ms"])
        # R4: close the phase window with the loader-reported end and real consumer liveness
        if self._inj_ctx is not None:
            self._inj_ctx.load_end_ms = self.timings["t_load_end"]
            self._inj_ctx.extra["consumers_alive"] = self._consumers_alive()
        self.r.runlog.append(stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                             command="[load] load generator finished", exit_status=rc)
        if rc != 0:
            raise RuntimeError(f"load generator exited {rc}")

    # -- injector --------------------------------------------------------------------------
    def _start_injector_thread(self):
        if self.injector is None:
            return None

        def _fire():
            try:
                if self.injector.owns_receipts:
                    if self.run["fault"] in ("K2", "K5"):
                        self.injector.fire()      # internal phase marker waits
                    else:
                        marker = INJ.seeded_progress_marker(
                            self.run["injection_seed"], int(self.run["load_events"]))
                        self._inj_ctx.extra["progress_marker"] = marker
                        self._wait_progress_marker(marker)
                        self.injector.fire()
                else:
                    self.injector.wait_for_app_fired(timeout_s=300)
            except Exception as exc:  # noqa: BLE001 - recorded as an invalid run
                self.notes.append(f"injector: {type(exc).__name__}: {exc}")
                self.exclusions.append(EXCLUDE_NOT_FIRED)

        thread = threading.Thread(target=_fire, daemon=True)
        thread.start()
        return thread

    def _wait_progress_marker(self, marker: int, timeout_s: float = 300.0) -> None:
        if self._inj_ctx is None or self._inj_ctx.progress_probe is None:
            raise RuntimeError("progress_probe unavailable for the Level-2 trigger")
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                if int(self._inj_ctx.progress_probe()) >= int(marker):
                    return
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.05)
        raise RuntimeError(f"Level-2 progress marker {marker} never reached")

    def _consumers_alive(self) -> bool:
        """Real liveness of the consumer instances at phase time (R4)."""
        if self.processes:
            return all(proc.poll() is None for proc in self.processes.values())
        try:
            return bool(self.admin.group_exists(self.group))
        except Exception:  # noqa: BLE001
            return False

    def _expected_load_seconds(self) -> float:
        rate = self._effective_rate()
        if rate == "max":
            return 60.0
        try:
            return float(self.run["load_events"]) / float(rate)
        except (TypeError, ValueError):
            return 60.0

    def _receipts_for_record(self) -> list[dict]:
        """Receipts of the FINAL attempt only, deduplicated.

        Invalid reruns share the ``run_id`` (prereg §8.3), so the global fault log accumulates
        receipts from earlier attempts.  The run record must attribute exactly one fired/restored
        pair per injection; receipts are therefore restricted to the final attempt's wall-clock
        window and identical app receipts (a crash-restart can append the same fired line twice)
        are collapsed.
        """
        start = int(self.timings.get("t_start") or 0) - 5_000
        end = int(self.timings.get("t_end") or utc_ms()) + 5_000
        out = []
        seen = set()
        for rec in INJ.read_receipts(self.r.faults_log):
            if rec.get("run_id") != self.run_id:
                continue
            ts = rec.get("ts_utc")
            if isinstance(ts, str) and ts:
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    ts_ms = int(dt.timestamp() * 1000)
                except ValueError:
                    ts_ms = None
                if ts_ms is not None and not (start <= ts_ms <= end):
                    continue
            key = (rec.get("fault"), rec.get("event"), rec.get("detail"),
                   RCPT.classify(rec))
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
        return out

    def _collect_fault_receipts(self) -> None:
        receipts = self._receipts_for_record()
        cfg = INJ.failpoint_config_for_run(self._inj_ctx or self._injector_context()) or {}
        result = RCPT.validate_run_receipts(
            receipts, run_id=self.run_id,
            injector_mode=self.run.get("injector_mode", "none"),
            fault=self.run["fault"], fault_level=self.run["fault_level"],
            failpoint=cfg.get("name"), action=cfg.get("action"),
        )
        if not result["attested"]:
            self.exclusions.append(EXCLUDE_RECEIPTS)
            self.notes.append(result.get("reason") or "receipt attestation failed")
            return
        s4_fired = [rec for rec in receipts
                    if rec.get("run_id") == self.run_id and rec.get("event") == "fired"
                    and RCPT.classify(rec) == "s4"]
        real_start = self.timings.get("t_load_start")
        real_end = self.timings.get("t_load_end")
        for rec in s4_fired:
            phase = rec.get("phase") or {}
            fire_ms = phase.get("fire_ms")
            planned_end = phase.get("load_end_ms")
            outside = bool(phase.get("outside_window"))
            if isinstance(fire_ms, int) and isinstance(real_start, int) and \
                    isinstance(real_end, int):
                # R4: enforce the REAL load window (receipt carries the planned upper bound)
                if fire_ms < real_start or fire_ms > real_end + PHASE_MARGIN_MS:
                    outside = True
            if outside or (isinstance(fire_ms, int) and isinstance(planned_end, int)
                           and fire_ms > planned_end):
                self.exclusions.append(EXCLUDE_WINDOW)
                self.notes.append("fault fired outside the processing window "
                                  "(uninformative_fire_outside_window)")

    # -- quiescence ------------------------------------------------------------------------
    def _quiesce(self) -> dict:
        quiet_ms = self.r.opts.quiet_period_ms
        tmax = self.r.opts.quiesce_timeout_ms
        if self.run["mechanism"] == "M1-kafka":
            result = KA.wait_publication_quiescence(
                bootstrap="127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093",
                sink_topic=self.sink_topic, quiet_period_ms=quiet_ms, timeout_ms=tmax)
            self.timings["t_quiesce"] = utc_ms()
            return result
        relay_fn = self._relay_drained_fn() if self.run["mechanism"] in ("M2", "M2+M3") else None
        result = KA.wait_consumer_quiescence(
            self.admin, group=self.group, source_topic=self.source_topic,
            quiet_period_ms=quiet_ms, timeout_ms=tmax,
            sink_count_fn=self._sink_count,
            pg_idle_fn=self._pg_idle,
            relay_drained_fn=relay_fn,
        )
        self.timings["t_quiesce"] = utc_ms()
        return result

    def _sink_count(self) -> int | None:
        proc = self._psql(f"SELECT count(*) FROM {self.schema}.posting", "sink count",
                          check=False)
        try:
            return int((proc.stdout or "").strip())
        except ValueError:
            return None

    def _pg_idle(self) -> bool:
        proc = self._psql(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = 'wxeo' "
            "AND usename = 'wxeo_app' AND state IN ('active','idle in transaction',"
            "'idle in transaction (aborted)')", "pg_stat_activity", user="wxeo_read",
            check=False)
        try:
            return int((proc.stdout or "0").strip() or 0) == 0
        except ValueError:
            return False

    def _relay_drained_fn(self):
        def _drained() -> bool:
            proc = self._psql(f"SELECT count(*) FROM {self.schema}.outbox "
                              "WHERE published_at IS NULL", "relay drain", check=False)
            try:
                return int((proc.stdout or "0").strip() or 0) == 0
            except ValueError:
                return False
        return _drained

    # -- dumps + oracle --------------------------------------------------------------------
    def _dump(self) -> dict:
        out: dict = {}
        out["source"] = O.dump_source_topic(
            bootstrap="127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093",
            topic=self.source_topic, out_ndjson=self.p.source_dump,
            group=f"wxeo.dump.{self.run_id}.{self.attempt}")
        out["sink"] = None
        if self.db_schema_used:
            out["sink"] = O.dump_pg(
                pg_dsn=self._db_dsn_read(), schema=self.schema,
                out_csv=self.p.sink_dump, attestation_out=None)
            # S3 §8.9: attestation queried as the APPLICATION role via SchemaTool --mode=verify
            self._write_schema_attestation((out["sink"] or {}).get("db_rows"))
        isolation = ("read_committed"
                     if self.run["mechanism"] in ("M1-kafka", "M1x", "M1x+M3")
                     else "read_uncommitted")
        out["publication"] = O.dump_kafka_topic(
            bootstrap="127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093",
            topic=self.sink_topic, out_ndjson=self.p.publication_dump,
            group=f"wxeo.dump.{self.run_id}.{self.attempt}", isolation=isolation)
        out["publication_isolation"] = isolation
        return out

    def _write_schema_attestation(self, posting_rows: int | None) -> None:
        jar = os.path.join(self.r.repo, "artifact", "target", "wxeo-service.jar")
        proc = self.r.call(
            [self.env.get("java") or "java", "-cp", jar,
             "com.wxeo.sink.SchemaTool", "--mode=verify",
             "--db-url=jdbc:postgresql://127.0.0.1:5433/wxeo",
             "--db-user=wxeo_app", f"--schema={self.schema}",
             f"--mechanism={self.run['mechanism']}"],
            stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
            purpose="schema-attestation (app role)", check=False, timeout=120)
        checks: dict = {}
        try:
            doc = json.loads((proc.stdout or "").strip() or "{}")
            checks = doc.get("checks") or doc
        except Exception:  # noqa: BLE001
            checks = {}
        att = pathsafe.scrub({
            "can_update": checks.get("can_update"),
            "can_delete": checks.get("can_delete"),
            "posting_triggers": checks.get("append_only_triggers",
                                           checks.get("posting_triggers")),
            "posting_rows": (posting_rows if posting_rows is not None
                             else checks.get("posting_rows")),
            "app_role": "wxeo_app",
            "source": "artifact:SchemaTool --mode=verify",
            "schema": self.schema,
            "note": "queried as the application role (artifact/README §8.9)",
            "raw_checks": checks,
        })
        with open(self.p.attestation, "w", encoding="utf-8", newline="") as fh:
            json.dump(att, fh, indent=2)
            fh.write("\n")

    def _run_oracle(self) -> None:
        sink_kind = "kafka" if self.run["mechanism"] == "M1-kafka" else "db"
        oracle_out = os.path.join(self.r.oracle_dir, f"oracle_{self.run_id}.json")
        self.oracle_result = O.oracle_run(
            run_id=self.run_id, mechanism=self.run["mechanism"], sink_kind=sink_kind,
            master_seed_u64=self.seed_u64, n_events=int(self.run["load_events"]),
            partitions=int(self.run["partitions"]),
            gt_path=self.p.gt, source_dump_path=self.p.source_dump,
            produced_manifest_path=self.p.produced,
            sink_dump_path=self.p.sink_dump if sink_kind == "db" else None,
            publication_dump_path=self.p.publication_dump,
            attestation_path=self.p.attestation if sink_kind == "db" else None,
            out_path=oracle_out, shards=D.DEFAULT_SHARDS, workdir=self.p.oracle_work)
        if not self.oracle_result["oracle"]["source_integrity"]:
            self.exclusions.append(EXCLUDE_SOURCE)
            self.notes.append("source-integrity gate failed (prereg §8.2 item 2)")

    # -- endpoints and roles (artifact/README §8.2/§8.9) -----------------------------------
    def _endpoints(self) -> dict:
        """Service endpoints for this cell (S5 B7/A6 endpoint contract).

        Direct by default; the Kafka fault link is proxied: F3 -> producer F4a -> consumer,
        K2 (M1-kafka) -> producer, all via the contract's group-member ``kafka.fault_bootstrap``
        (127.0.0.1:19091, read from deploy/docker/endpoints.json, fail-closed if missing).
        F5 -> DB through db (15433); K2 for DB sinks -> db proxy. Non-fault/native unchanged.
        """
        if is_kafka_fault_path(self.run):
            contract = self.r.endpoint_contract(strict=True)
            fault_bootstrap = contract["fault_bootstrap"]
        else:
            fault_bootstrap = None
        return endpoints_for_run(self.run, fault_bootstrap)

    def _fault_role_dsn(self) -> str:
        return "host=127.0.0.1 port=5433 dbname=wxeo user=wxeo_fault"

    # -- performance pipeline (F-M1) -------------------------------------------------------
    def _performance_block(self) -> dict:
        """Warm-up-excluded latency percentiles, throughput, generator lag, time to consistent."""
        warmup = self._warmup_events()
        manifest_rows = _read_ndjson(self.p.produced) if os.path.exists(self.p.produced) else []
        result = {**{k: None for k in VR.PERF_KEYS},
                  "warmup_events_excluded": warmup,
                  "perf_window": {"start": None, "end": None},
                  "n_instances_merged": 0, "generator_lag_p99_ms": None,
                  "generator_lag_flag": None}
        # per-instance histogram percentiles (event histogram = primary)
        percentiles = _merge_hist_percentiles(self.p)
        if percentiles:
            for key in ("throughput_eps_median", "p50_ms", "p90_ms", "p99_ms", "p999_ms",
                        "max_ms"):
                result[key] = percentiles.get(key)
            result["n_instances_merged"] = percentiles.get("n_instances", 0)
            result["histogram_path"] = percentiles.get("histogram_path")
        # throughput from committed events in the steady-state window (loader ack times)
        result["committed_events"] = None
        if manifest_rows and self.loader_result:
            acks = [(int(r["ack_ts_ms"]), int(r["scheduled_at_ms"]))
                    for r in manifest_rows if r.get("ack_ts_ms") is not None
                    and r.get("scheduled_at_ms") is not None]
            acks.sort()
            steady = acks[warmup:] if len(acks) > warmup else []
            if len(steady) >= 2:
                t0, _ = steady[0]
                t1 = steady[-1][0]
                if t1 > t0:
                    result["throughput_eps_median"] = round(len(steady) / ((t1 - t0) / 1000.0), 3)
                    result["perf_window"] = {"start": t0, "end": t1,
                                             "events": len(steady)}
                    result["committed_events"] = int(len(steady))
            # generator lag covariate: actual send time vs scheduled time (fsync manifest field)
            lags = []
            for row in manifest_rows:
                send = row.get("send_ts_ms")
                sched = row.get("scheduled_at_ms")
                if isinstance(send, int) and isinstance(sched, int):
                    lags.append(send - sched)
            if lags:
                lags.sort()
                p99 = lags[min(len(lags) - 1, int(0.99 * len(lags)))]
                result["generator_lag_p99_ms"] = int(p99)
                result["generator_lag_flag"] = bool(p99 > 5)
        result["t_consistent_ms"] = self.quiescence.get("t_consistent_ms")
        result["censored"] = bool(self.quiescence.get("censored"))
        return result

    # -- analysis artefacts ----------------------------------------------------------------
    def _config_block(self) -> dict:
        """Full effective config from the app's config artefact (G6 contract), else stub."""
        config_file = os.path.join(self.p.outputs, f"config_{self.run_id}_1.json")
        doc = read_json(config_file)
        if doc:
            shared = doc.get("shared_sha256")
            intrinsic = doc.get("intrinsic_sha256")
            config_sha = doc.get("config_sha256")
            block = {
                "source": "app:config_<run>_1.json",
                "mechanism": self.run["mechanism"],
                "ordering_variant": self.run["ordering_variant"],
                "shared_sha256": shared,
                "intrinsic_sha256": intrinsic,
                "config_sha256": config_sha or sha256_file(config_file),
                "effective_config": doc,
            }
            return block
        stub = {
            "source": "stub:app config artefact missing",
            "mechanism": self.run["mechanism"],
            "ordering_variant": self.run["ordering_variant"],
            "load_events": int(self.run["load_events"]),
            "partitions": int(self.run["partitions"]),
        }
        stub["config_sha256"] = hashlib.sha256(
            json.dumps(stub, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return stub

    def _process_limits(self) -> dict:
        # A3.1: the host commit limit forced -Xms512m/-Xmx1g for the service and -Xmx1g for the
        # loader; the record must state what actually ran, not the original Appendix-C 2g values
        # (S5 R13).  Resource limits are read from the artifact's effective config and the live
        # process table so the same code works on Windows and Linux.
        limits = {
            "jvm_xms": "512m", "jvm_xmx": "1g", "jvm_gc": "G1",
            "affinity_mask": None, "priority_class": None, "hikari_pool": None,
            "broker_heap": "512m", "pg_shared_buffers": "2GB",
        }
        config_file = os.path.join(self.p.outputs, f"config_{self.run_id}_1.json")
        doc = read_json(config_file) or {}
        shared = doc.get("shared") or doc.get("effective_config") or {}
        try:
            limits["hikari_pool"] = (shared.get("hikari_pool_size")
                                     or shared.get("hikari", {}).get("pool_size"))
        except Exception:  # noqa: BLE001
            pass
        pids = list(self.consumer_pids.values())
        if pids:
            try:
                if os.name == "nt":
                    proc = subprocess.run(
                        [discover_pwsh(), "-NoProfile", "-Command",
                         ";".join(f"(Get-Process -Id {pid} -ErrorAction SilentlyContinue | "
                                  f"Select-Object -ExpandProperty ProcessorAffinity)"
                                  for pid in pids)],
                        capture_output=True, text=True, timeout=30)
                    limits["affinity_mask"] = (proc.stdout or "").strip() or None
                    proc2 = subprocess.run(
                        [discover_pwsh(), "-NoProfile", "-Command",
                         f"(Get-Process -Id {pids[0]}).PriorityClass"],
                        capture_output=True, text=True, timeout=30)
                    limits["priority_class"] = (proc2.stdout or "").strip() or None
                else:
                    masks = []
                    for pid in pids:
                        try:
                            masks.append(str(os.sched_getaffinity(pid)))
                        except Exception:  # noqa: BLE001
                            pass
                    limits["affinity_mask"] = ";".join(masks) if masks else None
                    try:
                        nice = os.getpriority(os.PRIO_PROCESS, pids[0])
                        limits["priority_class"] = f"nice={nice}"
                    except Exception:  # noqa: BLE001
                        pass
            except Exception:  # noqa: BLE001
                pass
        return limits

    # -- finish ----------------------------------------------------------------------------
    def _finish(self) -> dict:
        self.timings["t_end"] = utc_ms()
        self.disk_end = self.r._disk_footprint()
        self.scratch_bytes = self.r._dir_bytes(self.p.scratch)
        oracle = self.oracle_result
        if oracle is None:
            raise RuntimeError("oracle result missing")
        self.clock_after = clock_sanity_probe()
        clock_ok = bool(self.clock_before and self.clock_before["ok"]
                        and self.clock_after and self.clock_after["ok"]
                        and self.clock_before.get("boot_utc") == self.clock_after.get("boot_utc"))
        if not clock_ok:
            self.exclusions.append(EXCLUDE_CLOCK)
            self.notes.append("clock sanity probe failed (step or boot change during run)")
        valid = not self.exclusions
        exclude_reason = self.exclusions[0] if self.exclusions else None
        performance = self._performance_block()
        hist_json = os.path.join(self.p.outputs, f"hist_{self.run_id}_1.json")
        archived, _manifest = self.r._archive_run(self.run_id, {
            "gt_manifest": self.p.manifest,
            "gt": self.p.gt,
            "source_dump": self.p.source_dump,
            "sink_dump": self.p.sink_dump,
            "publication_dump": self.p.publication_dump,
            "attestation": self.p.attestation,
            "oracle": os.path.join(self.r.oracle_dir, f"oracle_{self.run_id}.json"),
            "hist_percentiles": hist_json if os.path.exists(hist_json) else None,
            "config": os.path.join(self.p.outputs, f"config_{self.run_id}_1.json"),
            "app": os.path.join(self.p.outputs, f"app_{self.run_id}_1.json"),
            "loader_result": self.p.loader_result if os.path.exists(self.p.loader_result)
            else None,
        }, retain_map=self._retention_map(oracle))
        record = self.r._base_record(self.run, self.preflight)
        config_block = self._config_block()
        config_block["runner"] = self.r._lifecycle_block(self, getattr(self, "reset_info", {}))
        record.update({
            "config": config_block,
            "process_limits": self._process_limits(),
            "timings": self.timings,
            "quiescence": {
                "lag_per_partition": {str(k): v for k, v in
                                      (self.quiescence.get("lag_per_partition") or {}).items()},
                "quiet_period_ms": self.quiescence.get("quiet_period_ms"),
                "censored": bool(self.quiescence.get("censored")),
                "t_consistent_ms": self.quiescence.get("t_consistent_ms"),
                "checks": self.quiescence.get("checks"),
            },
            "performance": performance,
            "counts": oracle["counts"],
            "violations": oracle["violations"],
            "publication": oracle["publication"],
            "oracle": oracle["oracle"],
            "fault_receipts": self._receipts_for_record(),
            "validity": {
                "valid": valid,
                "excluded": not valid,
                "exclude_reason": exclude_reason,
                "residue_check": EXCLUDE_RESIDUE not in self.exclusions,
                "clock_check": clock_ok,
                "disk_free_start_gb": free_space_gb(eos_runtime_root()),
                "notes": self.notes or ["live run"],
            },
            "artifacts": archived,
            "stdout_path": pathsafe.repo_rel(getattr(
                self, "loader_log",
                os.path.join(self.p.logs, "loadgenerator.stdout.log"))),
            "stderr_path": pathsafe.repo_rel(os.path.join(self.p.logs,
                                                          "loadgenerator.stderr.log")),
            "exit_status": {
                "consumer": dict(self._consumer_exit),
                "load_generator": (self.load_proc.returncode if self.load_proc else None),
            },
        })
        record = pathsafe.scrub(record)
        pathsafe.assert_clean(record, f"run record {self.run_id}")
        errors = VR.validate_record(record)
        if errors:
            raise RuntimeError("internal bug: live run record fails validation:\n"
                               + "\n".join(errors))
        record_path = os.path.join(self.r.records_dir, f"run_{self.run_id}.json")
        with open(record_path, "w", encoding="utf-8", newline="") as fh:
            json.dump(record, fh, indent=2)
            fh.write("\n")
        self._drop_state()
        if not valid:
            return {"status": "invalid", "reason": exclude_reason,
                    "record": pathsafe.repo_rel(record_path)}
        if record["quiescence"]["censored"]:
            return {"status": "censored", "record": pathsafe.repo_rel(record_path),
                    "violating_run": True}
        return {"status": "valid", "record": pathsafe.repo_rel(record_path),
                "violating_run": record["violations"]["violating_run"]}

    def _retention_map(self, oracle: dict) -> dict:
        violating = bool((oracle or {}).get("violations", {}).get("violating_run"))
        control = self.run.get("fault") in ("F0", "none", "ctrl")
        sample = int(self.run.get("rep", -1)) in (0, 5, 10, 20)
        keep = bool(violating or control or sample)
        return {"gt": keep, "source_dump": keep, "sink_dump": keep, "publication_dump": keep}

    # -- cleanup ---------------------------------------------------------------------------
    def _psql(self, sql: str, purpose: str, *, user: str = "wxeo_app",
              check: bool = True) -> subprocess.CompletedProcess:
        psql = self.env.get("psql") or "psql"
        cmd = [psql, "-h", "127.0.0.1", "-p", "5433", "-U", user, "-d", "wxeo", "-tAc", sql]
        return self.r.call(cmd, stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                           purpose=purpose, check=check, timeout=120)

    def _db_dsn_read(self) -> str:
        return "host=127.0.0.1 port=5433 dbname=wxeo user=wxeo_read"

    def _db_dsn_admin(self) -> str:
        return "host=127.0.0.1 port=5433 dbname=wxeo user=postgres"

    def _drop_state(self) -> None:
        if self.db_schema_used:
            jar = os.path.join(self.r.repo, "artifact", "target", "wxeo-service.jar")
            self.r.call([self.env.get("java") or "java", "-cp", jar,
                         "com.wxeo.sink.SchemaTool", "--mode=drop",
                         "--db-url=jdbc:postgresql://127.0.0.1:5433/wxeo",
                         "--db-user=postgres", f"--schema={self.schema}"],
                        stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                        purpose="schema-drop", check=False, timeout=120)
        # M5: topics are NEVER deleted per run (fatal Windows KRaft topic-dir rename bug).
        # Fresh state is provided by unique attempt-scoped topic names + fresh group + fresh
        # schema; accumulated topics/segments are reclaimed by periodic `reset-kafka`.
        self.notes.append(f"topics retained (M5): {self.source_topic}, {self.sink_topic}; "
                          f"reclaimed by periodic reset-kafka")
        self.r._append_summary("disk-footprint", {
            "utc": utc_iso(), "run_id": self.run_id, "load_events": int(self.run["load_events"]),
            "attempt": self.attempt,
            "disk_free_start_gb": self.disk_start.get("free_gb"),
            "disk_free_end_gb": self.disk_end.get("free_gb"),
            "scratch_bytes": self.scratch_bytes,
            "topics_live_after": self.r._live_topic_count(),
            "git_sha": self.r.runlog.sha})
        shutil.rmtree(self.p.scratch, ignore_errors=True)

    def _emergency_cleanup(self) -> None:
        try:
            if self.injector is not None:
                self.injector.restore()
        except Exception:  # noqa: BLE001
            pass
        try:
            if self.processes:
                for proc in self.processes.values():
                    try:
                        proc.kill()
                    except Exception:  # noqa: BLE001
                        pass
        except Exception:  # noqa: BLE001
            pass
        try:
            self._drop_state()
        except Exception:  # noqa: BLE001
            pass


class RunnerPaths:
    """Per-run scratch/output paths (local non-synced scratch; D11)."""

    def __init__(self, run_id: str, scratch_root: str | None = None, attempt: int = 1):
        root = os.path.join(scratch_root or os.path.join(eos_runtime_root(), "runs"),
                            run_id, f"attempt{attempt}")
        self.scratch = root
        self.outputs = os.path.join(root, "outputs")
        self.logs = os.path.join(root, "logs")
        self.oracle_work = os.path.join(root, "oracle")
        os.makedirs(self.outputs, exist_ok=True)
        os.makedirs(self.logs, exist_ok=True)
        self.gt = os.path.join(root, f"gt_{run_id}.ndjson")
        self.manifest = os.path.join(root, f"manifest_{run_id}.json")
        self.produced = os.path.join(root, f"produced_{run_id}.ndjson")
        self.source_dump = os.path.join(root, f"source_dump_{run_id}.ndjson")
        self.sink_dump = os.path.join(root, f"sink_dump_{run_id}.csv")
        self.publication_dump = os.path.join(root, f"publication_{run_id}.ndjson")
        self.attestation = os.path.join(root, f"attestation_{run_id}.json")
        self.loader_result = os.path.join(root, f"loader_result_{run_id}.json")


# --------------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------------
def _read_ndjson(path: str) -> list[dict]:
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except OSError:
        return []
    return rows


def _parse_loader_result(stdout: str) -> dict | None:
    """Parse the loader's printed result JSON (last JSON object in stdout)."""
    candidates = []
    depth = 0
    start = None
    for index, ch in enumerate(stdout or ""):
        if ch == "{":
            if depth == 0:
                start = index
            depth += 1
        elif ch == "}":
            depth = max(0, depth - 1)
            if depth == 0 and start is not None:
                candidates.append(stdout[start:index + 1])
                start = None
    for text in reversed(candidates):
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and ("events" in obj or "load_end_ms" in obj
                                     or "acknowledged" in obj):
            return obj
    return None


def _merge_hist_percentiles(paths: "RunnerPaths") -> dict | None:
    """Merge the per-instance HdrHistogram percentile files (instance count weighted).

    The service writes one ``hist_<run>_<instance>.json`` per instance with event-histogram
    percentiles and count. Exact multi-instance percentile merging requires the raw histogram;
    for the campaign we use the count-weighted interpolation across instance percentile points
    and record the method. If a merged artefact from the artifact's ``HistMerge`` tool is
    present, it takes precedence.
    """
    merged_path = os.path.join(paths.outputs, "hist_merged.json")
    if os.path.exists(merged_path):
        doc = read_json(merged_path, {}) or {}
        if "event" in doc:
            doc = doc["event"]
        return {"p50_ms": doc.get("p50_ms"), "p90_ms": doc.get("p90_ms"),
                "p99_ms": doc.get("p99_ms"), "p999_ms": doc.get("p999_ms"),
                "max_ms": doc.get("max_ms"), "throughput_eps_median": None,
                "n_instances": doc.get("n_instances"), "histogram_path": merged_path,
                "method": "artifact:hist_merged.json"}
    per_instance = []
    for path in sorted(glob(os.path.join(paths.outputs, "hist_*_event.json"))):
        doc = read_json(path, {})
        if doc:
            per_instance.append(doc)
    if not per_instance:
        per_instance = [read_json(p, {}) for p in
                        sorted(glob(os.path.join(paths.outputs, "hist_*.json")))
                        if not p.endswith("_event.json")]
        per_instance = [d for d in per_instance if d and "event" in d]
    if not per_instance:
        return None
    total = sum(int(d.get("count") or 0) for d in per_instance) or 0
    if total == 0:
        return None

    def weighted(key: str):
        num = 0.0
        for doc in per_instance:
            src = doc.get("event") if isinstance(doc.get("event"), dict) else doc
            value = src.get(key)
            count = int(doc.get("count") or src.get("count") or 0)
            if isinstance(value, (int, float)) and count:
                num += float(value) * count
        return round(num / total, 3)

    return {
        "p50_ms": weighted("p50_ms"), "p90_ms": weighted("p90_ms"),
        "p99_ms": weighted("p99_ms"), "p999_ms": weighted("p999_ms"),
        "max_ms": max((float((d.get("event") or d).get("max_ms") or 0))
                      for d in per_instance),
        "throughput_eps_median": None, "n_instances": len(per_instance),
        "histogram_path": pathsafe.repo_rel(os.path.dirname(per_instance[0].get("_path", "")))
        if per_instance[0].get("_path") else None,
        "method": "count-weighted interpolation across per-instance percentile files",
    }


# --------------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="WXEO experiment runner")
    p.add_argument("--stage", default="pilot",
                   choices=["pilot", "M7cal", "M7sat", "A", "B", "C", "all"])
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--run-id", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--fixture", default=None)
    p.add_argument("--results-dir", default=os.path.join(_REPO, "results"))
    p.add_argument("--scratch-dir", default=None)
    p.add_argument("--rate", default="max")
    p.add_argument("--no-resume", action="store_true")
    p.add_argument("--runlog", default=None)
    p.add_argument("--write-deviations", action="store_true")
    p.add_argument("--check-env", action="store_true")
    p.add_argument("--quiet-period-ms", type=int, default=QUIET_PERIOD_MS)
    p.add_argument("--quiesce-timeout-ms", type=int, default=QUIESCE_TIMEOUT_MS)
    p.add_argument("--warmup-events", type=int, default=None)
    p.add_argument("--no-freeze-check", action="store_true")
    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.write_deviations:
        runner = Runner(RunnerOptions(results_dir=args.results_dir))
        path = runner.write_deviations()
        print(json.dumps({"deviations": pathsafe.repo_rel(path)}, indent=2))
        return 0
    if args.check_env:
        runner = Runner(RunnerOptions(results_dir=args.results_dir))
        print(json.dumps({"interface": runner.check_env(),
                          "freeze": runner.verify_freeze(), "host": host_block()}, indent=2))
        return 0
    opts = RunnerOptions(
        stage=args.stage, limit=args.limit, run_id=args.run_id, dry_run=args.dry_run,
        fixture=args.fixture, results_dir=args.results_dir,
        scratch_dir=args.scratch_dir or "", rate=args.rate, resume=not args.no_resume,
        quiet_period_ms=args.quiet_period_ms,
        quiesce_timeout_ms=args.quiesce_timeout_ms,
        warmup_events=args.warmup_events,
        verify_freeze=not args.no_freeze_check,
    )
    if args.runlog:
        opts.runlog_path = args.runlog
    elif args.dry_run:
        opts.runlog_path = os.path.join(_REPO, "results", "RUNLOG.md")
    runner = Runner(opts)
    if args.dry_run:
        out = runner.run_dry()
    else:
        out = runner.run_live()
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
