#!/usr/bin/env python3
"""Experiment runner: exact per-run sequence of docs/prereg.md §4.1 (S4-owned, post-S5 v2).

Two modes
---------
``--dry-run --fixture DIR``
    Executes the complete orchestration path against a synthetic fixture bundle: matrix row ->
    oracle -> run record -> artefact archive + digest manifest -> RUNLOG entry.  No
    Kafka/PostgreSQL/Toxiproxy is touched.  Records are schema-valid but labelled ``DRY-RUN`` in
    ``validity.notes`` and written outside ``results/raw/``.

``--stage pilot|A|B|C`` (live)
    Real native stack, exactly as frozen.  Post-S5 fix-pass changes:

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
from experiments.fixtures import make_fixtures as FX  # noqa: E402
from experiments.oracle import derive as D  # noqa: E402
from experiments.oracle import oracle as O  # noqa: E402

QUIET_PERIOD_MS = 10_000          # prereg §4.6 / method §7.3 (T_quiet)
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
EXCLUDE_WINDOW = "uninformative_fire_outside_window"

CLOSED_EXCLUSIONS = {
    EXCLUDE_RECEIPTS, EXCLUDE_NOT_FIRED, EXCLUDE_SOURCE, EXCLUDE_RESIDUE, EXCLUDE_CLOCK,
    EXCLUDE_DISK, EXCLUDE_CONFIG, EXCLUDE_START, EXCLUDE_OUTCOME, EXCLUDE_WINDOW,
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
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return default


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


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
    except Exception:  # noqa: BLE001 - best effort host record
        ram = None
    return {
        "os_build": f"{platform.system()} {platform.release()} ({platform.version()})",
        "cpu_model": os.environ.get("PROCESSOR_IDENTIFIER", platform.processor() or "unknown"),
        "logical_cpus": os.cpu_count(),
        "ram_gb": ram,
        "disk_free_start_gb": free_space_gb(eos_runtime_root()),
    }


def version_block() -> dict:
    versions = {
        "java": None, "kafka": None, "postgresql": None, "toxiproxy": None,
        "powershell": None, "python": sys.version.split()[0],
        "python_executable": sys.executable,
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
    def preflight(self) -> dict:
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
        self._write_reserved_summary()
        return {"free_gb": free, "interface": interface, "host": host_block(),
                "versions": version_block(), "git_sha": self.runlog.sha,
                "freeze": freeze_report, "clock": clock}

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
        return {"ok": not drift, "drift": drift,
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
        kafka_home = discover_tool(["kafka_2.*/libs", "kafka/libs"]) or ""
        if kafka_home:
            kafka_home = os.path.dirname(kafka_home.rstrip("\\/"))
        pg_bin = discover_tool(["pgsql/bin/psql.exe", "**/psql.exe"])
        toxiproxy = discover_tool(["toxiproxy-server*.exe"])
        return {
            "service_jar": {"path": "artifact/target/wxeo-service.jar",
                            "present": os.path.exists(jar)},
            "native_stack_ps1": {"path": "artifact/harness/native-stack.ps1",
                                 "present": os.path.exists(harness)},
            "stack_state": {"path": "<scratch>/run/stack-state.json",
                            "present": os.path.exists(stack_state)},
            "kafka_home": "<scratch>/vendor/kafka" if kafka_home else None,
            "psql": "<scratch>/vendor/pgsql/bin/psql.exe" if pg_bin else None,
            "toxiproxy": "<scratch>/vendor/bin/toxiproxy-server" if toxiproxy else None,
            "java": ("<scratch>/vendor/jdk21/bin/java" if discover_java() else None),
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

    def _attempt_loop(self, run: dict, preflight: dict) -> dict:
        """prereg §8.3: invalid attempts are rerun up to R_max with the same (cell, rep) seeds."""
        attempts = []
        final = None
        for attempt in range(1, RERUN_CAP + 1):
            self._restart_epoch = self._restart_epoch if hasattr(self, "_restart_epoch") else 0
            outcome = self._attempt_once(run, preflight, attempt)
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

    def _attempt_once(self, run: dict, preflight: dict, attempt: int) -> dict:
        try:
            seq = LiveSequence(self, run, preflight, attempt)
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
        preflight = self.preflight()
        results = []
        completed = 0
        for run in runs:
            record_path = os.path.join(self.records_dir, f"run_{run['run_id']}.json")
            if self.opts.resume and os.path.exists(record_path):
                existing = read_json(record_path, {})
                if (existing.get("validity") or {}).get("valid"):
                    results.append({"run_id": run["run_id"], "skipped": "valid record exists"})
                    continue
            completed += 1
            if completed % RESTART_EVERY_N_RUNS == 0:
                self._full_stack_restart(run)
            results.append(self._attempt_loop(run, preflight))
        return {"mode": "live", "runs": results, "preflight": preflight,
                "attempts_summary": pathsafe.repo_rel(
                    os.path.join(self.summaries_dir, "attempts.csv"))}

    def _full_stack_restart(self, run: dict) -> None:
        harness = os.path.join(self.repo, "artifact", "harness", "native-stack.ps1")
        self.call([discover_pwsh(), "-File", harness, FULL_STACK_RESTART_VERB],
                  stage=run["stage_label"], run_id=run["run_id"],
                  seed=run["data_seed"], purpose="restart-all", timeout=900)
        self._restart_epoch += 1
        restart_log = os.path.join(self.results_dir, "fault_restart.log")
        with open(restart_log, "a", encoding="utf-8", newline="") as fh:
            fh.write(json.dumps({"ts_utc": utc_iso(), "action": "restart-all",
                                 "after_runs": RESTART_EVERY_N_RUNS,
                                 "epoch": self._restart_epoch,
                                 "git_sha": self.runlog.sha}) + "\n")

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

    def __init__(self, runner: Runner, run: dict, preflight: dict, attempt: int):
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
        self.source_topic = f"wxeo.src.{self.run_id}"
        self.sink_topic = f"wxeo.sink.{self.run_id}"
        self.group = f"wxeo.cg.{self.run_id}"
        self.schema = f"wxeo_run_{self.run_id}"
        self.db_schema_used = self.run["mechanism"] != "M1-kafka"
        self.injector = None
        self.oracle_result = None
        self.exclusions: list[str] = []
        self.notes: list[str] = []
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
            failpoint_after = INJ.seeded_failpoint_after(self._empty_ctx())
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
        self.injector = INJ.make_injector(self._injector_context())
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
                         "--read-role=wxeo_read"],
                        stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                        purpose="schema-init", check=True, timeout=180)

    # -- services --------------------------------------------------------------------------
    def _service_args(self, instance: int) -> list[str]:
        jar = os.path.join(self.r.repo, "artifact", "target", "wxeo-service.jar")
        java = self.env.get("java") or "java"
        warmup = self._warmup_events()
        args = [
            java, "-Xms2g", "-Xmx2g", "-XX:+UseG1GC", "-XX:+ExitOnOutOfMemoryError",
            "-jar", jar,
            f"--wxeo.mechanism={self.run['mechanism']}",
            f"--wxeo.ordering={self.run['ordering_variant']}",
            f"--wxeo.run-id={self.run_id}",
            f"--wxeo.instance-id={instance}",
            "--wxeo.bootstrap=127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093",
            "--wxeo.db-url=jdbc:postgresql://127.0.0.1:15433/wxeo",
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
        fp = INJ.failpoint_config_for_run(self._injector_context())
        if fp and fp.get("enabled"):
            args += ["--wxeo.failpoint.enabled=true", f"--wxeo.failpoint.name={fp['name']}",
                     f"--wxeo.failpoint.action={fp['action']}",
                     f"--wxeo.failpoint.after={fp['after']}"]
        else:
            args.append("--wxeo.failpoint.enabled=false")
        return args

    def _warmup_events(self) -> int:
        if self.r.opts.warmup_events is not None:
            return int(self.r.opts.warmup_events)
        return max(2_000, int(int(self.run["load_events"]) * 0.10))

    def _start_services(self) -> None:
        n = int(self.run["consumer_instances"])
        for instance in range(1, n + 1):
            if instance in self.consumer_pids:
                continue
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
        """Readiness = assigned partitions AND zero lag for 5 s (prereg §4.1 step 3)."""
        deadline = time.monotonic() + timeout
        n = int(self.run["consumer_instances"])
        lag_zero_since: float | None = None
        while time.monotonic() < deadline:
            assigned = 0
            for instance in range(1, n + 1):
                info = read_json(os.path.join(self.p.outputs,
                                              f"ready_{self.run_id}_{instance}.json"))
                if info and info.get("status") == "assigned":
                    assigned += 1
            lag = self.admin.lag(self.group, self.source_topic)
            lag_zero = bool(lag) and all(v == 0 for v in lag.values())
            if assigned == n and lag_zero:
                lag_zero_since = lag_zero_since or time.monotonic()
                if (time.monotonic() - lag_zero_since) * 1000 >= READY_STABLE_MS:
                    self.r.runlog.append(stage=self.stage, run_id=self.run_id,
                                         seed=self.seed_hex,
                                         command="[ready] group assigned, lag=0 for 5s",
                                         exit_status=0)
                    return
            else:
                lag_zero_since = None
            time.sleep(0.5)
        raise RuntimeError(f"consumer readiness timeout: assigned={assigned}/{n} lag={lag}")

    def _restart_service(self, instance: int) -> int:
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
        self._start_services()
        return self.consumer_pids.get(instance, -1)

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
                              user="postgres", check=False)
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
                user="postgres", check=False)
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
    def _effective_rate(self) -> str:
        mode = self.run.get("load_mode")
        if self.opts_rate_table_missing():
            if mode == "saturation" or self.stage in ("B", "C"):
                raise RuntimeError(
                    "experiments/rates.json is missing: fixed-rate/saturation modes are "
                    "pre-registered (prereg §6.2) and may not fall back to 'max' (S5 F-I3)")
            return "max"
        table = read_json(self.r.opts.rate_table, {}) or {}
        if mode == "saturation":
            return str(table.get("saturation_rate", "max"))
        key = f"{self.run['load_events']}|p{self.run['partitions']}"
        return str((table.get("fixed_rate") or {}).get(key, self.r.opts.rate))

    def opts_rate_table_missing(self) -> bool:
        return not os.path.exists(self.r.opts.rate_table)

    def _start_load_generator(self) -> None:
        jar = os.path.join(self.r.repo, "artifact", "target", "wxeo-service.jar")
        java = self.env.get("java") or "java"
        rate = self._effective_rate()
        args = [
            java, "-cp", jar, "com.wxeo.load.LoadGenerator",
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
        self.r.runlog.append(stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                             command="[load] load generator finished", exit_status=rc)
        if rc != 0:
            raise RuntimeError(f"load generator exited {rc}")

    # -- injector --------------------------------------------------------------------------
    def _start_injector_thread(self) -> threading.Thread:
        if self.injector is None:
            return threading.Thread(target=lambda: None)

        def _fire():
            try:
                if self.injector.owns_receipts:
                    if self.run["fault"] in ("K2", "K5"):
                        self.injector.fire()      # internal phase marker waits
                    else:
                        duration = self._expected_load_seconds()
                        delay = INJ.level2_trigger_time(
                            self._injector_context(), load_duration_s=duration)
                        time.sleep(max(0.0, delay))
                        self.injector.fire()
                else:
                    self.injector.wait_for_app_fired(timeout_s=300)
            except Exception as exc:  # noqa: BLE001 - recorded as an invalid run
                self.notes.append(f"injector: {type(exc).__name__}: {exc}")
                self.exclusions.append(EXCLUDE_NOT_FIRED)

        thread = threading.Thread(target=_fire, daemon=True)
        thread.start()
        return thread

    def _expected_load_seconds(self) -> float:
        rate = self._effective_rate()
        if rate == "max":
            return 60.0
        try:
            return float(self.run["load_events"]) / float(rate)
        except (TypeError, ValueError):
            return 60.0

    def _collect_fault_receipts(self) -> None:
        receipts = INJ.read_receipts(self.r.faults_log)
        cfg = INJ.failpoint_config_for_run(self._injector_context()) or {}
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
        for rec in s4_fired:
            phase = rec.get("phase") or {}
            if phase.get("outside_window") or (
                    isinstance(phase.get("fire_ms"), int) and
                    isinstance(phase.get("load_end_ms"), int) and
                    phase["fire_ms"] > phase["load_end_ms"] - PHASE_MARGIN_MS):
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
        if self.db_schema_used:
            out["sink"] = O.dump_pg(
                pg_dsn=self._db_dsn_read(), schema=self.schema,
                out_csv=self.p.sink_dump, attestation_out=self.p.attestation,
                app_role="wxeo_app")
        isolation = ("read_committed"
                     if self.run["mechanism"] in ("M1-kafka", "M1x", "M1x+M3")
                     else "read_uncommitted")
        out["publication"] = O.dump_kafka_topic(
            bootstrap="127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093",
            topic=self.sink_topic, out_ndjson=self.p.publication_dump,
            group=f"wxeo.dump.{self.run_id}.{self.attempt}", isolation=isolation)
        out["publication_isolation"] = isolation
        return out

    def _run_oracle(self) -> None:
        sink_kind = "kafka" if self.run["mechanism"] == "M1-kafka" else "db"
        oracle_out = os.path.join(self.r.oracle_dir, f"oracle_{self.run_id}.json")
        self.oracle_result = O.oracle_run(
            run_id=self.run_id, mechanism=self.run["mechanism"], sink_kind=sink_kind,
            master_seed_u64=self.seed_u64, n_events=int(self.run["load_events"]),
            partitions=int(self.run["partitions"]),
            gt_path=self.p.gt, source_dump_path=self.p.source_dump,
            produced_manifest_path=self.p.manifest,
            sink_dump_path=self.p.sink_dump if sink_kind == "db" else None,
            publication_dump_path=self.p.publication_dump,
            attestation_path=self.p.attestation if sink_kind == "db" else None,
            out_path=oracle_out, shards=D.DEFAULT_SHARDS, workdir=self.p.oracle_work)
        if not self.oracle_result["oracle"]["source_integrity"]:
            self.exclusions.append(EXCLUDE_SOURCE)
            self.notes.append("source-integrity gate failed (prereg §8.2 item 2)")

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
        limits = {
            "jvm_xms": "2g", "jvm_xmx": "2g", "jvm_gc": "G1",
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
                proc = subprocess.run(
                    [discover_pwsh(), "-NoProfile", "-Command",
                     ";".join(f"(Get-Process -Id {pid} -ErrorAction SilentlyContinue | "
                              f"Select-Object -ExpandProperty ProcessorAffinity)" for pid in pids)],
                    capture_output=True, text=True, timeout=30)
                limits["affinity_mask"] = (proc.stdout or "").strip() or None
                proc2 = subprocess.run(
                    [discover_pwsh(), "-NoProfile", "-Command",
                     f"(Get-Process -Id {pids[0]}).PriorityClass"],
                    capture_output=True, text=True, timeout=30)
                limits["priority_class"] = (proc2.stdout or "").strip() or None
            except Exception:  # noqa: BLE001
                pass
        return limits

    # -- finish ----------------------------------------------------------------------------
    def _finish(self) -> dict:
        self.timings["t_end"] = utc_ms()
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
        record.update({
            "config": self._config_block(),
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
            "fault_receipts": [r for r in INJ.read_receipts(self.r.faults_log)
                               if r.get("run_id") == self.run_id],
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
            "stdout_path": pathsafe.repo_rel(self.p.loader_log),
            "stderr_path": pathsafe.repo_rel(os.path.join(self.p.logs,
                                                          "loadgenerator.stderr.log")),
            "exit_status": {
                "consumer": {inst: proc.returncode for inst, proc in self.processes.items()},
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
        for topic in (self.source_topic, self.sink_topic):
            try:
                self.admin.delete_topic(topic)
                self.admin.wait_topic_absent(topic, timeout=120)
            except Exception as exc:  # noqa: BLE001
                self.notes.append(f"topic drop failed: {exc}")
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
    p.add_argument("--stage", default="pilot", choices=["pilot", "A", "B", "C", "all"])
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
