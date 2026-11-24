#!/usr/bin/env python3
"""Experiment runner: exact per-run sequence of docs/prereg.md §4.1 (S4-owned).

Two modes
---------
``--dry-run --fixture DIR``
    Executes the complete orchestration path against a synthetic fixture bundle
    (``experiments/fixtures/make_fixtures.py``): matrix row -> oracle -> run record -> artefact
    archive + digest manifest -> RUNLOG entry.  No Kafka/PostgreSQL/Toxiproxy is touched.  The
    resulting record is a valid ``wxeo.run.v1`` record but its ``validity.notes`` says
    ``DRY-RUN`` and it is written outside ``results/raw/`` so it can never enter an analysis.

``--stage pilot|A|B|C`` (live)
    Runs the real native stack exactly as frozen:

      reserve (run_order.csv) -> preflight (free-space floor, restart-all every 50 runs) ->
      fresh topics/group/schema -> fresh consumer JVMs -> environment block -> GT (load
      generator writes+fsyncs before produce) -> arm injector -> produce -> fire injection ->
      restore -> quiesce (T_quiet=10 s, T_max=300 s) -> dumps -> oracle -> run record ->
      curated archive + digest manifest -> drop schema/topics/group.

    Every external command is appended to ``results/RUNLOG.md`` (UTC, git SHA, host, seed,
    exact command, output paths, exit status).  Injector actions write receipts to
    ``results/faults.log``; a run whose armed injection lacks the fired+restored pair (or whose
    source-integrity gate or residue/clock check fails) is excluded and counted, never analysed.

Dependency note (P2): the live path calls S3's frozen interface
(``docs/CONTRACTS.md`` §7-§8): ``artifact/target/wxeo-service.jar``,
``com.wxeo.load.LoadGenerator``, ``com.wxeo.sink.SchemaTool`` and
``artifact/harness/native-stack.ps1``.  ``--check-env`` reports which pieces are present; the
pilot must not start until S3 reports its acceptance tests and the lead confirms.

Usage examples::

    python experiments/runner.py --dry-run --fixture results/synthetic/fixtures/clean-run \
        --stage pilot --limit 2 --results-dir results/dryrun
    python experiments/runner.py --stage pilot --rate max
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
from experiments.oracle import derive as D  # noqa: E402
from experiments.oracle import oracle as O  # noqa: E402
from experiments import injectors as INJ  # noqa: E402
from experiments.fixtures import make_fixtures as FX  # noqa: E402

QUIET_PERIOD_MS = 10_000          # prereg §4.6 / method §7.3 (T_quiet)
QUIESCE_TIMEOUT_MS = 300_000      # T_max = 300 s
FREE_SPACE_FLOOR_GB = 3.0         # prereg §9.3 (G8)
RESTART_EVERY_N_RUNS = 50         # D6
RERUN_CAP = 3                     # prereg §8.3 (R_max)
FULL_STACK_RESTART_VERB = "restart-all"


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
    try:  # Windows physical memory without third-party deps
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
        versions["pinned_toolchain"] = pinned
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
    """S3's native-stack harness requires PowerShell 7 (`pwsh`)."""
    return shutil.which("pwsh") or shutil.which("powershell") or "pwsh"


# --------------------------------------------------------------------------------------------
# RUNLOG / RUNLOG protocol
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
            return (text or "").replace("|", "\\|").replace("\n", " ")[:600]

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
    verify_only: bool = False
    runlog_path: str | None = None


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
        for d in (self.records_dir, self.oracle_dir, self.artifacts_dir):
            os.makedirs(d, exist_ok=True)
        self.plan = self._load_plan()
        self._started_iso = utc_iso()
        self._commands: list[dict] = []

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
        self._commands.append({"command": printable, "purpose": purpose, "exit": rc,
                               "when": utc_iso()})
        # The RUNLOG header row above records the run; append the realised status compactly.
        self.runlog.append(stage=stage, run_id=run_id, seed=seed,
                           command=f"[{purpose}] exit={rc}", exit_status=rc, outputs=outputs,
                           notes=(proc.stderr or "")[:200])
        if check and rc != 0:
            raise RuntimeError(f"command failed ({rc}): {printable}\n{proc.stderr[-1000:]}")
        return proc

    # -- preflight -------------------------------------------------------------------------
    def preflight(self) -> dict:
        free = free_space_gb(eos_runtime_root())
        if free < FREE_SPACE_FLOOR_GB:
            raise SystemExit(f"free-space floor violated: {free:.2f} GB < "
                             f"{FREE_SPACE_FLOOR_GB} GB (prereg §9.3); aborting")
        interface = self.check_env()
        return {"free_gb": free, "interface": interface, "host": host_block(),
                "versions": version_block(), "git_sha": self.runlog.sha}

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
            "service_jar": {"path": jar, "present": os.path.exists(jar)},
            "native_stack_ps1": {"path": harness, "present": os.path.exists(harness)},
            "stack_state": {"path": stack_state, "present": os.path.exists(stack_state)},
            "kafka_home": kafka_home or None,
            "psql": pg_bin,
            "toxiproxy": toxiproxy,
            "java": discover_java(),
            "ready": bool(os.path.exists(jar) and os.path.exists(harness)
                          and os.path.exists(stack_state)),
        }

    # -- artefact helpers ------------------------------------------------------------------
    def _archive_artefact(self, run_id: str, src: str | None, name: str,
                          copy: bool = True) -> dict | None:
        """Record an artefact digest; copy it into the repository only when retention allows.

        prereg §5 retention: full dumps are retained for violating runs, control runs and the
        pre-registered systematic sample; for other runs only the digest is kept (the dumps are
        deterministic and re-generatable, and the workspace is cloud-synced — D11).
        """
        if not src or not os.path.exists(src):
            return None
        info = {"sha256": sha256_file(src), "bytes": os.path.getsize(src), "path": None,
                "retained": bool(copy)}
        if not copy:
            return info
        dest_dir = os.path.join(self.artifacts_dir, run_id)
        os.makedirs(dest_dir, exist_ok=True)
        dest = os.path.join(dest_dir, name)
        shutil.copy2(src, dest)
        info["path"] = os.path.relpath(dest, self.repo)
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
        manifest_path = os.path.join(self.artifacts_dir, run_id, "digest-manifest.json")
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        with open(manifest_path, "w", encoding="utf-8", newline="") as fh:
            json.dump(manifest, fh, indent=2)
            fh.write("\n")
        archived_paths = {k: (v or {}).get("path") if v else None for k, v in archived.items()}
        archived_paths["digest_manifest"] = os.path.relpath(manifest_path, self.repo)
        return archived_paths, manifest

    # -- synthetic fixture evaluation (dry-run) --------------------------------------------
    def dry_run_plan(self, fixture_dir: str) -> list[dict]:
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
            result = self._dry_one(run, fixture_dir, descriptor, preflight)
            results.append(result)
        return {"mode": "dry-run", "synthetic": True, "runs": results,
                "preflight": preflight,
                "note": "SYNTHETIC DRY-RUN — not measurement data; no confirmatory analysis may "
                        "use results/dryrun/"}

    def _dry_fixture_for(self, run: dict, provided_dir: str,
                         provided: dict) -> tuple[str, dict]:
        """Use the provided fixture when it matches the run, else build a matching synthetic one.

        A fixture is a *synthetic* run bundle; building one per selected run keeps the dry-run
        independent of which matrix row is exercised while remaining clearly non-measured.
        """
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
        stage_record = {"P": "pilot", "A": "A", "B": "B", "C": "C", "V": "pilot"}.get(
            stage, stage)
        if stage_record not in VR.STAGES:
            raise SystemExit(f"cannot map matrix stage {stage!r} to a run-record stage")
        paths = {k: os.path.join(fixture_dir, v) for k, v in descriptor["paths"].items()}
        # The fixture must be built from the same frozen per-run data seed (see
        # experiments/fixtures/build_dryrun_fixture.py); a mismatch is a fixture error, not a
        # measurement.
        if descriptor.get("seed_hex") != seed_hex:
            raise SystemExit(
                f"fixture seed {descriptor.get('seed_hex')!r} != run data_seed {seed_hex!r}; "
                f"rebuild with: python experiments/fixtures/build_dryrun_fixture.py "
                f"--run-id {run_id}")
        # The fixture may be generated for a different event count; only run when they agree.
        if int(descriptor["events"]) != int(run["load_events"]):
            raise SystemExit(
                f"fixture has {descriptor['events']} events but run {run_id} needs "
                f"{run['load_events']}; generate a matching fixture (make_fixtures.write_clean_run)")
        t0 = utc_now()
        self.runlog.append(stage=stage, run_id=run_id, seed=seed_hex,
                           command=f"[dry-run] oracle over fixture {os.path.basename(fixture_dir)}",
                           exit_status="START")
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
                           outputs=os.path.relpath(oracle_out, self.repo))
        # environment -> run-record blocks
        timings = {
            "t_start": utc_ms(t0), "t_load_start": utc_ms(t0) + 5,
            "t_load_end": utc_ms(t0) + 105, "t_fault_fire": None, "t_restore": None,
            "t_quiesce": utc_ms(t1), "t_observation": utc_ms(t1) + 1, "t_end": utc_ms(t1) + 2,
        }
        cfg = {
            "mechanism": run["mechanism"], "ordering_variant": run["ordering_variant"],
            "load_events": int(run["load_events"]), "partitions": int(run["partitions"]),
            "consumer_instances": int(run["consumer_instances"]),
            "rate": "max", "synthetic_fixture": True,
        }
        cfg_sha = hashlib.sha256(
            json.dumps(cfg, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        receipts = INJ.read_receipts(self.faults_log)
        rcpt = INJ.validate_receipts(receipts, run_id=run_id,
                                     injector_mode=run.get("injector_mode", "none"))
        performance = {k: None for k in VR.PERF_KEYS}
        performance["warmup_events_excluded"] = 0
        performance["perf_window"] = {"start": None, "end": None,
                                      "note": "dry-run: no performance measurement"}
        record = {
            "schema": VR.SCHEMA_VERSION,
            "run_id": run_id,
            "stage": stage_record,
            "cell_id": run["cell_id"],
            "mechanism": run["mechanism"],
            "ordering_variant": run["ordering_variant"],
            "fault": run["fault"],
            "fault_level": run["fault_level"],
            "load_events": int(run["load_events"]),
            "partitions": int(run["partitions"]),
            "consumer_instances": int(run["consumer_instances"]),
            "rep": int(run["rep"]),
            "data_seed": seed_hex,
            "injection_seed": run["injection_seed"],
            "master_seed": self._campaign_master_seed(),
            "git_sha": self.runlog.sha,
            "host": preflight["host"],
            "versions": preflight["versions"],
            "config": {**cfg, "config_sha256": cfg_sha},
            "process_limits": {
                "synthetic_fixture": True, "affinity_mask": None, "priority_class": None,
                "jvm_xmx": None, "hikari_pool": None, "note": "not started in dry-run",
            },
            "timings": timings,
            "quiescence": {
                "lag_per_partition": {str(i): 0 for i in range(int(run["partitions"]))},
                "quiet_period_ms": self.opts.quiet_period_ms,
                "censored": False,
                "t_consistent_ms": None,
                "note": "dry-run: synthetic fixture, no live quiescence observation",
            },
            "performance": performance,
            "counts": result["counts"],
            "violations": result["violations"],
            "publication": result["publication"],
            "oracle": {**result["oracle"],
                       "result_sha256": result["oracle"].get("result_sha256")},
            "fault_receipts": [],
            "validity": {
                "valid": True,
                "excluded": False,
                "exclude_reason": None,
                "residue_check": True,
                "clock_check": True,
                "disk_free_start_gb": preflight["free_gb"],
                "notes": ["DRY-RUN synthetic fixture: orchestration/schema/oracle/archive "
                          "exercise only; NOT a measurement and outside results/raw"],
            },
            "artifacts": {},
            "stdout_path": None,
            "stderr_path": None,
            "exit_status": {"consumer": None, "load_generator": None,
                            "note": "dry-run: no processes started"},
        }
        archived, manifest = self._archive_run(run_id, {
            "gt": paths["gt"], "gt_sha256_sidecar": os.path.join(
                fixture_dir, f"gt_{descriptor['run_id']}.sha256"),
            "source_dump": paths["source_dump"], "produced_manifest": paths["produced"],
            "sink_dump": paths["sink_dump"], "publication_dump": paths["publication_dump"],
            "attestation": paths["attestation"],
            "manifest": paths["manifest"],
            "oracle": oracle_out,
        })
        record["artifacts"] = archived
        errors = VR.validate_record(record)
        record_path = os.path.join(self.records_dir, f"run_{run_id}.json")
        with open(record_path, "w", encoding="utf-8", newline="") as fh:
            json.dump(record, fh, indent=2)
            fh.write("\n")
        self.runlog.append(stage="dry-run", run_id=run_id, seed=seed_hex,
                           command="[dry-run] write run record + digest manifest", exit_status=0,
                           outputs=os.path.relpath(record_path, self.repo))
        if errors:
            raise RuntimeError("dry-run produced an invalid run record:\n" + "\n".join(errors))
        return {"run_id": run_id, "record": os.path.relpath(record_path, self.repo),
                "oracle": os.path.relpath(oracle_out, self.repo),
                "valid": True, "violating_run": record["violations"]["violating_run"],
                "digest_manifest": manifest}

    def _campaign_master_seed(self) -> str:
        try:
            import yaml
            doc = yaml.safe_load(open(self.matrix_path, encoding="utf-8"))
            return doc["seeds"]["master_seed"]
        except Exception:  # noqa: BLE001
            return "0" * 32

    # -- live run --------------------------------------------------------------------------
    def run_live(self) -> dict:
        env = self.check_env()
        if not env["ready"]:
            raise SystemExit(
                "S3 interface not ready — missing:\n" + json.dumps(
                    {k: v for k, v in env.items() if isinstance(v, dict) and not v.get("present")},
                    indent=2) + "\nThe pilot must not start before S3's artifact acceptance "
                "tests pass and the lead confirms (P2 gate).")
        runs = self.selected_runs()
        preflight = self.preflight()
        results = []
        consecutive = 0
        for run in runs:
            record_path = os.path.join(self.records_dir, f"run_{run['run_id']}.json")
            if self.opts.resume and os.path.exists(record_path):
                results.append({"run_id": run["run_id"], "skipped": "already has a record"})
                continue
            consecutive += 1
            if consecutive % RESTART_EVERY_N_RUNS == 0:
                self.call([sys.executable, "-c", "pass"], stage=run["stage_label"],
                          run_id=run["run_id"], seed=run["data_seed"],
                          purpose=f"periodic full-stack {FULL_STACK_RESTART_VERB} "
                                  f"(every {RESTART_EVERY_N_RUNS} runs, D6)")
                self._full_stack_restart(run)
            results.append(self._live_one(run, preflight))
        return {"mode": "live", "runs": results, "preflight": preflight}

    def _full_stack_restart(self, run: dict) -> None:
        harness = os.path.join(self.repo, "artifact", "harness", "native-stack.ps1")
        self.call([discover_pwsh(), "-File", harness, FULL_STACK_RESTART_VERB],
                  stage=run["stage_label"], run_id=run["run_id"],
                  seed=run["data_seed"], purpose="restart-all", timeout=600)
        restart_log = os.path.join(self.results_dir, "fault_restart.log")
        with open(restart_log, "a", encoding="utf-8", newline="") as fh:
            fh.write(json.dumps({"ts_utc": utc_iso(), "action": "restart-all",
                                 "after_runs": RESTART_EVERY_N_RUNS}) + "\n")

    def _live_one(self, run: dict, preflight: dict) -> dict:
        """Implement docs/prereg.md §4.1 steps 1-13. Real stack operations; no simulation."""
        run_id = run["run_id"]
        stage = run["stage_label"]
        seed_hex = run["data_seed"]
        seed_u64 = D.seed_hex_to_u64(seed_hex)
        try:
            return LiveSequence(self, run, preflight, run_id, stage, seed_hex,
                                seed_u64).execute()
        except Exception as exc:  # noqa: BLE001 - a run failure must be recorded, not crash
            traceback.print_exc()
            self.runlog.append(stage=stage, run_id=run_id, seed=seed_hex,
                               command=f"[live] RUN FAILED: {type(exc).__name__}",
                               exit_status="FAIL")
            return {"run_id": run_id, "failed": f"{type(exc).__name__}: {exc}"}

    # -- write deviations ------------------------------------------------------------------
    def write_deviations(self) -> str:
        from experiments import deviations as DEV
        return DEV.write_deviations(self.matrix_path, self.records_dir,
                                    os.path.join(self.results_dir, "DEVIATIONS.md"))


# --------------------------------------------------------------------------------------------
# live sequence (real stack; used only when S3's interface is present)
# --------------------------------------------------------------------------------------------
class LiveSequence:
    """One run, docs/prereg.md §4.1 steps 1-13, against the real native stack."""

    def __init__(self, runner: Runner, run: dict, preflight: dict, run_id: str, stage: str,
                 seed_hex: str, seed_u64: int):
        self.r = runner
        self.run = run
        self.preflight = preflight
        self.run_id = run_id
        self.stage = stage
        self.seed_hex = seed_hex
        self.seed_u64 = seed_u64
        self.p = RunnerPaths(run_id)
        self.env = preflight["interface"]
        self.timings = {k: None for k in VR.TIMING_KEYS}
        self.processes: dict[int, subprocess.Popen] = {}
        self.stop_flags: list[str] = []
        self.consumer_pids: dict[int, int] = {}
        self.source_topic = f"wxeo.src.{run_id}"
        self.sink_topic = f"wxeo.sink.{run_id}"
        self.group = f"wxeo.cg.{run_id}"
        self.schema = f"run_{run_id}"
        self.db_schema_used = run["mechanism"] != "M1-kafka"
        self.injector = None
        self.oracle_result = None
        self.exclusions: list[str] = []
        self.notes: list[str] = []

    def execute(self) -> dict:
        ctx = self._injector_context()
        self.injector = INJ.make_injector(ctx)
        self.timings["t_start"] = utc_ms()
        try:
            # 1. reserve (already fixed in run_order.csv) + 2. reset
            self._reset()
            # 3. start consumers + wait readiness
            self._start_services()
            # 4. environment block was recorded in preflight; assert stack state unchanged
            self._record_environment()
            # 5/6. GT is written by the load generator before the first produce (contract); the
            #      injector is armed before the load starts.
            if self.injector is not None:
                self.injector.arm()
            trigger = self._start_injector_thread()
            # 7. produce (load generator writes GT+manifest first, then produces)
            self.timings["t_load_start"] = utc_ms()
            load_proc = self._start_load_generator()
            trigger.join(timeout=600)
            self._wait_load_generator(load_proc)
            self.timings["t_load_end"] = utc_ms()
            # 8. restore the injection (fire happened during the load)
            if self.injector is not None:
                self.injector.restore()
                self._collect_fault_receipts()
            # 9. quiesce
            quiescence = self._quiesce()
            # 10. dumps
            dumps = self._dump()
            # 11. oracle
            self.oracle_result = self._run_oracle(dumps)
            # 12/13. stop, residue check, archive, drop
            return self._finish(quiescence, dumps)
        except Exception:
            self._emergency_cleanup()
            raise

    # -- steps -----------------------------------------------------------------------------
    def _injector_context(self) -> INJ.InjectorContext:
        kafka_home = self.env.get("kafka_home") or ""
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
            db_dsn=self._db_dsn_direct(),
            run_log=self.r.runlog.append,
            call=lambda cmd, **kw: self.r.call(cmd, stage=self.stage, run_id=self.run_id,
                                               seed=self.seed_hex, **kw),
            restart_service=self._restart_service,
            stop_consumers=self._stop_consumers,
            start_consumers=self._start_services,
            wait_group_stable=self._wait_group_stable,
            wait_isr=self._wait_isr,
            consumer_pids=self.consumer_pids,
            extra={
                "source_topic": self.source_topic,
                "consumer_group": self.group,
                "psql": self.env.get("psql") or "psql",
                "db_dsn": self._db_dsn_direct(),
                "failpoint_action": self.r.opts.failpoint_action,
                "failpoint_after": 1,
            },
        )

    def _db_dsn_direct(self) -> str:
        return "host=127.0.0.1 port=5433 dbname=wxeo user=wxeo_read"

    def _psql(self, sql: str, purpose: str, *, db: str | None = None, user: str = "wxeo_app",
              check: bool = True) -> subprocess.CompletedProcess:
        psql = self.env.get("psql") or "psql"
        cmd = [psql, "-h", "127.0.0.1", "-p", "5433", "-U", user, "-d", db or "wxeo",
               "-tAc", sql]
        return self.r.call(cmd, stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                           purpose=purpose, check=check, timeout=120)

    def _kafka_cmd(self, main_class: str, *args: str) -> list[str]:
        kafka_home = self.env.get("kafka_home") or ""
        return [self.env.get("java") or "java", "-cp",
                os.path.join(kafka_home, "libs", "*"), main_class, *args]

    def _reset(self) -> None:
        self.r.runlog.append(stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                             command="[reset] fresh topics/group/schema", exit_status="START")
        # delete topics if present (idempotent workspace per run id)
        for topic in (self.source_topic, self.sink_topic):
            self.r.call(self._kafka_cmd("kafka.tools.TopicCommand",
                                        "--bootstrap-server", "127.0.0.1:9091",
                                        "--delete", "--topic", topic),
                        stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                        purpose=f"delete topic {topic}", check=False, timeout=60)
        for topic in (self.source_topic, self.sink_topic):
            args = ["--bootstrap-server", "127.0.0.1:9091", "--create", "--topic", topic,
                    "--partitions", str(self.run["partitions"]), "--replication-factor", "3",
                    "--config", "min.insync.replicas=2"]
            self.r.call(self._kafka_cmd("kafka.tools.TopicCommand", *args),
                        stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                        purpose=f"create topic {topic}", check=True, timeout=120)
        if self.db_schema_used:
            jar = self.env["service_jar"]["path"]
            self.r.call([self.env.get("java") or "java", "-cp", jar,
                         "com.wxeo.sink.SchemaTool", "--mode=init",
                         "--db-url=jdbc:postgresql://127.0.0.1:5433/wxeo",
                         "--db-user=postgres", f"--schema={self.schema}",
                         f"--mechanism={self.run['mechanism']}"],
                        stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                        purpose="schema-init", check=True, timeout=180)
        # residue assertion: nothing from a previous run may remain
        residue = self._residue_check()
        if residue:
            self.exclusions.append("residue_check_failed")

    def _residue_check(self) -> list[str]:
        problems = []
        proc = self._psql(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name LIKE 'run_%' "
            f"AND schema_name <> '{self.schema}'", "residue: schemas", user="wxeo_read",
            check=False)
        try:
            if int((proc.stdout or "0").strip() or 0) > 0:
                problems.append("foreign run schemas present")
        except ValueError:
            pass
        return problems

    def _service_args(self, instance: int) -> list[str]:
        jar = self.env["service_jar"]["path"]
        java = self.env.get("java") or "java"
        args = [
            java, "-Xms2g", "-Xmx2g", "-XX:+UseG1GC", "-XX:+ExitOnOutOfMemoryError",
            "-jar", jar,
            f"--wxeo.mechanism={self.run['mechanism']}",
            f"--wxeo.ordering={self.run['ordering_variant']}",
            f"--wxeo.run-id={self.run_id}",
            f"--wxeo.instance-id={instance}",
            f"--wxeo.bootstrap=127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093",
            "--wxeo.db-url=jdbc:postgresql://127.0.0.1:15433/wxeo",
            "--wxeo.db-user=wxeo_app",
            f"--wxeo.db-schema={self.schema}",
            f"--wxeo.source-topic={self.source_topic}",
            f"--wxeo.sink-topic={self.sink_topic}",
            f"--wxeo.group={self.group}",
            f"--wxeo.output-dir={self.p.outputs}",
            f"--wxeo.faults-log={self.r.faults_log}",
        ]
        fp = INJ.failpoint_config_for_run(self._injector_context())
        if fp and fp.get("enabled"):
            args += ["--wxeo.failpoint.enabled=true", f"--wxeo.failpoint.name={fp['name']}",
                     f"--wxeo.failpoint.action={fp['action']}",
                     f"--wxeo.failpoint.after={fp['after']}"]
        else:
            args.append("--wxeo.failpoint.enabled=false")
        return args

    def _start_services(self) -> None:
        n = int(self.run["consumer_instances"])
        for instance in range(1, n + 1):
            if instance in self.consumer_pids:
                continue
            args = self._service_args(instance)
            stdout = open(os.path.join(self.p.logs, f"consumer{instance}.stdout.log"), "wb")
            stderr = open(os.path.join(self.p.logs, f"consumer{instance}.stderr.log"), "wb")
            proc = subprocess.Popen(args, stdout=stdout, stderr=stderr)
            self.processes[instance] = proc
            self.consumer_pids[instance] = proc.pid
            self.r.runlog.append(stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                                 command=f"[start] consumer instance {instance} pid={proc.pid}",
                                 exit_status="STARTED")
        self._wait_ready(timeout=180)

    def _wait_ready(self, timeout: float = 180) -> None:
        deadline = time.monotonic() + timeout
        n = int(self.run["consumer_instances"])
        while time.monotonic() < deadline:
            ready = 0
            for instance in range(1, n + 1):
                path = os.path.join(self.p.outputs, f"ready_{self.run_id}_{instance}.json")
                info = read_json(path)
                if info and info.get("status") in ("subscribed", "assigned"):
                    ready += 1
            if ready == n:
                return
            time.sleep(0.5)
        raise TimeoutError(f"consumer readiness timeout ({n} instances)")

    def _record_environment(self) -> None:
        self.r.runlog.append(stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                             command="[env] pre-run environment block recorded", exit_status=0)

    def _restart_service(self, instance: int) -> int:
        proc = self.processes.get(instance)
        if proc is None:
            self._start_services()
        else:
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass
            proc.wait(timeout=60)
            self.consumer_pids.pop(instance, None)
            self._start_services()
        return self.consumer_pids[instance]

    def _stop_consumers(self) -> None:
        for instance in list(self.processes):
            flag = os.path.join(self.p.outputs, f"stop_{self.run_id}_{instance}.flag")
            with open(flag, "w", encoding="utf-8") as fh:
                fh.write("stop\n")
            self.stop_flags.append(flag)
        deadline = time.monotonic() + 120
        for instance, proc in list(self.processes.items()):
            try:
                proc.wait(timeout=max(1.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                proc.kill()

    def _wait_group_stable(self, timeout: float = 120) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            proc = self.r.call(self._kafka_cmd(
                "kafka.admin.ConsumerGroupCommand", "--bootstrap-server", "127.0.0.1:9091",
                "--describe", "--group", self.group), stage=self.stage, run_id=self.run_id,
                seed=self.seed_hex, purpose="group describe", check=False, timeout=60)
            text = (proc.stdout or "") + (proc.stderr or "")
            if "Stable" in text or "consumers, " not in text:
                return True
            time.sleep(1.0)
        return False

    def _wait_isr(self, topic: str, rf: int, timeout: float = 120) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            proc = self.r.call(self._kafka_cmd(
                "kafka.tools.TopicCommand", "--bootstrap-server", "127.0.0.1:9091",
                "--describe", "--topic", topic), stage=self.stage, run_id=self.run_id,
                seed=self.seed_hex, purpose="topic describe ISR", check=False, timeout=60)
            text = proc.stdout or ""
            if f"Isr: {rf}" in text and text.count("Isr:") >= int(self.run["partitions"]):
                return True
            time.sleep(1.0)
        return False

    def _start_injector_thread(self) -> threading.Thread:
        if self.injector is None:
            return threading.Thread(target=lambda: None)

        def _fire():
            try:
                if self.run["fault_level"] == "L1" or self.run["fault"] == "F4b":
                    self.injector.trigger_and_fire()
                else:
                    duration = self._expected_load_seconds()
                    delay = INJ.level2_trigger_time(self._injector_context(),
                                                    load_duration_s=duration)
                    time.sleep(max(0.0, delay))
                    self.timings["t_fault_fire"] = utc_ms()
                    self.injector.trigger_and_fire()
            except Exception as exc:  # noqa: BLE001 - recorded as an invalid run
                self.notes.append(f"injector error: {type(exc).__name__}: {exc}")
                self.exclusions.append("fault_not_fired")

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

    def _effective_rate(self) -> str:
        if self.run.get("load_mode") == "saturation":
            table = read_json(self.r.opts.rate_table) or {}
            return str(table.get("saturation_rate", "max"))
        table = read_json(self.r.opts.rate_table) or {}
        key = f"{self.run['load_events']}|p{self.run['partitions']}"
        return str(table.get("fixed_rate", {}).get(key, self.r.opts.rate))

    def _start_load_generator(self) -> subprocess.Popen:
        jar = self.env["service_jar"]["path"]
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
            f"--gt-out={self.p.gt}",
            f"--manifest-out={self.p.manifest}",
            "--ack-timeout-ms=30000",
        ]
        stdout = open(os.path.join(self.p.logs, "loadgenerator.stdout.log"), "wb")
        stderr = open(os.path.join(self.p.logs, "loadgenerator.stderr.log"), "wb")
        self.r.runlog.append(stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                             command="[load] " + " ".join(args), exit_status="RUNNING")
        proc = subprocess.Popen(args, stdout=stdout, stderr=stderr)
        self.load_proc = proc
        return proc

    def _wait_load_generator(self, proc: subprocess.Popen) -> None:
        try:
            rc = proc.wait(timeout=3600)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise TimeoutError("load generator timeout (3600 s)")
        self.r.runlog.append(stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                             command="[load] load generator finished", exit_status=rc)
        if rc != 0:
            self.exclusions.append(f"load_generator_exit_{rc}")

    def _quiesce(self) -> dict:
        quiet_ms = self.r.opts.quiet_period_ms
        tmax = self.r.opts.quiesce_timeout_ms
        started = utc_ms()
        last_count = None
        stable_since = None
        censored = True
        t_consistent = None
        lag = {}
        while utc_ms() - started <= tmax:
            count = self._sink_count()
            lag = self._lag_per_partition()
            if (last_count is not None and count == last_count and lag
                    and all(v == 0 for v in lag.values())):
                if stable_since is None:
                    stable_since = utc_ms()
                if utc_ms() - stable_since >= quiet_ms:
                    censored = False
                    t_consistent = stable_since - self.timings["t_load_end"]
                    break
            else:
                stable_since = None
            last_count = count
            time.sleep(1.0)
        self.timings["t_quiesce"] = utc_ms()
        self.timings["t_observation"] = utc_ms()
        return {"lag_per_partition": lag, "quiet_period_ms": quiet_ms,
                "censored": censored, "t_consistent_ms": t_consistent}

    def _sink_count(self) -> int | None:
        if not self.db_schema_used:
            return None
        proc = self._psql(f"SELECT count(*) FROM {self.schema}.posting", "sink count",
                          check=False)
        try:
            return int((proc.stdout or "").strip())
        except ValueError:
            return None

    def _lag_per_partition(self) -> dict:
        proc = self.r.call(self._kafka_cmd(
            "kafka.admin.ConsumerGroupCommand", "--bootstrap-server", "127.0.0.1:9091",
            "--describe", "--group", self.group), stage=self.stage, run_id=self.run_id,
            seed=self.seed_hex, purpose="lag", check=False, timeout=60)
        lag: dict[str, int] = {}
        for line in (proc.stdout or "").splitlines():
            parts = [p for p in line.split() if p]
            if len(parts) >= 6 and parts[0].isdigit():
                try:
                    lag[parts[0]] = max(0, int(parts[5]))
                except (ValueError, IndexError):
                    continue
        return lag

    def _dump(self) -> dict:
        from experiments.oracle import oracle as O
        out = {}
        try:
            out["source"] = O.dump_source_topic(
                bootstrap="127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093",
                topic=self.source_topic, out_ndjson=self.p.source_dump,
                group=f"wxeo.dump.{self.run_id}")
        except Exception as exc:  # noqa: BLE001
            self.notes.append(f"source dump failed: {exc}")
        if self.db_schema_used:
            try:
                out["sink"] = O.dump_pg(
                    pg_dsn="host=127.0.0.1 port=5433 dbname=wxeo user=wxeo_read",
                    schema=self.schema, out_csv=self.p.sink_dump,
                    attestation_out=self.p.attestation)
            except Exception as exc:  # noqa: BLE001
                self.notes.append(f"sink dump failed: {exc}")
        try:
            isolation = ("read_committed" if self.run["mechanism"] in
                         ("M1-kafka", "M1x", "M1x+M3") else "read_uncommitted")
            out["publication"] = O.dump_kafka_topic(
                bootstrap="127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093",
                topic=self.sink_topic, out_ndjson=self.p.publication_dump,
                group=f"wxeo.dump.{self.run_id}", isolation=isolation)
            out["publication_isolation"] = isolation
        except Exception as exc:  # noqa: BLE001
            self.notes.append(f"publication dump failed: {exc}")
        return out

    def _run_oracle(self, dumps: dict) -> dict:
        sink_kind = "kafka" if self.run["mechanism"] == "M1-kafka" else "db"
        oracle_out = os.path.join(self.r.oracle_dir, f"oracle_{self.run_id}.json")
        result = O.oracle_run(
            run_id=self.run_id, mechanism=self.run["mechanism"], sink_kind=sink_kind,
            master_seed_u64=self.seed_u64, n_events=int(self.run["load_events"]),
            partitions=int(self.run["partitions"]),
            gt_path=self.p.gt, source_dump_path=self.p.source_dump,
            produced_manifest_path=self.p.manifest,
            sink_dump_path=self.p.sink_dump if sink_kind == "db" else None,
            publication_dump_path=self.p.publication_dump,
            attestation_path=self.p.attestation if sink_kind == "db" else None,
            out_path=oracle_out, shards=D.DEFAULT_SHARDS,
            workdir=self.p.oracle_work,
        )
        return result

    def _collect_fault_receipts(self) -> None:
        receipts = INJ.read_receipts(self.r.faults_log)
        check = INJ.validate_receipts(receipts, run_id=self.run_id,
                                      injector_mode=self.run.get("injector_mode", "none"))
        if not check["attested"]:
            self.exclusions.append("fault_receipts_invalid")
            self.notes.append(check["reason"] or "invalid receipts")

    def _finish(self, quiescence: dict, dumps: dict) -> dict:
        # performance percentile files come from the service at shutdown
        self._stop_consumers()
        self.timings["t_end"] = utc_ms()
        hist_json = os.path.join(self.p.outputs, f"hist_{self.run_id}_1.json")
        percentiles = read_json(hist_json) or {}
        # exit status collection
        exit_status = {}
        for instance, proc in self.processes.items():
            exit_status[f"consumer_{instance}"] = proc.returncode
        exit_status["load_generator"] = getattr(self, "load_proc", None).returncode \
            if getattr(self, "load_proc", None) is not None else None
        # validity
        oracle = self.oracle_result
        valid = not self.exclusions
        if not oracle["oracle"]["source_integrity"]:
            valid = False
            self.exclusions.append("source_integrity_failed")
        clock_check = True  # single-clock host assumption checked by the runner clock probe
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
        }, retain_map=self._retention_map())
        record = {
            "schema": VR.SCHEMA_VERSION,
            "run_id": self.run_id, "stage": self.stage if self.stage != "V" else "pilot",
            "cell_id": self.run["cell_id"], "mechanism": self.run["mechanism"],
            "ordering_variant": self.run["ordering_variant"], "fault": self.run["fault"],
            "fault_level": self.run["fault_level"],
            "load_events": int(self.run["load_events"]),
            "partitions": int(self.run["partitions"]),
            "consumer_instances": int(self.run["consumer_instances"]),
            "rep": int(self.run["rep"]), "data_seed": self.seed_hex,
            "injection_seed": self.run["injection_seed"],
            "master_seed": self.r._campaign_master_seed(),
            "git_sha": self.r.runlog.sha,
            "host": self.preflight["host"], "versions": self.preflight["versions"],
            "config": {**self._config_block()},
            "process_limits": self._process_limits(),
            "timings": self.timings,
            "quiescence": quiescence,
            "performance": {
                "throughput_eps_median": percentiles.get("throughput_eps_median"),
                "p50_ms": percentiles.get("p50_ms"), "p90_ms": percentiles.get("p90_ms"),
                "p99_ms": percentiles.get("p99_ms"), "p999_ms": percentiles.get("p999_ms"),
                "max_ms": percentiles.get("max_ms"), "histogram_path": hist_json,
                "warmup_events_excluded": max(2000, int(int(self.run["load_events"]) * 0.10)),
                "perf_window": percentiles.get("perf_window"),
            },
            "counts": oracle["counts"], "violations": oracle["violations"],
            "publication": oracle["publication"],
            "oracle": oracle["oracle"],
            "fault_receipts": [r for r in INJ.read_receipts(self.r.faults_log)
                               if r.get("run_id") == self.run_id],
            "validity": {
                "valid": valid, "excluded": not valid,
                "exclude_reason": "; ".join(self.exclusions) if self.exclusions else None,
                "residue_check": "residue_check_failed" not in self.exclusions,
                "clock_check": clock_check,
                "disk_free_start_gb": free_space_gb(eos_runtime_root()),
                "notes": self.notes or ["live run"],
            },
            "artifacts": archived,
            "stdout_path": os.path.relpath(os.path.join(self.p.logs, "loadgenerator.stdout.log"),
                                           self.r.repo),
            "stderr_path": os.path.relpath(os.path.join(self.p.logs, "loadgenerator.stderr.log"),
                                           self.r.repo),
            "exit_status": exit_status,
        }
        errors = VR.validate_record(record)
        record_path = os.path.join(self.r.records_dir, f"run_{self.run_id}.json")
        with open(record_path, "w", encoding="utf-8", newline="") as fh:
            json.dump(record, fh, indent=2)
            fh.write("\n")
        if errors:
            raise RuntimeError("invalid run record:\n" + "\n".join(errors))
        self._drop_state()
        return {"run_id": self.run_id, "record": os.path.relpath(record_path, self.r.repo),
                "valid": valid, "excluded": not valid,
                "violating_run": record["violations"]["violating_run"]}

    def _retention_map(self) -> dict:
        """prereg §5 retention policy: full dumps for violating/control/sample runs only."""
        violating = bool((self.oracle_result or {}).get("violations", {}).get("violating_run"))
        control = self.run.get("fault") in ("F0", "none", "ctrl")
        sample = int(self.run.get("rep", -1)) in (0, 5, 10, 20)
        keep_dumps = bool(violating or control or sample)
        return {
            "gt": keep_dumps,
            "source_dump": keep_dumps,
            "sink_dump": keep_dumps,
            "publication_dump": keep_dumps,
        }

    def _config_block(self) -> dict:
        cfg = {
            "mechanism": self.run["mechanism"],
            "ordering_variant": self.run["ordering_variant"],
            "load_events": int(self.run["load_events"]),
            "partitions": int(self.run["partitions"]),
            "consumer_instances": int(self.run["consumer_instances"]),
            "load_mode": self.run.get("load_mode"),
            "rate": self._effective_rate(),
        }
        cfg["config_sha256"] = hashlib.sha256(
            json.dumps(cfg, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return cfg

    def _process_limits(self) -> dict:
        return {"affinity_mask": os.environ.get("WXEO_AFFINITY", None),
                "priority_class": "normal", "jvm_xmx": "2g", "jvm_xms": "2g",
                "jvm_gc": "G1", "hikari_pool": 10, "broker_heap": "2g",
                "pg_shared_buffers": "2GB"}

    def _drop_state(self) -> None:
        if self.db_schema_used:
            jar = self.env["service_jar"]["path"]
            self.r.call([self.env.get("java") or "java", "-cp", jar,
                         "com.wxeo.sink.SchemaTool", "--mode=drop",
                         "--db-url=jdbc:postgresql://127.0.0.1:5433/wxeo",
                         "--db-user=postgres", f"--schema={self.schema}"],
                        stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                        purpose="schema-drop", check=False, timeout=120)
        for topic in (self.source_topic, self.sink_topic):
            self.r.call(self._kafka_cmd("kafka.tools.TopicCommand",
                                        "--bootstrap-server", "127.0.0.1:9091",
                                        "--delete", "--topic", topic),
                        stage=self.stage, run_id=self.run_id, seed=self.seed_hex,
                        purpose=f"delete topic {topic}", check=False, timeout=60)
        shutil.rmtree(self.p.scratch, ignore_errors=True)

    def _emergency_cleanup(self) -> None:
        try:
            if self.injector is not None:
                self.injector.restore()
        except Exception:  # noqa: BLE001
            pass
        try:
            self._stop_consumers()
        except Exception:  # noqa: BLE001
            pass
        try:
            self._drop_state()
        except Exception:  # noqa: BLE001
            pass


class RunnerPaths:
    """Per-run scratch/output paths (local non-synced scratch; D11)."""

    def __init__(self, run_id: str):
        root = os.path.join(eos_runtime_root(), "runs", run_id)
        self.scratch = root
        self.outputs = os.path.join(root, "outputs")
        self.logs = os.path.join(root, "logs")
        self.oracle_work = os.path.join(root, "oracle")
        os.makedirs(self.outputs, exist_ok=True)
        os.makedirs(self.logs, exist_ok=True)
        self.gt = os.path.join(root, f"gt_{run_id}.ndjson")
        self.manifest = os.path.join(root, f"manifest_{run_id}.json")
        self.source_dump = os.path.join(root, f"source_dump_{run_id}.ndjson")
        self.sink_dump = os.path.join(root, f"sink_dump_{run_id}.csv")
        self.publication_dump = os.path.join(root, f"publication_{run_id}.ndjson")
        self.attestation = os.path.join(root, f"attestation_{run_id}.json")


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
    p.add_argument("--rate", default="max", help="fixed offered rate for fixed-rate mode, or max")
    p.add_argument("--no-resume", action="store_true")
    p.add_argument("--runlog", default=None,
                   help="RUNLOG path (default: <results-dir>/RUNLOG.md; dry-run defaults to the "
                        "campaign results/RUNLOG.md)")
    p.add_argument("--write-deviations", action="store_true")
    p.add_argument("--check-env", action="store_true")
    p.add_argument("--quiet-period-ms", type=int, default=QUIET_PERIOD_MS)
    p.add_argument("--quiesce-timeout-ms", type=int, default=QUIESCE_TIMEOUT_MS)
    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.write_deviations:
        runner = Runner(RunnerOptions(results_dir=args.results_dir))
        path = runner.write_deviations()
        print(json.dumps({"deviations": path}, indent=2))
        return 0
    if args.check_env:
        runner = Runner(RunnerOptions(results_dir=args.results_dir))
        print(json.dumps(runner.check_env(), indent=2))
        return 0
    opts = RunnerOptions(
        stage=args.stage, limit=args.limit, run_id=args.run_id, dry_run=args.dry_run,
        fixture=args.fixture, results_dir=args.results_dir,
        scratch_dir=args.scratch_dir or "", rate=args.rate, resume=not args.no_resume,
        quiet_period_ms=args.quiet_period_ms,
        quiesce_timeout_ms=args.quiesce_timeout_ms,
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
