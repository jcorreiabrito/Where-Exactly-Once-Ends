#!/usr/bin/env python3
"""Build the synthetic dry-run fixture used by ``experiments/runner.py --dry-run``.

The fixture is a complete 1,000-event bundle (matching a pilot cell's 1k load) with a perfectly
clean sink.  It is synthetic: clearly labelled in ``fixture.json``, stored under
``results/dryrun/fixtures/`` and never used as measurement data.

By default the fixture takes its seed, event count and partition count from the run's row in
``experiments/run_order.csv`` so that it matches the frozen per-run data seed exactly (the oracle
re-derives the expected set from that seed, and a mismatch would — correctly — fail the
source-integrity gate).

Usage::

    python experiments/fixtures/build_dryrun_fixture.py [--run-id <id>] [--events N]
                                                        [--out results/dryrun/fixtures]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from experiments.fixtures import make_fixtures as F  # noqa: E402


def plan_row(run_id: str) -> dict | None:
    order = os.path.join(_REPO, "experiments", "run_order.csv")
    if not os.path.exists(order):
        return None
    with open(order, "r", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["run_id"] == run_id:
                return row
    return None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build the synthetic dry-run fixture")
    parser.add_argument("--events", type=int, default=None)
    parser.add_argument("--partitions", type=int, default=None)
    parser.add_argument("--seed-hex", default=None)
    parser.add_argument("--run-id", default="pilot-m2-f0-1k-p3-r00")
    parser.add_argument("--out", default=os.path.join(_REPO, "results", "dryrun", "fixtures"))
    args = parser.parse_args(argv)
    row = plan_row(args.run_id)
    if row is None:
        raise SystemExit(f"run {args.run_id!r} not found in experiments/run_order.csv")
    seed_hex = args.seed_hex or row["data_seed"]
    events = args.events if args.events is not None else int(row["load_events"])
    partitions = args.partitions if args.partitions is not None else int(row["partitions"])
    out_dir = os.path.join(os.path.abspath(args.out), args.run_id)
    descriptor = F.write_clean_run(
        out_dir, run_id=args.run_id, mechanism=row["mechanism"], sink_kind="db",
        seed_hex=seed_hex, n_events=events, partitions=partitions,
        label="synthetic-dryrun",
    )
    descriptor["plan"] = {k: row[k] for k in
                          ("run_id", "cell_id", "stage", "mechanism", "fault",
                           "fault_level", "ordering_variant", "load_events", "partitions",
                           "consumer_instances", "rep", "data_seed", "injection_seed")}
    with open(os.path.join(out_dir, "fixture.json"), "w", encoding="utf-8", newline="") as fh:
        json.dump(descriptor, fh, indent=2)
        fh.write("\n")
    print(json.dumps({
        "fixture_dir": out_dir,
        "run_id": descriptor["run_id"],
        "mechanism": descriptor["mechanism"],
        "events": descriptor["events"],
        "partitions": descriptor["partitions"],
        "seed_hex": descriptor["seed_hex"],
        "gt_sha256": descriptor["gt_sha256"],
        "synthetic": True,
        "note": "SYNTHETIC dry-run fixture — not measurement data",
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
