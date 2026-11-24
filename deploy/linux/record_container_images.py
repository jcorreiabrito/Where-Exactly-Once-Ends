#!/usr/bin/env python3
"""Record the container image digests of the campaign stack (S5 B5, reproducibility).

Run on the Linux campaign host by ``deploy/linux/bootstrap.sh`` *after* the images are pulled and
*before* the stack is declared ready. It writes ``results/harness/container-images.json`` with the
repo digests (``repo@sha256:...``) of every image in the compose file, the docker/compose versions and
the host facts, and FAILS CLOSED if any image has no repo digest (a tag can be re-pointed; the digest
is the reproducibility claim). The lead's ``freeze.py`` binds this file as required evidence.

JSON shape (``wxeo.container-images.v1``)::

    {
      "schema": "wxeo.container-images.v1",
      "generated_utc": "2026-11-24T21:00:00Z",
      "host": {"platform": "Linux-...", "machine": "x86_64", "kernel": "..."},
      "docker": {"version": "Docker version 24.0.7, build ...",
                 "compose_version": "2.24.5"},
      "compose": {"file": "deploy/docker/docker-compose.yml",
                  "project": "wxeo-campaign",
                  "config_images": ["apache/kafka:3.7.2", "postgres:16",
                                    "ghcr.io/shopify/toxiproxy:2.9.0"]},
      "images": [
        {"tag": "apache/kafka:3.7.2",
         "repo_digests": ["apache/kafka@sha256:..."],
         "image_id": "sha256:..."}
      ],
      "passed": true
    }
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_COMPOSE = os.path.join(REPO, "deploy", "docker", "docker-compose.yml")
DEFAULT_OUT = os.path.join(REPO, "results", "harness", "container-images.json")
EXPECTED_TAGS = ("apache/kafka:3.7.2", "postgres:16", "ghcr.io/shopify/toxiproxy:2.9.0")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run(cmd: list[str], timeout: float = 120.0) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _compose_config_images(compose: str) -> list[str]:
    proc = _run(["docker", "compose", "-f", compose, "-p", "wxeo-campaign", "config", "--images"])
    if proc.returncode != 0:
        raise RuntimeError("docker compose config --images failed: %s"
                           % ((proc.stderr or proc.stdout or "").strip()[-400:]))
    return [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]


def _image_record(tag: str) -> dict:
    fmt = "{{json .RepoTags}}|{{json .RepoDigests}}|{{.Id}}"
    proc = _run(["docker", "image", "inspect", "--format", fmt, tag])
    if proc.returncode != 0:
        raise RuntimeError("docker image inspect failed for %s: %s"
                           % (tag, ((proc.stderr or proc.stdout or "").strip()[-400:])))
    parts = (proc.stdout or "").strip().split("|")
    if len(parts) != 3:
        raise RuntimeError("unexpected docker image inspect output for %s: %r" % (tag, proc.stdout))
    try:
        repo_tags = json.loads(parts[0]) or []
        repo_digests = json.loads(parts[1]) or []
    except json.JSONDecodeError as exc:
        raise RuntimeError("cannot parse docker image inspect JSON for %s" % tag) from exc
    image_id = parts[2].strip()
    if not repo_digests:
        raise RuntimeError("image %s has no RepoDigests (not pulled from a registry / digest "
                           "missing); refusing to bind an unpinned image" % tag)
    return {"tag": tag, "repo_tags": repo_tags, "repo_digests": repo_digests,
            "image_id": image_id}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="record container image repo digests")
    parser.add_argument("--compose", default=DEFAULT_COMPOSE)
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    try:
        config_images = _compose_config_images(args.compose)
        missing_expected = [t for t in EXPECTED_TAGS if t not in config_images]
        if missing_expected:
            raise RuntimeError("compose does not reference the expected pinned images: %s"
                               % missing_expected)
        docker_version = (_run(["docker", "--version"]).stdout or "").strip()
        compose_proc = _run(["docker", "compose", "version", "--short"])
        compose_version = (compose_proc.stdout or compose_proc.stderr or "").strip().splitlines()
        compose_version = compose_version[-1].strip() if compose_version else ""
        images = [_image_record(tag) for tag in config_images]
        uname = platform.uname()
        report = {
            "schema": "wxeo.container-images.v1",
            "generated_utc": _utc_now(),
            "host": {"platform": platform.platform(), "machine": platform.machine(),
                     "kernel": "%s %s" % (uname.system, uname.release)},
            "docker": {"version": docker_version, "compose_version": compose_version},
            "compose": {"file": "deploy/docker/docker-compose.yml",
                        "project": "wxeo-campaign",
                        "config_images": config_images},
            "images": images,
            "passed": True,
        }
        out = args.out if os.path.isabs(args.out) else os.path.join(REPO, args.out)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, sort_keys=True)
            fh.write("\n")
        for img in images:
            print("[IMAGE] %-34s %s" % (img["tag"], img["repo_digests"][0]))
        print("container-images: %d images recorded (docker %s, compose %s) -> %s"
              % (len(images), docker_version, compose_version, out))
        return 0
    except Exception as exc:  # noqa: BLE001 - fail closed with a diagnostic
        print("container-images: FAILED: %s: %s" % (type(exc).__name__, exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
