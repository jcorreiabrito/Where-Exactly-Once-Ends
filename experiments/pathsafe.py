"""Path hygiene for every committed artefact (lead P0 addendum to the S5 fix pass).

Committed run records, oracle JSON, manifests, CSVs, tables and figures must never contain an
absolute local path (drive letter, UNC share, home directory) or the local username: the workspace
is cloud-synced and the public repository must not leak host/environment detail.  Rules:

* paths under the repository are recorded **repo-relative** (forward slashes);
* paths outside the repository are recorded as ``<scratch>/<basename>`` placeholders;
* :func:`assert_clean` is used as a final write barrier by the runner/oracle/analysis;
* :func:`find_violations` is used by ``experiments/validate_record.py`` and the test suite.
"""

from __future__ import annotations

import os
import re

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_USERNAME = os.environ.get("USERNAME") or os.environ.get("USER") or ""
_DRIVE = re.compile(r"(?<![A-Za-z0-9])[A-Za-z]:[\\/]")
_UNC = re.compile(r"\\\\[A-Za-z0-9_.\-]+[\\/]")
_HOME = [
    re.compile(r"[A-Za-z]:[\\/]Users[\\/][^\\/\s\"']+", re.IGNORECASE),
    re.compile(r"/Users/[^/\s\"']+"),
    re.compile(r"/home/[^/\s\"']+"),
]
_LOCALAPPDATA = re.compile(r"%LOCALAPPDATA%[\\/]", re.IGNORECASE)


class PathHygieneError(RuntimeError):
    """Raised when an artefact that is about to be committed contains a local path."""


def text_violations(text: str) -> list[str]:
    """Return a list of path-hygiene violation descriptions in ``text`` (empty == clean)."""
    if not isinstance(text, str) or not text:
        return []
    found: list[str] = []
    for label, pattern in (("drive-letter path", _DRIVE), ("UNC path", _UNC),
                           ("home-directory path", _HOME[0]),
                           ("home-directory path", _HOME[1]),
                           ("home-directory path", _HOME[2])):
        match = pattern.search(text)
        if match:
            found.append(f"{label}: {match.group(0)[:80]}")
    if _USERNAME and len(_USERNAME) >= 3 and _USERNAME.lower() in text.lower():
        # only flag the username in path-like contexts, not inside ordinary words
        if re.search(r"[\\/]" + re.escape(_USERNAME), text, re.IGNORECASE) \
                or re.search(re.escape(_USERNAME) + r"[\\/]", text, re.IGNORECASE):
            found.append(f"local username in path: {_USERNAME}")
    return found


def find_violations(obj, where: str = "") -> list[str]:
    """Recursively find path-hygiene violations in a JSON-like object."""
    out: list[str] = []

    def walk(value, path: str) -> None:
        if isinstance(value, str):
            for violation in text_violations(value):
                out.append(f"{where}{path}: {violation}")
        elif isinstance(value, dict):
            for key, item in value.items():
                walk(item, f"{path}.{key}" if path else str(key))
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                walk(item, f"{path}[{index}]")

    walk(obj, "")
    return out


def assert_clean(obj, where: str = "artefact") -> None:
    violations = find_violations(obj, where)
    if violations:
        raise PathHygieneError(
            "refusing to write artefact with local paths: " + "; ".join(violations[:5]))


def repo_rel(path: str | None) -> str | None:
    """Repo-relative POSIX path when inside the repo; ``<scratch>/<name>`` otherwise."""
    if path is None:
        return None
    try:
        abs_path = os.path.abspath(path)
        rel = os.path.relpath(abs_path, REPO_ROOT)
    except (OSError, ValueError):
        return "<scratch>/" + os.path.basename(str(path))
    if rel.startswith(".."):
        return "<scratch>/" + os.path.basename(abs_path)
    return rel.replace(os.sep, "/")


def scrub_string(value: str) -> str:
    """Replace absolute local paths inside a string with placeholders."""
    if not isinstance(value, str) or not value:
        return value
    text = value
    # repo root -> .
    text = text.replace(REPO_ROOT, ".")
    text = _LOCALAPPDATA.sub("<scratch>/", text)
    text = _HOME[0].sub("<home>", text)
    text = _HOME[1].sub("<home>", text)
    text = _HOME[2].sub("<home>", text)
    # remaining drive-letter / UNC absolute paths -> <scratch>/<last component>
    def _drive_sub(match: re.Match) -> str:
        tail = text[match.end():]
        tail = re.split(r"[\s\"',;)]", tail, maxsplit=1)[0]
        name = tail.replace("\\", "/").rstrip("/").split("/")[-1]
        return "<scratch>/" + (name or "path")

    text = _DRIVE.sub(_drive_sub, text)
    return text


def scrub(obj):
    """Recursively scrub a JSON-like structure (returns a new object)."""
    if isinstance(obj, str):
        return scrub_string(obj)
    if isinstance(obj, dict):
        return {k: scrub(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [scrub(v) for v in obj]
    return obj


__all__ = [
    "PathHygieneError",
    "REPO_ROOT",
    "text_violations",
    "find_violations",
    "assert_clean",
    "repo_rel",
    "scrub_string",
    "scrub",
]
