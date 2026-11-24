#!/usr/bin/env python3
"""verify-refs.py -- acceptance test for the S1 reference base.

Reads refs/refs.bib and refs/verified.csv and checks, fail-closed:

  1. structural: every bib key appears in verified.csv and vice versa; no
     duplicate bib keys; no duplicate DOIs; type in {peer, gray}.
  2. attribution: every entry carries a quote of >= 15 characters, a
     non-empty supports_claim and a non-empty paper_section.
  3. peer review: every peer entry has a DOI, doi_verified_crossref == Y, and
     the DOI resolves through Crossref to a record whose title fuzzy-matches
     the BibTeX title with ratio >= 0.90 (difflib, normalised strings).
  4. gray literature: no DOI, doi_verified_crossref == NA, live URL and
     retrieval timestamp present, and the bib entry carries url + urldate.
  5. access evidence: a cached raw source exists under data/raw/lit/ for every
     verified entry (cached .html/.pdf/.txt/.json(.s2/.openalex) or its
     crossref/openaire metadata record).

Crossref lookups use the local cache in data/raw/crossref/<key>.json by
default so the check is reproducible offline; pass --live to re-query
https://api.crossref.org (rate-limited to 1 request/s, mailto set).

Exit status 0 iff every check passes.

Usage:
  python tools/verify-refs.py            # offline (cached Crossref records)
  python tools/verify-refs.py --live     # live Crossref re-resolution
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import difflib
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIB = ROOT / "refs" / "refs.bib"
CSV = ROOT / "refs" / "verified.csv"
RAW = ROOT / "data" / "raw" / "lit"
CR_CACHE = RAW / "_crossref"
MAILTO = "correiadebrito.joao@gmail.com"
UA = f"verify-refs/1.0 (mailto:{MAILTO})"
MIN_QUOTE = 15
MIN_FUZZY = 0.90
COLS = ["key", "type", "title", "authors", "year", "venue", "doi",
        "doi_verified_crossref", "url", "retrieved_utc", "quote",
        "supports_claim", "paper_section", "notes"]


class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.checks = 0

    def check(self, cond: bool, label: str, detail: str = "") -> bool:
        self.checks += 1
        if cond:
            print(f"  PASS  {label}")
        else:
            print(f"  FAIL  {label}" + (f" :: {detail}" if detail else ""))
            self.failures.append(f"{label} :: {detail}")
        return cond


# ------------------------------------------------------------------ bib parse
def parse_bib(text: str) -> dict[str, dict[str, str]]:
    entries: dict[str, dict[str, str]] = {}
    i = 0
    while True:
        m = re.search(r"@(\w+)\s*\{\s*([^,\s]+)\s*,", text[i:])
        if not m:
            break
        etype, key = m.group(1).lower(), m.group(2)
        j = i + m.end()
        depth, fields_body = 1, []
        while j < len(text) and depth > 0:
            ch = text[j]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
            fields_body.append(ch)
            j += 1
        body = "".join(fields_body[:-1])
        fields = _parse_fields(body)
        fields["__type__"] = etype
        if key in entries:
            raise SystemExit(f"duplicate bib key detected: {key}")
        entries[key] = fields
        i = j
    return entries


def _parse_fields(body: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    i = 0
    n = len(body)
    while i < n:
        m = re.compile(r"\s*,?\s*([A-Za-z][A-Za-z0-9_-]*)\s*=\s*").search(body, i)
        if not m:
            break
        name = m.group(1).lower()
        i = m.end()
        while i < n and body[i].isspace():
            i += 1
        if i >= n:
            break
        if body[i] == "{":
            depth, start = 1, i + 1
            i += 1
            while i < n and depth > 0:
                if body[i] == "{":
                    depth += 1
                elif body[i] == "}":
                    depth -= 1
                i += 1
            value = body[start:i - 1]
        elif body[i] == '"':
            start = i + 1
            i += 1
            while i < n and body[i] != '"':
                if body[i] == "\\":
                    i += 1
                i += 1
            value = body[start:i]
            i += 1
        else:
            m2 = re.match(r"[^,}]+", body[i:])
            value = (m2.group(0) if m2 else "").strip()
            i += len(value)
        fields[name] = value
    return fields


def norm_title(s: str) -> str:
    s = re.sub(r"\\[a-zA-Z]+", " ", s)
    s = s.replace("{", "").replace("}", "")
    s = s.replace("&amp;", "&")
    s = re.sub(r"[^0-9a-z]+", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def fuzzy(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, norm_title(a), norm_title(b)).ratio()


# ------------------------------------------------------------------ crossref
def crossref_record(doi: str, key: str, live: bool) -> tuple[dict | None, str]:
    if live:
        url = ("https://api.crossref.org/works/" + urllib.parse.quote(doi, safe="")
               + "?mailto=" + urllib.parse.quote(MAILTO))
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=45) as resp:
                    msg = json.loads(resp.read())["message"]
                time.sleep(1.1)
                return msg, "live"
            except Exception as exc:  # noqa: BLE001
                if attempt == 2:
                    return None, f"live error: {type(exc).__name__}: {exc}"
                time.sleep(2 + 2 * attempt)
    path = CR_CACHE / f"{key}.json"
    if not path.exists():
        return None, f"no cached crossref record at {path.relative_to(ROOT)}"
    try:
        msg = json.loads(path.read_text(encoding="utf-8-sig"))["message"]
    except Exception as exc:  # noqa: BLE001
        return None, f"cached record unreadable: {exc}"
    return msg, "cache"


def cr_title(msg: dict) -> str:
    t = (msg.get("title") or [""])[0]
    st = (msg.get("subtitle") or [""])[0]
    t = re.sub(r"<[^>]+>", "", t or "")
    st = re.sub(r"<[^>]+>", "", st or "")
    if st and st.lower() not in t.lower():
        t = f"{t}: {st}"
    return t


# ------------------------------------------------------------------ evidence
def cached_evidence(key: str) -> list[str]:
    out = []
    for suf in (".html", ".pdf", ".txt", ".json", ".s2.json", ".openalex.json"):
        p = RAW / f"{key}{suf}"
        if p.exists() and p.stat().st_size > 0:
            out.append(str(p.relative_to(ROOT)).replace("\\", "/"))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="re-query Crossref live")
    args = ap.parse_args()

    print("=" * 78)
    print(f"verify-refs.py  {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}  "
          f"mode={'LIVE Crossref' if args.live else 'cached Crossref records'}")
    print("=" * 78)

    bib = parse_bib(BIB.read_text(encoding="utf-8-sig"))
    with CSV.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != COLS:
            print(f"FATAL: unexpected CSV header: {reader.fieldnames}")
            return 1
        rows = list(reader)
    by_key = {r["key"]: r for r in rows}
    rep = Report()

    print(f"\n[1] structure ({len(bib)} bib entries, {len(rows)} CSV rows)")
    rep.check(len(bib) == len(by_key), "bib keys and CSV keys both unique",
              f"{len(bib)} vs {len(by_key)}")
    missing_csv = sorted(set(bib) - set(by_key))
    missing_bib = sorted(set(by_key) - set(bib))
    rep.check(not missing_csv, "every bib key present in verified.csv", ", ".join(missing_csv))
    rep.check(not missing_bib, "every verified.csv key present in refs.bib", ", ".join(missing_bib))
    types = {r["type"] for r in rows}
    rep.check(types <= {"peer", "gray"}, "all types in {peer, gray}", str(types))

    print("\n[2] attribution / quotes")
    short = [r["key"] for r in rows if len((r["quote"] or "").strip()) < MIN_QUOTE]
    rep.check(not short, f"every entry has a quote >= {MIN_QUOTE} chars", ", ".join(short))
    noc = [r["key"] for r in rows if len((r["supports_claim"] or "").strip()) < 10]
    rep.check(not noc, "every entry states what it supports", ", ".join(noc))
    nos = [r["key"] for r in rows if not (r["paper_section"] or "").strip()]
    rep.check(not nos, "every entry names a paper section", ", ".join(nos))

    print("\n[3] peer-reviewed entries: DOI + Crossref title match")
    peer_rows = [r for r in rows if r["type"] == "peer"]
    print(f"  ({len(peer_rows)} peer entries)")
    dois = [r["doi"].strip().lower() for r in rows if (r["doi"] or "").strip()]
    dup = sorted({d for d in dois if dois.count(d) > 1})
    rep.check(not dup, "no duplicate DOIs", ", ".join(dup))
    live_fail = 0
    for r in peer_rows:
        k, doi = r["key"], (r["doi"] or "").strip()
        ok_doi = rep.check(bool(doi), f"{k}: DOI present")
        if not ok_doi:
            continue
        rep.check(r["doi_verified_crossref"] == "Y", f"{k}: marked Crossref-verified",
                  r["doi_verified_crossref"])
        msg, src = crossref_record(doi, k, args.live)
        if msg is None:
            rep.check(False, f"{k}: Crossref resolves {doi}", src)
            live_fail += 1
            continue
        got = cr_title(msg)
        bib_title = bib[k].get("title", "")
        score = max(fuzzy(bib_title, got),
                    fuzzy(norm_title(bib_title), norm_title((msg.get("title") or [""])[0])))
        rep.check(score >= MIN_FUZZY, f"{k}: Crossref title fuzzy {score:.3f} >= {MIN_FUZZY}",
                  f"bib={bib_title!r} crossref={got!r}")

    print("\n[4] gray literature: live URL, retrieval time, no DOI")
    grave_rows = [r for r in rows if r["type"] == "gray"]
    print(f"  ({len(grave_rows)} gray entries, {len(grave_rows)/len(rows):.1%} of base)")
    for r in grave_rows:
        k = r["key"]
        rep.check(not (r["doi"] or "").strip(), f"{k}: no DOI claimed")
        rep.check(r["doi_verified_crossref"] == "NA", f"{k}: doi_verified_crossref=NA",
                  r["doi_verified_crossref"])
        rep.check((r["url"] or "").startswith("http"), f"{k}: live URL recorded", r["url"][:60])
        rep.check(bool((r["retrieved_utc"] or "").strip()), f"{k}: retrieval UTC recorded")
        b = bib[k]
        rep.check("url" in b and "urldate" in b, f"{k}: bib carries url+urldate")
    rep.check(len(grave_rows) / len(rows) <= 0.30, "gray literature <= 30% of entries",
              f"{len(grave_rows)/len(rows):.1%}")

    print("\n[5] access evidence cached under data/raw/lit/")
    no_cache = [r["key"] for r in rows if not cached_evidence(r["key"])]
    rep.check(not no_cache, "every entry has a cached raw source", ", ".join(no_cache))
    rep.check(BIB.exists() and CSV.exists(), "deliverable files exist")

    print("\n" + "=" * 78)
    npeer = len(peer_rows)
    summary = (f"{len(rows)} verified entries | {npeer} peer (Crossref-verified DOI) | "
               f"{len(grave_rows)} gray ({len(grave_rows)/len(rows):.1%}) | "
               f"{rep.checks} checks")
    print(summary)
    if rep.failures:
        print(f"RESULT: FAIL ({len(rep.failures)} failing checks)")
        for f in rep.failures:
            print("  - " + f)
        return 1
    print("RESULT: PASS - reference base is internally consistent and DOI-verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())

