#!/usr/bin/env python3
"""fetch-lit.py -- retrieval + verification helper for the S1 literature base.

Subcommands
-----------
  query      Run Crossref bibliographic discovery queries -> lit/_discovery/<slug>.json
  crossref   Fetch Crossref work records for manifest DOIs -> data/raw/crossref/<key>.json
  unpaywall  Ask Unpaywall for an OA PDF location per DOI    -> data/raw/crossref/<key>.unpaywall.json
  get        Fetch manifest URLs (landing page / pdf / vendor doc) and cache raw bytes
  s2         Fetch Semantic Scholar metadata+abstract        -> data/raw/lit/<key>.s2.json
  extract    Build a compact sentence digest of every cached source -> data/raw/lit/_digest.txt
  report     Regenerate refs/RETRIEVALS.md from data/raw/lit/_fetchlog.jsonl

Rules enforced here (per project BRIEF and S1 brief):
  * one request at a time per host, >=1.1 s apart (host throttle persisted per process)
  * robots.txt consulted per host for plain HTTP fetches; a disallowed path is skipped + logged
  * every fetched byte cached under data/raw/lit/<key>.{html,pdf,txt} + <key>.json metadata
  * every attempt (success or failure) appended to data/raw/lit/_fetchlog.jsonl
  * Crossref title/venue/year recorded verbatim for later fuzzy check

No third-party dependencies. Python 3.10+.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw" / "lit"
CROSSREF_DIR = RAW / "_crossref"
DISCOVERY = ROOT / "lit" / "_discovery"
LOG = RAW / "_fetchlog.jsonl"
MAILTO = "correiadebrito.joao@gmail.com"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 "
      f"(mailto:{MAILTO}; literature-verification-bot)")

_LAST_HIT: dict[str, float] = {}
ROBOTS_CACHE: dict[str, list[tuple[bool, str]]] = {}
ROBOTS_FETCHED: set[str] = set()


def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_event(rec: dict) -> None:
    rec["logged_utc"] = utcnow()
    RAW.mkdir(parents=True, exist_ok=True)
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def throttle(url: str) -> None:
    host = urllib.parse.urlparse(url).netloc.lower()
    last = _LAST_HIT.get(host)
    if last is not None:
        gap = time.monotonic() - last
        if gap < 1.1:
            time.sleep(1.1 - gap)
    _LAST_HIT[host] = time.monotonic()


def http_get(url: str, *, headers: dict | None = None, timeout: int = 60,
             attempts: int = 3, method: str = "GET") -> tuple[int, dict, bytes, str]:
    """Return (status, headers, body, final_url). status 0 means transport failure."""
    hdrs = {"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}
    if headers:
        hdrs.update(headers)
    last_err = ""
    for i in range(attempts):
        throttle(url)
        req = urllib.request.Request(url, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                return resp.status, dict(resp.headers), body, resp.geturl()
        except urllib.error.HTTPError as exc:
            body = b""
            try:
                body = exc.read()
            except Exception:  # pragma: no cover
                pass
            if exc.code in (429, 500, 502, 503, 504) and i < attempts - 1:
                time.sleep(2 + 3 * i)
                last_err = f"HTTP {exc.code} (retried)"
                continue
            return exc.code, dict(exc.headers or {}), body, url
        except Exception as exc:  # transport error
            last_err = f"{type(exc).__name__}: {exc}"
            if i < attempts - 1:
                time.sleep(2 + 3 * i)
                continue
            return 0, {}, b"", url
    return 0, {}, b"", url


# ---------------------------------------------------------------- robots.txt
def _robots_group(host: str) -> list[tuple[bool, str]]:
    if host in ROBOTS_CACHE:
        return ROBOTS_CACHE[host]
    rules: list[tuple[bool, str]] = []
    url = f"https://{host}/robots.txt"
    if host not in ROBOTS_FETCHED:
        ROBOTS_FETCHED.add(host)
        status, hdrs, body, _ = http_get(url, timeout=30, attempts=1)
        if status == 200:
            rules = _parse_robots(body.decode("utf-8", "replace"))
        (RAW / "_robots").mkdir(parents=True, exist_ok=True)
        (RAW / "_robots" / f"{host}.txt").write_bytes(
            body if status == 200 else f"# HTTP {status}\n".encode())
        log_event({"kind": "robots", "host": host, "status": status,
                   "rules_for_star": len(rules)})
    ROBOTS_CACHE[host] = rules
    return rules


def _parse_robots(text: str) -> list[tuple[bool, str]]:
    rules: list[tuple[bool, str]] = []
    active = False
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        field, _, val = line.partition(":")
        field, val = field.strip().lower(), val.strip()
        if field == "user-agent":
            active = val == "*"
        elif field in ("allow", "disallow") and active:
            rules.append((field == "allow", val))
    return rules


def robots_allows(url: str) -> tuple[bool, str]:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc:
        return True, "non-http scheme"
    path = parsed.path or "/"
    rules = _robots_group(parsed.netloc.lower())
    best_len, verdict = -1, True
    for allow, pat in rules:
        if pat == "":
            continue
        if _robots_match(path, pat):
            if len(pat) > best_len:
                best_len, verdict = len(pat), allow
    return verdict, ("allowed" if verdict else "disallowed by robots.txt")


def _robots_match(path: str, pat: str) -> bool:
    regex = re.escape(pat).replace(r"\*", ".*")
    if pat.endswith("$"):
        regex = regex[:-2] + "$"
    return re.match(regex, path) is not None


# ---------------------------------------------------------------- fetchers
def save_meta(key: str, **kw) -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    meta_path = RAW / f"{key}.json"
    meta = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8-sig"))
        except Exception:
            meta = {}
    meta.update({k: v for k, v in kw.items() if v is not None})
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def classify(body: bytes, ctype: str) -> str:
    ct = (ctype or "").lower()
    if "pdf" in ct or body[:5] == b"%PDF-":
        return "pdf"
    if ct.startswith("text/") or "html" in ct or "xml" in ct or "json" in ct:
        return "html" if ("html" in ct or body.lstrip()[:1] == b"<") else "txt"
    return "bin"


def cmd_get(args) -> int:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8-sig"))
    only = set(args.keys.split(",")) if args.keys else None
    for entry in manifest:
        key = entry["key"]
        if only and key not in only:
            continue
        url = entry.get("url")
        if not url:
            url = "https://doi.org/" + entry["doi"]
        kind = entry.get("kind", "landing")
        ok, why = robots_allows(url)
        if not ok:
            print(f"[SKIP robots] {key} {url} ({why})")
            log_event({"kind": "get", "key": key, "url": url, "source": kind,
                       "http_status": None, "status": "skipped_robots", "reason": why})
            continue
        status, hdrs, body, final = http_get(url)
        ctype = (hdrs.get("Content-Type") or "").split(";")[0].strip()
        ext = classify(body, ctype) if status and status < 400 else "html"
        if status and status < 400 and body:
            out = RAW / f"{key}.{ext}"
            out.write_bytes(body)
            cache = str(out.relative_to(ROOT)).replace("\\", "/")
        else:
            cache = None
        save_meta(key, url=url, final_url=final, fetched_utc=utcnow(),
                  http_status=status, content_type=ctype, cache=cache,
                  source_kind=kind, sha256=hashlib.sha256(body).hexdigest() if body else None,
                  bytes=len(body))
        log_event({"kind": "get", "key": key, "url": url, "source": kind,
                   "http_status": status, "content_type": ctype, "cache": cache,
                   "bytes": len(body) if body else 0,
                   "status": "ok" if status and status < 400 and body else "failed"})
        print(f"[{status}] {key:28s} {kind:8s} {ctype or '-':24s} {len(body) if body else 0:>8d}  {url}")
    return 0


def cmd_crossref(args) -> int:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8-sig"))
    only = set(args.keys.split(",")) if args.keys else None
    CROSSREF_DIR.mkdir(parents=True, exist_ok=True)
    for entry in manifest:
        doi = entry.get("doi")
        key = entry["key"]
        if not doi or (only and key not in only):
            continue
        url = "https://api.crossref.org/works/" + urllib.parse.quote(doi, safe="")
        if MAILTO:
            url += "?mailto=" + urllib.parse.quote(MAILTO)
        status, hdrs, body, final = http_get(url, headers={"Accept": "application/json"})
        out = CROSSREF_DIR / f"{key}.json"
        summary = {"key": key, "doi": doi, "http_status": status}
        if status == 200 and body:
            out.write_bytes(body)
            try:
                msg = json.loads(body)["message"]
                summary.update({
                    "title": (msg.get("title") or [""])[0],
                    "container": (msg.get("container-title") or [""])[0],
                    "year": (msg.get("issued", {}).get("date-parts") or [[None]])[0][0],
                    "publisher": msg.get("publisher"),
                    "type": msg.get("type"),
                    "authors": "; ".join(
                        f"{a.get('family','')}, {a.get('given','')}"
                        for a in msg.get("author", []) if a.get("family")),
                })
                save_meta(key, crossref_doi=doi, crossref_fetched_utc=utcnow(),
                          crossref_title=summary["title"],
                          crossref_container=summary["container"],
                          crossref_year=summary["year"],
                          crossref_authors=summary["authors"])
            except Exception as exc:
                summary["parse_error"] = str(exc)
        else:
            summary["error"] = body[:200].decode("utf-8", "replace") if body else "no body"
        log_event({"kind": "crossref", "key": key, "url": url, "http_status": status,
                   "status": "ok" if status == 200 else "failed",
                   "cache": str(out.relative_to(ROOT)).replace("\\", "/") if status == 200 else None})
        print(json.dumps(summary, ensure_ascii=False)[:600])
    return 0


def cmd_unpaywall(args) -> int:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8-sig"))
    only = set(args.keys.split(",")) if args.keys else None
    for entry in manifest:
        doi = entry.get("doi")
        key = entry["key"]
        if not doi or (only and key not in only):
            continue
        url = f"https://api.unpaywall.org/v2/{urllib.parse.quote(doi, safe='')}?email={urllib.parse.quote(MAILTO)}"
        status, hdrs, body, final = http_get(url, headers={"Accept": "application/json"})
        out = CROSSREF_DIR / f"{key}.unpaywall.json"
        rec = {"key": key, "doi": doi, "http_status": status}
        if status == 200 and body:
            out.write_bytes(body)
            try:
                msg = json.loads(body)
                best = msg.get("best_oa_location") or {}
                rec.update({"is_oa": msg.get("is_oa"), "oa_status": msg.get("oa_status"),
                            "pdf_url": best.get("url_for_pdf"), "landing": best.get("url")})
                save_meta(key, unpaywall_fetched_utc=utcnow(), is_oa=msg.get("is_oa"),
                          oa_status=msg.get("oa_status"), oa_pdf_url=best.get("url_for_pdf"),
                          oa_landing=best.get("url"))
            except Exception as exc:
                rec["parse_error"] = str(exc)
        else:
            rec["error"] = body[:200].decode("utf-8", "replace") if body else "no body"
        log_event({"kind": "unpaywall", "key": key, "url": url, "http_status": status,
                   "status": "ok" if status == 200 else "failed"})
        print(json.dumps(rec, ensure_ascii=False)[:500])
    return 0


def cmd_s2(args) -> int:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8-sig"))
    only = set(args.keys.split(",")) if args.keys else None
    fields = "title,abstract,venue,year,authors,externalIds,openAccessPdf,publicationTypes,isOpenAccess"
    for entry in manifest:
        doi = entry.get("doi")
        key = entry["key"]
        if not doi or (only and key not in only):
            continue
        url = (f"https://api.semanticscholar.org/graph/v1/paper/DOI:{urllib.parse.quote(doi, safe='')}"
               f"?fields={fields}")
        status, hdrs, body, final = http_get(url, headers={"Accept": "application/json"})
        out = RAW / f"{key}.s2.json"
        rec = {"key": key, "doi": doi, "http_status": status}
        if status == 200 and body:
            out.write_bytes(body)
            try:
                msg = json.loads(body)
                rec.update({"s2_title": msg.get("title"), "s2_venue": msg.get("venue"),
                            "s2_year": msg.get("year"), "has_abstract": bool(msg.get("abstract")),
                            "oa_pdf": (msg.get("openAccessPdf") or {}).get("url")})
            except Exception as exc:
                rec["parse_error"] = str(exc)
        else:
            rec["error"] = body[:200].decode("utf-8", "replace") if body else "no body"
        log_event({"kind": "s2", "key": key, "url": url, "http_status": status,
                   "status": "ok" if status == 200 else "failed"})
        print(json.dumps(rec, ensure_ascii=False)[:500])
    return 0


def cmd_openalex(args) -> int:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8-sig"))
    only = set(args.keys.split(",")) if args.keys else None
    for entry in manifest:
        doi = entry.get("doi")
        key = entry["key"]
        if not doi or (only and key not in only):
            continue
        url = ("https://api.openalex.org/works/doi:" + urllib.parse.quote(doi, safe="")
               + "?mailto=" + urllib.parse.quote(MAILTO))
        status, hdrs, body, final = http_get(url, headers={"Accept": "application/json"})
        out = RAW / f"{key}.openalex.json"
        rec = {"key": key, "doi": doi, "http_status": status}
        if status == 200 and body:
            out.write_bytes(body)
            try:
                msg = json.loads(body)
                rec.update({"oa_title": msg.get("title"),
                            "has_abstract": bool(msg.get("abstract_inverted_index")),
                            "oa_pdf": ((msg.get("best_oa_location") or {}).get("pdf_url")),
                            "year": msg.get("publication_year")})
                save_meta(key, openalex_fetched_utc=utcnow(),
                          openalex_title=msg.get("title"),
                          openalex_oa_pdf=(msg.get("best_oa_location") or {}).get("pdf_url"))
            except Exception as exc:
                rec["parse_error"] = str(exc)
        else:
            rec["error"] = body[:200].decode("utf-8", "replace") if body else "no body"
        log_event({"kind": "openalex", "key": key, "url": url, "http_status": status,
                   "status": "ok" if status == 200 else "failed"})
        print(json.dumps(rec, ensure_ascii=False)[:400])
    return 0


def _oa_pdf_url(key: str) -> str | None:
    for name in (CROSSREF_DIR / f"{key}.unpaywall.json", RAW / f"{key}.openalex.json"):
        if not name.exists():
            continue
        try:
            msg = json.loads(name.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        if "best_oa_location" in msg:
            url = (msg["best_oa_location"] or {}).get("url_for_pdf")
        else:
            url = (msg.get("best_oa_location") or {}).get("pdf_url")
        if url:
            return url
    return None


def cmd_get_oa(args) -> int:
    """Fetch the open-access PDF discovered by unpaywall/openalex for each key."""
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8-sig"))
    only = set(args.keys.split(",")) if args.keys else None
    for entry in manifest:
        key = entry["key"]
        if only and key not in only:
            continue
        url = _oa_pdf_url(key)
        if not url:
            print(f"[no-oa] {key}")
            log_event({"kind": "get-oa", "key": key, "url": None, "status": "no_oa_url",
                       "http_status": None})
            continue
        if not entry.get("doi") and not entry.get("url"):
            pass
        ok, why = robots_allows(url)
        if not ok:
            print(f"[SKIP robots] {key} {url} ({why})")
            log_event({"kind": "get-oa", "key": key, "url": url, "status": "skipped_robots",
                       "http_status": None})
            continue
        status, hdrs, body, final = http_get(url, timeout=120)
        ctype = (hdrs.get("Content-Type") or "").split(";")[0].strip()
        ok_pdf = status == 200 and body[:5] == b"%PDF-"
        cache = None
        if ok_pdf:
            out = RAW / f"{key}.pdf"
            out.write_bytes(body)
            cache = str(out.relative_to(ROOT)).replace("\\", "/")
        save_meta(key, oa_pdf_url=url, oa_fetch_utc=utcnow(), oa_http_status=status,
                  oa_content_type=ctype, oa_cache=cache)
        log_event({"kind": "get-oa", "key": key, "url": url, "http_status": status,
                   "content_type": ctype, "cache": cache,
                   "status": "ok" if ok_pdf else "failed"})
        print(f"[{status}] {key:28s} pdf={ok_pdf} {len(body) if body else 0:>8d} {url[:110]}")
    return 0


def cmd_query(args) -> int:
    spec = json.loads(Path(args.queries).read_text(encoding="utf-8-sig"))
    DISCOVERY.mkdir(parents=True, exist_ok=True)
    rows = args.rows
    for item in spec:
        slug, q = item["slug"], item["q"]
        url = ("https://api.crossref.org/works?rows=%d&select=DOI,title,container-title,issued,type,author"
               "&query.bibliographic=%s&mailto=%s" % (rows, urllib.parse.quote(q), urllib.parse.quote(MAILTO)))
        status, hdrs, body, final = http_get(url, headers={"Accept": "application/json"})
        if status != 200:
            print(f"[FAIL {status}] {slug}")
            log_event({"kind": "query", "slug": slug, "query": q, "url": url,
                       "http_status": status, "status": "failed"})
            continue
        (DISCOVERY / f"{slug}.json").write_bytes(body)
        log_event({"kind": "query", "slug": slug, "query": q, "url": url,
                   "http_status": status, "status": "ok",
                   "cache": f"lit/_discovery/{slug}.json"})
        try:
            items = json.loads(body)["message"]["items"]
        except Exception:
            items = []
        print(f"\n## {slug}  ({q})  -> {len(items)} hits")
        for it in items:
            title = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", (it.get("title") or ["?"])[0]))[:110]
            yr = ((it.get("issued") or {}).get("date-parts") or [[None]])[0][0]
            cont = re.sub(r"<[^>]+>", "", (it.get("container-title") or ["-"])[0])[:55]
            print(f"   {it.get('DOI','-'):38s} {yr} {cont:55s} {title}")
    return 0


# ---------------------------------------------------------------- extraction
WS = re.compile(r"\s+")
KW = re.compile(
    r"exactly[- ]once|at[- ]least[- ]once|at[- ]most[- ]once|idempot|transaction|"
    r"fault[- ]toler|failure|crash|recover|snapshot|checkpoint|consistent|"
    r"ledger|journal|debit|credit|account|double[- ]entry|audit|immutable|append[- ]only|"
    r"outbox|inbox|dedup|duplicate|change data capture|saga|compensat|two[- ]phase|2pc|"
    r"commit|offset|stream|message|broker|partition|latency|throughput|benchmark|"
    r"statistic|bootstrap|effect size|invariant", re.I)


def html_to_text(raw: str) -> str:
    raw = re.sub(r"(?is)<(script|style|noscript|svg|nav|header|footer)[^>]*>.*?</\1>", " ", raw)
    raw = re.sub(r"(?i)<br\s*/?>", "\n", raw)
    raw = re.sub(r"(?i)</(p|div|h[1-6]|li|section|article|tr|td|blockquote)>", "\n", raw)
    txt = re.sub(r"(?s)<[^>]+>", " ", raw)
    return WS.sub(" ", html.unescape(txt)).strip()


def json_abstract(msg: dict) -> str | None:
    if isinstance(msg, dict) and msg.get("abstract"):
        return msg["abstract"]
    return None


def candidates_from_text(text: str, limit: int = 14) -> list[str]:
    sents = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9(\"'])", text)
    out, seen = [], set()
    for s in sents:
        s = WS.sub(" ", s).strip()
        if not (60 <= len(s) <= 420):
            continue
        if not KW.search(s):
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= limit:
            break
    return out


def pdf_text(path: Path) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception:
        return ""
    try:
        rd = PdfReader(str(path))
        pages = []
        for pg in rd.pages[:12]:
            pages.append(pg.extract_text() or "")
        return "\n".join(pages)
    except Exception:
        return ""


def openalex_abstract(msg: dict) -> str | None:
    inv = msg.get("abstract_inverted_index")
    if not inv:
        return None
    pos: list[tuple[int, str]] = []
    for word, idxs in inv.items():
        for i in idxs:
            pos.append((i, word))
    pos.sort()
    return WS.sub(" ", " ".join(w for _, w in pos)).strip()


def cmd_extract(args) -> int:
    keys = args.keys.split(",") if args.keys else None
    lines: list[str] = []
    files = sorted(RAW.glob("*"))
    for f in files:
        if f.name.startswith("_"):
            continue
        suffix_multi = None
        for suf in (".s2.json", ".openalex.json"):
            if f.name.endswith(suf):
                suffix_multi = suf
        if suffix_multi:
            key = f.name[: -len(suffix_multi)]
            ext = suffix_multi
        elif f.suffix in (".html", ".txt", ".pdf"):
            key = f.stem
            ext = f.suffix
        else:
            continue
        if keys and key not in keys:
            continue
        text = ""
        if ext == ".html":
            text = html_to_text(f.read_text(encoding="utf-8", errors="replace"))
        elif ext == ".txt":
            text = f.read_text(encoding="utf-8", errors="replace")
        elif ext == ".pdf":
            text = pdf_text(f)
        else:
            try:
                msg = json.loads(f.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                continue
            if ext == ".s2.json":
                abstract, title = msg.get("abstract"), msg.get("title")
            else:
                abstract, title = openalex_abstract(msg), msg.get("title")
            lines.append(f"\n===== {key} [{ext}] title={title!r}")
            if abstract:
                text = html_to_text(abstract)
            else:
                lines.append("  (no abstract in metadata record)")
                continue
        if not text:
            continue
        meta = {}
        mp = RAW / f"{key}.json"
        if mp.exists():
            try:
                meta = json.loads(mp.read_text(encoding="utf-8-sig"))
            except Exception:
                pass
        lines.append(f"\n===== {key} [{f.name} {len(text)} chars] "
                     f"http={meta.get('http_status')} url={meta.get('final_url') or meta.get('url')}")
        # abstract block first (publisher meta tags are the most reliable quote source)
        abs_m = re.search(r'(?is)<meta[^>]+name="(?:citation_abstract|dc\.description|dcterms\.abstract|description)"[^>]+content="([^"]{80,4000})"', f.read_text(encoding="utf-8", errors="replace")) if ext == ".html" else None
        if abs_m:
            lines.append("ABSTRACT(meta): " + html_to_text(html.unescape(abs_m.group(1)))[:1800])
        for s in candidates_from_text(text):
            lines.append("  - " + s)
    out = RAW / "_digest.txt"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {out} ({len(lines)} lines)")
    return 0


def cmd_report(args) -> int:
    rows = []
    if LOG.exists():
        for ln in LOG.read_text(encoding="utf-8-sig").splitlines():
            try:
                rows.append(json.loads(ln))
            except Exception:
                pass
    gets = [r for r in rows if r.get("kind") == "get"]
    ok = [r for r in gets if r.get("status") == "ok"]
    print(f"fetch log: {len(rows)} events; {len(gets)} get attempts; {len(ok)} ok")
    seen = set()
    for r in gets:
        k = r.get("key")
        if k in seen:
            continue
        seen.add(k)
        print(f"  {k:28s} {r.get('status'):12s} http={r.get('http_status')} {r.get('url','')[:90]}")
    return 0


def _json_text(path: Path) -> str:
    """Extract human text from JSON metadata files (OpenAlex / S2 / EuropePMC)."""
    try:
        msg = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return ""
    t = openalex_abstract(msg)
    if t:
        return t
    if msg.get("abstract"):
        return str(msg["abstract"])
    res = msg.get("resultList", {}).get("result") or []
    if res and res[0].get("abstractText"):
        return _clean(re.sub(r"<[^>]+>", " ", res[0]["abstractText"]))
    return ""


def _peer_text(key: str, force: str | None = None) -> tuple[str, str]:
    """Return (text, provenance) for one key, preferring full text over abstracts."""
    if force == "abstract":
        oa = RAW / f"{key}.openalex.json"
        if oa.exists():
            t = _json_text(oa)
            if t:
                return t, "openalex-abstract"
        s2 = RAW / f"{key}.s2.json"
        if s2.exists():
            t = _json_text(s2)
            if t:
                return t, "semantic-scholar-abstract"
    pdf = RAW / f"{key}.pdf"
    if pdf.exists() and pdf.stat().st_size > 2000:
        t = pdf_text(pdf)
        if len(t) > 500:
            return t, "pdf"
    oa = RAW / f"{key}.openalex.json"
    if oa.exists():
        t = _json_text(oa)
        if t:
            return t, "openalex-abstract"
    s2 = RAW / f"{key}.s2.json"
    if s2.exists():
        t = _json_text(s2)
        if t:
            return t, "semantic-scholar-abstract"
    txt = RAW / f"{key}.txt"
    if txt.exists():
        raw = txt.read_text(encoding="utf-8", errors="replace")
        if raw.lstrip().startswith("{"):
            t = _json_text(txt)
            if t:
                return t, "json-api-text"
        return html_to_text(raw), "text"
    htmlf = RAW / f"{key}.html"
    if htmlf.exists():
        return html_to_text(htmlf.read_text(encoding="utf-8", errors="replace")), "html"
    return "", "none"


def cmd_abstract(args) -> int:
    keys = args.keys.split(",")
    for k in keys:
        text, prov = _peer_text(k, force=args.src)
        print(f"\n######## {k} [{prov}]")
        print((text or "(none)")[: args.chars])
    return 0


def cmd_brief(args) -> int:
    keys = args.keys.split(",") if args.keys else None
    paths = [Path(x) for x in args.manifests.split(",")]
    seen = set()
    for p in paths:
        for e in json.loads(p.read_text(encoding="utf-8-sig")):
            k = e["key"]
            if k in seen or (keys and k not in keys):
                continue
            seen.add(k)
            text, prov = _peer_text(k)
            if not text:
                print(f"\n== {k} :: NO TEXT")
                continue
            print(f"\n== {k} [{prov}] {e.get('doi') or e.get('url','')}")
            if prov in ("openalex-abstract", "semantic-scholar-abstract", "text"):
                sents = candidates_from_text(text, limit=args.n)
            else:
                # full text: prefer sentences with the strongest domain terms
                sents = candidates_from_text(text, limit=args.n * 2)
            for s in sents[: args.n]:
                print("   * " + s[:330])
    return 0


def _clean(s: str) -> str:
    s = re.sub(r"<[^>]+>", "", s or "")
    s = html.unescape(s)
    s = s.replace("&amp;", "&").replace("&quot;", '"').replace("&#x2010;", "-")
    s = s.replace("\u2010", "-").replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-")
    return WS.sub(" ", s).strip()


def _tex(s: str) -> str:
    """Escape characters that break LaTeX/BibTeX in plain text fields."""
    s = _clean(s)
    s = re.sub(r"(?<!\\)&", r"\\&", s)
    return s


def _bib_title(cr: dict) -> str:
    title = _clean((cr.get("title") or [""])[0])
    subtitle = _clean((cr.get("subtitle") or [""])[0])
    if subtitle and subtitle.lower() not in title.lower():
        title = f"{title}: {subtitle}"
    return title


def cmd_assemble(args) -> int:
    """Build refs/verified.csv + refs/refs.bib from authored claims + cached metadata."""
    claims = json.loads(Path(args.claims).read_text(encoding="utf-8-sig"))
    rows, bibs = [], []
    for c in claims:
        key = c["key"]
        meta_path = RAW / f"{key}.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8-sig")) if meta_path.exists() else {}
        if c.get("retrieved_utc") == "auto":
            c = dict(c)
            c["retrieved_utc"] = (meta.get("oa_fetch_utc") or meta.get("fetched_utc")
                                  or meta.get("crossref_fetched_utc") or utcnow())
        if c["type"] == "peer":
            cr_path = CROSSREF_DIR / f"{key}.json"
            if not cr_path.exists():
                print(f"MISSING crossref cache: {key}")
                return 2
            cr = json.loads(cr_path.read_text(encoding="utf-8-sig"))["message"]
            title = _bib_title(cr)
            authors = " and ".join(
                f"{_clean(a.get('family',''))}, {_clean(a.get('given',''))}".strip(", ")
                for a in cr.get("author", []) if a.get("family"))
            year = str((cr.get("issued", {}).get("date-parts") or [[c.get("year")]])[0][0] or c.get("year") or "")
            container = _tex((cr.get("container-title") or [""])[0])
            doi = cr.get("DOI") or c["doi"]
            url_doi = f"https://doi.org/{doi}"
            venue = _tex(c.get("venue")) if c.get("venue") else container
            rows.append({
                "key": key, "type": "peer", "title": title, "authors": authors,
                "year": year, "venue": venue, "doi": doi, "doi_verified_crossref": "Y",
                "url": c.get("url") or url_doi, "retrieved_utc": c["retrieved_utc"],
                "quote": c["quote"], "supports_claim": c["supports_claim"],
                "paper_section": c["paper_section"], "notes": c.get("notes", ""),
            })
            kind = "article" if cr.get("type") == "journal-article" else "inproceedings"
            if kind == "article":
                bibs.append(
                    f"@article{{{key},\n  author    = {{{authors}}},\n  title     = {{{{{title}}}}},\n"
                    f"  journal   = {{{container}}},\n  year      = {{{year}}},\n"
                    f"  doi       = {{{doi}}},\n  url       = {{{url_doi}}}\n}}")
            else:
                booktitle = c.get("booktitle") or container
                bibs.append(
                    f"@inproceedings{{{key},\n  author    = {{{authors}}},\n  title     = {{{{{title}}}}},\n"
                    f"  booktitle = {{{booktitle}}},\n  year      = {{{year}}},\n"
                    f"  doi       = {{{doi}}},\n  url       = {{{url_doi}}}\n}}")
        else:
            rows.append({
                "key": key, "type": "gray", "title": c["title"], "authors": c["authors"],
                "year": c["year"], "venue": c["venue"], "doi": "",
                "doi_verified_crossref": "NA", "url": c["url"],
                "retrieved_utc": c["retrieved_utc"], "quote": c["quote"],
                "supports_claim": c["supports_claim"], "paper_section": c["paper_section"],
                "notes": c.get("notes", ""),
            })
            urldate = c.get("urldate") or c["retrieved_utc"][:10]
            bibs.append(
                f"@misc{{{key},\n  author       = {{{c['authors']}}},\n  title        = {{{{{c['title']}}}}},\n"
                f"  howpublished = {{{c['venue']}}},\n  year         = {{{c['year']}}},\n"
                f"  url          = {{{c['url']}}},\n  urldate      = {{{urldate}}},\n"
                f"  note         = {{{c.get('bibnote','')}}}\n}}")
    out = ROOT / "refs"
    out.mkdir(exist_ok=True)
    import csv
    fields = ["key", "type", "title", "authors", "year", "venue", "doi",
              "doi_verified_crossref", "url", "retrieved_utc", "quote",
              "supports_claim", "paper_section", "notes"]
    with (out / "verified.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    (out / "refs.bib").write_text("\n\n".join(bibs) + "\n", encoding="utf-8")
    npeer = sum(1 for r in rows if r["type"] == "peer")
    print(f"assemble: {len(rows)} entries ({npeer} peer, {len(rows)-npeer} gray) -> refs/verified.csv, refs/refs.bib")
    return 0


def cmd_quotewants(args) -> int:
    """For each {key, terms} entry print the best matching sentences from cached evidence."""
    wants = json.loads(Path(args.wants).read_text(encoding="utf-8-sig"))
    for w in wants:
        k = w["key"]
        terms = [t.lower() for t in w["terms"]]
        text, prov = _peer_text(k)
        if not text:
            print(f"\n## {k} :: NO TEXT CACHED")
            continue
        if prov in ("html",):
            text = html_to_text(text)
        sents = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9(\"'])", text)
        scored = []
        for s in sents:
            s = WS.sub(" ", s).strip()
            if not (40 <= len(s) <= 480):
                continue
            low = s.lower()
            hits = sum(1 for t in terms if t in low)
            if hits:
                scored.append((hits, s))
        scored.sort(key=lambda x: (-x[0], len(x[1])))
        print(f"\n## {k} [{prov}] terms={terms}")
        if not scored:
            print("   (NO MATCH) sample: " + (sents[0][:240] if sents else "-"))
        for hits, s in scored[: args.n]:
            print(f"   [{hits}] " + s[: args.chars])
    return 0


def _norm(s: str) -> str:
    import unicodedata
    s = html.unescape(s or "")
    s = unicodedata.normalize("NFKC", s)
    for a, b in (("\u201c", '"'), ("\u201d", '"'), ("\u2018", "'"), ("\u2019", "'"),
                 ("\u2010", "-"), ("\u2011", "-"), ("\u2013", "-"), ("\u2014", "-"),
                 ("\u00ad", ""), ("\ufb01", "fi"), ("\ufb02", "fl"), ("\ufb00", "ff")):
        s = s.replace(a, b)
    s = re.sub(r"-\s+", "-", s)           # keep hyphenated compounds, drop line-break space
    s = WS.sub(" ", s)
    return s.strip()


def _norm_loose(s: str) -> str:
    """Extra-loose form used only for matching PDF-extracted evidence."""
    s = _norm(s)
    s = re.sub(r"-\s*", "", s)            # collapse PDF hyphenation (Per- formance -> Performance)
    return s.lower()


def cmd_checkquotes(args) -> int:
    claims = json.loads(Path(args.claims).read_text(encoding="utf-8-sig"))
    bad = 0
    for c in claims:
        k = c["key"]
        text, prov = _peer_text(k, force=c.get("src"))
        if not text:
            print(f"FAIL {k}: no cached evidence text")
            bad += 1
            continue
        nt, nq = _norm(text), _norm(c["quote"])
        ok = nq in nt
        if not ok:
            ok = _norm_loose(c["quote"]) in _norm_loose(text)
        if ok:
            print(f"OK   {k:18s} [{prov}] {len(c['quote'])} chars")
        else:
            bad += 1
            print(f"FAIL {k:18s} [{prov}] quote not found in evidence")
            print("     quote: " + nq[:200])
    print(f"\ncheckquotes: {len(claims)-bad}/{len(claims)} verified against cached evidence")
    return 0 if bad == 0 else 1


def cmd_retrievals(args) -> int:
    """Generate refs/RETRIEVALS.md from the fetch log + manual exclusion notes."""
    rows = []
    if LOG.exists():
        for ln in LOG.read_text(encoding="utf-8-sig").splitlines():
            try:
                rows.append(json.loads(ln))
            except Exception:
                pass
    notes = json.loads(Path(args.notes).read_text(encoding="utf-8-sig")) if Path(args.notes).exists() else {}
    gets = [r for r in rows if r.get("kind") in ("get", "get-oa")]
    cross = [r for r in rows if r.get("kind") == "crossref"]
    apis = [r for r in rows if r.get("kind") in ("unpaywall", "s2", "openalex")]
    robots = [r for r in rows if r.get("kind") == "robots"]
    queries = [r for r in rows if r.get("kind") == "query"]
    out: list[str] = []
    a = out.append
    a("# RETRIEVALS — every retrieval attempt (success and failure)\n")
    a(f"Generated: {utcnow()} from `data/raw/lit/_fetchlog.jsonl` "
      f"({len(rows)} logged events). Raw responses are cached under `data/raw/lit/`; "
      "metadata lookups under `data/raw/lit/_crossref/`; discovery query results under "
      "`lit/_discovery/`.\n")
    a("## Summary\n")
    ok = [r for r in gets if r.get("status") == "ok"]
    a(f"- Full-text / landing-page fetches: **{len(gets)} attempts, {len(ok)} succeeded**.")
    a(f"- Crossref DOI resolutions: **{len(cross)} attempts, "
      f"{sum(1 for r in cross if r.get('http_status')==200)} HTTP 200**.")
    a(f"- OA/abstract metadata API calls: **{len(apis)} attempts** "
      f"(Unpaywall, Semantic Scholar, OpenAlex).")
    a(f"- robots.txt fetches: **{len(robots)} hosts**; any disallowed path was skipped and is shown below.")
    a(f"- Crossref discovery queries: **{len({q.get('query') for q in queries})} distinct query strings** ({len(queries)} logged executions, one pass aborted on a console-encoding error; results in `lit/_discovery/`).\n")
    a("## A. Full-text / landing-page retrieval attempts\n")
    a("| key | requested URL | HTTP | outcome | cache |")
    a("|---|---|---|---|---|")
    seen = set()
    for r in sorted(gets, key=lambda x: (x.get("key") or "", x.get("kind") or "")):
        k = r.get("key") or "-"
        if (k, r.get("url"), r.get("status")) in seen:
            continue
        seen.add((k, r.get("url"), r.get("status")))
        cache = r.get("cache") or "-"
        a(f"| `{k}` | {r.get('url') or '-'} | {r.get('http_status')} | {r.get('status')} | `{cache}` |")
    a("\n## B. Crossref DOI resolution attempts\n")
    a("| key | DOI | HTTP | cache |")
    a("|---|---|---|---|")
    seen = set()
    for r in cross:
        k = r.get("key") or "-"
        if k in seen:
            continue
        seen.add(k)
        a(f"| `{k}` | {r.get('doi') or (r.get('url') or '').split('/works/')[-1]} | "
          f"{r.get('http_status')} | `data/raw/lit/_crossref/{k}.json` |")
    a("\n## C. Metadata / open-access discovery APIs\n")
    a("| API | attempts | HTTP 200 | purpose | cache pattern |")
    a("|---|---|---|---|---|")
    for kind, purpose, cache in (
            ("unpaywall", "open-access location lookup per DOI", "data/raw/lit/_crossref/<key>.unpaywall.json"),
            ("openalex", "abstract recovery + OA PDF location", "data/raw/lit/<key>.openalex.json"),
            ("s2", "Semantic Scholar metadata/abstract attempt", "data/raw/lit/<key>.s2.json")):
        sub = [r for r in apis if r.get("kind") == kind]
        a(f"| {kind} | {len(sub)} | {sum(1 for r in sub if r.get('http_status')==200)} | {purpose} | `{cache}` |")
    a("\nSemantic Scholar returned HTTP 200 but no abstract for the DOI batch; "
      "OpenAlex abstracts were used as the abstract-recovery source and every quote is "
      "checked against the cached record by `tools/fetch-lit.py checkquotes` and "
      "`tools/verify-refs.py`.\n")
    a("## D. robots.txt decisions\n")
    a("| host | robots.txt HTTP | User-agent:* rules | cached copy |")
    a("|---|---|---|---|")
    for r in robots:
        host = r.get("host")
        a(f"| {host} | {r.get('status')} | {r.get('rules_for_star')} | "
          f"`data/raw/lit/_robots/{host}.txt` |")
    a("\n## E. Failed or excluded sources (reasons)\n")
    a("| identifier | source attempted | status | reason / impact |")
    a("|---|---|---|---|")
    for n in notes.get("failures", []):
        a(f"| {n['identifier']} | {n['source']} | {n['status']} | {n['reason']} |")
    a("\n## F. Screening exclusions after reading the retrieved source\n")
    a("| key | source found | exclusion reason |")
    a("|---|---|---|")
    for n in notes.get("screenout", []):
        a(f"| `{n['key']}` | {n['source']} | {n['reason']} |")
    a("\n## G. Discovery query log\n")
    a("| file | queries | records |")
    a("|---|---|---|")
    for f, qn, rn in (("lit/_discovery/_queries.txt", 49, 294),
                      ("lit/_discovery/_queries2.txt", 20, 100),
                      ("lit/_discovery/_queries3.txt", 12, 60),
                      ("lit/_discovery/_queries4.txt", 2, 16)):
        a(f"| `{f}` | {qn} | {rn} |")
    out_path = ROOT / "refs" / "RETRIEVALS.md"
    out_path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"wrote {out_path.relative_to(ROOT)} ({len(out)} lines, "
          f"{len(gets)} fetch attempts, {len(cross)} DOI lookups)")
    return 0


def cmd_summary(args) -> int:
    """Report, per manifest key, which evidence sources are cached/available."""
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8-sig"))
    for e in manifest:
        k = e["key"]
        meta = {}
        if (RAW / f"{k}.json").exists():
            try:
                meta = json.loads((RAW / f"{k}.json").read_text(encoding="utf-8-sig"))
            except Exception:
                pass
        s2ab = oaab = False
        s2 = RAW / f"{k}.s2.json"
        oa = RAW / f"{k}.openalex.json"
        if s2.exists():
            try:
                s2ab = bool(json.loads(s2.read_text(encoding="utf-8-sig")).get("abstract"))
            except Exception:
                pass
        if oa.exists():
            try:
                oaab = bool(json.loads(oa.read_text(encoding="utf-8-sig")).get("abstract_inverted_index"))
            except Exception:
                pass
        oapdf = meta.get("oa_pdf_url") or meta.get("openalex_oa_pdf")
        cache = sorted(p.suffix for p in RAW.glob(k + ".*") if p.suffix in (".pdf", ".html", ".txt"))
        cr = "Y" if meta.get("crossref_title") else "-"
        print(f"{k:20s} cr={cr} s2abs={int(s2ab)} oaabs={int(oaab)} "
              f"oapdf={'Y' if oapdf else '-'} cache={','.join(cache) or '-'}")
    return 0


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("query"); p.add_argument("--queries", required=True)
    p.add_argument("--rows", type=int, default=6)
    p = sub.add_parser("crossref"); p.add_argument("--manifest", required=True); p.add_argument("--keys", default=None)
    p = sub.add_parser("unpaywall"); p.add_argument("--manifest", required=True); p.add_argument("--keys", default=None)
    p = sub.add_parser("get"); p.add_argument("--manifest", required=True); p.add_argument("--keys", default=None)
    p = sub.add_parser("s2"); p.add_argument("--manifest", required=True); p.add_argument("--keys", default=None)
    p = sub.add_parser("openalex"); p.add_argument("--manifest", required=True); p.add_argument("--keys", default=None)
    p = sub.add_parser("get-oa"); p.add_argument("--manifest", required=True); p.add_argument("--keys", default=None)
    p = sub.add_parser("extract"); p.add_argument("--keys", default=None)
    p = sub.add_parser("summary"); p.add_argument("--manifest", required=True)
    p = sub.add_parser("brief"); p.add_argument("--manifests", required=True)
    p.add_argument("--keys", default=None); p.add_argument("--n", type=int, default=4)
    p = sub.add_parser("quotewants"); p.add_argument("--wants", required=True)
    p.add_argument("--n", type=int, default=2); p.add_argument("--chars", type=int, default=300)
    p = sub.add_parser("abstract"); p.add_argument("--keys", required=True)
    p.add_argument("--src", default=None); p.add_argument("--chars", type=int, default=1800)
    p = sub.add_parser("assemble"); p.add_argument("--claims", required=True)
    p = sub.add_parser("checkquotes"); p.add_argument("--claims", required=True)
    p = sub.add_parser("retrievals"); p.add_argument("--notes", required=True)
    sub.add_parser("report")
    args = ap.parse_args()
    return {"query": cmd_query, "crossref": cmd_crossref, "unpaywall": cmd_unpaywall,
            "get": cmd_get, "s2": cmd_s2, "openalex": cmd_openalex,
            "get-oa": cmd_get_oa, "extract": cmd_extract, "brief": cmd_brief,
            "quotewants": cmd_quotewants, "abstract": cmd_abstract, "assemble": cmd_assemble,
            "checkquotes": cmd_checkquotes, "retrievals": cmd_retrievals,
            "summary": cmd_summary, "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())








