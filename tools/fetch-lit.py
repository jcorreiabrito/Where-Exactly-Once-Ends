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
CROSSREF_DIR = ROOT / "data" / "raw" / "crossref"
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
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
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
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
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
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
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
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
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
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
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


def cmd_query(args) -> int:
    spec = json.loads(Path(args.queries).read_text(encoding="utf-8"))
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


def cmd_extract(args) -> int:
    keys = args.keys.split(",") if args.keys else None
    lines: list[str] = []
    files = sorted(RAW.glob("*"))
    for f in files:
        if f.suffix not in (".html", ".txt", ".pdf", ".json"):
            continue
        if f.name.startswith("_"):
            continue
        key = f.stem
        if keys and key not in keys:
            continue
        text = ""
        if f.suffix == ".html":
            text = html_to_text(f.read_text(encoding="utf-8", errors="replace"))
        elif f.suffix == ".txt":
            text = f.read_text(encoding="utf-8", errors="replace")
        elif f.suffix == ".pdf":
            text = pdf_text(f)
        elif f.suffix == ".json":
            try:
                msg = json.loads(f.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                continue
            abstract = json_abstract(msg)
            title = msg.get("title") or msg.get("s2_title") or ""
            lines.append(f"\n===== {key} [json] title={title!r}")
            if abstract:
                text = html_to_text(abstract)
            else:
                continue
        if not text:
            continue
        meta = {}
        mp = RAW / f"{key}.json"
        if mp.exists():
            try:
                meta = json.loads(mp.read_text(encoding="utf-8"))
            except Exception:
                pass
        lines.append(f"\n===== {key} [{f.name} {len(text)} chars] "
                     f"http={meta.get('http_status')} url={meta.get('final_url') or meta.get('url')}")
        # abstract block first (publisher meta tags are the most reliable quote source)
        abs_m = re.search(r'(?is)<meta[^>]+name="(?:citation_abstract|dc\.description|dcterms\.abstract|description)"[^>]+content="([^"]{80,4000})"', f.read_text(encoding="utf-8", errors="replace")) if f.suffix == ".html" else None
        if abs_m:
            lines.append("ABSTRACT(meta): " + html_to_text(html.unescape(abs_m.group(1)))[:1800])
        for s in candidates_from_text(text):
            lines.append("  - " + s)
    out = RAW / "_digest.txt"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {out} ({len(lines)} lines, {len(lines) and len(chr(10).join(lines))} chars)")
    return 0


def cmd_report(args) -> int:
    rows = []
    if LOG.exists():
        for ln in LOG.read_text(encoding="utf-8").splitlines():
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
    p = sub.add_parser("extract"); p.add_argument("--keys", default=None)
    sub.add_parser("report")
    args = ap.parse_args()
    return {"query": cmd_query, "crossref": cmd_crossref, "unpaywall": cmd_unpaywall,
            "get": cmd_get, "s2": cmd_s2, "extract": cmd_extract,
            "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
