Here’s a clean, production-ready collector.py that:

pulls jobs from Greenhouse and Lever (public, no-login JSON),

filters to Analyst+ non-sales roles only,

de-dupes and scores each job,

and POSTs top results to your Google Sheets Apps Script Web App via SHEET_WEBAPP_URL.

It’s designed for GitHub Actions (Python 3.11) with aiohttp only.

Before you run:

Put your Apps Script deployment URL into the repo secret SHEET_WEBAPP_URL.

(Optional) Create plain text files slugs_greenhouse.txt and/or slugs_lever.txt (one company slug per line) to scale up without editing code.

# collector.py
# Purpose: Collect Analyst+ (non-sales) roles from Greenhouse & Lever and push to Google Sheets (Apps Script Web App).
# Runtime: Python 3.11 (GitHub Actions), dependency: aiohttp
# Env var required: SHEET_WEBAPP_URL  -> your Apps Script Web App URL (POST endpoint)

import os
import re
import json
import time
import asyncio
import aiohttp
import hashlib
from pathlib import Path
from typing import List, Dict, Any

# -----------------------------
# Config: Role filters
# -----------------------------
# Include ONLY titles that match one of these "Analyst+" non-sales patterns:
INCLUDE_TITLE_PATTERNS = [
    r"\bfinancial analyst\b",
    r"\bcapital markets analyst\b",
    r"\binvestment analyst\b",
    r"\bportfolio analyst\b",
    r"\bportfolio coordinator\b",
    r"\bcredit analyst\b",
    r"\bbusiness intelligence analyst\b",
    r"\bbi analyst\b",
    r"\bdata analyst\b",
    r"\breporting analyst\b",
    r"\bdata operations analyst\b",
    r"\breal estate analyst\b",
    r"\basset management analyst\b",
    r"\bunderwriting analyst\b",
    r"\brisk analyst\b",
    r"\bfp&a analyst\b",
    r"\bcorporate finance analyst\b",
    r"\btreasury analyst\b",
    r"\bstrategy analyst\b",
    r"\boperations analyst\b",
]

# EXCLUDE if title contains any of these (sales-y / out-of-scope):
EXCLUDE_TITLE_PATTERNS = [
    r"\bsales\b",
    r"\bbusiness development\b",
    r"\bbd[r]?\b",
    r"\bsdr\b",
    r"\baccount executive\b",
    r"\bcustomer success\b",
    r"\bmarketing\b",
    r"\bretail\b",
    r"\bteller\b",
    r"\bloan officer\b",
    r"\boriginator\b",
    r"\brecruiter\b",
    r"\badmissions\b",
]

INCLUDE_RE = re.compile("|".join(INCLUDE_TITLE_PATTERNS), re.I)
EXCLUDE_RE = re.compile("|".join(EXCLUDE_TITLE_PATTERNS), re.I)

# Optional: prefer US/remote-friendly roles
LOC_OK_RE = re.compile(r"\b(remote|united states|usa|anywhere|california|los angeles)\b", re.I)

# -----------------------------
# Config: Sources (slugs)
# -----------------------------
# You can maintain slugs in text files for scale without touching code:
#   slugs_greenhouse.txt   (each line like: firstsolar)
#   slugs_lever.txt        (each line like: databricks)
def _load_slugs(fname: str, fallback: List[str]) -> List[str]:
    p = Path(fname)
    if p.exists():
        return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    return fallback

GREENHOUSE_SLUGS = _load_slugs("slugs_greenhouse.txt", [
    # add/remove freely; these are examples
    "firstsolar", "sunrun", "nextracker", "enphase-energy"
])

LEVER_SLUGS = _load_slugs("slugs_lever.txt", [
    # examples
    "affirm", "databricks", "stripe"
])

# -----------------------------
# Fit scoring (simple, transparent)
# -----------------------------
DESC_SKILLS = [
    "excel", "sql", "python", "model", "underwriting", "valuation",
    "capital markets", "fp&a", "dashboard", "automation", "apps script", "vba"
]

def fit_score(title: str, desc: str, loc: str) -> int:
    score = 0
    t = f"{title or ''} {desc or ''}".lower()

    # Base: strong match on Analyst+ include
    if INCLUDE_RE.search(title or ""):
        score += 3

    # Skills in description
    score += sum(1 for kw in DESC_SKILLS if kw in t)
    score = min(score, 5)

    # Location bonus
    if loc and LOC_OK_RE.search(loc):
        score = min(score + 1, 5)

    # Exclusion always zeroes out later (we filter before scoring), but keep returning >=0
    return max(score, 0)

def make_id(url: str, title: str) -> str:
    return hashlib.sha1(f"{url}|{title}".encode()).hexdigest()[:12]

# -----------------------------
# HTTP helpers
# -----------------------------
HEADERS = {"User-Agent": "Mozilla/5.0 (JobCollector/1.0)"}
TIMEOUT = aiohttp.ClientTimeout(total=25)

async def fetch_json(session: aiohttp.ClientSession, url: str) -> Any:
    try:
        async with session.get(url, timeout=TIMEOUT) as r:
            if r.status == 200:
                ct = r.headers.get("content-type", "")
                if "json" in ct:
                    return await r.json()
    except Exception:
        return None
    return None

# -----------------------------
# Greenhouse: https://boards.greenhouse.io/{slug}.json
# -----------------------------
async def fetch_greenhouse_company(session: aiohttp.ClientSession, slug: str) -> List[Dict[str, Any]]:
    url = f"https://boards.greenhouse.io/{slug}.json"
    data = await fetch_json(session, url)
    out: List[Dict[str, Any]] = []
    if not data:
        return out

    for j in data.get("jobs", []):
        title = (j.get("title") or "").strip()
        loc = ((j.get("location") or {}).get("name") or "").strip()
        jd_url = (j.get("absolute_url") or j.get("url") or "").strip()
        if not title or not jd_url:
            continue

        # Apply filters
        if EXCLUDE_RE.search(title):
            continue
        if not INCLUDE_RE.search(title):
            continue

        out.append({
            "source": "greenhouse",
            "company": slug,
            "title": title,
            "url": jd_url,
            "location": loc,
            "desc": ""  # greenhouse JSON endpoint doesn't include full desc here
        })
    return out

# -----------------------------
# Lever: https://api.lever.co/v0/postings/{slug}?mode=json
# -----------------------------
async def fetch_lever_company(session: aiohttp.ClientSession, slug: str) -> List[Dict[str, Any]]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    data = await fetch_json(session, url)
    out: List[Dict[str, Any]] = []
    if not data:
        return out

    for j in data:
        title = (j.get("text") or "").strip()
        jd_url = (j.get("hostedUrl") or j.get("applyUrl") or "").strip()
        if not title or not jd_url:
            continue

        # Title filters
        if EXCLUDE_RE.search(title):
            continue
        if not INCLUDE_RE.search(title):
            continue

        # Location (Lever stores under categories.location as a string)
        loc = ""
        cats = j.get("categories") or {}
        if isinstance(cats, dict):
            loc = (cats.get("location") or "").strip()

        # Description (plain text if available)
        desc = (j.get("descriptionPlain") or j.get("description") or "")
        if isinstance(desc, str):
            desc = desc[:2000]  # cap

        out.append({
            "source": "lever",
            "company": slug,
            "title": title,
            "url": jd_url,
            "location": loc,
            "desc": desc
        })
    return out

# -----------------------------
# Aggregate, filter, score, dedupe
# -----------------------------
async def gather_all() -> List[Dict[str, Any]]:
    connector = aiohttp.TCPConnector(limit=60)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        tasks = []
        for g in GREENHOUSE_SLUGS:
            tasks.append(fetch_greenhouse_company(session, g))
        for l in LEVER_SLUGS:
            tasks.append(fetch_lever_company(session, l))

        results = await asyncio.gather(*tasks, return_exceptions=True)

    jobs: List[Dict[str, Any]] = []
    for r in results:
        if isinstance(r, list):
            jobs.extend(r)

    # Score + dedupe
    seen = set()
    cleaned: List[Dict[str, Any]] = []
    for j in jobs:
        title = j.get("title", "")
        url = j.get("url", "")
        loc = j.get("location", "")
        desc = j.get("desc", "")

        # (Double) guard against excluded titles
        if EXCLUDE_RE.search(title):
            continue
        if not INCLUDE_RE.search(title):
            continue

        j["fitscore"] = fit_score(title, desc, loc)
        j["id"] = make_id(url, title)

        if j["id"] in seen:
            continue
        seen.add(j["id"])
        cleaned.append(j)

    # Sort by FitScore desc, then company/title
    cleaned.sort(key=lambda x: (-x["fitscore"], x.get("company",""), x.get("title","")))
    return cleaned

# -----------------------------
# Post to Google Sheets (Apps Script Web App)
# -----------------------------
async def post_to_sheet(rows: List[Dict[str, Any]]) -> None:
    endpoint = os.environ.get("SHEET_WEBAPP_URL", "").strip()
    if not endpoint:
        print("ERROR: SHEET_WEBAPP_URL is not set. Aborting.")
        # Non-zero exit so Actions shows failure; easier to debug.
        raise SystemExit(2)

    payload = {
        "rows": rows,
        "ts": int(time.time())
    }

    async with aiohttp.ClientSession(headers={"Content-Type": "application/json", **HEADERS}) as session:
        try:
            async with session.post(endpoint, data=json.dumps(payload), timeout=TIMEOUT) as r:
                txt = await r.text()
                print("POST → Sheets:", r.status, txt[:500])
        except Exception as e:
            print("POST failed:", repr(e))
            raise

# -----------------------------
# Main
# -----------------------------
async def main():
    jobs = await gather_all()

    # Keep strong matches only (fitscore >= 3) and cap for a single run to avoid sheet spam
    top = [{
        "id": j["id"],
        "source": j["source"],
        "company": j["company"],
        "title": j["title"],
        "url": j["url"],
        "location": j.get("location", ""),
        "fitscore": j.get("fitscore", 0)
    } for j in jobs if j.get("fitscore", 0) >= 3][:1000]

    print(f"Prepared {len(top)} rows to push")
    if top:
        await post_to_sheet(top)
    else:
        print("No rows matched filters this run.")

if __name__ == "__main__":
    asyncio.run(main())

Notes / Tips

Scaling up: add more company slugs to slugs_greenhouse.txt and slugs_lever.txt (one per line). Greenhouse & Lever JSON are public and safe to query in Actions.

Filters: If you want to tweak what’s “Analyst+,” edit INCLUDE_TITLE_PATTERNS and EXCLUDE_TITLE_PATTERNS only.

Location bias: LOC_OK_RE is a soft bonus; it doesn’t exclude.

Posting: The script sends { "rows":[...], "ts": <unix> } to your Apps Script. Make sure your Web App’s doPost(e) reads e.postData.contents and appends to the Targets tab.

Dependencies (GitHub Actions): pip install aiohttp.
