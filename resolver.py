import os, csv, io, json, time, asyncio, aiohttp, re
from urllib.parse import urlparse

HEADERS = {"User-Agent": "Mozilla/5.0 (QAResolver/1.0)"}
TIMEOUT = aiohttp.ClientTimeout(total=30)
KNOWN_HOSTS = ("boards.greenhouse.io","greenhouse.io","jobs.lever.co","lever.co",
               "linkedin.com","twitter.com","facebook.com","fb.com",
               "instagram.com","youtube.com","tiktok.com")

def norm_host(url):
    try:
        h = urlparse(url).hostname or ""
        if h.startswith("www."): h = h[4:]
        return h
    except Exception:
        return ""

def is_external_company_link(link):
    h = norm_host(link)
    return h and h not in KNOWN_HOSTS

LINK_RE = re.compile(r'https?://[^\s"\'<>]+', re.I)
LI_RE   = re.compile(r'https?://(?:www\.)?linkedin\.com/company/[A-Za-z0-9\-_/]+', re.I)

async def fetch_text(session, url):
    try:
        async with session.get(url, timeout=TIMEOUT) as r:
            if r.status == 200:
                return await r.text()
    except Exception:
        return ""
    return ""

async def read_targets_csv(session, csv_url, max_rows=50):
    txt = await fetch_text(session, csv_url)
    if not txt: return []
    rows = []
    reader = csv.reader(io.StringIO(txt))
    next(reader, None)
    for i, row in enumerate(reader):
        if i >= max_rows: break
        try:
            rid, source, company, title, url, location, fitscore = row[:7]
            rows.append({"id": rid, "source": source, "company": company, "title": title,
                         "url": url, "location": location, "fitscore": fitscore})
        except:
            continue
    return rows

async def resolve_from_jobpage(session, job_url):
    html = await fetch_text(session, job_url)
    if not html:
        return {"ResolvedCompanyURL": "", "LinkedInURL": "", "Issues": "no_html"}
    links = set(LINK_RE.findall(html))
    li = ""
    for lk in links:
        if LI_RE.match(lk):
            li = lk
            break
    homepage = ""
    for lk in links:
        if is_external_company_link(lk):
            homepage = lk
            break
    issues = ""
    if not homepage: issues = (";no_homepage" if not issues else issues + ";no_homepage")
    if not li: issues = (";no_linkedin" if not issues else issues + ";no_linkedin")
    return {"ResolvedCompanyURL": homepage, "LinkedInURL": li, "Issues": issues}

async def post_qa(sheet_webapp_url, rows):
    payload = {"mode": "qa", "rows": rows, "ts": int(time.time())}
    async with aiohttp.ClientSession(headers={"Content-Type": "application/json"}) as session:
        async with session.post(sheet_webapp_url, data=json.dumps(payload), timeout=TIMEOUT) as r:
            txt = await r.text()
            print("POST QA → Sheets:", r.status, txt[:300])
            return r.status == 200

async def main():
    csv_url = os.environ.get("TARGETS_CSV_URL", "").strip()
    sheet_ep = os.environ.get("SHEET_WEBAPP_URL", "").strip()
    if not csv_url or not sheet_ep:
        print("Missing TARGETS_CSV_URL or SHEET_WEBAPP_URL")
        raise SystemExit(2)
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        targets = await read_targets_csv(session, csv_url, max_rows=50)
        if not targets:
            print("No targets read from CSV.")
            return
        results = []
        for t in targets:
            rid = t.get("id", "")
            job_url = t.get("url", "")
            if not rid or not job_url:
                continue
            info = await resolve_from_jobpage(session, job_url)
            job_host = norm_host(job_url)
            home_host = norm_host(info["ResolvedCompanyURL"])
            domain_match = bool(home_host and home_host not in KNOWN_HOSTS and home_host != job_host)
            results.append({
                "ID": rid,
                "ResolvedCompanyURL": info["ResolvedCompanyURL"],
                "LinkedInURL": info["LinkedInURL"],
                "DomainMatch": "TRUE" if domain_match else "FALSE",
                "Issues": info["Issues"],
                "CheckedAt": time.strftime("%Y-%m-%d %H:%M:%S")
            })
        if results:
            ok = await post_qa(sheet_ep, results)
            if not ok:
                raise SystemExit(3)
        print(f"QA appended {len(results)} rows.")

if __name__ == "__main__":
    asyncio.run(main())