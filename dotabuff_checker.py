"""
Dotabuff block checker
Makes requests to Dotabuff and shows the status + slice of the response body.
Use real match IDs from your matches.
"""

import time
import random
import re

try:
    import cloudscraper
    _CLOUDSCRAPER_OK = True
except ImportError:
    import requests
    _CLOUDSCRAPER_OK = False
    print("⚠  cloudscraper not found, using requests")

# ── Settings ─────────────────────────────────────────────────────────────────
DELAY_LO   = 10.0   # minimum delay between requests (sec)
DELAY_HI   = 20.0   # maximum delay between requests (sec)
BODY_CHARS = 300     # how many body characters to print

# ── Real match IDs (replace with your own) ────────────────────────────────────────
TEST_MATCH_IDS = [
    "8204144490",
    "8204144491",
    "8204144492",
]

# ── UA pool ───────────────────────────────────────────────────────────────────
_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
 ]

_BROWSER_PROFILES = [
    {"browser": "chrome",  "platform": "windows", "desktop": True},
    {"browser": "chrome",  "platform": "linux",   "desktop": True},
    {"browser": "firefox", "platform": "windows", "desktop": True},
    {"browser": "chrome",  "platform": "darwin",  "desktop": True},
]

def _new_scraper():
    if _CLOUDSCRAPER_OK:
        return cloudscraper.create_scraper(browser=random.choice(_BROWSER_PROFILES))
    s = requests.Session()
    s.headers.update({"User-Agent": random.choice(_UA_POOL)})
    return s

def _headers():
    return {
        "User-Agent":      random.choice(_UA_POOL),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer":         "https://www.dotabuff.com/",
        "DNT":             "1",
    }

def _classify(status: int, body: str) -> tuple[str, bool]:
    """
    Returns (description, is_blocked).
    is_blocked=True only with a real CF/rate-limit block.
    """
    body_low = body.lower()
    cf_signs = "just a moment" in body_low or "cf-ray" in body_low or "checking your browser" in body_low

    if status == 200:
        if cf_signs:
            return "🚫 CF challenge (200 but it is a check)", True
        if "match overview" in body_low or "radiant victory" in body_low or "dire victory" in body_low:
            return "✅ OK (real match)", False
        if "dotabuff" in body_low:
            return "✅ OK (dotabuff responded)", False
        return f"❓ 200 unclear response (len={len(body)})", False

    if status == 404:
        # Real Dotabuff 404 contains their own "Not Found" page
        if "dotabuff" in body_low and "not found" in body_low:
            return "ℹ️  404 real (page has not appeared yet — normal)", False
        if cf_signs or len(body) < 500:
            return "🚫 404 CF block (suspiciously small body or CF signs)", True
        return "ℹ️  404 real", False

    if status == 403:
        return "🚫 403 Forbidden (CF block)", True

    if status == 503:
        return "🚫 503 Service Unavailable (CF)", True

    if status == 429:
        return "🚫 429 Rate Limited", True

    return f"❓ HTTP {status}", False

def _body_preview(body: str) -> str:
    """Returns the first BODY_CHARS characters of cleaned text."""
    # Remove HTML tags
    clean = re.sub(r'<[^>]+>', ' ', body)
    clean = re.sub(r'\s+', ' ', clean).strip()
    if len(clean) > BODY_CHARS:
        return clean[:BODY_CHARS] + "…"
    return clean

# ── Main loop ─────────────────────────────────────────────────────────────
def run():
    print("=" * 65)
    print("  Dotabuff Block Checker")
    print(f"  Delay: {DELAY_LO}–{DELAY_HI}s between requests")
    print(f"  cloudscraper: {'✅' if _CLOUDSCRAPER_OK else '⚠️  no, requests'}")
    print("=" * 65)
    print()

    scraper = _new_scraper()
    request_count  = 0
    recreate_every = random.randint(5, 8)
    blocked_streak = 0
    urls = []

    # Generate list of URLs: main, /kills, /objectives for each ID
    for mid in TEST_MATCH_IDS:
        urls.append((mid, f"https://www.dotabuff.com/matches/{mid}",            "main"))
        urls.append((mid, f"https://www.dotabuff.com/matches/{mid}/kills",      "kills"))
        urls.append((mid, f"https://www.dotabuff.com/matches/{mid}/objectives", "objectives"))

    idx = 0
    while True:
        url_entry = urls[idx % len(urls)]
        mid, url, page_type = url_entry
        idx += 1

        delay = random.uniform(DELAY_LO, DELAY_HI)

        # Recreate scraper on schedule
        if request_count >= recreate_every:
            scraper = _new_scraper()
            request_count  = 0
            recreate_every = random.randint(5, 8)
            print(f"  🔄 Scraper recreated (every {recreate_every} requests)\n")

        print(f"[#{request_count + 1}] {page_type.upper():12s} {url}")
        print(f"      delay until next: {delay:.1f}s")

        try:
            r = scraper.get(url, headers=_headers(), timeout=20)
            body    = r.text
            status  = r.status_code
            result, is_blocked = _classify(status, body)
            preview = _body_preview(body)

            print(f"      status:  {status}  ({len(body)} bytes)")
            print(f"      result:    {result}")
            print(f"      body:    {preview}")

            if is_blocked:
                blocked_streak += 1
                print(f"      ⚠️  Consecutive blocks: {blocked_streak}")
                if blocked_streak >= 3:
                    print(f"\n  ❌ BLOCKED {blocked_streak} times in a row — stop.")
                    break
            else:
                blocked_streak = 0

        except Exception as e:
            print(f"      ❌ Error: {e}")
            blocked_streak += 1

        print()
        request_count += 1
        time.sleep(delay)

if __name__ == "__main__":
    run()