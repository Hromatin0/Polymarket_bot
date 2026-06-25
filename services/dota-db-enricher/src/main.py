"""
Dota 2 Dotabuff Enricher Microservice
═══════════════════════════════════════════════════════════════════
  Dotabuff  → Duration · Счёт · Мульти-киллы · Рошаны · Бараки
  Saves stats to Redis under dota:dotabuff and dota:queue_stats.
═══════════════════════════════════════════════════════════════════
"""

import time
import json
import re
import asyncio
import random
import os
import threading
from datetime import datetime
from bs4 import BeautifulSoup
import redis
from dotenv import load_dotenv

load_dotenv()

try:
    import cloudscraper
    _CLOUDSCRAPER_OK = True
except ImportError:
    import requests
    _CLOUDSCRAPER_OK = False
    print("⚠  cloudscraper не найден — fallback на requests.")

# ── Config ─────────────────────────────────────────────────────────────────────
REDIS_HOST           = os.getenv("REDIS_HOST", "redis")
REDIS_PORT           = int(os.getenv("REDIS_PORT", "6379"))
DOTABUFF_KILLS_DELAY = 600    # 10 min after basic before fetching /kills + /objectives
DOTABUFF_FLEX_LO     = 10.0   # flex delay base: min seconds per active card
DOTABUFF_FLEX_HI     = 20.0   # flex delay base: max seconds per active card

r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# ── User-Agent pool ────────────────────────────────────────────────────────────
_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

_ACCEPT_LANG_POOL = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.8",
    "en-US,en;q=0.9,ru;q=0.8",
]

def _random_headers() -> dict:
    return {
        "User-Agent":      random.choice(_UA_POOL),
        "Accept-Language": random.choice(_ACCEPT_LANG_POOL),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         "https://www.dotabuff.com/",
        "DNT":             "1",
        "Connection":      "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

_BROWSER_PROFILES = [
    {"browser": "chrome",  "platform": "windows", "desktop": True},
    {"browser": "chrome",  "platform": "linux",   "desktop": True},
    {"browser": "firefox", "platform": "windows", "desktop": True},
    {"browser": "firefox", "platform": "linux",   "desktop": True},
    {"browser": "chrome",  "platform": "darwin",  "desktop": True},
]

def _new_scraper():
    if _CLOUDSCRAPER_OK:
        profile = random.choice(_BROWSER_PROFILES)
        return cloudscraper.create_scraper(browser=profile)
    else:
        import requests as req
        s = req.Session()
        s.headers.update(_random_headers())
        return s

MULTIKILL_LABELS = {5: "Rampage", 4: "Ultra Kill", 3: "Triple Kill", 2: "Double Kill"}

def _now():
    return datetime.now().strftime("%H:%M:%S")

def _log(msg: str):
    print(f"[{_now()}] {msg}")
    try:
        r_client.lpush("dota:errors", f"{_now()} {msg}")
        r_client.ltrim("dota:errors", 0, 14)
    except Exception as e:
        print(f"Redis Log Error: {e}")

class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text        = text
        self.status_code = status_code


# ── Dotabuff Tracker ──────────────────────────────────────────────────────────
class DotabuffTracker:
    def __init__(self):
        self._data : dict[str, dict] = {}
        self._lock = threading.Lock()
        self._need_new_scraper = False
        
        # Playwright fallback runner
        self._browser = None
        self._loop    = None
        self._ready   = threading.Event()
        self._pw_ok   = False
        
        threading.Thread(target=self._pw_thread_entry, daemon=True).start()
        self._ready.wait(timeout=30)
        
        threading.Thread(target=self._worker, daemon=True).start()

    _STEALTH_JS = """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins',   {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        window.chrome = {runtime: {}, loadTimes: function(){}, csi: function(){}, app: {}};
        const orig = window.HTMLIFrameElement.prototype.contentWindow;
        Object.defineProperty(window.HTMLIFrameElement.prototype, 'contentWindow', {
            get: function() {
                const win = orig.apply(this);
                try { win.navigator; } catch(e) {}
                return win;
            }
        });
    """

    def _pw_thread_entry(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._pw_main())

    async def _pw_main(self):
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as pw:
                self._browser = await pw.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--disable-extensions",
                        "--disable-blink-features=AutomationControlled",
                        "--disable-infobars",
                        "--lang=en-US",
                    ],
                )
                self._pw_ok = True
                self._ready.set()
                while True:
                    await asyncio.sleep(5)
        except Exception as e:
            _log(f"Playwright init error: {e}")
            self._ready.set()

    def fetch_dotabuff_html(self, url: str) -> "str | None":
        if not self._pw_ok or self._loop is None:
            return None
        future = asyncio.run_coroutine_threadsafe(
            self._fetch_dotabuff_async(url), self._loop
        )
        try:
            return future.result(timeout=65)
        except Exception as e:
            _log(f"fetch_dotabuff_html {url}: {e}")
            return None

    async def _fetch_dotabuff_async(self, url: str) -> "str | None":
        _VIEWPORTS = [
            {"width": 1920, "height": 1080},
            {"width": 1440, "height": 900},
            {"width": 1280, "height": 800},
            {"width": 1366, "height": 768},
        ]
        _TIMEZONES = ["America/New_York", "America/Chicago", "Europe/London",
                      "Europe/Berlin", "America/Los_Angeles"]
        ctx = None
        for attempt in range(2):
            try:
                ctx = await self._browser.new_context(
                    user_agent=random.choice(_UA_POOL),
                    viewport=random.choice(_VIEWPORTS),
                    locale="en-US",
                    timezone_id=random.choice(_TIMEZONES),
                    extra_http_headers={
                        "Accept-Language": random.choice(_ACCEPT_LANG_POOL),
                        "Referer": "https://www.dotabuff.com/",
                        "DNT": "1",
                    },
                )
                await ctx.add_init_script(self._STEALTH_JS)
                page = await ctx.new_page()

                await asyncio.sleep(random.uniform(1.5, 3.5))
                await page.goto(url, wait_until="domcontentloaded", timeout=22000)
                await asyncio.sleep(random.uniform(2.5, 4.5))

                await page.evaluate(
                    "window.scrollTo({top: Math.random()*400+100, behavior:'smooth'})"
                )
                await asyncio.sleep(random.uniform(0.8, 1.8))

                try:
                    await page.wait_for_selector(
                        "article.match-header, div.header-content-title, "
                        "section.match-overview, div.primary-attribute",
                        timeout=7000,
                    )
                except Exception:
                    pass

                html = await page.content()
                await ctx.close()
                ctx = None

                if html and len(html) > 3000 and "dotabuff" in html.lower():
                    return html

                _log(f"DB PW attempt {attempt+1}: подозрительный ответ (len={len(html) if html else 0}) для {url}")
                if attempt == 0:
                    await asyncio.sleep(random.uniform(8, 15))

            except Exception as e:
                if ctx:
                    try: await ctx.close()
                    except Exception: pass
                    ctx = None
                _log(f"_fetch_dotabuff_async attempt {attempt+1} {url}: {e}")
                if attempt == 0:
                    await asyncio.sleep(random.uniform(5, 10))

        return None

    def register(self, match_id: str, initial_phase: str = "waiting", priority: int = 0, **meta):
        mid = str(match_id).strip()
        if not mid or not mid.isdigit() or len(mid) < 8:
            return
        with self._lock:
            if mid in self._data:
                if priority < self._data[mid].get("priority", 0):
                    self._data[mid]["priority"] = priority
                return
            last_attempt = (
                time.time() - DOTABUFF_FLEX_HI + random.uniform(5, 15)
                if initial_phase == "pending" else 0.0
            )
            self._data[mid] = {
                "match_id":      mid,
                "phase":         initial_phase,
                "priority":      priority,
                "last_attempt":  last_attempt,
                "added_at":      _now(),
                "team_radiant":  meta.get("team_radiant", ""),
                "team_dire":     meta.get("team_dire", ""),
                "map_num":       meta.get("map_num", 0),
                "match_label":   meta.get("match_label", ""),
                "winner":        None,
                "duration":      None,
                "score_radiant": None,
                "score_dire":    None,
                "basic_at":      None,
                "basic_ts":      None,
                "multikill_radiant":  None,
                "multikill_dire":     None,
                "roshans_radiant":    0,
                "roshans_dire":       0,
                "barracks_radiant":   0,
                "barracks_dire":      0,
                "full_at":            None,
            }
        phase_label = "▶ сразу pending" if initial_phase == "pending" else "ждём победителя DLTV"
        prio_label  = "🔴 live" if priority == 0 else "📜 history"
        print(f"[{_now()}] 📌 Dotabuff: зарегистрирован матч {mid} ({meta.get('team_radiant','')} vs {meta.get('team_dire','')} карта {meta.get('map_num',0)}) [{phase_label}] [{prio_label}]")

    def activate(self, match_id: str):
        mid = str(match_id).strip()
        with self._lock:
            entry = self._data.get(mid)
            if entry and entry["phase"] == "waiting":
                self._data[mid]["phase"] = "pending"
                self._data[mid]["last_attempt"] = time.time() - DOTABUFF_FLEX_HI + random.uniform(5, 15)
                print(f"[{_now()}] 🔓 Dotabuff: {mid} → pending")

    def get_all(self) -> dict:
        with self._lock:
            return {k: dict(v) for k, v in self._data.items()}

    def get_queue_stats(self) -> dict:
        with self._lock:
            counts = {"waiting": 0, "pending": 0, "basic": 0, "full": 0}
            for v in self._data.values():
                counts[v["phase"]] = counts.get(v["phase"], 0) + 1
            return counts

    def _active_request_count(self, now: float) -> int:
        count = 0
        for entry in self._data.values():
            phase = entry["phase"]
            if phase == "pending":
                count += 1
            elif phase == "basic":
                basic_ts = entry.get("basic_ts") or 0
                if now - basic_ts >= DOTABUFF_KILLS_DELAY:
                    count += 1
        return max(count, 1)

    def _worker(self):
        scraper = _new_scraper()
        request_count = 0

        while True:
            now = time.time()
            with self._lock:
                items = [(k, dict(v)) for k, v in self._data.items()]

            items.sort(key=lambda x: x[1].get("priority", 0))

            live_needs_work = any(
                v.get("priority", 0) == 0 and v["phase"] in ("pending", "basic")
                for _, v in items
            )

            active_count = self._active_request_count(now)
            flex_retry   = random.uniform(DOTABUFF_FLEX_LO, DOTABUFF_FLEX_HI) * active_count

            for mid, entry in items:
                phase    = entry["phase"]
                priority = entry.get("priority", 0)
                last_att = entry["last_attempt"]

                if phase in ("full", "waiting"):
                    continue
                if priority == 1 and live_needs_work:
                    continue

                if phase == "pending":
                    if self._need_new_scraper:
                        scraper = _new_scraper()
                        request_count = 0
                        self._need_new_scraper = False
                        print(f"[{_now()}] 🔄 DB: скрапер пересоздан после 403")
                    if now - last_att < flex_retry:
                        continue
                    with self._lock:
                        self._data[mid]["last_attempt"] = now
                    result = self._scrape_main(scraper, mid)
                    request_count += 1
                    if result:
                        with self._lock:
                            self._data[mid].update(result)
                            self._data[mid]["phase"]    = "basic"
                            self._data[mid]["basic_at"] = _now()
                            self._data[mid]["basic_ts"] = time.time()
                        print(f"[{_now()}] ✅ Dotabuff basic: {mid} {result.get('winner','')} {result.get('duration','')} ⏳ /kills+/obj через {DOTABUFF_KILLS_DELAY//60} мин")
                        # Сохраняем в Redis
                        self._save_to_redis()

                elif phase == "basic":
                    if self._need_new_scraper:
                        scraper = _new_scraper()
                        request_count = 0
                        self._need_new_scraper = False
                        print(f"[{_now()}] 🔄 DB: скрапер пересоздан после 403")
                    basic_ts  = entry.get("basic_ts") or 0
                    elapsed   = now - basic_ts
                    remaining = DOTABUFF_KILLS_DELAY - elapsed
                    if remaining > 0:
                        continue
                    if now - last_att < flex_retry:
                        continue
                    with self._lock:
                        self._data[mid]["last_attempt"] = now
                    extra = self._scrape_extra(scraper, mid)
                    request_count += 1
                    if extra is not None:
                        with self._lock:
                            self._data[mid].update(extra)
                            self._data[mid]["phase"]   = "full"
                            self._data[mid]["full_at"] = _now()
                        print(f"[{_now()}] 🏆 Dotabuff full: {mid} MK_R={extra.get('multikill_radiant')} MK_D={extra.get('multikill_dire')} Roshan R:{extra.get('roshans_radiant')} D:{extra.get('roshans_dire')}")
                        # Сохраняем в Redis
                        self._save_to_redis()

                if request_count >= random.randint(12, 18):
                    scraper = _new_scraper()
                    request_count = 0

                time.sleep(random.uniform(3.0, 6.0))

            time.sleep(random.uniform(0.5, 1.5))

    def _save_to_redis(self):
        try:
            db_all = self.get_all()
            q_stats = self.get_queue_stats()
            r_client.set("dota:dotabuff", json.dumps(db_all, ensure_ascii=False))
            r_client.set("dota:queue_stats", json.dumps(q_stats, ensure_ascii=False))
            r_client.publish("dota:dotabuff_updated", "1")
        except Exception as e:
            print(f"Error saving to Redis: {e}")

    def _get(self, scraper, url: str):
        hdrs = _random_headers()
        try:
            print(f"[{_now()}] 🌐 DB GET {url}")
            r = scraper.get(url, headers=hdrs, timeout=20)
            print(f"[{_now()}] 📥 DB {r.status_code} ← {url} (len={len(r.text)})")
            if r.status_code == 429:
                wait = random.uniform(60, 120)
                _log(f"DB: rate limit 429 на {url} — ждём {wait:.0f}с…")
                time.sleep(wait)
                return None
            if r.status_code in (403, 503):
                self._need_new_scraper = True
                wait_pre = random.uniform(4, 9)
                _log(f"DB: {r.status_code} (CF?) на {url} — ждём {wait_pre:.0f}с, затем Playwright…")
                time.sleep(wait_pre)
                html = self.fetch_dotabuff_html(url)
                if html:
                    _log(f"DB: Playwright fallback успешен для {url}")
                    return _FakeResponse(html, 200)
                return None
            if r.status_code == 404:
                body_low = r.text.lower()
                cf_signs = ("just a moment" in body_low or "cf-ray" in body_low or "checking your browser" in body_low)
                real_404 = "dotabuff" in body_low and "not found" in body_low
                if real_404 and not cf_signs:
                    _log(f"DB: 404 реальный ({len(r.text)}б) на {url} — страница ещё не готова")
                    return None
                _log(f"DB: 404 CF-блок ({len(r.text)}б) на {url} — пересоздаём скрапер")
                self._need_new_scraper = True
                return None
            return r
        except Exception as e:
            _log(f"DB _get {url}: {e}")
            return None

    def _scrape_main(self, scraper, mid: str) -> dict | None:
        url = f"https://www.dotabuff.com/matches/{mid}"
        r = self._get(scraper, url)
        if r is None:
            print(f"[{_now()}] ⚠️  DB main {mid}: _get вернул None")
            return None
        if r.status_code == 404:
            print(f"[{_now()}] ⚠️  DB main {mid}: 404 Not Found")
            return None
        if r.status_code != 200:
            print(f"[{_now()}] ⚠️  DB main {mid}: статус {r.status_code}")
            return None

        try:
            soup = BeautifulSoup(r.text, "lxml")

            title_tag = soup.find("title")
            title_text = title_tag.get_text(strip=True) if title_tag else ""
            print(f"[{_now()}] 🔍 DB main {mid}: title='{title_text[:80]}'")

            if "not found" in title_text.lower():
                print(f"[{_now()}] ⚠️  DB main {mid}: 'not found' в title — пропускаем")
                return None
            og_title = soup.find("meta", property="og:title")
            if og_title:
                ot = og_title.get("content", "").lower()
                if "not found" in ot or ("match" not in ot and "overview" not in ot):
                    print(f"[{_now()}] ⚠️  DB main {mid}: og:title не похоже на матч ('{ot[:60]}')")
                    return None

            text = soup.get_text(" ", strip=True)
            result: dict = {}

            # Победитель
            win_m = re.search(r'(Radiant|Dire)\s+Victory', text, re.I)
            if win_m:
                result["winner"] = win_m.group(1).capitalize()

            # Длительность
            dur_span = soup.select_one(".match-victory-subtitle span.duration")
            if dur_span:
                t = dur_span.get_text(strip=True)
                if re.match(r'\d+:\d{2}$', t):
                    result["duration"] = t
                    print(f"[{_now()}] DB duration {mid}: match-victory-subtitle → {t}")
            if "duration" not in result:
                for dt_tag in soup.find_all("dt"):
                    if "duration" in dt_tag.get_text(strip=True).lower():
                        dd = dt_tag.find_next_sibling("dd")
                        if dd:
                            t = dd.get_text(strip=True)
                            if re.match(r'\d+:\d{2}$', t):
                                result["duration"] = t
                                break
            if "duration" not in result:
                dur_m = re.search(r'Duration\s+(\d{1,3}:\d{2})', text, re.I)
                if dur_m:
                    result["duration"] = dur_m.group(1)
            if "duration" not in result:
                title_tag = soup.find("title")
                if title_tag:
                    dur_m = re.search(r'(\d{2,3}:\d{2})', title_tag.get_text())
                    if dur_m:
                        result["duration"] = dur_m.group(1)

            # Счёт убийств
            rad_el  = soup.select_one("span.the-radiant.score, span.score.the-radiant")
            dire_el = soup.select_one("span.the-dire.score, span.score.the-dire")
            if rad_el and dire_el:
                try:
                    result["score_radiant"] = int(rad_el.get_text(strip=True))
                    result["score_dire"]    = int(dire_el.get_text(strip=True))
                except (ValueError, TypeError):
                    pass
            if "score_radiant" not in result:
                for header in soup.find_all(["header", "section"], class_=re.compile(r'header|overview|summary', re.I)):
                    htext = header.get_text(" ")
                    sc_m = re.search(r'\b(\d{1,3})\s*[-–]\s*(\d{1,3})\b', htext)
                    if sc_m:
                        result["score_radiant"] = int(sc_m.group(1))
                        result["score_dire"]    = int(sc_m.group(2))
                        break
            if "score_radiant" not in result:
                sc_m = re.search(r'\b(\d{1,3})\s*[-–]\s*(\d{1,3})\b', text)
                if sc_m:
                    a, b = int(sc_m.group(1)), int(sc_m.group(2))
                    if a < 100 and b < 100:
                        result["score_radiant"] = a
                        result["score_dire"]    = b

            print(f"[{_now()}] 🔍 DB main {mid}: найдено → {result}")
            return result if result else None
        except Exception as e:
            _log(f"DB main parse {mid}: {e}")
            return None

    def _scrape_kills(self, scraper, mid: str) -> dict | None:
        url = f"https://www.dotabuff.com/matches/{mid}/kills"
        time.sleep(random.uniform(2.0, 5.0))
        r = self._get(scraper, url)
        if r is None or r.status_code != 200:
            print(f"[{_now()}] ⚠️  DB kills {mid}: статус {getattr(r,'status_code','None')}")
            return None

        try:
            soup = BeautifulSoup(r.text, "lxml")
            result = {}

            def _parse_multi(table, team: str):
                multi_idx = None
                sub_row = table.select_one("thead tr.sub")
                if sub_row:
                    for i, th in enumerate(sub_row.find_all("th")):
                        acr = th.find("acronym")
                        title = (
                            (acr.get("oldtitle") or acr.get("title") or acr.get_text(strip=True))
                            if acr else th.get_text(strip=True)
                        )
                        if "multi" in title.lower():
                            multi_idx = i
                            break

                tfoot = table.find("tfoot")
                if not tfoot:
                    return
                tfoot_row = tfoot.find("tr")
                if not tfoot_row:
                    return

                all_tds = tfoot_row.find_all("td")
                cells   = [td for td in all_tds if "col-exclude" not in " ".join(td.get("class", []))]

                if multi_idx is not None:
                    excl    = sum(1 for td in all_tds[:multi_idx] if "col-exclude" in " ".join(td.get("class", [])))
                    eff_idx = multi_idx - excl
                else:
                    eff_idx = 2

                if len(cells) > eff_idx:
                    raw = cells[eff_idx].get_text(strip=True).strip("-").strip()
                    if raw.isdigit() and int(raw) >= 2:
                        val   = int(raw)
                        label = MULTIKILL_LABELS.get(val, f"{val}x Kill")
                        result[f"multikill_{team}"] = label
                        print(f"[{_now()}] ✅ DB kills {mid}: {team} multikill={label}")

            sections_found = 0
            for section in soup.find_all("section", class_=re.compile(r"radiant|dire", re.I)):
                classes = " ".join(section.get("class", []))
                team    = "radiant" if "radiant" in classes else "dire"
                table   = section.find("table")
                if table:
                    sections_found += 1
                    _parse_multi(table, team)

            if sections_found == 0:
                for table in soup.find_all("table"):
                    team = None
                    for row in table.select("tbody tr"):
                        cls = " ".join(row.get("class", []))
                        if "faction-radiant" in cls:
                            team = "radiant"; break
                        elif "faction-dire" in cls:
                            team = "dire"; break
                    if team:
                        _parse_multi(table, team)

            return result if result else None
        except Exception as e:
            _log(f"DB kills {mid}: {e}")
            return None

    def _scrape_objectives(self, scraper, mid: str) -> dict | None:
        url = f"https://www.dotabuff.com/matches/{mid}/objectives"
        time.sleep(random.uniform(2.0, 5.0))
        r = self._get(scraper, url)
        if r is None or r.status_code != 200:
            print(f"[{_now()}] ⚠️  DB obj {mid}: статус {getattr(r,'status_code','None')}")
            return None

        try:
            soup = BeautifulSoup(r.text, "lxml")
            result = {
                "roshans_radiant": 0, "roshans_dire": 0,
                "barracks_radiant": 0, "barracks_dire": 0,
            }

            text = soup.get_text(" ", strip=True)
            roshans = re.findall(r'(?:The\s+)?(Radiant|Dire)\s+killed\s+Roshan', text, re.I)
            result["roshans_radiant"] = sum(1 for x in roshans if x.lower() == "radiant")
            result["roshans_dire"]    = sum(1 for x in roshans if x.lower() == "dire")

            sections_found = 0
            for section in soup.find_all("section", class_=re.compile(r"radiant|dire", re.I)):
                classes = " ".join(section.get("class", []))
                team    = "radiant" if "radiant" in classes else "dire"
                table   = section.find("table")
                if not table:
                    continue

                sections_found += 1
                barracks_idx = None
                sub_row = table.select_one("thead tr.sub")
                if sub_row:
                    for i, th in enumerate(sub_row.find_all("th")):
                        acr = th.find("acronym")
                        title = (
                            (acr.get("oldtitle") or acr.get("title") or acr.get_text(strip=True))
                            if acr else th.get_text(strip=True)
                        )
                        if "barracks" in title.lower():
                            barracks_idx = i
                            break

                if barracks_idx is None:
                    continue

                tfoot = table.find("tfoot")
                if not tfoot:
                    continue
                tfoot_row = tfoot.find("tr")
                if not tfoot_row:
                    continue

                all_tds = tfoot_row.find_all("td")
                cells   = [td for td in all_tds if "col-exclude" not in " ".join(td.get("class", []))]
                excl    = sum(1 for td in all_tds[:barracks_idx] if "col-exclude" in " ".join(td.get("class", [])))
                eff_idx = barracks_idx - excl

                if len(cells) > eff_idx:
                    raw = cells[eff_idx].get_text(strip=True)
                    m = re.search(r'(\d+)\s*/\s*(?:\d+|-)', raw)
                    if m:
                        result[f"barracks_{team}"] = int(m.group(1))
                    else:
                        num_m = re.search(r'(\d+)', raw)
                        if num_m:
                            result[f"barracks_{team}"] = int(num_m.group(1))

            if sections_found == 0:
                for table in soup.find_all("table"):
                    team = None
                    for row in table.select("tbody tr"):
                        cls = " ".join(row.get("class", []))
                        if "faction-radiant" in cls:
                            team = "radiant"; break
                        elif "faction-dire" in cls:
                            team = "dire"; break
                    if not team:
                        continue
                    tfoot = table.find("tfoot")
                    if not tfoot:
                        continue
                    tfoot_row = tfoot.find("tr")
                    if not tfoot_row:
                        continue
                    cells = [td for td in tfoot_row.find_all("td") if "col-exclude" not in " ".join(td.get("class", []))]
                    if len(cells) > 1:
                        raw = cells[1].get_text(strip=True)
                        m = re.search(r'(\d+)', raw)
                        if m:
                            result[f"barracks_{team}"] = int(m.group(1))

            return result
        except Exception as e:
            _log(f"DB objectives {mid}: {e}")
            return None

    def _scrape_extra(self, scraper, mid: str) -> dict | None:
        kills = self._scrape_kills(scraper, mid)
        if kills is None:
            return None
        time.sleep(random.uniform(6.0, 10.0))
        objs = self._scrape_objectives(scraper, mid)
        result: dict = dict(kills)
        if objs:
            result.update(objs)
        return result


def sync_with_redis_matches(tracker: DotabuffTracker):
    """
    Периодически считывает dota:live_matches и dota:history_matches из Redis
    и регистрирует/активирует матчи в DotabuffTracker.
    """
    while True:
        try:
            live_str = r_client.get("dota:live_matches")
            hist_str = r_client.get("dota:history_matches")
            
            live_matches = json.loads(live_str) if live_str else []
            history_matches = json.loads(hist_str) if hist_str else []
            
            # Обрабатываем живые матчи
            for match in live_matches:
                t1 = match.get("team1")
                t2 = match.get("team2")
                label = f"{t1} vs {t2}"
                for map_data in match.get("maps", []):
                    mid = map_data.get("dota2_match_id")
                    if mid:
                        status = map_data.get("status")
                        winner = map_data.get("winner")
                        already_finished = bool(winner or status == "finished")
                        
                        tracker.register(
                            mid,
                            initial_phase="pending" if already_finished else "waiting",
                            priority=0, # Live
                            team_radiant=map_data.get("radiant_team") or t1,
                            team_dire=map_data.get("dire_team") or t2,
                            map_num=map_data.get("num"),
                            match_label=label
                        )
                        if already_finished:
                            tracker.activate(mid)
            
            # Обрабатываем исторические матчи
            for match in history_matches:
                t1 = match.get("team1")
                t2 = match.get("team2")
                label = f"{t1} vs {t2}"
                for map_data in match.get("maps", []):
                    mid = map_data.get("dota2_match_id")
                    if mid:
                        tracker.register(
                            mid,
                            initial_phase="pending",
                            priority=1, # History - lower priority
                            team_radiant=map_data.get("radiant_team") or t1,
                            team_dire=map_data.get("dire_team") or t2,
                            map_num=map_data.get("num"),
                            match_label=label
                        )
                        tracker.activate(mid)
                        
        except Exception as e:
            print(f"Error syncing with Redis matches: {e}")
            
        time.sleep(5)


if __name__ == "__main__":
    print("═" * 60)
    print("  📊  Dota 2 Dotabuff Enricher Microservice  ·  v6-micro")
    print("═" * 60)
    print(f"  Redis Host: {REDIS_HOST}:{REDIS_PORT}")
    print("═" * 60)
    print()

    tracker = DotabuffTracker()
    
    # Запускаем поток синхронизации матчей из Redis
    threading.Thread(target=sync_with_redis_matches, args=(tracker,), daemon=True).start()
    
    # Держим основной поток живым
    while True:
        time.sleep(1)
