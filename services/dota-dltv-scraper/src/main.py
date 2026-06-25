"""
Dota 2 DLTV Scraper Microservice
═══════════════════════════════════════════════════════════════════
  DLTV.org  → Match IDs per map  (Playwright, DOM polling)
  Saves state directly to Redis.
═══════════════════════════════════════════════════════════════════
"""

import time
import json
import re
import asyncio
import random
import os
import threading
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import redis
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
REDIS_HOST           = os.getenv("REDIS_HOST", "redis")
REDIS_PORT           = int(os.getenv("REDIS_PORT", "6379"))
POLL_INTERVAL        = 3      # main loop sleep (sec)
DLTV_SCAN_INT        = 5      # scan DLTV /matches every N loops (+ jitter)

r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# ── User-Agent pool (ротация) ──────────────────────────────────────────────────
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

def _jitter(base: float, lo: float = 0.7, hi: float = 1.4) -> float:
    return base * random.uniform(lo, hi) + random.uniform(0.5, 2.5)

# ── Team aliases ──────────────────────────────────────────────────────────────
TEAM_MAP = {
    "vp":          ["virtus.pro", "virtus pro"],
    "mouz":        ["mouz"],
    "liquid":      ["team liquid"],
    "spirit":      ["team spirit"],
    "bb4":         ["betboom", "betboom team"],
    "nemiga":      ["nemiga gaming"],
    "z10":         ["zero tenacity"],
    "lynx":        ["team lynx"],
    "l1ga":        ["l1ga team"],
    "og":          ["og"],
    "1win":        ["1win"],
    "heroic":      ["heroic"],
    "re":          ["rune eaters"],
    "ngx":         ["ngx"],
    "xctn":        ["execration"],
    "carst":       ["carstensz"],
    "glyph":       ["glyph"],
    "rnx":         ["rekonix"],
    "satan":       ["satan666"],
    "clo":         ["cloud rising"],
    "cloudd":      ["cloud dawning"],
    "ivo":         ["ivory"],
    "winter":      ["winter squadrons", "winter squadron"],
    "eb":          ["estar backs"],
    "cha":         ["chandogs"],
    "mideng":      ["mideng dreamer"],
    "yb1":         ["yakult brothers"],
    "sar1":        ["south america rejects"],
    "btcgam":      ["btc gaming"],
    "nem":         ["team nemesis"],
    "hkr":         ["hokori"],
    "zg":          ["zetta games"],
    "biz":         ["biz gaming"],
    "ave":         ["ave", "ave gaming"],
    "barrancobar": ["barrancobar"],
    "gl":          ["gamerlegion", "gamer legion"],
    "amaru":       ["amaru flame"],
    "x5":          ["x5 gaming"],
    "ysub":        ["yellow submarine"],
    "modus":       ["modus"],
    "pr":          ["power rangers"],
    "pain":        ["pain gaming"],
    "aurora":      ["aurora"],
    "falcons":     ["team falcons", "falcons"],
}

dltv_match_cache   : dict = {}
dltv_history_cache : dict = {}   # url → match_info
dltv_cache_lock  = threading.Lock()
history_lock        = threading.Lock()
_MAX_HISTORY        = 3

_dltv_page_html      : "str | None" = None
_dltv_page_fetched_at: float        = 0.0
_DLTV_HTML_MAX_AGE   : float        = 300.0
dltv_cache_lock_html  = threading.Lock()

def _now():
    return datetime.now().strftime("%H:%M:%S")

def _log(msg: str):
    print(f"[{_now()}] {msg}")
    try:
        r_client.lpush("dota:errors", f"{_now()} {msg}")
        r_client.ltrim("dota:errors", 0, 14)
    except Exception as e:
        print(f"Redis Log Error: {e}")

def _team_matches(text_lower: str, abbrev: str) -> bool:
    for v in TEAM_MAP.get(abbrev.lower(), [abbrev.lower()]):
        if v.lower() in text_lower:
            return True
    return False

# ── DLTV Watcher ─────────────────────────────────────────────────────────────
class DLTVWatcher:
    def __init__(self):
        self._cache   : dict = {}
        self._watched : set  = set()
        self._pages   : dict = {}
        self._lock    = threading.Lock()
        self._browser = None
        self._loop    = None
        self._ready   = threading.Event()
        self._pw_ok   = False

        threading.Thread(target=self._thread_entry, daemon=True).start()
        self._ready.wait(timeout=30)

    def _thread_entry(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._main())

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

    async def _main(self):
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
                        "--window-size=1920,1080",
                        "--start-maximized",
                        "--lang=en-US",
                    ],
                )
                self._pw_ok = True
                self._ready.set()
                while True:
                    await self._sync_pages()
                    await asyncio.sleep(2)
        except Exception as e:
            _log(f"Playwright main: {e}")
            self._ready.set()

    async def _sync_pages(self):
        with self._lock:
            wanted = set(self._watched)
        active = set(self._pages.keys())

        for url in wanted - active:
            try:
                page = await self._browser.new_page()
                await page.add_init_script(self._STEALTH_JS)
                await page.set_extra_http_headers({
                    "Accept-Language": random.choice(_ACCEPT_LANG_POOL),
                })
                await page.goto(url, wait_until="domcontentloaded", timeout=25000)
                self._pages[url] = page
                asyncio.create_task(self._dom_poller(page, url))
            except Exception as e:
                _log(f"DLTV open {url}: {e}")

        for url in active - wanted:
            try:
                await self._pages.pop(url).close()
            except Exception:
                pass

    async def _dom_poller(self, page, url: str):
        _log(f"DLTV: старт dom_poller {url}")
        consecutive_errs = 0
        while True:
            with self._lock:
                if url not in self._watched:
                    break
            try:
                # Читаем score/timer
                lt = None
                ls1 = None
                ls2 = None

                lt_tag = await page.query_selector("div.match__scoreboard-time, span.live-timer")
                if lt_tag:
                    lt = await lt_tag.inner_text()

                ls_tag1 = await page.query_selector("div.match__scoreboard-score.left")
                if ls_tag1:
                    ls1 = await ls_tag1.inner_text()

                ls_tag2 = await page.query_selector("div.match__scoreboard-score.right")
                if ls_tag2:
                    ls2 = await ls_tag2.inner_text()

                html = await page.content()
                self._parse_dom(url, html, live_timer=lt, live_scores=(ls1, ls2))
                consecutive_errs = 0
            except Exception as e:
                consecutive_errs += 1
                if consecutive_errs > 10:
                    _log(f"DLTV poller {url} error: {e}")
                    consecutive_errs = 0

            await asyncio.sleep(random.uniform(2.5, 4.5))

    def _parse_dom(self, url: str, html: str, live_timer=None, live_scores=None):
        soup = BeautifulSoup(html, "lxml")
        result_maps: dict[int, dict] = {}

        # ── Завершённые карты ─────────────────────────────────────────────────
        for card in soup.find_all("div", class_="map__finished-v2"):
            head = card.find("div", class_="map__finished-v2__head")
            if not head:
                continue

            span_tag = head.find("span")
            map_num_m = re.search(r'Map\s*#(\d+)', span_tag.get_text() if span_tag else "")
            if not map_num_m:
                continue
            map_num = int(map_num_m.group(1))

            small_tag = head.find("small")
            mid_m = re.search(r'Match\s*ID:\s*(\d{8,12})', small_tag.get_text() if small_tag else "")

            upd: dict = {"status": "finished"}
            if mid_m:
                upd["dota2_match_id"] = mid_m.group(1)

            scores_div = card.find("div", class_="map__finished-v2__scores")
            if scores_div:
                dur_div = scores_div.find("div", class_="duration")
                if dur_div:
                    b_tag = dur_div.find("b")
                    if b_tag:
                        upd["duration"] = b_tag.get_text(strip=True)

                for team_div in scores_div.find_all("div", class_="team", recursive=False):
                    name_span  = team_div.find("span", class_="name")
                    side_span  = team_div.find("span", class_="side")
                    kills_div  = team_div.find("div", class_="team__scores-kills")
                    winner_div = team_div.find("div", class_="winner")

                    name  = name_span.get_text(strip=True) if name_span else ""
                    side  = side_span.get_text(strip=True).lower() if side_span else ""
                    kills_text = kills_div.get_text(strip=True) if kills_div else "0"
                    kills = int(re.search(r'\d+', kills_text).group()) if re.search(r'\d+', kills_text) else 0

                    if "radiant" in side:
                        upd["radiant_team"]  = name
                        upd["kills_radiant"] = kills
                    elif "dire" in side:
                        upd["dire_team"]  = name
                        upd["kills_dire"] = kills

                    if winner_div and winner_div.get_text(strip=True).lower() == "win":
                        upd["winner"] = name

                    fb_container = (team_div.find("div", class_="team__fb") or
                                    team_div.find("span", class_="team__title-kills"))
                    if fb_container and any(s.get_text(strip=True) == "FB" for s in fb_container.find_all("span")):
                        if "radiant" in side:
                            upd["fb_radiant"] = True
                        elif "dire" in side:
                            upd["fb_dire"] = True

            result_maps[map_num] = upd

        # ── Живая карта ───────────────────────────────────────────────────────
        live_div = soup.find(id="live_scoreboard")
        if live_div:
            map_num = None

            for cls in ("card__title", "map__title", "map-title", "card-title"):
                card_title = live_div.find_previous(class_=cls)
                if card_title:
                    m = re.search(r'[Mm]ap\s*#?\s*(\d+)', card_title.get_text())
                    if m:
                        map_num = int(m.group(1))
                        break

            if map_num is None:
                for tag in reversed(live_div.find_all_previous(string=re.compile(r'[Mm]ap\s*#?\s*\d+'))):
                    m = re.search(r'[Mm]ap\s*#?\s*(\d+)', str(tag))
                    if m:
                        map_num = int(m.group(1))
                        break

            if map_num is None:
                parent = live_div.parent
                while parent and parent.name not in ("body", "[document]"):
                    title_el = parent.find(string=re.compile(r'[Mm]ap\s*#?\s*\d+'))
                    if title_el:
                        m = re.search(r'[Mm]ap\s*#?\s*(\d+)', str(title_el))
                        if m:
                            map_num = int(m.group(1))
                            break
                    parent = parent.parent

            if map_num is None:
                map_num = len(result_maps) + 1

            upd = {"status": "running"}

            info_match = live_div.find("div", class_="info__match")
            if info_match:
                try:
                    p_score1 = info_match.select_one("span.team1__score")
                    p_score2 = info_match.select_one("span.team2__score")
                    if p_score1 and p_score2:
                        upd["kills_radiant"] = int(p_score1.get_text(strip=True))
                        upd["kills_dire"]    = int(p_score2.get_text(strip=True))
                except Exception:
                    pass

            if live_scores and len(live_scores) == 2:
                try:
                    if live_scores[0] is not None: upd["kills_radiant"] = int(live_scores[0].strip())
                    if live_scores[1] is not None: upd["kills_dire"]    = int(live_scores[1].strip())
                except Exception:
                    pass

            if live_timer:
                upd["duration"] = live_timer.strip()

            result_maps[map_num] = upd

        # Save to cache
        with self._lock:
            self._cache[url] = result_maps

    def watch(self, url: str):
        with self._lock:
            self._watched.add(url)

    def unwatch(self, url: str):
        with self._lock:
            self._watched.discard(url)
            self._pages.pop(url, None)

    def get_maps(self, url: str) -> dict:
        with self._lock:
            return dict(self._cache.get(url, {}))

    def update_map(self, url: str, map_num: int, data: dict):
        with self._lock:
            if url not in self._cache:
                self._cache[url] = {}
            if map_num not in self._cache[url]:
                self._cache[url][map_num] = {}
            self._cache[url][map_num].update(data)

    def is_ok(self) -> bool:
        return self._pw_ok

    def fetch_page_html(self, url: str) -> "str | None":
        if not self._pw_ok or self._loop is None:
            return None
        future = asyncio.run_coroutine_threadsafe(
            self._fetch_html_async(url), self._loop
        )
        try:
            return future.result(timeout=45)
        except Exception as e:
            _log(f"fetch_page_html {url}: {e}")
            return None

    async def _fetch_html_async(self, url: str) -> "str | None":
        try:
            page = await self._browser.new_page()
            await page.add_init_script(self._STEALTH_JS)
            await page.set_extra_http_headers({
                "Accept-Language": random.choice(_ACCEPT_LANG_POOL),
            })
            await page.goto(url, wait_until="networkidle", timeout=30000)
            try:
                await page.wait_for_selector("a[href*='/matches/']", timeout=10000)
            except Exception:
                pass
            html = await page.content()
            await page.close()
            return html
        except Exception as e:
            _log(f"_fetch_html_async {url}: {e}")
            return None


# ── DLTV Watcher Instance ─────────────────────────────────────────────────────
dltv_watcher = DLTVWatcher()

def _parse_match_card_teams(card) -> tuple:
    event_tag = card.select_one("div.match__head-event span")
    event_str = event_tag.get_text(strip=True) if event_tag else ""

    bo = 3
    for fmt_div in card.select("div.match__head-format"):
        fmt_text = fmt_div.get_text(strip=True).lower()
        bo_m = re.search(r'bo\s*(\d)', fmt_text) or re.search(r'best\s*of\s*(\d)', fmt_text)
        if bo_m:
            bo = int(bo_m.group(1))
            break

    bracket = ""
    for fmt_div in card.select("div.match__head-format.red, div.match__head-format"):
        txt = fmt_div.get_text(strip=True)
        if txt.lower() not in ("bo1", "bo2", "bo3", "bo4", "bo5") and "best" not in txt.lower():
            bracket = txt
            break

    team_title_tags = card.select("div.team__title span")
    team_names = [t.get_text(strip=True) for t in team_title_tags if t.get_text(strip=True)]

    card_lower = card.get_text(" ", strip=True).lower()
    abbrevs: list[str] = []
    for abbrev in TEAM_MAP:
        if _team_matches(card_lower, abbrev) and abbrev not in abbrevs:
            abbrevs.append(abbrev)
        if len(abbrevs) >= 2:
            break

    t1 = (team_names[0]           if len(team_names) > 0
          else abbrevs[0].upper() if len(abbrevs) > 0
          else "Team1")
    t2 = (team_names[1]           if len(team_names) > 1
          else abbrevs[1].upper() if len(abbrevs) > 1
          else "Team2")

    return t1, t2, bo, event_str, bracket, abbrevs


def scan_dltv_matches(force: bool = False) -> tuple:
    global _dltv_page_html, _dltv_page_fetched_at

    found    = []
    upcoming = []
    try:
        now_ts = time.time()
        with dltv_cache_lock_html:
            age    = now_ts - _dltv_page_fetched_at
            need   = force or (_dltv_page_html is None) or (age > _DLTV_HTML_MAX_AGE)

        if need:
            fetched = dltv_watcher.fetch_page_html("https://dltv.org/matches")
            if fetched:
                with dltv_cache_lock_html:
                    _dltv_page_html      = fetched
                    _dltv_page_fetched_at = now_ts
                _log(f"DLTV /matches: HTML перезагружен (force={force}, прошло={age:.0f}s)")
            elif _dltv_page_html is None:
                return found, upcoming
            else:
                _log("DLTV /matches: загрузка не удалась, используем кэш")

        with dltv_cache_lock_html:
            html = _dltv_page_html
        if not html:
            return found, upcoming

        soup = BeautifulSoup(html, "lxml")

        # ── Live карточки ─────────────────────────────────────────────────────
        live_cards = soup.find_all("div", class_=lambda c: c and "match" in c.split() and "live" in c.split())

        for card in live_cards:
            a_tag = card.select_one("div.match__body-details > a[href]")
            if not a_tag:
                a_tag = card.select_one("div.match__head > a[href]")
            if not a_tag:
                a_tag = card.find("a", href=re.compile(r"/matches/\d+/"))

            href = a_tag["href"] if a_tag else ""
            m = re.match(r"(?:https://dltv\.org)?/matches/(\d+)/([^/?#\s]+)", href)
            if not m:
                continue

            mid_id = m.group(1)
            slug   = m.group(2)
            url    = f"https://dltv.org/matches/{mid_id}/{slug}"

            t1, t2, bo, event, bracket, abbrevs = _parse_match_card_teams(card)

            # Парсим счёт серий
            score1 = 0
            score2 = 0
            score_div = card.select_one("div.match__body-score")
            if score_div:
                scores = [s.get_text(strip=True) for s in score_div.select("span")]
                if len(scores) >= 2:
                    try:
                        score1 = int(scores[0])
                        score2 = int(scores[1])
                    except ValueError:
                        pass

            match_info = {
                "id":            mid_id,
                "url":           url,
                "team1":         t1,
                "team2":         t2,
                "score1":        score1,
                "score2":        score2,
                "bo":            bo,
                "event":         event,
                "bracket":       bracket,
                "abbreviations": abbrevs,
                "type":          "live",
            }

            found.append(match_info)

            with dltv_cache_lock:
                if url not in dltv_match_cache:
                    dltv_match_cache[url] = match_info
                else:
                    dltv_match_cache[url].update({
                        "score1": score1,
                        "score2": score2,
                        "bo":     bo,
                    })

        # ── Предстоящие карточки ──────────────────────────────────────────────
        up_cards = soup.find_all("div", class_=lambda c: c and "match" in c.split() and "upcoming" in c.split())
        for card in up_cards:
            a_tag = card.select_one("div.match__body-details > a[href]")
            if not a_tag:
                a_tag = card.find("a", href=re.compile(r"/matches/\d+/"))
            href = a_tag["href"] if a_tag else ""
            m = re.match(r"(?:https://dltv\.org)?/matches/(\d+)/([^/?#\s]+)", href)
            if not m:
                continue
            mid_id = m.group(1)
            slug   = m.group(2)
            url    = f"https://dltv.org/matches/{mid_id}/{slug}"

            t1, t2, bo, event, bracket, abbrevs = _parse_match_card_teams(card)

            time_span = card.select_one("div.match__body-time")
            time_str  = time_span.get_text(strip=True) if time_span else ""

            dt_str    = ""
            dt_display = ""
            date_span = card.select_one("div.match__head-date")
            if date_span:
                dt_display = date_span.get_text(strip=True)
                today_d    = datetime.now().strftime("%Y-%m-%d")
                if "today" in dt_display.lower() or "сегодня" in dt_display.lower():
                    dt_str = today_d
                elif "tomorrow" in dt_display.lower() or "завтра" in dt_display.lower():
                    dt_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    try:
                        parsed_d = datetime.strptime(f"{dt_display} {datetime.now().year}", "%d %b %Y")
                        dt_str   = parsed_d.strftime("%Y-%m-%d")
                    except Exception:
                        dt_str = today_d

            m_time = ""
            if dt_str and time_str:
                m_time = f"{dt_str} {time_str}:00"

            upcoming.append({
                "id":            mid_id,
                "url":           url,
                "team1":         t1,
                "team2":         t2,
                "bo":            bo,
                "event":         event,
                "bracket":       bracket,
                "display_time":  time_str,
                "display_date":  dt_display,
                "match_time":    m_time,
                "abbreviations": abbrevs,
                "type":          "upcoming",
            })

    except Exception as e:
        _log(f"scan_dltv_matches: {e}")

    return found, upcoming


def bot_loop():
    check_count             = 0
    next_dltv_scan          = 0
    no_live_streak          = 0
    smart_sleep_until       = 0.0
    known_finished_series: set  = set()
    next_insurance_scan: float  = 0.0
    _live_scan_done: bool       = False

    _next_forced_listing_scan: float = 0.0
    _FORCED_LISTING_INTERVAL:  float = 300.0

    NO_LIVE_THRESHOLD = 3

    _prev_finished_maps: set   = set()
    _upcoming_start_ts : dict  = {}

    while True:
        check_count += 1
        now = time.time()

        # ── Принудительный скан листинга каждые 5 мин ────────────────────────
        if now >= _next_forced_listing_scan:
            _forced_live, _forced_upcoming = scan_dltv_matches(force=True)
            _next_forced_listing_scan = now + _FORCED_LISTING_INTERVAL
            if _forced_live:
                _log(f"🔄 Принудительный скан: {len(_forced_live)} live-матч(а), smart_sleep сброшен")
                smart_sleep_until = 0.0
                no_live_streak    = 0
            for _fm in _forced_live:
                _furl = _fm.get("url", "")
                if _furl and dltv_watcher.is_ok():
                    dltv_watcher.watch(_furl)
                    _fmaps = dltv_watcher.get_maps(_furl)
                    if not _fmaps:
                        _log(f"🔄 Принудительный скан: новый матч {_fm.get('team1')} vs {_fm.get('team2')} — начинаем наблюдение")
            try:
                r_client.set("dota:upcoming_matches", json.dumps(_forced_upcoming, ensure_ascii=False))
            except Exception as e:
                print(f"Redis Write Error: {e}")
            if not _live_scan_done:
                _live_scan_done = True

        # ── Детектируем завершение серии → добавляем в историю ───────────────
        with dltv_cache_lock:
            _chk_list = list(dltv_match_cache.values())
        for _mi_chk in _chk_list:
            _url_chk = _mi_chk.get("url", "")
            if not _url_chk or _url_chk in known_finished_series:
                continue
            _bo_chk      = _mi_chk.get("bo", 3)
            _wins_needed = (_bo_chk + 1) // 2

            _maps_chk = dltv_watcher.get_maps(_url_chk)
            _win_counts: dict = {}
            for _mn, _md in _maps_chk.items():
                if _md.get("status") == "finished":
                    _w = _md.get("winner", "")
                    if _w:
                        _win_counts[_w] = _win_counts.get(_w, 0) + 1

            _finished_by_wins = False
            for _team, _wins in _win_counts.items():
                if _wins >= _wins_needed:
                    _finished_by_wins = True
                    break

            if _finished_by_wins:
                _log(f"🏆 Серия {_mi_chk.get('team1')} vs {_mi_chk.get('team2')} завершена по победам карт! ({_win_counts})")
                known_finished_series.add(_url_chk)
                dltv_watcher.unwatch(_url_chk)
                with history_lock:
                    if _url_chk not in dltv_history_cache:
                        dltv_history_cache[_url_chk] = _mi_chk
                        # Обрезаем историю
                        if len(dltv_history_cache) > _MAX_HISTORY:
                            _k = list(dltv_history_cache.keys())[0]
                            dltv_history_cache.pop(_k, None)

        do_normal_scan = True
        upcoming_just_started = False

        if _live_scan_done:
            for url, start_ts in list(_upcoming_start_ts.items()):
                if now >= start_ts - 45:
                    _log(f"⏰ Наступает время старта предстоящего матча {url}. Сбрасываем sleep.")
                    smart_sleep_until = 0.0
                    no_live_streak    = 0
                    upcoming_just_started = True
                    _upcoming_start_ts.pop(url, None)

            if now < smart_sleep_until:
                do_normal_scan = False

                if now >= next_insurance_scan:
                    _log("🛡 Sleep insurance: проверяем live/upcoming матчи…")
                    ins_live, ins_upcoming = scan_dltv_matches(force=True)
                    next_insurance_scan = now + random.uniform(240, 360)

                    try:
                        r_client.set("dota:upcoming_matches", json.dumps(ins_upcoming, ensure_ascii=False))
                    except Exception as e:
                        print(f"Redis Write Error: {e}")

                    if ins_live:
                        _log("🔴 Sleep insurance: найден live матч! Выходим из sleep.")
                        smart_sleep_until = 0.0
                        no_live_streak    = 0
                        do_normal_scan    = True
                        _upcoming_start_ts.clear()
                        for u in ins_upcoming:
                            try:
                                ts = datetime.strptime(u["match_time"], "%Y-%m-%d %H:%M:%S").timestamp()
                                if ts > now:
                                    _upcoming_start_ts[u["url"]] = ts
                            except Exception:
                                pass
                    elif ins_upcoming:
                        nearest = ins_upcoming[0]
                        try:
                            nearest_ts = datetime.strptime(nearest["match_time"], "%Y-%m-%d %H:%M:%S").timestamp()
                            if nearest_ts - 90 != smart_sleep_until:
                                smart_sleep_until = nearest_ts - 90
                                _log(f"🛡 Sleep insurance: расписание обновилось. Спим до {nearest['match_time']}. Разминка -90с.")
                        except Exception:
                            pass

        if do_normal_scan and now >= next_dltv_scan:
            live_found, upcoming_found = scan_dltv_matches(force=False)
            _live_scan_done = True

            for u in upcoming_found:
                try:
                    ts = datetime.strptime(u["match_time"], "%Y-%m-%d %H:%M:%S").timestamp()
                    if ts > now:
                        _upcoming_start_ts[u["url"]] = ts
                except Exception:
                    pass

            if live_found:
                no_live_streak = 0
                smart_sleep_until = 0.0
                for m in live_found:
                    url = m.get("url", "")
                    if url and dltv_watcher.is_ok():
                        dltv_watcher.watch(url)
            else:
                no_live_streak += 1
                if no_live_streak >= NO_LIVE_THRESHOLD:
                    if upcoming_found:
                        nearest = upcoming_found[0]
                        nearest_ts = 0.0
                        try:
                            nearest_ts = datetime.strptime(nearest["match_time"], "%Y-%m-%d %H:%M:%S").timestamp()
                        except Exception:
                            pass

                        if nearest_ts > now:
                            smart_sleep_until = nearest_ts - 90
                            wake_str = datetime.fromtimestamp(smart_sleep_until).strftime("%H:%M:%S")
                            _log(f"⏳ Нет live ({no_live_streak} сканов). Следующий: {nearest['team1']} vs {nearest['team2']} в {nearest['display_time']} {nearest['display_date']}. Сканирование возобновится в {wake_str}")
                            no_live_streak = 0
                            next_insurance_scan = now + random.uniform(240, 360)
                else:
                    smart_sleep_until = 0.0

            try:
                r_client.set("dota:upcoming_matches", json.dumps(upcoming_found, ensure_ascii=False))
            except Exception as e:
                print(f"Redis Write Error: {e}")

            next_dltv_scan = now + _jitter(POLL_INTERVAL * DLTV_SCAN_INT, 0.8, 1.3)
            if upcoming_just_started and not live_found:
                next_dltv_scan = max(next_dltv_scan, now + 30)

        live_display = []
        with dltv_cache_lock:
            match_list = list(dltv_match_cache.values())

        # Собираем данные live_display
        for mi in match_list:
            url   = mi.get("url", "")
            teams = [mi.get("team1", "Team1"), mi.get("team2", "Team2")]
            bo    = mi.get("bo", 3)

            if url in known_finished_series:
                continue

            maps_data = dltv_watcher.get_maps(url)
            maps_info = []

            for map_num in range(1, bo + 1):
                md     = maps_data.get(map_num, {})
                status = md.get("status", "upcoming")

                # Триггеры изменения статусов карт (в микросервисах мы можем это паблишить!)
                _f_key = (url, map_num)
                if status == "finished" and _f_key not in _prev_finished_maps:
                    _prev_finished_maps.add(_f_key)
                    _log(f"🔔 Карта {map_num} в серии {mi.get('team1')} vs {mi.get('team2')} завершена!")
                    # Публикуем событие
                    try:
                        r_client.publish("dota:events", json.dumps({
                            "event": "map_finished",
                            "url": url,
                            "map_num": map_num,
                            "match_id": md.get("dota2_match_id")
                        }))
                    except Exception as e:
                        pass

                maps_info.append({
                    "num":              map_num,
                    "status":           status,
                    "winner":           md.get("winner"),
                    "kills_radiant":    md.get("kills_radiant"),
                    "kills_dire":       md.get("kills_dire"),
                    "duration":         md.get("duration"),
                    "dota2_match_id":   md.get("dota2_match_id"),
                    "fb_radiant":       md.get("fb_radiant"),
                    "fb_dire":          md.get("fb_dire"),
                })

            live_display.append({
                "id":     mi.get("id"),
                "team1":  teams[0],
                "team2":  teams[1],
                "score1": mi.get("score1", 0),
                "score2": mi.get("score2", 0),
                "bo":     bo,
                "event":  mi.get("event", ""),
                "bracket": mi.get("bracket", ""),
                "url":    url,
                "maps":   maps_info,
            })

        # Формируем sleep_info для дашборда
        now2 = time.time()
        sleep_info: dict = {}
        if now2 < smart_sleep_until:
            try:
                ups_str = r_client.get("dota:upcoming_matches")
                ups = json.loads(ups_str) if ups_str else []
            except Exception:
                ups = []
            next_m = ups[0] if ups else {}
            sleep_info = {
                "sleeping":    True,
                "until":       datetime.fromtimestamp(smart_sleep_until).strftime("%H:%M:%S"),
                "remaining_min": round((smart_sleep_until - now2) / 60, 1),
                "next_match":  f"{next_m.get('team1','?')} vs {next_m.get('team2','?')}",
                "next_time":   next_m.get("display_time", ""),
                "next_date":   next_m.get("display_date", ""),
            }
        else:
            sleep_info = {"sleeping": False}

        # ── Собираем историю матчей (завершённые с DLTV) ─────────────────────────
        history_display = []
        with history_lock:
            history_list = list(dltv_history_cache.values())

        for hi in history_list:
            url   = hi.get("url", "")
            t1    = hi.get("team1", "Team1")
            t2    = hi.get("team2", "Team2")
            bo    = hi.get("bo", 3)

            maps_data = dltv_watcher.get_maps(url)
            maps_info = []

            for map_num in range(1, bo + 1):
                md     = maps_data.get(map_num, {})
                maps_info.append({
                    "num":              map_num,
                    "status":           md.get("status", "upcoming"),
                    "winner":           md.get("winner"),
                    "kills_radiant":    md.get("kills_radiant"),
                    "kills_dire":       md.get("kills_dire"),
                    "duration":         md.get("duration"),
                    "dota2_match_id":   md.get("dota2_match_id"),
                    "fb_radiant":       md.get("fb_radiant"),
                    "fb_dire":          md.get("fb_dire"),
                })

            history_display.append({
                "id":           hi.get("id"),
                "team1":        t1,
                "team2":        t2,
                "score1":       hi.get("score1", 0),
                "score2":       hi.get("score2", 0),
                "bo":           bo,
                "event":        hi.get("event", ""),
                "bracket":      hi.get("bracket", ""),
                "url":          url,
                "display_time": hi.get("display_time", ""),
                "display_date": hi.get("display_date", ""),
                "maps":         maps_info,
            })

        # Записываем состояние скрапера в Redis
        try:
            pipe = r_client.pipeline()
            pipe.set("dota:live_matches", json.dumps(live_display, ensure_ascii=False))
            pipe.set("dota:history_matches", json.dumps(history_display, ensure_ascii=False))
            pipe.set("dota:sleep_info", json.dumps(sleep_info, ensure_ascii=False))
            pipe.set("dota:updated_at", _now())
            pipe.execute()

            # Публикуем событие об обновлении состояния
            r_client.publish("dota:state_updated", "1")
        except Exception as e:
            print(f"Redis pipeline error: {e}")

        time.sleep(POLL_INTERVAL + random.uniform(-0.5, 0.5))


if __name__ == "__main__":
    print("═" * 60)
    print("  🎮  Dota 2 DLTV Scraper Microservice  ·  v6-micro")
    print("═" * 60)
    print(f"  📡  Источник карт:  DLTV.org  (Playwright + stealth)")
    print(f"  Redis Host:        {REDIS_HOST}:{REDIS_PORT}")
    print("═" * 60)
    print()

    bot_loop()
