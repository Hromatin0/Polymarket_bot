"""
Dota 2 Betting Engine Microservice
═══════════════════════════════════════════════════════════════════
  Reads state from Redis, resolves betting conditions,
  and pushes order placement commands to Redis queue polymarket:orders.
═══════════════════════════════════════════════════════════════════
"""

import os
import re
import sys
import time
import logging
import json
import threading
from datetime import datetime, timezone, timedelta
import redis
import requests
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
POLL_INTERVAL = 4.0

BET_TYPE_ENABLED: dict[str, bool] = {
    "any_rampage":      True,
    "any_ultra_kill":   True,
    "both_roshan":      False,   # disabled; change to True to enable
    "both_barracks":    True,
    "fb_radiant":       True,
    "fb_dire":          True,
    "ends_in_daytime":  True,
}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("bettor")

r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# ── Resolvers ─────────────────────────────────────────────────────────────────
MK_RANK = {"Rampage": 5, "Ultra Kill": 4, "Triple Kill": 3, "Double Kill": 2}

def _mk_rank(label: str) -> int:
    return MK_RANK.get(label or "", 0)

def _is_daytime(duration: str) -> bool | None:
    if not duration:
        return None
    m = re.match(r'^(\d+):(\d{2})$', duration)
    if not m:
        return None
    total_sec = int(m.group(1)) * 60 + int(m.group(2))
    return (total_sec // 300) % 2 == 0

DLTV_BASED_TYPES: frozenset = frozenset({
    "fb_radiant", "fb_dire",
})

RESOLVERS: dict[str, callable] = {
    "any_rampage": lambda mp: (
        None if mp.get("db_phase") != "full" else
        max(_mk_rank(mp.get("db_multikill_radiant")), _mk_rank(mp.get("db_multikill_dire"))) >= 5
    ),
    "any_ultra_kill": lambda mp: (
        None if mp.get("db_phase") != "full" else
        max(_mk_rank(mp.get("db_multikill_radiant")), _mk_rank(mp.get("db_multikill_dire"))) >= 4
    ),
    "both_roshan": lambda mp: (
        None if mp.get("db_phase") != "full" else
        bool((mp.get("db_roshans_radiant") or 0) >= 1 and (mp.get("db_roshans_dire") or 0) >= 1)
    ),
    "both_barracks": lambda mp: (
        None if mp.get("db_phase") != "full" else
        bool((mp.get("db_barracks_radiant") or 0) >= 1 and (mp.get("db_barracks_dire") or 0) >= 1)
    ),
    "fb_radiant": lambda mp: (
        None if not (mp.get("fb_radiant") or mp.get("fb_dire")) else bool(mp.get("fb_radiant"))
    ),
    "fb_dire": lambda mp: (
        None if not (mp.get("fb_radiant") or mp.get("fb_dire")) else bool(mp.get("fb_dire"))
    ),
    "ends_in_daytime": lambda mp: (
        None if mp.get("db_phase") not in ("basic", "full") else
        _is_daytime(mp.get("db_duration"))
    ),
}

BET_PHASE: dict[str, set] = {
    "any_rampage":      {"full"},
    "any_ultra_kill":   {"full"},
    "both_roshan":      {"full"},
    "both_barracks":    {"full"},
    "ends_in_daytime":  {"basic", "full"},
}

BET_KEYWORDS: dict[str, list] = {
    "any_rampage":      ["rampage"],
    "any_ultra_kill":   ["ultra kill", "ultra-kill"],
    "ends_in_daytime":  ["ends-in-daytime", "daytime", "day or night"],
    "both_roshan":      ["roshan"],
    "both_barracks":    ["barracks"],
    "fb_radiant":       ["first-blood", "first blood", "firstblood"],
    "fb_dire":          ["first-blood", "first blood", "firstblood"],
}

# ── Market search ────────────────────────────────────────────────────────────
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"

EVENTS_CACHE_TTL      = 120
MARKET_CACHE_TTL      = 3600
MARKET_CACHE_MISS_TTL = 60

_events_cache: list = []
_events_cache_ts: float = 0.0
_cache_lock = threading.Lock()

_market_cache: dict = {}
_market_cache_ts: dict = {}
_market_cache_hit: dict = {}

_NAME_STOPWORDS = frozenset({"gaming", "team", "esports", "esport", "club", "fc", "the"})

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

def _team_matches(text_lower: str, abbrev: str) -> bool:
    for v in TEAM_MAP.get(abbrev.lower(), [abbrev.lower()]):
        if v.lower() in text_lower:
            return True
    return False

def _http_get(url: str, timeout: int = 12) -> dict | list | None:
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception as e:
        log.debug(f"HTTP GET {url}: {e}")
        return None

def fetch_dota2_events(force: bool = False) -> list:
    global _events_cache, _events_cache_ts
    with _cache_lock:
        if not force and (time.time() - _events_cache_ts < EVENTS_CACHE_TTL and _events_cache):
            return list(_events_cache)

    url = (f"{GAMMA_BASE}/events/pagination"
           f"?limit=50&active=true&archived=false"
           f"&tag_slug=dota-2&closed=false"
           f"&order=volume24hr&ascending=false&offset=0")
    data = _http_get(url)
    events = []
    if isinstance(data, list):
        events = data
    elif isinstance(data, dict):
        events = data.get("events") or data.get("data") or []

    log.info(f"Gamma /events: loaded {len(events)} Dota 2 events")
    with _cache_lock:
        _events_cache = events
        _events_cache_ts = time.time()
    return events

def _name_parts(name: str) -> list[str]:
    parts = re.split(r'[\s\-_\.]', name.lower().strip())
    return [p for p in parts if len(p) >= 2 and p not in _NAME_STOPWORDS]

def _name_in_text(name: str, text: str) -> bool:
    text_low = text.lower()
    for part in _name_parts(name):
        if len(part) <= 3:
            if re.search(r'\b' + re.escape(part) + r'\b', text_low):
                return True
        else:
            if part in text_low:
                return True
    return False

def _event_score_fn(ev: dict, t1: str, t2: str, radiant_team: str = "", dire_team: str = "") -> int:
    title = (ev.get("title") or ev.get("slug") or "").lower()
    rt = radiant_team or t1
    dt = dire_team    or t2
    side1_names = [rt]
    side2_names = [dt]
    rt_low = rt.lower().strip()
    dt_low = dt.lower().strip()
    if t1 and t1.lower().strip() != dt_low and t1 not in side1_names:
        side1_names.append(t1)
    if t2 and t2.lower().strip() != rt_low and t2 not in side2_names:
        side2_names.append(t2)
    side1_found = any(_name_in_text(name, title) for name in side1_names)
    side2_found = any(_name_in_text(name, title) for name in side2_names)
    if not side1_found or not side2_found:
        return 0
    team_score  = sum(5 for name in side1_names if _name_in_text(name, title))
    team_score += sum(5 for name in side2_names if _name_in_text(name, title))
    date_bonus = 0
    for delta in (0, 1, -1):
        d = (datetime.now(timezone.utc) + timedelta(days=delta)).strftime("%Y-%m-%d")
        if d in (ev.get("slug") or ""):
            date_bonus = 4
            break
    return team_score + date_bonus

def _score_market(market: dict, map_num: int, bet_type: str, fb_team: str = "") -> int:
    if not _market_is_open(market):
        return 0
    slug = (market.get("slug") or "").lower()
    q = (market.get("question") or "").lower()
    kws = BET_KEYWORDS.get(bet_type, [])
    if not any(kw in slug or kw in q for kw in kws):
        return 0

    slug_gm = re.search(r'game-?(\d+)', slug)
    q_gm    = re.search(r'game\s*#?(\d+)', q, re.I)
    explicit_game = None
    if slug_gm:
        explicit_game = int(slug_gm.group(1))
    elif q_gm:
        explicit_game = int(q_gm.group(1))
    if explicit_game is not None and explicit_game != map_num:
        return 0

    score = 0
    if any(kw in slug for kw in kws): score += 10
    if any(kw in q for kw in kws): score += 8
    if re.search(rf"game-?{map_num}(?:\b|-)", slug): score += 15
    if re.search(rf"game\s*#?{map_num}\b", q, re.I): score += 12
    if bet_type in ("fb_radiant", "fb_dire") and fb_team:
        if _name_in_text(fb_team, slug) or _name_in_text(fb_team, q):
            score += 8
    return score

def _market_is_open(market: dict) -> bool:
    if not market.get("active", True) or market.get("closed", False):
        return False
    end = market.get("endDate") or market.get("end_date") or ""
    if end:
        try:
            dt  = datetime.fromisoformat(end.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            if dt < now - timedelta(hours=12):
                return False
            if dt > now + timedelta(hours=24):
                return False
        except Exception:
            pass
    return True

def _extract_tokens_from_list(tokens: list, bet_type: str = "", radiant_team: str = "", dire_team: str = "") -> tuple[str, str]:
    if not tokens or not isinstance(tokens[0], dict):
        return "", ""
    is_fb = bet_type in ("fb_radiant", "fb_dire")
    if not is_fb:
        yes_token = no_token = ""
        for t in tokens:
            outcome = (t.get("outcome") or "").strip().lower()
            tid = t.get("token_id", "")
            if outcome == "yes":
                yes_token = tid
            elif outcome == "no":
                no_token = tid
        return yes_token, no_token

    team_for_yes = radiant_team if bet_type == "fb_radiant" else dire_team
    team_for_no  = dire_team    if bet_type == "fb_radiant" else radiant_team
    yes_token = no_token = ""
    for t in tokens:
        outcome_raw = (t.get("outcome") or "").strip()
        tid = t.get("token_id", "")
        outcome_low = outcome_raw.lower()
        if team_for_yes and _name_in_text(team_for_yes, outcome_low):
            yes_token = tid
        elif team_for_no and _name_in_text(team_for_no, outcome_low):
            no_token = tid
        elif outcome_low == "yes" and not yes_token:
            yes_token = tid
        elif outcome_low == "no" and not no_token:
            no_token = tid
    if not yes_token and len(tokens) >= 1:
        yes_token = tokens[0].get("token_id", "")
    if not no_token:
        for t in tokens:
            tid = t.get("token_id", "")
            if tid and tid != yes_token:
                no_token = tid
                break
        if not no_token and len(tokens) >= 2:
            no_token = tokens[1].get("token_id", "")
    return yes_token, no_token

def get_clob_tokens(condition_id: str, market: dict | None = None, bet_type: str = "", radiant_team: str = "", dire_team: str = "") -> tuple[str, str, dict]:
    if not condition_id:
        return "", "", {}
    data = _http_get(f"{CLOB_BASE}/markets/{condition_id}") or {}
    tokens = data.get("tokens") or []
    if tokens and isinstance(tokens[0], dict):
        yes_tok, no_tok = _extract_tokens_from_list(tokens, bet_type, radiant_team, dire_team)
        return yes_tok, no_tok, data
    if market:
        tokens = market.get("tokens") or []
        if tokens and isinstance(tokens[0], dict):
            yes_tok, no_tok = _extract_tokens_from_list(tokens, bet_type, radiant_team, dire_team)
            return yes_tok, no_tok, data
    return "", "", data

def find_market(t1: str, t2: str, map_num: int, bet_type: str, fb_team: str = "", radiant_team: str = "", dire_team: str = "") -> dict | None:
    eff_t1 = radiant_team or t1
    eff_t2 = dire_team or t2
    cache_key = f"{t1.lower().strip()}:{t2.lower().strip()}:{map_num}:{bet_type}:{eff_t1.lower().strip()}:{eff_t2.lower().strip()}"

    # Try local memory cache or Redis cache
    with _cache_lock:
        if cache_key in _market_cache:
            is_hit = _market_cache_hit.get(cache_key, True)
            ttl = MARKET_CACHE_TTL if is_hit else MARKET_CACHE_MISS_TTL
            age = time.time() - _market_cache_ts.get(cache_key, 0)
            if age < ttl:
                return _market_cache[cache_key]

    events = fetch_dota2_events()
    if not events:
        return None

    scored = sorted(events, key=lambda ev: _event_score_fn(ev, t1, t2, radiant_team, dire_team), reverse=True)

    best_market = None
    best_score  = 0
    found_event = None

    for ev in scored[:5]:
        ev_score = _event_score_fn(ev, t1, t2, radiant_team, dire_team)
        if ev_score == 0:
            break
        markets = ev.get("markets") or []
        for m in markets:
            sc = _score_market(m, map_num, bet_type, fb_team=fb_team)
            if sc > best_score:
                best_score  = sc
                best_market = m
                found_event = ev

    if not best_market:
        log.info(f"Market not found: {bet_type} | {t1} vs {t2} game {map_num}")
        with _cache_lock:
            _market_cache[cache_key]    = None
            _market_cache_ts[cache_key] = time.time()
            _market_cache_hit[cache_key]= False
        return None

    condition_id = best_market.get("conditionId") or best_market.get("condition_id") or ""
    if not condition_id:
        return None

    yes_token, no_token, clob_data = get_clob_tokens(condition_id, best_market, bet_type, eff_t1, eff_t2)
    if not yes_token or not no_token:
        return None

    game_start_raw = clob_data.get("game_start_time") or ""
    if game_start_raw:
        try:
            game_start_dt = datetime.fromisoformat(game_start_raw.replace("Z", "+00:00"))
            if game_start_dt > datetime.now(timezone.utc):
                log.info(f"Market rejected (game_start_time in the future): {game_start_raw} | {best_market.get('question','')[:60]}")
                with _cache_lock:
                    _market_cache[cache_key]    = None
                    _market_cache_ts[cache_key] = time.time()
                    _market_cache_hit[cache_key]= False
                return None
        except Exception as e:
            log.debug(f"game_start_time parse error: {e}")

    log.info(f"Market found [{bet_type}]: \"{best_market.get('question','')[:55]}\" | score={best_score}")

    result = {
        "question":       best_market.get("question", ""),
        "slug":           best_market.get("slug", ""),
        "condition_id":   condition_id,
        "yes_token":      yes_token,
        "no_token":       no_token,
        "event_title":    found_event.get("title", "") if found_event else "",
        "score":          best_score,
        "game_start_time": game_start_raw,
    }

    with _cache_lock:
        _market_cache[cache_key]    = result
        _market_cache_ts[cache_key] = time.time()
        _market_cache_hit[cache_key]= True

    return result


def _game_has_started(mp: dict) -> bool:
    gt = (mp.get("game_time") or "").strip()
    if not gt or gt == "Draft...":
        return False
    return bool(re.match(r'^-?\d+:\d{2}$', gt))


class BettorBot:
    def __init__(self):
        self._no_market_skip: set[tuple] = set()
        self._miss_attempts: dict = {}

    def _process_map(self, mp: dict, match_info: dict, dotabuff_all: dict):
        t1            = match_info.get("team1", "?")
        t2            = match_info.get("team2", "?")
        dota_match_id = mp.get("dota2_match_id") or mp.get("dota_match_id", "")
        map_num       = mp.get("num", 1)
        status        = mp.get("status") or ""
        radiant_team  = mp.get("radiant_team") or t1
        dire_team     = mp.get("dire_team")    or t2

        if status == "upcoming":
            return

        # Навешиваем Dotabuff-данные на карту (если есть)
        db_entry = dotabuff_all.get(dota_match_id, {}) if dota_match_id else {}
        if db_entry:
            mp["db_phase"] = db_entry.get("phase")
            mp["db_duration"] = db_entry.get("duration")
            mp["db_multikill_radiant"] = db_entry.get("multikill_radiant")
            mp["db_multikill_dire"] = db_entry.get("multikill_dire")
            mp["db_roshans_radiant"] = db_entry.get("roshans_radiant")
            mp["db_roshans_dire"] = db_entry.get("roshans_dire")
            mp["db_barracks_radiant"] = db_entry.get("barracks_radiant")
            mp["db_barracks_dire"] = db_entry.get("barracks_dire")

        for bet_type, resolver in RESOLVERS.items():
            if not BET_TYPE_ENABLED.get(bet_type, True):
                continue

            # ── FB ──
            if bet_type in ("fb_radiant", "fb_dire"):
                if not (mp.get("fb_radiant") or mp.get("fb_dire")):
                    continue
                if status != "finished" and not _game_has_started(mp):
                    continue

            # ── Daytime ──
            elif bet_type == "ends_in_daytime":
                if status != "finished":
                    continue
                if mp.get("db_phase") not in ("basic", "full"):
                    continue
                if not mp.get("db_duration"):
                    continue

            # ── Dotabuff-типы ──
            else:
                if mp.get("db_phase") not in BET_PHASE.get(bet_type, set()):
                    continue

            skip_key = (dota_match_id, map_num, bet_type)
            if skip_key in self._no_market_skip:
                continue

            outcome = resolver(mp)
            if outcome is None:
                continue

            # Check in Redis Set to see if we already placed this bet!
            bet_key = f"cid:{dota_match_id}:{map_num}:{bet_type}"
            if r_client.sismember("polymarket:placed_bets", bet_key):
                continue

            if bet_type == "fb_radiant":
                fb_team = radiant_team
            elif bet_type == "fb_dire":
                fb_team = dire_team
            else:
                fb_team = ""

            market = find_market(
                t1=t1, t2=t2, map_num=map_num, bet_type=bet_type,
                fb_team=fb_team, radiant_team=radiant_team, dire_team=dire_team
            )

            if market is None:
                miss_key = (dota_match_id, map_num, bet_type)
                attempts = self._miss_attempts.get(miss_key, 0) + 1
                self._miss_attempts[miss_key] = attempts
                if attempts >= 3:
                    self._no_market_skip.add(skip_key)
                    log.info(f"permanent skip → {dota_match_id} map{map_num} {bet_type}")
                continue

            # Резолвер дал сигнал, маркет найден -> Отправляем команду в Redis Queue!
            bet_key_concrete = f"cid:{market.get('condition_id')}:{bet_type}"
            
            # Двойная проверка на уже сделанные ставки по concrete ID
            if r_client.sismember("polymarket:placed_bets", bet_key_concrete) or r_client.sismember("polymarket:placed_bets", bet_key):
                continue

            # Помечаем в Redis как размещенный в обработке, чтобы избежать гонки условий
            r_client.sadd("polymarket:placed_bets", bet_key)
            r_client.sadd("polymarket:placed_bets", bet_key_concrete)

            # Публикуем команду на исполнение в Redis List!
            order_command = {
                "market": market,
                "outcome": outcome,
                "resolver_name": bet_type,
                "map_data": {
                    "num": mp.get("num"),
                    "status": mp.get("status"),
                    "dota2_match_id": dota_match_id,
                    "db_phase": mp.get("db_phase"),
                    "duration": mp.get("duration"),
                    "db_duration": mp.get("db_duration"),
                    "radiant_team": radiant_team,
                    "dire_team": dire_team,
                    "fb_radiant": mp.get("fb_radiant"),
                    "fb_dire": mp.get("fb_dire"),
                },
                "match_info": {
                    "team1": t1,
                    "team2": t2,
                }
            }

            log.info(f"🎯 TRIGGER BET ORDER: {bet_type} | YES={outcome} | {t1} vs {t2} (Map {map_num})")
            r_client.rpush("polymarket:orders", json.dumps(order_command, ensure_ascii=False))


    def run(self):
        log.info("Betting Engine Microservice started!")
        fetch_dota2_events(force=True)

        while True:
            try:
                # Читаем состояние напрямую из Redis!
                live_str = r_client.get("dota:live_matches")
                hist_str = r_client.get("dota:history_matches")
                db_str   = r_client.get("dota:dotabuff")

                live_matches = json.loads(live_str) if live_str else []
                history_matches = json.loads(hist_str) if hist_str else []
                dotabuff_all = json.loads(db_str) if db_str else {}

                all_matches = live_matches + history_matches
                for match in all_matches:
                    for mp in (match.get("maps") or []):
                        self._process_map(mp, match, dotabuff_all)

            except Exception as e:
                log.error(f"Main loop error: {e}", exc_info=True)

            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    bot = BettorBot()
    bot.run()
