"""
Polymarket Execution Microservice
═══════════════════════════════════════════════════════════════════
  Listens to the Redis queue polymarket:orders and places orders via py-clob-client.
  Saves execution logs to bets_log.jsonl and Redis list polymarket:bet_records.
═══════════════════════════════════════════════════════════════════
"""

import os
import sys
import time
import json
import logging
from pathlib import Path
from datetime import datetime
import redis
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DRY_RUN    = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "yes")

MIN_SHARES    = 2
MIN_BET_USD   = 2
MAX_PRICE_YES = 0.991
MIN_PRICE_YES = 0.55

LOG_FILE = Path("bets_log.jsonl")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("executor")

r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# ── Polymarket client ─────────────────────────────────────────────────────────
_poly_client = None

def _init_poly_client() -> bool:
    global _poly_client
    if DRY_RUN:
        log.info("Running in DRY_RUN mode, Polymarket CLOB client won't be initialized with real key.")
        return True
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.constants import POLYGON
        pk = os.getenv("PRIVATE_KEY")
        funder = os.getenv("FUNDER_ADDRESS")
        if not pk:
            log.error("PRIVATE_KEY not set in .env"); return False
        if not funder:
            log.error("FUNDER_ADDRESS not set in .env"); return False
        _poly_client = ClobClient(
            host="https://clob.polymarket.com",
            key=pk, chain_id=POLYGON,
            signature_type=2,
            funder=funder,
        )
        _poly_client.set_api_creds(_poly_client.create_or_derive_api_creds())
        log.info("Polymarket CLOB client is ready")
        return True
    except ImportError:
        log.error("py_clob_client is not installed: pip install py-clob-client")
        return False
    except Exception as e:
        log.error(f"Client initialization error: {e}")
        return False


def _get_price(token_id: str) -> float | None:
    if _poly_client and not DRY_RUN:
        try:
            return float(_poly_client.get_price(token_id, side="SELL")["price"])
        except Exception:
            pass
    # Fallback to direct HTTP request
    try:
        import requests
        r = requests.get(f"https://clob.polymarket.com/price?token_id={token_id}&side=SELL")
        if r.status_code == 200:
            return float(r.json()["price"])
    except Exception as e:
        log.error(f"Error getting price for token {token_id}: {e}")
    return None


def _log_bet(record: dict):
    record.setdefault("logged_at", datetime.now().isoformat())
    
    # Log to file
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        log.error(f"Failed to write to local log file: {e}")

    # Log to Redis for real-time dashboard updates
    try:
        r_client.lpush("polymarket:bet_records", json.dumps(record, ensure_ascii=False))
        r_client.ltrim("polymarket:bet_records", 0, 99) # Keep last 100 entries
        r_client.publish("polymarket:bet_placed", json.dumps(record, ensure_ascii=False))
    except Exception as e:
        log.error(f"Failed to write to Redis log: {e}")


def execute_bet_order(order: dict):
    market        = order["market"]
    outcome       = order["outcome"]
    resolver_name = order["resolver_name"]
    map_data      = order["map_data"]
    match_info    = order["match_info"]

    token_id = market["yes_token"] if outcome else market["no_token"]
    side_str = "YES" if outcome else "NO"

    record = {
        "question":      market.get("question", "?"),
        "condition_id":  market.get("condition_id", ""),
        "slug":          market.get("slug", ""),
        "market_url":    f"https://polymarket.com/event/{market.get('slug', '')}",
        "outcome":       side_str,
        "token_id":      token_id,
        "resolver":      resolver_name,
        "source":        "DLTV" if resolver_name in ("fb_radiant", "fb_dire") else "Dotabuff",
        "dry_run":       DRY_RUN,
        "team1":         match_info.get("team1", ""),
        "team2":         match_info.get("team2", ""),
        "radiant_team":  map_data.get("radiant_team", ""),
        "dire_team":     map_data.get("dire_team", ""),
        "fb_radiant":    bool(map_data.get("fb_radiant")),
        "fb_die":        bool(map_data.get("fb_dire")),
        "map_num":       map_data.get("num"),
        "map_status":    map_data.get("status", ""),
        "dota_match_id": map_data.get("dota2_match_id"),
        "db_phase":      map_data.get("db_phase"),
        "dltv_duration": map_data.get("duration", ""),
        "db_duration":   map_data.get("db_duration", ""),
        "event_title":   market.get("event_title", ""),
    }

    price = _get_price(token_id)
    if price is None:
        record["status"] = "skip_no_price"
        _log_bet(record)
        return

    record["price"] = price

    if price >= MAX_PRICE_YES:
        record["status"] = "skip_price_too_high"
        _log_bet(record)
        return

    if outcome and price < MIN_PRICE_YES:
        record["status"] = "skip_price_too_low"
        _log_bet(record)
        return

    order_price = round(min(price, 0.999), 4)
    size        = round(MIN_BET_USD / price, 2)
    if size < MIN_SHARES:
        size = MIN_SHARES
    cost_usd    = round(size * order_price, 2)

    record.update({"order_price": order_price, "shares": size, "cost_usd": cost_usd})

    if DRY_RUN:
        log.info(f"\n  {'─'*65}\n  [DRY RUN] 🎯 {side_str} — {record['question']}\n"
                 f"  Match: {match_info.get('team1','?')} vs {match_info.get('team2','?')} game {map_data.get('num')}\n"
                 f"  Price: {price:.4f} → order {order_price:.4f}\n"
                 f"  Amount: ~${cost_usd:.2f} ({size} shares)\n  Resolver: {resolver_name}\n  {'─'*65}")
        record["status"] = "dry_run"
        _log_bet(record)
        return

    # Real trade
    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
        order_args   = OrderArgs(token_id=token_id, price=order_price, size=size, side=BUY)
        signed_order = _poly_client.create_order(order_args)
        response     = _poly_client.post_order(signed_order, OrderType.GTC)

        success       = response.get("success", False)
        error_msg     = response.get("errorMsg", "")
        order_id      = response.get("orderID") or response.get("id", "?")
        status        = response.get("status", "")
        taking_amount = response.get("takingAmount", "")

        record["order_id"]     = order_id
        record["raw_response"] = response

        if not success or error_msg:
            log.error(
                f"❌ ORDER REJECTED: {side_str} | {record['question'][:50]} | ${cost_usd:.2f}\n"
                f"   error={error_msg!r}  status={status!r}  response={response}"
            )
            record["status"] = f"rejected:{error_msg or status}"

        elif status == "matched" and taking_amount:
            tx = response.get("transactionsHashes", [])
            log.info(
                f"✅ BET EXECUTED: {side_str} | {record['question'][:50]} | ${cost_usd:.2f}\n"
                f"   order_id={order_id}  shares={taking_amount}  tx={tx[0] if tx else '—'}"
            )
            record["status"] = "placed"

        elif status in ("live", "delayed"):
            log.warning(
                f"⏳ ORDER IN OPEN ORDERS (not executed): {side_str} | {record['question'][:50]} | ${cost_usd:.2f}\n"
                f"   order_id={order_id}  price={order_price} — no counter-order at this price"
            )
            record["status"] = "open_order"

        else:
            log.warning(
                f"⚠️  UNKNOWN ORDER STATUS: {side_str} | {record['question'][:50]}\n"
                f"   status={status!r}  response={response}"
            )
            record["status"] = f"unknown:{status}"

    except Exception as e:
        log.error(f"Bet execution error [{resolver_name}]: {e}")
        record["status"] = f"error:{e}"

    _log_bet(record)


def main():
    log.info("═══════════════════════════════════════════════════════════════════")
    log.info("  🎯 Polymarket Execution Microservice is starting...")
    log.info(f"  Redis Host: {REDIS_HOST}:{REDIS_PORT}")
    log.info(f"  Mode:       {'DRY RUN (Simulated Bets)' if DRY_RUN else 'LIVE BETS'}")
    log.info("═══════════════════════════════════════════════════════════════════")

    if not _init_poly_client():
        if not DRY_RUN:
            log.error("Failed to initialize Polymarket client. Exiting.")
            sys.exit(1)

    log.info("Waiting for order placement requests on Redis queue polymarket:orders...")

    while True:
        try:
            # Blocking pop from the Redis queue (timeouts in 5 seconds if empty)
            res = r_client.blpop("polymarket:orders", timeout=5)
            if res:
                _, order_str = res
                order = json.loads(order_str)
                log.info(f"Received order: {order['resolver_name']} | Outcome: {order['outcome']}")
                execute_bet_order(order)
        except KeyboardInterrupt:
            log.info("Exiting on Ctrl+C")
            break
        except Exception as e:
            log.error(f"Error in execution worker loop: {e}", exc_info=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
