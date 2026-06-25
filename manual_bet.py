"""
Manual Bet  ·  manual bet by token_id (no slippage)
══════════════════════════════════════════════════════════════════
  Running via batch script:
    run_manual_bet.bat

  .env:
    PRIVATE_KEY=0x...
    FUNDER_ADDRESS=0x...
══════════════════════════════════════════════════════════════════
"""

from __future__ import annotations
import os, sys, json, urllib.request

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Settings ─────────────────────────────────────────────────────────────────
DRY_RUN       = False      # ← Set to False when you are ready to place real money bets
MIN_BET_USD   = 5       # minimum bet amount in USD

CLOB_BASE = "https://clob.polymarket.com"


def _get(url: str):
    try:
        req = urllib.request.Request(
            url, 
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=12) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"  HTTP error {url}: {e}")
        return None


def get_price(token_id: str) -> float | None:
    """Gets the current YES price (SELL side)"""
    data = _get(f"{CLOB_BASE}/price?token_id={token_id}&side=SELL")
    if data and "price" in data:
        return float(data["price"])
    return None


def place_bet(token_id: str, amount_usd: float = MIN_BET_USD):
    print()
    print("=" * 70)
    print(f"  MANUAL BET → Polymarket")
    print(f"  Token:   {token_id[:50]}{'...' if len(token_id) > 50 else ''}")
    print(f"  Amount:   ${amount_usd:.2f}")
    print(f"  Mode:   {'DRY RUN (test)' if DRY_RUN else '*** REAL BETS ***'}")
    print("=" * 70)

    print("\n  Getting current price...")
    price = get_price(token_id)
    if price is None:
        print("  ERROR: Failed to get token price.")
        return

    # No slippage — placing at market price
    order_price = round(min(price, 0.999), 4)          # maximum 0.999
    shares      = round(amount_usd / price, 2)
    cost_usd    = round(shares * order_price, 2)

    print(f"  Current price:     {price:.4f}  ({price*100:.1f}¢)")
    print(f"  Order price:      {order_price:.4f}")
    print(f"  Number of shares: {shares}")
    print(f"  Estimated cost: ${cost_usd:.2f}")

    if DRY_RUN:
        print("\n  [DRY RUN] Bet NOT placed — simulation only.")
        print("  To bet real money, change DRY_RUN = False")
        return

    # Real bet
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.constants import POLYGON
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
    except ImportError:
        print("  ERROR: py-clob-client is not installed.")
        print("  Install: pip install py-clob-client")
        return

    pk     = os.getenv("PRIVATE_KEY")
    funder = os.getenv("FUNDER_ADDRESS")

    if not pk or not funder:
        print("  ERROR: PRIVATE_KEY or FUNDER_ADDRESS not set in .env")
        return

    try:
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=pk, 
            chain_id=POLYGON,
            signature_type=2, 
            funder=funder,
        )
        client.set_api_creds(client.create_or_derive_api_creds())
        print("  CLOB client connected")
    except Exception as e:
        print(f"  ERROR connecting client: {e}")
        return

    confirm = input(f"\n  Place BUY of {shares} shares for ~${cost_usd:.2f}? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("  Cancelled.")
        return

    try:
        order_args   = OrderArgs(token_id=token_id, price=order_price, size=shares, side=BUY)
        signed_order = client.create_order(order_args)
        response     = client.post_order(signed_order, OrderType.GTC)
        print(f"\n  FULL POLYMARKET RESPONSE:")
        print(json.dumps(response, indent=2, ensure_ascii=False))
        order_id = response.get("orderID") or response.get("id", "?")
        print(f"\n  Order ID: {order_id}")
    except Exception as e:
        print(f"\n  ERROR placing bet: {e}")


if __name__ == "__main__":
    args = sys.argv[1:]
    
    if not args:
        print("\n  Usage:")
        print("    manual_bet.py <token_id> [amount_in_USD]")
        print("  Example:")
        print("    manual_bet.py 0x1234...abcd")
        print("    manual_bet.py 0x1234...abcd 8.0")
        sys.exit(1)

    token  = args[0].strip()
    amount = float(args[1]) if len(args) > 1 else MIN_BET_USD
    
    place_bet(token, amount)