"""
analyze_bets.py — bet statistics from bets_log.jsonl
Run: python analyze_bets.py [file]
"""
import json, sys
from pathlib import Path
from collections import defaultdict

def main():
    LOG_FILE = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("bets_log.jsonl")
    if not LOG_FILE.exists():
        print(f"File {LOG_FILE} not found."); sys.exit(1)

    records = []
    with LOG_FILE.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try: records.append(json.loads(line))
                except Exception: pass

    if not records:
        print("No records."); sys.exit(0)

    SEP = "═" * 65
    print(SEP)
    print(f"  Bet statistics  ({len(records)} records)  {LOG_FILE}")
    print(SEP)

    by_status  = defaultdict(int)
    by_type    = defaultdict(list)
    by_outcome = defaultdict(int)
    total_cost = 0.0

    for r in records:
        st = r.get("status", "?")
        by_status[st] += 1
        by_type[r.get("resolver", "?")].append(r)
        by_outcome[r.get("outcome", "?")] += 1
        if st in ("dry_run", "placed") and "cost_usd" in r:
            total_cost += r["cost_usd"]

    print()
    print(f"  By status:")
    for st, cnt in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f"    {st:<25} {cnt:>4}")

    print()
    print(f"  Total YES: {by_outcome.get('YES',0)}  NO: {by_outcome.get('NO',0)}")
    print(f"  Total bet amount: ${total_cost:.2f}")

    print()
    print(f"  {'Bet type':<22} {'Qty':>6}  {'YES':>5}  {'NO':>5}  {'Avg price':>9}")
    print(f"  {'─'*55}")
    for bt, recs in sorted(by_type.items()):
        yes = sum(1 for r in recs if r.get("outcome") == "YES")
        no  = sum(1 for r in recs if r.get("outcome") == "NO")
        prices = [r["price"] for r in recs if "price" in r]
        avg = sum(prices)/len(prices) if prices else 0
        print(f"  {bt:<22} {len(recs):>6}  {yes:>5}  {no:>5}  {avg:>9.4f}")

    print()
    print(f"  Last 15 records:")
    print(f"  {'─'*65}")
    for r in records[-15:]:
        ts   = (r.get("logged_at") or "")[:19]
        bt   = (r.get("resolver") or "?")[:18]
        out  = r.get("outcome", "?")
        prc  = f"{r['price']:.3f}" if "price" in r else " n/a"
        cost = f"${r['cost_usd']:.2f}" if "cost_usd" in r else "    "
        st   = (r.get("status") or "?")[:14]
        t1   = (r.get("team1") or "")[:10]
        t2   = (r.get("team2") or "")[:10]
        mn   = r.get("map_num", "?")
        print(f"  {ts}  {bt:<19} {out:<4} {prc:>6}  {cost:>6}  [{st}]  {t1} vs {t2} m{mn}")

    print()
    print(SEP)

if __name__ == "__main__":
    main()