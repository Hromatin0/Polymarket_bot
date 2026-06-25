# Dota 2 → Polymarket Auto-Bettor (Microservices Edition)

### Core Idea

Polymarket markets for in-game events (First Blood, Rampage, Roshan kill, etc.) resolve **after the match ends** — but the outcome is already determined the moment it happens in-game, often 10–40 minutes earlier.

The bot exploits this lag:

1. An event occurs in the game (e.g. a Rampage at 28:00).
2. The Polymarket market still shows YES at ~98¢ because it hasn't resolved yet.
3. The bot detects the event via DLTV/Dotabuff and immediately buys YES.
4. The market resolves to $1.00 shortly after the series ends.

**The bet is effectively on a known outcome.** The only risk is the spread — markets rarely sit above 95–99¢ even on confirmed events, so the edge per bet is small (1–5%), but it's close to risk-free when timed correctly.

---

## Architecture (Microservices)

The bot is designed as a modern, decoupled **microservices-based architecture** coordinated via a central **Redis** instance. Each microservice handles a specific domain of the data pipelines and execution workflow.

```
┌──────────────────┐      ┌──────────────────┐
│ dota-dltv-scraper│      │ dota-db-enricher │
└────────┬─────────┘      └────────┬─────────┘
         │ (write matches)         │ (write stats)
         ▼                         ▼
┌────────────────────────────────────────────┐
│                  REDIS                     │
└────────────────┬───────────────────────────┘
         │ (read)                  ▲ (write api data)
         ▼                         │
┌──────────────────┐      ┌────────┴─────────┐
│  betting-engine  │      │   api-gateway    │◄─── dashboard.html
└────────┬─────────┘      └──────────────────┘
         │ (push orders)
         ▼
┌──────────────────┐
│polymarket-exec   │◄─── (uses Polygon Wallet / private key)
└──────────────────┘
```

### Microservices

1. **`dota-dltv-scraper`**: Uses Playwright to monitor `dltv.org/matches` for active/upcoming live Dota 2 series, and saves lists of active series and upcoming match schedules to Redis.
2. **`dota-db-enricher`**: Queries Dotabuff (using a rotating user-agent proxy & Playwright fallbacks) to scrape detailed in-game stats—such as match duration, kills, multi-kills (Rampage/Ultra Kill), Roshan kills, and barracks status—saving them to Redis.
3. **`betting-engine`**: Analyzes match schedules and Dotabuff stats. For each match, it resolves betting conditions (First Blood, Ends in Daytime, Rampages, etc.), checks for open Polymarket conditions, and triggers buy/sell order payloads to a Redis queue (`polymarket:orders`).
4. **`polymarket-executor`**: Reads order payloads from the Redis queue, verifies order limits/slippage, signs the transaction with your Polygon private key, executes the bet on Polymarket CLOB, and writes log records to `bets_log.jsonl`.
5. **`api-gateway`**: A FastAPI application that reads the unified bot state from Redis and serves the `/api` and `/api/bets` JSON endpoints used by the dashboard.

---

## Core Root Files

- **`dashboard.html` / `dashboard.css`**: Live, beautiful web interface polling `http://localhost:5000/api` every 3 seconds to display active games, scores, and real-time outcomes of all tracked bet criteria.
- **`analyze_bets.py`**: A CLI analytics utility to extract statistics from `bets_log.jsonl`, such as quantities, Yes/No split, average purchase price, total USD spent, and last 15 actions.
- **`manual_bet.py`**: A helper utility to place an instant, manual buy order for any specific token ID directly.
- **`dotabuff_checker.py`**: A development utility to check if Dotabuff is currently rate-limiting or blocking requests.
- **`dev_liveserver.py`**: Auto-detects local host IP and configures VS Code Live Server settings.

---

## Bet Types

| Key | Condition | Data source |
|---|---|---|
| `fb_radiant` | Radiant gets First Blood | DLTV live stream data |
| `fb_dire` | Dire gets First Blood | DLTV live stream data |
| `ends_in_daytime` | Match ends during a Dota day phase (every 5 min cycle) | Dotabuff duration |
| `any_ultra_kill` | Any player gets an Ultra Kill (4-kill streak) | Dotabuff /kills |
| `any_rampage` | Any player gets a Rampage (5-kill streak) | Dotabuff /kills |
| `both_roshan` | Both teams kill Roshan at least once | Dotabuff /objectives |
| `both_barracks` | Both teams destroy at least one set of barracks | Dotabuff /objectives |

Each bet type can be enabled or disabled globally in the `betting-engine` configuration:

```python
BET_TYPE_ENABLED: dict[str, bool] = {
    "any_rampage":     True,
    "any_ultra_kill":  True,
    "both_roshan":     False,   # ← disabled
    "both_barracks":   True,
    ...
}
```

---

## Running with Docker Compose (Recommended)

The easiest way to launch the entire multi-service suite is using Docker Compose:

1. **Setup Environment**: Copy `.env.example` to `.env` and configure your credentials:
   ```env
   PRIVATE_KEY=0x...
   FUNDER_ADDRESS=0x...
   POLY_KEY=your_poly_key_here
   POLY_SECRET=your_poly_secret_here
   POLY_PASSPHRASE=your_poly_passphrase_here
   ```

2. **Launch Services**:
   ```bash
   docker-compose up --build -d
   ```

3. **Open Dashboard**:
   Open `http://localhost:5000/` or launch `dashboard.html` locally using Live Server.

---

## Running Tests

The application has a robust, clean unit test suite using `pytest` that covers all the critical decision-making logic of the bot, the microservice API endpoints, and the Dotabuff response classification engines.

To run the test suite:

1. Ensure requirements are installed:
   ```bash
   pip install pytest pytest-cov fastapi httpx
   ```
2. Run the tests with coverage tracking:
   ```bash
   python -m pytest --cov=.
   ```

---

## Anti-Ban & Smart Sleep

- **Layered Scraping**: Rotation of browser profiles, randomized headers, and headless Playwright fallbacks bypass Cloudflare on Dotabuff.
- **Smart Sleep**: When no live games are active, the bot schedules itself to wake up 90 seconds before the next scheduled match, performing lightweight periodic checks in between to handle calendar updates.

---

## Disclaimer

This project is for educational purposes. Automated betting carries financial risk. Always test with `DRY_RUN = True` before using real funds. You are responsible for compliance with the laws of your jurisdiction regarding prediction markets.
