"""
Dota 2 API Gateway Microservice
═══════════════════════════════════════════════════════════════════
  FastAPI server that reads state from Redis and serves the unified /api endpoint.
  Compatible with the original dashboard.html interface.
═══════════════════════════════════════════════════════════════════
"""

import os
import json
try:
    import redis
except ImportError:
    # Fall back to fakeredis for development/testing if redis-py is not installed.
    try:
        import fakeredis as redis  # type: ignore
    except ImportError:
        redis = None
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PORT = int(os.getenv("API_PORT", "5000"))

app = FastAPI(title="Dota 2 Bot API Gateway")

# Enable CORS for dashboard.html
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if redis is None:
    raise RuntimeError(
        "The 'redis' package is required. Install 'redis' or 'fakeredis' in your environment."
    )

r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


@app.get("/", response_class=HTMLResponse)
def read_dashboard():
    try:
        with open("/app/dashboard.html", "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"<h3>Error loading dashboard: {e}</h3>"


@app.get("/dashboard.css")
def read_css():
    try:
        return FileResponse("/app/dashboard.css")
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"CSS not found: {e}")


@app.get("/api")
def get_state():
    try:
        live_matches_str     = r_client.get("dota:live_matches")
        upcoming_matches_str = r_client.get("dota:upcoming_matches")
        history_matches_str  = r_client.get("dota:history_matches")
        sleep_info_str       = r_client.get("dota:sleep_info")
        dotabuff_str         = r_client.get("dota:dotabuff")
        queue_stats_str      = r_client.get("dota:queue_stats")
        updated_at           = r_client.get("dota:updated_at") or ""
        
        errors = r_client.lrange("dota:errors", 0, -1) or []

        state = {
            "updated_at":       updated_at,
            "live_matches":     json.loads(live_matches_str) if live_matches_str else [],
            "upcoming_matches": json.loads(upcoming_matches_str) if upcoming_matches_str else [],
            "history_matches":  json.loads(history_matches_str) if history_matches_str else [],
            "sleep_info":       json.loads(sleep_info_str) if sleep_info_str else {"sleeping": False},
            "dotabuff":         json.loads(dotabuff_str) if dotabuff_str else {},
            "queue_stats":      json.loads(queue_stats_str) if queue_stats_str else {"waiting": 0, "pending": 0, "basic": 0, "full": 0},
            "errors":           errors,
        }
        return state
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")


@app.get("/api/bets")
def get_bets():
    """Returns the history of placed bets from Redis."""
    try:
        bets = r_client.lrange("polymarket:bet_records", 0, -1) or []
        return [json.loads(b) for b in bets]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching bets: {e}")


@app.get("/health")
def health():
    try:
        r_client.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception as e:
        return {"status": "error", "redis": f"disconnected: {e}"}


if __name__ == "__main__":
    import uvicorn
    print(f"Starting API Gateway on port {PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
