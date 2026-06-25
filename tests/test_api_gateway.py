import sys
from pathlib import Path
from unittest.mock import MagicMock

# Remove 'main' from sys.modules if it is already there to avoid caching conflict
sys.modules.pop("main", None)

# Create a mock redis module and insert it into sys.modules
mock_redis = MagicMock()
sys.modules["redis"] = mock_redis

# Add services/api-gateway/src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "services" / "api-gateway" / "src"))

import pytest
from fastapi.testclient import TestClient

# Mock r_client inside main before importing main
import main  # type: ignore
main.r_client = MagicMock()

def test_api_get_state():
    # Setup mock values for Redis keys
    main.r_client.get.side_effect = lambda key: {
        "dota:live_matches": '[{"team1": "Liquid", "team2": "Spirit"}]',
        "dota:upcoming_matches": '[]',
        "dota:history_matches": '[]',
        "dota:sleep_info": '{"sleeping": false}',
        "dota:dotabuff": '{}',
        "dota:queue_stats": '{"waiting": 0, "pending": 0, "basic": 0, "full": 0}',
        "dota:updated_at": "2026-06-25 12:00:00"
    }.get(key)
    
    main.r_client.lrange.return_value = []

    client = TestClient(main.app)
    response = client.get("/api")
    assert response.status_code == 200
    data = response.json()
    assert data["updated_at"] == "2026-06-25 12:00:00"
    assert len(data["live_matches"]) == 1
    assert data["live_matches"][0]["team1"] == "Liquid"

def test_api_get_bets():
    main.r_client.lrange.return_value = ['{"id": "bet_1", "status": "placed"}']
    client = TestClient(main.app)
    response = client.get("/api/bets")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["id"] == "bet_1"
