import sys
from pathlib import Path

# Remove 'main' from sys.modules if it is already there to avoid caching conflict
sys.modules.pop("main", None)

# Add services/betting-engine/src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "services" / "betting-engine" / "src"))

import pytest
from main import _is_daytime, _score_market, _extract_tokens_from_list, _market_is_open  # type: ignore

def test_is_daytime():
    assert _is_daytime("") is None
    assert _is_daytime("invalid_format") is None
    # 04:30 = 270 seconds. 270 // 300 = 0 (day) -> True
    assert _is_daytime("4:30") is True
    # 05:15 = 315 seconds. 315 // 300 = 1 (night) -> False
    assert _is_daytime("5:15") is False
    # 10:05 = 605 seconds. 605 // 300 = 2 (day) -> True
    assert _is_daytime("10:05") is True

def test_market_is_open():
    # Active is default True, closed is default False
    assert _market_is_open({"active": True, "closed": False}) is True
    assert _market_is_open({"active": False, "closed": False}) is False
    assert _market_is_open({"active": True, "closed": True}) is False

def test_extract_tokens_from_list_normal():
    tokens = [
        {"outcome": "Yes", "token_id": "yes_123"},
        {"outcome": "No", "token_id": "no_456"}
    ]
    yes_tok, no_tok = _extract_tokens_from_list(tokens, bet_type="any_rampage")
    assert yes_tok == "yes_123"
    assert no_tok == "no_456"

def test_extract_tokens_from_list_first_blood():
    tokens = [
        {"outcome": "Team Liquid", "token_id": "team_liquid_tok"},
        {"outcome": "Team Spirit", "token_id": "team_spirit_tok"}
    ]
    # For fb_radiant, Team Liquid is radiant (yes) and Team Spirit is dire (no)
    yes_tok, no_tok = _extract_tokens_from_list(
        tokens,
        bet_type="fb_radiant",
        radiant_team="Team Liquid",
        dire_team="Team Spirit"
    )
    assert yes_tok == "team_liquid_tok"
    assert no_tok == "team_spirit_tok"

def test_score_market_closed():
    market = {"active": False, "closed": True}
    score = _score_market(market, map_num=1, bet_type="any_rampage")
    assert score == 0

def test_score_market_rampage():
    market = {
        "active": True,
        "closed": False,
        "slug": "dota-2-rampage-game-1",
        "question": "Will there be a Rampage in Game 1?"
    }
    # Matches "game-?1" and rampage keywords
    score = _score_market(market, map_num=1, bet_type="any_rampage")
    assert score > 0

    # Wrong game map_num
    score_wrong_map = _score_market(market, map_num=2, bet_type="any_rampage")
    assert score_wrong_map == 0
