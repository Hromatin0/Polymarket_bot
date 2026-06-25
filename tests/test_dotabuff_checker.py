import pytest
from dotabuff_checker import _classify, _body_preview

def test_classify_ok_real_match():
    status = 200
    body = "Here is the match overview page with radiant victory!"
    desc, is_blocked = _classify(status, body)
    assert "OK" in desc
    assert not is_blocked

def test_classify_ok_dotabuff_responded():
    status = 200
    body = "Welcome to dotabuff"
    desc, is_blocked = _classify(status, body)
    assert "OK" in desc
    assert not is_blocked

def test_classify_cf_challenge():
    status = 200
    body = "Checking your browser... just a moment..."
    desc, is_blocked = _classify(status, body)
    assert "CF challenge" in desc
    assert is_blocked

def test_classify_404_real():
    status = 404
    body = "This page was not found on dotabuff"
    desc, is_blocked = _classify(status, body)
    assert "404 real" in desc
    assert not is_blocked

def test_classify_404_cf_block():
    status = 404
    body = "Some small response body"
    desc, is_blocked = _classify(status, body)
    assert "CF block" in desc
    assert is_blocked

def test_classify_403_forbidden():
    status = 403
    body = "Access forbidden"
    desc, is_blocked = _classify(status, body)
    assert "403 Forbidden" in desc
    assert is_blocked

def test_classify_503_service_unavailable():
    status = 503
    body = "Service temporarily unavailable"
    desc, is_blocked = _classify(status, body)
    assert "503 Service Unavailable" in desc
    assert is_blocked

def test_classify_429_rate_limited():
    status = 429
    body = "Too many requests"
    desc, is_blocked = _classify(status, body)
    assert "Rate Limited" in desc
    assert is_blocked

def test_body_preview():
    html_body = "<div>Hello <b>world</b>! This is a long body content that we want to test for body preview.</div>"
    preview = _body_preview(html_body)
    assert "Hello world !" in preview
    assert "<div" not in preview
    assert "</b>" not in preview
