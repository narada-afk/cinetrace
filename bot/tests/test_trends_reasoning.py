"""
Reasoning step: trend + resolved actor's Cinetrace facts -> one LLM call
that may only pick a key from a Python-computed whitelist, never invent a
number. Mirrors the mocking style of test_generators.py.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from engine.config import TrendConfig
from engine.trends.provider import TrendCandidate
from engine.trends.reasoning import find_trend_connection
from engine.trends.resolver import ResolvedEntity

_MOVIES = [
    {"title": "Alpha", "release_year": 2010, "box_office": 50,  "industry": "Tamil"},
    {"title": "Beta",  "release_year": 2012, "box_office": 120, "industry": "Tamil"},
    {"title": "Gamma", "release_year": 2015, "box_office": 150, "industry": "Tamil"},
]
_PROFILE = {
    "movies": _MOVIES,
    "collaborators": [{"actor": "Co-star X", "films": 10}],
    "directors": [{"director": "Director Y", "films": 5}],
}
_CANDIDATE = TrendCandidate(
    title="Test Actor new film", approx_traffic="100K+",
    news_snippets=["Test Actor announces new film with Director Y"],
)
_RESOLVED = ResolvedEntity(actor_id=1, name="Test Actor", slug="test-actor", industry="Tamil")


def _mock_response(payload: str):
    msg = MagicMock()
    msg.content = [MagicMock(text=payload)]
    return msg


@pytest.mark.asyncio
async def test_relevant_connection_uses_whitelisted_value_not_llm_value():
    """The model may only choose a key; the numeric value attached to the
    returned TrendConnection must come from Python's own computation, even
    if the model's JSON tried to assert a different number somewhere."""
    reply = '{"relevant": true, "fact_key": "films_100cr", ' \
            '"why_trending": "new film announced", "tweet_angle": "ties record to news", ' \
            '"confidence": 80}'
    with patch("engine.trends.reasoning._client") as client:
        client.messages.create = AsyncMock(return_value=_mock_response(reply))
        conn = await find_trend_connection(_CANDIDATE, _RESOLVED, _PROFILE, TrendConfig())

    assert conn is not None
    assert conn.fact_key == "films_100cr"
    assert conn.metric_value == 2  # Beta (120) + Gamma (150) — computed, not LLM-supplied
    assert conn.confidence == pytest.approx(0.8)


@pytest.mark.asyncio
async def test_not_relevant_skips():
    reply = '{"relevant": false, "fact_key": null, "why_trending": "", "tweet_angle": "", "confidence": 0}'
    with patch("engine.trends.reasoning._client") as client:
        client.messages.create = AsyncMock(return_value=_mock_response(reply))
        conn = await find_trend_connection(_CANDIDATE, _RESOLVED, _PROFILE, TrendConfig())

    assert conn is None


@pytest.mark.asyncio
async def test_hallucinated_fact_key_is_rejected():
    """The model picks a key that isn't in the whitelist — must be dropped,
    never passed through as if it were a verified fact."""
    reply = '{"relevant": true, "fact_key": "box_office_total_ever", ' \
            '"why_trending": "x", "tweet_angle": "y", "confidence": 90}'
    with patch("engine.trends.reasoning._client") as client:
        client.messages.create = AsyncMock(return_value=_mock_response(reply))
        conn = await find_trend_connection(_CANDIDATE, _RESOLVED, _PROFILE, TrendConfig())

    assert conn is None


@pytest.mark.asyncio
async def test_llm_failure_degrades_to_skip():
    with patch("engine.trends.reasoning._client") as client:
        client.messages.create = AsyncMock(side_effect=Exception("rate limited"))
        conn = await find_trend_connection(_CANDIDATE, _RESOLVED, _PROFILE, TrendConfig())

    assert conn is None


@pytest.mark.asyncio
async def test_no_movies_skips_without_calling_llm():
    empty_profile = {"movies": [], "collaborators": [], "directors": []}
    with patch("engine.trends.reasoning._client") as client:
        client.messages.create = AsyncMock()
        conn = await find_trend_connection(_CANDIDATE, _RESOLVED, empty_profile, TrendConfig())

    assert conn is None
    client.messages.create.assert_not_called()
