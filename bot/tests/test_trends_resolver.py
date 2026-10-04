"""Resolver: trend title -> Cinetrace actor, via the existing /actors/search."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from engine.trends.resolver import resolve_trend_to_actor


def _resp(status: int, data):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = data
    return r


@pytest.mark.asyncio
async def test_exact_name_match_resolves_directly():
    """A cinema-relevant trend whose title names the actor outright."""
    with patch("engine.trends.resolver.httpx.AsyncClient") as ac:
        client = AsyncMock()
        client.get = AsyncMock(return_value=_resp(
            200, [{"id": 1, "name": "Rajinikanth", "industry": "Tamil"}]))
        ac.return_value.__aenter__.return_value = client

        resolved = await resolve_trend_to_actor("Rajinikanth")

    assert resolved is not None
    assert resolved.actor_id == 1
    assert resolved.name == "Rajinikanth"
    assert resolved.slug == "rajinikanth"


@pytest.mark.asyncio
async def test_non_cinema_trend_has_no_match_at_any_step():
    """A trend with no cinema relevance: every search returns empty, and
    the nickname LLM fallback also finds nothing — must resolve to None,
    never force a match just to produce a tweet."""
    with patch("engine.trends.resolver.httpx.AsyncClient") as ac, \
         patch("engine.trends.resolver._client") as claude:
        client = AsyncMock()
        client.get = AsyncMock(return_value=_resp(200, []))
        ac.return_value.__aenter__.return_value = client

        msg = MagicMock()
        msg.content = [MagicMock(text='{"actor_name": null}')]
        claude.messages.create = AsyncMock(return_value=msg)

        resolved = await resolve_trend_to_actor("India vs Australia 3rd ODI")

    assert resolved is None


@pytest.mark.asyncio
async def test_nickname_resolves_via_llm_fallback_then_search():
    """"Thalaivar" doesn't literally contain "Rajinikanth" — direct search
    and token search both miss, so the nickname LLM call must run, and its
    answer must be re-verified against Cinetrace before being trusted."""
    with patch("engine.trends.resolver.httpx.AsyncClient") as ac, \
         patch("engine.trends.resolver._client") as claude:
        client = AsyncMock()
        client.get = AsyncMock(side_effect=[
            _resp(200, []),  # step 1: raw title "Thalaivar" — no hit
            _resp(200, []),  # step 2: token search on "Thalaivar" itself — no hit
            _resp(200, [{"id": 1, "name": "Rajinikanth", "industry": "Tamil"}]),  # step 3: nickname search
        ])
        ac.return_value.__aenter__.return_value = client

        msg = MagicMock()
        msg.content = [MagicMock(text='{"actor_name": "Rajinikanth"}')]
        claude.messages.create = AsyncMock(return_value=msg)

        resolved = await resolve_trend_to_actor("Thalaivar")

    assert resolved is not None
    assert resolved.name == "Rajinikanth"


@pytest.mark.asyncio
async def test_nickname_call_logs_real_usage():
    """record_llm_call must be invoked with the response's real token
    counts — this is what makes engine.db.trend_llm_usage_summary() report
    actual spend instead of an estimate."""
    with patch("engine.trends.resolver.httpx.AsyncClient") as ac, \
         patch("engine.trends.resolver._client") as claude, \
         patch("engine.db.record_llm_call") as record:
        client = AsyncMock()
        # lowercase title: step 1 (raw title) is the only search call —
        # step 2's capitalised-word regex matches nothing, so it never fires
        client.get = AsyncMock(return_value=_resp(200, []))
        ac.return_value.__aenter__.return_value = client

        msg = MagicMock()
        msg.content = [MagicMock(text='{"actor_name": null}')]
        msg.usage = MagicMock(input_tokens=142, output_tokens=12)
        claude.messages.create = AsyncMock(return_value=msg)

        await resolve_trend_to_actor("trending now")

    record.assert_called_once_with("nickname", "claude-haiku-4-5-20251001", 142, 12)


@pytest.mark.asyncio
async def test_search_failure_resolves_to_none_not_an_exception():
    """Cinetrace backend unreachable — resolver must degrade to "no match",
    not raise and crash the scheduled run."""
    with patch("engine.trends.resolver.httpx.AsyncClient") as ac, \
         patch("engine.trends.resolver._client") as claude:
        client = AsyncMock()
        client.get = AsyncMock(side_effect=Exception("connection refused"))
        ac.return_value.__aenter__.return_value = client

        msg = MagicMock()
        msg.content = [MagicMock(text='{"actor_name": null}')]
        claude.messages.create = AsyncMock(return_value=msg)

        resolved = await resolve_trend_to_actor("anything")

    assert resolved is None
