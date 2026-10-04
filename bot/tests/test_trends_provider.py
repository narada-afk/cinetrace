"""GoogleTrendsProvider: RSS parsing + graceful degradation on failure."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from engine.config import TrendConfig
from engine.trends.provider import GoogleTrendsProvider

_SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:ht="https://trends.google.com/trending/rss">
<channel>
<title>Daily Search Trends</title>
<item>
<title>Test Actor</title>
<ht:approx_traffic>100,000+</ht:approx_traffic>
<ht:news_item>
<ht:news_item_title>Test Actor announces new film</ht:news_item_title>
</ht:news_item>
</item>
<item>
<title>Cricket Match</title>
</item>
</channel>
</rss>"""


@pytest.mark.asyncio
async def test_parses_titles_and_context_from_feed():
    provider = GoogleTrendsProvider(TrendConfig())
    with patch("engine.trends.provider.httpx.AsyncClient") as ac:
        client = AsyncMock()
        resp = MagicMock()
        resp.text = _SAMPLE_RSS
        resp.raise_for_status = MagicMock()
        client.get = AsyncMock(return_value=resp)
        ac.return_value.__aenter__.return_value = client

        candidates = await provider.get_trends()

    assert [c.title for c in candidates] == ["Test Actor", "Cricket Match"]
    assert candidates[0].approx_traffic == "100,000+"
    assert candidates[0].news_snippets == ["Test Actor announces new film"]
    assert candidates[1].approx_traffic is None
    assert candidates[1].news_snippets == []


@pytest.mark.asyncio
async def test_fetch_failure_returns_empty_list_not_an_exception():
    provider = GoogleTrendsProvider(TrendConfig())
    with patch("engine.trends.provider.httpx.AsyncClient") as ac:
        client = AsyncMock()
        client.get = AsyncMock(side_effect=Exception("network down"))
        ac.return_value.__aenter__.return_value = client

        candidates = await provider.get_trends()

    assert candidates == []


@pytest.mark.asyncio
async def test_malformed_feed_returns_empty_list_not_an_exception():
    provider = GoogleTrendsProvider(TrendConfig())
    with patch("engine.trends.provider.httpx.AsyncClient") as ac:
        client = AsyncMock()
        resp = MagicMock()
        resp.text = "<not valid xml"
        resp.raise_for_status = MagicMock()
        client.get = AsyncMock(return_value=resp)
        ac.return_value.__aenter__.return_value = client

        candidates = await provider.get_trends()

    assert candidates == []
