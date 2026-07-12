"""Twitter generator tests with a mocked Anthropic client."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from engine.generators import all_generators, get_generator
from engine.models import Platform


def _mock_response(text: str):
    msg = MagicMock()
    msg.content = [MagicMock(text=text)]
    return msg


@pytest.mark.asyncio
async def test_valid_tweet_returned(duo_insight):
    gen = get_generator(Platform.TWITTER)
    good = "Mohanlal & Priyadarshan: 44 films together, silence since 2014.\n#Mohanlal"
    with patch("engine.generators.twitter._client") as client:
        client.messages.create = AsyncMock(return_value=_mock_response(good))
        item = await gen.generate(duo_insight, insight_id=99)
    assert item is not None
    assert item.validated and item.text == good
    assert item.insight_id == 99 and item.platform == Platform.TWITTER
    assert item.media_ref == "mohanlal"


@pytest.mark.asyncio
async def test_hallucinated_number_retried_then_discarded(duo_insight):
    gen = get_generator(Platform.TWITTER)
    bad = "Mohanlal & Priyadarshan made 57 films together. #Mohanlal"
    with patch("engine.generators.twitter._client") as client:
        client.messages.create = AsyncMock(return_value=_mock_response(bad))
        item = await gen.generate(duo_insight, insight_id=1)
    assert item is None
    assert client.messages.create.call_count == 2  # one retry with feedback


@pytest.mark.asyncio
async def test_retry_can_recover(duo_insight):
    gen = get_generator(Platform.TWITTER)
    bad = "Mohanlal & Priyadarshan made 57 films together."
    good = "Mohanlal & Priyadarshan: 44 films together."
    with patch("engine.generators.twitter._client") as client:
        client.messages.create = AsyncMock(
            side_effect=[_mock_response(bad), _mock_response(good)])
        item = await gen.generate(duo_insight, insight_id=1)
    assert item is not None and item.text == good


def test_platform_registry_has_stubs():
    gens = all_generators()
    assert {Platform.TWITTER, Platform.INSTAGRAM, Platform.LINKEDIN} <= set(gens)
