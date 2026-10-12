"""Twitter generator tests with a mocked Anthropic client."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from engine.generators import all_generators, get_generator
from engine.models import Platform


def _mock_response(text: str):
    msg = MagicMock()
    msg.content = [MagicMock(text=text)]
    return msg


# The generator now emits a hook + ===REPLY=== + reveal.
_GOOD = (
    "Two actors.\nOne of cinema's great partnerships.\n\nThen it just stopped.\n↓"
    "\n===REPLY===\n"
    "Mohanlal & Priyadarshan.\n44 films together.\nNothing since 2014.\n#Mohanlal"
)


@pytest.mark.asyncio
async def test_valid_tweet_returned(duo_insight):
    gen = get_generator(Platform.TWITTER)
    with patch("engine.generators.twitter._client") as client:
        client.messages.create = AsyncMock(return_value=_mock_response(_GOOD))
        item = await gen.generate(duo_insight, insight_id=99)
    assert item is not None
    assert item.validated
    assert item.text.endswith("↓")                 # hook ends on the cliffhanger
    assert item.reply_text and "44 films" in item.reply_text
    assert "===REPLY===" not in item.text and "===REPLY===" not in item.reply_text
    assert item.insight_id == 99 and item.platform == Platform.TWITTER
    assert item.media_ref == "mohanlal"


@pytest.mark.asyncio
async def test_missing_reply_separator_retried(duo_insight):
    gen = get_generator(Platform.TWITTER)
    one_part = "Mohanlal & Priyadarshan: 44 films, nothing since 2014."
    with patch("engine.generators.twitter._client") as client:
        client.messages.create = AsyncMock(return_value=_mock_response(one_part))
        item = await gen.generate(duo_insight, insight_id=1)
    assert item is None
    assert client.messages.create.call_count == 2  # retried for the missing split


@pytest.mark.asyncio
async def test_hallucinated_number_retried_then_discarded(duo_insight):
    gen = get_generator(Platform.TWITTER)
    bad = "Two actors.\n↓\n===REPLY===\nMohanlal & Priyadarshan made 57 films. #Mohanlal"
    with patch("engine.generators.twitter._client") as client:
        client.messages.create = AsyncMock(return_value=_mock_response(bad))
        item = await gen.generate(duo_insight, insight_id=1)
    assert item is None
    assert client.messages.create.call_count == 2  # one retry with feedback


@pytest.mark.asyncio
async def test_retry_can_recover(duo_insight):
    gen = get_generator(Platform.TWITTER)
    bad = "Two actors.\n↓\n===REPLY===\nMohanlal & Priyadarshan made 57 films."
    with patch("engine.generators.twitter._client") as client:
        client.messages.create = AsyncMock(
            side_effect=[_mock_response(bad), _mock_response(_GOOD)])
        item = await gen.generate(duo_insight, insight_id=1)
    assert item is not None and item.reply_text and "44 films" in item.reply_text


def test_platform_registry_has_stubs():
    gens = all_generators()
    assert {Platform.TWITTER, Platform.INSTAGRAM, Platform.LINKEDIN} <= set(gens)
