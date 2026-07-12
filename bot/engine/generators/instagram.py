"""
Instagram caption generator (stub integration — output stored in
content_items, no posting pipeline yet).
"""

from __future__ import annotations

import json

import anthropic

from config import ANTHROPIC_API_KEY
from engine.generators._validator import validate
from engine.generators.base import ContentGenerator, register
from engine.generators.twitter import STYLE_HINTS
from engine.models import ContentItem, Insight, Platform
from engine.shared.logging import get_logger

log = get_logger("generators.instagram")

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """You write Instagram captions for a South Indian cinema analytics account.

You will receive a JSON fact package. Write ONE caption expressing ONLY those facts.
Do not change numbers. Do not invent facts. Do not exaggerate.

Format: a strong first line (shows before "more"), a short body (2-3 lines),
then 5-8 hashtags on the final line. Max {char_limit} characters.
Warmer than Twitter, still no fanboy energy.

Respond ONLY with the caption text."""


@register
class InstagramGenerator(ContentGenerator):
    platform = Platform.INSTAGRAM
    default_char_limit = 800

    async def generate(self, insight: Insight, insight_id: int,
                       tone: str | None = None,
                       char_limit: int | None = None) -> ContentItem | None:
        limit = char_limit or self.default_char_limit
        payload = {
            "insight": json.loads(insight.model_dump_json(exclude={"discovered_at"})),
            "style_hint": STYLE_HINTS.get(insight.rule, ""),
        }
        if tone:
            payload["tone"] = tone

        msg = await _client.messages.create(
            model=MODEL,
            max_tokens=600,
            system=SYSTEM_PROMPT.format(char_limit=limit),
            messages=[{"role": "user",
                       "content": json.dumps(payload, ensure_ascii=False)}],
        )
        text = msg.content[0].text.strip()
        ok, violations = validate(text, insight)
        if not ok:
            log.warning("instagram caption failed validation: %s", violations)
            return None
        return ContentItem(
            insight_id=insight_id, platform=self.platform, text=text,
            media_ref=insight.primary_entity.slug, model=MODEL, validated=True,
        )
