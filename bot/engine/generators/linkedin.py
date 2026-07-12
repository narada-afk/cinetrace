"""
LinkedIn post generator (stub integration — output stored in
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

log = get_logger("generators.linkedin")

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """You write LinkedIn posts for a South Indian cinema analytics platform.

You will receive a JSON fact package. Write ONE post expressing ONLY those facts.
Do not change numbers. Do not invent facts. Do not exaggerate.

Angle: what this data point says about careers, consistency, collaboration or
longevity — professional-lessons framing, never listicle clichés.
Format: hook line, blank line, 2-4 short paragraphs, max {char_limit} characters.
No hashtag walls (2 max). No emoji.

Respond ONLY with the post text."""


@register
class LinkedInGenerator(ContentGenerator):
    platform = Platform.LINKEDIN
    default_char_limit = 1500

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
            max_tokens=800,
            system=SYSTEM_PROMPT.format(char_limit=limit),
            messages=[{"role": "user",
                       "content": json.dumps(payload, ensure_ascii=False)}],
        )
        text = msg.content[0].text.strip()
        ok, violations = validate(text, insight)
        if not ok:
            log.warning("linkedin post failed validation: %s", violations)
            return None
        return ContentItem(
            insight_id=insight_id, platform=self.platform, text=text,
            media_ref=insight.primary_entity.slug, model=MODEL, validated=True,
        )
