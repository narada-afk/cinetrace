import json
import anthropic
from config import ANTHROPIC_API_KEY

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """You are the intelligence layer for CineTrace Stats Bot — a South Indian cinema analytics bot.
Your job is to analyse a tweet or trending topic and decide:
1. Whether it is safe and appropriate to engage with a stat reply
2. What type of stat angle would be most interesting

You must be conservative. When in doubt, do NOT engage.

Never engage if the tweet is about:
- Personal tragedy, death, grief, illness
- Political controversy or statements
- Family matters, relationships, personal life
- Inter-fan fights or rivalry baiting
- Brand endorsements or paid promotions
- Anything negative, critical or sensitive

Only engage if the tweet is:
- Celebratory (milestone, announcement, achievement)
- Film-related (new project, release, award)
- Personal and positive (gratitude, fan interaction)
- A trending topic clearly about cinema or box office

Respond ONLY with valid JSON. No explanation outside the JSON."""

ANALYSE_TEMPLATE = """Analyse this tweet from South Indian actor {actor_name} (@{handle}):

Tweet: "{tweet_text}"

Trending context (if any): {trend_context}

Respond with JSON:
{{
  "should_engage": true/false,
  "reason": "one sentence why or why not",
  "stat_angle": "what kind of stat would be most surprising/relevant (null if should_engage is false)",
  "response_type": "reply" or "thread" (null if should_engage is false),
  "safety_score": 0-100 (100 = completely safe)
}}"""

async def analyse_tweet(actor_name: str, handle: str,
                         tweet_text: str, trend_context: str = "") -> dict:
    prompt = ANALYSE_TEMPLATE.format(
        actor_name=actor_name,
        handle=handle,
        tweet_text=tweet_text,
        trend_context=trend_context or "none",
    )
    msg = await _client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    try:
        return json.loads(msg.content[0].text)
    except Exception:
        return {"should_engage": False, "reason": "parse error", "safety_score": 0}
