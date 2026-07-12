# CineTrace Insight Engine

```
Database → Discovery → Ranking → Dedup → Content Generation → Human Approval → Scheduler → Platforms
```

**Philosophy: the AI never invents facts.** Discovery modules run deterministic SQL
against the cinema graph and emit structured `Insight` objects containing no human
language. The LLM's only job is converting an insight into engaging platform copy —
and a validator rejects any output containing a number not present in the insight.

Code lives in `bot/engine/`. The pipeline is enabled with `INSIGHT_ENGINE_ENABLED=true`;
when off, the legacy generator/scorer broadcaster path runs unchanged (instant rollback).

## Data contracts

Every stage speaks Pydantic models (`bot/engine/models.py`):

```json
// Insight — the single contract all generators consume
{
  "rule": "collaboration_shock",
  "entities": [
    {"kind": "actor", "id": 123, "name": "Mohanlal", "slug": "mohanlal"},
    {"kind": "actor", "id": 456, "name": "Mammootty", "slug": "mammootty"}
  ],
  "metrics": [
    {"key": "collab_count", "value": 44, "unit": "films"},
    {"key": "years_since_last", "value": 12, "unit": "years"}
  ],
  "facts": {"last_film_year": 2014, "industry": "Malayalam"},
  "completeness": 1.0
}
```

`ContentItem` is one rendering of an insight for one platform, with lifecycle
`new → approved | rejected → posted | failed` (table `content_items`).

## How to add a discovery rule

Create one module in `bot/engine/discovery/` — nothing else changes
(the package auto-imports every module and `@register` adds it to the registry):

```python
from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric

@register
class MostRemadeFilms(DiscoveryRule):
    name = "most_remade_films"
    visual_potential = 0.6          # 0-1 hint for the ranker

    def sql(self) -> str:
        return "SELECT ... LIMIT %(limit)s"   # read-only, parametrised

    def rows_to_insights(self, rows) -> list[Insight]:   # pure — unit-test this
        return [Insight(rule=self.name,
                        entities=[Entity(kind="movie", id=r["id"], name=r["title"])],
                        metrics=[Metric(key="remake_count", value=r["n"], unit="remakes")])
                for r in rows]
```

Rules must contain **no prose** (a contract test in
`bot/tests/test_discovery_rules.py` enforces it — add sample rows there).
Use `engine/shared/sql.py` for the credits `cast UNION actor_movies` idiom and
BROKEN-movie guards; set `completeness < 1.0` when evidence is sparse (box
office, language) so the ranker can penalise it.

## How to add a platform generator

Create one module in `bot/engine/generators/`:

```python
from engine.generators.base import ContentGenerator, register
from engine.models import ContentItem, Insight, Platform

@register
class ThreadsGenerator(ContentGenerator):
    platform = Platform.THREADS
    default_char_limit = 500

    async def generate(self, insight, insight_id, tone=None, char_limit=None):
        ...  # LLM call → validate(text, insight) → ContentItem or None
```

Generators receive ONLY the insight (+ tone, char limit). **They never query
the database.** Always run `engine.generators._validator.validate()` on the
output — it rejects any numeral not present in the insight payload (year
gaps/spans derivable from payload years are whitelisted) and any output that
drops the primary entity's name. Failed validation ⇒ one retry with the
violations fed back, then discard.

## Ranking

Six features, each 0–1 (`engine/ranking/features.py`): novelty (fingerprint
seen before?), surprise (metric magnitude or rule-supplied percentile),
popularity (actor fame, ported from the backend), visual_potential (rule hint
+ slug bonus), recency (underlying period), completeness.

Weights are env-overridable — no deploy needed to tune:

```
ENGINE_WEIGHT_NOVELTY=0.25  ENGINE_WEIGHT_SURPRISE=0.25  ENGINE_WEIGHT_POPULARITY=0.20
ENGINE_WEIGHT_VISUAL_POTENTIAL=0.10  ENGINE_WEIGHT_RECENCY=0.10  ENGINE_WEIGHT_COMPLETENESS=0.10
ENGINE_MIN_COMPLETENESS=0.5  ENGINE_MIN_FAME=0.2  ENGINE_TOP_N=40
```

Every stored score carries `weights_version` (hash of the effective weights).

## Deduplication

`fingerprint = sha1(rule | sorted entities | metric key | bucketed value)` —
entity order and phrasing don't matter, and the value is bucketed to one
significant digit so "44 films" vs "45 films" collide. Three layers:

1. **Cooldown** — fingerprints posted within `ENGINE_COOLDOWN_DAYS` (default 90)
   are skipped; the cooldown starts when a content item is marked POSTED.
2. **Batch** — best-scored insight per fingerprint per run.
3. **Diversity** — max 1 insight per actor per day.

`deduper.semantic_check()` is a no-op hook for future embedding-based similarity.

## Approval & scheduling

Nightly at 9 PM IST (`broadcaster.generate_daily_schedule`):
discovery pipeline → `plan_slots()` picks one insight per slot (industry + rule
diversity) → Twitter generator → row in `content_items` (status `new`) →
Telegram review card with score provenance + stat-card portrait.
Buttons `✅ Approve` / `❌ Skip` → status `approved` / `rejected`.

At each slot hour, `post_scheduled_slot` posts the approved item with the
actor stat-card image; success ⇒ `posted` + cooldown upsert, tweepy failure ⇒
`failed` + Telegram alert (the slot is not retried automatically — approve a
regenerated draft or repost manually).

## Runbook

- **Enable**: set `INSIGHT_ENGINE_ENABLED=true` in the bot's env and restart.
- **Rollback**: unset the flag — the legacy `scheduled_tweets` path takes over immediately.
- **Dry run** (inside the bot container):
  `python -m engine.pipeline --dry-run` → prints top-20 ranked insights as JSON, no writes.
- **Tests**: `pip install -r requirements-dev.txt && pytest` (from `bot/`).
  Rules are tested as pure row-parsers; no database needed.
- **Failed post**: check `content_items.error`, fix, re-approve or post manually.
