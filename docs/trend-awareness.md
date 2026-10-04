# Trend-aware entity selection

```
Google Trends (India) → Cinetrace actor match → one LLM reasoning step → Insight → EXISTING engine flow
```

Adds trend-awareness to the scheduled tweet's entity selection. It does not
replace or duplicate anything in `docs/insight-engine.md` — a successful
trend match produces exactly the same `Insight` contract the discovery rules
produce, so it flows through the unmodified `TwitterGenerator`, validator,
Telegram review, and poster. Code lives in `bot/engine/trends/`.

## Flow

```
broadcaster.generate_daily_schedule()      (9 PM IST, unchanged schedule)
  → _generate_daily_schedule_trend_aware()
      → discover_trending_insight()         bot/engine/trends/pipeline.py
          → GoogleTrendsProvider.get_trends()   bot/engine/trends/provider.py
          → resolve_trend_to_actor()             bot/engine/trends/resolver.py
          → find_trend_connection()               bot/engine/trends/reasoning.py
      → engine_db.insert_insight(...)        same `insights` table every other rule writes to
      → TwitterGenerator.generate(insight)   EXISTING, unmodified
      → send_insight_for_review(...)         EXISTING, unmodified
```

`post_scheduled_slot` needs no changes — it reads whatever is in
`content_items` for the slot, regardless of which path produced it.

## Trend source — Google Trends only

`GoogleTrendsProvider` reads Google's public daily-trends RSS feed
(`https://trends.google.com/trending/rss?geo=IN`, no API key). This is the
*only* trend-discovery mechanism — there is no custom scoring, trend
database, clustering, or social listening here. The feed already groups
each trend with a handful of related news-item titles; those are reused
as-is for "why is this trending" context (`TrendCandidate.context_text`) —
we don't build a separate context lookup.

A second source could be added later (e.g. vidIQ) by implementing the
`TrendProvider` interface in `provider.py` — nothing downstream would
change. **Not implemented** — out of scope for now, per design.

## Cinetrace matching — actors only, via the existing search

`resolver.py` matches a trend title against Cinetrace with
`GET /actors/search?q=` (the same endpoint the frontend search box uses) —
no new entity-resolution system. It tries, cheapest first:

1. the raw trend title
2. each capitalised word in the title (titles are rarely *just* a name —
   "Rajinikanth Jailer 2 teaser" needs a word-level search)
3. one small Claude call, only when 1–2 miss, to map a nickname/alias to a
   real name ("Thalaivar" → "Rajinikanth") — re-verified against Cinetrace
   before being trusted; the model's answer is never taken on faith.

**Movies and directors are out of scope.** Cinetrace exposes no
`/movies/search` or `/directors/search` endpoint today, and the rest of the
bot (inventory, generators, stat-cards) is entirely actor-centric. A
trending movie or director title simply won't resolve, and the pipeline
SKIPs — that's correct, not a gap to patch around here.

## The one LLM reasoning step

`reasoning.py` computes a **whitelist of real Cinetrace facts** in Python
(reusing `generator._compute_cream` — the same deterministic math the
existing scheduled-tweet generator already uses) and asks Claude a single
question: *does any one of these facts make a genuinely interesting tweet
given why this is trending right now?* The model may only return a key from
that whitelist — never a number. If it names an unknown key, the result is
discarded. This mirrors the engine's existing "AI never invents facts" rule
(`docs/insight-engine.md`).

## SKIP is success

If the feed is empty, nothing resolves to a Cinetrace actor, or no fact
makes a genuine connection, the scheduled run produces **no tweet that
day** — it does not fall back to the discovery-pipeline engine path. A
quiet day is correct; a forced, generic tweet is the thing this exists to
prevent. See `FAILURE BEHAVIOUR` in the original design notes for the full
list of SKIP conditions.

## Configuration

All in `bot/engine/config.py` (`TrendConfig`), env-overridable, sensible
defaults — nothing hardcoded, nothing required to set:

```
TREND_AWARE_ENABLED=true                 # set false to go straight back to the discovery pipeline
TREND_GEO=IN
TREND_RSS_URL_TEMPLATE=https://trends.google.com/trending/rss?geo={geo}
TREND_REQUEST_TIMEOUT_SECONDS=10
TREND_MAX_CANDIDATES=15                  # how many trends to try before giving up for the day
TREND_MODEL=claude-haiku-4-5-20251001    # same model generators/twitter.py uses
```

Only takes effect when `INSIGHT_ENGINE_ENABLED=true` (the legacy
`generator.py`/`scorer.py` broadcaster path is unaffected either way).

## Rollback

Set `TREND_AWARE_ENABLED=false` and restart the bot — scheduled tweets go
straight back to the discovery-pipeline engine path (`run_discovery_pipeline`
→ `plan_slots` → generator), exactly as before this change, no deploy of a
different image needed.

## Observability

Every step logs through the same `engine.*` logger prefix
(`engine.trends.provider` / `.resolver` / `.reasoning` / and
`[broadcaster]` prints), so a quiet night is traceable: which trends were
fetched, which one (if any) resolved, and why a candidate was rejected.
Trend-selected insights land in the same `insights` table as discovery-rule
insights, tagged `rule = "trending_now"`, `facts.source = "google_trends"`.

## Limitations

- **Actor-only** (see above) — movies, directors, composers, and events
  don't resolve even when genuinely trending.
- **Google's trending feed, not live-tested from this environment** — the
  sandbox this was built in has no network egress to `trends.google.com`
  (its egress proxy returns 403 for that host specifically; PyPI, GitHub,
  etc. are reachable). All three layers (`provider.py`, `resolver.py`,
  `reasoning.py`) are unit-tested against mocked HTTP/LLM responses
  (`bot/tests/test_trends_*.py`); the live feed's exact XML shape should be
  double-checked against a real fetch before the first production run, in
  an environment that can actually reach it.
- **One trend-selected tweet per day** — matches the current
  `SLOT_HOURS = [19]` ("one tweet a day for now" in `inventory.py`). If
  `SLOT_HOURS` ever grows, only the first slot goes through trend
  selection; the rest still has no entity-selection story of its own
  outside the discovery pipeline. Revisit if/when multi-slot days return.
