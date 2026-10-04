# Trend-aware entity selection (real-time)

```
Google Trends (India) → Cinetrace actor match → one LLM reasoning step → Insight → EXISTING engine flow
```

Adds trend-driven tweets alongside the nightly broadcaster, as its own
real-time poller — not a once-a-night step. A successful trend match
produces exactly the same `Insight` contract the discovery rules produce
(`docs/insight-engine.md`), so it flows through the unmodified
`TwitterGenerator`, validator, and Telegram review. Discovery code lives in
`bot/engine/trends/`; the real-time runtime (polling, posting-cap, posting
on approval) lives in `bot/trend_realtime.py`.

## Why this isn't part of the nightly broadcaster

The first version of this lived inside `broadcaster.generate_daily_schedule`
(9 PM generation → next day's fixed slot). That meant up to a **~22 hour**
gap between "this is trending" and "tweet goes out" — fine for evergreen
stat facts, useless for anything actually time-sensitive. Trend-awareness
now runs as its own interval poller instead, and posts immediately once a
human approves — no fixed slot to wait for.

## Flow

```
main.py scheduler, every TREND_POLL_INTERVAL_SECONDS (config.py; default 1800s / 30 min)
  → trend_realtime.poll_and_maybe_post()
      → engine_db.count_trend_posts_today() >= daily_post_cap? → skip, nothing else runs
      → discover_trending_insight()               bot/engine/trends/pipeline.py
          → GoogleTrendsProvider.get_trends()         bot/engine/trends/provider.py
          → resolve_trend_to_actor()                   bot/engine/trends/resolver.py
          → find_trend_connection()                     bot/engine/trends/reasoning.py
      → engine_db.insert_insight(...)              same `insights` table every other rule writes to
      → TwitterGenerator.generate(insight)         EXISTING, unmodified
      → send_trend_insight_for_review(...)         new Telegram card — Approve posts NOW

(human approves whenever they check Telegram)
  → telegram_handler._handle_trend_approve
      → trend_realtime.post_approved_trend_tweet()  posts IMMEDIATELY, not at a cron'd slot
```

This is deliberately a separate path from the nightly broadcaster's
`ins_approve`/`sched_approve` (which post at the next fixed slot hour) —
`trend_approve`/`trend_skip` are new callback prefixes so neither behaviour
touches the other. `post_scheduled_slot` and the discovery-pipeline engine
path are completely unaffected by any of this.

## Real usage/cost — not an estimate

Both Claude calls (`resolver.py`'s nickname resolution, `reasoning.py`'s
relevance check) log their actual `response.usage` token counts to a
`trend_llm_calls` table (best-effort — a logging failure never breaks the
call it's recording). Check real spend any time:

```bash
python -m engine.trends.usage_report          # today / 7d / 30d, real tokens + $ at current Haiku pricing
```

or query `engine.db.trend_llm_usage_summary(days=N)` directly. The dollar
figure uses a hardcoded Haiku 4.5 price constant in `engine/db.py` — update
it if Anthropic's pricing changes; the stored token counts are ground
truth regardless.

## The daily post cap — the actual cost control

Polling every 30 minutes does **not** mean 48 Twitter posts/day. The
pipeline never reads from the X API at all (not even to check — only
Google's free public feed and Cinetrace's own API), so X usage scales with
*approved posts*, not poll frequency. The real cost that scales with
frequency is Claude calls (nickname resolution + reasoning, tried per
candidate per poll) — so `daily_post_cap` (`TREND_DAILY_POST_CAP`, default
**3**) is checked **before** the Trends fetch or either LLM call on every
single poll: once reached, the rest of the day's polls are a no-op query
and nothing else.

The default of 3 isn't arbitrary: engagement-rate data (Rival IQ) and
follower-count-specific guidance (OpenTweet: <10K followers → 2-5
tweets/day) both converge on **3-5 total tweets/day** for an account this
size. This account already posts 1 evergreen scheduled tweet/day, so
capping trend-driven posts at 3 lands total daily volume in that range.

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
   "Rajinikanth Jailer 2 teaser" needs a word-level search; this step finds
   nothing for non-Latin-script titles, which is most of a typical day's
   India feed — that's expected, not a bug, see Limitations)
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

If the daily cap is already hit, the feed is empty, nothing resolves to a
Cinetrace actor, or no fact makes a genuine connection, that poll produces
**nothing** — it does not fall back to the discovery-pipeline engine path,
and it does not force a tweet. A quiet poll is correct; a forced, generic
tweet is the thing this exists to prevent.

## Configuration

`bot/engine/config.py` (`TrendConfig`) plus one constant already in
`bot/config.py`, all env-overridable, sensible defaults:

```
TREND_AWARE_ENABLED=true                 # set false to stop the real-time poller entirely
TREND_GEO=IN
TREND_RSS_URL_TEMPLATE=https://trends.google.com/trending/rss?geo={geo}
TREND_REQUEST_TIMEOUT_SECONDS=10
TREND_MAX_CANDIDATES=8                   # how many trends to try before giving up THIS poll
TREND_MODEL=claude-haiku-4-5-20251001    # same model generators/twitter.py uses
TREND_DAILY_POST_CAP=3                   # hard ceiling on trend-driven POSTS per day
TREND_POLL_INTERVAL_SECONDS=1800         # bot/config.py — how often main.py polls (30 min)
```

Only takes effect when `INSIGHT_ENGINE_ENABLED=true`. Independent of
`REACTIVE_ENABLED` — that flag exists to gate the X-API-credit-burning
filtered stream, and this poller never reads from the X API at all, so it
keeps running even with `REACTIVE_ENABLED=false`.

## Rollback

Set `TREND_AWARE_ENABLED=false` and restart the bot — the real-time poller
job is never registered with the scheduler, and `trend_approve`/
`trend_skip` simply never fire because no card is ever sent. Nothing else
changes: the nightly broadcaster and discovery-pipeline engine path are
completely independent of this flag.

## Observability

Every step logs through the same `engine.*` logger prefix
(`engine.trends.provider` / `.resolver` / `.reasoning`) plus
`trend_realtime`'s own logger, so a quiet poll is traceable: whether the
cap was already hit, which trends were fetched, which one (if any)
resolved, and why a candidate was rejected. Trend-selected insights land in
the same `insights` table as discovery-rule insights, tagged
`rule = "trending_now"`, `facts.source = "google_trends"`; the daily cap
counts posted `content_items` joined back to that rule.

## Limitations

- **Actor-only** (see above) — movies, directors, composers, and events
  don't resolve even when genuinely trending.
- **Most of a real day's India trends are non-Latin-script** (Devanagari,
  Tamil, etc. — confirmed against the live feed), which the token-search
  step (step 2 above) can't tokenize, so those fall through to the
  nickname LLM call every time. That's correct behaviour, but means the
  per-poll Claude-call count is closer to "once per candidate" than "rare" —
  factor that into `TREND_MAX_CANDIDATES` if you tune it.
- **Human-approval latency is real latency.** "Real-time" here means
  *detection* happens within `TREND_POLL_INTERVAL_SECONDS` — actual posting
  still waits on someone tapping Approve in Telegram. If that's too slow in
  practice, the fix is operational (check Telegram more often, or revisit
  whether every trend-driven tweet needs a human gate), not a config value.
- **Live-tested against the real feed and real Cinetrace data; LLM calls
  were scripted.** `GoogleTrendsProvider` was run against the actual live
  `trends.google.com` feed and correctly parsed real trends, correctly
  skipped non-cinema ones (including two real tennis players), and
  correctly resolved a real cinema name to a real seeded Cinetrace actor,
  computing real stats from real data. The two Claude calls (nickname
  resolution, reasoning) were scripted in that run — no real
  `ANTHROPIC_API_KEY` was available in the environment this was built in.
  Posting itself (`post_approved_trend_tweet`) has only been unit-tested
  against a mocked `tweepy` client, never against the real X API.
