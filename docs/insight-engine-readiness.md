# Insight Engine v1.0 — Production Readiness Review

*2026-07-12 · dry-run audit against a full production-scale database (8,127 actors, 10,812 movies, 282,892 collaboration pairs). No content was published. Intermediate artifacts persisted (discovery stats, full ranked set, dedup analysis, 100 real generated tweets). Interactive audit dashboard: internal artifact `insight_dashboard.html` (regenerate with `python -m engine.dashboard --audit <dir>`).*

---

## 1. Executive Summary

The architecture is sound: discovery is deterministic SQL, the LLM only converts structured insights to language, the number-validator works exactly as designed (caught 10/10 hallucinated-number attempts across 110 LLM calls), and the pipeline runs end-to-end in ~35 seconds. The registry pattern makes new rules and platforms genuinely one-file additions.

However, the large-scale run surfaced **two correctness defects that would visibly embarrass the account on day one**, plus several quality gaps. They are all small fixes. **Verdict at the end: RECOMMEND FIXES BEFORE DEPLOYMENT.**

Headline findings:

| # | Finding | Severity |
|---|---------|----------|
| 1 | 463 movies have `release_year = 0`; rules guard `IS NOT NULL` but not sane ranges → **78 of 88 `longest_film_gaps` insights are garbage** ("1982-year gap") and dominate the top-100 with surprise = 1.0 | 🔴 Blocker |
| 2 | The validator checks number *presence*, not *meaning*: a generated tweet said "Mukesh vanished for **1982 years**" — 1982 is in the payload as a year, misused as a duration. Passed validation. | 🔴 Blocker |
| 3 | `debut_ages` rule can never fire — `actors.debut_year` is NULL for all 119 primary actors | 🟡 High |
| 4 | Repetitive phrasing: 3 of the first 5 tweets used the "That's not a career—that's a…" template; the generator has no memory of prior outputs | 🟡 High |
| 5 | Novelty feature is inert on early runs (no history yet) — spends 25% of the score budget discriminating nothing | 🟢 Medium |
| 6 | `shortest_path` is 7.7s of the 9.3s discovery runtime (83%) | 🟢 Medium |

## 2. Discovery Analysis

**767 candidates from 15 rules (target ≥500 ✓). Zero rule failures. One empty rule.**

| Rule | n | % | Time | Mean score | Assessment |
|---|---|---|---|---|---|
| most_frequent_costars | 100 | 13.0% | 0.01s | 0.749 | healthy |
| network_power | 95 | 12.4% | 0.03s | 0.782 | healthy, repetitive framing risk |
| longest_film_gaps | 88 | 11.5% | 0.03s | 0.809 | **89% garbage (year=0 data)** |
| career_peak_window | 72 | 9.4% | 0.05s | 0.665 | healthy |
| collaboration_diversity | 69 | 9.0% | 0.06s | 0.706 | healthy |
| cross_industry_reach | 68 | 8.9% | 0.04s | 0.619 | weak — most values are "3 industries" |
| most_multilingual | 63 | 8.2% | 0.10s | 0.609 | weak — 44 of bottom-100 |
| collaboration_shock | 58 | 7.6% | 1.17s | 0.724 | healthy |
| longest_careers | 41 | 5.3% | 0.00s | 0.766 | healthy (actor_stats years are clean) |
| hidden_dominance | 39 | 5.1% | 0.04s | 0.814 | strongest mean score |
| shortest_path | 33 | 4.3% | **7.68s** | 0.679 | slow; capped LIMIT hides best pairs |
| director_loyalty | 26 | 3.4% | 0.01s | 0.747 | healthy |
| director_box_office | 9 | 1.2% | 0.03s | 0.742 | thin (sparse box office) |
| blockbuster_streaks | 6 | 0.8% | 0.07s | 0.590 | thin (box office coverage) |
| debut_ages | **0** | 0% | 0.00s | — | **dead — debut_year 100% NULL** |

Recommendations: add a global sane-year guard (`release_year BETWEEN 1900 AND now+1`) in `shared/sql.py` used by every year-based rule; delete or feature-flag `debut_ages` until the column is backfilled; precompute `shortest_path` bridges into an analytics table (like `actor_collaborations`) or accept the 7.7s (nightly, tolerable) but move it last so a timeout doesn't starve other rules.

## 3. Ranking Analysis

Distribution over 763 ranked: min 0.53, max 0.91, mean 0.724, σ 0.082 — **compressed into a 0.38-wide band**. Histogram: 0.5×4 · 0.6×161 · 0.7×293 · 0.8×276 · 0.9×29.

Weighted feature contribution (share of the average score):

| Feature | Mean value | Weighted contribution | Verdict |
|---|---|---|---|
| novelty (w=.25) | **1.00, σ=0.00** | 0.250 | Inert — no history yet; pure constant on run #1 |
| popularity (w=.20) | 0.665 | 0.133 | Working |
| surprise (w=.25) | 0.465, σ=0.31 | 0.116 | Working — but poisoned by bad-year outliers |
| completeness (w=.10) | 0.99 | 0.099 | Near-constant; hard filter already handles it |
| visual (w=.10) | 0.758 | 0.076 | Working |
| recency (w=.10) | 0.496 | 0.050 | Working |

Top-100 is dominated by `longest_film_gaps` (43, mostly bogus) and `network_power` (20). Bottom-100 is `most_multilingual` (44) + `cross_industry_reach` (33) — correctly identified as weak, good sign. **Weights are not the problem; the inputs are.** After the year-guard fix, re-audit before touching weights (per instruction, none changed). One recommended change: novelty should decay within-run per (rule, actor) pair too, not only via posted history — that would break up the network_power block at the top.

## 4. Diversity Analysis

Top-100: 66 unique actors, but **Mohanlal appears 10×, Mammootty 8×, Jagathy Sreekumar 5×, Mukesh 5×** — Malayalam-heavy overall (50 Malayalam / 31 Tamil / 13 Telugu / 6 Kannada), a real reflection of data density, not a bug, but needs constraint at selection time. Directors barely appear in top-100 (0 entries — director rules score mid-pack).

The existing controls already handle most of this downstream: batch dedup enforces max-1-per-actor-per-day (763 → 154 exactly because of it) and `plan_slots()` enforces per-day rule + industry diversity. Gaps and recommendations:

- **Add per-actor weekly cap** (e.g. same actor ≤2 posts/7 days) — currently only "not scheduled yesterday" applies.
- **Add per-rule cooldown at planning** (e.g. `network_power` ≤2/week) so consecutive days don't all lead with the same story shape.
- **Add industry quota to `plan_slots` pass 1** explicitly targeting all four industries per day (it currently prefers diversity but can fill 3/4 slots from one industry when scores skew).
- Director diversity is a non-issue today; revisit when director-centric rules grow.

## 5. Deduplication Validation

- **Exact duplicates within a batch: 0** — fingerprints are unique per (rule, entities, metric, bucket). ✓
- **False positives:** none observed; value bucketing (one significant digit) correctly collided 44 vs 45.
- **False negatives / missed opportunities: 121 cross-rule entity overlaps.** The same actor (e.g. Kamal Haasan) legitimately appears in up to 7 rules. These are different *facts* about the same person, so fingerprints correctly differ — but posted back-to-back they *feel* repetitive. This is a **cadence problem, not a fingerprint problem**: the per-actor cooldown (recommendation §4) is the right fix, not looser fingerprints.
- The "Mohanlal×Priyadarshan 44 films" phrasing-independence requirement is met by construction (sorted entities + bucketing) and covered by unit tests.
- Semantic/embedding dedup remains unnecessary at this scale; the no-op hook is the right placeholder.

## 6. Generator Analysis

100/100 top insights produced validated tweets. 110 total LLM attempts → **9.1% retry rate, all 10 failures were hallucinated numbers, all caught, all recovered on retry**. Zero name-drop failures, zero over-length. Length: mean 154 chars (min 86, max 205, comfortably under 260). Wall clock 24s for 100 at concurrency 8.

- **Excellent** — `hidden_dominance`: "Jagathy Sreekumar appeared in 391 films as a supporting actor. The average lead actor manages 94. Some careers are measured in scenes, not posters."
- **Average** — `network_power`: "Kamal Haasan has worked with 659 unique co-stars across 209 films. That's not a career—that's a network." (correct, readable, formulaic)
- **Poor** — `longest_film_gaps`: "Mukesh vanished from Malayalam cinema for **1982 years**…" (bad input data + semantic misuse passing validation — the two blockers compounding)

Curiosity/readability is good when the underlying number is genuinely strong; weak inputs (3 industries, 4 languages) produce flat copy — ranking already pushes those down. The template-phrase repetition ("That's not a…") needs a variety mechanism: pass the last N approved tweet texts as "do not reuse these framings," or rotate style seeds per rule.

## 7. Performance Analysis

Measured: discovery 9.3s (shortest_path 7.7s; other 14 rules total 1.6s) · fame prefetch + ranking 0.13s for 763 · dedup <1ms · generation ~0.24s/tweet wall at concurrency 8 (12s p50 LLM latency).

| Scale | Discovery | Rank+dedup | Generation (4/day) | Pipeline total |
|---|---|---|---|---|
| 100 insights | ~9s | <0.1s | ~50s | ~1 min |
| 500 | ~9s | ~0.1s | — | ~1 min |
| 1,000 | ~10s | ~0.2s | — | ~1 min |
| 5,000 | ~15s (rule LIMITs raised) | ~1s | — | ~1.5 min |

Discovery cost is dominated by fixed SQL, not candidate count — it scales with the *database*, not the insight count. The only real bottleneck is `shortest_path` (O(famous²) self-join); everything else is precomputed-table lookups. Nightly budget (minutes) means **no performance work is required for v1**; flag shortest_path if the famous-actor threshold is lowered.

## 8. Architecture Review

| Question | Answer |
|---|---|
| 100 discovery rules? | **Yes** — registry auto-import scales; add per-rule timing/timeout in `pipeline.py` (recommended) so one slow rule can't starve the run. At 100 rules, move rule execution to a thread pool. |
| 100,000 insights? | **Yes for storage** (JSONB + indexed fingerprint). `recent_fingerprint_counts` loads a year of history into a dict — fine to ~1M rows; convert to a per-fingerprint EXISTS query beyond that. |
| Instagram / Threads / LinkedIn / YouTube / blogs / newsletters? | **Yes** — the Insight contract is platform-free; each is one generator module. Posting integrations (auth, media, threading) are the real work, and they live outside the engine. `content_items.platform` + per-platform slot uniqueness already model it. |
| Image generation? | Clean fit: `ContentItem.media_ref` is already an indirection; an image generator is a new media producer keyed by rule + entities. Stat-card infrastructure (SSR page + Playwright) is the template. |
| Daily reports / API exposure / mobile? | The `insights` table *is* the API surface — add a read-only FastAPI router in the backend re-using the same rows. Nothing in the engine assumes Twitter. |
| Maintenance risks | (a) Raw-SQL rules duplicate NULL/year guards — centralize in `shared/sql.py` (the year-guard fix does this); (b) engine imports bot's `config.py` (env-heavy) — acceptable now, extract an `engine/settings.py` if the engine is ever promoted out of the bot; (c) two content stores during migration (`scheduled_tweets` + `content_items`) — planned, remove after cutover window. |
| Refactor needed? | None structural. The layering (discovery / ranking / dedup / generation / approval / scheduling) held up under audit — every phase of this review was answerable from persisted intermediates, which is the property you want for years of autonomous operation. |

## 9. Dashboard Design (implemented)

`bot/engine/dashboard.py` generates a self-contained, admin-only HTML dashboard (never served publicly; run on demand):

- **Overview tiles** — insights stored, dry-run candidates, pending approvals, published, failed, avg quality, cooldowns, retry rate
- **Discovery** — insights-per-rule chart; rule health table (healthy/empty/slow/failed with timings + share)
- **Ranking** — score histogram, weighted feature contribution, mean score per rule
- **Diversity** — industry mix, actors appearing 3+, rule mix of top-100
- **Dedup** — fingerprint collisions, cross-rule overlaps, active cooldowns
- **Generator** — retry rate, hallucinations caught, discards, avg length
- **Scheduler** — content_items by status (queue, backlog, failures) when run against the live DB
- **Insight browser** — search by actor/director/text, filter by rule/status, sort by score/date; every insight expands to a detail view: original Insight JSON, per-feature ranking breakdown, generated text, publishing status, error, posted id

Current snapshot published as an internal artifact. Future upgrade path: the same queries as a `/admin` route in the backend behind the existing API-key guard.

## 10. Future Roadmap (post-v1, in value order)

1. **Backfill data quality** — fix 463 `release_year=0` movies, backfill `debut_year`; every rule gets better for free (biggest ROI item on this list).
2. **Insight clustering / story arcs** — group the 121 cross-rule overlaps per actor into weekly "deep dive" threads instead of suppressing them.
3. **Trending actor detection** — join Twitter signal volume (already collected by the reactive pipeline) into a `recency_boost` feature; birthdays/release dates as **seasonal insights** (a `calendar_hook` discovery rule).
4. **Automatic image generation** — per-rule visual templates beyond the stat-card (duo cards, career timelines already exist as social pages).
5. **Weekly digest generator** — newsletter platform generator consuming the week's top insights (the contract already supports it).
6. **REST API** — `GET /insights?actor=&rule=&min_score=` on the backend; powers mobile/embeds; GraphQL only if consumers demand shaping.
7. **Knowledge-graph explorer** — the shortest-path and cinema-universe endpoints already exist; surface insight overlays on them.

## 11. Recommended Improvements (prioritized)

| P | Fix | Impact | Effort |
|---|---|---|---|
| **P0** | Sane-year guard (`1900..now+1`) in `shared/sql.py`, applied in `longest_film_gaps`, `career_peak_window`, `collaboration_shock`, `blockbuster_streaks` | Kills 78 garbage insights poisoning the top of every ranking | ~30 min |
| **P0** | Year-vs-duration validation: numbers followed by "years"/"साल" must match a duration-typed metric, not a calendar year; plus prompt note "values in `period` are calendar years, not durations" | Prevents "vanished for 1982 years"-class semantic misuse | ~1 h |
| **P1** | Remove/flag `debut_ages` until `debut_year` is backfilled | Dead code honesty; re-enable with data | 10 min |
| **P1** | Anti-template phrasing: pass last 20 approved tweets into the generator prompt as banned framings | Stops "That's not a career—that's a…" repetition | ~30 min |
| **P1** | Per-actor 7-day cap + per-rule weekly cap in `plan_slots` | Cadence diversity (fixes §4 and §5 cross-rule feel-alikes) | ~1 h |
| **P2** | Within-run novelty decay per (rule, actor); revisit weights only after P0 re-audit | Restores 25% of score budget to useful work | ~1 h |
| **P2** | Per-rule timeout + duration metric in `pipeline.py`; log counters (discovered/filtered/deduped/generated/validated) as single-line JSON for grep-able ops | Observability for years-long autonomy | ~1 h |
| **P3** | Precompute shortest-path bridges nightly into an analytics table | 7.7s → ms; enables richer connection rules | ~half day |

## 12. Release Recommendation

**RECOMMEND FIXES BEFORE DEPLOYMENT**

The architecture, dedup, approval flow, safety flag, and anti-hallucination validator all passed a production-scale audit — those are the hard parts, and they work. But two P0 defects would put visibly wrong content in front of the approval queue (and plausibly past a tired reviewer) on the first night: bad-year data producing absurd top-ranked "insights," and the validator's blindness to a payload year being misused as a duration. Both fixes are small (~90 minutes combined), and the P1 items (dead rule, template phrasing, cadence caps) are strongly advised in the same patch since re-auditing once is cheaper than twice. After applying P0+P1, re-run this audit's dry-run script — expected outcome: `longest_film_gaps` drops from 43 to <10 of the top-100 and the top-20 spreads across ≥5 rules. Ship then.
