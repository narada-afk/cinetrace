# Insight Engine v1.0 — Production Readiness Review (Rev. 2, post-hardening)

*2026-07-13 · Second review after the P0/P1 hardening pass. Both audits run at production scale (8,127 actors, 10,812 movies, 282,892 collaboration pairs) against a full local replica. "Before" = the 2026-07-12 baseline; "After" = current code. Nothing published; flag stays off. Dashboard: internal artifact `insight_dashboard.html` (v2, extended with confidence / rule-health / data-quality).*

---

## 1. Executive Summary

The two blocker defects from the first review are fixed and verified at scale, and the P1 quality work landed. The headline proof points:

- **`longest_film_gaps` no longer dominates**: top-100 share **43 → 0**. The 463 `release_year = 0` rows are excluded at the SQL layer, so the rule dropped from 88 → 27 candidates and none reach the top of the ranking.
- **Low-confidence data can no longer win on shock value**: final score now multiplies interest by data confidence. Low-confidence insights (≤0.7) average **0.388** vs **0.741** for high-confidence, and **0 low-confidence insights appear in the top-100**.
- **The validator now understands meaning, not just presence**: "1982 years" (a calendar year misused as a duration) is rejected; unit tests lock in the year-vs-duration, count-vs-decade, and derived-span cases.
- **The feed will read as many editors, not one template**: 100/100 generated tweets had distinct opening phrases; banned rhetorical templates fell 7 → 2 per 100 via a style-only retry that never discards.
- **Scheduling diversity is enforced**: a fresh end-to-end daily plan produced 4 distinct rules / 4 actors / 4 categories. A latent interaction bug (top-N truncation starving the planner) was caught during this review and fixed.

One residual behavior worth stating plainly: **ranking alone still concentrates on the single strongest rule** (network_power fills much of the top-100), because confidence lifts verified-count rules and novelty is inert on a cold history. This is intentional and safe — diversity is a *scheduling* concern, and `plan_slots` demonstrably delivers a varied feed from that concentrated pool. **Verdict at the end: READY FOR PRODUCTION.**

## 2. Before vs After

| Metric | Before | After | Note |
|---|---|---|---|
| Candidates discovered | 767 | 706 | bad-year gaps removed; `debut_ages` deleted |
| Discovery rules | 15 (1 dead) | 14 (0 dead) | `debut_ages` removed (debut_year 100% NULL) |
| `longest_film_gaps` candidates | 88 | 27 | 61 garbage year-0 gaps gone |
| **`longest_film_gaps` in top-100** | **43** | **0** | primary goal ✓ |
| Rule failures | 0 | 0 | |
| Score mean / stdev | 0.724 / 0.082 | 0.650 / 0.120 | confidence multiplier widened the useful spread |
| Confidence in ranking | none | ×confidence | poor data can't dominate |
| Low-conf (≤0.7) in top-100 | n/a | 0 | ✓ |
| Low-conf vs high-conf mean score | n/a | 0.388 vs 0.741 | ranks by reliability ✓ |
| Generator retry rate | 9.1% | 11.9% | higher because the validator is stricter (14 vs 10 hallucinations caught) |
| Discards (per 100) | 0 | 0 | recovery on retry still works |
| Distinct opening phrases /100 | not measured | 100 | ✓ |
| Banned template hits /100 | ~7 (observed) | 2 | style retry ✓ |
| Avg tweet length | 154 | 146 | still well under 260 |
| Cross-rule entity overlaps | 121 | 117 | expected (same actor, different facts) — handled by scheduling caps |
| Pipeline runtime | ~35s | ~33s | unchanged; shortest_path still ~7s |

## 3. Discovery Analysis

14 rules, 706 candidates, zero failures, zero empty rules (the one chronically-empty rule was removed). The centralized `sane_year()` predicate in `shared/sql.py` (`>= 1900 AND <= current_year+1`, non-null) is now used by every year-touching rule — `longest_film_gaps`, `career_peak_window`, `collaboration_shock`, `blockbuster_streaks`. Adding a new year-based rule inherits the guard for free; this is the "fix once" property the brief asked for.

Rule contribution is unchanged in shape except for the corrected gap rule. `shortest_path` remains the only slow rule (~7s of ~33s) — acceptable for a nightly job; flagged for precomputation in the roadmap, not a blocker.

## 4. Ranking Analysis

`final = (Σ featureᵢ·weightᵢ) × confidence`. Confidence is stored as a component for transparency and multiplies the interest score. Effects:

- Score distribution widened (stdev 0.082 → 0.120) and shifted down (mean 0.724 → 0.650) — sparse-data rules (`director_box_office`, `blockbuster_streaks`, `most_multilingual`) now correctly sink toward the bottom.
- The top of the ranking concentrates on `network_power` / `most_frequent_costars` / `longest_careers` (confidence 1.0, high magnitude). This is *not* a defect: ranking's job is to float the best insights; the feed's variety is the scheduler's job (§6). Weights were **not** changed, per instruction — the re-audit shows the inputs, not the weights, were the problem, and the fix worked.
- Novelty is still a near-constant 1.0 on a cold insight history; it becomes discriminating once the `insights` table accumulates. Documented as expected, not blocking.

## 5. Confidence Analysis

Per-rule confidence (mean over the run): verified-count rules `network_power`, `longest_careers`, `most_frequent_costars`, `director_loyalty` = **1.0**; derived stats `career_peak_window`, `collaboration_shock`, `hidden_dominance`, `collaboration_diversity` = **0.85–0.9**; sparse/inferred `shortest_path` 0.75, `cross_industry_reach` 0.85, and coverage-scaled `most_multilingual` / `blockbuster_streaks` / `director_box_office` down to **0.45** when box-office/language coverage is thin. The lowest-confidence samples (Rajinikanth "multilingual" 0.45, Mani Ratnam gross 0.46) all landed in the bottom of the ranking — exactly the intended behavior. Confidence is stored in the `Insight` model, persisted to `insights.confidence`, surfaced in the dashboard, and travels in the JSON payload any future API/generator consumes.

## 6. Diversity & Rule Health

**Scheduling caps** (`plan_slots`, scheduling-only — ranking untouched): one insight per rule/day, ≤2 per actor/day-week, ≤2 per category/day, ≤3 per rule/week, and no two adjacent slots sharing a rule or category. Categories: career, collaboration, graph, language, timeline, box_office, discovery. A live end-to-end plan produced `network_power / hidden_dominance / director_box_office / career_peak_window` across 4 actors and 4 categories.

**Latent bug caught & fixed during this review:** `run_discovery_pipeline` persisted only the top-40 by score — all `network_power` — which would have starved the one-rule-per-day planner to a single tweet. Raised `top_n` to 200 so the planner draws from the full ~150-insight deduped pool (8+ distinct rules). Verified.

**Rule health** is now recorded every run into a `rule_health` table and shown on the dashboard: status (healthy / warning / broken), rows scanned, rows emitted, seconds, and a remediation reason. An empty rule reports "warning: 0 insights emitted — <remediation>" instead of failing silently; a broken query reports "broken" with the exception. This is the observability the brief asked for and the mechanism that would have flagged `debut_ages` automatically.

## 7. Validation Results

The validator now has two layers. **Presence**: every numeral must exist in the payload (formatting-tolerant) or be derivable year arithmetic. **Semantics**: a number paired with a unit word must belong to the matching class — film counts are films, calendar years are years, durations are durations. Concretely it rejects "1982 years", "44 decades", "1987 films"; accepts "since 2014", "44 films", "12 years of silence", "5 years" (derived from a period). Fixed a subtle bug where `window_years: 5` was classified as a calendar year (suffix `_years` → duration, `_year` → calendar). At scale: 14 hallucination attempts across 118 LLM calls, all caught, 0 discards, 100/100 valid tweets. Unit tests cover every semantic case.

## 8. Dashboard Improvements

The admin dashboard (`engine/dashboard.py`, self-contained HTML, admin-only) gained: **Confidence** (distribution histogram, average per rule, lowest-confidence insights with their final scores, and the low-conf-in-top-100 counter as a health signal); **Rule Health & Data Quality** (per-rule status/rows-scanned/emitted/time/remediation, with a "needs attention" badge); and extended **Generator** metrics (distinct openers, template-phrasing hits, retry reason). Existing sections (overview, discovery, ranking histogram, diversity, dedup, searchable insight browser with per-insight JSON + ranking breakdown + generated text + status) are retained. Published as an internal artifact.

## 9. Architecture Review — confidence as a platform primitive

Confidence is already a first-class field on the `Insight` contract, so every downstream consumer gets it for free without new plumbing:

- **Generators** (Twitter/Instagram/LinkedIn and future Threads/YouTube/blog/newsletter) can gate or hedge on it — e.g. suppress a hard claim below 0.6, or add "at least" language for coverage-limited stats.
- **Image generation** can pick templates by confidence (a bold hero number for 1.0; a softer "and counting" treatment for sparse data).
- **REST/GraphQL/mobile**: the field serializes in the payload today; a read-only `GET /insights?min_confidence=` filter is a one-line WHERE clause on the existing table.
- **Recommendations / newsletters / reports**: confidence is the natural "should we lead with this?" signal.

Recommended (non-blocking) abstraction for later: promote confidence from a single float to a small typed struct `{score, factors[]}` capturing *why* (coverage %, inferred-join flag, filtered-row count) so consumers and the dashboard can explain a low score, not just display it. The current float is sufficient for v1 and forward-compatible — widening it is additive.

## 10. Remaining Risks (all acceptable for v1)

| Risk | Why acceptable |
|---|---|
| Ranking top-end concentrates on one rule | Scheduling diversity caps demonstrably produce a varied feed; ranking's job is quality, not spread. Human approval is the final gate. |
| Novelty feature inert until history accrues | Self-resolves within days of real runs; other five features + confidence carry ranking meanwhile. |
| 2/100 tweets still use a soft-banned template on the 2nd attempt | Style-only, factually correct; a human approves every tweet before it posts. |
| `shortest_path` ~7s | Nightly job, minutes of budget; precompute later. |
| `debut_ages` removed rather than fixed | Honest — the column is 100% NULL; re-enable after a data backfill. Rule-health would flag it if reintroduced empty. |
| Every tweet still passes through Telegram approval | This is the ultimate backstop: nothing reaches an audience unreviewed. |

## 11. Deployment Recommendation

Every P0 and P1 item is implemented, unit-tested (51 tests green), and verified against a production-scale re-audit. The two original blockers are provably gone: garbage-year insights are out of the rankings (43 → 0), and low-confidence data now ranks below high-confidence data (0 low-confidence insights in the top-100; 0.388 vs 0.741 mean score). The generator produces varied, factually-validated copy with zero discards, and the scheduler delivers a genuinely diverse daily plan. The remaining risks are either self-healing (novelty), cosmetic (2% template phrasing), or backstopped by the mandatory human-approval step that gates every post. Confidence is a reusable platform primitive already flowing through the single `Insight` contract, so future generators and APIs inherit it without rework.

Ship it with the feature flag flipped after one supervised night of Telegram-approved drafts, exactly as the rollout runbook prescribes.

**READY FOR PRODUCTION**
