"""
Content categories — the coarse buckets the scheduler balances across.

Ranking never sees these; they exist purely so the *feed* stays varied
(a week shouldn't be all collaboration cards even if those score highest).
"""

RULE_CATEGORY: dict[str, str] = {
    "collaboration_shock":     "collaboration",
    "most_frequent_costars":   "collaboration",
    "director_loyalty":        "collaboration",
    "collaboration_diversity": "collaboration",
    "network_power":           "graph",
    "shortest_path":           "graph",
    "career_peak_window":      "career",
    "longest_careers":         "timeline",
    "longest_film_gaps":       "timeline",
    "hidden_dominance":        "career",
    "cross_industry_reach":    "language",
    "most_multilingual":       "language",
    "director_box_office":     "box_office",
    "blockbuster_streaks":     "box_office",
}

ALL_CATEGORIES = sorted(set(RULE_CATEGORY.values()))


def category_of(rule: str) -> str:
    return RULE_CATEGORY.get(rule, "discovery")
