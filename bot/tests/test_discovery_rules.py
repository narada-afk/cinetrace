"""
Contract tests for discovery rules — pure rows_to_insights, no DB.

Every registered rule must:
  - produce valid Insight objects from representative rows
  - contain NO human-language prose in facts (structured values only)
"""

import pytest

from engine.discovery import all_rules, get_rule
from engine.models import Insight

# Representative DB rows per rule (matching each rule's SQL SELECT columns)
SAMPLE_ROWS = {
    "collaboration_shock": [{
        "actor1_id": 1, "actor1_name": "Mohanlal", "industry1": "Malayalam",
        "actor2_id": 2, "actor2_name": "Mammootty", "films": 12, "last_year": 2010,
    }],
    "hidden_dominance": [{
        "id": 3, "name": "Brahmanandam", "industry": "Telugu",
        "film_count": 522, "lead_avg": 90,
    }],
    "cross_industry_reach": [{
        "id": 4, "name": "Kamal Haasan", "home_industry": "Tamil",
        "ind_count": 4, "film_count": 150,
        "industries": ["Tamil", "Telugu", "Malayalam", "Kannada"],
    }],
    "career_peak_window": [{
        "id": 5, "name": "Chiranjeevi", "industry": "Telugu",
        "peak_start": 1986, "peak_end": 1990, "win_films": 38, "total_films": 150,
    }],
    "network_power": [{
        "id": 6, "name": "Prakash Raj", "industry": "Tamil",
        "costar_count": 320, "film_count": 250,
    }],
    "director_loyalty": [{
        "actor_id": 7, "actor_name": "Mohanlal", "industry": "Malayalam",
        "director_name": "Priyadarshan", "dir_films": 44,
        "total_films": 300, "pct": 15,
    }],
    "director_box_office": [{
        "director": "S. S. Rajamouli", "total_cr": 3500, "biggest_cr": 1200,
        "films_with_bo": 10, "total_films": 12, "biggest_title": "Baahubali 2",
    }],
    "longest_careers": [{
        "id": 8, "name": "Sivaji Ganesan", "industry": "Tamil",
        "first_film_year": 1952, "last_film_year": 1999,
        "film_count": 280, "span_years": 47,
    }],
    "most_frequent_costars": [{
        "actor1_id": 10, "actor1_name": "Prem Nazir", "industry": "Malayalam",
        "actor2_id": 11, "actor2_name": "Sheela", "films": 130,
    }],
    "blockbuster_streaks": [{
        "id": 12, "name": "Rajinikanth", "industry": "Tamil",
        "streak_len": 5, "streak_start": 2014, "streak_end": 2018,
        "with_bo": 40, "total": 160,
    }],
    "longest_film_gaps": [{
        "id": 13, "name": "Suriya", "industry": "Tamil", "film_count": 40,
        "gap_start": 2019, "gap_end": 2025, "gap_years": 6,
    }],
    "most_multilingual": [{
        "id": 14, "name": "Kamal Haasan", "industry": "Tamil", "lang_count": 6,
        "languages": ["Tamil", "Telugu", "Hindi", "Malayalam", "Kannada", "Bengali"],
        "films_with_lang": 100, "total_films": 150,
    }],
    "collaboration_diversity": [{
        "id": 15, "name": "Fahadh Faasil", "industry": "Malayalam",
        "unique_directors": 45, "film_count": 55, "pct": 82,
    }],
    "shortest_path": [{
        "actor1_id": 16, "actor1_name": "Yash", "industry1": "Kannada",
        "actor2_id": 17, "actor2_name": "Dulquer Salmaan", "industry2": "Malayalam",
        "middle_id": 18, "middle_name": "Prakash Raj", "bridge_strength": 9,
    }],
}

_PROSE_MARKERS = (" the ", " is ", " was ", " who ", " has ", " have ")


def _no_prose(value) -> bool:
    """Structured facts may hold names/titles but never sentences."""
    if isinstance(value, str):
        low = f" {value.lower()} "
        return not any(m in low for m in _PROSE_MARKERS)
    if isinstance(value, dict):
        return all(_no_prose(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return all(_no_prose(v) for v in value)
    return True


def test_all_rules_registered():
    # debut_ages was removed in the v1 hardening pass — actors.debut_year is
    # 100% NULL; reinstate the rule once the column is backfilled.
    assert len(all_rules()) == 14


@pytest.mark.parametrize("rule_name", sorted(SAMPLE_ROWS))
def test_rule_produces_valid_insights(rule_name):
    rule = get_rule(rule_name)
    insights = rule.rows_to_insights(SAMPLE_ROWS[rule_name])
    assert insights, f"{rule_name} produced no insights from sample rows"
    for ins in insights:
        # Round-trips through the model contract
        Insight.model_validate(ins.model_dump())
        assert ins.rule == rule_name
        assert ins.entities and ins.metrics
        # NO human-language prose in facts
        assert _no_prose(ins.facts), f"{rule_name} facts contain prose: {ins.facts}"
        assert 0.0 <= ins.completeness <= 1.0


def test_every_registered_rule_has_sample_rows():
    missing = {r.name for r in all_rules()} - set(SAMPLE_ROWS)
    assert not missing, f"add sample rows for: {missing}"
