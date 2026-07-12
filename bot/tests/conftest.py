import os
import sys

# Make bot/ importable (engine, config, …) when pytest runs from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Engine modules import bot config.py which requires many env vars — stub them
_REQUIRED = [
    "TWITTER_API_KEY", "TWITTER_API_SECRET", "TWITTER_BEARER_TOKEN",
    "TWITTER_ACCESS_TOKEN", "TWITTER_ACCESS_TOKEN_SECRET",
    "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN", "DATABASE_URL",
]
for var in _REQUIRED:
    os.environ.setdefault(var, "test-placeholder")
os.environ.setdefault("TELEGRAM_CHAT_ID", "0")

import pytest

from engine.models import Entity, Insight, Metric


@pytest.fixture
def duo_insight() -> Insight:
    return Insight(
        rule="collaboration_shock",
        entities=[
            Entity(kind="actor", id=1, name="Mohanlal", slug="mohanlal"),
            Entity(kind="actor", id=2, name="Priyadarshan", slug="priyadarshan"),
        ],
        metrics=[
            Metric(key="collab_count", value=44, unit="films"),
            Metric(key="years_since_last", value=12, unit="years"),
        ],
        facts={"last_film_year": 2014, "industry": "Malayalam"},
    )


@pytest.fixture
def solo_insight() -> Insight:
    return Insight(
        rule="career_peak_window",
        entities=[Entity(kind="actor", id=7, name="Chiranjeevi", slug="chiranjeevi")],
        metrics=[
            Metric(key="films_in_window", value=38, unit="films", period=(1986, 1990)),
            Metric(key="total_films", value=150, unit="films"),
        ],
        facts={"window_years": 5, "industry": "Telugu", "share_of_career_pct": 25},
    )
