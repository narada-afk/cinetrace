from engine.config import EngineConfig
from engine.dedup.deduper import dedup
from engine.models import Score
from engine.models import RankedInsight
from engine.shared.fingerprint import canonical_fingerprint


def _ranked(insight, total=0.5):
    return RankedInsight(
        insight=insight,
        fingerprint=canonical_fingerprint(insight),
        score=Score(total=total, components={}, weights_version="test"),
    )


def test_cooldown_drops_fingerprint(duo_insight):
    r = _ranked(duo_insight)
    assert dedup([r], {r.fingerprint}, set(), EngineConfig()) == []
    assert dedup([r], set(), set(), EngineConfig()) == [r]


def test_batch_keeps_best_per_fingerprint(duo_insight):
    a = _ranked(duo_insight, total=0.9)
    b = _ranked(duo_insight, total=0.4)
    out = dedup([a, b], set(), set(), EngineConfig())
    assert out == [a]


def test_max_one_insight_per_actor(duo_insight, solo_insight):
    # second insight reuses actor id 1 → dropped
    other = solo_insight.model_copy()
    other.entities[0].id = 1
    out = dedup([_ranked(duo_insight), _ranked(other)], set(), set(), EngineConfig())
    assert len(out) == 1


def test_recently_used_actor_dropped(duo_insight):
    out = dedup([_ranked(duo_insight)], set(), {1}, EngineConfig())
    assert out == []
