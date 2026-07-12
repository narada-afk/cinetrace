from engine.config import EngineConfig
from engine.ranking import features
from engine.ranking.features import RankContext
from engine.ranking.ranker import rank, score_insight
from engine.shared.fingerprint import canonical_fingerprint


def test_novelty_decays_with_history(duo_insight):
    fp = canonical_fingerprint(duo_insight)
    fresh = features.novelty(duo_insight, fp, RankContext())
    seen = features.novelty(duo_insight, fp, RankContext(fingerprint_history={fp: 3}))
    assert fresh == 1.0 and seen < fresh


def test_surprise_scales_with_magnitude(duo_insight, solo_insight):
    big = duo_insight.model_copy()
    big.metrics[0].value = 500
    assert features.surprise(big, RankContext()) > features.surprise(solo_insight, RankContext())


def test_popularity_uses_fame_stats(duo_insight):
    ctx = RankContext(fame_stats={
        1: {"film_count": 300, "costar_count": 250, "is_primary": True},
        2: {"film_count": 200, "costar_count": 150, "is_primary": True},
    })
    assert features.popularity(duo_insight, ctx) > 0.9
    assert features.popularity(duo_insight, RankContext()) == 0.4  # neutral, no stats


def test_score_composes_weights(solo_insight):
    config = EngineConfig()
    ranked = score_insight(solo_insight, RankContext(), config)
    assert 0 <= ranked.score.total <= 1
    assert set(ranked.score.components) == set(config.weights)
    assert ranked.score.weights_version == config.weights_version


def test_rank_filters_low_completeness(solo_insight):
    config = EngineConfig()
    thin = solo_insight.model_copy(update={"completeness": 0.1})
    out = rank([solo_insight, thin], RankContext(), config)
    assert len(out) == 1
    assert out[0].insight.completeness == 1.0


def test_rank_sorted_best_first(duo_insight, solo_insight):
    ctx = RankContext(fame_stats={
        1: {"film_count": 300, "costar_count": 250, "is_primary": True},
        2: {"film_count": 300, "costar_count": 250, "is_primary": True},
        7: {"film_count": 30, "costar_count": 10, "is_primary": True},
    })
    out = rank([solo_insight, duo_insight], ctx, EngineConfig())
    totals = [r.score.total for r in out]
    assert totals == sorted(totals, reverse=True)
