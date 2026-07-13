from engine.models import Entity, Insight, Metric, RankedInsight, Score
from engine.scheduler.planner import DiversityLimits, PlanHistory, plan_slots
from engine.shared.fingerprint import canonical_fingerprint


def _ri(rule, actor_id, name, industry, score):
    ins = Insight(
        rule=rule,
        entities=[Entity(kind="actor", id=actor_id, name=name)],
        metrics=[Metric(key="k", value=10, unit="films")],
        facts={"industry": industry},
    )
    return RankedInsight(insight=ins, fingerprint=canonical_fingerprint(ins),
                         score=Score(total=score, components={}, weights_version="t"))


def test_one_rule_per_day():
    ranked = [_ri("network_power", i, f"A{i}", "Tamil", 0.9 - i*0.01) for i in range(6)]
    out = plan_slots(ranked, n_slots=4)
    assert len({r.insight.rule for r in out}) == 1  # only one network_power allowed
    assert len(out) == 1


def test_category_cap_per_day():
    # two collaboration rules + two graph rules; cat cap = 2 → all 4 fit
    ranked = [
        _ri("collaboration_shock", 1, "A", "Tamil", 0.9),
        _ri("most_frequent_costars", 2, "B", "Telugu", 0.85),
        _ri("network_power", 3, "C", "Malayalam", 0.8),
        _ri("shortest_path", 4, "D", "Kannada", 0.75),
    ]
    out = plan_slots(ranked, n_slots=4)
    assert len(out) == 4


def test_weekly_rule_cap_blocks():
    ranked = [_ri("network_power", 1, "A", "Tamil", 0.9)]
    hist = PlanHistory(rule_counts_last_week={"network_power": 3})
    out = plan_slots(ranked, n_slots=4, history=hist)
    assert out == []  # already hit weekly cap of 3


def test_weekly_actor_cap_blocks():
    ranked = [_ri("network_power", 7, "A", "Tamil", 0.9)]
    hist = PlanHistory(actor_ids_last_week={7})
    # max_per_actor_per_week default 2: 1 (history) + 1 (today) = 2 → allowed once,
    # a second same-actor insight would be blocked
    ranked2 = ranked + [_ri("longest_careers", 7, "A", "Tamil", 0.8)]
    out = plan_slots(ranked2, n_slots=4, history=hist)
    assert len(out) == 1


def test_no_adjacent_same_category():
    ranked = [
        _ri("collaboration_shock", 1, "A", "Tamil", 0.95),
        _ri("most_frequent_costars", 2, "B", "Telugu", 0.90),
        _ri("network_power", 3, "C", "Malayalam", 0.85),
    ]
    # raise category cap so both collaboration items are chosen
    out = plan_slots(ranked, n_slots=3,
                     limits=DiversityLimits(max_per_category_per_day=5))
    cats = [__import__("engine.shared.categories", fromlist=["category_of"])
            .category_of(r.insight.rule) for r in out]
    assert all(cats[i] != cats[i+1] for i in range(len(cats)-1))
