from engine.models import Entity, Insight, Metric
from engine.shared.fingerprint import canonical_fingerprint


def _make(rule, entities, key, value):
    return Insight(rule=rule, entities=entities,
                   metrics=[Metric(key=key, value=value, unit="films")])


def test_entity_order_does_not_matter(duo_insight):
    reversed_entities = list(reversed(duo_insight.entities))
    flipped = duo_insight.model_copy(update={"entities": reversed_entities})
    assert canonical_fingerprint(duo_insight) == canonical_fingerprint(flipped)


def test_value_bucketing_collides_nearby_values():
    a = _make("r", [Entity(kind="actor", id=1, name="X")], "collab_count", 44)
    b = _make("r", [Entity(kind="actor", id=1, name="X")], "collab_count", 45)
    assert canonical_fingerprint(a) == canonical_fingerprint(b)


def test_distant_values_differ():
    a = _make("r", [Entity(kind="actor", id=1, name="X")], "collab_count", 44)
    b = _make("r", [Entity(kind="actor", id=1, name="X")], "collab_count", 90)
    assert canonical_fingerprint(a) != canonical_fingerprint(b)


def test_different_rules_differ(duo_insight):
    other = duo_insight.model_copy(update={"rule": "most_frequent_costars"})
    assert canonical_fingerprint(duo_insight) != canonical_fingerprint(other)


def test_director_without_id_uses_name():
    a = _make("r", [Entity(kind="director", name="Priyadarshan")], "k", 10)
    b = _make("r", [Entity(kind="director", name="priyadarshan ")], "k", 10)
    assert canonical_fingerprint(a) == canonical_fingerprint(b)
