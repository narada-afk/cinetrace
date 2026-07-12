from engine.generators._validator import allowed_numbers, extract_numbers, validate


def test_extract_numbers_handles_commas():
    assert extract_numbers("₹1,200 Cr across 44 films") == [1200.0, 44.0]


def test_valid_tweet_passes(duo_insight):
    ok, violations = validate(
        "Mohanlal and Priyadarshan: 44 films together. Nothing since 2014.",
        duo_insight,
    )
    assert ok, violations


def test_hallucinated_number_fails(duo_insight):
    ok, violations = validate(
        "Mohanlal and Priyadarshan made 57 films together.",
        duo_insight,
    )
    assert not ok
    assert any("57" in v for v in violations)


def test_derived_year_gap_allowed(solo_insight):
    # 1990 - 1986 = 4 and inclusive span 5 are both derivable
    allowed = allowed_numbers(solo_insight)
    assert 4.0 in allowed and 5.0 in allowed


def test_missing_primary_entity_fails(duo_insight):
    ok, violations = validate("A great pair: 44 films together.", duo_insight)
    assert not ok
    assert any("Mohanlal" in v for v in violations)
