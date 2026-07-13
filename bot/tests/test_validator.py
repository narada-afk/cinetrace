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


def test_year_used_as_duration_fails(solo_insight):
    # 1986 is a calendar year in the payload — "1986 years" must be rejected
    ok, violations = validate(
        "Chiranjeevi disappeared for 1986 years and came back stronger.",
        solo_insight,
    )
    assert not ok
    assert any("calendar year" in v for v in violations)


def test_duration_with_years_unit_passes(duo_insight):
    # years_since_last=12 is a real duration metric
    ok, violations = validate(
        "Mohanlal and Priyadarshan: 44 films, but 12 years of silence.",
        duo_insight,
    )
    assert ok, violations


def test_count_with_wrong_unit_fails(duo_insight):
    # 44 is a film count — "44 decades" is nonsense
    ok, violations = validate(
        "Mohanlal spent 44 decades with Priyadarshan.", duo_insight)
    assert not ok


def test_year_count_as_films_fails(duo_insight):
    # 2014 is in the payload (year) but "2014 films" is a misuse
    ok, violations = validate(
        "Mohanlal made 2014 films with Priyadarshan.", duo_insight)
    assert not ok


def test_derived_span_as_duration_passes(solo_insight):
    # period (1986, 1990) → "5 years" (inclusive) is derivable and valid
    ok, violations = validate(
        "Chiranjeevi: 38 films in 5 years. A golden era.", solo_insight)
    assert ok, violations


def test_missing_primary_entity_fails(duo_insight):
    ok, violations = validate("A great pair: 44 films together.", duo_insight)
    assert not ok
    assert any("Mohanlal" in v for v in violations)
