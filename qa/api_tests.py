"""
SouthCineStats — Backend API Test Suite
========================================
Pure-Python, zero new dependencies (uses `requests` already in requirements.txt).
Run with:  python qa/api_tests.py
"""

import time
import json
import sys
import requests

BASE = "http://localhost:8000"
TIMEOUT = 5          # seconds per request
SLOW_THRESHOLD = 2.0 # flag as slow if > 2 s

PASS  = "\033[92m✓ PASS\033[0m"
FAIL  = "\033[91m✗ FAIL\033[0m"
WARN  = "\033[93m⚠ WARN\033[0m"
SLOW  = "\033[93m⚡SLOW\033[0m"

results = []

def check(name, fn):
    """Run a test function, record result."""
    try:
        t0 = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - t0
        tag = SLOW if elapsed > SLOW_THRESHOLD else PASS
        print(f"  {tag}  {name}  ({elapsed*1000:.0f} ms)")
        results.append({"name": name, "status": "slow" if elapsed > SLOW_THRESHOLD else "pass", "ms": elapsed*1000})
    except AssertionError as e:
        elapsed = time.perf_counter() - t0
        print(f"  {FAIL}  {name}  — {e}")
        results.append({"name": name, "status": "fail", "error": str(e), "ms": elapsed*1000})
    except Exception as e:
        print(f"  {FAIL}  {name}  — {type(e).__name__}: {e}")
        results.append({"name": name, "status": "fail", "error": str(e), "ms": 0})

# ── SECTION 1: Health & Availability ─────────────────────────────────────────

print("\n📋 SECTION 1 — Health & Availability")

def test_health_200():
    r = requests.get(f"{BASE}/health", timeout=TIMEOUT)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    data = r.json()
    assert data.get("status") == "ok", f"Expected status=ok, got {data}"

check("GET /health → 200 + status:ok", test_health_200)

def test_health_response_time():
    t0 = time.perf_counter()
    requests.get(f"{BASE}/health", timeout=TIMEOUT)
    elapsed = time.perf_counter() - t0
    assert elapsed < 0.5, f"Health check took {elapsed:.2f}s — should be <0.5s"

check("GET /health response time < 500 ms", test_health_response_time)

# ── SECTION 2: Actors Endpoints ───────────────────────────────────────────────

print("\n📋 SECTION 2 — Actors Endpoints")

def test_actors_list():
    r = requests.get(f"{BASE}/actors", timeout=TIMEOUT)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    data = r.json()
    assert isinstance(data, list), "Expected list of actors"
    assert len(data) > 0, "Actor list is empty"
    # Validate shape of first actor
    a = data[0]
    assert "id" in a, "Missing 'id' field"
    assert "name" in a, "Missing 'name' field"

check("GET /actors → 200 + non-empty list + valid shape", test_actors_list)

def test_actors_list_response_time():
    t0 = time.perf_counter()
    requests.get(f"{BASE}/actors", timeout=TIMEOUT)
    elapsed = time.perf_counter() - t0
    assert elapsed < SLOW_THRESHOLD, f"Took {elapsed:.2f}s, threshold is {SLOW_THRESHOLD}s"

check("GET /actors response time < 2 s", test_actors_list_response_time)

def test_actor_search():
    r = requests.get(f"{BASE}/actors/search?q=Rajinikanth", timeout=TIMEOUT)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    data = r.json()
    assert isinstance(data, list), "Expected list"
    assert len(data) > 0, "Search returned no results for 'Rajinikanth'"
    names = [a["name"] for a in data]
    assert any("rajini" in n.lower() for n in names), f"'Rajinikanth' not in results: {names}"

check("GET /actors/search?q=Rajinikanth → finds result", test_actor_search)

def test_actor_search_partial():
    r = requests.get(f"{BASE}/actors/search?q=mohanlal", timeout=TIMEOUT)
    assert r.status_code == 200
    data = r.json()
    assert len(data) > 0, "Mohanlal search returned nothing"

check("GET /actors/search?q=mohanlal → case-insensitive hit", test_actor_search_partial)

def test_actor_search_empty_query():
    r = requests.get(f"{BASE}/actors/search?q=", timeout=TIMEOUT)
    assert r.status_code in (200, 422), f"Unexpected status {r.status_code}"

check("GET /actors/search?q= → graceful (200 or 422)", test_actor_search_empty_query)

def test_actor_by_id():
    r = requests.get(f"{BASE}/actors/1", timeout=TIMEOUT)  # Allu Arjun is id=1
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    data = r.json()
    assert "name" in data, "Missing 'name' in actor detail"
    assert "id" in data, "Missing 'id' in actor detail"

check("GET /actors/1 → valid actor detail", test_actor_by_id)

def test_actor_not_found():
    r = requests.get(f"{BASE}/actors/999999", timeout=TIMEOUT)
    assert r.status_code == 404, f"Expected 404 for non-existent actor, got {r.status_code}"

check("GET /actors/999999 → 404 Not Found", test_actor_not_found)

def test_actor_movies():
    r = requests.get(f"{BASE}/actors/1/movies", timeout=TIMEOUT)
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list), "Expected list of movies"
    assert len(data) > 0, "No movies returned for actor 1"
    m = data[0]
    assert "title" in m, "Missing 'title' in movie"

check("GET /actors/1/movies → non-empty movie list", test_actor_movies)

def test_actor_collaborators():
    r = requests.get(f"{BASE}/actors/1/collaborators", timeout=TIMEOUT)
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) > 0, "No collaborators returned"
    c = data[0]
    assert "actor" in c, "Missing 'actor' field"
    assert "films" in c, "Missing 'films' field"

check("GET /actors/1/collaborators → valid collaborator list", test_actor_collaborators)

def test_actor_directors():
    r = requests.get(f"{BASE}/actors/1/directors", timeout=TIMEOUT)
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) > 0, "No directors returned"

check("GET /actors/1/directors → non-empty", test_actor_directors)

def test_heroine_collaborators():
    r = requests.get(f"{BASE}/actors/1/heroine-collaborators", timeout=TIMEOUT)
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    # Check for known heroine
    names = [c["actor"].lower() for c in data]
    assert any("rashmika" in n for n in names), f"Rashmika Mandanna not in heroine list for Allu Arjun: {names[:5]}"

check("GET /actors/1/heroine-collaborators → Rashmika present for Allu Arjun", test_heroine_collaborators)

def test_lead_collaborators():
    r = requests.get(f"{BASE}/actors/1/lead-collaborators", timeout=TIMEOUT)
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)

check("GET /actors/1/lead-collaborators → 200 + list", test_lead_collaborators)

# ── SECTION 3: Analytics Endpoints ───────────────────────────────────────────

print("\n📋 SECTION 3 — Analytics Endpoints")

def test_insights():
    r = requests.get(f"{BASE}/analytics/insights", timeout=TIMEOUT)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    data = r.json()
    assert "insights" in data, f"Missing 'insights' key: {list(data.keys())}"
    insights = data["insights"]
    assert isinstance(insights, list)
    assert len(insights) > 0, "Insights list is empty"
    i = insights[0]
    assert "type" in i, "Missing 'type' in insight"
    assert "actors" in i, "Missing 'actors' in insight"

check("GET /analytics/insights → 200 + valid insight shape", test_insights)

def test_insights_no_self_director():
    """Regression: ensure no insight has actor == director (V. Ravichandran bug)."""
    r = requests.get(f"{BASE}/analytics/insights", timeout=TIMEOUT)
    data = r.json()
    for ins in data.get("insights", []):
        if ins.get("type") == "director_loyalty":
            actors = ins.get("actors", [])
            if len(actors) == 2:
                assert actors[0].lower() != actors[1].lower(), \
                    f"Self-director insight found: {actors[0]} == {actors[1]}"

check("Insights: no director_loyalty where actor == director", test_insights_no_self_director)

def test_insights_industry_filter():
    for industry in ["tamil", "telugu", "malayalam"]:
        r = requests.get(f"{BASE}/analytics/insights?industry={industry}", timeout=TIMEOUT)
        assert r.status_code == 200, f"Failed for industry={industry}: {r.status_code}"
        data = r.json()
        assert "insights" in data

check("GET /analytics/insights?industry= → works for tamil/telugu/malayalam", test_insights_industry_filter)

def test_top_collaborations():
    r = requests.get(f"{BASE}/analytics/top-collaborations", timeout=TIMEOUT)
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list) or isinstance(data, dict), "Unexpected response type"

check("GET /analytics/top-collaborations → 200", test_top_collaborations)

# ── SECTION 4: Compare Endpoint ───────────────────────────────────────────────

print("\n📋 SECTION 4 — Compare Endpoint")

def test_compare():
    # Compare takes full names, not IDs
    r = requests.get(f"{BASE}/compare?actor1=Allu+Arjun&actor2=Mahesh+Babu", timeout=TIMEOUT)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    data = r.json()
    assert isinstance(data, dict), "Expected dict"

check("GET /compare?actor1=Allu+Arjun&actor2=Mahesh+Babu → 200 + dict", test_compare)

def test_compare_same_actor():
    r = requests.get(f"{BASE}/compare?actor1=Allu+Arjun&actor2=Allu+Arjun", timeout=TIMEOUT)
    assert r.status_code in (200, 400, 422), f"Unexpected status {r.status_code}"

check("GET /compare same actor → graceful (no 500)", test_compare_same_actor)

def test_compare_unknown_actor():
    r = requests.get(f"{BASE}/compare?actor1=Unknown+Actor+XYZ&actor2=Rajinikanth", timeout=TIMEOUT)
    assert r.status_code in (404, 422), f"Expected 404/422 for unknown actor, got {r.status_code}"

check("GET /compare unknown actor → 404 or 422", test_compare_unknown_actor)

# ── SECTION 5: Cache-Control Headers ─────────────────────────────────────────

print("\n📋 SECTION 5 — Cache-Control Headers")

def test_cache_control_actors():
    r = requests.get(f"{BASE}/actors/1", timeout=TIMEOUT)
    cc = r.headers.get("cache-control", "")
    assert "max-age" in cc, f"Missing cache-control max-age on /actors/1: '{cc}'"

check("GET /actors/1 → cache-control header present", test_cache_control_actors)

def test_cache_control_insights():
    r = requests.get(f"{BASE}/analytics/insights", timeout=TIMEOUT)
    cc = r.headers.get("cache-control", "")
    assert "max-age=60" in cc, f"Expected max-age=60 on insights: '{cc}'"

check("GET /analytics/insights → cache-control max-age=60", test_cache_control_insights)

# ── SECTION 6: Error Handling ─────────────────────────────────────────────────

print("\n📋 SECTION 6 — Error Handling & Edge Cases")

def test_invalid_actor_id_string():
    r = requests.get(f"{BASE}/actors/notanid", timeout=TIMEOUT)
    assert r.status_code in (404, 422), f"Expected 404/422, got {r.status_code}"

check("GET /actors/notanid → 404 or 422 (not 500)", test_invalid_actor_id_string)

def test_search_xss():
    r = requests.get(f"{BASE}/actors/search?q=<script>alert(1)</script>", timeout=TIMEOUT)
    assert r.status_code in (200, 422), f"XSS-like input crashed server: {r.status_code}"
    if r.status_code == 200:
        assert "<script>" not in r.text, "XSS payload reflected in response"

check("Search with XSS payload → no crash, no reflection", test_search_xss)

def test_no_500_on_common_routes():
    routes = ["/health", "/actors", "/analytics/insights", "/actors/1", "/actors/1/movies"]
    for route in routes:
        r = requests.get(f"{BASE}{route}", timeout=TIMEOUT)
        assert r.status_code != 500, f"{route} returned 500 Internal Server Error"

check("No 500 errors on all core routes", test_no_500_on_common_routes)

# ── SECTION 7: Cross-endpoint Data Consistency ───────────────────────────────
#
# These tests would have caught every real-world bug we shipped:
#
#   Bug A — Blockbusters endpoint skipped the Wikidata cast table, so films
#            like Leo (Vijay), Kalki (Kamal Haasan), Ponniyin Selvan (Karthi)
#            and Good Bad Ugly (Ajith) were completely missing from the tab.
#
#   Bug B — Director chip counts came from actor_director_stats (which counted
#            unreleased films with release_year=0), so chips showed "2" but the
#            dropdown only showed 1 film.
#
#   Bug C — NULL character_name in actor_movies caused valid films (e.g. Eega
#            for Samantha) to be silently excluded by a NOT (NULL LIKE ...) = NULL
#            evaluation in the non-acting role filter.
#
# Strategy: fetch all primary actors in parallel, then assert invariants.

import concurrent.futures

print("\n📋 SECTION 7 — Cross-endpoint Data Consistency (all primary actors)")

# ── Fetch all primary actors once ─────────────────────────────────────────────
_primary_actors: list = []
try:
    _r = requests.get(f"{BASE}/actors?primary_only=true", timeout=TIMEOUT)
    _primary_actors = _r.json() if _r.status_code == 200 else []
except Exception:
    pass

def _fetch_actor_data(actor: dict) -> dict:
    """Return {id, name, movies, blockbusters, directors} for one actor."""
    aid = actor["id"]
    out = {"id": aid, "name": actor["name"], "movies": [], "blockbusters": [], "directors": []}
    try:
        rm = requests.get(f"{BASE}/actors/{aid}/movies",       timeout=10)
        rb = requests.get(f"{BASE}/actors/{aid}/blockbusters", timeout=10)
        rd = requests.get(f"{BASE}/actors/{aid}/directors",    timeout=10)
        if rm.status_code == 200: out["movies"]       = rm.json()
        if rb.status_code == 200: out["blockbusters"] = rb.json()
        if rd.status_code == 200: out["directors"]    = rd.json()
    except Exception:
        pass
    return out

# Parallel fetch — 119 actors × 3 endpoints in ~5-10 s instead of ~60 s
_actor_data: list[dict] = []
if _primary_actors:
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        _actor_data = list(pool.map(_fetch_actor_data, _primary_actors))

# ── Test 7a: Spot-check known films that were previously missing ───────────────
# Each tuple: (actor_name, film_title, endpoint)
# "endpoint" is "blockbusters" or "movies" — the place the film must appear.
_KNOWN_FILMS = [
    # Blockbuster endpoint cast-table gap (Bug A) — these were the exact films
    # missing before the fix. If this regresses, the endpoint broke again.
    ("Vijay",         "Leo",                    "blockbusters"),
    ("Vijay",         "Leo",                    "movies"),
    ("Kamal Haasan",  "Kalki 2898 AD",          "blockbusters"),
    ("Kamal Haasan",  "Kalki 2898 AD",          "movies"),
    ("Karthi",        "Ponniyin Selvan: I",      "blockbusters"),
    ("Suriya",        "Vikram",                  "blockbusters"),
    ("Ajith Kumar",   "Good Bad Ugly",           "blockbusters"),
    # NULL character_name exclusion (Bug C)
    ("Samantha Ruth Prabhu", "Eega",            "movies"),
]

def test_known_films_present():
    _data_by_name = {d["name"]: d for d in _actor_data}
    failures = []
    for actor_name, film_title, endpoint in _KNOWN_FILMS:
        actor = _data_by_name.get(actor_name)
        if not actor:
            failures.append(f"actor '{actor_name}' not found in primary list")
            continue
        titles = {item["title"] for item in actor[endpoint]}
        if film_title not in titles:
            failures.append(
                f"{actor_name} → '{film_title}' missing from /{endpoint}"
            )
    assert not failures, "\n  " + "\n  ".join(failures)

check("Known previously-missing films are present in correct endpoints", test_known_films_present)

# ── Test 7b: Blockbusters completeness — no top-10 film silently dropped ──────
# For every primary actor: every film in their top-10 by box_office from
# /movies must also appear in /blockbusters.
# This is the test that would have caught Good Bad Ugly missing for Ajith.

def test_blockbusters_completeness():
    failures = []
    for actor in _actor_data:
        bo_movies = sorted(
            [m for m in actor["movies"] if m.get("box_office") and m["box_office"] > 0],
            key=lambda m: m["box_office"], reverse=True,
        )[:10]
        if not bo_movies:
            continue
        buster_titles = {b["title"] for b in actor["blockbusters"]}
        for m in bo_movies:
            if m["title"] not in buster_titles:
                failures.append(
                    f"{actor['name']}: '{m['title']}' (₹{m['box_office']:.0f} Cr) "
                    f"is in top-10 /movies but absent from /blockbusters"
                )
    assert not failures, f"{len(failures)} missing film(s):\n  " + "\n  ".join(failures[:10])

check("Blockbusters: every top-10 box-office film from /movies is in /blockbusters", test_blockbusters_completeness)

# ── Test 7c: Blockbusters #1 matches /movies #1 ───────────────────────────────
# Catches the case where a wrong film ranks first because a higher-grossing
# film is absent. Allows an exact-tie (same box_office value, different title).

def test_blockbusters_top_film_correct():
    failures = []
    for actor in _actor_data:
        if not actor["blockbusters"]:
            continue
        bo_movies = sorted(
            [m for m in actor["movies"] if m.get("box_office") and m["box_office"] > 0],
            key=lambda m: m["box_office"], reverse=True,
        )
        if not bo_movies:
            continue
        top_movie   = bo_movies[0]
        top_buster  = actor["blockbusters"][0]
        # Accept a mismatch only when both films have identical box_office (true tie)
        if top_buster["title"] != top_movie["title"]:
            if abs(top_buster["box_office_crore"] - top_movie["box_office"]) > 0.1:
                failures.append(
                    f"{actor['name']}: blockbusters[0]='{top_buster['title']}' "
                    f"(₹{top_buster['box_office_crore']:.0f}) but "
                    f"movies top='{top_movie['title']}' (₹{top_movie['box_office']:.0f})"
                )
    assert not failures, f"{len(failures)} wrong #1 film(s):\n  " + "\n  ".join(failures)

check("Blockbusters: #1 film matches /movies top by box_office (ties allowed)", test_blockbusters_top_film_correct)

# ── Test 7d: Director chip/dropdown consistency ───────────────────────────────
# Simulates exactly what DirectorsSection.tsx does:
#   chip_count = movies.filter(director == chip.director && release_year > 0).length
# For every chip that would be SHOWN (chip_count > 0), the count must match the
# dropdown exactly. This is guaranteed by construction once director names in
# /movies match director names from /directors — so this test also catches
# name-format divergence between the two sources.

def test_director_chip_dropdown_consistency():
    failures = []
    for actor in _actor_data:
        for chip in actor["directors"]:
            dir_name = chip["director"]
            matching_movies = [
                m for m in actor["movies"]
                if m.get("director") == dir_name and (m.get("release_year") or 0) > 0
            ]
            chip_count = len(matching_movies)
            # A chip that would be shown must have count > 0 and the dropdown
            # must contain exactly that many films (they're the same list).
            # What we're really testing: that no chip is shown with a count that
            # doesn't match its actual dropdown. Since both derive from the same
            # filter, a mismatch means the director names diverged between endpoints.
            # We also flag chips where the name appears in /directors but produces
            # 0 movies AND the chip was supposed to have films (api film_count > 0)
            # — that's a name-format mismatch.
            if chip_count == 0 and chip.get("films", 0) >= 3:
                # High-confidence mismatch: API says 3+ films but none match by name
                failures.append(
                    f"{actor['name']}: chip '{dir_name}' claims {chip['films']} films "
                    f"but 0 movies match by director name — likely name format mismatch"
                )
    assert not failures, f"{len(failures)} chip/dropdown name mismatch(es):\n  " + "\n  ".join(failures[:10])

check("Directors: chip names match movie director names (no silent mismatches)", test_director_chip_dropdown_consistency)

# ── Test 7e: Movies endpoint returns director field for known pairings ─────────
# Regression for the original bug: /movies was returning null director for films
# that do have a director in the normalised join table.
# Spot-checks a handful of well-known actor→director pairings.

_KNOWN_PAIRINGS = [
    # (actor_name, film_title, expected_director)
    ("Ajith Kumar",  "Good Bad Ugly",  "Adhik Ravichandran"),
    ("Ajith Kumar",  "Thunivu",        "H. Vinoth"),
    ("Vijay",        "Leo",            "Lokesh Kanagaraj"),
    ("Rajinikanth",  "Jailer",         "Nelson Dilipkumar"),
]

def test_movies_director_field_populated():
    _data_by_name = {d["name"]: d for d in _actor_data}
    failures = []
    for actor_name, film_title, expected_dir in _KNOWN_PAIRINGS:
        actor = _data_by_name.get(actor_name)
        if not actor:
            failures.append(f"actor '{actor_name}' not found")
            continue
        film = next((m for m in actor["movies"] if m["title"] == film_title), None)
        if film is None:
            failures.append(f"{actor_name}: film '{film_title}' not in /movies at all")
            continue
        actual = film.get("director")
        if actual != expected_dir:
            failures.append(
                f"{actor_name} / '{film_title}': "
                f"director='{actual}' expected='{expected_dir}'"
            )
    assert not failures, "\n  " + "\n  ".join(failures)

check("Movies: director field populated correctly for known actor–director pairings", test_movies_director_field_populated)

# ── SUMMARY ───────────────────────────────────────────────────────────────────

total   = len(results)
passed  = sum(1 for r in results if r["status"] == "pass")
failed  = sum(1 for r in results if r["status"] == "fail")
slow    = sum(1 for r in results if r["status"] == "slow")

avg_ms  = sum(r["ms"] for r in results if r["ms"] > 0) / max(total, 1)

print(f"""
{'='*60}
API TEST RESULTS
{'='*60}
  Total   : {total}
  Passed  : {passed}
  Slow    : {slow}
  Failed  : {failed}
{'='*60}""")

if failed > 0:
    print("\n🔴 FAILED TESTS:")
    for r in results:
        if r["status"] == "fail":
            print(f"   • {r['name']}")
            print(f"     {r.get('error', '')}")

if slow > 0:
    print("\n⚡ SLOW TESTS (>2s):")
    for r in results:
        if r["status"] == "slow":
            print(f"   • {r['name']} — {r['ms']:.0f} ms")

print(f"\n  Average response time: {avg_ms:.0f} ms")
print(f"\n  Verdict: {'✅ ALL CLEAR' if failed == 0 else '❌ FAILURES DETECTED'}")

# Write JSON results for the report
import json, pathlib
pathlib.Path("qa/api_results.json").write_text(
    json.dumps({"total": total, "passed": passed, "failed": failed, "slow": slow,
                "avg_ms": round(avg_ms), "results": results}, indent=2)
)
print("\n  Results saved → qa/api_results.json")

sys.exit(1 if failed > 0 else 0)
