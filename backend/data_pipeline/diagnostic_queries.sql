-- ─────────────────────────────────────────────────────────────────────────────
-- South Cinema Analytics — Data Diagnostic Queries
-- ─────────────────────────────────────────────────────────────────────────────
-- Run any query standalone:
--   psql $DATABASE_URL -f data_pipeline/diagnostic_queries.sql
-- Or paste individual queries into psql / DBeaver.
-- ─────────────────────────────────────────────────────────────────────────────


-- ── 1. Movies with NO director at all ────────────────────────────────────────
-- Checks both the normalized movie_directors table AND the legacy director TEXT.
-- These are highest-priority data gaps.

SELECT m.id, m.title, m.release_year, m.tmdb_id
FROM   movies m
WHERE  NOT EXISTS (
    SELECT 1 FROM movie_directors md WHERE md.movie_id = m.id
)
AND    (m.director IS NULL OR m.director = '')
ORDER  BY m.release_year DESC;


-- ── 2. Movies with a legacy director TEXT but NOT in movie_directors ──────────
-- These were ingested before the director normalization sprint.
-- They should be migrated via enrich_directors.py.

SELECT m.id, m.title, m.release_year, m.director AS legacy_director
FROM   movies m
WHERE  m.director IS NOT NULL
  AND  m.director != ''
  AND  NOT EXISTS (
    SELECT 1 FROM movie_directors md WHERE md.movie_id = m.id
)
ORDER  BY m.release_year DESC;


-- ── 3. Movies with NO primary actor in actor_movies ───────────────────────────
-- These have cast data but none is classified as 'primary'.
-- Could indicate ingestion gaps or misclassified roles.

SELECT m.id, m.title, m.release_year,
       COUNT(am.actor_id) FILTER (WHERE am.role_type = 'supporting') AS supporting_count,
       COUNT(am.actor_id) FILTER (WHERE am.role_type = 'primary')    AS primary_count
FROM   movies m
LEFT   JOIN actor_movies am ON am.movie_id = m.id
WHERE  m.tmdb_id IS NOT NULL     -- only check enriched movies
GROUP  BY m.id, m.title, m.release_year
HAVING COUNT(am.actor_id) FILTER (WHERE am.role_type = 'primary') = 0
ORDER  BY m.release_year DESC;


-- ── 4. Movies with NO cast at all (neither table) ─────────────────────────────

SELECT m.id, m.title, m.release_year, m.tmdb_id
FROM   movies m
WHERE  NOT EXISTS (SELECT 1 FROM actor_movies am WHERE am.movie_id = m.id)
  AND  NOT EXISTS (SELECT 1 FROM "cast" c      WHERE c.movie_id   = m.id)
  AND  m.tmdb_id IS NOT NULL
ORDER  BY m.release_year DESC;


-- ── 5. Duplicate cast entries in actor_movies ─────────────────────────────────
-- Same actor appearing more than once in actor_movies for the same movie.

SELECT am.movie_id, m.title, a.name AS actor_name, COUNT(*) AS occurrences
FROM   actor_movies am
JOIN   movies  m ON m.id = am.movie_id
JOIN   actors  a ON a.id = am.actor_id
GROUP  BY am.movie_id, m.title, a.name
HAVING COUNT(*) > 1
ORDER  BY occurrences DESC, m.title;


-- ── 6. Duplicate cast entries in the legacy cast table ───────────────────────

SELECT c.movie_id, m.title, a.name AS actor_name, COUNT(*) AS occurrences
FROM   "cast" c
JOIN   movies m ON m.id = c.movie_id
JOIN   actors a ON a.id = c.actor_id
GROUP  BY c.movie_id, m.title, a.name
HAVING COUNT(*) > 1
ORDER  BY occurrences DESC, m.title;


-- ── 7. Duplicate movies (same title + same release year) ─────────────────────
-- Exact title match.  Use the trigram index for fuzzy near-duplicates instead.

SELECT title, release_year, COUNT(*) AS copies,
       array_agg(id ORDER BY id) AS movie_ids
FROM   movies
GROUP  BY title, release_year
HAVING COUNT(*) > 1
ORDER  BY copies DESC, title;


-- ── 8. Near-duplicate movies (trigram similarity ≥ 0.7, same year) ───────────
-- Requires pg_trgm extension (Sprint 25 migration).
-- Catches transliteration variants: "Vikram" vs "Vikrum", etc.

SELECT a.id AS id_a, a.title AS title_a,
       b.id AS id_b, b.title AS title_b,
       a.release_year,
       similarity(a.title, b.title) AS sim
FROM   movies a
JOIN   movies b
       ON  a.id < b.id
       AND a.release_year = b.release_year
       AND similarity(a.title, b.title) >= 0.70
ORDER  BY sim DESC;


-- ── 9. Invalid / missing ratings ─────────────────────────────────────────────

SELECT id, title, release_year, vote_average
FROM   movies
WHERE  vote_average IS NOT NULL
  AND  (vote_average < 0 OR vote_average > 10)
ORDER  BY title;

-- Count of movies still missing ratings
SELECT COUNT(*) AS missing_ratings
FROM   movies
WHERE  vote_average IS NULL AND tmdb_id IS NOT NULL;


-- ── 10. Invalid box-office values ─────────────────────────────────────────────

SELECT id, title, release_year, box_office
FROM   movies
WHERE  box_office IS NOT NULL
  AND  (
       box_office < 0
    OR box_office = 0
    OR box_office > 50000     -- > ₹50,000 crore is physically impossible
  )
ORDER  BY box_office DESC;


-- ── 11. Movies without a tmdb_id (cannot be cross-validated) ─────────────────

SELECT COUNT(*)                                      AS total_movies,
       COUNT(*) FILTER (WHERE tmdb_id IS NOT NULL)   AS with_tmdb_id,
       COUNT(*) FILTER (WHERE tmdb_id IS NULL)       AS without_tmdb_id,
       ROUND(
           COUNT(*) FILTER (WHERE tmdb_id IS NOT NULL)::numeric
           / COUNT(*)::numeric * 100, 1
       )                                             AS tmdb_coverage_pct
FROM movies;


-- ── 12. Actor coverage: actors in DB but NOT matched to any TMDB person ───────

SELECT COUNT(*)                                           AS total_actors,
       COUNT(*) FILTER (WHERE tmdb_person_id IS NOT NULL) AS with_tmdb_id,
       COUNT(*) FILTER (WHERE tmdb_person_id IS NULL)     AS without_tmdb_id
FROM actors;


-- ── 13. Validation results summary (after running validate_all_movies) ─────────

SELECT status,
       COUNT(*)                        AS count,
       ROUND(AVG(confidence_score), 3) AS avg_confidence,
       ROUND(MIN(confidence_score), 3) AS min_confidence,
       ROUND(MAX(confidence_score), 3) AS max_confidence
FROM   movie_validation_results
GROUP  BY status
ORDER  BY avg_confidence DESC;


-- ── 14. Top 20 most-broken movies ─────────────────────────────────────────────

SELECT mvr.movie_id, m.title, m.release_year,
       mvr.confidence_score, mvr.status,
       mvr.issues
FROM   movie_validation_results mvr
JOIN   movies m ON m.id = mvr.movie_id
WHERE  mvr.status = 'BROKEN'
ORDER  BY mvr.confidence_score ASC
LIMIT  20;


-- ── 15. Issue frequency breakdown ─────────────────────────────────────────────
-- Which data problems are most common across all validated movies?

SELECT issue,
       COUNT(*) AS affected_movies
FROM   movie_validation_results,
       jsonb_array_elements_text(issues) AS issue
GROUP  BY issue
ORDER  BY affected_movies DESC
LIMIT  20;


-- ── 16. Stale validation results (not checked in the past 7 days) ─────────────

SELECT movie_id, confidence_score, status, last_checked_at
FROM   movie_validation_results
WHERE  last_checked_at < NOW() - INTERVAL '7 days'
ORDER  BY last_checked_at ASC;
