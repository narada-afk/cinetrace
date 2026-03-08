-- =============================================================================
-- Migration: Sprint 4 — Analytics materialized views
-- File     : backend/migrations/sprint4_materialized_views.sql
-- Target   : PostgreSQL 14+
--
-- What this migration does:
--   Creates two pre-computed materialized views that eliminate heavy
--   multi-table joins for the most common analytics queries.
--
-- Performance impact:
--   Without views → every analytics request JOINs actors + cast + movies
--   With views    → a single indexed scan of a small pre-computed table
--   Typical speedup: 10–100× depending on dataset size
--
-- Refresh strategy:
--   Materialized views are NOT updated automatically.  Run:
--       python -m data_pipeline.refresh_analytics_views
--   after every Wikidata ingestion or Wikipedia enrichment run.
--
-- Safe to re-run:
--   CREATE MATERIALIZED VIEW uses IF NOT EXISTS.
--   DROP + RECREATE is NOT used — existing data is preserved until refresh.
-- =============================================================================

BEGIN;


-- ---------------------------------------------------------------------------
-- View 1: actor_film_counts
-- ---------------------------------------------------------------------------
-- Powers: "Top actors by film count" leaderboard, actor profile pages.
-- Replaces the slow live query:
--   SELECT a.name, COUNT(c.movie_id) FROM actors a JOIN cast c ON ... GROUP BY ...

CREATE MATERIALIZED VIEW IF NOT EXISTS actor_film_counts AS
SELECT
    a.id          AS actor_id,
    a.name        AS actor_name,
    a.industry    AS industry,
    COUNT(c.movie_id) AS film_count
FROM   actors a
JOIN   "cast" c ON a.id = c.actor_id
GROUP  BY a.id, a.name, a.industry
WITH   DATA;    -- populate immediately on creation

-- Unique index on actor_id allows REFRESH MATERIALIZED VIEW CONCURRENTLY
-- (non-blocking refresh while queries continue reading the old data).
CREATE UNIQUE INDEX IF NOT EXISTS idx_actor_film_counts_actor_id
    ON actor_film_counts (actor_id);

-- Supporting index for the most common filter/sort patterns.
CREATE INDEX IF NOT EXISTS idx_actor_film_counts_industry
    ON actor_film_counts (industry);

CREATE INDEX IF NOT EXISTS idx_actor_film_counts_film_count
    ON actor_film_counts (film_count DESC);

COMMENT ON MATERIALIZED VIEW actor_film_counts IS
    'Pre-computed actor film counts. Refresh after ingestion via refresh_analytics_views.py.';


-- ---------------------------------------------------------------------------
-- View 2: actor_director_collaborations
-- ---------------------------------------------------------------------------
-- Powers: "Director collaboration" charts, "who works with whom" analytics.
-- Replaces the live query:
--   SELECT a.name, m.director, COUNT(*) FROM cast c
--   JOIN actors a ON ... JOIN movies m ON ... WHERE m.director IS NOT NULL GROUP BY ...

CREATE MATERIALIZED VIEW IF NOT EXISTS actor_director_collaborations AS
SELECT
    a.name        AS actor,
    a.industry    AS industry,
    m.director    AS director,
    COUNT(*)      AS collaborations
FROM   "cast" c
JOIN   actors a ON a.id  = c.actor_id
JOIN   movies m ON m.id  = c.movie_id
WHERE  m.director IS NOT NULL
GROUP  BY a.name, a.industry, m.director
WITH   DATA;

-- Support ORDER BY collaborations DESC efficiently.
CREATE INDEX IF NOT EXISTS idx_actor_dir_collab_actor
    ON actor_director_collaborations (actor);

CREATE INDEX IF NOT EXISTS idx_actor_dir_collab_director
    ON actor_director_collaborations (director);

CREATE INDEX IF NOT EXISTS idx_actor_dir_collab_count
    ON actor_director_collaborations (collaborations DESC);

COMMENT ON MATERIALIZED VIEW actor_director_collaborations IS
    'Pre-computed actor-director collaboration counts. Refresh after ingestion.';


-- ---------------------------------------------------------------------------
-- Verification (uncomment to inspect immediately after migration)
-- ---------------------------------------------------------------------------

-- Top 5 actors by film count:
-- SELECT actor_name, industry, film_count
-- FROM   actor_film_counts
-- ORDER  BY film_count DESC
-- LIMIT  5;

-- Most frequent collaborations:
-- SELECT actor, director, collaborations
-- FROM   actor_director_collaborations
-- ORDER  BY collaborations DESC
-- LIMIT  10;


COMMIT;
