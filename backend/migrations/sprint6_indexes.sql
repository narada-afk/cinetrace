-- =============================================================================
-- Migration: Sprint 6 — API Performance Indexes
-- File     : backend/migrations/sprint6_indexes.sql
-- Target   : PostgreSQL 14+
--
-- What this migration does:
--   Adds the remaining indexes required for the analytics API layer
--   introduced in Sprint 6. All indexes use IF NOT EXISTS so the file
--   is safe to re-run without error.
--
-- Context — indexes already in place from earlier sprints:
--   Sprint 4 (sprint4_indexes_and_constraints.sql):
--     idx_movies_title_year         ON movies(title, release_year)
--     idx_cast_actor                ON "cast"(actor_id)
--     idx_cast_movie                ON "cast"(movie_id)
--     idx_actor_registry_qid        ON actor_registry(wikidata_id)
--   Sprint 5 (sprint5_analytics_tables.sql):
--     idx_actor_stats_actor         ON actor_stats(actor_id)
--     idx_collab_actor1             ON actor_collaborations(actor1_id)
--     idx_director_actor            ON actor_director_stats(actor_id)
--     idx_production_actor          ON actor_production_stats(actor_id)
--     idx_director_name             ON actor_director_stats(director)
--     idx_production_company        ON actor_production_stats(production_company)
--
-- New indexes added here:
--   idx_movies_title                ON movies(title)
--       Speeds up title-only lookups independently of release_year.
--       The compound idx_movies_title_year is still preferred when both
--       columns appear in the query; this index serves title-only plans.
--
--   idx_collab_actor2               ON actor_collaborations(actor2_id)
--       Enables efficient "who has actor X appeared with?" reverse lookups.
--       Although the bidirectional storage means actor1_id queries cover
--       most cases, this index future-proofs reverse-direction analytics.
--
--   idx_actors_name_lower           ON actors(lower(name))
--       Functional index on the lowercased name for case-insensitive exact
--       matches (used by the /compare and search endpoints internally).
--       Note: ILIKE '%fragment%' with a leading wildcard cannot use a
--       btree index regardless — sequential scan is fine for 13 actors.
--
-- Run order: after sprint5_*.sql migrations.
-- =============================================================================

BEGIN;


-- ---------------------------------------------------------------------------
-- movies(title) — standalone title index
-- ---------------------------------------------------------------------------
-- The Sprint 4 compound index (title, release_year) covers title-prefix
-- queries. This standalone index makes title-only predicates explicit and
-- easy for the query planner to choose.

CREATE INDEX IF NOT EXISTS idx_movies_title
    ON movies (title);

COMMENT ON INDEX idx_movies_title IS
    'Standalone title index for title-only lookups (Sprint 6).';


-- ---------------------------------------------------------------------------
-- actor_collaborations(actor2_id) — reverse-direction lookup index
-- ---------------------------------------------------------------------------
-- Composite PK is (actor1_id, actor2_id); that PK index only helps when
-- filtering by actor1_id first. This index covers the reverse direction:
-- "find all rows where a specific actor appears as actor2".

CREATE INDEX IF NOT EXISTS idx_collab_actor2
    ON actor_collaborations (actor2_id);

COMMENT ON INDEX idx_collab_actor2 IS
    'Reverse-direction index on actor_collaborations for actor2 lookups (Sprint 6).';


-- ---------------------------------------------------------------------------
-- actors(lower(name)) — case-insensitive name lookup index
-- ---------------------------------------------------------------------------
-- actors.name already has a UNIQUE btree index (created by SQLAlchemy).
-- This functional index on lower(name) accelerates the pattern:
--   WHERE lower(actors.name) = lower(:name)
-- used by the /compare endpoint and get_actor_by_name().

CREATE INDEX IF NOT EXISTS idx_actors_name_lower
    ON actors (lower(name));

COMMENT ON INDEX idx_actors_name_lower IS
    'Functional index for case-insensitive exact name lookups (Sprint 6).';


-- ---------------------------------------------------------------------------
-- Verification queries (uncomment to confirm after applying)
-- ---------------------------------------------------------------------------

-- List all sprint6 indexes:
-- SELECT indexname, indexdef
-- FROM   pg_indexes
-- WHERE  indexname IN (
--            'idx_movies_title',
--            'idx_collab_actor2',
--            'idx_actors_name_lower'
--        )
-- ORDER  BY indexname;


COMMIT;
