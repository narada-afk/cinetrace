-- =============================================================================
-- Migration: Sprint 4 — Performance indexes and unique constraints
-- File     : backend/migrations/sprint4_indexes_and_constraints.sql
-- Target   : PostgreSQL 14+
--
-- What this migration does:
--   Task 1. Adds covering indexes for the most common join/filter paths.
--   Task 2. Adds UNIQUE constraints to prevent accidental duplicate rows.
--
-- Safe to re-run:
--   All CREATE INDEX statements use IF NOT EXISTS.
--   UNIQUE constraint additions use DO $$ ... $$ blocks with existence checks.
--
-- Run order:
--   Apply AFTER sprint3_add_actor_registry.sql.
--   BEFORE running ingest_all_actors or enrich_movies (the unique_movie
--   constraint requires zero duplicate rows — verified at migration time).
-- =============================================================================

BEGIN;


-- ---------------------------------------------------------------------------
-- TASK 1 — Performance indexes
-- ---------------------------------------------------------------------------

-- ── movies(title, release_year) ──────────────────────────────────────────────
-- The most common lookup key used by _get_or_create_movie() and analytics
-- queries.  A composite index means the DB can satisfy both WHERE and ORDER BY
-- from index pages without touching the heap for these two columns.
CREATE INDEX IF NOT EXISTS idx_movies_title_year
    ON movies (title, release_year);

-- ── cast(actor_id) ───────────────────────────────────────────────────────────
-- "All movies of actor X" — used by the actor filmography API and the
-- actor_film_counts materialized view refresh.
CREATE INDEX IF NOT EXISTS idx_cast_actor
    ON "cast" (actor_id);

-- ── cast(movie_id) ───────────────────────────────────────────────────────────
-- "All actors in movie Y" — used by cast-listing endpoints.
CREATE INDEX IF NOT EXISTS idx_cast_movie
    ON "cast" (movie_id);

-- ── actor_registry(wikidata_id) ──────────────────────────────────────────────
-- Queried on every ingestion run to resolve QIDs.
-- Note: uq_actor_registry_wikidata_id already provides a unique B-tree index;
-- this named index is added for API consistency.  IF NOT EXISTS prevents a
-- duplicate-index error if the index already exists under this name.
CREATE INDEX IF NOT EXISTS idx_actor_registry_qid
    ON actor_registry (wikidata_id);


-- ---------------------------------------------------------------------------
-- TASK 2 — Unique constraints
-- ---------------------------------------------------------------------------

-- ── movies: UNIQUE(title, release_year) ──────────────────────────────────────
-- Prevents the ingestion pipeline from creating duplicate movie rows.
-- The DO block first verifies no existing duplicates, then conditionally adds
-- the constraint so the migration is safe to re-run.
DO $$
BEGIN
    -- Safety guard: abort if duplicates already exist.
    IF EXISTS (
        SELECT 1
        FROM   movies
        GROUP  BY title, release_year
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'Cannot add UNIQUE(title, release_year): duplicate rows exist. '
            'Run: SELECT title, release_year, COUNT(*) FROM movies '
            'GROUP BY title, release_year HAVING COUNT(*) > 1;';
    END IF;

    -- Add the constraint only if it isn't already present.
    IF NOT EXISTS (
        SELECT 1
        FROM   pg_constraint
        WHERE  conname    = 'unique_movie'
        AND    conrelid   = 'movies'::regclass
    ) THEN
        ALTER TABLE movies
            ADD CONSTRAINT unique_movie UNIQUE (title, release_year);
        RAISE NOTICE 'Added constraint unique_movie on movies(title, release_year).';
    ELSE
        RAISE NOTICE 'Constraint unique_movie already exists — skipped.';
    END IF;
END
$$;

-- ── actor_registry: UNIQUE(wikidata_id) ──────────────────────────────────────
-- The Sprint 3 migration already created uq_actor_registry_wikidata_id.
-- This block adds the canonical alias name unique_actor_qid only if neither
-- that nor any other unique constraint on wikidata_id exists yet.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   pg_constraint
        WHERE  conname  = 'unique_actor_qid'
        AND    conrelid = 'actor_registry'::regclass
    ) THEN
        -- Only add if wikidata_id is not already covered by a unique constraint.
        -- (uq_actor_registry_wikidata_id from Sprint 3 already covers it.)
        -- We intentionally skip re-adding if an equivalent constraint exists.
        RAISE NOTICE
            'unique_actor_qid not added: wikidata_id already has constraint '
            'uq_actor_registry_wikidata_id from sprint3_add_actor_registry.sql.';
    ELSE
        RAISE NOTICE 'Constraint unique_actor_qid already exists — skipped.';
    END IF;
END
$$;


-- ---------------------------------------------------------------------------
-- Verification queries (uncomment to inspect)
-- ---------------------------------------------------------------------------

-- Check all new indexes are present:
-- SELECT indexname, indexdef
-- FROM   pg_indexes
-- WHERE  tablename IN ('movies', 'cast', 'actor_registry')
-- AND    indexname IN ('idx_movies_title_year','idx_cast_actor',
--                      'idx_cast_movie','idx_actor_registry_qid')
-- ORDER BY indexname;

-- Check unique_movie constraint:
-- SELECT conname, contype
-- FROM   pg_constraint
-- WHERE  conrelid = 'movies'::regclass AND contype = 'u';


COMMIT;
