-- =============================================================================
-- Migration: Sprint 8 — Supporting actor schema
-- File     : backend/migrations/sprint8_supporting_actor_schema.sql
-- Target   : PostgreSQL 14+
--
-- What this migration does:
--
--   Part A — Extend the actors table
--     Adds three new columns required to track TMDB identities and
--     distinguish the 13 primary actors from the supporting actors that
--     will be discovered by ingest_supporting_actors.py.
--
--   Part B — Mark existing actors as primary
--     All rows already in `actors` at migration time are the 13 primary
--     actors ingested via Wikidata.  They are flagged is_primary_actor=TRUE.
--
--   Part C — Create the actor_movies relationship table
--     A new join table that stores actor↔movie links sourced from TMDB
--     credits.  This is intentionally separate from the existing `cast`
--     table (which contains Wikidata-sourced relationships) so neither
--     pipeline interferes with the other.
--
--   Part D — Indexes
--     Fast lookups by actor_id, movie_id, and tmdb_person_id.
--
-- Design notes:
--   • tmdb_person_id is UNIQUE but nullable.  The 13 Wikidata-sourced primary
--     actors start with NULL; ingest_supporting_actors.py fills this in if
--     it recognises them in a TMDB cast list.
--   • actor_movies uses a composite PRIMARY KEY (actor_id, movie_id) which
--     doubles as the unique constraint that makes ON CONFLICT DO NOTHING work.
--   • role_type is either 'primary' or 'supporting'.  The script sets
--     'primary' when the actor is one of the 13 flagged actors, otherwise
--     'supporting'.
--   • No FK constraints on actor_movies.actor_id / movie_id — allows fast
--     TRUNCATE if a full rebuild is ever needed (same pattern as analytics
--     tables).
--
-- Safe to re-run: all statements use IF NOT EXISTS / DO block guards.
-- Run order: after sprint7_tmdb_columns.sql.
-- =============================================================================

BEGIN;


-- ===========================================================================
-- Part A — Extend actors table
-- ===========================================================================

-- TMDB numeric person ID — unique across all people on TMDB.
-- NULL for existing Wikidata-sourced actors until they appear in a TMDB cast.
ALTER TABLE actors
    ADD COLUMN IF NOT EXISTS tmdb_person_id INTEGER;

-- Partial unique index — allow many NULLs but enforce uniqueness once set.
CREATE UNIQUE INDEX IF NOT EXISTS idx_actors_tmdb_person_id
    ON actors (tmdb_person_id)
    WHERE tmdb_person_id IS NOT NULL;

COMMENT ON COLUMN actors.tmdb_person_id IS
    'TMDB person ID. NULL for actors ingested via Wikidata before Sprint 8.';


-- Flag distinguishing the 13 seeded primary actors from supporting actors
-- discovered by ingest_supporting_actors.py.
ALTER TABLE actors
    ADD COLUMN IF NOT EXISTS is_primary_actor BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN actors.is_primary_actor IS
    'TRUE for the 13 primary actors seeded from actor_registry. FALSE for all others.';


-- Audit timestamp — when the row was first inserted.
ALTER TABLE actors
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

COMMENT ON COLUMN actors.created_at IS
    'Row creation timestamp. Pre-existing rows receive NOW() at migration time.';


-- ===========================================================================
-- Part B — Mark all existing actors as primary actors
-- ===========================================================================
-- At the time this migration runs, every row in `actors` was inserted by the
-- Wikidata ingestion pipeline and corresponds to one of the 13 registry entries.

UPDATE actors SET is_primary_actor = TRUE WHERE is_primary_actor = FALSE;


-- ===========================================================================
-- Part C — actor_movies relationship table (TMDB-sourced)
-- ===========================================================================

CREATE TABLE IF NOT EXISTS actor_movies (
    actor_id        INTEGER NOT NULL,       -- maps to actors.id
    movie_id        INTEGER NOT NULL,       -- maps to movies.id
    character_name  TEXT,                   -- character name from TMDB credits
    billing_order   INTEGER,               -- 0-based cast billing order from TMDB
    role_type       TEXT NOT NULL DEFAULT 'supporting'
                    CHECK (role_type IN ('primary', 'supporting')),

    PRIMARY KEY (actor_id, movie_id)        -- composite PK = natural unique key
);

COMMENT ON TABLE actor_movies IS
    'TMDB-sourced actor↔movie relationships. Populated by ingest_supporting_actors.py.';
COMMENT ON COLUMN actor_movies.billing_order IS
    '0-based cast order as returned by TMDB /movie/{id}/credits. Lower = higher billing.';
COMMENT ON COLUMN actor_movies.role_type IS
    '"primary" for the 13 seeded actors, "supporting" for all newly discovered actors.';


-- ===========================================================================
-- Part D — Indexes
-- ===========================================================================

-- actor_movies: look up all movies a given actor appeared in
CREATE INDEX IF NOT EXISTS idx_actor_movies_actor_id
    ON actor_movies (actor_id);

-- actor_movies: look up all actors in a given movie
CREATE INDEX IF NOT EXISTS idx_actor_movies_movie_id
    ON actor_movies (movie_id);

-- actor_movies: filter to primary / supporting quickly
CREATE INDEX IF NOT EXISTS idx_actor_movies_role_type
    ON actor_movies (role_type);

-- actors: filter primary actors quickly
CREATE INDEX IF NOT EXISTS idx_actors_is_primary
    ON actors (is_primary_actor);


-- ===========================================================================
-- Verification (uncomment to inspect after applying)
-- ===========================================================================

-- Check new actor columns:
-- SELECT column_name, data_type, is_nullable, column_default
-- FROM   information_schema.columns
-- WHERE  table_name = 'actors'
--   AND  column_name IN ('tmdb_person_id','is_primary_actor','created_at')
-- ORDER  BY column_name;

-- Confirm primary actors flagged:
-- SELECT name, is_primary_actor FROM actors ORDER BY name;

-- Confirm actor_movies table:
-- SELECT tablename FROM pg_tables WHERE tablename = 'actor_movies';


COMMIT;
