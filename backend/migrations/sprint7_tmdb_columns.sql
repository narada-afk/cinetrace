-- =============================================================================
-- Migration: Sprint 7 — TMDB enrichment columns
-- File     : backend/migrations/sprint7_tmdb_columns.sql
-- Target   : PostgreSQL 14+
--
-- What this migration does:
--   Adds three TMDB-sourced columns to the movies table so the new
--   enrich_tmdb_movies.py pipeline has somewhere to write its results.
--
--   tmdb_id      — TMDB's internal integer movie ID (unique per film).
--                  Used as the "already enriched" sentinel: if tmdb_id IS NOT
--                  NULL the enrichment script skips that row.
--   vote_average — TMDB community vote average (0.0–10.0 float).
--   popularity   — TMDB popularity score (unbounded positive float).
--
--   poster_url and backdrop_url already exist on the movies table (added
--   when the table was first created). This migration does NOT touch them.
--
-- Design notes:
--   • All three columns are nullable. NULL means "not yet enriched".
--   • tmdb_id carries a PARTIAL UNIQUE index (WHERE tmdb_id IS NOT NULL)
--     so that multiple unenriched rows (all NULL) are permitted while still
--     preventing duplicate TMDB IDs once a value is written.
--   • All statements use IF NOT EXISTS — safe to re-run.
--
-- Run order: after sprint6_indexes.sql.
-- =============================================================================

BEGIN;


-- ---------------------------------------------------------------------------
-- New columns on movies
-- ---------------------------------------------------------------------------

ALTER TABLE movies
    ADD COLUMN IF NOT EXISTS tmdb_id      INTEGER,
    ADD COLUMN IF NOT EXISTS vote_average FLOAT,
    ADD COLUMN IF NOT EXISTS popularity   FLOAT;

COMMENT ON COLUMN movies.tmdb_id      IS 'TMDB integer movie ID. NULL = not yet enriched.';
COMMENT ON COLUMN movies.vote_average IS 'TMDB community vote average (0–10). Populated by enrich_tmdb_movies.py.';
COMMENT ON COLUMN movies.popularity   IS 'TMDB popularity score. Populated by enrich_tmdb_movies.py.';


-- ---------------------------------------------------------------------------
-- Partial unique index on tmdb_id
-- ---------------------------------------------------------------------------
-- Prevents two movie rows from being linked to the same TMDB entry while
-- still allowing multiple NULL values (unenriched rows).

CREATE UNIQUE INDEX IF NOT EXISTS idx_movies_tmdb_id
    ON movies (tmdb_id)
    WHERE tmdb_id IS NOT NULL;

COMMENT ON INDEX idx_movies_tmdb_id IS
    'Partial unique index — enforces one row per TMDB ID while allowing many NULLs.';


-- ---------------------------------------------------------------------------
-- Verification (uncomment to inspect after applying)
-- ---------------------------------------------------------------------------

-- Confirm columns exist:
-- SELECT column_name, data_type, is_nullable
-- FROM   information_schema.columns
-- WHERE  table_name = 'movies'
--   AND  column_name IN ('tmdb_id', 'vote_average', 'popularity')
-- ORDER  BY column_name;

-- Confirm index:
-- SELECT indexname, indexdef
-- FROM   pg_indexes
-- WHERE  indexname = 'idx_movies_tmdb_id';


COMMIT;
