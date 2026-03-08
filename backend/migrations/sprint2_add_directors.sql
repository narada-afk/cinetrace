-- =============================================================================
-- Migration: Sprint 2 — Add normalized director tables
-- File     : backend/migrations/sprint2_add_directors.sql
-- Target   : PostgreSQL (tested on PG 14+)
--
-- What this migration does:
--   1. Creates the `directors` table (one row per unique director).
--   2. Creates the `movie_directors` join table (movie ↔ director, many-to-many).
--   3. Backfills data from the existing movies.director TEXT column so that
--      historical seed data becomes queryable via the new normalized tables.
--   4. Does NOT remove movies.director — that column stays for backward compat.
--
-- Safe to re-run: every statement uses IF NOT EXISTS / ON CONFLICT DO NOTHING.
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. directors
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS directors (
    id   SERIAL       PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    CONSTRAINT uq_directors_name UNIQUE (name)
);

-- Explicit index on id mirrors SQLAlchemy's index=True on the PK column.
CREATE INDEX IF NOT EXISTS ix_directors_id   ON directors (id);
-- Explicit index on name speeds up the frequent lookup-by-name upsert.
CREATE INDEX IF NOT EXISTS ix_directors_name ON directors (name);

COMMENT ON TABLE  directors      IS 'Normalized director entities (Sprint 2).';
COMMENT ON COLUMN directors.name IS 'Full English name, e.g. "Sukumar". Unique.';


-- ---------------------------------------------------------------------------
-- 2. movie_directors  (join table)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id    INTEGER NOT NULL
                    REFERENCES movies(id)    ON DELETE CASCADE ON UPDATE CASCADE,
    director_id INTEGER NOT NULL
                    REFERENCES directors(id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (movie_id, director_id)   -- composite PK = uniqueness constraint
);

-- Individual indexes on FKs for efficient reverse lookups:
--   "find all movies by director X"  →  ix_movie_directors_director_id
--   "find all directors of movie Y"  →  covered by the PK index on movie_id
CREATE INDEX IF NOT EXISTS ix_movie_directors_director_id
    ON movie_directors (director_id);

COMMENT ON TABLE  movie_directors             IS 'Movie ↔ Director join table (Sprint 2).';
COMMENT ON COLUMN movie_directors.movie_id    IS 'FK → movies.id';
COMMENT ON COLUMN movie_directors.director_id IS 'FK → directors.id';


-- ---------------------------------------------------------------------------
-- 3. Backfill from movies.director TEXT column
--
-- For every movie that already has a non-empty director string, we:
--   a) Insert the director name into `directors` (skip duplicates).
--   b) Insert the (movie_id, director_id) link into `movie_directors` (skip dupes).
--
-- This ensures existing seed data is immediately queryable via the new tables.
-- ---------------------------------------------------------------------------

-- 3a. Seed directors from the legacy text column.
INSERT INTO directors (name)
SELECT DISTINCT TRIM(director)          -- deduplicate and strip whitespace
FROM   movies
WHERE  director IS NOT NULL
  AND  TRIM(director) <> ''
ON CONFLICT (name) DO NOTHING;          -- idempotent: skip if already exists

-- 3b. Seed movie_directors join rows.
INSERT INTO movie_directors (movie_id, director_id)
SELECT m.id,
       d.id
FROM   movies    m
JOIN   directors d ON d.name = TRIM(m.director)
WHERE  m.director IS NOT NULL
  AND  TRIM(m.director) <> ''
ON CONFLICT DO NOTHING;                 -- idempotent: skip existing (movie_id, director_id) pairs


-- ---------------------------------------------------------------------------
-- 4. Verification queries  (comment out in production; uncomment to inspect)
-- ---------------------------------------------------------------------------

-- Check newly created directors:
-- SELECT * FROM directors ORDER BY name;

-- Check movie → director links:
-- SELECT m.title, m.release_year, d.name AS director
-- FROM   movie_directors md
-- JOIN   movies    m ON m.id = md.movie_id
-- JOIN   directors d ON d.id = md.director_id
-- ORDER  BY m.release_year DESC, m.title;

-- Count summary:
-- SELECT
--     (SELECT COUNT(*) FROM directors)     AS total_directors,
--     (SELECT COUNT(*) FROM movie_directors) AS total_links;


COMMIT;
