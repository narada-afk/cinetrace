-- migrations/sprint25_trigram_search.sql
-- ========================================
-- Upgrade actor name search from ILIKE '%q%' (sequential scan) to
-- trigram similarity (GIN index).  Apply when the actor table grows
-- large enough that search latency becomes noticeable (typically >50k rows).
--
-- Prerequisites: PostgreSQL 9.1+  (pg_trgm is bundled, just needs enabling)
--
-- Apply with:
--   docker compose exec postgres psql -U sca -d sca -f /migrations/sprint25_trigram_search.sql
--
-- After applying, update actor_repository.py search() to use similarity:
--
--   from sqlalchemy import func
--   query = (
--       db.query(models.Actor.id, models.Actor.name)
--       .filter(func.similarity(models.Actor.name, q) > 0.2)
--       .order_by(func.similarity(models.Actor.name, q).desc())
--   )
--
-- The ILIKE fallback in actor_repository.py is intentionally left in place
-- until this migration is confirmed applied on the target DB.

-- Step 1: Enable the extension (idempotent)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Step 2: GIN trigram index on actor names
--   CONCURRENTLY avoids a full table lock — safe to run on live DB.
--   Drop the old btree index on name if it exists (optional, saves ~2 MB).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_actors_name_trgm
    ON actors
    USING GIN (name gin_trgm_ops);

-- Step 3 (optional): functional btree index for exact lower() lookups
--   Already created by an earlier migration; kept here for reference.
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_actors_name_lower
--     ON actors (lower(name));

-- Verify
SELECT
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'actors'
  AND indexname LIKE '%trgm%';
