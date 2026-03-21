-- sprint24_gender_column.sql
-- Add gender column to actors table to support 3-category actor classification:
--   Lead Actors   : is_primary_actor = TRUE  AND gender = 'M'
--   Lead Actresses: is_primary_actor = TRUE  AND gender = 'F'
--   Supporting    : is_primary_actor = FALSE (gender may be NULL for most)
--
-- All currently-ingested is_primary_actor=TRUE actors are known male leads,
-- so we backfill gender='M' for them in the same transaction.

BEGIN;

-- 1. Add the column (idempotent — safe to re-run)
ALTER TABLE actors
  ADD COLUMN IF NOT EXISTS gender VARCHAR(1)
  CHECK (gender IN ('M', 'F'));

-- 2. Backfill all current primary actors as male
--    (Every is_primary_actor=TRUE actor ingested before this sprint is male)
UPDATE actors
SET    gender = 'M'
WHERE  is_primary_actor = TRUE
  AND  gender IS NULL;

COMMIT;

-- Verification
SELECT
  gender,
  COUNT(*) FILTER (WHERE is_primary_actor = TRUE)  AS primary_actors,
  COUNT(*) FILTER (WHERE is_primary_actor = FALSE) AS supporting_actors,
  COUNT(*)                                          AS total
FROM actors
GROUP BY gender
ORDER BY gender NULLS LAST;
