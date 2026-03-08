-- =============================================================================
-- Migration: Sprint 3 — Add actor_registry table
-- File     : backend/migrations/sprint3_add_actor_registry.sql
-- Target   : PostgreSQL (tested on PG 14+)
--
-- What this migration does:
--   1. Creates the actor_registry table (one row per actor we want to ingest).
--   2. Inserts 13 South Indian actors with their Wikidata QIDs.
--
-- QID verification:
--   Every QID below links to the actor's canonical Wikidata page.
--   Verify before running in production:
--       https://www.wikidata.org/wiki/<QID>
--   Example:  https://www.wikidata.org/wiki/Q352416  →  Allu Arjun
--
-- Safe to re-run: CREATE TABLE uses IF NOT EXISTS, INSERT uses ON CONFLICT DO NOTHING.
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. Create actor_registry table
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS actor_registry (
    id          SERIAL       PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    wikidata_id VARCHAR(20)  NOT NULL,
    industry    VARCHAR(100) NOT NULL,
    CONSTRAINT uq_actor_registry_wikidata_id UNIQUE (wikidata_id)
);

CREATE INDEX IF NOT EXISTS ix_actor_registry_id          ON actor_registry (id);
CREATE INDEX IF NOT EXISTS ix_actor_registry_wikidata_id ON actor_registry (wikidata_id);
CREATE INDEX IF NOT EXISTS ix_actor_registry_industry    ON actor_registry (industry);

COMMENT ON TABLE  actor_registry             IS 'Catalog of actors for Wikidata ingestion (Sprint 3).';
COMMENT ON COLUMN actor_registry.wikidata_id IS 'Canonical Wikidata QID, e.g. Q352416. Unique per actor.';
COMMENT ON COLUMN actor_registry.industry    IS 'Film industry label used for new movie rows, e.g. Telugu.';


-- ---------------------------------------------------------------------------
-- 2. Insert initial actor data
--
-- QID verification URLs (open in browser to confirm):
--   Telugu actors:
--     Allu Arjun   → https://www.wikidata.org/wiki/Q352416
--     Mahesh Babu  → https://www.wikidata.org/wiki/Q1373503
--     Prabhas      → https://www.wikidata.org/wiki/Q297491
--     Ram Charan   → https://www.wikidata.org/wiki/Q3419703
--     Jr. NTR      → https://www.wikidata.org/wiki/Q942132
--     Pawan Kalyan → https://www.wikidata.org/wiki/Q469302
--   Tamil actors:
--     Vijay        → https://www.wikidata.org/wiki/Q536725
--     Ajith Kumar  → https://www.wikidata.org/wiki/Q535632
--     Suriya       → https://www.wikidata.org/wiki/Q1365617
--     Dhanush      → https://www.wikidata.org/wiki/Q560524
--     Karthi       → https://www.wikidata.org/wiki/Q1163208
--     Rajinikanth  → https://www.wikidata.org/wiki/Q351478
--     Kamal Haasan → https://www.wikidata.org/wiki/Q330829
-- ---------------------------------------------------------------------------

INSERT INTO actor_registry (name, wikidata_id, industry) VALUES

    -- ── Telugu ───────────────────────────────────────────────────────────────
    ('Allu Arjun',          'Q352416',  'Telugu'),   -- https://www.wikidata.org/wiki/Q352416
    ('Mahesh Babu',         'Q1373503', 'Telugu'),   -- https://www.wikidata.org/wiki/Q1373503
    ('Prabhas',             'Q297491',  'Telugu'),   -- https://www.wikidata.org/wiki/Q297491
    ('Ram Charan',          'Q3419703', 'Telugu'),   -- https://www.wikidata.org/wiki/Q3419703
    ('N. T. Rama Rao Jr.',  'Q942132',  'Telugu'),   -- https://www.wikidata.org/wiki/Q942132
    ('Pawan Kalyan',        'Q469302',  'Telugu'),   -- https://www.wikidata.org/wiki/Q469302

    -- ── Tamil ────────────────────────────────────────────────────────────────
    ('Vijay',               'Q536725',  'Tamil'),    -- https://www.wikidata.org/wiki/Q536725
    ('Ajith Kumar',         'Q535632',  'Tamil'),    -- https://www.wikidata.org/wiki/Q535632
    ('Suriya',              'Q1365617', 'Tamil'),    -- https://www.wikidata.org/wiki/Q1365617
    ('Dhanush',             'Q560524',  'Tamil'),    -- https://www.wikidata.org/wiki/Q560524
    ('Karthi',              'Q1163208', 'Tamil'),    -- https://www.wikidata.org/wiki/Q1163208
    ('Rajinikanth',         'Q351478',  'Tamil'),    -- https://www.wikidata.org/wiki/Q351478
    ('Kamal Haasan',        'Q330829',  'Tamil')     -- https://www.wikidata.org/wiki/Q330829

ON CONFLICT (wikidata_id) DO NOTHING;    -- idempotent: skip if QID already exists


-- ---------------------------------------------------------------------------
-- 3. Verification
-- ---------------------------------------------------------------------------

-- Quick sanity check (uncomment to inspect):
-- SELECT id, name, wikidata_id, industry FROM actor_registry ORDER BY industry, name;

-- Expected output: 13 rows (6 Telugu, 7 Tamil).
-- SELECT COUNT(*) FROM actor_registry;
-- SELECT industry, COUNT(*) FROM actor_registry GROUP BY industry ORDER BY industry;


COMMIT;
