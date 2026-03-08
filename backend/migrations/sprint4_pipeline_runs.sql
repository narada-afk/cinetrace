-- =============================================================================
-- Migration: Sprint 4 — Pipeline run tracking table
-- File     : backend/migrations/sprint4_pipeline_runs.sql
-- Target   : PostgreSQL 14+
--
-- What this migration does:
--   Creates the pipeline_runs table that records every execution of the
--   Wikidata ingestion and Wikipedia enrichment pipelines.
--
-- Why a separate table?
--   Provides an audit trail for debugging, performance monitoring, and
--   understanding when data was last refreshed without adding noise to the
--   core cinema schema.
--
-- Safe to re-run: CREATE TABLE uses IF NOT EXISTS.
-- =============================================================================

BEGIN;


-- ---------------------------------------------------------------------------
-- Create pipeline_runs table
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          SERIAL       PRIMARY KEY,

    -- What kind of run: "wikidata_ingestion" or "wikipedia_enrichment"
    run_type    VARCHAR(100) NOT NULL,

    -- Wall-clock start/finish timestamps (with timezone).
    started_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,           -- NULL while still running

    -- Terminal state: "running" → "success" | "failed"
    status      VARCHAR(20)  NOT NULL DEFAULT 'running'
                             CHECK (status IN ('running', 'success', 'failed')),

    -- Optional JSON blob with per-run statistics.
    -- Example for wikidata_ingestion:
    --   {"actors": 13, "films_found": 759, "inserted": 2, "skipped": 757}
    -- Example for wikipedia_enrichment:
    --   {"processed": 50, "updated": 18, "skipped": 30, "errors": 2}
    details     TEXT
);

-- Indexes for the most common query patterns.
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_run_type
    ON pipeline_runs (run_type);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_started_at
    ON pipeline_runs (started_at DESC);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_status
    ON pipeline_runs (status);

-- Table / column documentation.
COMMENT ON TABLE  pipeline_runs            IS 'Audit log of data pipeline executions (Sprint 4).';
COMMENT ON COLUMN pipeline_runs.run_type   IS 'One of: wikidata_ingestion, wikipedia_enrichment.';
COMMENT ON COLUMN pipeline_runs.status     IS 'Lifecycle state: running → success | failed.';
COMMENT ON COLUMN pipeline_runs.details    IS 'JSON string with per-run statistics.';


-- ---------------------------------------------------------------------------
-- Verification (uncomment to inspect after migration)
-- ---------------------------------------------------------------------------

-- SELECT column_name, data_type, is_nullable, column_default
-- FROM   information_schema.columns
-- WHERE  table_name = 'pipeline_runs'
-- ORDER  BY ordinal_position;


COMMIT;
