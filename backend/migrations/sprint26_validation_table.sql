-- ─────────────────────────────────────────────────────────────────────────────
-- Sprint 26: Movie Validation Results Table
-- Run once:  psql $DATABASE_URL -f migrations/sprint26_validation_table.sql
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS movie_validation_results (
    movie_id         INTEGER     PRIMARY KEY REFERENCES movies(id) ON DELETE CASCADE,
    confidence_score NUMERIC(5,4) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    status           TEXT        NOT NULL CHECK (status IN ('VERIFIED', 'WARNING', 'BROKEN')),
    issues           JSONB       NOT NULL DEFAULT '[]',
    field_scores     JSONB       NOT NULL DEFAULT '{}',
    last_checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast look-ups by status (e.g. "show me all BROKEN movies")
CREATE INDEX IF NOT EXISTS idx_mvr_status
    ON movie_validation_results (status);

-- Fast look-ups by confidence band (e.g. "movies scoring below 0.6")
CREATE INDEX IF NOT EXISTS idx_mvr_confidence
    ON movie_validation_results (confidence_score);

-- Partial index: rows that need attention
CREATE INDEX IF NOT EXISTS idx_mvr_needs_review
    ON movie_validation_results (last_checked_at)
    WHERE status != 'VERIFIED';

COMMENT ON TABLE movie_validation_results IS
    'Per-movie data quality scores produced by data_pipeline/validate_movies.py. '
    'One row per movie; re-running validate_all_movies() upserts in-place.';

COMMENT ON COLUMN movie_validation_results.confidence_score IS
    'Weighted 0–1 score. ≥0.9 = VERIFIED, 0.6–0.9 = WARNING, <0.6 = BROKEN.';

COMMENT ON COLUMN movie_validation_results.issues IS
    'JSON array of issue strings, e.g. ["director:missing", "primary_cast:mismatch …"].';

COMMENT ON COLUMN movie_validation_results.field_scores IS
    'Per-field breakdown, e.g. {"director":1.0, "primary_cast":0.5, …}.';
