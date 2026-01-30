-- DataWarp v3.1 MVP Schema
-- Only 2 tables: pipeline configs + load history

CREATE SCHEMA IF NOT EXISTS datawarp;
CREATE SCHEMA IF NOT EXISTS staging;

-- Pipeline configs stored as JSONB
CREATE TABLE IF NOT EXISTS datawarp.tbl_pipeline_configs (
    pipeline_id VARCHAR(63) PRIMARY KEY,
    config JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Track what's been loaded
CREATE TABLE IF NOT EXISTS datawarp.tbl_load_history (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(63),
    period VARCHAR(20) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    source_file TEXT,
    sheet_name VARCHAR(100),
    rows_loaded INT,
    -- Reconciliation columns (source vs loaded)
    source_rows INT,           -- Row count in original file/sheet
    source_columns INT,        -- Column count in original file/sheet
    source_path TEXT,          -- Full path within archive (for ZIPs)
    loaded_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(pipeline_id, period, table_name, sheet_name)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_load_history_pipeline
ON datawarp.tbl_load_history(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_load_history_period
ON datawarp.tbl_load_history(period);

-- Enrichment API call logging for observability
CREATE TABLE IF NOT EXISTS datawarp.tbl_enrichment_log (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(63),
    source_file VARCHAR(255),
    sheet_name VARCHAR(100),

    -- LLM details
    provider VARCHAR(50),
    model VARCHAR(100),

    -- Request/Response
    prompt_text TEXT,
    response_text TEXT,

    -- Tokens and cost
    input_tokens INT,
    output_tokens INT,
    total_tokens INT,
    cost_usd NUMERIC(10, 6),

    -- Timing
    duration_ms INT,

    -- Results
    suggested_table_name VARCHAR(63),
    suggested_columns JSONB,
    success BOOLEAN DEFAULT true,
    error_message TEXT,

    -- Column compression tracking (for timeseries data)
    original_column_count INT,
    compressed_column_count INT,
    pattern_detected VARCHAR(100),

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_enrichment_log_pipeline
ON datawarp.tbl_enrichment_log(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_enrichment_log_created
ON datawarp.tbl_enrichment_log(created_at DESC);

-- CLI run tracker (eventstore pattern for observability)
CREATE TABLE IF NOT EXISTS datawarp.tbl_cli_runs (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(63),  -- nullable for commands that don't target a pipeline

    -- Command details
    command VARCHAR(50) NOT NULL,  -- bootstrap, scan, backfill, list, history
    args JSONB,  -- command arguments as JSON

    -- Timing
    started_at TIMESTAMP DEFAULT NOW(),
    ended_at TIMESTAMP,
    duration_ms INT,

    -- Result
    status VARCHAR(20) NOT NULL DEFAULT 'running',  -- running, success, failed, cancelled
    error_message TEXT,
    result_summary JSONB,  -- flexible summary (rows loaded, files processed, etc.)

    -- Context
    hostname VARCHAR(255),
    username VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_cli_runs_pipeline
ON datawarp.tbl_cli_runs(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_cli_runs_started
ON datawarp.tbl_cli_runs(started_at DESC);

CREATE INDEX IF NOT EXISTS idx_cli_runs_status
ON datawarp.tbl_cli_runs(status);

-- ============================================================================
-- MIGRATIONS (for existing databases)
-- ============================================================================
-- These ALTER statements add columns if they don't exist (idempotent)
-- Must run BEFORE views that reference these columns

-- Add reconciliation columns to tbl_load_history
ALTER TABLE datawarp.tbl_load_history ADD COLUMN IF NOT EXISTS source_rows INT;
ALTER TABLE datawarp.tbl_load_history ADD COLUMN IF NOT EXISTS source_columns INT;
ALTER TABLE datawarp.tbl_load_history ADD COLUMN IF NOT EXISTS source_path TEXT;

-- ============================================================================
-- METADATA VIEWS (for easy querying)
-- ============================================================================

-- View: All tables with their metadata
DROP VIEW IF EXISTS datawarp.v_table_metadata CASCADE;
CREATE VIEW datawarp.v_table_metadata AS
SELECT
    pc.pipeline_id,
    pc.config->>'name' as publication_name,
    pc.config->>'landing_page' as landing_page,
    m->>'table_name' as table_name,
    m->>'table_description' as table_description,
    m->>'grain' as grain,
    m->>'grain_column' as grain_column,
    m->>'grain_description' as grain_description,
    m->'column_mappings' as column_mappings,
    m->'column_descriptions' as column_descriptions,
    -- Version tracking for incremental enrichment
    COALESCE((m->>'mappings_version')::int, 1) as mappings_version,
    m->>'last_enriched' as last_enriched,
    pc.created_at as config_created,
    pc.updated_at as config_updated
FROM datawarp.tbl_pipeline_configs pc,
     jsonb_array_elements(pc.config->'file_patterns') as fp,
     jsonb_array_elements(fp->'sheet_mappings') as m;

-- View: Column-level metadata (one row per column)
DROP VIEW IF EXISTS datawarp.v_column_metadata CASCADE;
CREATE VIEW datawarp.v_column_metadata AS
SELECT
    pc.pipeline_id,
    m->>'table_name' as table_name,
    m->>'grain' as grain,
    col.key as original_name,
    col.value as semantic_name,
    -- Column is enriched if semantic_name differs from original
    col.key != col.value as is_enriched,
    COALESCE(
        m->'column_descriptions'->>col.value,
        m->'column_descriptions'->>col.key,
        ''
    ) as column_description,
    -- Version tracking
    COALESCE((m->>'mappings_version')::int, 1) as mappings_version,
    m->>'last_enriched' as last_enriched
FROM datawarp.tbl_pipeline_configs pc,
     jsonb_array_elements(pc.config->'file_patterns') as fp,
     jsonb_array_elements(fp->'sheet_mappings') as m,
     jsonb_each_text(m->'column_mappings') as col;

-- View: Load reconciliation (compare source vs loaded rows)
DROP VIEW IF EXISTS datawarp.v_load_reconciliation CASCADE;
CREATE VIEW datawarp.v_load_reconciliation AS
SELECT
    pipeline_id,
    period,
    table_name,
    source_file,
    source_path,
    source_rows,
    source_columns,
    rows_loaded,
    CASE
        WHEN source_rows IS NULL THEN 'no_source_info'
        WHEN source_rows = rows_loaded THEN 'match'
        WHEN source_rows > rows_loaded THEN 'rows_lost'
        ELSE 'rows_added'
    END as reconciliation_status,
    source_rows - rows_loaded as row_difference,
    loaded_at
FROM datawarp.tbl_load_history;

-- View: Load statistics per table
DROP VIEW IF EXISTS datawarp.v_table_stats CASCADE;
CREATE VIEW datawarp.v_table_stats AS
SELECT
    lh.pipeline_id,
    lh.table_name,
    COUNT(DISTINCT lh.period) as periods_loaded,
    SUM(lh.rows_loaded) as total_rows,
    MIN(lh.period) as earliest_period,
    MAX(lh.period) as latest_period,
    MAX(lh.loaded_at) as last_loaded
FROM datawarp.tbl_load_history lh
GROUP BY lh.pipeline_id, lh.table_name;

-- View: Combined table info (metadata + stats)
DROP VIEW IF EXISTS datawarp.v_tables CASCADE;
CREATE VIEW datawarp.v_tables AS
SELECT
    COALESCE(tm.pipeline_id, ts.pipeline_id) as pipeline_id,
    COALESCE(tm.table_name, ts.table_name) as table_name,
    tm.publication_name,
    tm.landing_page,
    tm.table_description,
    tm.grain,
    tm.grain_column,
    tm.grain_description,
    tm.mappings_version,
    tm.last_enriched,
    ts.periods_loaded,
    ts.total_rows,
    ts.earliest_period,
    ts.latest_period,
    ts.last_loaded
FROM datawarp.v_table_metadata tm
FULL OUTER JOIN datawarp.v_table_stats ts
    ON tm.table_name = ts.table_name;
