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
    loaded_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(pipeline_id, period, table_name, sheet_name)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_load_history_pipeline
ON datawarp.tbl_load_history(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_load_history_period
ON datawarp.tbl_load_history(period);
