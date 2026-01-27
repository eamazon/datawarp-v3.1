#!/bin/bash
# Reset DataWarp database for testing
# - Drops all staging tables
# - Truncates datawarp config tables (keeps schema)
# Safe to run repeatedly

DB_NAME="${POSTGRES_DB:-datawalker}"

echo "Resetting database: $DB_NAME"

# Drop all staging tables
echo "Dropping staging tables..."
psql -d "$DB_NAME" -t -c "
    SELECT 'DROP TABLE IF EXISTS staging.' || table_name || ' CASCADE;'
    FROM information_schema.tables
    WHERE table_schema = 'staging'
    AND table_type = 'BASE TABLE'
" | psql -d "$DB_NAME" 2>/dev/null

# Truncate datawarp config tables (keep structure)
echo "Truncating datawarp tables..."
psql -d "$DB_NAME" -c "
    TRUNCATE TABLE datawarp.tbl_pipeline_configs CASCADE;
    TRUNCATE TABLE datawarp.tbl_load_history CASCADE;
" 2>/dev/null || echo "  (tables may not exist yet - OK)"

echo "Done. Database reset."
