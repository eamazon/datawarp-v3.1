# Current Tasks - DataWarp v3.1

**Last Updated:** 2025-01-27 20:00

---

## Session Summary (2025-01-27)

### Completed This Session

1. **Enrichment Logging** - Added `tbl_enrichment_log` table to track LLM calls
   - Logs: pipeline_id, source_file, sheet_name, provider, model
   - Logs: prompt_text, response_text, tokens, duration_ms
   - Logs: suggested_table_name, suggested_columns, success/error

2. **Fixed Table Names** - LLM names now used directly (not combined with pipeline_id)
   - Before: `tbl_patients_registered_at_a_gp_practice_gp_practice_patient_list`
   - After: `tbl_gp_patients`
   - Updated LLM prompt to request short names (max 30 chars)

3. **Metadata Views** - Created 4 views for easy metadata querying
   - `v_tables` - All tables with metadata + stats
   - `v_table_metadata` - Table descriptions, grain, column mappings
   - `v_column_metadata` - Column-level descriptions
   - `v_table_stats` - Load statistics per table

4. **Documentation Refresh**
   - Updated `CLAUDE.md` - Reflects current state, not outdated MVP spec
   - Created `docs/ARCHITECTURE.md` - System design and data flow
   - Created `docs/DATABASE_SPEC.md` - Complete schema documentation
   - Created `docs/USER_GUIDE.md` - CLI cheat sheet + SQL queries

5. **DType Warning Fix** - Added `low_memory=False` to CSV reads

---

## Current State

| Component | Status |
|-----------|--------|
| Discovery | ✅ Working |
| Period Detection | ✅ Working |
| Grain Detection | ✅ Working |
| LLM Enrichment | ✅ Working |
| Enrichment Logging | ✅ Working |
| Data Loading | ✅ Working |
| Metadata Views | ✅ Working |
| MCP Server | ✅ Working |

---

## Next Session

### Priority Tasks

1. **Test GP Registrations** - Run bootstrap with `--enrich` to verify short table names
2. **Test Multi-Period** - Verify scan appends to existing tables correctly
3. **MCP Integration Test** - Verify Claude can query metadata via MCP

### Queued (Pick 0-1)

- [ ] Add cost calculation to enrichment logging (based on token counts)
- [ ] Add schema drift detection (new columns in subsequent periods)
- [ ] Add data quality checks (null counts, duplicate detection)

---

## Files Changed This Session

```
Modified:
- CLAUDE.md (complete rewrite)
- sql/schema.sql (added enrichment_log, metadata views)
- src/datawarp/metadata/enrich.py (logging, shorter prompts)
- src/datawarp/loader/excel.py (low_memory=False)
- scripts/pipeline.py (fixed table naming, enrichment params)

Created:
- docs/ARCHITECTURE.md
- docs/DATABASE_SPEC.md
- docs/USER_GUIDE.md
- docs/tasks/CURRENT.md
```

---

## Quick Start Next Session

```bash
# 1. Check git status
cd /Users/speddi/projectx/datawarp-v3.1
git status

# 2. Test bootstrap with enrichment
PYTHONPATH=src python scripts/pipeline.py bootstrap \
  --url "https://digital.nhs.uk/.../patients-registered-at-a-gp-practice/january-2026" \
  --id gp_reg \
  --enrich

# 3. Check metadata
psql -d datawalker -c "SELECT * FROM datawarp.v_tables;"
```
