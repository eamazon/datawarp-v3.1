# Current Tasks - DataWarp v3.1

**Last Updated:** 2025-01-28 Session End

---

## Session Summary (2025-01-28)

### Completed This Session

1. **CLI Refactoring** - Moved 1178-line pipeline.py into modular CLI structure
   - `src/datawarp/cli/bootstrap.py` - Bootstrap command
   - `src/datawarp/cli/scan.py` - Scan command
   - `src/datawarp/cli/backfill.py` - Backfill command
   - `src/datawarp/cli/list_history.py` - List and history commands
   - `src/datawarp/cli/console.py` - Shared Rich console with theme
   - `src/datawarp/cli/helpers.py` - Shared utilities
   - `src/datawarp/cli/file_processor.py` - File processing logic
   - `src/datawarp/cli/sheet_selector.py` - Sheet analysis UI

2. **Console Colors** - Fixed for light terminal backgrounds
   - All colors now dark blue for readability
   - Disabled Rich auto-highlighter that was causing cyan dates

3. **Period URL Detection** - Fixed classifier for complex period URLs
   - URLs like `/final-october-2025-provisional-november-2025-official-statistics` now detected
   - Scrapes specific period URL instead of entire landing page

4. **Existing Pipeline Detection** - Bootstrap now checks if pipeline exists
   - Shows loaded periods and suggests using `scan` command
   - Asks for confirmation before re-bootstrapping

5. **Multi-Period Selection** - When ≤3 periods, offers "all" option
   - Can load files from multiple periods in one bootstrap
   - Each file uses its own detected period

6. **Column Type Fix** - Description columns now use TEXT not VARCHAR(255)
   - Fixed truncation errors for long text fields

7. **Smart Period Replace** - Loader deletes existing period data before insert
   - Prevents duplicate rows when re-loading same period

8. **Tables Created vs Updated** - Summary now distinguishes new vs existing tables

---

## Pending Task: Multi-Period File Pattern Matching (Task #31)

### Problem
When bootstrapping 6 files (3 Oct + 3 Nov with same schemas), creates 6 tables instead of 3.

### Root Cause
Bootstrap processes each file independently instead of grouping by file type first.

### Recommended Solution: Hybrid Deterministic Approach

```
1. FILENAME GROUPING (deterministic)
   Extract type: "msds-oct2025-exp-data.csv" → "data"
   Group: {data: [oct, nov], measures: [...], dq: [...]}

2. SCHEMA VALIDATION (safety check)
   Verify files in each group have matching columns

3. LLM ENRICHMENT (where it adds value)
   Enrich ONE file per group, apply mapping to all
```

### Why NOT Two-Phase LLM
- NHS filenames follow predictable conventions
- Deterministic grouping is faster, cheaper, more reliable
- LLM only where it adds unique value (semantic naming)

### Implementation
- Refactor `_bootstrap_impl` to group files before processing
- Add `src/datawarp/cli/file_grouper.py` for grouping logic

---

## Current State

| Component | Status |
|-----------|--------|
| Discovery | ✅ Working |
| Period Detection | ✅ Working |
| Grain Detection | ✅ Working |
| LLM Enrichment | ✅ Working |
| Data Loading | ✅ Working |
| CLI Modular Structure | ✅ Working |
| Multi-Period Bootstrap | ⚠️ Creates too many tables |

---

## Next Session Priority

1. **Implement Task #31** - File pattern grouping for multi-period bootstrap
2. **Test** - Verify 6 files → 3 tables with maternity data

---

## Files Changed This Session

```
Modified:
- scripts/pipeline.py (reduced to thin CLI entry point)
- src/datawarp/cli/*.py (all CLI modules)
- src/datawarp/loader/excel.py (period replace, TEXT for descriptions)
- src/datawarp/loader/extractor.py (TEXT for description columns)
- src/datawarp/discovery/classifier.py (period URL detection fix)
- src/datawarp/metadata/enrich.py (reverted band-aid changes)

Created:
- src/datawarp/tracking.py (run tracking)
```

---

## Quick Start Next Session

```bash
cd /Users/speddi/projectx/datawarp-v3.1
git status

# Read Task #31 for context
cat docs/tasks/CURRENT.md

# Test current state
python scripts/pipeline.py bootstrap \
  --url https://digital.nhs.uk/.../maternity-services-monthly-statistics/final-october-2025-provisional-november-2025-official-statistics \
  --enrich
```
