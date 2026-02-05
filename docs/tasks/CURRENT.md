# Current Tasks - DataWarp v3.1

**Last Updated:** 2026-02-05 Session End

---

## Session Summary (2026-02-05)

### Completed This Session

1. **Created Comprehensive Architecture & Technical Reference**
   - Created `docs/ARCHITECTURE.md` (2,194 lines) — the definitive document for understanding, maintaining, and extending DataWarp v3.1
   - Covers: System Overview, Architecture Deep Dive, Module Reference (all function signatures), Integration & Extension Guide
   - For both humans and coding agents — serves as feeder document for v3.2 integration
   - Includes: package map, data model, all algorithms (grain detection, period detection, enrichment), MCP server, observability
   - Full module reference with import paths, parameters, and return types for every public API

2. **Archive Documentation Audit & Cleanup**
   - Examined all 10 files in `docs/archive/` (4,204 lines total)
   - Examined 3 files in `docs/goagent/` (3,379 lines) — v3.2 chatbot design docs
   - **Absorbed** 4 files into ARCHITECTURE.md:
     - `DATAWARP_OBJECT_MAPPING.md` → Python→SQL lifecycle diagram added to Section 2.2
     - `MCP_PROMPT.md` → Concrete MCP tool output examples added to Section 2.11
     - `DATAWARP_TESTING_SPEC.md` → V3 regression table + testing framework added to Section 4.7
     - `CLAUDE_AGENT_TESTING.md` → 6 SQL validation suites + remediation guide added to Section 4.7
   - **Deleted** 2 superseded files:
     - `docs/archive/ARCHITECTURE.md` (297 lines) — fully replaced by new docs/ARCHITECTURE.md
     - `docs/archive/DATAWARP_V3.1_COMPLETE_SPEC.md` (711 lines) — fully replaced
   - **Retained** 6 files with ongoing reference value:
     - `DATABASE_SPEC.md`, `GRAIN_DETECTION_DESIGN.md`, `GRAIN_DETECTION_V3_ANALYSIS.md`, `USER_GUIDE.md`
     - `DATAWARP_OBJECT_MAPPING.md`, `DATAWARP_TESTING_SPEC.md`, `CLAUDE_AGENT_TESTING.md`, `MCP_PROMPT.md`

3. **Updated CLAUDE.md**
   - Added reference to ARCHITECTURE.md in Documentation Structure section
   - Clarified role of each document: ARCHITECTURE.md for internals, DATAWARP_GUIDE.md for operations, goagent/ for v3.2

### Files Changed

```
Created:
- docs/ARCHITECTURE.md (2,194 lines — comprehensive architecture & technical reference)

Modified:
- CLAUDE.md (updated Documentation Structure section)
- docs/tasks/CURRENT.md (this file)

Deleted:
- docs/archive/ARCHITECTURE.md (superseded)
- docs/archive/DATAWARP_V3.1_COMPLETE_SPEC.md (superseded)
```

---

## Current State

| Component | Status |
|-----------|--------|
| Discovery | ✅ Working |
| Period Detection | ✅ Working (date ranges, year-only, quarterly) |
| Grain Detection | ✅ Working (ONS codes, LA, Sub-ICB) |
| LLM Enrichment | ✅ Working (temporal qualifiers preserved) |
| Notes Extraction | ✅ Working (clinical definitions, codes) |
| MCP Metadata | ✅ Working (full context attached) |
| Data Loading | ✅ Working |
| ZIP Schema Grouping | ✅ Working (CSVs grouped by fingerprint) |
| Documentation | ✅ Comprehensive (ARCHITECTURE.md + DATAWARP_GUIDE.md) |

---

## Documentation Map

| Document | Purpose | Lines |
|----------|---------|-------|
| `docs/ARCHITECTURE.md` | Architecture, internals, module API, integration guide | 2,194 |
| `docs/mcp/DATAWARP_GUIDE.md` | Operational guide (CLI, SQL, MCP, visual diagrams) | 2,627 |
| `CLAUDE.md` | Session workflow, coding rules, quick reference | ~200 |
| `docs/goagent/` | v3.2 chatbot design (README, DESIGN_PLAN, TECHNICAL_SPEC) | 3,379 |
| `docs/archive/` | 8 historical design docs (retained for reference) | ~2,800 |

---

## Known Issues / Pending

### 1. Landing Page Scraper Missing 2020+ Periods (Some Publications)

**Observed with:** Women's Smoking Status at Time of Delivery publication

**Problem:**
- Landing page scraper finds only 2015-2019 files (56 files)
- 2020-2025 files exist on sub-pages but aren't discovered
- URL pattern mismatch: classifier expects `/january-2025` but publication uses `/-q2-2025-26`

**Workaround:** Use period-specific URL directly:
```bash
python scripts/pipeline.py bootstrap \
  --url "https://.../-q2-2025-26/data-tables" \
  --id smoking-delivery --enrich
```

**Root cause:** `src/datawarp/discovery/scraper.py` - sub-page traversal or URL pattern detection

---

## Quick Start Next Session

```bash
cd /Users/speddi/projectx/datawarp-v3.1
source .venv/bin/activate
git status

# Run all tests
PYTHONPATH=src python -m pytest tests/ -v

# Key documentation
cat docs/ARCHITECTURE.md   # Architecture & technical reference (2,194 lines)
cat docs/mcp/DATAWARP_GUIDE.md  # Operational guide (2,627 lines)

# Test bootstrap with NHS e-Referral (has 72 CSVs in ZIP)
python scripts/pipeline.py bootstrap \
  --url "https://digital.nhs.uk/data-and-information/publications/statistical/mi-nhs-e-referral-service-open-data/current/data-tables" \
  --id e-referral --enrich
```

---

## Ready for v3.2 Integration

The documentation is now comprehensive and integration-ready:

1. **`docs/ARCHITECTURE.md` Section 4.1** — Specific integration points for the v3.2 chatbot
2. **`docs/goagent/DESIGN_PLAN.md`** — Product spec with multi-LLM strategy, UI mockups
3. **`docs/goagent/TECHNICAL_SPEC.md`** — Implementation guide with working code examples

All APIs, data structures, and extension patterns are documented with import paths and examples.
