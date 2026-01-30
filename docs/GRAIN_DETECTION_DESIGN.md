# Grain Detection Enhancement Design

**Purpose:** Implementation design for porting v3 EntityDetector features to v3.1 grain.py

**Target File:** `/Users/speddi/projectx/datawarp-v3.1/src/datawarp/metadata/grain.py`

**Constraints:**
- No new database tables (per CLAUDE.md: only 3 config tables allowed)
- File must stay under 300 lines (currently 166 lines)
- Maintain backward compatibility with existing `detect_grain(df) -> dict` API

---

## 1. Design Decisions

### 1.1 Pattern Storage: Hardcoded Constants

**Decision:** All entity patterns stored as Python constants, not database tables.

**Rationale:**
- CLAUDE.md explicitly prohibits new database tables beyond the 3 config tables
- V3's `tbl_entities` table added complexity without significant benefit for v3.1's scope
- Patterns are stable (NHS entity codes don't change format frequently)
- Code changes require deployment anyway; database-driven patterns add unnecessary indirection

**Trade-offs:**
- (+) Simpler deployment (no seed data)
- (+) Patterns visible in code for debugging
- (+) No database dependency for grain detection
- (-) Requires code change to add new entity types
- (-) Cannot enable/disable entities at runtime

**Conclusion:** Trade-offs favor hardcoded patterns for v3.1's focused scope.

---

### 1.2 API Design: Preserve Existing Signature

**Decision:** Keep `detect_grain(df: pd.DataFrame) -> dict` as the only public function.

**Rationale:**
- Existing callers (pipeline.py, enrich.py) use this signature
- Internal refactoring should not break external interface
- Complex detection logic stays private via underscore-prefixed helpers

**Public API (unchanged):**
```python
def detect_grain(df: pd.DataFrame) -> dict:
    """
    Returns:
        {
            "grain": "icb",  # or "trust", "gp_practice", "national", "unknown"
            "grain_column": "org_code",
            "confidence": 0.95,
            "description": "ICB level data"
        }
    """
```

**New Internal Functions:**
```python
def _is_primary_org_column(col_name: str) -> bool
def _is_likely_entity_column(col_name: str) -> bool
def _clean_values(values: list[str]) -> list[str]
def _detect_entity_in_column(values: list[str], min_confidence: float) -> Optional[dict]
```

---

### 1.3 Algorithm: Two-Pass Detection

**Decision:** Implement two-pass detection algorithm from v3.

**Problem Solved:**
NHS hierarchical tables contain multiple entity columns (Region + ICB + Trust). Without special handling, Region (with 100% confidence) beats Trust in the "Org code" column (with <70% due to mixed types).

**Algorithm:**
```
Pass 1: Check PRIMARY_ORG_COLUMN patterns with LOW threshold (0.3)
        -> Captures "Org code" columns with mixed entity types

Pass 2: Standard detection with NORMAL threshold (0.5)
        -> All other entity columns checked normally

Decision: If Pass 2 finds "region" but Pass 1 found a higher-priority entity,
          prefer Pass 1 result (the region is just an aggregation level)
```

**Rationale:**
- Mirrors v3 logic that was proven on real NHS data
- Handles hierarchical tables correctly (most common edge case)
- Maintains simplicity for simple single-entity tables

---

### 1.4 Confidence Thresholds

**Decision:** Use dual thresholds.

| Constant | Value | Purpose |
|----------|-------|---------|
| `MIN_CONFIDENCE` | 0.5 (50%) | Standard detection threshold |
| `MIN_CONFIDENCE_ORG_COLUMN` | 0.3 (30%) | Lower threshold for primary org columns |
| `MIN_MATCHES` | 3 | Minimum matching values to avoid false positives |

**Rationale:**
- 0.5 (current v3.1) is already lower than v3's 0.7, which helps catch more cases
- 0.3 for primary org columns matches v3 and is necessary for mixed-entity columns
- MIN_MATCHES=3 prevents single-value false positives

---

### 1.5 CCG Pattern Addition

**Decision:** Add CCG (sub_icb) entity pattern.

**Rationale:**
- CCGs (Clinical Commissioning Groups) appear in legacy and transition data
- Pattern: `^[0-9]{2}[A-Z]$` (e.g., "00J", "00K")
- V3 had this; omitting it causes detection failures on older datasets

---

## 2. Complete Constants

### 2.1 Entity Patterns (Updated)

```python
ENTITY_PATTERNS = {
    'trust': {
        'pattern': r'^R[A-Z0-9]{1,4}$',
        'description': 'NHS Trust level',
        'examples': ['RJ1', 'RXH', 'R0A'],
        'priority': 100
    },
    'icb': {
        'pattern': r'^Q[A-Z0-9]{2}$',
        'description': 'Integrated Care Board level',
        'examples': ['QWE', 'QOP', 'QHG'],
        'priority': 100
    },
    'gp_practice': {
        'pattern': r'^[A-Z][0-9]{5}$',
        'description': 'GP Practice level',
        'examples': ['A81001', 'B82001'],
        'priority': 100
    },
    'ccg': {  # NEW - legacy entity still in data
        'pattern': r'^[0-9]{2}[A-Z]$',
        'description': 'Clinical Commissioning Group (legacy)',
        'examples': ['00J', '00K', '01A'],
        'priority': 70
    },
    'region': {
        'pattern': r'^Y[0-9]{2}$',
        'description': 'NHS Region level',
        'examples': ['Y56', 'Y58', 'Y59'],
        'priority': 50
    },
    'national': {
        'pattern': None,
        'keywords': ['ENGLAND', 'NATIONAL', 'TOTAL', 'ALL'],
        'description': 'National aggregate',
        'priority': 10
    }
}
```

### 2.2 Name Patterns (Unchanged)

```python
NAME_PATTERNS = {
    'trust': {
        'keywords': ['NHS TRUST', 'NHS FOUNDATION TRUST', 'UNIVERSITY HOSPITAL'],
        'description': 'NHS Trust level (by name)',
        'priority': 80
    },
    'icb': {
        'keywords': ['INTEGRATED CARE BOARD', ' ICB'],
        'description': 'Integrated Care Board (by name)',
        'priority': 80
    },
}
```

### 2.3 New Constants to Add

```python
# Thresholds
MIN_CONFIDENCE = 0.5  # Standard detection threshold
MIN_CONFIDENCE_ORG_COLUMN = 0.3  # Lower threshold for primary org columns
MIN_MATCHES = 3  # Minimum matching values to avoid false positives

# Primary org column patterns (hierarchical table detection)
PRIMARY_ORG_COLUMN_PATTERNS = [
    'org code', 'org_code',
    'organisation code', 'organization code',
    'provider code', 'provider_code',
    'org id', 'org_id',
    'provider id', 'provider_id'
]

# Values to exclude from entity detection
EXCLUDE_VALUES = {
    'UNKNOWN', 'OTHER', 'UNSPECIFIED',
    'N/A', 'NA', '-', '', 'NULL', 'NONE',
    'ALL PROVIDERS', 'ALL TRUSTS', 'ALL ICBS',
    'SUPPRESSED', 'REDACTED', '*'
}

# Extended measure keywords (from v3)
MEASURE_KEYWORDS = [
    # Existing
    'count', 'total', 'number', 'percent', 'rate', 'ratio',
    'average', 'mean', 'median', 'sum', 'referrals', 'waiting',
    # New from v3
    'deliveries', 'admissions', 'episodes', 'attendances',
    'appointments', 'anaesthetic', 'caesarean', 'spontaneous',
    'surgical', 'stay', 'length', 'duration', 'days', 'hours',
    'weeks', 'breaches', 'waits', 'patients'
]

# Entity column keywords (for _is_likely_entity_column)
ENTITY_KEYWORDS = [
    'code', ' id', 'org', 'provider code', 'trust code', 'icb code',
    'region code', 'practice code', 'commissioner code',
    'org name', 'provider name', 'trust name', 'geography'
]
```

---

## 3. Algorithm Pseudocode

### 3.1 Main Function: detect_grain()

```
FUNCTION detect_grain(df: DataFrame) -> dict:
    IF df.empty:
        RETURN {"grain": "unknown", ...}

    # ========== PASS 1: Primary Org Column Detection ==========
    primary_org_match = None

    FOR col IN first_10_columns(df):
        IF is_measure_column(col):
            CONTINUE

        IF is_primary_org_column(col):
            values = clean_values(get_column_values(df, col))
            match = detect_entity_in_column(values, threshold=0.3)
            IF match:
                primary_org_match = match
                primary_org_match["grain_column"] = col

    # ========== PASS 2: Standard Entity Detection ==========
    best_match = None
    best_priority = 0

    FOR col IN first_10_columns(df):
        IF is_measure_column(col):
            CONTINUE
        IF NOT is_likely_entity_column(col):
            CONTINUE  # Skip long descriptive columns

        values = clean_values(get_column_values(df, col))
        match = detect_entity_in_column(values, threshold=0.5)

        IF match:
            priority = match["priority"] * match["confidence"]
            IF priority > best_priority:
                best_priority = priority
                best_match = match
                best_match["grain_column"] = col

    # ========== DECISION: Resolve Hierarchical Tables ==========
    IF primary_org_match AND best_match:
        IF best_match["grain"] == "region":
            # Region is just aggregation level; prefer primary org entity
            IF ENTITY_PATTERNS[primary_org_match["grain"]]["priority"] > 50:
                RETURN primary_org_match

    IF best_match:
        RETURN best_match

    IF primary_org_match:
        RETURN primary_org_match

    # ========== PASS 3: Name-based Detection (fallback) ==========
    FOR col IN first_10_columns(df):
        match = detect_by_name_patterns(df, col)
        IF match:
            RETURN match

    # ========== PASS 4: National Keywords (last resort) ==========
    match = detect_national_keywords(df)
    IF match:
        RETURN match

    RETURN {"grain": "unknown", "grain_column": None, "confidence": 0, "description": ""}
```

### 3.2 Helper: _is_primary_org_column()

```
FUNCTION is_primary_org_column(col_name: str) -> bool:
    col_lower = col_name.lower().replace('_', ' ').replace('\n', ' ')
    RETURN any(pattern IN col_lower FOR pattern IN PRIMARY_ORG_COLUMN_PATTERNS)
```

### 3.3 Helper: _is_likely_entity_column()

```
FUNCTION is_likely_entity_column(col_name: str) -> bool:
    col_lower = col_name.lower().replace('_', ' ').replace('\n', ' ')

    # Measure keywords -> NOT an entity column
    IF any(keyword IN col_lower FOR keyword IN MEASURE_KEYWORDS):
        RETURN False

    # Short column names (<= 20 chars) are likely entity columns
    IF len(col_name) <= 20:
        RETURN True

    # Check for entity keywords
    RETURN any(keyword IN col_lower FOR keyword IN ENTITY_KEYWORDS)
```

### 3.4 Helper: _clean_values()

```
FUNCTION clean_values(values: list[str]) -> list[str]:
    """Remove excluded values that would skew detection."""
    RETURN [v FOR v IN values IF v.upper() NOT IN EXCLUDE_VALUES]
```

### 3.5 Helper: _detect_entity_in_column()

```
FUNCTION detect_entity_in_column(values: list[str], min_confidence: float) -> Optional[dict]:
    """Try each entity pattern against values, return best match."""
    IF NOT values:
        RETURN None

    best_match = None
    best_priority = 0

    FOR entity_type, config IN ENTITY_PATTERNS.items():
        IF NOT config.get("pattern"):  # Skip keyword-only (national)
            CONTINUE

        matches = count(v FOR v IN values IF regex.match(config["pattern"], v))
        confidence = matches / len(values)

        IF confidence >= min_confidence AND matches >= MIN_MATCHES:
            priority = config["priority"] * confidence
            IF priority > best_priority:
                best_priority = priority
                best_match = {
                    "grain": entity_type,
                    "confidence": round(confidence, 2),
                    "description": config["description"],
                    "priority": config["priority"]
                }

    RETURN best_match
```

---

## 4. Function Signatures

### 4.1 Public Function (Unchanged)

```python
def detect_grain(df: pd.DataFrame) -> Dict:
    """
    Scan DataFrame columns for entity codes to determine data granularity.

    Uses two-pass detection:
    1. Primary org columns with low threshold (0.3) for hierarchical tables
    2. Standard detection with normal threshold (0.5) for all entity columns

    Returns:
        {
            "grain": "icb",  # or "trust", "gp_practice", "ccg", "region", "national", "unknown"
            "grain_column": "org_code",  # which column has the entity
            "confidence": 0.95,
            "description": "ICB level data"
        }
    """
```

### 4.2 New Internal Functions

```python
def _is_primary_org_column(col_name: str) -> bool:
    """Check if column name indicates a primary organization column."""

def _is_likely_entity_column(col_name: str) -> bool:
    """Check if column name suggests it contains entity codes/names."""

def _clean_values(values: list[str]) -> list[str]:
    """Remove excluded values (UNKNOWN, N/A, etc.) from list."""

def _detect_entity_in_column(
    values: list[str],
    min_confidence: float = MIN_CONFIDENCE
) -> Optional[Dict]:
    """
    Detect entity type from column values.

    Args:
        values: Cleaned, uppercase string values from column
        min_confidence: Minimum match ratio required

    Returns:
        Match dict or None
    """
```

---

## 5. File Structure Plan

### 5.1 Current State (166 lines)

```
Lines 1-5:    Imports
Lines 7-38:   ENTITY_PATTERNS
Lines 40-52:  NAME_PATTERNS
Lines 54-57:  MEASURE_KEYWORDS
Lines 60-159: detect_grain()
Lines 162-166: _is_measure_column()
```

### 5.2 Proposed Structure (~250 lines)

```
Lines 1-7:     Imports + docstring (7 lines)

Lines 9-25:    Thresholds and constants (17 lines)
               - MIN_CONFIDENCE, MIN_CONFIDENCE_ORG_COLUMN, MIN_MATCHES
               - PRIMARY_ORG_COLUMN_PATTERNS
               - EXCLUDE_VALUES

Lines 27-55:   ENTITY_PATTERNS with CCG (29 lines)

Lines 57-67:   NAME_PATTERNS (11 lines)

Lines 69-80:   MEASURE_KEYWORDS extended (12 lines)

Lines 82-88:   ENTITY_KEYWORDS (7 lines)

Lines 90-110:  Helper functions (21 lines)
               - _is_primary_org_column()
               - _is_likely_entity_column()
               - _clean_values()
               - _is_measure_column() [existing, moved]

Lines 112-165: _detect_entity_in_column() (54 lines)

Lines 167-250: detect_grain() main function (84 lines)
               - Pass 1: Primary org column detection
               - Pass 2: Standard entity detection
               - Decision: Hierarchical table resolution
               - Pass 3: Name-based detection
               - Pass 4: National keywords

TOTAL: ~250 lines (under 300 limit)
```

### 5.3 Line Budget

| Section | Lines | Notes |
|---------|-------|-------|
| Imports + module docstring | 7 | |
| Constants (new) | 17 | Thresholds, patterns, excluded values |
| ENTITY_PATTERNS | 29 | +6 for CCG |
| NAME_PATTERNS | 11 | Unchanged |
| MEASURE_KEYWORDS | 12 | Extended |
| ENTITY_KEYWORDS | 7 | New |
| Helper functions | 21 | 4 small functions |
| _detect_entity_in_column | 54 | Extracted from detect_grain |
| detect_grain (main) | 84 | Two-pass algorithm |
| **Total** | **~242** | Under 300 limit |

---

## 6. Migration Notes

### 6.1 Backward Compatibility

**Guaranteed:**
- `detect_grain(df)` signature unchanged
- Return value structure unchanged
- Existing grain values (`trust`, `icb`, `gp_practice`, `region`, `national`, `unknown`) unchanged

**New grain value:**
- `ccg` - Only returned for data with CCG patterns; does not affect existing data

### 6.2 Behavior Changes

| Scenario | Before | After |
|----------|--------|-------|
| Hierarchical table (Region + Trust) | Returns `region` (highest confidence) | Returns `trust` (primary org column) |
| Mixed entity column (trust + ICB + CCG) | May fail detection | Detects at 30% threshold |
| "Org code" column with trusts | 50% threshold | 30% threshold (more permissive) |
| CCG codes (e.g., "00J") | `unknown` | `ccg` |

### 6.3 Testing Requirements

Test cases to validate migration:

1. **Simple trust table** - Single column with trust codes -> detect `trust`
2. **Simple ICB table** - Single column with ICB codes -> detect `icb`
3. **Hierarchical table** - Region + ICB + Trust columns -> detect `trust` (not `region`)
4. **Mixed entity column** - Trusts + ICBs + CCGs in "Org code" -> detect most prevalent
5. **Legacy CCG data** - Column with "00J", "00K" codes -> detect `ccg`
6. **National aggregate** - Values contain "ENGLAND" -> detect `national`
7. **No entities** - Only measure columns -> return `unknown`
8. **Measure column filtering** - Skip "Patient count" columns

### 6.4 Rollback Plan

If issues arise:
1. The old grain.py is in git history
2. New features are additive; removing them reverts to old behavior
3. No database migrations required

---

## 7. Implementation Checklist

- [ ] Add threshold constants (MIN_CONFIDENCE, MIN_CONFIDENCE_ORG_COLUMN, MIN_MATCHES)
- [ ] Add PRIMARY_ORG_COLUMN_PATTERNS constant
- [ ] Add EXCLUDE_VALUES constant
- [ ] Add CCG to ENTITY_PATTERNS
- [ ] Extend MEASURE_KEYWORDS with v3 keywords
- [ ] Add ENTITY_KEYWORDS constant
- [ ] Implement _is_primary_org_column()
- [ ] Implement _is_likely_entity_column()
- [ ] Implement _clean_values()
- [ ] Extract _detect_entity_in_column()
- [ ] Refactor detect_grain() with two-pass algorithm
- [ ] Add hierarchical table decision logic
- [ ] Write unit tests for 8 scenarios above
- [ ] Verify file stays under 300 lines
- [ ] Test with real NHS URLs

---

*Document created: 2026-01-28*
*For implementation in grain.py enhancement task*
