# Grain Detection V3 Analysis

**Purpose:** Comprehensive documentation of the v3 EntityDetector implementation to inform the v3.1 grain detection enhancement.

**Source Files Analyzed:**
- `/Users/speddi/projectx/datawarp-v3/src/datawarp/models/entities.py` - EntityDetector class
- `/Users/speddi/projectx/datawarp-v3/src/datawarp/models/dataclasses.py` - Entity and EntityMatch dataclasses
- `/Users/speddi/projectx/datawarp-v3/scripts/schema/21_fdp_entity_alignment.sql` - Entity pattern seed data
- `/Users/speddi/projectx/datawarp-v3/docs/operations/ENTITY_DETECTION_SUMMARY.md` - Operational documentation

---

## 1. Entity Patterns and Priorities

### 1.1 Pattern Storage Architecture

V3 stores entity patterns in a PostgreSQL database table (`datawarp.tbl_entities`) rather than hardcoding them. This allows:
- Dynamic enable/disable of entity types
- Priority adjustments without code changes
- Learning from NHS publication patterns

**Entity Dataclass:**
```python
@dataclass
class Entity:
    entity_type: str
    description: str
    code_pattern: str  # Regex pattern e.g., ^R[A-Z0-9]{1,4}$
    code_examples: list[str] = field(default_factory=list)
    name_column_hint: Optional[str] = None
    code_column_hints: list[str] = field(default_factory=list)
    nhs_data_dict_url: Optional[str] = None
    priority: int = 50  # Detection priority (higher = more specific)
    enabled: bool = True  # Whether this entity is active for detection
    is_organizational: bool = True  # True for org entities, False for dimensions
```

### 1.2 Entity Patterns Reference Table

| Entity Type | Priority | Code Pattern | Examples | Description |
|-------------|----------|--------------|----------|-------------|
| `trust` | 100 | `^R[A-Z0-9]{1,4}$` | RJ1, RXH, R0A | NHS Trust (~220 in England) |
| `icb` | 100 | `^Q[A-Z0-9]{2}$` | QWE, QOP, QHG | Integrated Care Board (42 in England) |
| `gp_practice` | 100 | `^[A-Z][0-9]{5}$` | A81001, B82001 | GP Practice (~6,500 in England) |
| `ambulance_trust` | 80 | `^R[A-Z][A-Z0-9]$` | RRU, RYC, RX9 | Ambulance Trust (10 in England) |
| `specialty` | 90 | `^[0-9]{3}$` | 100, 110, 300 | Medical Specialty (disabled by default) |
| `site` | 90 | `^[A-Z0-9]{5}$` | RJ101, RXH01 | NHS Site (disabled by default) |
| `pcn` | 70 | `^U[0-9]{5}$` | U12345 | Primary Care Network (~1,250 in England) |
| `sub_icb` | 70 | `^[0-9]{2}[A-Z]$` | 00J, 00K | Sub-ICB Location (formerly CCG, ~100+) |
| `local_authority` | 60 | `^E[0-9]{8}$` | E06000001 | Local Authority (~150 in England) |
| `region` | 50 | `^Y[0-9]{2}$` | Y56, Y58, Y61 | NHS England Region (7 regions) |
| `national` | 10 | `^(ENGLAND\|ALL\|TOTAL)$` | England, Total | National aggregate |
| `unknown` | 1 | `.*` | (any) | Fallback with `--allow-unknown-entity` |

### 1.3 Priority Logic

Higher priority = more specific entity type. When confidence is equal, priority breaks ties:

```
Priority order: trust/icb/gp (100) > specialty/site (90) > ambulance (80) > pcn/sub_icb (70)
                > local_authority (60) > region (50) > national (10) > unknown (1)
```

**Example decision:**
- Data has both Region codes (Y61) AND Trust codes (RJ1)
- Both match at similar confidence
- Trust (100) beats Region (50) -> Detected as Trust

---

## 2. Column Detection Strategies

### 2.1 Core Detection Method

```python
def detect(self, column_values: list[str], min_confidence: float = None) -> Optional[EntityMatch]:
    """Detect entity type from column values.

    Args:
        column_values: Sample values from potential entity column
        min_confidence: Override minimum confidence threshold (default: MIN_CONFIDENCE)

    Returns:
        EntityMatch if confidence >= min_confidence, else None
    """
    cleaned = self._clean_values(column_values)  # Remove EXCLUDE_VALUES
    if not cleaned:
        return None

    threshold = min_confidence if min_confidence is not None else self.MIN_CONFIDENCE
    best_match: Optional[EntityMatch] = None
    best_confidence = 0.0

    for entity_type, pattern in self._compiled.items():
        if entity_type == 'unknown':
            continue  # Skip 'unknown' entity - only used with allow_unknown flag

        matched = sum(1 for v in cleaned if pattern.match(v))
        confidence = matched / len(cleaned) if cleaned else 0.0

        if confidence >= threshold and confidence > best_confidence:
            best_confidence = confidence
            best_match = EntityMatch(
                entity_type=entity_type,
                confidence=confidence,
                matched_values=matched,
                total_values=len(cleaned),
                pattern_used=self.entities[entity_type].code_pattern
            )

    return best_match
```

### 2.2 Confidence Thresholds

| Constant | Value | Purpose |
|----------|-------|---------|
| `MIN_CONFIDENCE` | 0.7 (70%) | Standard threshold for entity detection |
| `MIN_CONFIDENCE_ORG_COLUMN` | 0.3 (30%) | Lower threshold for columns named "Org code" etc. |

**Rationale:** Primary org columns in hierarchical tables often have mixed entity types (trusts + ICBs + CCGs), so a lower threshold is needed.

### 2.3 Column Name Filtering

The `_is_likely_entity_column()` function filters which columns to check:

```python
def _is_likely_entity_column(self, column_name: str) -> bool:
    """Check if column name suggests it contains entity codes/names."""
    col_lower = column_name.lower().replace('_', ' ').replace('\n', ' ')

    # Measure keywords -> NOT an entity column
    measure_keywords = [
        'count', 'total', 'number', 'percent', 'rate', 'ratio',
        'average', 'mean', 'median', 'sum', 'deliveries', 'admissions',
        'episodes', 'attendances', 'appointments', 'referrals',
        'anaesthetic', 'caesarean', 'spontaneous', 'surgical',
        'stay', 'length', 'duration', 'days', 'hours', 'weeks',
        'breaches', 'waits', 'waiting', 'patients'
    ]
    if any(keyword in col_lower for keyword in measure_keywords):
        return False

    # Short column names (<= 15 chars) are likely entity columns
    if len(column_name) <= 15:
        return True

    # Keywords that strongly indicate entity columns
    entity_keywords = [
        'code', ' id', 'org', 'provider code', 'trust code', 'icb code',
        'region code', 'practice code', 'commissioner code',
        'org name', 'provider name', 'trust name', 'geography'
    ]
    return any(keyword in col_lower for keyword in entity_keywords)
```

### 2.4 Primary Org Column Patterns

```python
PRIMARY_ORG_COLUMN_PATTERNS = [
    'org code', 'org_code', 'organisation code', 'organization code',
    'provider code', 'provider_code'
]
```

These patterns identify columns that should be preferred in hierarchical tables even with lower confidence.

---

## 3. Hierarchical Table Handling

### 3.1 The Problem

NHS tables often have hierarchical structure with multiple entity columns:

| NHSE code | ICS code | Org code | Org name | Value |
|-----------|----------|----------|----------|-------|
| Y56 | QKK | RJ1 | Guy's and St Thomas' | 100 |
| Y56 | QKK | RJZ | King's College Hospital | 95 |

Without special handling:
- Region column (Y56) matches at 100% confidence
- ICB column (QKK) matches at 98% confidence
- Org code column has mixed types (trust/icb/ccg) at <70% for any single type
- Result: Incorrectly detected as **Region** level data

### 3.2 Two-Pass Detection Algorithm

```python
def detect_from_multiple_columns(
    self,
    columns: dict[str, list[str]],
    allow_unknown: bool = False
) -> Optional[tuple[str, EntityMatch]]:
    """Detect entity from multiple columns, return best match.

    Uses two-pass detection:
    1. First pass: Check primary org columns with lower threshold
    2. Second pass: Standard detection for all entity columns
    Then decide based on priority and hierarchical logic.
    """
    # Initialize tracking variables
    best_column: Optional[str] = None
    best_match: Optional[EntityMatch] = None
    best_confidence = 0.0
    best_priority = 0

    # ==================== FIRST PASS ====================
    # Check if there's a primary org column (hierarchical table detection)
    primary_org_match: Optional[tuple[str, EntityMatch]] = None

    for column_name, values in columns.items():
        if not self._is_likely_entity_column(column_name):
            continue

        if self._is_primary_org_column(column_name):
            # Use lower threshold for primary org columns
            match = self.detect(values, min_confidence=self.MIN_CONFIDENCE_ORG_COLUMN)
            if match:
                primary_org_match = (column_name, match)
                match.column_name = column_name

    # ==================== SECOND PASS ====================
    # Standard detection for all entity columns
    for column_name, values in columns.items():
        if not self._is_likely_entity_column(column_name):
            continue

        match = self.detect(values)  # Uses standard MIN_CONFIDENCE (0.7)
        if match:
            entity_priority = self.entities[match.entity_type].priority

            # Pick match if:
            # 1. Higher confidence, OR
            # 2. Same confidence but higher priority (more specific entity)
            if (match.confidence > best_confidence or
                (match.confidence == best_confidence and entity_priority > best_priority)):
                best_confidence = match.confidence
                best_priority = entity_priority
                best_match = match
                best_match.column_name = column_name
                best_column = column_name

    # ==================== DECISION ====================
    # If we have both a primary org match AND other matches, decide which to use
    if primary_org_match and best_match:
        primary_col, primary_match = primary_org_match
        primary_priority = self.entities[primary_match.entity_type].priority

        # Prefer primary org column over region-level detection
        if best_match.entity_type == 'region' and primary_priority > best_priority:
            logger.info(
                f"Hierarchical table detected: preferring '{primary_col}' ({primary_match.entity_type}) "
                f"over '{best_column}' ({best_match.entity_type}) - region is aggregation level"
            )
            best_column = primary_col
            best_match = primary_match
            best_confidence = primary_match.confidence
            best_priority = primary_priority

    # If no standard match but we have a primary org match, use it
    if not best_match and primary_org_match:
        best_column, best_match = primary_org_match
        best_confidence = best_match.confidence
        best_priority = self.entities[best_match.entity_type].priority

    return (best_column, best_match) if best_match else None
```

### 3.3 Algorithm Pseudocode

```
FUNCTION detect_from_multiple_columns(columns, allow_unknown):
    primary_org_match = None
    best_match = None
    best_confidence = 0
    best_priority = 0

    # PASS 1: Primary org column detection (low threshold)
    FOR each column IN columns:
        IF NOT is_likely_entity_column(column.name):
            CONTINUE
        IF is_primary_org_column(column.name):
            match = detect(column.values, threshold=0.3)
            IF match:
                primary_org_match = (column.name, match)

    # PASS 2: Standard detection (high threshold)
    FOR each column IN columns:
        IF NOT is_likely_entity_column(column.name):
            CONTINUE
        match = detect(column.values, threshold=0.7)
        IF match:
            priority = entity_priority[match.entity_type]
            IF confidence > best_confidence OR
               (confidence == best_confidence AND priority > best_priority):
                best_match = match
                best_column = column.name
                best_confidence = match.confidence
                best_priority = priority

    # DECISION: Prefer primary org over region aggregation
    IF primary_org_match AND best_match:
        IF best_match.entity_type == 'region':
            IF primary_org_match.priority > best_priority:
                RETURN primary_org_match  # Most granular entity

    IF NOT best_match AND primary_org_match:
        RETURN primary_org_match

    IF best_match:
        RETURN (best_column, best_match)

    IF allow_unknown:
        RETURN (first_column, EntityMatch(entity_type='unknown', confidence=0))

    RETURN None
```

---

## 4. Edge Cases and Constants

### 4.1 Excluded Values

Values excluded from entity detection (except for national/regional):

```python
EXCLUDE_VALUES = {
    'UNKNOWN', 'OTHER',
    'N/A', 'NA', '-', '', 'NULL', 'NONE', 'UNSPECIFIED',
    'ALL PROVIDERS', 'ALL TRUSTS', 'ALL ICBS', 'SUPPRESSED'
}
```

### 4.2 National Indicators

Values that indicate national-level data:

```python
NATIONAL_INDICATORS = {
    'ENGLAND', 'NATIONAL', 'ALL', 'TOTAL'
}
```

### 4.3 EntityMatch Result Dataclass

```python
@dataclass
class EntityMatch:
    entity_type: str
    confidence: float  # 0.0 to 1.0
    matched_values: int
    total_values: int
    pattern_used: str
    column_name: Optional[str] = None  # Which column was matched
```

### 4.4 Mixed Entity Type Handling

Mixed entity types in a single column (e.g., trusts + ICBs + CCGs) are handled by:

1. Using lower confidence threshold (30%) for primary org columns
2. Detecting the most prevalent entity type in the column
3. Preferring higher-priority (more granular) entity types when confidence is equal

---

## 5. Key Differences: V3 vs V3.1 Current Implementation

| Feature | V3 EntityDetector | V3.1 grain.py |
|---------|-------------------|---------------|
| Pattern storage | Database (tbl_entities) | Hardcoded dict |
| Hierarchical detection | Two-pass algorithm | Single pass |
| Primary org column | Special handling | Not implemented |
| Confidence thresholds | 70%/30% dual thresholds | 50% single threshold |
| Measure column filtering | Comprehensive list | Basic list |
| Entity enable/disable | Database flag | N/A |
| Name-based detection | Via code_column_hints | Via NAME_PATTERNS |
| Unknown entity | --allow-unknown-entity flag | Returns "unknown" |
| Column hint system | Database stored | Not implemented |

---

## 6. Recommendations for V3.1 Enhancement

Based on this analysis, the following features from V3 should be ported to V3.1:

### 6.1 Critical Features

1. **Two-pass hierarchical detection** - Essential for tables with multiple entity columns
2. **Primary org column detection** - Handles "Org code" columns with mixed types
3. **Dual confidence thresholds** - 70% for standard, 30% for primary org columns
4. **Extended measure keyword list** - Prevents false positives on value columns

### 6.2 Nice-to-Have Features

1. **Database-driven patterns** - Not essential for v3.1's simpler architecture
2. **Entity enable/disable** - Could be useful for specialty filtering
3. **Column hints** - Could improve detection accuracy

### 6.3 Implementation Approach

Enhance `grain.py` to add:
1. `PRIMARY_ORG_COLUMN_PATTERNS` constant
2. `_is_primary_org_column()` helper function
3. Two-pass detection in `detect_grain()`
4. Lower confidence threshold for primary org columns
5. Extended `MEASURE_KEYWORDS` list

---

## 7. Reference: Complete Constants

### 7.1 Measure Keywords (Extended List)

```python
MEASURE_KEYWORDS = [
    'count', 'total', 'number', 'percent', 'rate', 'ratio',
    'average', 'mean', 'median', 'sum', 'deliveries', 'admissions',
    'episodes', 'attendances', 'appointments', 'referrals',
    'anaesthetic', 'caesarean', 'spontaneous', 'surgical',
    'stay', 'length', 'duration', 'days', 'hours', 'weeks',
    'breaches', 'waits', 'waiting', 'patients'
]
```

### 7.2 Entity Keywords

```python
ENTITY_KEYWORDS = [
    'code', ' id', 'org', 'provider code', 'trust code', 'icb code',
    'region code', 'practice code', 'commissioner code',
    'org name', 'provider name', 'trust name', 'geography'
]
```

### 7.3 Primary Org Column Patterns

```python
PRIMARY_ORG_COLUMN_PATTERNS = [
    'org code', 'org_code', 'organisation code', 'organization code',
    'provider code', 'provider_code'
]
```

---

## 8. Test Cases from V3

Key scenarios the V3 detector handles:

1. **Simple trust table**: Single column with trust codes (R*) -> detect trust
2. **Simple ICB table**: Single column with ICB codes (Q**) -> detect ICB
3. **Hierarchical table**: Region + ICS + Org columns -> detect org (trust) not region
4. **Mixed entity column**: Column with trusts, ICBs, CCGs -> detect most prevalent
5. **National aggregate**: Values contain "ENGLAND", "TOTAL" -> detect national
6. **No entity detected**: Data columns only -> return unknown if allowed
7. **Measure column filtering**: Skip "Patient count", "Total admissions" columns

---

*Document created: 2026-01-28*
*For use in v3.1 grain detection enhancement*
