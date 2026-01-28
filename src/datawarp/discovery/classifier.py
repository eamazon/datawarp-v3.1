"""URL classification for NHS publications.

Determines how to discover files for a publication:
- template: NHS Digital with predictable URLs → generate from pattern
- discover: NHS England or hash codes → scrape landing page
- explicit: User provides exact URLs

Ported from DataWarp v3 add_publication.py
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


# Configuration patterns
PATTERNS = {
    'month_names': {
        'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
        'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
    },
    'url_extraction': [
        # (regex pattern, landing_group, year_group, month_group, pattern_template)
        (r'(.+)/for-(\w+)-(\d{4})', 1, 3, 2, '{landing_page}/for-{month_name}-{year}'),
        (r'(.+)/performance-(\w+)-(\d{4})', 1, 3, 2, '{landing_page}/performance-{month_name}-{year}'),
        (r'(.+)/england-(\w+)-(\d{4})', 1, 3, 2, '{landing_page}/england-{month_name}-{year}'),
        (r'(.+)/(\w+)-(\d{4})', 1, 3, 2, '{landing_page}/{month_name}-{year}'),
        (r'(.+)/(\d{4})-(\d{2})', 1, 2, 3, '{landing_page}/{year}-{month:02d}'),
    ],
    'expansions': {
        'mi': 'Management Information', 'gp': 'GP', 'nhs': 'NHS',
        'adhd': 'ADHD', 'ae': 'A&E', 'rtt': 'RTT'
    }
}


@dataclass
class URLClassification:
    """Result of URL classification."""

    # Publication identification
    publication_id: str
    name: str

    # Source information
    source: str  # nhs_digital, nhs_england, nhs_digital_redirect_england, unknown
    landing_page: str

    # Discovery configuration
    discovery_mode: str  # template, discover, explicit
    url_pattern: Optional[str] = None

    # Frequency
    frequency: str = 'monthly'  # monthly, quarterly, annual

    # Period information
    detected_periods: List[str] = field(default_factory=list)
    period_from: Optional[str] = None
    period_to: Optional[str] = None

    # Flags
    is_landing_page: bool = True
    has_hash: bool = False
    redirects_to_england: bool = False

    # Original URL (for when user provides period-specific URL)
    original_url: Optional[str] = None
    is_period_url: bool = False  # True if user gave a specific period URL like /january-2026


def classify_url(url: str) -> URLClassification:
    """Classify NHS URL and determine discovery strategy.

    Args:
        url: NHS publication URL

    Returns:
        URLClassification with all metadata
    """
    parsed = urlparse(url)
    url_lower = url.lower()

    # Detect source
    source = 'nhs_digital' if 'digital.nhs.uk' in parsed.netloc else \
             'nhs_england' if 'england.nhs.uk' in parsed.netloc else 'unknown'

    # Check if this is a landing page (no file extension)
    is_landing_page = not any(url_lower.endswith(ext)
                              for ext in ['.xlsx', '.xls', '.zip', '.csv', '.pdf', '.ods'])

    # Extract period, landing page, and pattern
    period_info = None
    landing_page = None
    url_pattern = None

    for regex, land_grp, year_grp, month_grp, template in PATTERNS['url_extraction']:
        match = re.search(regex, url_lower, re.IGNORECASE)
        if match:
            landing_page = url[:match.start(land_grp)] + match.group(land_grp)
            year_str = match.group(year_grp)
            month_val = match.group(month_grp)

            # Parse month
            if month_val.isdigit():
                month = int(month_val)
            else:
                month = PATTERNS['month_names'].get(month_val.lower())

            if month and 1 <= month <= 12:
                year = int(year_str)
                if 2020 <= year <= 2030:
                    period_info = (year, month)
                    url_pattern = template
                    break

    # Fallback landing page extraction
    if not landing_page:
        if is_landing_page:
            landing_page = url.rstrip('/')
        elif source == 'nhs_england' and '/statistical-work-areas/' in parsed.path:
            match = re.search(r'(/statistics/statistical-work-areas/[^/]+/?)', parsed.path)
            landing_page = f"{parsed.scheme}://{parsed.netloc}{match.group(1)}" if match else \
                          f"{parsed.scheme}://{parsed.netloc}/statistics/"
        else:
            landing_page = url.rsplit('/', 1)[0]

    # Detect hash codes (5+ char alphanumeric with mixed case or numbers)
    has_hash = bool(re.search(r'[a-zA-Z0-9]{5,}', parsed.path) and
                    re.search(r'\d.*[A-Z]|[A-Z].*\d', parsed.path))

    # Check if NHS Digital page redirects to NHS England files
    redirects_to_england = False
    if source == 'nhs_digital' and is_landing_page:
        redirects_to_england = _check_redirects_to_england(url, landing_page)
        if redirects_to_england:
            source = 'nhs_digital_redirect_england'

    # Generate publication code
    path_parts = [p for p in urlparse(landing_page).path.split('/') if p]

    # Check if last segment is a period (like "31-december-2025" or "january-2025")
    # If so, use the previous segment as the publication code
    from ..utils.period import parse_period
    is_period_url = False
    if path_parts:
        last_segment = path_parts[-1]
        if parse_period(last_segment):
            is_period_url = True
            # Use second-to-last segment as publication name
            if len(path_parts) > 1:
                path_parts = path_parts[:-1]
                # Update landing_page to remove period segment
                landing_page = landing_page.rsplit('/', 1)[0]

    if source == 'nhs_england' and 'statistical-work-areas' in path_parts:
        idx = path_parts.index('statistical-work-areas')
        code = path_parts[idx + 1] if idx + 1 < len(path_parts) else 'new_pub'
    else:
        code = path_parts[-1] if path_parts else 'new_pub'

    # Clean up code
    code = re.sub(r'^(mi-|statistical-|rtt-data-)', '', code).replace('-', '_')

    # Generate name
    name = code.replace('_', ' ').title()
    for abbr, full in PATTERNS['expansions'].items():
        name = re.sub(rf'\b{abbr}\b', full, name, flags=re.IGNORECASE)

    # Determine discovery mode
    detected_periods = []
    detected_frequency = 'monthly'

    if is_landing_page and (source == 'nhs_england' or redirects_to_england):
        # NHS England or redirected → use discover mode (scrape)
        discovery_mode = 'discover'
    elif has_hash:
        # Hash detected → explicit mode
        discovery_mode = 'explicit'
    else:
        # NHS Digital → template mode
        discovery_mode = 'template'

        # For NHS Digital landing pages, detect periods and frequency
        if is_landing_page and source == 'nhs_digital':
            detected_periods, detected_frequency, _, _ = _detect_nhs_digital_periods(landing_page)

    # Determine frequency
    if detected_frequency in ['quarterly', 'monthly']:
        frequency = detected_frequency
    else:
        frequency = 'quarterly' if re.search(r'\bq[1-4]\b|quarter|fy\d{2}', url_lower) else 'monthly'

    # Fix URL pattern for template mode
    if discovery_mode == 'template' and not url_pattern:
        url_pattern = '{landing_page}/{month_name}-{year}'

    # Build period range
    period_from = detected_periods[0] if detected_periods else None
    period_to = detected_periods[-1] if detected_periods else None

    if not period_from and period_info:
        period_from = f"{period_info[0]}-{period_info[1]:02d}"

    # Detect if user gave a period-specific URL (e.g., /january-2026)
    # Don't overwrite if already detected from path segment check above
    if not is_period_url:
        is_period_url = period_info is not None and url.rstrip('/') != landing_page.rstrip('/')

    return URLClassification(
        publication_id=code,
        name=name,
        source=source,
        landing_page=landing_page,
        discovery_mode=discovery_mode,
        url_pattern=url_pattern,
        frequency=frequency,
        detected_periods=detected_periods,
        period_from=period_from,
        period_to=period_to,
        is_landing_page=is_landing_page,
        has_hash=has_hash,
        redirects_to_england=redirects_to_england,
        original_url=url,
        is_period_url=is_period_url,
    )


def _check_redirects_to_england(url: str, landing_page: str) -> bool:
    """Check if NHS Digital page has data files hosted on NHS England."""
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return False

        soup = BeautifulSoup(response.content, 'html.parser')
        data_extensions = ['.xlsx', '.xls', '.csv', '.zip']

        # Check for england.nhs.uk data file links
        england_links = soup.find_all('a', href=re.compile(r'england\.nhs\.uk'))
        for link in england_links:
            href = link.get('href', '')
            if any(ext in href.lower() for ext in data_extensions):
                return True

        # Check sub-pages (one level deep)
        subpage_links = soup.find_all('a', href=re.compile(rf'{re.escape(landing_page)}/'))
        if subpage_links:
            subpage_url = subpage_links[0].get('href')
            if subpage_url and subpage_url.startswith('http'):
                try:
                    subpage_response = requests.get(subpage_url, timeout=10)
                    if subpage_response.status_code == 200:
                        subpage_soup = BeautifulSoup(subpage_response.content, 'html.parser')
                        for link in subpage_soup.find_all('a', href=re.compile(r'england\.nhs\.uk')):
                            href = link.get('href', '')
                            if any(ext in href.lower() for ext in data_extensions):
                                return True
                except Exception:
                    pass

        return False
    except Exception as e:
        logger.debug(f"Error checking redirect: {e}")
        return False


def _detect_nhs_digital_periods(landing_page: str) -> Tuple[List[str], str, Optional[str], Optional[str]]:
    """Detect available periods from NHS Digital landing page.

    Returns:
        (periods_list, frequency, earliest_period, latest_period)
    """
    try:
        from collections import Counter

        response = requests.get(landing_page, timeout=10)
        if response.status_code != 200:
            return ([], 'monthly', None, None)

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract month-year patterns from sub-page links
        periods = []
        month_map = PATTERNS['month_names']

        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            # Match patterns like "/mi-adhd/august-2025"
            match = re.search(r'/([a-z]+)-(\d{4})(?:/|$)', href)
            if match:
                month_name = match.group(1)
                year = int(match.group(2))
                if month_name in month_map and 2020 <= year <= 2030:
                    month_num = month_map[month_name]
                    periods.append((year, month_num, f"{year}-{month_num:02d}"))

        if not periods:
            return ([], 'monthly', None, None)

        # Sort and deduplicate
        periods = sorted(set(periods))
        period_strs = [p[2] for p in periods]

        # Detect frequency
        if len(periods) >= 3:
            months = Counter([p[1] for p in periods])
            frequency = 'quarterly' if len(months) <= 4 else 'monthly'
        else:
            frequency = 'monthly'

        earliest = period_strs[0] if period_strs else None
        latest = period_strs[-1] if period_strs else None

        return (period_strs, frequency, earliest, latest)

    except Exception as e:
        logger.debug(f"Error detecting periods: {e}")
        return ([], 'monthly', None, None)


def generate_period_urls(url_pattern: str, landing_page: str,
                         start_period: str, end_period: str) -> List[str]:
    """Generate URLs from template pattern for a period range.

    Args:
        url_pattern: Template like '{landing_page}/{month_name}-{year}'
        landing_page: Base URL
        start_period: Start period (YYYY-MM)
        end_period: End period (YYYY-MM)

    Returns:
        List of generated URLs
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    month_names = ['january', 'february', 'march', 'april', 'may', 'june',
                   'july', 'august', 'september', 'october', 'november', 'december']

    urls = []

    # Parse periods
    start = datetime.strptime(start_period, '%Y-%m')
    end = datetime.strptime(end_period, '%Y-%m')

    current = start
    while current <= end:
        url = url_pattern.format(
            landing_page=landing_page,
            year=current.year,
            month=current.month,
            month_name=month_names[current.month - 1],
        )
        urls.append(url)
        current += relativedelta(months=1)

    return urls


def get_classification_summary(cls: URLClassification) -> str:
    """Generate human-readable summary of classification."""
    lines = [
        f"Publication: {cls.name}",
        f"  ID: {cls.publication_id}",
        f"  Source: {cls.source}",
        f"  Frequency: {cls.frequency}",
        f"  Discovery: {cls.discovery_mode}",
    ]

    if cls.detected_periods:
        lines.append(f"  Periods: {len(cls.detected_periods)} ({cls.period_from} to {cls.period_to})")

    if cls.discovery_mode == 'template':
        lines.append(f"  URL pattern: {cls.url_pattern}")
    elif cls.discovery_mode == 'discover':
        lines.append("  Will scrape landing page for files")

    if cls.redirects_to_england:
        lines.append("  ⚠️  NHS Digital page with NHS England data files")

    return '\n'.join(lines)
