"""Scrape NHS landing pages for data files"""
import re
from dataclasses import dataclass
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from ..utils.period import parse_period, extract_period_from_url


@dataclass
class DiscoveredFile:
    """A file discovered from an NHS landing page"""
    url: str
    filename: str
    file_type: str  # xlsx, csv, xls, zip
    period: Optional[str]  # YYYY-MM or None
    title: Optional[str]  # Link text or nearby heading


# File extensions we care about
DATA_EXTENSIONS = {'.xlsx', '.xls', '.csv', '.zip'}


def scrape_landing_page(url: str, follow_links: bool = True) -> List[DiscoveredFile]:
    """
    Scrape NHS landing page for data files.

    Handles:
    - NHS Digital (hierarchical: main page -> sub-pages per period)
    - NHS England (flat: all files on one page)

    Args:
        url: Landing page URL
        follow_links: If True, follow links to sub-pages (for NHS Digital structure)

    Returns:
        List of discovered files with metadata
    """
    files = []
    visited: Set[str] = set()

    # Check if URL itself is a period page (e.g., /january-2026)
    # If so, files on this page should inherit that period
    page_period = extract_period_from_url(url)

    # Scrape main page (pass period if this IS a period page)
    main_files, sub_links = _scrape_page(url, inherit_period=page_period)
    files.extend(main_files)
    visited.add(url)

    # Follow sub-links if enabled (handles NHS Digital structure)
    if follow_links:
        for link in sub_links:
            if link not in visited:
                visited.add(link)
                # Extract period from the sub-page URL (e.g., /december-2025/)
                # Files on this page inherit this period if they don't have their own
                page_period = extract_period_from_url(link)
                sub_files, _ = _scrape_page(link, inherit_period=page_period)
                files.extend(sub_files)

    # Dedupe by URL
    seen_urls = set()
    unique_files = []
    for f in files:
        if f.url not in seen_urls:
            seen_urls.add(f.url)
            unique_files.append(f)

    return unique_files


def _scrape_page(url: str, inherit_period: Optional[str] = None) -> tuple[List[DiscoveredFile], List[str]]:
    """
    Scrape a single page for files and sub-links.

    Args:
        url: Page URL to scrape
        inherit_period: Period to assign to files that don't have their own
                       (used when scraping sub-pages like /december-2025/)

    Returns:
        Tuple of (discovered files, sub-page links to follow)
    """
    files = []
    sub_links = []

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return files, sub_links

    soup = BeautifulSoup(response.content, 'html.parser')
    base_url = url

    # Find all links
    for link in soup.find_all('a', href=True):
        href = link['href']
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        path = parsed.path.lower()

        # Check if it's a data file
        ext = _get_extension(path)
        if ext in DATA_EXTENSIONS:
            filename = path.split('/')[-1]
            title = _get_link_context(link)
            # Try filename first, then URL path, then link context, then inherit from page
            period = (parse_period(filename) or
                     extract_period_from_url(full_url) or
                     parse_period(title or '') or
                     inherit_period)

            files.append(DiscoveredFile(
                url=full_url,
                filename=filename,
                file_type=ext.lstrip('.'),
                period=period,
                title=title,
            ))

        # Check if it's a sub-page to follow (NHS Digital pattern)
        elif _is_subpage_link(full_url, url):
            sub_links.append(full_url)

    return files, sub_links


def _get_extension(path: str) -> str:
    """Extract file extension from path."""
    for ext in DATA_EXTENSIONS:
        if path.endswith(ext):
            return ext
    return ''


def _get_link_context(link) -> Optional[str]:
    """Get text context around a link (link text, parent heading, etc.)"""
    # Try link text first
    text = link.get_text(strip=True)
    if text and len(text) > 3:
        return text

    # Try title attribute
    title = link.get('title')
    if title:
        return title

    # Try parent elements for context
    for parent in link.parents:
        if parent.name in ['h1', 'h2', 'h3', 'h4', 'li', 'td']:
            text = parent.get_text(strip=True)
            if text:
                return text[:200]  # Truncate long text
        if parent.name == 'body':
            break

    return None


def _is_subpage_link(link_url: str, base_url: str) -> bool:
    """
    Check if a link is a sub-page we should follow.

    NHS Digital often has a main page with links to individual months/quarters.
    """
    # Must be same domain
    link_domain = urlparse(link_url).netloc
    base_domain = urlparse(base_url).netloc
    if link_domain != base_domain:
        return False

    # Must be under the same path prefix
    link_path = urlparse(link_url).path
    base_path = urlparse(base_url).path

    # Sub-pages typically extend the base path
    if not link_path.startswith(base_path.rstrip('/')):
        return False

    # Avoid following to completely different sections
    if '/resources/' in link_path or '/about/' in link_path:
        return False

    # Must have a period indicator (suggests it's a dated release)
    period = parse_period(link_path)
    if period:
        return True

    # Check for month/year keywords in path
    path_lower = link_path.lower()
    month_keywords = ['january', 'february', 'march', 'april', 'may', 'june',
                      'july', 'august', 'september', 'october', 'november', 'december']
    for kw in month_keywords:
        if kw in path_lower:
            return True

    return False
