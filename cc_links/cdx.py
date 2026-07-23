"""Query the Common Crawl CDX index (replaces the need for Athena's index queries)."""
import json
import logging
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Union

import requests

CDX_URL = "https://index.commoncrawl.org/{crawl}-index"
logger = logging.getLogger(__name__)


def get_cdx_records(
    url_pattern: str,
    crawl: str,
    limit: int = 1000,
    filters: Optional[Union[str, Sequence[str]]] = None,
) -> List[Dict[str, Any]]:
    """Fetch CDX records for a URL/domain pattern from a given crawl (e.g. CC-MAIN-2024-33).

    url_pattern: e.g. "example.com/*" for all pages on a domain, or "example.com" for exact URL.
    Returns a list of dicts with keys: urlkey, timestamp, url, mime, status, digest, length, offset, filename.
    """
    params: Dict[str, Any] = {
        "url": url_pattern,
        "output": "json",
        "limit": limit,
    }
    if filters:
        params["filter"] = filters

    resp = requests.get(CDX_URL.format(crawl=crawl), params=params, timeout=30)
    resp.raise_for_status()

    records: List[Dict[str, Any]] = []
    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(_parse_json_line(line))
        except ValueError:
            continue
    return records


def _parse_json_line(line: str) -> Dict[str, Any]:
    return json.loads(line)


def iter_cdx_records(
    domains: Iterable[str],
    crawl: str,
    limit_per_domain: int = 1000,
    delay: float = 0.5,
) -> Iterator[Dict[str, Any]]:
    """Yield CDX records across multiple domains, with a small delay to be polite to the API."""
    for domain in domains:
        pattern = domain if "*" in domain else f"{domain}/*"
        try:
            records = get_cdx_records(pattern, crawl, limit=limit_per_domain)
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
        ) as exc:
            logger.warning("[cdx] failed for %s: %s", domain, exc)
            continue
        for r in records:
            yield r
        time.sleep(delay)
