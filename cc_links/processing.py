"""CPU-bound WARC decompression, parsing, and classification."""

from typing import Any, Dict, List, Set, Tuple

from cc_links.engines import classify_engine
from cc_links.exclusions import is_excluded
from cc_links.fetch import (
    EXPECTED_FETCH_ERRORS,
    domain_of,
    extract_links_from_html,
    make_soup,
    parse_html_record,
)

Candidate = Dict[str, Any]
PageResult = Dict[str, Any]

_worker_excluded: Set[str] = set()
_worker_extract_links = False


def classify_warc_bytes(
    record: Candidate,
    raw_bytes: bytes,
    excluded: Set[str],
    extract_links: bool,
) -> PageResult:
    """Decompress and classify one fetched WARC range."""
    url = record["url"]
    try:
        html = parse_html_record(raw_bytes)
        if html is None:
            return {"url": url, "ok": False, "error": "no-html-record"}

        category, engine_name, _signal = classify_engine(html, url)
        links: List[Tuple[str, str]]
        if extract_links:
            soup = make_soup(html)
            links = extract_links_from_html(html, url, soup=soup)
            links = [
                (target, anchor)
                for target, anchor in links
                if not is_excluded(domain_of(target), excluded)
            ]
        else:
            links = []

        return {
            "url": url,
            "ok": True,
            "tld": record.get("url_host_tld"),
            "bucket": record.get("bucket"),
            "category": category,
            "engine_name": engine_name,
            "links": links,
        }
    except EXPECTED_FETCH_ERRORS as exc:
        return {"url": url, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def initialize_cpu_worker(excluded: Set[str], extract_links: bool) -> None:
    """Initialize immutable process-local classification settings."""
    global _worker_excluded, _worker_extract_links
    _worker_excluded = excluded
    _worker_extract_links = extract_links


def classify_in_cpu_worker(record: Candidate, raw_bytes: bytes) -> PageResult:
    """ProcessPool entry point; settings come from initialize_cpu_worker."""
    return classify_warc_bytes(
        record,
        raw_bytes,
        _worker_excluded,
        _worker_extract_links,
    )
