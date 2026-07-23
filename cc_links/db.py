"""Compatibility helpers for scripts that explicitly operate on SQLite files."""
import sqlite3
from typing import Iterable, Optional, Tuple

from cc_links.storage import SQLITE_SCHEMA

SCHEMA = SQLITE_SCHEMA


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def insert_page(
    conn: sqlite3.Connection,
    url: str,
    domain: str,
    crawl: str,
    timestamp: str,
    tld: Optional[str] = None,
    country: Optional[str] = None,
    bucket: Optional[str] = None,
    engine_category: Optional[str] = None,
    engine_name: Optional[str] = None,
    outlink_count: Optional[int] = None,
) -> None:
    conn.execute(
        """INSERT OR IGNORE INTO pages
           (url, domain, crawl, timestamp, tld, country, bucket, engine_category, engine_name, outlink_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (url, domain, crawl, timestamp, tld, country, bucket, engine_category, engine_name, outlink_count),
    )


def insert_links(
    conn: sqlite3.Connection,
    source_url: str,
    links: Iterable[Tuple[str, str]],
) -> None:
    conn.executemany(
        "INSERT INTO links (source_url, target_url, target_domain, anchor_text) VALUES (?, ?, ?, ?)",
        [(source_url, target, _extract_domain(target), anchor) for target, anchor in links],
    )


def _extract_domain(url: str) -> str:
    from urllib.parse import urlparse
    try:
        return urlparse(url).netloc.lower()
    except ValueError:
        return ""
