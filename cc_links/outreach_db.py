"""Isolated SQLite storage for outreach discovery evidence."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

SCHEMA = """
CREATE TABLE IF NOT EXISTS outreach_pages (
    url TEXT NOT NULL,
    crawl TEXT NOT NULL,
    url_path TEXT NOT NULL,
    registered_domain TEXT NOT NULL,
    tld TEXT,
    content_languages TEXT,
    fetch_time TEXT,
    pattern_id TEXT NOT NULL,
    pattern_language TEXT NOT NULL,
    matched_expression TEXT NOT NULL,
    matched_pattern_ids TEXT NOT NULL,
    pattern_weight INTEGER NOT NULL,
    path_specificity INTEGER NOT NULL,
    source_part TEXT NOT NULL,
    warc_filename TEXT,
    warc_record_offset INTEGER,
    warc_record_length INTEGER,
    discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (url, crawl)
);

CREATE INDEX IF NOT EXISTS idx_outreach_pages_domain
    ON outreach_pages(registered_domain);
CREATE INDEX IF NOT EXISTS idx_outreach_pages_tld
    ON outreach_pages(tld);
CREATE INDEX IF NOT EXISTS idx_outreach_pages_language
    ON outreach_pages(pattern_language);
CREATE INDEX IF NOT EXISTS idx_outreach_pages_pattern
    ON outreach_pages(pattern_id);
CREATE INDEX IF NOT EXISTS idx_outreach_pages_crawl
    ON outreach_pages(crawl);

CREATE TABLE IF NOT EXISTS outreach_prospects (
    registered_domain TEXT PRIMARY KEY,
    best_url TEXT NOT NULL,
    tld TEXT,
    language TEXT,
    best_pattern_id TEXT NOT NULL,
    best_pattern_weight INTEGER NOT NULL,
    best_path_specificity INTEGER NOT NULL,
    first_crawl TEXT NOT NULL,
    last_crawl TEXT NOT NULL,
    first_fetch_time TEXT,
    last_fetch_time TEXT,
    match_count INTEGER NOT NULL DEFAULT 1,
    discovery_score REAL NOT NULL,
    qualification_score REAL,
    status TEXT NOT NULL DEFAULT 'discovered'
        CHECK(status IN (
            'discovered', 'review', 'approved', 'rejected', 'unreachable'
        )),
    rejection_reason TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_outreach_prospects_tld
    ON outreach_prospects(tld);
CREATE INDEX IF NOT EXISTS idx_outreach_prospects_language
    ON outreach_prospects(language);
CREATE INDEX IF NOT EXISTS idx_outreach_prospects_pattern
    ON outreach_prospects(best_pattern_id);
CREATE INDEX IF NOT EXISTS idx_outreach_prospects_status
    ON outreach_prospects(status);
"""

_PAGE_COLUMNS = (
    "url",
    "crawl",
    "url_path",
    "registered_domain",
    "tld",
    "content_languages",
    "fetch_time",
    "pattern_id",
    "pattern_language",
    "matched_expression",
    "matched_pattern_ids",
    "pattern_weight",
    "path_specificity",
    "source_part",
    "warc_filename",
    "warc_record_offset",
    "warc_record_length",
)


def init_outreach_db(path: str | Path) -> sqlite3.Connection:
    """Open an outreach database and initialize its isolated schema."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(target))
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.executescript(SCHEMA)
    connection.commit()
    return connection


def _required_text(record: Mapping[str, Any], key: str) -> str:
    value = str(record.get(key, "")).strip()
    if not value:
        raise ValueError(f"outreach record is missing {key}")
    return value


def _normalized_page_values(record: Mapping[str, Any]) -> dict[str, Any]:
    values = dict(record)
    for key in (
        "url",
        "crawl",
        "url_path",
        "registered_domain",
        "pattern_id",
        "pattern_language",
        "matched_expression",
        "source_part",
    ):
        values[key] = _required_text(record, key)
    matched_ids = record.get("matched_pattern_ids", [])
    if isinstance(matched_ids, str):
        try:
            decoded = json.loads(matched_ids)
        except json.JSONDecodeError as exc:
            raise ValueError("matched_pattern_ids is not valid JSON") from exc
        matched_ids = decoded
    if not isinstance(matched_ids, list) or not all(
        isinstance(item, str) for item in matched_ids
    ):
        raise ValueError("matched_pattern_ids must be a list of strings")
    values["matched_pattern_ids"] = json.dumps(
        sorted(set(matched_ids)), ensure_ascii=False, separators=(",", ":")
    )
    values["pattern_weight"] = int(record.get("pattern_weight", 0))
    values["path_specificity"] = int(record.get("path_specificity", 0))
    if not 1 <= values["pattern_weight"] <= 100:
        raise ValueError("pattern_weight must be 1..100")
    if values["path_specificity"] < 1:
        raise ValueError("path_specificity must be positive")
    for key in (
        "tld",
        "content_languages",
        "fetch_time",
        "warc_filename",
        "warc_record_offset",
        "warc_record_length",
    ):
        values.setdefault(key, None)
    return values


def _refresh_prospect(connection: sqlite3.Connection, domain: str) -> None:
    best = connection.execute(
        """
        SELECT url, tld, pattern_language, pattern_id, pattern_weight,
               path_specificity
        FROM outreach_pages
        WHERE registered_domain = ?
        ORDER BY pattern_weight DESC,
                 path_specificity DESC,
                 COALESCE(fetch_time, '') DESC,
                 LENGTH(url),
                 url
        LIMIT 1
        """,
        (domain,),
    ).fetchone()
    aggregate = connection.execute(
        """
        SELECT MIN(crawl), MAX(crawl), MIN(fetch_time), MAX(fetch_time), COUNT(*)
        FROM outreach_pages
        WHERE registered_domain = ?
        """,
        (domain,),
    ).fetchone()
    (
        best_url,
        best_tld,
        best_language,
        best_pattern_id,
        best_weight,
        best_specificity,
    ) = best
    first_crawl, last_crawl, first_fetch, last_fetch, match_count = aggregate
    connection.execute(
        """
        INSERT INTO outreach_prospects (
            registered_domain, best_url, tld, language, best_pattern_id,
            best_pattern_weight, best_path_specificity, first_crawl, last_crawl,
            first_fetch_time, last_fetch_time, match_count, discovery_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(registered_domain) DO UPDATE SET
            best_url=excluded.best_url,
            tld=excluded.tld,
            language=excluded.language,
            best_pattern_id=excluded.best_pattern_id,
            best_pattern_weight=excluded.best_pattern_weight,
            best_path_specificity=excluded.best_path_specificity,
            first_crawl=excluded.first_crawl,
            last_crawl=excluded.last_crawl,
            first_fetch_time=excluded.first_fetch_time,
            last_fetch_time=excluded.last_fetch_time,
            match_count=excluded.match_count,
            discovery_score=excluded.discovery_score,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            domain,
            best_url,
            best_tld,
            best_language,
            best_pattern_id,
            best_weight,
            best_specificity,
            first_crawl,
            last_crawl,
            first_fetch,
            last_fetch,
            match_count,
            float(best_weight),
        ),
    )


def _insert_outreach_page(
    connection: sqlite3.Connection, record: Mapping[str, Any]
) -> tuple[bool, str]:
    values = _normalized_page_values(record)
    placeholders = ", ".join("?" for _ in _PAGE_COLUMNS)
    connection.execute(
        f"INSERT OR IGNORE INTO outreach_pages ({', '.join(_PAGE_COLUMNS)}) "
        f"VALUES ({placeholders})",
        tuple(values[column] for column in _PAGE_COLUMNS),
    )
    inserted = bool(connection.execute("SELECT changes()").fetchone()[0])
    return inserted, str(values["registered_domain"])


def save_outreach_page(
    connection: sqlite3.Connection, record: Mapping[str, Any]
) -> bool:
    """Insert one URL/crawl observation and update its domain-level prospect.

    Returns ``True`` only for a new URL/crawl observation. Replaying a part is
    idempotent and does not inflate ``match_count``.
    """
    inserted, domain = _insert_outreach_page(connection, record)
    if not inserted:
        return False
    _refresh_prospect(connection, domain)
    return True


def save_outreach_pages(
    connection: sqlite3.Connection,
    records: Iterable[Mapping[str, Any]],
    *,
    max_per_domain: int | None = None,
) -> int:
    """Save observations and optionally retain only each domain's best N rows."""
    if max_per_domain is not None and max_per_domain < 1:
        raise ValueError("max_per_domain must be at least one")
    inserted = 0
    affected: set[str] = set()
    with connection:
        for record in records:
            was_inserted, domain = _insert_outreach_page(connection, record)
            inserted += int(was_inserted)
            if was_inserted:
                affected.add(domain)
        if max_per_domain is not None:
            for domain in affected:
                connection.execute(
                    """
                    DELETE FROM outreach_pages
                    WHERE rowid IN (
                        SELECT rowid
                        FROM outreach_pages
                        WHERE registered_domain = ?
                        ORDER BY pattern_weight DESC,
                                 path_specificity DESC,
                                 COALESCE(fetch_time, '') DESC,
                                 LENGTH(url),
                                 url
                        LIMIT -1 OFFSET ?
                    )
                    """,
                    (domain, max_per_domain),
                )
        for domain in affected:
            _refresh_prospect(connection, domain)
    return inserted


def domain_page_counts(connection: sqlite3.Connection) -> dict[str, int]:
    """Return current evidence-row counts by registered domain."""
    return {
        str(domain): int(count)
        for domain, count in connection.execute(
            "SELECT registered_domain, COUNT(*) "
            "FROM outreach_pages GROUP BY registered_domain"
        )
    }


def iter_selected_pages(
    connection: sqlite3.Connection, max_per_domain: int = 2
) -> Iterator[dict[str, Any]]:
    """Yield the strongest deterministic URL evidence under a domain cap."""
    if max_per_domain < 1:
        raise ValueError("max_per_domain must be at least one")
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT *
        FROM (
            SELECT outreach_pages.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY registered_domain
                       ORDER BY pattern_weight DESC,
                                path_specificity DESC,
                                COALESCE(fetch_time, '') DESC,
                                LENGTH(url),
                                url
                   ) AS domain_rank
            FROM outreach_pages
        )
        WHERE domain_rank <= ?
        ORDER BY registered_domain, domain_rank
        """,
        (max_per_domain,),
    )
    for row in rows:
        yield dict(row)
