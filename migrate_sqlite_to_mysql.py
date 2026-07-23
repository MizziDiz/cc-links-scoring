#!/usr/bin/env python3
"""Copy the pages table from an existing SQLite database into MySQL.

The migration is idempotent: MySQLStorage performs a no-op on duplicate
SHA-256 URL hashes, so an interrupted pages migration can be rerun safely.
MYSQL_DSN must be supplied through the environment.
"""

import argparse
import logging
import sqlite3
from typing import Iterable, List, Optional, Sequence, Tuple

from cc_links.logging_config import configure_logging
from cc_links.storage import PageRecord, Storage, create_storage

logger = logging.getLogger(__name__)

PAGE_COLUMNS: Tuple[str, ...] = (
    "url",
    "domain",
    "crawl",
    "timestamp",
    "tld",
    "country",
    "bucket",
    "engine_category",
    "engine_name",
    "outlink_count",
    "fetched_at",
)


def rows_to_pages(rows: Iterable[Sequence[object]]) -> List[PageRecord]:
    """Convert SQLite result rows to backend-neutral page records."""
    return [
        PageRecord(
            url=str(row[0]),
            domain=str(row[1] or ""),
            crawl=str(row[2] or ""),
            timestamp=str(row[3] or ""),
            tld=_optional_str(row[4]),
            country=_optional_str(row[5]),
            bucket=_optional_str(row[6]),
            engine_category=_optional_str(row[7]),
            engine_name=_optional_str(row[8]),
            outlink_count=int(str(row[9])) if row[9] is not None else None,
            fetched_at=_optional_str(row[10]),
        )
        for row in rows
    ]


def migrate_pages(source: sqlite3.Connection, target: Storage, batch_size: int) -> int:
    """Stream pages from SQLite to MySQL in resumable batches."""
    available = {
        str(row[1])
        for row in source.execute("PRAGMA table_info(pages)").fetchall()
    }
    missing = set(PAGE_COLUMNS) - available
    if missing:
        raise ValueError(f"SQLite pages table is missing columns: {sorted(missing)}")

    cursor = source.execute(f"SELECT {', '.join(PAGE_COLUMNS)} FROM pages")
    migrated = 0
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        pages = rows_to_pages(rows)
        target.save_pages(pages)
        target.commit()
        migrated += len(pages)
        logger.info("migrated %d pages", migrated)
    return migrated


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="Migrate SQLite pages to MYSQL_DSN")
    parser.add_argument("--sqlite", default="latam.db", help="Source SQLite database")
    parser.add_argument("--batch-size", type=int, default=2000)
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")

    source = sqlite3.connect(args.sqlite)
    target: Optional[Storage] = None
    try:
        target = create_storage(args.sqlite, backend="mysql")
        source_count = int(source.execute("SELECT COUNT(*) FROM pages").fetchone()[0])
        attempted = migrate_pages(source, target, args.batch_size)
        target_count = int(target.query("SELECT COUNT(*) FROM pages").rows[0][0])
        logger.info(
            "migration complete: source=%d attempted=%d mysql_total=%d",
            source_count,
            attempted,
            target_count,
        )
    finally:
        source.close()
        if target is not None:
            target.close()


def _optional_str(value: object) -> Optional[str]:
    return None if value is None else str(value)


if __name__ == "__main__":
    main()
