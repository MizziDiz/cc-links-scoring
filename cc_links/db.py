"""SQLite storage layer -- stands in for the Athena table/queries."""
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS pages (
    url TEXT PRIMARY KEY,
    domain TEXT,
    crawl TEXT,
    timestamp TEXT,
    tld TEXT,
    country TEXT,
    bucket TEXT,
    engine_category TEXT,
    engine_name TEXT,
    outlink_count INTEGER,
    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url TEXT NOT NULL,
    target_url TEXT NOT NULL,
    target_domain TEXT,
    anchor_text TEXT,
    FOREIGN KEY (source_url) REFERENCES pages(url)
);

CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_url);
CREATE INDEX IF NOT EXISTS idx_links_target_domain ON links(target_domain);
CREATE INDEX IF NOT EXISTS idx_pages_engine ON pages(engine_category);
CREATE INDEX IF NOT EXISTS idx_pages_country ON pages(country);
CREATE INDEX IF NOT EXISTS idx_pages_bucket ON pages(bucket);
"""


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def insert_page(conn, url: str, domain: str, crawl: str, timestamp: str,
                 tld: str = None, country: str = None, bucket: str = None,
                 engine_category: str = None, engine_name: str = None,
                 outlink_count: int = None):
    conn.execute(
        """INSERT OR IGNORE INTO pages
           (url, domain, crawl, timestamp, tld, country, bucket, engine_category, engine_name, outlink_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (url, domain, crawl, timestamp, tld, country, bucket, engine_category, engine_name, outlink_count),
    )


def insert_links(conn, source_url: str, links):
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
