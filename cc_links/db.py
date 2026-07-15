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

CREATE TABLE IF NOT EXISTS candidates (
    normalized_url TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    domain TEXT,
    registered_domain TEXT,
    crawl TEXT,
    tld TEXT,
    country TEXT,
    bucket TEXT,
    family TEXT NOT NULL,
    platform TEXT,
    score INTEGER NOT NULL,
    matched_signals TEXT NOT NULL,
    warc_filename TEXT,
    warc_offset INTEGER,
    warc_length INTEGER,
    source TEXT DEFAULT 'common_crawl',
    status TEXT DEFAULT 'archived_match',
    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_candidates_family ON candidates(family);
CREATE INDEX IF NOT EXISTS idx_candidates_country ON candidates(country);
CREATE INDEX IF NOT EXISTS idx_candidates_score ON candidates(score);

CREATE TABLE IF NOT EXISTS processed_urls (
    normalized_url TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    crawl TEXT,
    outcome TEXT NOT NULL,
    score INTEGER,
    processed_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_processed_urls_outcome ON processed_urls(outcome);
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


def upsert_candidate(conn, *, normalized_url, url, domain, registered_domain, crawl,
                     tld, country, bucket, family, platform, score, matched_signals,
                     warc_filename, warc_offset, warc_length):
    conn.execute(
        """INSERT INTO candidates
           (normalized_url, url, domain, registered_domain, crawl, tld, country, bucket,
            family, platform, score, matched_signals, warc_filename, warc_offset, warc_length)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(normalized_url) DO UPDATE SET
             last_seen=CURRENT_TIMESTAMP,
             score=MAX(candidates.score, excluded.score),
             family=CASE WHEN excluded.score >= candidates.score
                         THEN excluded.family ELSE candidates.family END,
             platform=CASE WHEN excluded.score >= candidates.score
                           THEN excluded.platform ELSE candidates.platform END,
             matched_signals=CASE WHEN excluded.score >= candidates.score
                                  THEN excluded.matched_signals ELSE candidates.matched_signals END""",
        (normalized_url, url, domain, registered_domain, crawl, tld, country, bucket,
         family, platform, score, matched_signals, warc_filename, warc_offset, warc_length),
    )


def mark_url_processed(conn, normalized_url, url, crawl, outcome, score=None):
    """Record a definitive fetch result so later runs never download it again.

    Transient fetch errors are intentionally not recorded by the caller.
    """
    conn.execute(
        """INSERT INTO processed_urls (normalized_url, url, crawl, outcome, score)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(normalized_url) DO UPDATE SET
             crawl=excluded.crawl, outcome=excluded.outcome, score=excluded.score,
             processed_at=CURRENT_TIMESTAMP""",
        (normalized_url, url, crawl, outcome, score),
    )


def _extract_domain(url: str) -> str:
    from urllib.parse import urlparse
    try:
        return urlparse(url).netloc.lower()
    except ValueError:
        return ""
