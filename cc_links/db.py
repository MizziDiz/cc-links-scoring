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
    discovery_tier INTEGER,
    pattern_id TEXT,
    prefetch_score INTEGER,
    matched_discovery TEXT,
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
    registered_domain TEXT,
    country TEXT,
    bucket TEXT,
    discovery_tier INTEGER,
    pattern_id TEXT,
    prefetch_score INTEGER,
    matched_discovery TEXT,
    final_family TEXT,
    final_platform TEXT,
    final_rule_id TEXT,
    matched_signals TEXT,
    processed_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_processed_urls_outcome ON processed_urls(outcome);

CREATE TABLE IF NOT EXISTS fetch_attempts (
    normalized_url TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    crawl TEXT,
    attempts INTEGER NOT NULL DEFAULT 1,
    last_error TEXT,
    last_attempt TEXT DEFAULT CURRENT_TIMESTAMP,
    resolved_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_fetch_attempts_resolved
ON fetch_attempts(resolved_at);
"""

ATTRIBUTION_COLUMNS = {
    "candidates": {
        "discovery_tier": "INTEGER",
        "pattern_id": "TEXT",
        "prefetch_score": "INTEGER",
        "matched_discovery": "TEXT",
    },
    "processed_urls": {
        "registered_domain": "TEXT",
        "country": "TEXT",
        "bucket": "TEXT",
        "discovery_tier": "INTEGER",
        "pattern_id": "TEXT",
        "prefetch_score": "INTEGER",
        "matched_discovery": "TEXT",
        "final_family": "TEXT",
        "final_platform": "TEXT",
        "final_rule_id": "TEXT",
        "matched_signals": "TEXT",
    },
}


def _ensure_columns(conn: sqlite3.Connection, table: str, columns) -> None:
    existing = {
        row[1] for row in conn.execute(f"PRAGMA table_info({table})")
    }
    for name, definition in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    for table, columns in ATTRIBUTION_COLUMNS.items():
        _ensure_columns(conn, table, columns)
    # Existing databases receive attribution columns via ALTER TABLE above, so
    # indexes that reference those columns must be created afterwards.
    conn.execute(
        """CREATE INDEX IF NOT EXISTS idx_processed_urls_pattern
           ON processed_urls(pattern_id)"""
    )
    conn.commit()
    return conn


def load_domain_priors(conn: sqlite3.Connection, minimum_score: int = 50):
    """Return confirmed-domain counts and confidence for prefetch prioritization."""
    return {
        domain: {"candidate_count": int(count), "max_score": int(max_score)}
        for domain, count, max_score in conn.execute(
            """
            SELECT registered_domain, COUNT(*), MAX(score)
            FROM candidates
            WHERE registered_domain IS NOT NULL
              AND registered_domain != ''
              AND score >= ?
            GROUP BY registered_domain
            """,
            (minimum_score,),
        )
    }


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
                     warc_filename, warc_offset, warc_length,
                     discovery_tier=None, pattern_id=None, prefetch_score=None,
                     matched_discovery=None):
    conn.execute(
        """INSERT INTO candidates
           (normalized_url, url, domain, registered_domain, crawl, tld, country, bucket,
            family, platform, score, matched_signals, warc_filename, warc_offset, warc_length,
            discovery_tier, pattern_id, prefetch_score, matched_discovery)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(normalized_url) DO UPDATE SET
             last_seen=CURRENT_TIMESTAMP,
             score=MAX(candidates.score, excluded.score),
             family=CASE WHEN excluded.score >= candidates.score
                         THEN excluded.family ELSE candidates.family END,
             platform=CASE WHEN excluded.score >= candidates.score
                           THEN excluded.platform ELSE candidates.platform END,
             matched_signals=CASE WHEN excluded.score >= candidates.score
                                  THEN excluded.matched_signals ELSE candidates.matched_signals END,
             discovery_tier=COALESCE(excluded.discovery_tier, candidates.discovery_tier),
             pattern_id=CASE
                          WHEN COALESCE(excluded.prefetch_score, -1)
                               >= COALESCE(candidates.prefetch_score, -1)
                          THEN excluded.pattern_id ELSE candidates.pattern_id END,
             prefetch_score=MAX(COALESCE(candidates.prefetch_score, 0),
                                COALESCE(excluded.prefetch_score, 0)),
             matched_discovery=COALESCE(excluded.matched_discovery,
                                        candidates.matched_discovery)""",
        (normalized_url, url, domain, registered_domain, crawl, tld, country, bucket,
         family, platform, score, matched_signals, warc_filename, warc_offset, warc_length,
         discovery_tier, pattern_id, prefetch_score, matched_discovery),
    )


def mark_url_processed(conn, normalized_url, url, crawl, outcome, score=None,
                       discovery_tier=None, pattern_id=None, prefetch_score=None,
                       matched_discovery=None, registered_domain=None,
                       country=None, bucket=None, final_family=None,
                       final_platform=None, final_rule_id=None,
                       matched_signals=None):
    """Record a definitive fetch result so later runs never download it again.

    Transient fetch errors are intentionally not recorded by the caller.
    """
    conn.execute(
        """INSERT INTO processed_urls
           (normalized_url, url, crawl, outcome, score, registered_domain,
            country, bucket, discovery_tier, pattern_id, prefetch_score,
            matched_discovery, final_family, final_platform, final_rule_id,
            matched_signals)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(normalized_url) DO UPDATE SET
             crawl=excluded.crawl, outcome=excluded.outcome, score=excluded.score,
             registered_domain=COALESCE(excluded.registered_domain,
                                        processed_urls.registered_domain),
             country=COALESCE(excluded.country, processed_urls.country),
             bucket=COALESCE(excluded.bucket, processed_urls.bucket),
             discovery_tier=COALESCE(excluded.discovery_tier,
                                     processed_urls.discovery_tier),
             pattern_id=COALESCE(excluded.pattern_id, processed_urls.pattern_id),
             prefetch_score=COALESCE(excluded.prefetch_score,
                                     processed_urls.prefetch_score),
             matched_discovery=COALESCE(excluded.matched_discovery,
                                        processed_urls.matched_discovery),
             final_family=COALESCE(excluded.final_family,
                                   processed_urls.final_family),
             final_platform=COALESCE(excluded.final_platform,
                                     processed_urls.final_platform),
             final_rule_id=COALESCE(excluded.final_rule_id,
                                    processed_urls.final_rule_id),
             matched_signals=COALESCE(excluded.matched_signals,
                                      processed_urls.matched_signals),
             processed_at=CURRENT_TIMESTAMP""",
        (normalized_url, url, crawl, outcome, score, registered_domain, country,
         bucket, discovery_tier, pattern_id, prefetch_score, matched_discovery,
         final_family, final_platform, final_rule_id, matched_signals),
    )
    conn.execute(
        """UPDATE fetch_attempts
           SET resolved_at=CURRENT_TIMESTAMP
           WHERE normalized_url = ?""",
        (normalized_url,),
    )


def record_fetch_error(conn, normalized_url, url, crawl, error):
    """Persist retryable failures without marking the URL as processed."""
    conn.execute(
        """INSERT INTO fetch_attempts
           (normalized_url, url, crawl, attempts, last_error, resolved_at)
           VALUES (?, ?, ?, 1, ?, NULL)
           ON CONFLICT(normalized_url) DO UPDATE SET
             crawl=excluded.crawl,
             attempts=fetch_attempts.attempts + 1,
             last_error=excluded.last_error,
             last_attempt=CURRENT_TIMESTAMP,
             resolved_at=NULL""",
        (normalized_url, url, crawl, error),
    )


def enforce_candidate_floor(conn, minimum_score):
    """Archive and remove historical candidates below the active score floor."""
    count = conn.execute(
        "SELECT COUNT(*) FROM candidates WHERE score < ?", (minimum_score,)
    ).fetchone()[0]
    if not count:
        return 0
    conn.execute(
        """INSERT OR REPLACE INTO processed_urls
           (normalized_url, url, crawl, outcome, score, registered_domain,
            country, bucket, discovery_tier, pattern_id, prefetch_score,
            matched_discovery, final_family, final_platform, matched_signals,
            processed_at)
           SELECT normalized_url, url, crawl, 'below_threshold', score,
                  registered_domain, country, bucket, discovery_tier, pattern_id,
                  prefetch_score, matched_discovery, family, platform,
                  matched_signals, CURRENT_TIMESTAMP
           FROM candidates WHERE score < ?""",
        (minimum_score,),
    )
    conn.execute("DELETE FROM candidates WHERE score < ?", (minimum_score,))
    conn.commit()
    return count


def enforce_domain_cap(conn, maximum_per_domain):
    """Keep the highest-scoring N candidates per registered domain."""
    if not maximum_per_domain:
        return 0
    ranked = """
        SELECT normalized_url FROM (
            SELECT normalized_url,
                   ROW_NUMBER() OVER (
                       PARTITION BY COALESCE(NULLIF(registered_domain, ''),
                                             NULLIF(domain, ''), normalized_url)
                       ORDER BY score DESC, normalized_url
                   ) AS rn
            FROM candidates
        ) WHERE rn > ?
    """
    urls = [row[0] for row in conn.execute(ranked, (maximum_per_domain,))]
    if not urls:
        return 0
    conn.executemany(
        """INSERT OR REPLACE INTO processed_urls
           (normalized_url, url, crawl, outcome, score, registered_domain,
            country, bucket, discovery_tier, pattern_id, prefetch_score,
            matched_discovery, final_family, final_platform, matched_signals,
            processed_at)
           SELECT normalized_url, url, crawl, 'domain_cap', score,
                  registered_domain, country, bucket, discovery_tier, pattern_id,
                  prefetch_score, matched_discovery, family, platform,
                  matched_signals, CURRENT_TIMESTAMP
           FROM candidates WHERE normalized_url = ?""",
        [(url,) for url in urls],
    )
    conn.executemany("DELETE FROM candidates WHERE normalized_url = ?",
                     [(url,) for url in urls])
    conn.commit()
    return len(urls)


def _extract_domain(url: str) -> str:
    from urllib.parse import urlparse
    try:
        return urlparse(url).netloc.lower()
    except ValueError:
        return ""
