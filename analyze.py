"""Analyze collected links through the configured storage backend.

Usage:
  python analyze.py --db links.db --report top-domains
  python analyze.py --db links.db --report external-vs-internal
  python analyze.py --db links.db --sql "SELECT * FROM links LIMIT 10"
"""
import argparse
import logging
import os
from typing import Dict

from cc_links.logging_config import configure_logging
from cc_links.storage import QueryResult, create_storage

logger = logging.getLogger(__name__)

REPORTS: Dict[str, str] = {
    "top-domains": """
        SELECT target_domain, COUNT(*) AS link_count
        FROM links
        GROUP BY target_domain
        ORDER BY link_count DESC
        LIMIT 20;
    """,
    "top-pages-by-outlinks": """
        SELECT source_url, COUNT(*) AS outlink_count
        FROM links
        GROUP BY source_url
        ORDER BY outlink_count DESC
        LIMIT 20;
    """,
    "external-vs-internal": """
        SELECT
            p.domain AS source_domain,
            SUM(CASE WHEN l.target_domain = p.domain THEN 1 ELSE 0 END) AS internal_links,
            SUM(CASE WHEN l.target_domain != p.domain THEN 1 ELSE 0 END) AS external_links
        FROM links l
        JOIN pages p ON p.url = l.source_url
        GROUP BY p.domain;
    """,
    "summary": """
        SELECT
            (SELECT COUNT(*) FROM pages) AS pages_crawled,
            (SELECT COUNT(*) FROM links) AS total_links,
            (SELECT COUNT(DISTINCT target_domain) FROM links) AS unique_target_domains;
    """,
    "engine-distribution": """
        SELECT engine_category, COUNT(*) AS pages
        FROM pages
        WHERE engine_category IS NOT NULL
        GROUP BY engine_category
        ORDER BY pages DESC;
    """,
    "engine-detail": """
        SELECT engine_category, engine_name, COUNT(*) AS pages
        FROM pages
        WHERE engine_category IS NOT NULL
        GROUP BY engine_category, engine_name
        ORDER BY engine_category, pages DESC;
    """,
    "engine-by-country": """
        SELECT country, engine_category, COUNT(*) AS pages
        FROM pages
        WHERE engine_category IS NOT NULL AND country IS NOT NULL
        GROUP BY country, engine_category
        ORDER BY country, pages DESC;
    """,
    "country-coverage": """
        SELECT country, tld, COUNT(*) AS pages_crawled,
               SUM(CASE WHEN engine_category IS NOT NULL THEN 1 ELSE 0 END) AS classified_pages
        FROM pages
        WHERE country IS NOT NULL
        GROUP BY country, tld
        ORDER BY pages_crawled DESC;
    """,
    "bucket-coverage": """
        SELECT bucket,
               COUNT(*) AS pages,
               SUM(CASE WHEN engine_category IS NOT NULL THEN 1 ELSE 0 END) AS classified,
               ROUND(100.0 * SUM(CASE WHEN engine_category IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS unclassified_pct
        FROM pages
        WHERE bucket IS NOT NULL
        GROUP BY bucket
        ORDER BY pages DESC;
    """,
    "engine-by-bucket": """
        SELECT bucket, engine_category, COUNT(*) AS pages
        FROM pages
        WHERE engine_category IS NOT NULL AND bucket IS NOT NULL
        GROUP BY bucket, engine_category
        ORDER BY bucket, pages DESC;
    """,
    "engine-share-by-bucket": """
        SELECT bucket,
               engine_category,
               COUNT(*) AS pages,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY bucket), 1) AS pct_of_bucket
        FROM pages
        WHERE engine_category IS NOT NULL AND bucket IS NOT NULL
        GROUP BY bucket, engine_category
        ORDER BY bucket, pages DESC;
    """,
    "platform-share-by-bucket": """
        SELECT bucket,
               engine_name,
               COUNT(*) AS pages,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY bucket), 1) AS pct_of_bucket
        FROM pages
        WHERE engine_name IS NOT NULL AND bucket IS NOT NULL
        GROUP BY bucket, engine_name
        ORDER BY bucket, pages DESC;
    """,
    "engine-share-by-bucket-sites": """
        WITH sites AS (
            SELECT DISTINCT bucket, engine_category, domain
            FROM pages
            WHERE engine_category IS NOT NULL AND bucket IS NOT NULL
        )
        SELECT bucket,
               engine_category,
               COUNT(*) AS sites,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY bucket), 1) AS pct_of_bucket_sites
        FROM sites
        GROUP BY bucket, engine_category
        ORDER BY bucket, sites DESC;
    """,
    "platform-sites": """
        SELECT engine_category,
               engine_name,
               COUNT(DISTINCT domain) AS sites,
               COUNT(*) AS pages
        FROM pages
        WHERE engine_name IS NOT NULL
        GROUP BY engine_category, engine_name
        ORDER BY sites DESC;
    """,
    "unclassified-rate": """
        SELECT
            COUNT(*) AS total_pages,
            SUM(CASE WHEN engine_category IS NULL THEN 1 ELSE 0 END) AS unclassified,
            ROUND(100.0 * SUM(CASE WHEN engine_category IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS unclassified_pct
        FROM pages;
    """,
}

MYSQL_REPORT_OVERRIDES: Dict[str, str] = {
    "external-vs-internal": REPORTS["external-vs-internal"].replace(
        "p.url = l.source_url",
        "p.url_hash = l.source_url_hash",
    )
}


def print_table(result: QueryResult) -> None:
    header = " | ".join(result.columns)
    logger.info(header)
    logger.info("-" * len(header))
    for row in result.rows:
        logger.info(" | ".join(str(v) for v in row))
    logger.info("(%d rows)", len(result.rows))


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="Analyze the collected Common Crawl link data.")
    parser.add_argument("--db", default="links.db", help="SQLite path (ignored for MySQL)")
    parser.add_argument(
        "--db-backend",
        choices=["sqlite", "mysql"],
        default=os.getenv("DB_BACKEND", "sqlite"),
        help="Storage backend (default: DB_BACKEND or sqlite)",
    )
    parser.add_argument("--report", choices=list(REPORTS.keys()), help="Run a canned report")
    parser.add_argument("--sql", help="Run a custom SQL query instead")
    args = parser.parse_args()

    if not args.report and not args.sql:
        parser.error("Specify --report <name> or --sql \"<query>\"")

    storage = create_storage(args.db, args.db_backend)
    try:
        if args.sql:
            query = args.sql
        elif args.db_backend == "mysql":
            query = MYSQL_REPORT_OVERRIDES.get(args.report, REPORTS[args.report])
        else:
            query = REPORTS[args.report]
        print_table(storage.query(query))
    finally:
        storage.close()


if __name__ == "__main__":
    main()
