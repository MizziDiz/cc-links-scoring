"""Analyze collected links using local SQLite (replaces Athena SQL queries).

Usage:
  python analyze.py --db links.db --report top-domains
  python analyze.py --db links.db --report external-vs-internal
  python analyze.py --db links.db --sql "SELECT * FROM links LIMIT 10"
"""
import argparse
import sqlite3

REPORTS = {
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


def print_table(cursor):
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    print(" | ".join(cols))
    print("-" * (len(" | ".join(cols))))
    for row in rows:
        print(" | ".join(str(v) for v in row))
    print(f"\n({len(rows)} rows)")


def main():
    parser = argparse.ArgumentParser(description="Analyze the collected Common Crawl link data.")
    parser.add_argument("--db", default="links.db", help="SQLite database path")
    parser.add_argument("--report", choices=list(REPORTS.keys()), help="Run a canned report")
    parser.add_argument("--sql", help="Run a custom SQL query instead")
    args = parser.parse_args()

    if not args.report and not args.sql:
        parser.error("Specify --report <name> or --sql \"<query>\"")

    conn = sqlite3.connect(args.db)
    query = args.sql if args.sql else REPORTS[args.report]
    cursor = conn.execute(query)
    print_table(cursor)
    conn.close()


if __name__ == "__main__":
    main()
