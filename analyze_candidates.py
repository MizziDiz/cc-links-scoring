#!/usr/bin/env python3
"""Quality and yield reports for a Common Crawl prospect database."""
import argparse
import sqlite3

REPORTS = {
    "summary": """
        SELECT COUNT(*) candidates,
               COUNT(DISTINCT registered_domain) domains,
               ROUND(AVG(score), 1) avg_score,
               SUM(score >= 70) high_confidence
        FROM candidates
    """,
    "families": """
        SELECT family, COUNT(*) candidates, COUNT(DISTINCT registered_domain) domains,
               ROUND(AVG(score), 1) avg_score
        FROM candidates GROUP BY family ORDER BY candidates DESC
    """,
    "countries": """
        SELECT country, COUNT(*) candidates, COUNT(DISTINCT registered_domain) domains,
               ROUND(AVG(score), 1) avg_score
        FROM candidates GROUP BY country ORDER BY candidates DESC
    """,
    "platforms": """
        SELECT family, COALESCE(platform, 'Unknown') platform, COUNT(*) candidates,
               ROUND(AVG(score), 1) avg_score
        FROM candidates GROUP BY family, platform ORDER BY candidates DESC
    """,
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="prospects.db")
    p.add_argument("--report", choices=REPORTS, default="summary")
    args = p.parse_args()
    conn = sqlite3.connect(args.db)
    cur = conn.execute(REPORTS[args.report])
    headers = [d[0] for d in cur.description]
    print(" | ".join(headers))
    print("-" * len(" | ".join(headers)))
    for row in cur:
        print(" | ".join(str(v) for v in row))
    conn.close()


if __name__ == "__main__":
    main()
