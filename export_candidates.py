#!/usr/bin/env python3
"""Export filtered Common Crawl prospects as TXT, CSV or JSONL."""
import argparse
import csv
import json
import sqlite3


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="prospects.db")
    p.add_argument("--out", required=True)
    p.add_argument("--format", choices=["txt", "csv", "jsonl"], default="txt")
    p.add_argument("--family")
    p.add_argument("--country")
    p.add_argument("--platform")
    p.add_argument("--min-score", type=int, default=50)
    args = p.parse_args()

    where = ["score >= ?"]
    values = [args.min_score]
    for column in ("family", "country", "platform"):
        value = getattr(args, column)
        if value:
            where.append(f"{column} = ?")
            values.append(value)
    columns = ["url", "domain", "registered_domain", "country", "tld", "bucket",
               "family", "platform", "score", "matched_signals", "crawl", "last_seen"]
    sql = f"SELECT {', '.join(columns)} FROM candidates WHERE {' AND '.join(where)} ORDER BY score DESC, url"
    conn = sqlite3.connect(args.db)
    rows = conn.execute(sql, values)
    with open(args.out, "w", encoding="utf-8", newline="") as out:
        if args.format == "txt":
            for row in rows:
                out.write(row[0] + "\n")
        elif args.format == "csv":
            writer = csv.writer(out)
            writer.writerow(columns)
            writer.writerows(rows)
        else:
            for row in rows:
                out.write(json.dumps(dict(zip(columns, row)), ensure_ascii=False) + "\n")
    conn.close()


if __name__ == "__main__":
    main()
