#!/usr/bin/env python3
"""Create a stratified CSV sample for manual prospect-quality review."""
import argparse
import csv
import sqlite3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="prospects.db")
    parser.add_argument("--out", required=True)
    parser.add_argument("--per-family", type=int, default=50)
    parser.add_argument("--min-score", type=int, default=50)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    families = [row[0] for row in conn.execute(
        "SELECT DISTINCT family FROM candidates WHERE score >= ? ORDER BY family",
        (args.min_score,))]
    columns = ["url", "family", "platform", "score", "country", "registered_domain",
               "matched_signals", "verdict", "correct_family", "notes"]
    written = 0
    with open(args.out, "w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(columns)
        for family in families:
            rows = conn.execute(
                """SELECT url, family, platform, score, country, registered_domain,
                          matched_signals
                   FROM candidates
                   WHERE family = ? AND score >= ?
                   ORDER BY RANDOM() LIMIT ?""",
                (family, args.min_score, args.per_family))
            for row in rows:
                writer.writerow(list(row) + ["", "", ""])
                written += 1
    conn.close()
    print(f"wrote {written} labeled-review rows -> {args.out}")


if __name__ == "__main__":
    main()
