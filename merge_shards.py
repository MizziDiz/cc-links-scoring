#!/usr/bin/env python3
"""Merge shard databases (from parallel `--shard i/N` runs) into one.

    python merge_shards.py latam.db shard0.db shard1.db shard2.db shard3.db

The first argument is the target (created if missing); the rest are shard DBs.
Rows are copied with INSERT OR IGNORE so re-running is safe and any accidental
overlap between shards de-duplicates on the pages primary key (url).
"""
import sqlite3
import sys

from cc_links.db import init_db


def main():
    if len(sys.argv) < 3:
        sys.exit("usage: merge_shards.py <target.db> <shard0.db> [shard1.db ...]")
    target, shards = sys.argv[1], sys.argv[2:]
    conn = init_db(target)
    for s in shards:
        conn.execute("ATTACH DATABASE ? AS sh", (s,))
        n = conn.execute("SELECT COUNT(*) FROM sh.pages").fetchone()[0]
        conn.execute("INSERT OR IGNORE INTO pages SELECT * FROM sh.pages")
        try:
            conn.execute("INSERT INTO links (source_url, target_url, target_domain, anchor_text) "
                         "SELECT source_url, target_url, target_domain, anchor_text FROM sh.links")
        except sqlite3.OperationalError:
            pass  # --no-links run: no links table to merge
        conn.commit()
        conn.execute("DETACH DATABASE sh")
        print(f"  {s}: +{n} pages")
    total = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    print(f"merged -> {target}: {total} pages")
    conn.close()


if __name__ == "__main__":
    main()
