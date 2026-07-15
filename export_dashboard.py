#!/usr/bin/env python3
"""Aggregate a run's latam.db into dashboard_data.json (small, ~10KB) for the
Platform Census dashboard. Run it on whichever machine holds the DB:

    python export_dashboard.py            # reads latam.db -> dashboard_data.json
    python export_dashboard.py my.db out.json
"""
import json
import sqlite3
import sys
from collections import defaultdict


def main():
    db = sys.argv[1] if len(sys.argv) > 1 else "latam.db"
    out = sys.argv[2] if len(sys.argv) > 2 else "dashboard_data.json"
    c = sqlite3.connect(db, timeout=60)

    tot = c.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    dom = c.execute("SELECT COUNT(DISTINCT domain) FROM pages").fetchone()[0]
    cl = c.execute("SELECT COUNT(*) FROM pages WHERE engine_category IS NOT NULL").fetchone()[0]

    cats = c.execute(
        "WITH s AS (SELECT DISTINCT engine_category, domain FROM pages "
        "WHERE engine_category IS NOT NULL) "
        "SELECT engine_category, COUNT(*) n FROM s GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    plats = c.execute(
        "SELECT engine_name, COUNT(DISTINCT domain) FROM pages "
        "WHERE engine_name IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 16"
    ).fetchall()

    dc = c.execute(
        "WITH pc AS (SELECT bucket, domain, engine_category, COUNT(*) c FROM pages "
        "WHERE engine_category IS NOT NULL GROUP BY 1,2,3), "
        "ranked AS (SELECT bucket, domain, engine_category, "
        "ROW_NUMBER() OVER (PARTITION BY bucket,domain ORDER BY c DESC) rn FROM pc) "
        "SELECT bucket, engine_category, COUNT(*) FROM ranked WHERE rn=1 GROUP BY 1,2"
    ).fetchall()
    bcat = defaultdict(dict)
    for b, cat, n in dc:
        bcat[b][cat] = n
    btot = {b: n for b, n in c.execute(
        "SELECT bucket, COUNT(DISTINCT domain) FROM pages WHERE bucket IS NOT NULL GROUP BY 1")}
    btop = defaultdict(list)
    for b, name, n in c.execute(
        "WITH s AS (SELECT DISTINCT bucket, engine_name, domain FROM pages "
        "WHERE engine_name IS NOT NULL) "
        "SELECT bucket, engine_name, COUNT(*) n FROM s GROUP BY 1,2 ORDER BY 1, n DESC"):
        if len(btop[b]) < 5:
            btop[b].append([name, n])

    buckets = [{"name": b, "domains": btot[b], "cats": bcat[b], "top": btop[b]}
               for b in sorted(btot, key=lambda x: -btot[x])]
    data = {"pages": tot, "domains": dom, "classified_pct": round(100 * cl / tot),
            "cats": [[k, v] for k, v in cats], "platforms": plats, "buckets": buckets}
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"wrote {out}: {tot} pages, {dom} domains, {len(buckets)} buckets")


if __name__ == "__main__":
    main()
