#!/usr/bin/env python3
"""Export URL + prospect score from an existing cc-links SQLite database."""
import argparse
import csv
import gzip
import sqlite3
from collections import Counter

from cc_links.prospects import load_prospect_rules

TARGET_CATEGORY_TO_FAMILY = {
    "Blog Comment": "blog_comment",
    "Forum": "forum",
    "Guestbook": "guestbook",
    "Directory": "directory",
    "Article": "article_submit",
    "Image Comment": "image_comment",
    "Trackback": "trackback",
    "Social Network": "social_network",
    "Microblog": "microblog",
}
PLATFORM_CATEGORIES = {"CMS", "E-commerce", "Site Builder"}


def build_url_terms(footprints=None):
    _, rules = load_prospect_rules(footprints)
    result = []
    for rule in rules:
        for term in rule.get("signals", {}).get("url_contains", []):
            term = term.lower()
            if term != "#respond":
                result.append((term, rule["family"]))
    return result


def score_url(url, engine_category, outlink_count, terms):
    url_lower = (url or "").lower()
    engine_family = TARGET_CATEGORY_TO_FAMILY.get(engine_category)
    matched_families = {family for term, family in terms if term in url_lower}

    if engine_family and engine_family in matched_families:
        score = 90
    elif engine_family:
        score = 70
    elif matched_families:
        score = 55
    elif engine_category in PLATFORM_CATEGORIES:
        score = 20
    else:
        score = 0

    links = outlink_count or 0
    if score:
        score += 10 if links >= 100 else 6 if links >= 50 else 3 if links >= 10 else 0
    return min(score, 100)


def main():
    p = argparse.ArgumentParser(description="Score URLs already stored in a cc-links database")
    p.add_argument("--db", required=True)
    p.add_argument("--out", required=True, help="CSV or CSV.GZ containing only url,score")
    p.add_argument("--footprints")
    p.add_argument("--min-score", type=int, default=0)
    args = p.parse_args()

    terms = build_url_terms(args.footprints)
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    rows = conn.execute("SELECT url, engine_category, outlink_count FROM pages")
    opener = gzip.open if args.out.lower().endswith(".gz") else open
    distribution = Counter()
    written = 0
    with opener(args.out, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["url", "score"])
        for url, category, outlinks in rows:
            score = score_url(url, category, outlinks, terms)
            distribution[(score // 10) * 10] += 1
            if score >= args.min_score:
                writer.writerow([url, score])
                written += 1
    conn.close()
    print(f"wrote {written} URLs -> {args.out}")
    print("score buckets: " + ", ".join(
        f"{bucket:02d}-{min(bucket + 9, 100):02d}={count}"
        for bucket, count in sorted(distribution.items())))


if __name__ == "__main__":
    main()
