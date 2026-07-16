#!/usr/bin/env python3
"""Run prospect collection across recent Common Crawl snapshots until a target is met."""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
from urllib.request import urlopen

COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"


def candidate_count(db_path):
    if not os.path.exists(db_path):
        return 0
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()
        return row[0] if row else 0
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def load_crawls(limit):
    with urlopen(COLLINFO_URL, timeout=30) as response:
        data = json.load(response)
    return [item["id"] for item in data[:limit]]


def build_command(args, crawl, candidates_file):
    command = [
        sys.executable, os.path.join(os.path.dirname(__file__), "prospect_pipeline.py"),
        "--categories-file", args.categories_file,
        "--per-category-limit", str(args.per_category_limit),
        "--crawl", crawl,
        "--db", args.db,
        "--candidates-file", candidates_file,
        "--min-score", str(args.min_score),
        "--workers", str(args.workers),
        "--max-parts", str(args.max_parts),
        "--max-per-domain", str(args.max_per_domain),
        "--source", args.source,
        "--progress-interval", str(args.progress_interval),
    ]
    if args.footprints:
        command.extend(["--footprints", args.footprints])
    if args.exclude_file:
        command.extend(["--exclude-file", args.exclude_file])
    if args.proxy:
        command.extend(["--proxy", args.proxy])
    if args.proxy_file:
        command.extend(["--proxy-file", args.proxy_file])
    return command


def run(args):
    os.makedirs(args.state_dir, exist_ok=True)
    crawls = args.crawls or load_crawls(args.max_crawls)
    print(f"[multi] target={args.target_total}, current={candidate_count(args.db)}, "
          f"crawls={crawls}", flush=True)

    for crawl in crawls:
        current = candidate_count(args.db)
        if current >= args.target_total:
            print(f"[multi] target reached: {current} candidates", flush=True)
            return 0
        candidates_file = os.path.join(args.state_dir, f"{crawl}.jsonl")
        print(f"[multi] starting {crawl}: current={current}, "
              f"need={args.target_total - current}", flush=True)
        command = build_command(args, crawl, candidates_file)
        result = subprocess.run(command)
        if result.returncode:
            print(f"[multi] {crawl} failed with exit code {result.returncode}", flush=True)
            return result.returncode
        after = candidate_count(args.db)
        print(f"[multi] finished {crawl}: +{after - current}, total={after}", flush=True)

    final = candidate_count(args.db)
    print(f"[multi] crawl list exhausted: total={final}, target={args.target_total}", flush=True)
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Accumulate Common Crawl prospects across snapshots")
    parser.add_argument("--target-total", type=int, default=100000)
    parser.add_argument("--max-crawls", type=int, default=12)
    parser.add_argument("--crawls", nargs="+",
                        help="Explicit ordered crawl IDs; default is latest from collinfo.json")
    parser.add_argument("--state-dir", default="crawl_states")
    parser.add_argument("--categories-file", default="categories.json")
    parser.add_argument("--footprints")
    parser.add_argument("--per-category-limit", type=int, default=5000)
    parser.add_argument("--db", default="prospects.db")
    parser.add_argument("--min-score", type=int, default=50)
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--max-parts", type=int, default=300)
    parser.add_argument("--max-per-domain", type=int, default=10)
    parser.add_argument("--source", choices=["cloudfront", "s3"], default="s3")
    parser.add_argument("--progress-interval", type=float, default=60)
    parser.add_argument("--exclude-file")
    parser.add_argument("--proxy")
    parser.add_argument("--proxy-file")
    args = parser.parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
