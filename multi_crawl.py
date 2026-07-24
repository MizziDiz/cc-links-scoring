#!/usr/bin/env python3
"""Run prospect collection across recent Common Crawl snapshots until a target is met."""
import argparse
import glob
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen

from cc_links.feedback import priority_profile_hash
from cc_links.prospects import discovery_ruleset_identity, normalize_url

COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
DISCOVERY_MARKER_SUFFIX = ".discovery-complete"
SHARD_STATE_RE = re.compile(r"\.shard-(\d+)-of-(\d+)\.jsonl\.state\.json$")


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


def discovery_state_complete(candidates_file, expected_parts):
    """Return whether a resumable discovery checkpoint has finished its work."""
    state_path = candidates_file + ".state.json"
    if not os.path.exists(state_path):
        return False
    try:
        with open(state_path, encoding="utf-8") as source:
            state = json.load(source)
    except (OSError, ValueError, TypeError):
        return False

    remaining = state.get("remaining", {})
    if remaining and all(value <= 0 for value in remaining.values()):
        return True
    scanned = set(state.get("scanned_parts", []))
    required = state.get("allowed_parts_count", expected_parts)
    return bool(required) and len(scanned) >= required


def discovery_marker(candidates_file):
    return candidates_file + DISCOVERY_MARKER_SUFFIX


def read_discovery_marker(candidates_file):
    marker = discovery_marker(candidates_file)
    if not os.path.exists(marker):
        return None
    try:
        with open(marker, encoding="utf-8") as source:
            payload = json.load(source)
        return payload.get("identity", {})
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        # Legacy markers contained only the word "complete".
        return {}


def mark_discovery_complete(candidates_file, identity=None):
    marker = discovery_marker(candidates_file)
    temporary = marker + ".tmp"
    with open(temporary, "w", encoding="utf-8") as output:
        if identity:
            json.dump({"identity": identity}, output, sort_keys=True)
            output.write("\n")
        else:
            output.write("complete\n")
    os.replace(temporary, marker)


def run_identity(args, crawl):
    return discovery_ruleset_identity(
        crawl, args.footprints, args.discovery_profile,
        args.broad_index_sample,
        priority_profile_hash(getattr(args, "pattern_priorities", None)),
    )


def resolve_discovery_shards(requested, cpu_count=None):
    """Resolve 0 (auto) to a safe shard count for the available CPUs."""
    if requested > 0:
        return requested
    available = cpu_count if cpu_count is not None else os.cpu_count()
    return max(1, min(4, available or 1))


def existing_shard_count(state_dir, crawl):
    """Find an interrupted sharded layout so upgrades resume it unchanged."""
    prefix = os.path.join(state_dir, f"{crawl}.shard-")
    layouts = {}
    for path in glob.glob(prefix + "*-of-*.jsonl.state.json"):
        match = SHARD_STATE_RE.search(path)
        if not match:
            continue
        index, count = (int(value) for value in match.groups())
        if 0 <= index < count:
            layouts.setdefault(count, set()).add(index)
    if not layouts:
        return None
    # Prefer the layout with the most surviving checkpoints, then the larger
    # shard count. Missing shard files are recreated by run_parallel_discovery.
    return max(layouts, key=lambda count: (len(layouts[count]), count))


def build_command(args, crawl, candidates_file, *, per_category_limit=None,
                  discovery_only=False, skip_discovery=False, part_shard=None,
                  category_limit_divisor=1):
    command = [
        sys.executable, os.path.join(os.path.dirname(__file__), "prospect_pipeline.py"),
        "--categories-file", args.categories_file,
        "--discovery-profile", args.discovery_profile,
        "--broad-quota-fraction", str(args.broad_quota_fraction),
        "--broad-index-sample", str(args.broad_index_sample),
        "--per-category-limit", str(per_category_limit or args.per_category_limit),
        "--crawl", crawl,
        "--db", args.db,
        "--candidates-file", candidates_file,
        "--min-score", str(args.min_score),
        "--workers", str(args.workers),
        "--max-parts", str(args.max_parts),
        "--max-per-domain", str(args.max_per_domain),
        "--source", args.source,
        "--index-source", args.index_source,
        "--progress-interval", str(args.progress_interval),
    ]
    if args.footprints:
        command.extend(["--footprints", args.footprints])
    if getattr(args, "pattern_priorities", None):
        command.extend(["--pattern-priorities", args.pattern_priorities])
    if args.category_limits:
        command.extend(["--category-limits", args.category_limits])
    if getattr(args, "discovery_metrics", False):
        command.append("--discovery-metrics")
    if category_limit_divisor > 1:
        command.extend(["--category-limit-divisor", str(category_limit_divisor)])
    if args.exclude_file:
        command.extend(["--exclude-file", args.exclude_file])
    if args.proxy:
        command.extend(["--proxy", args.proxy])
    if args.proxy_file:
        command.extend(["--proxy-file", args.proxy_file])
    if discovery_only:
        command.append("--discovery-only")
    if skip_discovery:
        command.append("--skip-discovery")
    if part_shard:
        command.extend(["--part-shard", part_shard])
    return command


def merge_candidate_files(paths, output_path):
    """Merge shard JSONLs, de-duplicating normalized URLs."""
    temporary = output_path + ".tmp"
    seen = set()
    written = 0
    with open(temporary, "w", encoding="utf-8") as output:
        for path in paths:
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as source:
                for line in source:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    normalized = normalize_url(record["url"])
                    if normalized in seen:
                        continue
                    seen.add(normalized)
                    output.write(json.dumps(record, ensure_ascii=False) + "\n")
                    written += 1
    os.replace(temporary, output_path)
    return written


def run_parallel_discovery(args, crawl, combined_file, shard_count=None):
    shard_count = shard_count or args.discovery_shards
    per_shard_limit = max(1, int(math.ceil(args.per_category_limit / shard_count)))
    shard_files = [
        os.path.join(args.state_dir, f"{crawl}.shard-{i}-of-{shard_count}.jsonl")
        for i in range(shard_count)
    ]

    def run_shard(index):
        command = build_command(
            args, crawl, shard_files[index], per_category_limit=per_shard_limit,
            discovery_only=True, part_shard=f"{index}/{shard_count}",
            category_limit_divisor=(shard_count if args.category_limits else 1))
        return subprocess.run(command).returncode

    print(f"[multi] parallel discovery {crawl}: shards={shard_count}, "
          f"per_shard_category_limit={per_shard_limit}", flush=True)
    with ThreadPoolExecutor(max_workers=shard_count) as pool:
        codes = list(pool.map(run_shard, range(shard_count)))
    if any(codes):
        return next(code for code in codes if code)
    merged = merge_candidate_files(shard_files, combined_file)
    print(f"[multi] merged {merged} unique candidates for {crawl}", flush=True)
    if all(discovery_state_complete(path, args.max_parts) for path in shard_files):
        mark_discovery_complete(combined_file, run_identity(args, crawl))
    else:
        print(f"[multi] {crawl} has retryable unscanned parts; completion marker "
              "not written", flush=True)
    return 0


def run(args):
    os.makedirs(args.state_dir, exist_ok=True)
    requested_shards = args.discovery_shards
    args.discovery_shards = resolve_discovery_shards(requested_shards)
    if requested_shards == 0:
        print(f"[multi] auto discovery shards={args.discovery_shards} "
              f"for cpu_count={os.cpu_count() or 1}", flush=True)
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
        identity = run_identity(args, crawl)
        marker_identity = read_discovery_marker(candidates_file)
        marker_exists = marker_identity is not None
        if marker_identity and marker_identity != identity:
            print(
                f"[multi] checkpoint ruleset mismatch for {crawl}; "
                "use the original taxonomy/profile or a new --state-dir "
                "for an explicit backfill",
                flush=True,
            )
            return 2
        combined_state_exists = os.path.exists(candidates_file + ".state.json")
        combined_complete = discovery_state_complete(candidates_file, args.max_parts)

        if marker_exists or combined_complete:
            if combined_complete and not marker_exists:
                with open(candidates_file + ".state.json", encoding="utf-8") as source:
                    combined_state = json.load(source)
                state_identity = combined_state.get("checkpoint_identity")
                mark_discovery_complete(
                    candidates_file, identity if state_identity else None)
            print(f"[multi] discovery already complete for {crawl}; "
                  f"reusing {candidates_file}", flush=True)
            code = subprocess.run(build_command(
                args, crawl, candidates_file, skip_discovery=True)).returncode
        elif combined_state_exists:
            # A crawl started by an older sequential release must resume against
            # its original checkpoint. Starting fresh shards here would rescan
            # completed Parquet parts and could discard candidates already written.
            print(f"[multi] resuming legacy sequential discovery for {crawl}",
                  flush=True)
            code = subprocess.run(build_command(args, crawl, candidates_file)).returncode
            if not code and discovery_state_complete(candidates_file, args.max_parts):
                mark_discovery_complete(candidates_file, identity)
        elif args.discovery_shards > 1 or existing_shard_count(args.state_dir, crawl):
            resume_shards = existing_shard_count(args.state_dir, crawl)
            if resume_shards and resume_shards != args.discovery_shards:
                print(f"[multi] resuming existing {resume_shards}-shard layout for "
                      f"{crawl}; configured future shard count is "
                      f"{args.discovery_shards}", flush=True)
            code = run_parallel_discovery(
                args, crawl, candidates_file,
                shard_count=resume_shards or args.discovery_shards)
            if not code:
                code = subprocess.run(build_command(
                    args, crawl, candidates_file, skip_discovery=True)).returncode
        else:
            code = subprocess.run(build_command(args, crawl, candidates_file)).returncode
        if code:
            print(f"[multi] {crawl} failed with exit code {code}", flush=True)
            return code
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
    parser.add_argument("--category-limits")
    parser.add_argument("--discovery-profile", choices=["precise", "broad"],
                        default="precise")
    parser.add_argument("--broad-quota-fraction", type=float, default=0.25,
                        help="Maximum share of each category filled by broad-only URLs")
    parser.add_argument("--broad-index-sample", type=float, default=0.02,
                        help="Deterministic share of broad-only index matches to rank/fetch")
    parser.add_argument("--footprints")
    parser.add_argument(
        "--pattern-priorities",
        help="Optional JSON generated by feedback_report.py; affects fetch order only")
    parser.add_argument("--per-category-limit", type=int, default=5000)
    parser.add_argument("--db", default="prospects.db")
    parser.add_argument("--min-score", type=int, default=50)
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--max-parts", type=int, default=300)
    parser.add_argument("--max-per-domain", type=int, default=10)
    parser.add_argument("--discovery-shards", type=int, default=0,
                        help="Parallel non-overlapping Parquet discovery workers; "
                             "0 selects min(CPUs, 4)")
    parser.add_argument("--source", choices=["cloudfront", "s3"], default="s3")
    parser.add_argument(
        "--index-source", choices=["auto", "https", "s3"], default="auto",
        help="Where DuckDB reads the Parquet index; auto uses S3 with --source s3")
    parser.add_argument("--progress-interval", type=float, default=60)
    parser.add_argument(
        "--discovery-metrics", action="store_true",
        help="Persist optional Parquet row counts and discovery-funnel counters")
    parser.add_argument("--exclude-file")
    parser.add_argument("--proxy")
    parser.add_argument("--proxy-file")
    args = parser.parse_args()
    if not 0.0 <= args.broad_quota_fraction <= 1.0:
        parser.error("--broad-quota-fraction must be between 0 and 1")
    if not 0.0 <= args.broad_index_sample <= 1.0:
        parser.error("--broad-index-sample must be between 0 and 1")
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
