#!/usr/bin/env python3
"""Build a scored link-prospect database from Common Crawl only."""
import argparse
import json
import sys
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from tqdm import tqdm

from cc_links import fetch as fetch_mod
from cc_links.cc_index import discover_by_countries, load_candidates_shuffled, load_proxies
from cc_links.countries import country_name, load_category_map
from cc_links.db import init_db, mark_url_processed, upsert_candidate
from cc_links.exclusions import is_excluded, load_excluded_domains
from cc_links.fetch import domain_of, fetch_warc_record, parse_html_record
from cc_links.prospects import (classify_prospect, discovery_url_terms,
                                normalize_url)


def fetch_and_classify(record, footprints, minimum_score):
    try:
        raw = fetch_warc_record(record["filename"], record["offset"], record["length"])
        html = parse_html_record(raw)
        if html is None:
            return {"ok": False, "error": "no-html-record"}
        matches = classify_prospect(html, record["url"], footprints, minimum_score)
        return {"ok": True, "record": record, "matches": matches}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def run(args):
    excluded = load_excluded_domains(args.exclude_file)
    categories, tld_to_category = load_category_map(args.categories_file)
    budgets = {name: args.per_category_limit for name in categories}
    candidates_file = args.candidates_file or args.db + ".prospects.jsonl"
    terms = discovery_url_terms(args.footprints)
    print(f"[footprints] {len(terms)} selective URL terms used for index discovery")

    discovery_proxies = None
    fetch_mod.rate_limiter.set_rate(args.rate_limit)
    if args.source == "s3":
        fetch_mod.enable_s3(pool_size=max(args.workers * 2, 64))
    elif args.proxy_file:
        fetch_mod.load_proxy_file(args.proxy_file)
        discovery_proxies = load_proxies(args.proxy_file)
    elif args.proxy:
        fetch_mod.set_proxy(args.proxy)

    if not args.skip_discovery:
        discover_by_countries(
            args.crawl, budgets, tld_to_category,
            lambda d: is_excluded(d, excluded), candidates_file,
            max_parts=args.max_parts, max_per_domain=args.max_per_domain,
            progress=lambda m: print(f"[discover] {m}"), proxies=discovery_proxies,
            part_delay=args.discover_delay, url_terms=terms,
        )
    else:
        print(f"[discover] skipped; using {candidates_file}")

    if args.discovery_only:
        return

    conn = init_db(args.db)
    existing = {r[0] for r in conn.execute("SELECT normalized_url FROM candidates")}
    existing.update(r[0] for r in conn.execute("SELECT normalized_url FROM processed_urls"))
    print(f"[resume] {len(existing)} URLs already processed; they will not be fetched again")

    scheduled = set(existing)

    def records():
        for rec in load_candidates_shuffled(candidates_file):
            normalized = normalize_url(rec["url"])
            if normalized not in scheduled:
                scheduled.add(normalized)
                yield rec

    # Count without consuming the de-duplicating iterator used by the workers.
    count_seen = set(existing)
    total = 0
    for rec in load_candidates_shuffled(candidates_file):
        normalized = normalize_url(rec["url"])
        if normalized not in count_seen:
            count_seen.add(normalized)
            total += 1
    pending = set()
    iterator = records()
    stats = Counter()
    processed = 0
    started_at = time.monotonic()
    last_report = started_at

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        def fill():
            for rec in iterator:
                pending.add(pool.submit(fetch_and_classify, rec, args.footprints, args.min_score))
                if len(pending) >= args.workers * 4:
                    break

        fill()
        with tqdm(total=total) as progress:
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    result = future.result()
                    processed += 1
                    progress.update(1)
                    if not result["ok"]:
                        stats["fetch_error"] += 1
                        continue
                    rec = result["record"]
                    normalized = normalize_url(rec["url"])
                    if not result["matches"]:
                        stats["unmatched"] += 1
                        mark_url_processed(conn, normalized, rec["url"], args.crawl, "unmatched")
                        continue
                    # One URL may legitimately match multiple families. Store the
                    # best match in candidates; preserve every signal in JSON.
                    best = result["matches"][0]
                    all_matches = [m.to_dict() for m in result["matches"]]
                    upsert_candidate(
                        conn, normalized_url=normalized, url=rec["url"],
                        domain=domain_of(rec["url"]),
                        registered_domain=rec.get("url_host_registered_domain"),
                        crawl=args.crawl, tld=rec.get("url_host_tld"),
                        country=country_name(rec.get("url_host_tld")), bucket=rec.get("bucket"),
                        family=best.family, platform=best.platform, score=best.score,
                        matched_signals=json.dumps(all_matches, ensure_ascii=False),
                        warc_filename=rec.get("filename"), warc_offset=rec.get("offset"),
                        warc_length=rec.get("length"),
                    )
                    stats[f"family:{best.family}"] += 1
                    stats["stored"] += 1
                    mark_url_processed(conn, normalized, rec["url"], args.crawl,
                                       "stored", best.score)
                    if processed % args.commit_every == 0:
                        conn.commit()
                    now = time.monotonic()
                    if now - last_report >= args.progress_interval:
                        elapsed = max(now - started_at, 0.001)
                        print(f"[progress] processed={processed}/{total} "
                              f"stored={stats['stored']} unmatched={stats['unmatched']} "
                              f"errors={stats['fetch_error']} rate={processed / elapsed:.1f}/s",
                              flush=True)
                        last_report = now
                fill()
    conn.commit()
    conn.close()
    print("[result] " + ", ".join(f"{k}={v}" for k, v in stats.most_common()))


def main():
    parser = argparse.ArgumentParser(description="Collect scored link prospects from Common Crawl")
    parser.add_argument("--categories-file", default="categories.json")
    parser.add_argument("--footprints", default=None)
    parser.add_argument("--per-category-limit", type=int, default=10000)
    parser.add_argument("--crawl", default="CC-MAIN-2026-25")
    parser.add_argument("--db", default="prospects.db")
    parser.add_argument("--candidates-file")
    parser.add_argument("--min-score", type=int, default=50)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--rate-limit", type=float, default=15)
    parser.add_argument("--max-parts", type=int)
    parser.add_argument("--max-per-domain", type=int, default=10)
    parser.add_argument("--discover-delay", type=float, default=0.0)
    parser.add_argument("--source", choices=["cloudfront", "s3"], default="cloudfront")
    parser.add_argument("--proxy")
    parser.add_argument("--proxy-file")
    parser.add_argument("--exclude-file")
    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument("--discovery-only", action="store_true")
    parser.add_argument("--commit-every", type=int, default=200)
    parser.add_argument("--progress-interval", type=float, default=60,
                        help="Emit one plain progress line every N seconds (systemd/journal friendly)")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
