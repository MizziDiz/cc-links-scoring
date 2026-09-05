#!/usr/bin/env python3
"""Re-check pages an older taxonomy rejected, without repeating discovery.

Every URL the collector ever fetched is in ``processed_urls``; the ones it
rejected carry ``outcome = 'unmatched'`` and their WARC offsets are still in
the saved discovery manifests. When the taxonomy improves (v3 recovers 93% of
the pages the ``guestbook`` pattern found, for example) those pages can be
fetched again and classified with the current rules for the price of one
range request each, instead of 28 minutes of Parquet discovery per snapshot.

    python recheck_unmatched.py --db prospects.db \
        --state-dir crawl_states --state-dir crawl_states-broad \
        --group guestbook:0 --group guestbook:1 --limit 50000 \
        --source s3 --workers 16

Each re-checked URL is written back through the same functions the collector
uses: a match becomes a candidate and its outcome flips to ``stored``; a
repeat rejection refreshes ``processed_at`` so the yield gate sees a current
decision. Before/after lines go to ``--log`` (JSONL) so a batch can be
reviewed or reverted. The domain cap is enforced at the end exactly as the
collector does.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import sys
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from cc_links import fetch as fetch_mod
from cc_links.countries import country_name
from cc_links.db import enforce_domain_cap, init_db, mark_url_processed, upsert_candidate
from cc_links.fetch import domain_of
from cc_links.prospects import normalize_url, taxonomy_version
from prospect_pipeline import fetch_and_classify


def select_targets(conn: sqlite3.Connection, groups: list[str], since: str | None,
                   until: str | None, limit: int | None) -> dict[str, dict]:
    """Rejected URLs to re-check, keyed by normalized URL."""
    where = "outcome = 'unmatched'"
    params: list = []
    if groups:
        where += " AND pattern_id IN (%s)" % ", ".join("?" for _ in groups)
        params.extend(groups)
    if since:
        where += " AND processed_at >= ?"
        params.append(since)
    if until:
        where += " AND processed_at < ?"
        params.append(until)
    sql = (f"SELECT normalized_url, url, crawl, pattern_id, discovery_tier, "
           f"registered_domain, bucket FROM processed_urls WHERE {where}")
    if limit:
        sql += f" LIMIT {int(limit)}"
    targets = {}
    for normalized, url, crawl, pattern_id, tier, domain, bucket in conn.execute(sql, params):
        targets[normalized] = {"url": url, "crawl": crawl, "pattern_id": pattern_id,
                               "discovery_tier": tier, "registered_domain": domain,
                               "bucket": bucket}
    return targets


def resolve_offsets(state_dirs: list[str], targets: dict[str, dict],
                    progress=None) -> dict[str, dict]:
    """Scan manifests once and attach filename/offset/length to each target."""
    resolved: dict[str, dict] = {}
    paths = []
    for state_dir in state_dirs:
        paths.extend(sorted(glob.glob(os.path.join(state_dir, "*.jsonl"))))
    combined = [p for p in paths if ".shard-" not in os.path.basename(p)]
    shards = [p for p in paths if ".shard-" in os.path.basename(p)]
    for path in combined + shards:          # combined files first, shards fill gaps
        if len(resolved) >= len(targets):
            break
        with open(path, encoding="utf-8") as source:
            for line in source:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                normalized = normalize_url(str(record.get("url", "")))
                if normalized in targets and normalized not in resolved:
                    if not record.get("filename") or record.get("offset") is None:
                        continue
                    resolved[normalized] = {**targets[normalized], **{
                        "filename": record["filename"], "offset": record["offset"],
                        "length": record.get("length"),
                        "fetch_status": record.get("fetch_status"),
                        "url_host_tld": record.get("url_host_tld"),
                        "url_host_registered_domain": record.get("url_host_registered_domain")
                        or targets[normalized].get("registered_domain"),
                        "prefetch_score": record.get("prefetch_score"),
                        "matched_discovery": record.get("matched_discovery", []),
                    }}
        if progress:
            progress(f"{os.path.basename(path)}: resolved {len(resolved)}/{len(targets)}")
    return resolved


def run(args) -> int:
    conn = init_db(args.db)
    version = taxonomy_version(args.footprints)
    targets = select_targets(conn, args.group, args.since, args.until, args.limit)
    print(f"[recheck] taxonomy v{version}; {len(targets)} rejected URLs selected "
          f"(groups={args.group or 'all'})", flush=True)
    if not targets:
        conn.close()
        return 0
    records = resolve_offsets(args.state_dir, targets,
                              progress=lambda m: print(f"[manifest] {m}", flush=True))
    missing = len(targets) - len(records)
    print(f"[recheck] {len(records)} with WARC offsets, {missing} not found in manifests",
          flush=True)

    fetch_mod.rate_limiter.set_rate(args.rate_limit)
    if args.source == "s3":
        fetch_mod.enable_s3(pool_size=max(args.workers * 2, 16))

    log = open(args.log, "a", encoding="utf-8") if args.log else None
    stats: Counter = Counter()
    by_group: dict[str, Counter] = {}
    processed = 0
    started = time.monotonic()
    last_report = started
    iterator = iter(records.items())
    pending = set()

    def fill(pool):
        for normalized, rec in iterator:
            pending.add(pool.submit(
                lambda n, r: (n, fetch_and_classify(r, args.footprints, args.min_score)),
                normalized, rec))
            if len(pending) >= args.workers * 4:
                break

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            fill(pool)
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    normalized, result = future.result()
                    rec = result["record"]
                    processed += 1
                    group = by_group.setdefault(str(rec.get("pattern_id")), Counter())
                    attribution = {
                        "registered_domain": rec.get("url_host_registered_domain"),
                        "country": country_name(rec.get("url_host_tld")),
                        "bucket": rec.get("bucket"),
                        "discovery_tier": rec.get("discovery_tier"),
                        "pattern_id": rec.get("pattern_id"),
                        "prefetch_score": rec.get("prefetch_score"),
                        "matched_discovery": json.dumps(
                            rec.get("matched_discovery", []), ensure_ascii=False),
                    }
                    if not result["ok"]:
                        stats["fetch_error"] += 1
                        group["fetch_error"] += 1
                        outcome = "error"
                    elif not result["matches"]:
                        stats["still_unmatched"] += 1
                        group["still_unmatched"] += 1
                        mark_url_processed(conn, normalized, rec["url"], rec["crawl"],
                                           "unmatched", **attribution)
                        outcome = "unmatched"
                    else:
                        best = result["matches"][0]
                        all_matches = [m.to_dict() for m in result["matches"]]
                        upsert_candidate(
                            conn, normalized_url=normalized, url=rec["url"],
                            domain=domain_of(rec["url"]),
                            registered_domain=rec.get("url_host_registered_domain"),
                            crawl=rec["crawl"], tld=rec.get("url_host_tld"),
                            country=country_name(rec.get("url_host_tld")),
                            bucket=rec.get("bucket"), family=best.family,
                            platform=best.platform, score=best.score,
                            matched_signals=json.dumps(all_matches, ensure_ascii=False),
                            warc_filename=rec.get("filename"), warc_offset=rec.get("offset"),
                            warc_length=rec.get("length"),
                            discovery_tier=rec.get("discovery_tier"),
                            pattern_id=rec.get("pattern_id"),
                            prefetch_score=rec.get("prefetch_score"),
                            matched_discovery=attribution["matched_discovery"],
                        )
                        mark_url_processed(conn, normalized, rec["url"], rec["crawl"],
                                           "stored", best.score, final_family=best.family,
                                           final_platform=best.platform,
                                           final_rule_id=best.rule_id,
                                           matched_signals=json.dumps(
                                               all_matches, ensure_ascii=False),
                                           **attribution)
                        stats["stored"] += 1
                        stats[f"family:{best.family}"] += 1
                        group["stored"] += 1
                        outcome = f"stored:{best.rule_id}:{best.score}"
                    if log:
                        log.write(json.dumps({"url": rec["url"], "before": "unmatched",
                                              "after": outcome, "taxonomy": version},
                                             ensure_ascii=False) + "\n")
                    if processed % args.commit_every == 0:
                        conn.commit()
                        if log:
                            log.flush()
                    now = time.monotonic()
                    if now - last_report >= args.progress_interval:
                        elapsed = max(now - started, 0.001)
                        print(f"[progress] rechecked={processed}/{len(records)} "
                              f"stored={stats['stored']} still_unmatched={stats['still_unmatched']} "
                              f"errors={stats['fetch_error']} rate={processed / elapsed:.1f}/s",
                              flush=True)
                        last_report = now
                fill(pool)
    except BaseException:
        conn.commit()
        conn.close()
        if log:
            log.close()
        raise
    conn.commit()
    capped = enforce_domain_cap(conn, args.max_per_domain)
    if capped:
        print(f"[domain-cap] archived {capped} candidates above {args.max_per_domain} per domain",
              flush=True)
    conn.close()
    if log:
        log.close()
    print("[result] " + ", ".join(f"{k}={v}" for k, v in stats.most_common()), flush=True)
    for group, counter in sorted(by_group.items(), key=lambda kv: -sum(kv[1].values())):
        total = sum(counter.values())
        print(f"[group] {group}: {counter['stored']}/{total} stored "
              f"({counter['stored'] / max(total, 1):.1%}), errors={counter['fetch_error']}",
              flush=True)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", required=True)
    parser.add_argument("--state-dir", action="append", required=True,
                        help="Directory with discovery manifests; repeatable")
    parser.add_argument("--group", action="append", default=[],
                        help="pattern_id to re-check (repeatable); default all rejected URLs")
    parser.add_argument("--since", help="Only rejections decided at or after YYYY-MM-DD")
    parser.add_argument("--until", help="Only rejections decided before YYYY-MM-DD "
                                        "(e.g. the date the new taxonomy went live)")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--footprints")
    parser.add_argument("--min-score", type=int, default=50)
    parser.add_argument("--max-per-domain", type=int, default=10)
    parser.add_argument("--source", choices=["cloudfront", "s3"], default="cloudfront")
    parser.add_argument("--rate-limit", type=float, default=15)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--commit-every", type=int, default=200)
    parser.add_argument("--progress-interval", type=float, default=60)
    parser.add_argument("--log", help="JSONL with one before/after line per URL")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    sys.exit(run(args))


if __name__ == "__main__":
    main()
