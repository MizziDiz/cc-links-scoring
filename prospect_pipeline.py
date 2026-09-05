#!/usr/bin/env python3
"""Build a scored link-prospect database from Common Crawl only."""
import argparse
import math
import json
import sys
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from tqdm import tqdm

from cc_links import fetch as fetch_mod
from cc_links.cc_index import (discover_by_countries,
                               load_candidates_prioritized, load_proxies)
from cc_links.countries import country_name, load_category_limits, load_category_map
from cc_links.db import (enforce_candidate_floor, enforce_domain_cap, init_db,
                         load_domain_priors, mark_url_processed,
                         record_fetch_error, upsert_candidate)
from cc_links.exclusions import is_excluded, load_excluded_domains
from cc_links.fetch import domain_of, fetch_warc_record, parse_html_record
from cc_links.feedback import load_priority_adjustments
from cc_links.prospects import (broad_discovery_regex,
                                classify_discovery_url, classify_prospect,
                                discovery_ruleset_identity, discovery_url_patterns,
                                normalize_url, precise_discovery_regex)
from cc_links.yield_gate import (DomainGate, YieldGate, apply_gates,
                                 collect_pattern_yield, load_known_domains,
                                 load_yield_profile)


def fetch_and_classify(record, footprints, minimum_score):
    status = int(record.get("fetch_status") or 200)
    if 300 <= status <= 399:
        matches = [match for match in classify_prospect(
            "", record["url"], footprints, minimum_score)
                   if match.family == "redirect_backlink"]
        for match in matches:
            match.score = max(match.score, 85)
            match.signals.append(f"cc_status:{status}")
        if matches:
            return {"ok": True, "record": record, "matches": matches}
        return {"ok": True, "record": record, "matches": []}
    try:
        raw = fetch_warc_record(record["filename"], record["offset"], record["length"])
        html = parse_html_record(raw)
        if html is None:
            return {"ok": False, "record": record, "error": "no-html-record"}
        matches = classify_prospect(html, record["url"], footprints, minimum_score)
        return {"ok": True, "record": record, "matches": matches}
    except Exception as exc:
        return {
            "ok": False,
            "record": record,
            "error": f"{type(exc).__name__}: {exc}",
        }


def run(args):
    excluded = load_excluded_domains(args.exclude_file)
    categories, tld_to_category = load_category_map(args.categories_file)
    budgets = load_category_limits(
        args.category_limits, categories, args.per_category_limit)
    if args.category_limit_divisor > 1:
        budgets = {
            name: max(1, int(math.ceil(limit / args.category_limit_divisor)))
            for name, limit in budgets.items()
        }
    candidates_file = args.candidates_file or args.db + ".prospects.jsonl"
    patterns = discovery_url_patterns(args.footprints)
    redirect_patterns = discovery_url_patterns(args.footprints, family="redirect_backlink")
    compound = sum(len(pattern) > 1 for pattern in patterns)
    precise_regex = precise_discovery_regex(args.footprints)
    discovery_regex = (broad_discovery_regex(args.footprints)
                       if args.discovery_profile == "broad"
                       else precise_regex)
    broad_budgets = None
    if args.discovery_profile == "broad":
        broad_budgets = {
            name: int(math.ceil(limit * args.broad_quota_fraction))
            for name, limit in budgets.items()
        }
    if args.discovery_profile == "broad":
        print(f"[footprints] broad discovery regex enabled; "
              f"{len(patterns)} precise patterns retained as priority tier; "
              f"broad index sample={args.broad_index_sample:.1%}")
    else:
        print(f"[footprints] {len(patterns)} selective URL patterns compiled into "
              f"one discovery regex ({compound} compound)")
    print(f"[budget] {budgets}")
    if broad_budgets is not None:
        print(f"[budget] broad-only cap={args.broad_quota_fraction:.0%}: "
              f"{broad_budgets}")

    discovery_proxies = None
    index_source = args.index_source
    if index_source == "auto":
        index_source = "s3" if args.source == "s3" else "https"
    print(f"[discover] index_source={index_source}")
    checkpoint_identity = discovery_ruleset_identity(
        args.crawl, args.footprints, args.discovery_profile,
        args.broad_index_sample,
    )
    priority_profile = load_priority_adjustments(args.pattern_priorities)
    fetch_mod.rate_limiter.set_rate(args.rate_limit)
    if args.source == "s3":
        fetch_mod.enable_s3(pool_size=max(args.workers * 2, 64))
    elif args.proxy_file:
        fetch_mod.load_proxy_file(args.proxy_file)
        discovery_proxies = load_proxies(args.proxy_file)
    elif args.proxy:
        fetch_mod.set_proxy(args.proxy)

    prior_conn = init_db(args.db)
    domain_priors = load_domain_priors(prior_conn, args.min_score)
    prior_conn.close()
    initial_domain_counts = {
        domain: values["candidate_count"]
        for domain, values in domain_priors.items()
    }
    if domain_priors:
        print(
            f"[domain-prior] {len(domain_priors)} confirmed domains; "
            "existing candidates count toward the domain cap"
        )

    if not args.skip_discovery:
        def candidate_metadata(url, discovery_tier, registered_domain, bucket):
            evidence = classify_discovery_url(
                url, args.footprints,
                include_broad=(args.discovery_profile == "broad"),
            )
            prior = domain_priors.get(registered_domain or "")
            prior_boost = (
                15 if prior and prior["max_score"] >= 70
                else 10 if prior else 0
            )
            if not evidence:
                return {
                    "prefetch_score": prior_boost,
                    "pattern_id": "",
                    "pattern_family": "",
                    "matched_discovery": [],
                    "domain_prior": prior_boost,
                }
            best = evidence[0]
            return {
                "prefetch_score": min(100, best.score + prior_boost),
                "pattern_id": best.pattern_id,
                "pattern_family": best.family,
                "matched_discovery": [
                    match.to_dict() for match in evidence
                ],
                "domain_prior": prior_boost,
                "domain_prior_candidates": (
                    prior["candidate_count"] if prior else 0
                ),
            }

        discover_by_countries(
            args.crawl, budgets, tld_to_category,
            lambda d: is_excluded(d, excluded), candidates_file,
            max_parts=args.max_parts, max_per_domain=args.max_per_domain,
            progress=lambda m: print(f"[discover] {m}"), proxies=discovery_proxies,
            part_delay=args.discover_delay,
            url_regex=discovery_regex,
            priority_url_regex=(precise_regex
                                if args.discovery_profile == "broad" else None),
            redirect_url_patterns=redirect_patterns,
            part_shard=(tuple(int(v) for v in args.part_shard.split("/"))
                        if args.part_shard else None),
            index_source=index_source,
            broad_category_budgets=broad_budgets,
            broad_sample_fraction=(args.broad_index_sample
                                   if args.discovery_profile == "broad" else None),
            collect_metrics=args.discovery_metrics,
            candidate_metadata_fn=candidate_metadata,
            checkpoint_identity=checkpoint_identity,
            initial_domain_counts=initial_domain_counts,
        )
    else:
        print(f"[discover] skipped; using {candidates_file}")

    if args.discovery_only:
        return

    conn = init_db(args.db)
    archived = enforce_candidate_floor(conn, args.min_score)
    if archived:
        print(f"[score-floor] archived {archived} historical candidates below {args.min_score}")
    capped = enforce_domain_cap(conn, args.max_per_domain)
    if capped:
        print(f"[domain-cap] archived {capped} historical candidates above "
              f"{args.max_per_domain} per domain")
    existing = {r[0] for r in conn.execute("SELECT normalized_url FROM candidates")}
    existing.update(r[0] for r in conn.execute("SELECT normalized_url FROM processed_urls"))
    print(f"[resume] {len(existing)} URLs already processed; they will not be fetched again")

    # Pre-fetch gates. They only decide what is worth a WARC request; the
    # classifier, --min-score and --max-per-domain still decide what is stored.
    yield_profile = None
    if args.pattern_min_yield is not None:
        if args.pattern_yield_file:
            yield_profile = load_yield_profile(args.pattern_yield_file)
        else:
            yield_profile = collect_pattern_yield(
                args.db, since=args.pattern_yield_since,
                minimum_decisions=args.pattern_min_decisions)
        probe = YieldGate(yield_profile, args.pattern_min_yield,
                          args.pattern_min_decisions, args.pattern_explore_share)
        low = probe.low_yield_groups()
        print(f"[yield-gate] {len(yield_profile['groups'])} pattern/tier groups known; "
              f"{len(low)} below {args.pattern_min_yield:.0%} yield will be skipped "
              f"(explore share {args.pattern_explore_share:.0%})")
        for group in low[:10]:
            print(f"[yield-gate]   skip {group['pattern_id']}|tier{group['discovery_tier']}: "
                  f"{group['stored']}/{group['decisions']} stored "
                  f"({group['yield']:.1%})")
    known_domains = set()
    if args.new_domains_only:
        known_domains = load_known_domains(conn)
        print(f"[domain-gate] {len(known_domains)} domains already have a candidate; "
              "their URLs will not be fetched in this run")
    if args.per_run_domain_cap:
        print(f"[domain-gate] at most {args.per_run_domain_cap} URL(s) per domain "
              "scheduled in this run")

    def make_gates():
        return [
            YieldGate(yield_profile, args.pattern_min_yield or 0.0,
                      args.pattern_min_decisions, args.pattern_explore_share),
            DomainGate(known_domains, skip_known=args.new_domains_only,
                       per_run_cap=args.per_run_domain_cap),
        ]

    gates = make_gates()
    scheduled = set(existing)

    def attribution(rec):
        return {
            "discovery_tier": rec.get("discovery_tier"),
            "pattern_id": rec.get("pattern_id"),
            "prefetch_score": rec.get("prefetch_score"),
            "matched_discovery": json.dumps(
                rec.get("matched_discovery", []), ensure_ascii=False
            ),
        }

    def processing_attribution(rec):
        return {
            "registered_domain": rec.get("url_host_registered_domain"),
            "country": country_name(rec.get("url_host_tld")),
            "bucket": rec.get("bucket"),
            **attribution(rec),
        }

    def schedulable(seen, active_gates):
        """New manifest URLs that pass every gate, in priority order."""
        emitted = 0
        source = load_candidates_prioritized(
            candidates_file, priority_profile=priority_profile)

        def unseen():
            for rec in source:
                normalized = normalize_url(rec["url"])
                if normalized not in seen:
                    seen.add(normalized)
                    yield rec

        for rec in apply_gates(unseen(), active_gates):
            yield rec
            emitted += 1
            if args.fetch_limit and emitted >= args.fetch_limit:
                return

    def records():
        yield from schedulable(scheduled, gates)

    # Count without consuming the de-duplicating iterator used by the workers.
    # Fresh gate instances keep the per-run domain counters of the real pass intact.
    total = sum(1 for _ in schedulable(set(existing), make_gates()))
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
                    rec = result["record"]
                    normalized = normalize_url(rec["url"])
                    if not result["ok"]:
                        stats["fetch_error"] += 1
                        record_fetch_error(
                            conn, normalized, rec["url"], args.crawl,
                            result["error"],
                        )
                    elif not result["matches"]:
                        stats["unmatched"] += 1
                        mark_url_processed(
                            conn, normalized, rec["url"], args.crawl,
                            "unmatched", **processing_attribution(rec))
                    else:
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
                            **attribution(rec),
                        )
                        stats[f"family:{best.family}"] += 1
                        stats["stored"] += 1
                        mark_url_processed(
                            conn, normalized, rec["url"], args.crawl,
                            "stored", best.score, final_family=best.family,
                            final_platform=best.platform,
                            final_rule_id=best.rule_id,
                            matched_signals=json.dumps(
                                all_matches, ensure_ascii=False
                            ),
                            **processing_attribution(rec))
                    # Commit on every Nth result regardless of its outcome. Until
                    # 2026-09 this ran only for stored results, so a run whose
                    # every Nth page was unmatched never checkpointed and a
                    # SIGINT/Spot interruption replayed hours of paid fetches.
                    if processed % args.commit_every == 0:
                        conn.commit()
                    now = time.monotonic()
                    if now - last_report >= args.progress_interval:
                        elapsed = max(now - started_at, 0.001)
                        print(f"[progress] processed={processed}/{total} "
                              f"stored={stats['stored']} unmatched={stats['unmatched']} "
                              f"errors={stats['fetch_error']} "
                              f"skipped_yield={gates[0].stats['skipped']} "
                              f"skipped_domain={gates[1].stats['skipped_known'] + gates[1].stats['skipped_cap']} "
                              f"rate={processed / elapsed:.1f}/s",
                              flush=True)
                        last_report = now
                fill()
    conn.commit()
    final_capped = enforce_domain_cap(conn, args.max_per_domain)
    if final_capped:
        print(f"[domain-cap] archived {final_capped} newly collected candidates above "
              f"{args.max_per_domain} per domain")
    conn.close()
    print("[result] " + ", ".join(f"{k}={v}" for k, v in stats.most_common()))
    print("[yield-gate] " + ", ".join(f"{k}={v}" for k, v in gates[0].stats.items()))
    print("[domain-gate] " + ", ".join(f"{k}={v}" for k, v in gates[1].stats.items()))


def main():
    parser = argparse.ArgumentParser(description="Collect scored link prospects from Common Crawl")
    parser.add_argument("--categories-file", default="categories.json")
    parser.add_argument("--category-limits",
                        help="JSON with per-category discovery limits; unspecified categories "
                             "fall back to --per-category-limit")
    parser.add_argument("--category-limit-divisor", type=int, default=1,
                        help=argparse.SUPPRESS)
    parser.add_argument("--footprints", default=None)
    parser.add_argument(
        "--pattern-priorities",
        help="Optional JSON generated by feedback_report.py; affects fetch order only")
    parser.add_argument(
        "--fetch-limit", type=int,
        help="Fetch at most N new manifest URLs (primarily for reproducible pilots)")
    parser.add_argument(
        "--pattern-min-yield", type=float,
        help="Skip URLs whose discovery pattern/tier historically stored fewer than "
             "this share of fetched pages (e.g. 0.2). Off unless given.")
    parser.add_argument(
        "--pattern-yield-file",
        help="Yield profile JSON from yield_report.py; without it the profile is "
             "computed from --db at start")
    parser.add_argument(
        "--pattern-yield-since",
        help="Only count processed_urls rows at or after this timestamp "
             "(YYYY-MM-DD) when computing the profile from --db")
    parser.add_argument("--pattern-min-decisions", type=int, default=20,
                        help="Groups with fewer decisions are always fetched (exploration)")
    parser.add_argument("--pattern-explore-share", type=float, default=0.05,
                        help="Deterministic share of low-yield URLs still fetched")
    parser.add_argument(
        "--new-domains-only", action="store_true",
        help="Do not fetch URLs of registered domains that already have a candidate")
    parser.add_argument(
        "--per-run-domain-cap", type=int, default=0,
        help="Schedule at most N URLs per registered domain in this run (0 = off)")
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
    parser.add_argument(
        "--discovery-metrics", action="store_true",
        help="Persist optional Parquet row counts and discovery-funnel counters")
    parser.add_argument("--discovery-profile", choices=["precise", "broad"],
                        default="precise")
    parser.add_argument("--broad-quota-fraction", type=float, default=0.25,
                        help="Maximum share of each category filled by broad-only URLs")
    parser.add_argument("--broad-index-sample", type=float, default=0.02,
                        help="Deterministic share of broad-only index matches to rank/fetch")
    parser.add_argument("--source", choices=["cloudfront", "s3"], default="cloudfront")
    parser.add_argument(
        "--index-source", choices=["auto", "https", "s3"], default="auto",
        help="Where DuckDB reads the Parquet index; auto uses S3 with --source s3")
    parser.add_argument("--proxy")
    parser.add_argument("--proxy-file")
    parser.add_argument("--exclude-file")
    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument("--discovery-only", action="store_true")
    parser.add_argument("--part-shard",
                        help="Scan only shard i/N of index parts, e.g. 0/4")
    parser.add_argument("--commit-every", type=int, default=200)
    parser.add_argument("--progress-interval", type=float, default=60,
                        help="Emit one plain progress line every N seconds (systemd/journal friendly)")
    args = parser.parse_args()
    if args.category_limit_divisor < 1:
        parser.error("--category-limit-divisor must be at least 1")
    if not 0.0 <= args.broad_quota_fraction <= 1.0:
        parser.error("--broad-quota-fraction must be between 0 and 1")
    if not 0.0 <= args.broad_index_sample <= 1.0:
        parser.error("--broad-index-sample must be between 0 and 1")
    if args.fetch_limit is not None and args.fetch_limit < 1:
        parser.error("--fetch-limit must be at least 1")
    if args.pattern_min_yield is not None and not 0.0 <= args.pattern_min_yield <= 1.0:
        parser.error("--pattern-min-yield must be between 0 and 1")
    if not 0.0 <= args.pattern_explore_share <= 1.0:
        parser.error("--pattern-explore-share must be between 0 and 1")
    if args.per_run_domain_cap < 0:
        parser.error("--per-run-domain-cap must be 0 or positive")
    run(args)


if __name__ == "__main__":
    main()
