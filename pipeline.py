"""MVP pipeline: collect + classify links from Common Crawl, no Athena required.

Two modes:

  domains  -- crawl specific domains (via the CDX Index API)
      python pipeline.py domains --domains example.com another.org \\
          --crawl CC-MAIN-2026-25 --limit 50 --db links.db

  countries -- discover pages across whole ccTLDs, with per-country priority
               weights or a flat per-country budget, by querying the public
               cc-index Parquet table directly with DuckDB (the same table
               Athena would query, no AWS needed). Discovery streams to a
               JSONL checkpoint file and fetching runs concurrently, so large
               (100k+ pages/country) runs can be interrupted and resumed.
      python pipeline.py countries --countries co cl pe ec uy mx ar \\
          --per-country-limit 200000 --workers 24 --crawl CC-MAIN-2026-25 --db links.db

Every fetched page is classified into a platform "engine" (Forum, Blog Comment,
Directory, Guestbook, Image Comment, Trackback, Article, Microblog, Social
Network -- see cc_links/footprints.json) for platform-market analysis.
Global mega-platforms (facebook, twitter/x, telegram, youtube, ...) are
excluded from both crawling and outbound-link storage -- see
cc_links/exclusions.json.
"""
import argparse
import json
import sys
import time
import zlib
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

from tqdm import tqdm
from bs4 import BeautifulSoup

from cc_links.cdx import get_cdx_records
from cc_links import fetch as fetch_mod
from cc_links.fetch import fetch_warc_record, parse_html_record, extract_links_from_html, domain_of, make_soup
from cc_links.db import init_db, insert_page, insert_links
from cc_links.engines import classify_engine
from cc_links.exclusions import load_excluded_domains, is_excluded
from cc_links.countries import load_priorities, allocate_budget, country_name, load_category_map
from cc_links.cc_index import discover_by_countries, load_candidates, load_candidates_shuffled, load_proxies


def process_page(conn, crawl, url, filename, offset, length, excluded, tld=None, country=None, delay=0.0):
    """Fetch one page's WARC record, classify its engine, and store page + outbound links.
    Used by the (single-threaded) `domains` mode."""
    try:
        raw = fetch_warc_record(filename, offset, length)
        html = parse_html_record(raw)
    except Exception as e:
        print(f"[fetch] skipping {url}: {e}", file=sys.stderr)
        return 0

    if html is None:
        return 0

    soup = make_soup(html)
    category, engine_name, _signal = classify_engine(html, url, soup=soup)
    links = extract_links_from_html(html, url, soup=soup)
    links = [(t, a) for t, a in links if not is_excluded(domain_of(t), excluded)]

    insert_page(conn, url, domain_of(url), crawl, "", tld=tld, country=country,
                engine_category=category, engine_name=engine_name)
    insert_links(conn, url, links)
    conn.commit()

    if delay:
        time.sleep(delay)
    return len(links)


def run_domains(domains, crawl, limit, db_path, delay, exclude_file):
    excluded = load_excluded_domains(exclude_file)
    conn = init_db(db_path)
    total_links = 0

    for domain in domains:
        if is_excluded(domain, excluded):
            print(f"[skip] {domain} is on the excluded global-platform list")
            continue

        pattern = domain if "*" in domain else f"{domain}/*"
        print(f"[cdx] querying {pattern} in {crawl} ...")
        try:
            records = get_cdx_records(pattern, crawl, limit=limit)
        except Exception as e:
            print(f"[cdx] error for {domain}: {e}", file=sys.stderr)
            continue

        records = [r for r in records if r.get("status") == "200" and "html" in r.get("mime", "")]
        print(f"[cdx] {len(records)} html pages found for {domain}")

        for r in tqdm(records, desc=domain):
            total_links += process_page(
                conn, crawl, r["url"], r["filename"], r["offset"], r["length"],
                excluded, delay=delay,
            )

    print(f"Done. {total_links} links stored in {db_path}")
    conn.close()


def _fetch_and_classify(record, excluded, extract_links=True):
    """Network + parsing only (thread-safe, no DB access) -- runs in worker threads.

    Wrapped end-to-end: a single malformed page anywhere in ~1.4M real-world
    pages must never crash the whole (multi-hour) run -- an uncaught exception
    here would propagate through Future.result() and kill the main loop.

    extract_links=False (i.e. --no-links) skips the <a href> parse+urljoin loop
    entirely -- pure CPU savings, since those links would only be counted and
    thrown away. This roughly halves per-page CPU on the classify-only path.
    """
    url = record["url"]
    try:
        raw = fetch_warc_record(record["filename"], record["offset"], record["length"])
        html = parse_html_record(raw)
        if html is None:
            return {"url": url, "ok": False, "error": "no-html-record"}

        category, engine_name, _signal = classify_engine(html, url)
        if extract_links:
            # Only build the DOM when we actually need the <a href> graph.
            soup = make_soup(html)
            links = extract_links_from_html(html, url, soup=soup)
            links = [(t, a) for t, a in links if not is_excluded(domain_of(t), excluded)]
        else:
            links = []

        return {
            "url": url, "ok": True,
            "tld": record.get("url_host_tld"), "bucket": record.get("bucket"),
            "category": category, "engine_name": engine_name, "links": links,
        }
    except Exception as e:
        return {"url": url, "ok": False, "error": f"{type(e).__name__}: {e}"}


def run_countries(countries, crawl, total_limit, per_country_limit, priorities_file, db_path,
                   workers, max_parts, exclude_file, candidates_file, commit_every, skip_discovery,
                   rate_limit, max_per_domain, proxy, proxy_file, store_links,
                   categories_file, per_category_limit, discovery_only, discover_delay, source,
                   shard=None):
    excluded = load_excluded_domains(exclude_file)
    candidates_file = candidates_file or (db_path + ".candidates.jsonl")
    fetch_mod.rate_limiter.set_rate(rate_limit)
    discovery_proxies = None
    if source == "s3":
        # High-throughput path for running inside AWS: fetch WARC records straight
        # from S3 (no CloudFront per-IP throttle, no proxies). Requires an EC2 role
        # that can read S3. Discovery still uses the CloudFront/parquet path.
        fetch_mod.enable_s3(pool_size=max(workers * 2, 64))
        print(f"[source] fetching WARC records from s3://commoncrawl (signed, no rate limit)")
    elif proxy_file:
        n = fetch_mod.load_proxy_file(proxy_file)
        discovery_proxies = load_proxies(proxy_file)  # rotate index reads across IPs too
        print(f"[proxy] rotating across {n} proxies from {proxy_file}")
        fetch_mod.start_proxy_refresher(proxy_file)  # hot-reload as an external harvester tops it up
    elif proxy:
        fetch_mod.set_proxy(proxy)
        print(f"[proxy] routing fetches through {proxy.split('@')[-1] if '@' in proxy else proxy}")

    # Budgets are keyed by "category". A category may be a single ccTLD (plain
    # per-country run) or a named bucket spanning several ccTLDs that share one
    # budget (--categories-file). tld_to_category maps each scanned ccTLD to the
    # budget it draws from; for a plain run it's the identity map.
    if categories_file:
        categories, tld_to_category = load_category_map(categories_file)
        if per_category_limit:
            budgets = {name: per_category_limit for name in categories}
        else:
            budgets = allocate_budget({name: 1.0 for name in categories}, total_limit)
        is_tld_label = False
    else:
        tld_to_category = {t: t for t in countries}
        if per_country_limit:
            budgets = {t: per_country_limit for t in countries}
        else:
            priorities = load_priorities(priorities_file, countries=countries)
            priorities = {t: w for t, w in priorities.items() if t in countries}
            budgets = allocate_budget(priorities, total_limit)
        is_tld_label = True
    print(f"[budget] {budgets}")

    if not skip_discovery:
        def progress(msg):
            print(f"[discover] {msg}")

        shortfall = discover_by_countries(
            crawl, budgets, tld_to_category, lambda d: is_excluded(d, excluded),
            out_path=candidates_file, max_parts=max_parts, max_per_domain=max_per_domain,
            progress=progress, proxies=discovery_proxies, part_delay=discover_delay,
        )
        for name, remaining in shortfall.items():
            if remaining > 0:
                label = f"{name} ({country_name(name)})" if is_tld_label else name
                print(f"[discover] WARNING: {label} short by {remaining} pages "
                      f"-- crawl may not have enough matching pages, or increase --max-parts")
    else:
        print(f"[discover] skipped, reusing {candidates_file}")

    if discovery_only:
        from collections import Counter as _Counter
        by_bucket = _Counter()
        for rec in load_candidates(candidates_file):
            by_bucket[rec.get("bucket", rec.get("url_host_tld"))] += 1
        total = sum(by_bucket.values())
        print(f"[discovery-only] {total} candidates in {candidates_file}:")
        for name, n in sorted(by_bucket.items(), key=lambda kv: -kv[1]):
            print(f"   {n:>7}  {name}")
        print(f"[discovery-only] done -- rerun with --skip-discovery to fetch")
        return

    conn = init_db(db_path)
    already = {row[0] for row in conn.execute("SELECT url FROM pages")}
    print(f"[resume] {len(already)} pages already stored, will be skipped")

    # --shard i/N: run N processes in parallel (one per CPU core, each to its own
    # --db) over disjoint slices of the candidates, split by a stable hash of the
    # URL so the split needs no coordination between processes. Merge the shard
    # DBs afterwards with merge_shards.py.
    shard_i = shard_n = None
    if shard:
        shard_i, shard_n = (int(x) for x in shard.split("/"))
        print(f"[shard] this process handles slice {shard_i} of {shard_n}")

    def candidate_iter():
        # Shuffled so a rate-limit block mid-run loses a random slice of every
        # country instead of wiping out whichever ccTLDs hadn't been reached yet
        # (discovery writes results in contiguous per-ccTLD blocks).
        for rec in load_candidates_shuffled(candidates_file):
            if shard_n is not None and zlib.crc32(rec["url"].encode()) % shard_n != shard_i:
                continue
            if rec["url"] not in already:
                yield rec

    if shard_n is not None:
        total_count = sum(1 for r in load_candidates(candidates_file)
                          if zlib.crc32(r["url"].encode()) % shard_n == shard_i)
    else:
        total_count = sum(1 for _ in load_candidates(candidates_file))
    total_links = 0
    processed = 0
    consecutive_failures = 0
    error_counts = Counter()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        pending = set()
        it = candidate_iter()

        def fill():
            for rec in it:
                pending.add(ex.submit(_fetch_and_classify, rec, excluded, store_links))
                if len(pending) >= workers * 4:
                    break

        fill()
        with tqdm(total=total_count, initial=len(already)) as pbar:
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    res = fut.result()
                    if res["ok"]:
                        consecutive_failures = 0
                        cname = country_name(res["tld"])
                        insert_page(conn, res["url"], domain_of(res["url"]), crawl, "",
                                    tld=res["tld"], country=cname, bucket=res.get("bucket"),
                                    engine_category=res["category"], engine_name=res["engine_name"],
                                    outlink_count=len(res["links"]))
                        if store_links:
                            insert_links(conn, res["url"], res["links"])
                        total_links += len(res["links"])
                    else:
                        consecutive_failures += 1
                        error_counts[res["error"]] += 1
                    processed += 1
                    pbar.update(1)
                    if processed % commit_every == 0:
                        conn.commit()

                # Circuit breaker: a long unbroken streak of failures across the whole
                # pool means we're likely being throttled (e.g. CloudFront 403s) --
                # pause and back off the global rate rather than burning through the
                # remaining candidates at a 100% failure rate.
                if consecutive_failures >= workers * 3:
                    current_rate = 1.0 / fetch_mod.rate_limiter.min_interval
                    new_rate = max(current_rate / 2, 1.0)
                    print(f"\n[throttle] {consecutive_failures} failures in a row "
                          f"({dict(error_counts.most_common(3))}); pausing 90s and cutting rate "
                          f"{current_rate:.1f} -> {new_rate:.1f} req/s")
                    time.sleep(90)
                    fetch_mod.rate_limiter.set_rate(new_rate)
                    consecutive_failures = 0

                fill()

    conn.commit()
    if error_counts:
        print(f"[errors] {sum(error_counts.values())} failed fetches:")
        for err, count in error_counts.most_common(10):
            print(f"   {count:>6}  {err}")
    links_msg = f"{total_links} links stored" if store_links else f"{total_links} links counted (not stored, --no-links)"
    print(f"Done. {links_msg} in {db_path}")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Collect and classify links from Common Crawl without Athena.")
    sub = parser.add_subparsers(dest="mode", required=True)

    p_domains = sub.add_parser("domains", help="Crawl specific domains via the CDX Index API")
    p_domains.add_argument("--domains", nargs="+", required=True)
    p_domains.add_argument("--crawl", default="CC-MAIN-2026-25")
    p_domains.add_argument("--limit", type=int, default=50, help="Max pages per domain")
    p_domains.add_argument("--db", default="links.db")
    p_domains.add_argument("--delay", type=float, default=0.2)
    p_domains.add_argument("--exclude-file", help="Extra exclusions JSON (adds to cc_links/exclusions.json)")

    p_countries = sub.add_parser("countries", help="Discover pages across ccTLDs via the Parquet cc-index (DuckDB)")
    p_countries.add_argument("--countries", nargs="+", help="ccTLDs, e.g. co cl pe ec uy mx ar "
                                                             "(omit when using --categories-file)")
    p_countries.add_argument("--categories-file", help="JSON grouping ccTLDs into named categories that "
                                                       "each share one budget: {\"Other Africa\": [\"eg\", "
                                                       "\"ng\", ...], \"Colombia\": [\"co\"], ...}. Use with "
                                                       "--per-category-limit. Replaces --countries.")
    p_countries.add_argument("--per-category-limit", type=int,
                              help="Flat budget per category (with --categories-file), e.g. 100000")
    p_countries.add_argument("--crawl", default="CC-MAIN-2026-25")
    p_countries.add_argument("--total-limit", type=int, default=300,
                              help="Total pages split by --priorities (ignored if --per-country-limit is set)")
    p_countries.add_argument("--per-country-limit", type=int,
                              help="Flat budget per ccTLD, e.g. 200000 -- overrides --total-limit/--priorities")
    p_countries.add_argument("--priorities", help="JSON file: {\"ru\": 3, \"de\": 1, ...} priority weights")
    p_countries.add_argument("--db", default="links.db")
    p_countries.add_argument("--workers", type=int, default=20, help="Concurrent fetch workers")
    p_countries.add_argument("--rate-limit", type=float, default=15,
                              help="Max requests/sec to data.commoncrawl.org across all workers "
                                   "(observed 403 throttling above ~90/s; auto-halves on sustained failures)")
    p_countries.add_argument("--max-per-domain", type=int, default=None,
                              help="Cap pages taken from any single registered domain during discovery "
                                   "(prevents one high-volume site from dominating a ccTLD's sample)")
    p_countries.add_argument("--source", choices=["cloudfront", "s3"], default="cloudfront",
                              help="Where to fetch WARC records: 'cloudfront' (data.commoncrawl.org, "
                                   "per-IP throttled, works anywhere) or 's3' (s3://commoncrawl, no "
                                   "throttle, but only from inside AWS -- e.g. an EC2 instance with an "
                                   "S3-read IAM role). Discovery always uses the CloudFront/parquet path.")
    p_countries.add_argument("--proxy", help="Single proxy URL, e.g. a rotating-gateway endpoint "
                                              "http://user:pass@gateway:port")
    p_countries.add_argument("--proxy-file", help="File with one proxy per line (host:port:user:pass) -- "
                                                   "requests round-robin across the whole pool, which "
                                                   "lifts the per-IP throttle ceiling so --rate-limit can "
                                                   "go much higher")
    p_countries.add_argument("--max-parts", type=int, default=None,
                              help="Cap on Parquet index parts scanned (default: all ~300, needed for large budgets)")
    p_countries.add_argument("--exclude-file", help="Extra exclusions JSON (adds to cc_links/exclusions.json)")
    p_countries.add_argument("--candidates-file", help="JSONL discovery checkpoint (default: <db>.candidates.jsonl)")
    p_countries.add_argument("--commit-every", type=int, default=200, help="SQLite commit interval (# pages)")
    p_countries.add_argument("--skip-discovery", action="store_true",
                              help="Reuse an existing candidates file instead of re-scanning the index")
    p_countries.add_argument("--discovery-only", action="store_true",
                              help="Run only the index scan (write + summarize candidates), then stop "
                                   "before fetching. Resume the fetch later with --skip-discovery.")
    p_countries.add_argument("--config", help="JSON config file supplying defaults for any option "
                                              "below (e.g. run.config.json). Explicit CLI flags override it.")
    p_countries.add_argument("--shard", help="Run one of N parallel workers over a disjoint URL "
                                             "slice, e.g. --shard 0/4 (each shard needs its own --db). "
                                             "Merge the shard DBs afterwards with merge_shards.py.")
    p_countries.add_argument("--discover-delay", type=float, default=0.0,
                              help="Seconds to pause between index parts during discovery -- paces direct "
                                   "(un-proxied) parquet reads under CloudFront's throttle threshold. "
                                   "Try 1-2s for a large multi-part scan.")
    p_countries.add_argument("--no-links", action="store_true",
                              help="Don't store individual outbound links (only their count per page). "
                                   "For engine-market-share analysis the links table isn't needed and is "
                                   "by far the biggest cost: ~100+ link rows/page means ~50GB+ for 1.4M "
                                   "pages, vs a few hundred MB for pages alone")

    args = parser.parse_args()

    # A single editable config file can supply any option; explicit CLI flags win.
    if getattr(args, "config", None):
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        on_cli = {a.lstrip("-").split("=")[0].replace("-", "_")
                  for a in sys.argv[1:] if a.startswith("--")}
        for key, val in cfg.items():
            if key.startswith(("comment", "_")):
                continue
            if key in on_cli or not hasattr(args, key):
                continue
            setattr(args, key, val)

    if args.mode == "domains":
        run_domains(args.domains, args.crawl, args.limit, args.db, args.delay, args.exclude_file)
    elif args.mode == "countries":
        if bool(args.countries) == bool(args.categories_file):
            parser.error("provide exactly one of --countries or --categories-file")
        run_countries(args.countries, args.crawl, args.total_limit, args.per_country_limit,
                       args.priorities, args.db, args.workers, args.max_parts, args.exclude_file,
                       args.candidates_file, args.commit_every, args.skip_discovery, args.rate_limit,
                       args.max_per_domain, args.proxy, args.proxy_file, not args.no_links,
                       args.categories_file, args.per_category_limit, args.discovery_only,
                       args.discover_delay, args.source, args.shard)


if __name__ == "__main__":
    main()
