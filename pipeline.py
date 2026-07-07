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
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

from tqdm import tqdm
from bs4 import BeautifulSoup

from cc_links.cdx import get_cdx_records
from cc_links import fetch as fetch_mod
from cc_links.fetch import fetch_warc_record, parse_html_record, extract_links_from_html, domain_of
from cc_links.db import init_db, insert_page, insert_links
from cc_links.engines import classify_engine
from cc_links.exclusions import load_excluded_domains, is_excluded
from cc_links.countries import load_priorities, allocate_budget, country_name
from cc_links.cc_index import discover_by_countries, load_candidates, load_candidates_shuffled


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

    soup = BeautifulSoup(html, "html.parser")
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


def _fetch_and_classify(record, excluded):
    """Network + parsing only (thread-safe, no DB access) -- runs in worker threads."""
    url = record["url"]
    try:
        raw = fetch_warc_record(record["filename"], record["offset"], record["length"])
        html = parse_html_record(raw)
    except Exception as e:
        return {"url": url, "ok": False, "error": str(e)}

    if html is None:
        return {"url": url, "ok": False, "error": "no-html-record"}

    soup = BeautifulSoup(html, "html.parser")
    category, engine_name, _signal = classify_engine(html, url, soup=soup)
    links = extract_links_from_html(html, url, soup=soup)
    links = [(t, a) for t, a in links if not is_excluded(domain_of(t), excluded)]

    return {
        "url": url, "ok": True,
        "tld": record.get("url_host_tld"),
        "category": category, "engine_name": engine_name, "links": links,
    }


def run_countries(countries, crawl, total_limit, per_country_limit, priorities_file, db_path,
                   workers, max_parts, exclude_file, candidates_file, commit_every, skip_discovery,
                   rate_limit, max_per_domain, proxy, proxy_file, store_links):
    excluded = load_excluded_domains(exclude_file)
    candidates_file = candidates_file or (db_path + ".candidates.jsonl")
    fetch_mod.rate_limiter.set_rate(rate_limit)
    if proxy_file:
        n = fetch_mod.load_proxy_file(proxy_file)
        print(f"[proxy] rotating across {n} proxies from {proxy_file}")
    elif proxy:
        fetch_mod.set_proxy(proxy)
        print(f"[proxy] routing fetches through {proxy.split('@')[-1] if '@' in proxy else proxy}")

    if per_country_limit:
        budgets = {t: per_country_limit for t in countries}
    else:
        priorities = load_priorities(priorities_file, countries=countries)
        priorities = {t: w for t, w in priorities.items() if t in countries}
        budgets = allocate_budget(priorities, total_limit)
    print(f"[budget] {budgets}")

    if not skip_discovery:
        def progress(msg):
            print(f"[discover] {msg}")

        shortfall = discover_by_countries(
            crawl, budgets, lambda d: is_excluded(d, excluded),
            out_path=candidates_file, max_parts=max_parts, max_per_domain=max_per_domain, progress=progress,
        )
        for tld, remaining in shortfall.items():
            if remaining > 0:
                print(f"[discover] WARNING: {tld} ({country_name(tld)}) short by {remaining} pages "
                      f"-- crawl may not have enough matching pages, or increase --max-parts")
    else:
        print(f"[discover] skipped, reusing {candidates_file}")

    conn = init_db(db_path)
    already = {row[0] for row in conn.execute("SELECT url FROM pages")}
    print(f"[resume] {len(already)} pages already stored, will be skipped")

    def candidate_iter():
        # Shuffled so a rate-limit block mid-run loses a random slice of every
        # country instead of wiping out whichever ccTLDs hadn't been reached yet
        # (discovery writes results in contiguous per-ccTLD blocks).
        for rec in load_candidates_shuffled(candidates_file):
            if rec["url"] not in already:
                yield rec

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
                pending.add(ex.submit(_fetch_and_classify, rec, excluded))
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
                                    tld=res["tld"], country=cname,
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
    p_countries.add_argument("--countries", nargs="+", required=True, help="ccTLDs, e.g. co cl pe ec uy mx ar")
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
    p_countries.add_argument("--no-links", action="store_true",
                              help="Don't store individual outbound links (only their count per page). "
                                   "For engine-market-share analysis the links table isn't needed and is "
                                   "by far the biggest cost: ~100+ link rows/page means ~50GB+ for 1.4M "
                                   "pages, vs a few hundred MB for pages alone")

    args = parser.parse_args()

    if args.mode == "domains":
        run_domains(args.domains, args.crawl, args.limit, args.db, args.delay, args.exclude_file)
    elif args.mode == "countries":
        run_countries(args.countries, args.crawl, args.total_limit, args.per_country_limit,
                       args.priorities, args.db, args.workers, args.max_parts, args.exclude_file,
                       args.candidates_file, args.commit_every, args.skip_discovery, args.rate_limit,
                       args.max_per_domain, args.proxy, args.proxy_file, not args.no_links)


if __name__ == "__main__":
    main()
