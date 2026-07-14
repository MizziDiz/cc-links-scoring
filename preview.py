#!/usr/bin/env python3
"""Preview the actual page content behind discovery candidates *before* a run
collects them -- fetch the WARC record, show how the classifier reads it, and
optionally dump the full HTML so you can open it in a browser.

  python preview.py                       # 3 random candidates
  python preview.py -n 5                   # 5 random candidates
  python preview.py --bucket "🇷🇺 Россия"  # random pages from one category
  python preview.py --url https://site/x   # a specific candidate URL
  python preview.py -n 3 --save            # also write preview_1.html ... to open
  python preview.py --source s3            # fetch from S3 (on EC2) instead of CloudFront

By default it reads latam.db.candidates.jsonl; override with --candidates-file.
"""
import argparse
import random

from cc_links import fetch as fetch_mod
from cc_links.fetch import fetch_warc_record, parse_html_record, make_soup, load_proxy_file
from cc_links.engines import classify_engine, get_generator
from cc_links.cc_index import load_candidates


def pick(candidates_file, url, bucket, n):
    if url:
        for rec in load_candidates(candidates_file):
            if rec["url"] == url:
                return [rec]
        raise SystemExit(f"URL not found in {candidates_file}: {url}")
    recs = []
    for rec in load_candidates(candidates_file):
        if bucket and rec.get("bucket") != bucket:
            continue
        recs.append(rec)
        if len(recs) >= 20000:  # cap the scan, then sample from what we have
            break
    if not recs:
        raise SystemExit("no matching candidates")
    return random.sample(recs, min(n, len(recs)))


def visible_snippet(soup, limit=400):
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()
    text = " ".join(soup.get_text(" ").split())
    return text[:limit]


def main():
    ap = argparse.ArgumentParser(description="Preview page content behind discovery candidates.")
    ap.add_argument("--candidates-file", default="latam.db.candidates.jsonl")
    ap.add_argument("--url", help="preview one specific candidate URL")
    ap.add_argument("--bucket", help="only preview pages from this category")
    ap.add_argument("-n", type=int, default=3, help="how many random pages to preview")
    ap.add_argument("--save", action="store_true", help="also write the full HTML to preview_N.html")
    ap.add_argument("--source", choices=["cloudfront", "s3"], default="cloudfront")
    ap.add_argument("--proxy", help="route fetches through a proxy / rotating gateway URL")
    ap.add_argument("--proxy-file", help="rotate fetches across a proxy pool file")
    args = ap.parse_args()

    if args.source == "s3":
        fetch_mod.enable_s3()
    elif args.proxy_file:
        load_proxy_file(args.proxy_file)
    elif args.proxy:
        fetch_mod.set_proxy(args.proxy)

    for i, rec in enumerate(pick(args.candidates_file, args.url, args.bucket, args.n), 1):
        print("=" * 78)
        print(f"[{i}] {rec['url']}")
        print(f"    ccTLD {rec.get('url_host_tld')} · category {rec.get('bucket')} · "
              f"domain {rec.get('url_host_registered_domain')}")
        try:
            raw = fetch_warc_record(rec["filename"], int(rec["offset"]), int(rec["length"]))
            html = parse_html_record(raw)
        except Exception as e:
            print(f"    fetch failed: {e}")
            continue
        if html is None:
            print("    no HTML response record (redirect / non-HTML)")
            continue

        soup = make_soup(html)
        category, engine, signal = classify_engine(html, rec["url"], soup=soup)
        title = (soup.title.get_text(strip=True) if soup.title else "")[:90]
        gen = get_generator(html)[:60]
        print(f"    classified : {category or '—'} / {engine or 'unclassified'}"
              + (f"   (matched {signal})" if signal else ""))
        print(f"    <title>    : {title}")
        if gen:
            print(f"    generator  : {gen}")
        print(f"    html bytes : {len(html)}")
        print(f"    text       : {visible_snippet(soup)}")
        if args.save:
            path = f"preview_{i}.html"
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"    saved      : {path}  (open in a browser)")


if __name__ == "__main__":
    main()
