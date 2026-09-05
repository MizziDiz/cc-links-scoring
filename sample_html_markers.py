#!/usr/bin/env python3
"""Sample real pages from Common Crawl and report which HTML markers they carry.

Use it to turn a URL pattern into a classifier rule with evidence, or to find
out why pages a pattern discovers are rejected by the classifier:

    # Why are pages discovered by guestbook:0 rejected? Sample 300 of them.
    python sample_html_markers.py --db prospects.db --state-dir crawl_states \
        --group guestbook:0 --group broad:/profile/ --per-group 300 --source s3 \
        --markers markers.json --out markers-report.json

    # Which markers do pages matching a proposed discovery clause carry?
    python sample_html_markers.py --state-dir crawl_states \
        --clause "home.php?mod=space" --clause "subaction=getlist" --per-group 200

``--group`` takes pattern ids from ``processed_urls`` (outcome unmatched by
default); ``--clause`` takes URL substrings (all must match) looked up straight
in the discovery manifests. Every page is fetched once, read-only, from S3 or
CloudFront; nothing is written to the database.

The report lists, per group: meta generator values, "powered by" phrases,
form action and input names, frequent class/id tokens, title words, the share
of pages the current taxonomy already classifies, and the hit rate of every
candidate marker from ``--markers`` (JSON: {"marker name": ["substr", ...]}).
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import random
import re
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from cc_links import fetch as fetch_mod
from cc_links.fetch import fetch_warc_record, parse_html_record
from cc_links.prospects import classify_prospect, normalize_url

GENERATOR_RE = re.compile(r'<meta[^>]+name=["\']generator["\'][^>]+content=["\']([^"\']{1,80})', re.I)
GENERATOR_RE2 = re.compile(r'<meta[^>]+content=["\']([^"\']{1,80})["\'][^>]+name=["\']generator["\']', re.I)
POWERED_RE = re.compile(r'powered\s+by\s*(?:<[^>]+>\s*)*([A-Za-z][A-Za-z0-9 .\-_]{1,30})', re.I)
TITLE_RE = re.compile(r'<title[^>]*>(.{0,200}?)</title>', re.I | re.S)
FORM_ACTION_RE = re.compile(r'<form[^>]+action=["\']([^"\']{1,120})', re.I)
INPUT_NAME_RE = re.compile(r'<(?:input|textarea|select)[^>]+name=["\']([^"\']{1,40})', re.I)
CLASS_RE = re.compile(r'\s(?:class|id)=["\']([^"\']{1,120})', re.I)
SCRIPT_RE = re.compile(r'<(?:script|link)[^>]+(?:src|href)=["\']([^"\']{1,160})', re.I)
WORD_RE = re.compile(r"[a-zA-Zа-яА-ЯёЁÀ-ɏͰ-Ͽ぀-ヿ一-鿿가-힯]{3,}")


def collect_group_urls(db: str, group: str, outcome: str, since: str | None,
                       per_group: int) -> list[str]:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        where = "outcome = ? AND pattern_id = ?"
        params: list[Any] = [outcome, group]
        if since:
            where += " AND processed_at >= ?"
            params.append(since)
        rows = conn.execute(
            f"SELECT url, registered_domain FROM processed_urls WHERE {where}", params).fetchall()
    finally:
        conn.close()
    random.Random(7).shuffle(rows)
    seen_domains: set[str] = set()
    urls = []
    for url, domain in rows:            # one URL per domain first, then fill
        if domain in seen_domains:
            continue
        seen_domains.add(domain)
        urls.append(url)
        if len(urls) >= per_group:
            break
    if len(urls) < per_group:
        for url, _ in rows:
            if url not in urls:
                urls.append(url)
                if len(urls) >= per_group:
                    break
    return urls


def manifest_lookup(state_dir: str, wanted: dict[str, str], clauses: list[tuple[str, ...]],
                    per_group: int) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    """Scan manifests once: resolve wanted URLs and collect clause matches."""
    resolved: dict[str, dict] = {}
    clause_hits: dict[str, list[dict]] = {"|".join(c): [] for c in clauses}
    clause_domains: dict[str, set] = {"|".join(c): set() for c in clauses}
    paths = sorted(glob.glob(os.path.join(state_dir, "*.jsonl")))
    paths = [p for p in paths if ".shard-" not in os.path.basename(p)] or paths
    for path in paths:
        with open(path, encoding="utf-8") as source:
            for line in source:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                url = record.get("url", "")
                normalized = normalize_url(url)
                if normalized in wanted and normalized not in resolved:
                    resolved[normalized] = record
                if clauses:
                    lower = url.lower()
                    for clause in clauses:
                        key = "|".join(clause)
                        if len(clause_hits[key]) >= per_group * 3:
                            continue
                        if all(term in lower for term in clause):
                            domain = record.get("url_host_registered_domain") or ""
                            if domain in clause_domains[key]:
                                continue
                            clause_domains[key].add(domain)
                            clause_hits[key].append(record)
        if len(resolved) >= len(wanted) and all(
                len(v) >= per_group * 3 for v in clause_hits.values()):
            break
    return resolved, clause_hits


def page_features(html: str, url: str, markers: dict[str, list[str]]) -> dict[str, Any]:
    lower = html.lower()
    generator = (GENERATOR_RE.search(html) or GENERATOR_RE2.search(html))
    title = TITLE_RE.search(html)
    classes: set[str] = set()
    for blob in CLASS_RE.findall(html):
        for token in blob.split():
            if 3 <= len(token) <= 40 and not token.isdigit():
                classes.add(token.lower())
    scripts = {os.path.basename(s.split("?")[0]).lower() for s in SCRIPT_RE.findall(html)}
    scripts = {s for s in scripts if s and len(s) <= 60}
    return {
        "generator": (generator.group(1).strip().lower()[:60] if generator else None),
        "powered": [p.strip().lower()[:40] for p in POWERED_RE.findall(html)[:3]],
        "title_words": set(w.lower() for w in WORD_RE.findall(title.group(1))[:20]) if title else set(),
        "form_actions": {os.path.basename(a.split("?")[0]).lower() or a.lower()[:40]
                         for a in FORM_ACTION_RE.findall(html)},
        "input_names": {n.lower() for n in INPUT_NAME_RE.findall(html)},
        "classes": classes,
        "scripts": scripts,
        "markers": {name: any(s.lower() in lower for s in subs) for name, subs in markers.items()},
        "classified": bool(classify_prospect(html, url)),
        "has_form": "<form" in lower,
        "has_textarea": "<textarea" in lower,
    }


def summarize(features: list[dict[str, Any]], top: int = 40) -> dict[str, Any]:
    n = max(1, len(features))

    def df(field: str) -> list[tuple[str, int]]:
        counter: collections.Counter = collections.Counter()
        for f in features:
            for value in f[field]:
                counter[value] += 1
        return [(k, v) for k, v in counter.most_common(top) if v >= max(3, n * 0.03)]

    generators = collections.Counter(f["generator"] for f in features if f["generator"])
    powered = collections.Counter(p for f in features for p in f["powered"])
    marker_hits = collections.Counter()
    for f in features:
        for name, hit in f["markers"].items():
            if hit:
                marker_hits[name] += 1
    return {
        "pages": len(features),
        "already_classified": sum(f["classified"] for f in features),
        "with_form": sum(f["has_form"] for f in features),
        "with_textarea": sum(f["has_textarea"] for f in features),
        "generators": generators.most_common(25),
        "powered_by": powered.most_common(25),
        "title_words": df("title_words"),
        "form_actions": df("form_actions"),
        "input_names": df("input_names"),
        "classes": df("classes"),
        "scripts": df("scripts"),
        "marker_hit_rate": {name: round(marker_hits[name] / n, 3) for name in
                            sorted(set(k for f in features for k in f["markers"]))},
    }


def fetch_one(record: dict, markers: dict[str, list[str]]) -> dict[str, Any] | None:
    try:
        raw = fetch_warc_record(record["filename"], record["offset"], record["length"])
        html = parse_html_record(raw)
    except Exception as exc:  # noqa: BLE001 - one bad page must not stop the sample
        return {"error": f"{type(exc).__name__}: {exc}"}
    if not html:
        return {"error": "no-html"}
    features = page_features(html, record["url"], markers)
    features["url"] = record["url"]
    return features


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", help="prospects.db for --group lookups (read-only)")
    parser.add_argument("--state-dir", required=True, help="directory with discovery manifests (*.jsonl)")
    parser.add_argument("--group", action="append", default=[], help="pattern_id from processed_urls")
    parser.add_argument("--outcome", default="unmatched")
    parser.add_argument("--clause", action="append", default=[],
                        help="URL substrings, '+'-separated, all must match, e.g. 'home.php+mod=space'")
    parser.add_argument("--since")
    parser.add_argument("--per-group", type=int, default=200)
    parser.add_argument("--source", choices=["cloudfront", "s3"], default="cloudfront")
    parser.add_argument("--rate-limit", type=float, default=10)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--markers", help="JSON {name: [substrings]} of candidate markers to test")
    parser.add_argument("--out", help="JSON report path")
    args = parser.parse_args()
    if not args.group and not args.clause:
        parser.error("give --group and/or --clause")
    if args.group and not args.db:
        parser.error("--group needs --db")
    markers: dict[str, list[str]] = {}
    if args.markers:
        with open(args.markers, encoding="utf-8") as source:
            markers = json.load(source)

    wanted: dict[str, str] = {}
    group_urls: dict[str, list[str]] = {}
    for group in args.group:
        urls = collect_group_urls(args.db, group, args.outcome, args.since, args.per_group)
        group_urls[group] = urls
        for url in urls:
            wanted[normalize_url(url)] = group
        print(f"[group] {group}: {len(urls)} URLs selected", flush=True)
    clauses = [tuple(t.strip().lower() for t in c.split("+") if t.strip()) for c in args.clause]
    resolved, clause_hits = manifest_lookup(args.state_dir, wanted, clauses, args.per_group)
    print(f"[manifest] resolved {len(resolved)}/{len(wanted)} group URLs; "
          + ", ".join(f"{k}: {len(v)}" for k, v in clause_hits.items()), flush=True)

    fetch_mod.rate_limiter.set_rate(args.rate_limit)
    if args.source == "s3":
        fetch_mod.enable_s3(pool_size=max(args.workers * 2, 16))

    jobs: dict[str, list[dict]] = collections.defaultdict(list)
    for normalized, record in resolved.items():
        jobs[wanted[normalized]].append(record)
    for key, records in clause_hits.items():
        random.Random(11).shuffle(records)
        jobs["clause:" + key] = records[:args.per_group]

    report: dict[str, Any] = {"groups": {}}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for name, records in jobs.items():
            results = list(pool.map(lambda r: fetch_one(r, markers), records))
            features = [r for r in results if r and "error" not in r]
            errors = collections.Counter(r["error"].split(":")[0] for r in results if r and "error" in r)
            summary = summarize(features)
            summary["errors"] = dict(errors)
            summary["sample_urls"] = [f["url"] for f in features[:15]]
            report["groups"][name] = summary
            print(f"\n=== {name}: {summary['pages']} pages, already classified "
                  f"{summary['already_classified']}, with form {summary['with_form']}, "
                  f"errors {dict(errors)}")
            print("  generators:", summary["generators"][:10])
            print("  powered by:", summary["powered_by"][:10])
            print("  form actions:", summary["form_actions"][:12])
            print("  input names:", summary["input_names"][:15])
            print("  classes/ids:", summary["classes"][:25])
            print("  scripts:", summary["scripts"][:12])
            print("  title words:", summary["title_words"][:20])
            if markers:
                print("  marker hit rate:", summary["marker_hit_rate"])
            sys.stdout.flush()
    if args.out:
        with open(args.out, "w", encoding="utf-8") as output:
            json.dump(report, output, ensure_ascii=False, indent=1)
        print(f"\nreport written to {args.out}")


if __name__ == "__main__":
    main()
