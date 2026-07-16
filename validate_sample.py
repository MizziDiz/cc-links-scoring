#!/usr/bin/env python3
"""Read-only live validation for a CSV produced by sample_candidates.py."""
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor

from cc_links.prospects import classify_prospect

USER_AGENT = "Mozilla/5.0 (compatible; cc-prospects-quality-check/1.0)"


def fetch_url(url, timeout):
    import requests
    return requests.get(
        url, timeout=timeout, allow_redirects=True,
        headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"},
    )


def validate(row, timeout, minimum_score):
    result = dict(row)
    try:
        response = fetch_url(row["url"], timeout)
        content_type = response.headers.get("content-type", "")
        result["live_http_status"] = response.status_code
        result["live_final_url"] = response.url
        if "html" not in content_type.lower():
            result["live_family"] = ""
            result["live_platform"] = ""
            result["live_score"] = ""
            result["live_error"] = f"non-html:{content_type[:80]}"
            return result
        # Limit classifier input while retaining the head and normal page body.
        html = response.text[:2_000_000]
        matches = classify_prospect(html, response.url, minimum_score=minimum_score)
        if matches:
            best = matches[0]
            result["live_family"] = best.family
            result["live_platform"] = best.platform or ""
            result["live_score"] = best.score
            result["live_error"] = ""
        else:
            result["live_family"] = ""
            result["live_platform"] = ""
            result["live_score"] = ""
            result["live_error"] = "no-current-match"
    except Exception as exc:
        result["live_http_status"] = ""
        result["live_final_url"] = ""
        result["live_family"] = ""
        result["live_platform"] = ""
        result["live_score"] = ""
        result["live_error"] = f"{type(exc).__name__}: {exc}"[:300]
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--min-score", type=int, default=50)
    args = parser.parse_args()

    with open(args.input, encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source))
        input_columns = list(rows[0].keys()) if rows else []
    live_columns = ["live_http_status", "live_final_url", "live_family",
                    "live_platform", "live_score", "live_error"]
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(
            lambda row: validate(row, args.timeout, args.min_score), rows))
    with open(args.out, "w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=input_columns + live_columns)
        writer.writeheader()
        writer.writerows(results)
    live = sum(1 for row in results if str(row.get("live_http_status", "")).startswith("2"))
    matched = sum(1 for row in results if row.get("live_family"))
    print(f"validated={len(results)} live_2xx={live} current_match={matched} -> {args.out}")


if __name__ == "__main__":
    main()
