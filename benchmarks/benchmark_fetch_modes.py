"""Compare the threads and async fetch paths on the same candidate manifest."""

import argparse
import asyncio
import json
import os
import platform
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cc_links import fetch as fetch_mod
from cc_links.async_fetch import AsyncFetchSettings, run_async_fetch
from cc_links.cc_index import load_candidates
from cc_links.cdx import get_cdx_records
from cc_links.processing import Candidate, PageResult
from pipeline import _fetch_and_classify


def summarize(
    mode: str,
    records: int,
    ok: int,
    failed: int,
    elapsed: float,
    workers: int,
    cpu_workers: int,
    source: str,
) -> Dict[str, Any]:
    """Return one machine-readable benchmark result."""
    return {
        "mode": mode,
        "source": source,
        "records": records,
        "ok": ok,
        "failed": failed,
        "elapsed_seconds": round(elapsed, 3),
        "records_per_second": round(records / elapsed, 3) if elapsed else None,
        "network_concurrency": workers,
        "cpu_workers": cpu_workers if mode == "async" else None,
        "host": {
            "os": platform.system(),
            "python": platform.python_version(),
            "logical_cpus": os.cpu_count(),
        },
    }


def benchmark_threads(
    records: Sequence[Candidate],
    workers: int,
    rate_limit: float,
    extract_links: bool,
    source: str,
) -> Dict[str, Any]:
    """Run the backwards-compatible threaded fetch path."""
    fetch_mod.rate_limiter = fetch_mod.RateLimiter(rate_limit)
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(
            executor.map(
                lambda record: _fetch_and_classify(
                    record,
                    set(),
                    extract_links,
                ),
                records,
            )
        )
    elapsed = time.perf_counter() - started
    ok = sum(bool(result["ok"]) for result in results)
    return summarize(
        "threads",
        len(records),
        ok,
        len(records) - ok,
        elapsed,
        workers,
        0,
        source,
    )


async def benchmark_async(
    records: Sequence[Candidate],
    workers: int,
    cpu_workers: int,
    rate_limit: float,
    extract_links: bool,
    proxy: Optional[str],
) -> Dict[str, Any]:
    """Run aiohttp downloads feeding the process-based CPU stage."""
    results: List[PageResult] = []

    async def collect(result: PageResult) -> Optional[Tuple[float, float]]:
        results.append(result)
        return None

    started = time.perf_counter()
    await run_async_fetch(
        records,
        set(),
        extract_links,
        AsyncFetchSettings(concurrency=workers, rate_limit=rate_limit),
        cpu_workers,
        collect,
        proxy=proxy,
    )
    elapsed = time.perf_counter() - started
    ok = sum(bool(result["ok"]) for result in results)
    return summarize(
        "async",
        len(records),
        ok,
        len(records) - ok,
        elapsed,
        workers,
        cpu_workers,
        "gateway" if proxy else "cloudfront",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark both WARC fetch modes on an existing candidate JSONL",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--candidates-file")
    source.add_argument(
        "--url-pattern",
        help="Build an in-memory benchmark sample through the Common Crawl CDX API",
    )
    parser.add_argument("--crawl", default="CC-MAIN-2026-25")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--cpu-workers", type=int, default=1)
    parser.add_argument("--rate-limit", type=float, default=60)
    parser.add_argument(
        "--mode",
        choices=["both", "threads", "async"],
        default="both",
    )
    parser.add_argument(
        "--with-links",
        action="store_true",
        help="Also parse and normalize outbound links (default: classify only)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.limit < 1 or args.workers < 1 or args.cpu_workers < 1:
        raise SystemExit("limit, workers, and cpu-workers must be positive")

    if args.candidates_file:
        records = list(islice(load_candidates(args.candidates_file), args.limit))
    else:
        records = [
            record
            for record in get_cdx_records(
                args.url_pattern,
                args.crawl,
                limit=args.limit,
                filters=["status:200", "mime:text/html"],
            )
            if record.get("status") == "200" and "html" in record.get("mime", "")
        ]
        for record in records:
            record.setdefault("bucket", "benchmark")
    if not records:
        raise SystemExit("candidate manifest is empty")

    proxy = os.getenv("CC_GATEWAY_PROXY")
    if proxy:
        fetch_mod.set_proxy(proxy)

    results: List[Dict[str, Any]] = []
    if args.mode in {"both", "threads"}:
        results.append(
            benchmark_threads(
                records,
                args.workers,
                args.rate_limit,
                args.with_links,
                "gateway" if proxy else "cloudfront",
            )
        )
    if args.mode in {"both", "async"}:
        results.append(
            asyncio.run(
                benchmark_async(
                    records,
                    args.workers,
                    args.cpu_workers,
                    args.rate_limit,
                    args.with_links,
                    proxy,
                )
            )
        )

    sys.stdout.write(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
