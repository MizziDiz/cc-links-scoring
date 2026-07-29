"""CLI wiring for the optional outreach pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

from cc_links.exclusions import is_excluded, load_excluded_domains
from cc_links.outreach_discovery import discover_outreach
from cc_links.outreach_report import (
    build_pilot_report,
    evaluate_review_csv,
    write_json_report,
    write_review_sample,
)
from cc_links.partmap import build_part_map

LOGGER = logging.getLogger(__name__)


def _parse_shard(value: str) -> tuple[int, int]:
    try:
        index_text, count_text = value.split("/", 1)
        index, count = int(index_text), int(count_text)
    except (ValueError, TypeError) as exc:
        raise argparse.ArgumentTypeError("shard must be INDEX/COUNT") from exc
    if count < 1 or not 0 <= index < count:
        raise argparse.ArgumentTypeError("shard must satisfy 0 <= INDEX < COUNT")
    return index, count


def add_outreach_parser(subparsers: Any) -> argparse.ArgumentParser:
    """Register the optional ``pipeline.py outreach`` command tree."""
    parser = subparsers.add_parser(
        "outreach",
        help="Discover and qualify editorial outreach pages (optional pipeline)",
    )
    commands = parser.add_subparsers(dest="outreach_command", required=True)

    partmap = commands.add_parser(
        "partmap", help="Build a resumable url_surtkey range map for one crawl"
    )
    partmap.add_argument("--crawl", required=True)
    partmap.add_argument("--out", required=True)
    partmap.add_argument("--no-resume", action="store_true")
    partmap.add_argument("--max-retries", type=int, default=3)
    partmap.add_argument(
        "--index-source",
        choices=["https", "s3"],
        default="https",
        help="Read Parquet via public HTTPS or the Common Crawl S3 bucket",
    )
    partmap.add_argument(
        "--reconnect-every",
        type=int,
        default=15,
        help="Recycle the DuckDB connection after this many mapped parts",
    )

    discover = commands.add_parser(
        "discover", help="Run URL-only outreach discovery over selected index parts"
    )
    discover.add_argument("--crawl", required=True)
    discover.add_argument("--tlds", nargs="+", required=True)
    discover.add_argument("--out", required=True, help="Final selected JSONL output")
    discover.add_argument("--db", required=True, help="Separate outreach SQLite DB")
    discover.add_argument("--patterns", help="Outreach pattern registry JSON")
    discover.add_argument("--part-map", help="Crawl-specific part-map JSON")
    discover.add_argument("--max-parts", type=int)
    discover.add_argument("--max-per-domain", type=int, default=2)
    discover.add_argument("--shard", type=_parse_shard)
    discover.add_argument("--no-resume", action="store_true")
    discover.add_argument("--max-retries", type=int, default=3)
    discover.add_argument("--retry-backoff", type=float, default=2.0)
    discover.add_argument("--exclude-file", help="Additional excluded domains JSON")

    sample = commands.add_parser(
        "sample", help="Create a stratified CSV for the 50-URL manual pilot audit"
    )
    sample.add_argument("--db", required=True)
    sample.add_argument("--out", required=True)
    sample.add_argument("--size", type=int, default=50)
    sample.add_argument("--seed", type=int, default=42)

    report = commands.add_parser(
        "report", help="Write URL/domain/pattern/geo distributions as JSON"
    )
    report.add_argument("--db", required=True)
    report.add_argument("--out", required=True)

    audit = commands.add_parser(
        "audit", help="Evaluate a manually labelled pilot CSV against quality gates"
    )
    audit.add_argument("--input", required=True)
    audit.add_argument("--out", required=True)
    return parser


def configure_logging() -> None:
    """Configure standard logging from LOG_LEVEL if no handler exists."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def run_outreach_command(args: argparse.Namespace) -> None:
    """Execute one parsed outreach subcommand."""
    configure_logging()
    command = args.outreach_command
    if command == "partmap":
        payload = build_part_map(
            args.crawl,
            args.out,
            index_source=args.index_source,
            resume=not args.no_resume,
            max_retries=args.max_retries,
            reconnect_every=args.reconnect_every,
            progress=LOGGER.info,
        )
        LOGGER.info("part map complete: %d parts", len(payload["parts"]))
        return
    if command == "discover":
        excluded = load_excluded_domains(args.exclude_file)
        summary = discover_outreach(
            crawl=args.crawl,
            tlds=args.tlds,
            out_path=args.out,
            db_path=args.db,
            patterns_path=args.patterns,
            part_map_path=args.part_map,
            max_parts=args.max_parts,
            max_per_domain=args.max_per_domain,
            part_shard=args.shard,
            resume=not args.no_resume,
            max_retries=args.max_retries,
            retry_backoff=args.retry_backoff,
            is_excluded=lambda domain: is_excluded(domain, excluded),
            progress=LOGGER.info,
        )
        LOGGER.info("outreach discovery complete: %s", json.dumps(asdict(summary)))
        return
    if command == "sample":
        count = write_review_sample(args.db, args.out, size=args.size, seed=args.seed)
        LOGGER.info("wrote %d review rows to %s", count, args.out)
        return
    if command == "report":
        write_json_report(args.out, build_pilot_report(args.db))
        LOGGER.info("wrote pilot report to %s", args.out)
        return
    if command == "audit":
        result = evaluate_review_csv(args.input)
        write_json_report(args.out, result)
        LOGGER.info(
            "pilot gate %s; report=%s",
            "PASSED" if result["gate"]["passed"] else "FAILED",
            Path(args.out),
        )
        return
    raise ValueError(f"unknown outreach command: {command}")
