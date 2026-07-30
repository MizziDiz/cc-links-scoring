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
from cc_links.outreach_enrich import (
    EnrichmentConfig,
    enrich_outreach,
    write_enrichment_report,
)
from cc_links.outreach_live import (
    ValidationConfig,
    qualify_outreach,
    write_qualification_outputs,
)
from cc_links.outreach_report import (
    build_pilot_report,
    evaluate_review_csv,
    write_json_report,
    write_review_sample,
)
from cc_links.outreach_score import build_page_scores, write_score_outputs
from cc_links.outreach_terms import (
    TermsConfig,
    build_placement_terms,
    write_terms_outputs,
)
from cc_links.outreach_value import (
    CostConfig,
    build_value_scores,
    import_domain_metrics,
    import_outcomes,
    write_metrics_template,
    write_value_outputs,
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
    discover.add_argument(
        "--index-source",
        choices=["auto", "https", "s3"],
        default="auto",
        help="DuckDB Parquet source; auto infers it from the selected parts",
    )
    discover.add_argument(
        "--reconnect-every",
        type=int,
        default=15,
        help="Recycle the DuckDB connection after this many completed parts",
    )
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

    qualify = commands.add_parser(
        "qualify",
        help="Run resumable, polite GET-only checks over every discovered page",
    )
    qualify.add_argument("--db", required=True, help="Read-only outreach discovery DB")
    qualify.add_argument(
        "--out-db", required=True, help="Separate checkpoint/result DB"
    )
    qualify.add_argument("--report", required=True, help="Qualification summary JSON")
    qualify.add_argument("--export-dir", help="Status-separated domain CSV directory")
    qualify.add_argument("--workers", type=int, default=20)
    qualify.add_argument("--timeout", type=float, default=15.0)
    qualify.add_argument("--retries", type=int, default=2)
    qualify.add_argument("--retry-backoff", type=float, default=1.0)
    qualify.add_argument("--max-bytes", type=int, default=1_500_000)
    qualify.add_argument("--no-resume", action="store_true")

    enrich = commands.add_parser(
        "enrich",
        help="Extract scoring features from live and archived HTML plus sitemaps",
    )
    enrich.add_argument("--db", required=True, help="Read-only outreach discovery DB")
    enrich.add_argument(
        "--validation-db", required=True, help="Completed live-validation DB"
    )
    enrich.add_argument("--out-db", required=True, help="Separate enrichment DB")
    enrich.add_argument("--report", required=True)
    enrich.add_argument("--export", help="Scoring-ready page CSV")
    enrich.add_argument("--warc-workers", type=int, default=24)
    enrich.add_argument("--live-workers", type=int, default=20)
    enrich.add_argument("--sitemap-workers", type=int, default=20)
    enrich.add_argument("--timeout", type=float, default=15.0)
    enrich.add_argument("--retries", type=int, default=2)
    enrich.add_argument("--retry-backoff", type=float, default=1.0)
    enrich.add_argument("--max-html-bytes", type=int, default=5_000_000)
    enrich.add_argument("--max-sitemap-bytes", type=int, default=5_000_000)
    enrich.add_argument("--max-sitemap-documents", type=int, default=4)
    enrich.add_argument("--fetch-source", choices=["s3", "https"], default="s3")

    score = commands.add_parser(
        "score",
        help="Combine discovery, live, content and freshness evidence",
    )
    score.add_argument("--db", required=True, help="Read-only outreach discovery DB")
    score.add_argument("--validation-db", required=True)
    score.add_argument("--enrichment-db", required=True)
    score.add_argument("--out-db", required=True)
    score.add_argument("--report", required=True)
    score.add_argument("--export", required=True, help="Full scored page CSV")
    score.add_argument("--text-dir", required=True, help="URL-only high/medium/low")
    score.add_argument(
        "--profile",
        choices=["v1", "v2"],
        default="v1",
        help="Versioned score calibration; v1 preserves the original behavior",
    )

    terms = commands.add_parser(
        "terms",
        help="Extract publication promises, placement type and advertised price",
    )
    terms.add_argument("--db", required=True, help="Read-only outreach discovery DB")
    terms.add_argument("--out-db", required=True, help="Separate resumable terms DB")
    terms.add_argument("--workers", type=int, default=24)
    terms.add_argument(
        "--max-pages",
        type=int,
        help="Deterministic pilot prefix; rerun without it to resume the full set",
    )
    terms.add_argument("--max-html-bytes", type=int, default=5_000_000)
    terms.add_argument("--retries", type=int, default=2)
    terms.add_argument("--retry-backoff", type=float, default=1.0)
    terms.add_argument("--fetch-source", choices=["s3", "https"], default="s3")
    terms.add_argument("--report", help="Optional terms summary JSON")
    terms.add_argument("--export", help="Optional full terms CSV")

    metrics = commands.add_parser(
        "metrics",
        help="Import provider-neutral DR/DA/TF/CF/traffic metrics from CSV",
    )
    metrics.add_argument("--input", required=True)
    metrics.add_argument("--out-db", required=True, help="Value SQLite DB")

    metrics_template = commands.add_parser(
        "metrics-template",
        help="Export one blank provider-neutral metrics row per domain",
    )
    metrics_template.add_argument("--scores-db", required=True)
    metrics_template.add_argument("--out", required=True)

    outcomes = commands.add_parser(
        "outcomes",
        help="Import observed contacted/replied/accepted/published outcomes",
    )
    outcomes.add_argument("--input", required=True)
    outcomes.add_argument("--out-db", required=True, help="Value SQLite DB")

    value = commands.add_parser(
        "value",
        help="Estimate placement effectiveness and cost per publication",
    )
    value.add_argument("--scores-db", required=True)
    value.add_argument("--terms-db", required=True)
    value.add_argument("--out-db", required=True)
    value.add_argument(
        "--metrics", help="Optional metrics CSV to import before scoring"
    )
    value.add_argument("--outcomes", help="Optional observed-outcomes CSV to import")
    value.add_argument("--fx", help="CSV: currency,rate_to_base")
    value.add_argument("--contact-cost", type=float, default=0.0)
    value.add_argument("--content-cost", type=float, default=0.0)
    value.add_argument("--base-currency", default="USD")
    value.add_argument("--report", required=True)
    value.add_argument("--export", required=True)
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
            index_source=args.index_source,
            reconnect_every=args.reconnect_every,
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
    if command == "qualify":
        config = ValidationConfig(
            timeout=args.timeout,
            retries=args.retries,
            retry_backoff=args.retry_backoff,
            max_bytes=args.max_bytes,
        )
        summary = qualify_outreach(
            input_db=args.db,
            out_db=args.out_db,
            workers=args.workers,
            config=config,
            resume=not args.no_resume,
            progress=LOGGER.info,
        )
        report = write_qualification_outputs(args.out_db, args.report, args.export_dir)
        LOGGER.info(
            "outreach qualification complete: %s",
            json.dumps(
                {"run": asdict(summary), "report": report},
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
        return
    if command == "enrich":
        config = EnrichmentConfig(
            max_html_bytes=args.max_html_bytes,
            max_sitemap_bytes=args.max_sitemap_bytes,
            timeout=args.timeout,
            retries=args.retries,
            retry_backoff=args.retry_backoff,
            max_sitemap_documents=args.max_sitemap_documents,
        )
        summary = enrich_outreach(
            input_db=args.db,
            validation_db=args.validation_db,
            out_db=args.out_db,
            warc_workers=args.warc_workers,
            live_workers=args.live_workers,
            sitemap_workers=args.sitemap_workers,
            fetch_source=args.fetch_source,
            config=config,
            progress=LOGGER.info,
        )
        report = write_enrichment_report(args.out_db, args.report, args.export)
        LOGGER.info(
            "outreach enrichment complete: %s",
            json.dumps(
                {"run": asdict(summary), "report": report},
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
        return
    if command == "score":
        report = build_page_scores(
            enrichment_db=args.enrichment_db,
            discovery_db=args.db,
            validation_db=args.validation_db,
            out_db=args.out_db,
            profile=args.profile,
        )
        write_score_outputs(
            args.out_db,
            report_path=args.report,
            csv_path=args.export,
            text_dir=args.text_dir,
            report=report,
        )
        LOGGER.info(
            "outreach scoring complete: %s",
            json.dumps(report, ensure_ascii=False, sort_keys=True),
        )
        return
    if command == "terms":
        terms_config = TermsConfig(
            max_html_bytes=args.max_html_bytes,
            retries=args.retries,
            retry_backoff=args.retry_backoff,
        )
        terms_summary = build_placement_terms(
            input_db=args.db,
            out_db=args.out_db,
            workers=args.workers,
            max_pages=args.max_pages,
            fetch_source=args.fetch_source,
            config=terms_config,
            progress=LOGGER.info,
        )
        terms_report = write_terms_outputs(
            args.out_db,
            report_path=args.report,
            csv_path=args.export,
        )
        LOGGER.info(
            "outreach terms extraction complete: %s",
            json.dumps(
                {"run": asdict(terms_summary), "report": terms_report},
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
        return
    if command == "metrics":
        metrics_summary = import_domain_metrics(args.input, args.out_db)
        LOGGER.info(
            "domain metrics import complete: %s",
            json.dumps(metrics_summary, ensure_ascii=False, sort_keys=True),
        )
        return
    if command == "metrics-template":
        domain_count = write_metrics_template(args.scores_db, args.out)
        LOGGER.info("wrote %d metric-template domains to %s", domain_count, args.out)
        return
    if command == "outcomes":
        outcomes_summary = import_outcomes(args.input, args.out_db)
        LOGGER.info(
            "outreach outcomes import complete: %s",
            json.dumps(outcomes_summary, ensure_ascii=False, sort_keys=True),
        )
        return
    if command == "value":
        if args.metrics:
            LOGGER.info(
                "domain metrics import: %s",
                json.dumps(
                    import_domain_metrics(args.metrics, args.out_db),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        if args.outcomes:
            LOGGER.info(
                "outreach outcomes import: %s",
                json.dumps(
                    import_outcomes(args.outcomes, args.out_db),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        cost_config = CostConfig(
            contact_cost=args.contact_cost,
            content_cost=args.content_cost,
            base_currency=args.base_currency,
        )
        value_report = build_value_scores(
            scores_db=args.scores_db,
            terms_db=args.terms_db,
            out_db=args.out_db,
            cost_config=cost_config,
            fx_csv=args.fx,
        )
        write_value_outputs(
            args.out_db,
            report_path=args.report,
            csv_path=args.export,
            report=value_report,
        )
        LOGGER.info(
            "outreach value scoring complete: %s",
            json.dumps(value_report, ensure_ascii=False, sort_keys=True),
        )
        return
    raise ValueError(f"unknown outreach command: {command}")
