#!/usr/bin/env python3
"""Build a read-only baseline report for the Common Crawl prospect pipeline."""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from cc_links.prospects import normalize_url


def _read_only_connection(path: str) -> sqlite3.Connection:
    database = Path(path).resolve().as_posix()
    return sqlite3.connect(f"file:{database}?mode=ro", uri=True)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def _rows_as_dicts(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def collect_database_metrics(path: str) -> dict[str, Any]:
    """Return candidate, domain, score and processing metrics without mutating DB."""
    conn = _read_only_connection(path)
    try:
        if not _table_exists(conn, "candidates"):
            raise ValueError(f"{path} does not contain a candidates table")

        summary_cursor = conn.execute(
            """
            SELECT COUNT(*) AS candidates,
                   COUNT(DISTINCT registered_domain) AS domains,
                   ROUND(AVG(score), 2) AS avg_score,
                   SUM(CASE WHEN score >= 70 THEN 1 ELSE 0 END) AS high_confidence
            FROM candidates
            """
        )
        summary = _rows_as_dicts(summary_cursor)[0]
        summary["max_urls_per_domain"] = conn.execute(
            """
            SELECT COALESCE(MAX(candidate_count), 0)
            FROM (
                SELECT COUNT(*) AS candidate_count
                FROM candidates
                GROUP BY COALESCE(NULLIF(registered_domain, ''),
                                  NULLIF(domain, ''), normalized_url)
            )
            """
        ).fetchone()[0]

        score_bands = _rows_as_dicts(conn.execute(
            """
            SELECT CASE
                     WHEN score >= 70 THEN '70+'
                     WHEN score >= 50 THEN '50-69'
                     WHEN score >= 30 THEN '30-49'
                     ELSE '0-29'
                   END AS score_band,
                   COUNT(*) AS candidates,
                   COUNT(DISTINCT registered_domain) AS domains
            FROM candidates
            GROUP BY score_band
            ORDER BY MIN(score) DESC
            """
        ))
        families = _rows_as_dicts(conn.execute(
            """
            SELECT family, COUNT(*) AS candidates,
                   COUNT(DISTINCT registered_domain) AS domains,
                   ROUND(AVG(score), 2) AS avg_score
            FROM candidates
            GROUP BY family
            ORDER BY candidates DESC
            """
        ))
        countries = _rows_as_dicts(conn.execute(
            """
            SELECT country, COUNT(*) AS candidates,
                   COUNT(DISTINCT registered_domain) AS domains,
                   ROUND(AVG(score), 2) AS avg_score
            FROM candidates
            GROUP BY country
            ORDER BY candidates DESC
            """
        ))
        crawls = _rows_as_dicts(conn.execute(
            """
            SELECT crawl, COUNT(*) AS candidates,
                   COUNT(DISTINCT registered_domain) AS domains
            FROM candidates
            GROUP BY crawl
            ORDER BY candidates DESC
            """
        ))

        outcomes: dict[str, int] = {}
        if _table_exists(conn, "processed_urls"):
            outcomes = {
                str(outcome): int(count)
                for outcome, count in conn.execute(
                    """
                    SELECT outcome, COUNT(*)
                    FROM processed_urls
                    GROUP BY outcome
                    ORDER BY COUNT(*) DESC
                    """
                )
            }
        processed_total = sum(outcomes.values())
        qualified = outcomes.get("stored", 0) + outcomes.get("domain_cap", 0)
        processing = {
            "processed_urls": processed_total,
            "outcomes": outcomes,
            "qualified_decisions": qualified,
            "unmatched_decisions": outcomes.get("unmatched", 0),
            "qualified_rate": (
                round(qualified / processed_total, 6) if processed_total else None
            ),
        }
        return {
            "path": str(Path(path).resolve()),
            "summary": summary,
            "score_bands": score_bands,
            "families": families,
            "countries": countries,
            "crawls": crawls,
            "processing": processing,
        }
    finally:
        conn.close()


def collect_manifest_metrics(paths: Iterable[str]) -> dict[str, Any]:
    """Summarize discovery JSONL manifests, including exact/broad tiers."""
    tiers: Counter[str] = Counter()
    buckets: Counter[str] = Counter()
    patterns: Counter[str] = Counter()
    unique_urls: set[str] = set()
    unique_domains: set[str] = set()
    records = invalid = 0

    for raw_path in paths:
        path = Path(raw_path)
        with path.open(encoding="utf-8") as source:
            for line in source:
                try:
                    record = json.loads(line)
                    url = str(record["url"])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    invalid += 1
                    continue
                records += 1
                unique_urls.add(normalize_url(url))
                domain = record.get("url_host_registered_domain")
                if domain:
                    unique_domains.add(str(domain).lower())
                tiers[str(record.get("discovery_tier", 0))] += 1
                if record.get("bucket"):
                    buckets[str(record["bucket"])] += 1
                if record.get("pattern_id"):
                    patterns[str(record["pattern_id"])] += 1

    return {
        "files": [str(Path(path).resolve()) for path in paths],
        "records": records,
        "invalid_records": invalid,
        "unique_urls": len(unique_urls),
        "unique_domains": len(unique_domains),
        "tiers": dict(sorted(tiers.items())),
        "buckets": dict(buckets.most_common()),
        "patterns": dict(patterns.most_common()),
    }


def collect_checkpoint_metrics(state_dir: str) -> dict[str, Any]:
    """Aggregate resumable discovery checkpoint progress and optional counters."""
    paths = sorted(Path(state_dir).glob("*.state.json"))
    scanned_parts = expected_parts = 0
    counters: Counter[str] = Counter()
    complete = 0

    for path in paths:
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            counters["invalid_state_files"] += 1
            continue
        scanned = len(set(state.get("scanned_parts", [])))
        expected = int(state.get("allowed_parts_count") or 0)
        scanned_parts += scanned
        expected_parts += expected
        remaining = state.get("remaining", {})
        if ((expected and scanned >= expected)
                or (remaining and all(int(value) <= 0 for value in remaining.values()))):
            complete += 1
        for name, value in (state.get("metrics") or {}).items():
            if isinstance(value, (int, float)):
                counters[name] += value

    metrics: dict[str, Any] = {
        "state_dir": str(Path(state_dir).resolve()),
        "state_files": len(paths),
        "complete_state_files": complete,
        "scanned_parts": scanned_parts,
        "expected_parts": expected_parts,
        "completion_rate": (
            round(scanned_parts / expected_parts, 6) if expected_parts else None
        ),
        "counters": dict(counters),
    }
    index_rows = counters.get("index_rows_scanned", 0)
    written = counters.get("candidates_written", 0)
    if index_rows:
        metrics["candidates_per_million_index_rows"] = round(
            written * 1_000_000 / index_rows, 6
        )
    return metrics


def collect_validation_metrics(path: str) -> dict[str, Any]:
    """Summarize a manual/live validation CSV when one is available."""
    with Path(path).open(encoding="utf-8-sig", newline="") as source:
        rows = list(csv.DictReader(source))

    live_2xx = sum(
        1 for row in rows if str(row.get("live_http_status", "")).startswith("2")
    )
    current_match = sum(1 for row in rows if row.get("live_family"))
    comparable = [
        row for row in rows if row.get("family") and row.get("live_family")
    ]
    family_agreement = sum(
        1 for row in comparable if row["family"] == row["live_family"]
    )
    verdicts = Counter(
        row.get("verdict", "").strip().lower()
        for row in rows
        if row.get("verdict", "").strip()
    )
    return {
        "path": str(Path(path).resolve()),
        "rows": len(rows),
        "live_2xx": live_2xx,
        "live_2xx_rate": round(live_2xx / len(rows), 6) if rows else None,
        "current_match": current_match,
        "current_match_rate": (
            round(current_match / len(rows), 6) if rows else None
        ),
        "family_comparable": len(comparable),
        "family_agreement": family_agreement,
        "family_agreement_rate": (
            round(family_agreement / len(comparable), 6) if comparable else None
        ),
        "verdicts": dict(verdicts),
    }


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    report: dict[str, Any] = {"database": collect_database_metrics(args.db)}
    if args.manifest:
        report["manifest"] = collect_manifest_metrics(args.manifest)
    if args.state_dir:
        report["checkpoints"] = collect_checkpoint_metrics(args.state_dir)
    if args.validation_csv:
        report["validation"] = collect_validation_metrics(args.validation_csv)
    return report


def print_report(report: dict[str, Any]) -> None:
    database = report["database"]
    summary = database["summary"]
    processing = database["processing"]
    print("== COMMON CRAWL PROSPECT BASELINE ==")
    print(
        f"candidates={summary['candidates']} domains={summary['domains']} "
        f"avg_score={summary['avg_score']} high_confidence={summary['high_confidence']} "
        f"max_url/domain={summary['max_urls_per_domain']}"
    )
    print(
        f"processed={processing['processed_urls']} "
        f"qualified={processing['qualified_decisions']} "
        f"unmatched={processing['unmatched_decisions']} "
        f"qualified_rate={processing['qualified_rate']}"
    )
    print("families:")
    for row in database["families"]:
        print(
            f"  {row['family']:<20} candidates={row['candidates']:<8} "
            f"domains={row['domains']:<7} avg_score={row['avg_score']}"
        )

    manifest = report.get("manifest")
    if manifest:
        print(
            f"manifest: records={manifest['records']} unique_urls={manifest['unique_urls']} "
            f"domains={manifest['unique_domains']} tiers={manifest['tiers']}"
        )
    checkpoints = report.get("checkpoints")
    if checkpoints:
        print(
            f"checkpoints: files={checkpoints['state_files']} "
            f"complete={checkpoints['complete_state_files']} "
            f"parts={checkpoints['scanned_parts']}/{checkpoints['expected_parts']}"
        )
        if checkpoints.get("candidates_per_million_index_rows") is not None:
            print(
                "discovery_yield="
                f"{checkpoints['candidates_per_million_index_rows']} candidates/M rows"
            )
    validation = report.get("validation")
    if validation:
        print(
            f"validation: rows={validation['rows']} live_2xx={validation['live_2xx']} "
            f"current_match={validation['current_match']} "
            f"family_agreement={validation['family_agreement_rate']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only baseline metrics for a prospect run"
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--manifest", action="append", default=[])
    parser.add_argument("--state-dir")
    parser.add_argument("--validation-csv")
    parser.add_argument("--json-out")
    args = parser.parse_args()
    report = build_report(args)
    print_report(report)
    if args.json_out:
        destination = Path(args.json_out)
        destination.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"json={destination.resolve()}")


if __name__ == "__main__":
    main()
