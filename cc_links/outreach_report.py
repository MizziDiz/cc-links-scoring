"""Pilot reports and deterministic manual-review samples for outreach data."""

from __future__ import annotations

import csv
import json
import random
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

REVIEW_FIELDS = (
    "registered_domain",
    "url",
    "tld",
    "language",
    "pattern_id",
    "discovery_score",
    "label",
    "reason",
    "notes",
)
ALLOWED_LABELS = {"relevant", "noise", "uncertain"}


def _query_distribution(
    connection: sqlite3.Connection, column: str
) -> list[dict[str, Any]]:
    allowed = {"tld", "language", "best_pattern_id", "status"}
    if column not in allowed:
        raise ValueError(f"unsupported distribution column: {column}")
    return [
        {"value": value, "domains": int(count)}
        for value, count in connection.execute(
            f"SELECT COALESCE({column}, ''), COUNT(*) "
            "FROM outreach_prospects "
            f"GROUP BY {column} ORDER BY COUNT(*) DESC, {column}"
        )
    ]


def build_pilot_report(db_path: str | Path) -> dict[str, Any]:
    """Build a read-only summary of URL evidence and unique domains."""
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        pages = int(
            connection.execute("SELECT COUNT(*) FROM outreach_pages").fetchone()[0]
        )
        domains = int(
            connection.execute("SELECT COUNT(*) FROM outreach_prospects").fetchone()[0]
        )
        max_pages = int(
            connection.execute(
                "SELECT COALESCE(MAX(n), 0) FROM ("
                "SELECT COUNT(*) AS n FROM outreach_pages "
                "GROUP BY registered_domain)"
            ).fetchone()[0]
        )
        return {
            "pages": pages,
            "domains": domains,
            "max_pages_per_domain": max_pages,
            "by_tld": _query_distribution(connection, "tld"),
            "by_language": _query_distribution(connection, "language"),
            "by_pattern": _query_distribution(connection, "best_pattern_id"),
            "by_status": _query_distribution(connection, "status"),
        }
    finally:
        connection.close()


def _stratified_rows(
    rows: Sequence[dict[str, Any]], size: int, seed: int
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (
            str(row.get("tld") or ""),
            str(row.get("language") or ""),
            str(row.get("pattern_id") or ""),
        )
        groups[key].append(row)
    randomizer = random.Random(seed)
    for values in groups.values():
        randomizer.shuffle(values)
    selected: list[dict[str, Any]] = []
    keys = sorted(groups)
    while keys and len(selected) < size:
        next_keys: list[tuple[str, str, str]] = []
        for key in keys:
            values = groups[key]
            if values and len(selected) < size:
                selected.append(values.pop())
            if values:
                next_keys.append(key)
        keys = next_keys
    return selected


def write_review_sample(
    db_path: str | Path,
    out_path: str | Path,
    *,
    size: int = 50,
    seed: int = 42,
) -> int:
    """Write a deterministic domain-level CSV stratified by geo/language/pattern."""
    if size < 1:
        raise ValueError("sample size must be at least one")
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = [
            {
                "registered_domain": row["registered_domain"],
                "url": row["best_url"],
                "tld": row["tld"],
                "language": row["language"],
                "pattern_id": row["best_pattern_id"],
                "discovery_score": row["discovery_score"],
                "label": "",
                "reason": "",
                "notes": "",
            }
            for row in connection.execute(
                "SELECT * FROM outreach_prospects ORDER BY registered_domain"
            )
        ]
    finally:
        connection.close()
    selected = _stratified_rows(rows, min(size, len(rows)), seed)
    target = Path(out_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REVIEW_FIELDS)
        writer.writeheader()
        writer.writerows(selected)
    return len(selected)


def _noise_rate(relevant: int, noise: int) -> float | None:
    decisive = relevant + noise
    return noise / decisive if decisive else None


def evaluate_review_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    required_decisive: int = 40,
    maximum_noise_rate: float = 0.20,
    maximum_pattern_noise_rate: float = 0.30,
) -> dict[str, Any]:
    """Evaluate the documented pilot gate from manually labelled rows."""
    totals = {"relevant": 0, "noise": 0, "uncertain": 0, "unlabelled": 0}
    patterns: dict[str, dict[str, int]] = defaultdict(
        lambda: {"relevant": 0, "noise": 0, "uncertain": 0}
    )
    invalid_labels: list[str] = []
    total_rows = 0
    for row in rows:
        total_rows += 1
        label = str(row.get("label", "")).strip().lower()
        if not label:
            totals["unlabelled"] += 1
            continue
        if label not in ALLOWED_LABELS:
            invalid_labels.append(label)
            continue
        totals[label] += 1
        pattern_id = str(row.get("pattern_id", "")).strip() or "(missing)"
        patterns[pattern_id][label] += 1

    decisive = totals["relevant"] + totals["noise"]
    overall_rate = _noise_rate(totals["relevant"], totals["noise"])
    pattern_results: list[dict[str, Any]] = []
    noisy_patterns: list[str] = []
    for pattern_id in sorted(patterns):
        counts = patterns[pattern_id]
        rate = _noise_rate(counts["relevant"], counts["noise"])
        if rate is not None and rate > maximum_pattern_noise_rate:
            noisy_patterns.append(pattern_id)
        pattern_results.append({"pattern_id": pattern_id, **counts, "noise_rate": rate})

    passed = (
        not invalid_labels
        and decisive >= required_decisive
        and overall_rate is not None
        and overall_rate < maximum_noise_rate
        and not noisy_patterns
    )
    return {
        "rows": total_rows,
        "totals": totals,
        "decisive": decisive,
        "noise_rate": overall_rate,
        "patterns": pattern_results,
        "noisy_patterns": noisy_patterns,
        "invalid_labels": sorted(set(invalid_labels)),
        "gate": {
            "required_decisive": required_decisive,
            "maximum_noise_rate": maximum_noise_rate,
            "maximum_pattern_noise_rate": maximum_pattern_noise_rate,
            "passed": passed,
        },
    }


def evaluate_review_csv(path: str | Path) -> dict[str, Any]:
    """Read a manual-review CSV and evaluate the pilot gate."""
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return evaluate_review_rows(csv.DictReader(handle))


def write_json_report(path: str | Path, report: Mapping[str, Any]) -> None:
    """Write a stable UTF-8 JSON report."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
