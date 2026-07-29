"""Read-only pattern feedback and optional prefetch-priority adjustments."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from cc_links.prospects import classify_discovery_url, normalize_url

QUALIFIED_OUTCOMES = ("stored", "domain_cap")
DECISION_OUTCOMES = ("stored", "domain_cap", "unmatched", "below_threshold")


def _read_only_connection(path: str) -> sqlite3.Connection:
    database = Path(path).resolve().as_posix()
    return sqlite3.connect(f"file:{database}?mode=ro", uri=True)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _bounded_adjustment(
        qualified: int,
        decisions: int,
        baseline_rate: float,
        minimum_samples: int,
        prior_strength: float,
) -> int:
    if decisions < minimum_samples:
        return 0
    posterior = (
        qualified + baseline_rate * prior_strength
    ) / (decisions + prior_strength)
    confidence = min(1.0, decisions / max(minimum_samples * 4, 1))
    return max(-15, min(15, round((posterior - baseline_rate) * 50 * confidence)))


def collect_pattern_feedback(
        path: str,
        minimum_samples: int = 20,
        prior_strength: float = 20.0,
        manifest_paths: Iterable[str] | None = None) -> dict[str, Any]:
    """Measure qualified yield by discovery pattern, tier and geography."""
    conn = _read_only_connection(path)
    try:
        manifest_paths = list(manifest_paths or [])
        columns = _table_columns(conn, "processed_urls")
        required = {"normalized_url", "outcome", "score"}
        missing = sorted(required - columns)
        if missing:
            raise ValueError(
                "processed_urls lacks feedback attribution columns: "
                + ", ".join(missing)
            )
        if manifest_paths:
            conn.execute(
                """
                CREATE TEMP TABLE manifest_attribution (
                    normalized_url TEXT PRIMARY KEY,
                    registered_domain TEXT,
                    bucket TEXT,
                    discovery_tier INTEGER,
                    pattern_id TEXT
                )
                """
            )
            pending = []
            for raw_path in manifest_paths:
                with Path(raw_path).open(encoding="utf-8") as source:
                    for line in source:
                        try:
                            record = json.loads(line)
                            normalized = normalize_url(str(record["url"]))
                        except (
                            KeyError, TypeError, ValueError, json.JSONDecodeError
                        ):
                            continue
                        pattern_id = record.get("pattern_id")
                        if not pattern_id:
                            evidence = classify_discovery_url(
                                str(record["url"]), include_broad=True
                            )
                            if evidence:
                                pattern_id = evidence[0].pattern_id
                        pending.append((
                            normalized,
                            record.get("url_host_registered_domain"),
                            record.get("bucket"),
                            record.get("discovery_tier"),
                            pattern_id,
                        ))
                        if len(pending) >= 1_000:
                            conn.executemany(
                                """INSERT OR REPLACE INTO manifest_attribution
                                   VALUES (?, ?, ?, ?, ?)""",
                                pending,
                            )
                            pending.clear()
            if pending:
                conn.executemany(
                    """INSERT OR REPLACE INTO manifest_attribution
                       VALUES (?, ?, ?, ?, ?)""",
                    pending,
                )

        def attributed(column: str, fallback: str) -> str:
            processed = (
                f"NULLIF(p.{column}, '')"
                if column in columns else "NULL"
            )
            manifest = (
                f"m.{column}" if manifest_paths else "NULL"
            )
            return f"COALESCE({processed}, {manifest}, {fallback})"

        pattern_expr = attributed("pattern_id", "''")
        tier_expr = attributed("discovery_tier", "-1")
        bucket_expr = attributed("bucket", "''")
        domain_expr = attributed("registered_domain", "''")
        manifest_join = (
            """LEFT JOIN manifest_attribution m
               ON m.normalized_url = p.normalized_url"""
            if manifest_paths else ""
        )

        placeholders = ", ".join("?" for _ in DECISION_OUTCOMES)
        total, qualified = conn.execute(
            f"""
            SELECT COUNT(*),
                   SUM(CASE WHEN p.outcome IN (?, ?) THEN 1 ELSE 0 END)
            FROM processed_urls p
            {manifest_join}
            WHERE p.outcome IN ({placeholders})
              AND {pattern_expr} != ''
            """,
            (*QUALIFIED_OUTCOMES, *DECISION_OUTCOMES),
        ).fetchone()
        total = int(total or 0)
        qualified = int(qualified or 0)
        baseline_rate = qualified / total if total else 0.5

        rows = conn.execute(
            f"""
            SELECT {pattern_expr} AS pattern_id,
                   {tier_expr} AS discovery_tier,
                   {bucket_expr} AS bucket,
                   COUNT(*) AS decisions,
                   SUM(CASE WHEN p.outcome IN (?, ?) THEN 1 ELSE 0 END)
                       AS qualified,
                   SUM(CASE WHEN p.outcome = 'unmatched' THEN 1 ELSE 0 END)
                       AS unmatched,
                   SUM(CASE WHEN p.outcome = 'below_threshold' THEN 1 ELSE 0 END)
                       AS below_threshold,
                   COUNT(DISTINCT NULLIF({domain_expr}, ''))
                       AS unique_domains,
                   ROUND(AVG(p.score), 2) AS avg_score
            FROM processed_urls p
            {manifest_join}
            WHERE p.outcome IN ({placeholders})
              AND {pattern_expr} != ''
            GROUP BY 1, 2, 3
            ORDER BY decisions DESC, pattern_id, bucket
            """,
            (*QUALIFIED_OUTCOMES, *DECISION_OUTCOMES),
        ).fetchall()

        grouped: dict[str, dict[str, Any]] = {}
        by_bucket: dict[str, dict[str, Any]] = {}
        aggregate: dict[str, list[int]] = {}
        for (pattern_id, tier, bucket, decisions, good, unmatched,
             below_threshold, domains, avg_score) in rows:
            pattern_id = str(pattern_id)
            decisions = int(decisions)
            good = int(good or 0)
            entry = {
                "pattern_id": pattern_id,
                "discovery_tier": int(tier),
                "bucket": str(bucket),
                "decisions": decisions,
                "qualified": good,
                "qualified_rate": round(good / decisions, 6),
                "unmatched": int(unmatched or 0),
                "below_threshold": int(below_threshold or 0),
                "unique_domains": int(domains or 0),
                "avg_score": avg_score,
            }
            by_bucket[f"{pattern_id}|{bucket}"] = entry
            totals = aggregate.setdefault(pattern_id, [0, 0, 0])
            totals[0] += decisions
            totals[1] += good
            totals[2] += int(domains or 0)

        for pattern_id, (decisions, good, domains) in aggregate.items():
            adjustment = _bounded_adjustment(
                good, decisions, baseline_rate, minimum_samples, prior_strength
            )
            grouped[pattern_id] = {
                "decisions": decisions,
                "qualified": good,
                "qualified_rate": round(good / decisions, 6),
                "unique_domain_observations": domains,
                "score_adjustment": adjustment,
                "exploration": decisions < minimum_samples,
            }

        for key, entry in by_bucket.items():
            entry["score_adjustment"] = _bounded_adjustment(
                entry["qualified"], entry["decisions"], baseline_rate,
                minimum_samples, prior_strength,
            )
            entry["exploration"] = entry["decisions"] < minimum_samples

        unresolved_errors = 0
        retry_attempts = 0
        if _table_columns(conn, "fetch_attempts"):
            unresolved_errors, retry_attempts = conn.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(attempts), 0)
                FROM fetch_attempts
                WHERE resolved_at IS NULL
                """
            ).fetchone()

        return {
            "version": 1,
            "source_db": str(Path(path).resolve()),
            "source_manifests": [
                str(Path(item).resolve()) for item in manifest_paths
            ],
            "parameters": {
                "minimum_samples": minimum_samples,
                "prior_strength": prior_strength,
                "exploration_score_adjustment": 0,
                "maximum_absolute_adjustment": 15,
            },
            "summary": {
                "attributed_decisions": total,
                "qualified": qualified,
                "baseline_qualified_rate": round(baseline_rate, 6),
                "patterns": len(grouped),
                "unresolved_fetch_errors": int(unresolved_errors or 0),
                "retry_attempts": int(retry_attempts or 0),
            },
            "patterns": grouped,
            "pattern_buckets": by_bucket,
        }
    finally:
        conn.close()


def load_priority_adjustments(path: str | None) -> dict[str, Any]:
    """Load a generated feedback profile; an omitted path means no adjustment."""
    if not path:
        return {"patterns": {}, "pattern_buckets": {}}
    with Path(path).open(encoding="utf-8") as source:
        data = json.load(source)
    if int(data.get("version", 0)) != 1:
        raise ValueError(f"unsupported pattern-priority profile: {path}")
    return data


def priority_adjustment(
        profile: dict[str, Any],
        pattern_id: str,
        bucket: str | None = None) -> int:
    """Prefer a sufficiently sampled geo-specific weight, then the global one."""
    if bucket:
        local = profile.get("pattern_buckets", {}).get(f"{pattern_id}|{bucket}")
        if local and not local.get("exploration", False):
            return int(local.get("score_adjustment", 0))
    global_entry = profile.get("patterns", {}).get(pattern_id, {})
    return int(global_entry.get("score_adjustment", 0))


def priority_profile_hash(path: str | None) -> str | None:
    """Bind resumable discovery state to the exact optional feedback profile."""
    if not path:
        return None
    content = Path(path).read_bytes()
    return hashlib.sha256(content).hexdigest()[:16]
