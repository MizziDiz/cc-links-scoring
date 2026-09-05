"""Pre-fetch gates: skip URLs whose discovery pattern historically yields nothing,
and prefer new registered domains over more URLs from known ones.

Both gates act on the fetch queue only. Discovery, the WARC classifier, the
minimum score and the per-domain cap stay authoritative for what is stored.

Why this exists. On the production run of 25-30 July 2026, 46% of fetched pages
came back ``unmatched``. A cut of ``processed_urls`` by pattern and tier showed
the waste sits in a handful of groups (``guestbook:0`` alone: 541k fetches at a
4.5% yield; broad ``/profile/``, ``/user/``, ``/topic/`` under 2%). Skipping
groups with a historical yield below 20% would have avoided 46% of fetches and
97% of unmatched results while losing 3% of stored URLs.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

STORED_OUTCOMES = ("stored",)
DECISION_OUTCOMES = ("stored", "unmatched", "below_threshold")


def _read_only(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def group_key(pattern_id: Optional[str], tier: Any) -> str:
    try:
        tier_value = int(tier) if tier is not None and tier != "" else -1
    except (TypeError, ValueError):
        tier_value = -1
    return f"{pattern_id or ''}|{tier_value}"


def collect_pattern_yield(db_path: str, since: Optional[str] = None,
                          minimum_decisions: int = 20) -> dict[str, Any]:
    """Measure historical stored-rate per (pattern_id, discovery_tier).

    Only real fetch decisions count: ``stored`` against ``unmatched`` and
    ``below_threshold``. ``domain_cap`` rows are excluded because they were
    stored first and archived later; fetch errors never reach processed_urls.
    Opens the database read-only.
    """
    conn = _read_only(db_path)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(processed_urls)")}
        if not {"pattern_id", "discovery_tier", "outcome"} <= columns:
            return {"groups": {}, "since": since, "minimum_decisions": minimum_decisions,
                    "note": "processed_urls lacks pattern attribution"}
        params: list[Any] = list(DECISION_OUTCOMES)
        where = "outcome IN (%s) AND pattern_id IS NOT NULL AND pattern_id <> ''" % (
            ", ".join("?" for _ in DECISION_OUTCOMES))
        if since:
            where += " AND processed_at >= ?"
            params.append(since)
        rows = conn.execute(
            f"""SELECT pattern_id, discovery_tier, COUNT(*),
                       SUM(CASE WHEN outcome = 'stored' THEN 1 ELSE 0 END),
                       COUNT(DISTINCT CASE WHEN outcome = 'stored'
                                           THEN registered_domain END)
                FROM processed_urls WHERE {where}
                GROUP BY pattern_id, discovery_tier""",
            params,
        ).fetchall()
    finally:
        conn.close()
    groups = {}
    for pattern_id, tier, decisions, stored, stored_domains in rows:
        groups[group_key(pattern_id, tier)] = {
            "pattern_id": pattern_id,
            "discovery_tier": tier,
            "decisions": int(decisions),
            "stored": int(stored or 0),
            "stored_domains": int(stored_domains or 0),
            "yield": (int(stored or 0) / int(decisions)) if decisions else 0.0,
        }
    return {"groups": groups, "since": since, "minimum_decisions": minimum_decisions}


def save_yield_profile(profile: dict[str, Any], path: str) -> None:
    with open(path, "w", encoding="utf-8") as output:
        json.dump(profile, output, ensure_ascii=False, indent=1, sort_keys=True)


def load_yield_profile(path: Optional[str]) -> Optional[dict[str, Any]]:
    if not path:
        return None
    with open(path, encoding="utf-8") as source:
        profile = json.load(source)
    if not isinstance(profile, dict) or not isinstance(profile.get("groups"), dict):
        raise ValueError(f"{path}: not a yield profile (expected an object with 'groups')")
    return profile


def _stable_fraction(text: str) -> float:
    digest = hashlib.sha1(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / float(1 << 64)


@dataclass
class YieldGate:
    """Skip URLs from pattern/tier groups whose historical yield is too low.

    Groups with fewer than ``minimum_decisions`` observations are always
    allowed: new signals must keep being explored. A deterministic share of the
    would-be-skipped URLs is still let through (``explore_share``) so a group
    can recover if the taxonomy or the web changes.
    """
    profile: Optional[dict[str, Any]]
    minimum_yield: float = 0.2
    minimum_decisions: int = 20
    explore_share: float = 0.05
    stats: dict[str, int] = field(default_factory=lambda: {
        "allowed": 0, "explored": 0, "skipped": 0, "unknown": 0})

    def decide(self, record: dict[str, Any]) -> bool:
        if not self.profile:
            self.stats["allowed"] += 1
            return True
        key = group_key(record.get("pattern_id"), record.get("discovery_tier"))
        group = self.profile["groups"].get(key)
        if group is None or group.get("decisions", 0) < self.minimum_decisions:
            self.stats["unknown"] += 1
            return True
        if group.get("yield", 0.0) >= self.minimum_yield:
            self.stats["allowed"] += 1
            return True
        if self.explore_share > 0 and _stable_fraction(
                "explore:" + str(record.get("url", ""))) < self.explore_share:
            self.stats["explored"] += 1
            return True
        self.stats["skipped"] += 1
        return False

    def low_yield_groups(self) -> list[dict[str, Any]]:
        if not self.profile:
            return []
        return sorted(
            (g for g in self.profile["groups"].values()
             if g.get("decisions", 0) >= self.minimum_decisions
             and g.get("yield", 0.0) < self.minimum_yield),
            key=lambda g: -g.get("decisions", 0),
        )


@dataclass
class DomainGate:
    """Prefer new registered domains.

    ``known_domains`` are domains that already have a stored candidate; with
    ``skip_known`` they are not fetched again in this run. ``per_run_cap``
    bounds how many URLs of one domain are scheduled in this run (0 = off),
    which spreads a fixed fetch budget across more sites than the global
    ``--max-per-domain`` alone.
    """
    known_domains: set[str] = field(default_factory=set)
    skip_known: bool = False
    per_run_cap: int = 0
    scheduled: dict[str, int] = field(default_factory=dict)
    stats: dict[str, int] = field(default_factory=lambda: {
        "allowed": 0, "skipped_known": 0, "skipped_cap": 0})

    def decide(self, record: dict[str, Any]) -> bool:
        domain = (record.get("url_host_registered_domain") or "").lower()
        if not domain:
            self.stats["allowed"] += 1
            return True
        if self.skip_known and domain in self.known_domains:
            self.stats["skipped_known"] += 1
            return False
        if self.per_run_cap > 0:
            seen = self.scheduled.get(domain, 0)
            if seen >= self.per_run_cap:
                self.stats["skipped_cap"] += 1
                return False
            self.scheduled[domain] = seen + 1
        self.stats["allowed"] += 1
        return True


def load_known_domains(conn: sqlite3.Connection) -> set[str]:
    return {
        (row[0] or "").lower()
        for row in conn.execute(
            "SELECT DISTINCT registered_domain FROM candidates "
            "WHERE registered_domain IS NOT NULL AND registered_domain <> ''")
    }


def apply_gates(records: Iterable[dict[str, Any]], gates: Iterable[Any]):
    """Yield only records every gate accepts, in the original order."""
    gates = list(gates)
    for record in records:
        if all(gate.decide(record) for gate in gates):
            yield record
