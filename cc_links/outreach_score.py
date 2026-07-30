"""Deterministic scoring over outreach discovery, live and HTML evidence."""

from __future__ import annotations

import csv
import json
import re
import sqlite3
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCORE_SCHEMA_VERSION = 1

EDITORIAL_TITLE_RE = re.compile(
    r"\b(?:"
    r"write\s+for\s+us|submit\s+(?:an?\s+)?article|"
    r"guest\s+post\s+guidelines|become\s+(?:an?\s+)?contributor|"
    r"escrib(?:e|ir)\s+para\s+nosotros|publica\s+con\s+nosotros|"
    r"envi(?:a|ar)\s+(?:tu\s+)?articulo|guia\s+para\s+autores|"
    r"normas\s+de\s+publicacion|"
    r"escreva\s+para\s+nos|envie\s+(?:seu|um)\s+artigo|"
    r"publique\s+conosco|(?:guia|diretrizes)\s+para\s+autores|"
    r"schreib(?:e|en)?\s+fur\s+uns|gastbeitrag\s+einreichen|"
    r"autor\s+werden|beitrag\s+einreichen|"
    r"ecrivez\s+pour\s+nous|soumettre\s+un\s+article|"
    r"devenir\s+(?:auteur|contributeur)"
    r")\b",
    re.IGNORECASE,
)

SCORE_SCHEMA = """
CREATE TABLE IF NOT EXISTS page_scores (
    url TEXT PRIMARY KEY,
    registered_domain TEXT NOT NULL,
    tld TEXT,
    pattern_id TEXT,
    pattern_language TEXT,
    live_outcome TEXT,
    selected_source TEXT NOT NULL,
    discovery_score REAL NOT NULL,
    qualification_score REAL NOT NULL,
    content_score REAL NOT NULL,
    freshness_score REAL NOT NULL,
    freshness_confidence REAL NOT NULL,
    effective_freshness_score REAL NOT NULL,
    combined_score REAL NOT NULL,
    score_band TEXT NOT NULL CHECK(score_band IN ('high', 'medium', 'low')),
    best_lastmod TEXT,
    best_lastmod_source TEXT,
    title TEXT,
    meta_description TEXT,
    h1 TEXT,
    html_lang TEXT,
    engine_name TEXT,
    word_count INTEGER NOT NULL,
    form_count INTEGER NOT NULL,
    submission_form_count INTEGER NOT NULL,
    contact_link_count INTEGER NOT NULL,
    email_link_count INTEGER NOT NULL,
    invitation_phrases TEXT NOT NULL,
    guideline_signals TEXT NOT NULL,
    risk_flags TEXT NOT NULL,
    score_reasons TEXT NOT NULL,
    scored_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_page_scores_domain
    ON page_scores(registered_domain);
CREATE INDEX IF NOT EXISTS idx_page_scores_combined
    ON page_scores(combined_score);
CREATE INDEX IF NOT EXISTS idx_page_scores_band
    ON page_scores(score_band);
CREATE INDEX IF NOT EXISTS idx_page_scores_lastmod
    ON page_scores(best_lastmod);
"""


@dataclass(frozen=True)
class ScoreComponents:
    """Auditable component scores for one candidate page."""

    discovery: float
    qualification: float
    content: float
    freshness: float
    freshness_confidence: float
    effective_freshness: float
    combined: float
    band: str
    reasons: tuple[str, ...]


def _clamp(value: float) -> float:
    return round(max(0.0, min(100.0, value)), 2)


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if not value:
        return []
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return [str(item) for item in decoded] if isinstance(decoded, list) else []


def _parse_date(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text[:10], "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def content_component(features: Mapping[str, Any]) -> tuple[float, list[str]]:
    """Score page content without using freshness or discovery metadata."""
    score = 20.0
    reasons: list[str] = []
    invitations = _json_list(features.get("invitation_phrases"))
    guidelines = _json_list(features.get("guideline_signals"))
    risks = set(_json_list(features.get("risk_flags")))
    submission_forms = int(features.get("submission_form_count") or 0)
    forms = int(features.get("form_count") or 0)
    contacts = int(features.get("contact_link_count") or 0)
    emails = int(features.get("email_link_count") or 0)
    words = int(features.get("word_count") or 0)

    additions = (
        (bool(invitations), 35, "invitation_phrase"),
        (bool(submission_forms), 15, "submission_form"),
        (bool(guidelines), 10, "editorial_guidelines"),
        (bool(contacts), 7, "contact_link"),
        (bool(emails), 3, "email_link"),
        (bool(features.get("engine_name")), 5, "known_engine"),
        (words >= 250, 3, "substantive_text"),
        (words >= 750, 2, "deep_text"),
    )
    for present, points, reason in additions:
        if present:
            score += points
            reasons.append(reason)
    if forms and not submission_forms:
        score += 2
        reasons.append("generic_form")
    if "noindex" in str(features.get("meta_robots") or "").lower():
        score -= 10
        reasons.append("noindex")

    penalties = {
        "parked_or_for_sale": 60,
        "soft_404": 45,
        "demo_host": 45,
        "taxonomy_path": 35,
        "careers_context": 25,
        "anti_bot_challenge": 15,
    }
    for risk, penalty in penalties.items():
        if risk in risks and not (risk == "careers_context" and invitations):
            score -= penalty
            reasons.append(f"risk:{risk}")
    return _clamp(score), reasons


def _normalized_text(*values: Any) -> str:
    text = " ".join(str(value or "") for value in values)
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(
        character for character in normalized if not unicodedata.combining(character)
    )


def content_component_v2(
    features: Mapping[str, Any],
) -> tuple[float, list[str]]:
    """Score content with conservative multilingual and challenge handling."""
    score = 15.0
    reasons: list[str] = []
    invitations = _json_list(features.get("invitation_phrases"))
    guidelines = _json_list(features.get("guideline_signals"))
    risks = set(_json_list(features.get("risk_flags")))
    title_text = _normalized_text(features.get("title"), features.get("h1"))
    editorial_title = bool(EDITORIAL_TITLE_RE.search(title_text))
    submission_forms = int(features.get("submission_form_count") or 0)
    forms = int(features.get("form_count") or 0)
    contacts = int(features.get("contact_link_count") or 0)
    emails = int(features.get("email_link_count") or 0)
    words = int(features.get("word_count") or 0)

    if invitations:
        score += 40
        reasons.append("invitation_phrase")
    elif editorial_title:
        score += 20
        reasons.append("multilingual_editorial_title")
    if submission_forms:
        score += 15
        reasons.append("submission_form")
    elif forms:
        score += 2
        reasons.append("generic_form")
    if guidelines:
        score += 10
        reasons.append("editorial_guidelines")
    if contacts:
        score += 7
        reasons.append("contact_link")
    if emails:
        score += 3
        reasons.append("email_link")
    if features.get("engine_name"):
        score += 3
        reasons.append("known_engine")
    if words >= 250:
        score += 3
        reasons.append("substantive_text")
    if words >= 750:
        score += 2
        reasons.append("deep_text")
    if "noindex" in str(features.get("meta_robots") or "").lower():
        score -= 10
        reasons.append("noindex")

    penalties = {
        "parked_or_for_sale": 60,
        "soft_404": 45,
        "demo_host": 45,
        "taxonomy_path": 35,
    }
    for risk, penalty in penalties.items():
        if risk in risks:
            score -= penalty
            reasons.append(f"risk:{risk}")
    positive_editorial = bool(invitations or editorial_title)
    if "careers_context" in risks and not positive_editorial:
        score -= 25
        reasons.append("risk:careers_context")
    if (
        "anti_bot_challenge" in risks
        and not positive_editorial
        and words < 200
    ):
        score -= 15
        reasons.append("risk:anti_bot_challenge")
    elif "anti_bot_challenge" in risks:
        reasons.append("challenge_signal_ignored_as_weak")
    return _clamp(score), reasons


def discovery_component_v2(
    *,
    pattern_weight: float,
    path_specificity: int,
) -> float:
    """Spread narrow 88..100 registry weights using path evidence."""
    return _clamp(
        50.0
        + 2.0 * (pattern_weight - 88.0)
        + min(max(path_specificity, 0), 24)
    )


def freshness_component(
    lastmod: Any,
    source: Any,
    *,
    now: datetime | None = None,
) -> tuple[float, float, float, list[str]]:
    """Return raw, confidence and confidence-shrunk freshness scores."""
    observed = _parse_date(lastmod)
    if observed is None:
        return 50.0, 0.0, 50.0, ["freshness_unknown"]
    reference = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    age_days = (reference - observed).days
    if age_days < -30:
        raw, age_band = 20.0, "future_date"
    elif age_days <= 90:
        raw, age_band = 100.0, "lastmod_0_90d"
    elif age_days <= 180:
        raw, age_band = 90.0, "lastmod_91_180d"
    elif age_days <= 365:
        raw, age_band = 80.0, "lastmod_181_365d"
    elif age_days <= 730:
        raw, age_band = 65.0, "lastmod_1_2y"
    elif age_days <= 1460:
        raw, age_band = 45.0, "lastmod_2_4y"
    else:
        raw, age_band = 25.0, "lastmod_over_4y"
    confidence = {
        "sitemap_exact": 1.0,
        "live_html_or_header": 0.9,
        "warc_html_or_header": 0.45,
    }.get(str(source or ""), 0.25)
    effective = _clamp(50.0 + (raw - 50.0) * confidence)
    return raw, confidence, effective, [age_band, f"date:{source}"]


def score_page(
    features: Mapping[str, Any],
    *,
    discovery_score: float,
    qualification_score: float,
    live_outcome: str,
    now: datetime | None = None,
    profile: str = "v1",
    pattern_weight: float | None = None,
    path_specificity: int | None = None,
) -> ScoreComponents:
    """Combine independent evidence while retaining every component."""
    if profile not in ("v1", "v2"):
        raise ValueError("profile must be v1 or v2")
    if profile == "v2":
        content, reasons = content_component_v2(features)
    else:
        content, reasons = content_component(features)
    freshness, confidence, effective, date_reasons = freshness_component(
        features.get("best_lastmod"),
        features.get("best_lastmod_source"),
        now=now,
    )
    if (
        profile == "v2"
        and pattern_weight is not None
        and path_specificity is not None
    ):
        discovery = discovery_component_v2(
            pattern_weight=pattern_weight,
            path_specificity=path_specificity,
        )
        reasons.append("discovery_rescaled")
    else:
        discovery = _clamp(discovery_score)
    qualification = _clamp(qualification_score)
    if profile == "v2":
        combined = _clamp(
            0.05 * discovery
            + 0.45 * qualification
            + 0.35 * content
            + 0.15 * effective
        )
        reasons.append("profile:v2")
    else:
        combined = _clamp(
            0.15 * discovery
            + 0.40 * qualification
            + 0.30 * content
            + 0.15 * effective
        )
        reasons.append("profile:v1")
    risks = set(_json_list(features.get("risk_flags")))
    if risks & {
        "parked_or_for_sale",
        "soft_404",
        "demo_host",
        "taxonomy_path",
    }:
        combined = min(combined, 25.0)
        reasons.append("hard_risk_cap")
    if live_outcome == "rejected":
        combined = min(combined, 35.0)
        reasons.append("live_rejected_cap")
    elif live_outcome == "unreachable":
        combined = min(combined, 30.0)
        reasons.append("live_unreachable_cap")
    band = "high" if combined >= 70 else "medium" if combined >= 50 else "low"
    return ScoreComponents(
        discovery=discovery,
        qualification=qualification,
        content=content,
        freshness=freshness,
        freshness_confidence=confidence,
        effective_freshness=effective,
        combined=combined,
        band=band,
        reasons=tuple(sorted(set(reasons + date_reasons))),
    )


def _read_rows(path: str | Path, query: str) -> list[sqlite3.Row]:
    connection = sqlite3.connect(f"file:{Path(path)}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return list(connection.execute(query))
    finally:
        connection.close()


def build_page_scores(
    *,
    enrichment_db: str | Path,
    discovery_db: str | Path,
    validation_db: str | Path,
    out_db: str | Path,
    now: datetime | None = None,
    profile: str = "v1",
) -> dict[str, Any]:
    """Join the three evidence stores and write a compact scoring database."""
    feature_rows = _read_rows(enrichment_db, "SELECT * FROM scoring_features")
    discovery_pages = {
        str(row["url"]): dict(row)
        for row in _read_rows(
            discovery_db,
            """
            SELECT p.url,p.registered_domain,p.tld,p.pattern_id,
                   p.pattern_language,p.pattern_weight,p.path_specificity,
                   d.discovery_score
            FROM outreach_pages p
            JOIN outreach_prospects d USING(registered_domain)
            """,
        )
    }
    validation_pages = {
        str(row["url"]): dict(row)
        for row in _read_rows(
            validation_db,
            "SELECT url,outcome,qualification_score FROM page_checks",
        )
    }
    target = Path(out_db)
    target.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(target)
    connection.executescript(SCORE_SCHEMA)
    connection.execute("DELETE FROM page_scores")
    scored_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    placeholders = ",".join("?" for _ in range(32))
    missing_discovery = 0
    missing_validation = 0
    for raw in feature_rows:
        features = dict(raw)
        url = str(features["url"])
        discovery = discovery_pages.get(url, {})
        validation = validation_pages.get(url, {})
        missing_discovery += not bool(discovery)
        missing_validation += not bool(validation)
        live_outcome = str(validation.get("outcome") or "unreachable")
        components = score_page(
            features,
            discovery_score=float(discovery.get("discovery_score") or 0),
            qualification_score=float(
                validation.get("qualification_score") or 0
            ),
            live_outcome=live_outcome,
            now=now,
            profile=profile,
            pattern_weight=(
                float(discovery["pattern_weight"])
                if discovery.get("pattern_weight") is not None
                else None
            ),
            path_specificity=(
                int(discovery["path_specificity"])
                if discovery.get("path_specificity") is not None
                else None
            ),
        )
        connection.execute(
            f"INSERT INTO page_scores VALUES ({placeholders})",
            (
                url,
                features["registered_domain"],
                discovery.get("tld"),
                discovery.get("pattern_id"),
                discovery.get("pattern_language"),
                live_outcome,
                features["selected_source"],
                components.discovery,
                components.qualification,
                components.content,
                components.freshness,
                components.freshness_confidence,
                components.effective_freshness,
                components.combined,
                components.band,
                features["best_lastmod"],
                features["best_lastmod_source"],
                features["title"],
                features["meta_description"],
                features["h1"],
                features["html_lang"],
                features["engine_name"],
                int(features["word_count"] or 0),
                int(features["form_count"] or 0),
                int(features["submission_form_count"] or 0),
                int(features["contact_link_count"] or 0),
                int(features["email_link_count"] or 0),
                features["invitation_phrases"],
                features["guideline_signals"],
                features["risk_flags"],
                json.dumps(components.reasons, ensure_ascii=False),
                scored_at,
            ),
        )
    connection.commit()
    averages = connection.execute(
        """
        SELECT ROUND(AVG(discovery_score),2),
               ROUND(AVG(qualification_score),2),
               ROUND(AVG(content_score),2),
               ROUND(AVG(freshness_score),2),
               ROUND(AVG(combined_score),2)
        FROM page_scores
        """
    ).fetchone()
    report = {
        "schema_version": SCORE_SCHEMA_VERSION,
        "profile": profile,
        "scored_at": scored_at,
        "pages": len(feature_rows),
        "missing_discovery": missing_discovery,
        "missing_validation": missing_validation,
        "bands": dict(
            connection.execute(
                "SELECT score_band,COUNT(*) FROM page_scores GROUP BY score_band"
            )
        ),
        "average_scores": dict(
            zip(
                ("discovery", "qualification", "content", "freshness", "combined"),
                averages,
            )
        ),
        "lastmod_sources": dict(
            connection.execute(
                """
                SELECT COALESCE(best_lastmod_source,'missing'),COUNT(*)
                FROM page_scores GROUP BY best_lastmod_source
                """
            )
        ),
        "quick_check": connection.execute("PRAGMA quick_check").fetchone()[0],
    }
    connection.close()
    return report


def write_score_outputs(
    db_path: str | Path,
    *,
    report_path: str | Path,
    csv_path: str | Path,
    text_dir: str | Path,
    report: Mapping[str, Any],
) -> None:
    """Write a report, full CSV and URL-only score bands."""
    report_target = Path(report_path)
    csv_target = Path(csv_path)
    report_target.parent.mkdir(parents=True, exist_ok=True)
    csv_target.parent.mkdir(parents=True, exist_ok=True)
    report_target.write_text(
        json.dumps(dict(report), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    connection = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT * FROM page_scores ORDER BY combined_score DESC,url"
        )
        with csv_target.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([column[0] for column in rows.description])
            writer.writerows(rows)
        output = Path(text_dir)
        output.mkdir(parents=True, exist_ok=True)
        for band in ("high", "medium", "low"):
            urls = connection.execute(
                """
                SELECT url FROM page_scores WHERE score_band=?
                ORDER BY combined_score DESC,url
                """,
                (band,),
            )
            (output / f"{band}.txt").write_text(
                "".join(f"{row[0]}\n" for row in urls),
                encoding="utf-8",
            )
    finally:
        connection.close()
