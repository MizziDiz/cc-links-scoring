"""Offline placement-model and outbound-link graph for outreach pages."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

from lxml import etree  # type: ignore[import-untyped]
from lxml import html as lxml_html  # type: ignore[import-untyped]

try:
    import tldextract  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - production requirements include it
    tldextract = None  # type: ignore[assignment]

SCHEMA_VERSION = 3
EXTRACTOR_VERSION = 3
PLACEMENT_MODELS = {"external_service", "self_hosted", "hybrid", "unknown"}
LINK_ROLES = {
    "placement_example",
    "publisher_inventory",
    "self_hosted",
    "reference",
    "social",
    "contact",
}

PLACEMENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS placement_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS services (
    service_id TEXT PRIMARY KEY,
    registered_domain TEXT NOT NULL UNIQUE,
    canonical_url TEXT NOT NULL,
    placement_model TEXT NOT NULL,
    model_confidence REAL NOT NULL,
    model_reasons TEXT NOT NULL DEFAULT '[]',
    page_count INTEGER NOT NULL,
    successful_page_count INTEGER NOT NULL,
    example_placement_count INTEGER NOT NULL,
    publisher_domain_count INTEGER NOT NULL,
    reliability_status TEXT NOT NULL,
    reliability_score REAL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS service_pages (
    url TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    registered_domain TEXT NOT NULL,
    crawl TEXT NOT NULL,
    fetched_at TEXT,
    fetch_status TEXT NOT NULL,
    placement_model TEXT NOT NULL,
    model_confidence REAL NOT NULL,
    external_score REAL NOT NULL,
    self_hosted_score REAL NOT NULL,
    model_reasons TEXT NOT NULL DEFAULT '[]',
    model_evidence TEXT NOT NULL DEFAULT '[]',
    promise_level TEXT,
    promise_probability REAL,
    placement_types TEXT NOT NULL DEFAULT '[]',
    commercial_model TEXT,
    price_status TEXT,
    price_min REAL,
    price_max REAL,
    currency TEXT,
    page_quality_score REAL,
    score_band TEXT,
    best_lastmod TEXT,
    best_lastmod_source TEXT,
    error_kind TEXT,
    error_detail TEXT,
    FOREIGN KEY(service_id) REFERENCES services(service_id)
);

CREATE INDEX IF NOT EXISTS idx_service_pages_service ON service_pages(service_id);
CREATE INDEX IF NOT EXISTS idx_service_pages_model ON service_pages(placement_model);

CREATE TABLE IF NOT EXISTS outbound_links (
    link_id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    source_url TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    destination_registered_domain TEXT,
    same_registered_domain INTEGER NOT NULL,
    anchor_text TEXT,
    rel TEXT,
    context_text TEXT,
    dom_section TEXT,
    link_role TEXT NOT NULL,
    role_confidence REAL NOT NULL,
    role_reasons TEXT NOT NULL DEFAULT '[]',
    source_snapshot TEXT NOT NULL,
    source_lastmod TEXT,
    observed_at TEXT NOT NULL,
    FOREIGN KEY(service_id) REFERENCES services(service_id),
    FOREIGN KEY(source_url) REFERENCES service_pages(url)
);

CREATE INDEX IF NOT EXISTS idx_outbound_links_service ON outbound_links(service_id);
CREATE INDEX IF NOT EXISTS idx_outbound_links_destination
    ON outbound_links(destination_registered_domain);
CREATE INDEX IF NOT EXISTS idx_outbound_links_role ON outbound_links(link_role);

CREATE TABLE IF NOT EXISTS placements (
    placement_id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    evidence_url TEXT NOT NULL,
    placement_url TEXT NOT NULL,
    publisher_registered_domain TEXT,
    relationship TEXT NOT NULL,
    association_confidence REAL NOT NULL,
    association_reasons TEXT NOT NULL DEFAULT '[]',
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    FOREIGN KEY(service_id) REFERENCES services(service_id)
);

CREATE INDEX IF NOT EXISTS idx_placements_service ON placements(service_id);
CREATE INDEX IF NOT EXISTS idx_placements_publisher
    ON placements(publisher_registered_domain);

CREATE TABLE IF NOT EXISTS placement_evaluations (
    evaluation_id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    evidence_url TEXT NOT NULL UNIQUE,
    placement_model TEXT NOT NULL,
    service_reliability_score REAL,
    placement_quality_score REAL,
    publication_probability REAL,
    advertised_price REAL,
    advertised_currency TEXT,
    expected_utility_points REAL,
    cost_per_utility_point REAL,
    evaluation_status TEXT NOT NULL,
    evaluation_reasons TEXT NOT NULL DEFAULT '[]',
    evaluated_at TEXT NOT NULL,
    FOREIGN KEY(service_id) REFERENCES services(service_id),
    FOREIGN KEY(evidence_url) REFERENCES service_pages(url)
);

CREATE INDEX IF NOT EXISTS idx_placement_evaluations_status
    ON placement_evaluations(evaluation_status);
CREATE INDEX IF NOT EXISTS idx_placement_evaluations_utility
    ON placement_evaluations(expected_utility_points);

CREATE TABLE IF NOT EXISTS placement_outcomes (
    placement_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    target_url TEXT,
    target_registered_domain TEXT,
    placed_at TEXT,
    cost REAL,
    currency TEXT,
    status TEXT NOT NULL DEFAULT 'planned',
    published_url TEXT,
    notes TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (placement_id, project_id),
    FOREIGN KEY(placement_id) REFERENCES placements(placement_id)
);

CREATE TABLE IF NOT EXISTS impact_observations (
    placement_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    link_live INTEGER,
    indexed INTEGER,
    target_rating REAL,
    target_organic_traffic REAL,
    target_keywords REAL,
    target_referring_domains REAL,
    observation_source TEXT NOT NULL,
    control_label TEXT,
    notes TEXT,
    PRIMARY KEY (placement_id, project_id, observed_at, observation_source),
    FOREIGN KEY(placement_id,project_id)
        REFERENCES placement_outcomes(placement_id,project_id)
);
"""

EXTERNAL_SERVICE_RE = re.compile(
    r"\b(?:link\s+building\s+(?:agency|service)|"
    r"publisher\s+network|our\s+network\s+of\s+(?:sites|publishers|websites)|"
    r"choose\s+from\s+(?:our\s+)?(?:sites|publishers|websites)|"
    r"placements?\s+(?:across|on)\s+(?:hundreds|thousands|multiple)\s+of\s+sites|"
    r"we\s+(?:secure|arrange|build|place)\s+(?:your\s+)?(?:links?|placements?)\s+on|"
    r"servicio\s+de\s+(?:guest\s+post|link\s+building|construccion\s+de\s+enlaces)|"
    r"red\s+de\s+(?:sitios|editores)|rede\s+de\s+(?:sites|editores))\b",
    re.IGNORECASE,
)
SELF_HOSTED_RE = re.compile(
    r"\b(?:we\s+(?:publish|feature)\s+(?:your\s+)?(?:article|post|content)\s+"
    r"(?:on|in)\s+our\s+(?:site|blog|magazine)|"
    r"publish(?:ed)?\s+(?:on|in)\s+our\s+(?:site|blog|magazine)|"
    r"submit\s+(?:your\s+)?(?:article|post|story)\s+to\s+(?:us|our)|"
    r"write\s+for\s+us|become\s+(?:a\s+)?contributor|"
    r"publicamos\s+tu\s+(?:articulo|contenido)\s+en\s+nuestro|"
    r"escribe\s+para\s+nosotros|publique\s+(?:seu\s+)?artigo\s+em\s+nosso|"
    r"escreva\s+para\s+nos)\b",
    re.IGNORECASE,
)
EXAMPLE_RE = re.compile(
    r"\b(?:example|sample|case\s+stud(?:y|ies)|portfolio|our\s+work|"
    r"recent\s+placements?|published\s+(?:at|on|in)|live\s+links?|"
    r"ejemplos?|casos?\s+de\s+exito|portafolio|exemplos?|portfolio)\b",
    re.IGNORECASE,
)
INVENTORY_RE = re.compile(
    r"\b(?:publishers?|sites?|websites?|inventory|catalog|marketplace|network|"
    r"medios|sitios|editores|inventario|rede)\b",
    re.IGNORECASE,
)
CONTACT_RE = re.compile(
    r"\b(?:contact|about|privacy|terms|login|sign\s*up)\b", re.IGNORECASE
)
SOCIAL_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "pinterest.com",
    "tiktok.com",
    "twitter.com",
    "t.me",
    "x.com",
    "youtube.com",
}
_TLD_EXTRACT = tldextract.TLDExtract(suffix_list_urls=()) if tldextract else None
_COMMON_SECOND_LEVEL_SUFFIXES = {
    "ac.uk",
    "co.in",
    "co.jp",
    "co.nz",
    "co.uk",
    "com.au",
    "com.br",
    "com.cn",
    "com.in",
    "com.mx",
    "com.sg",
    "com.tr",
    "com.tw",
    "gov.uk",
    "net.au",
    "org.au",
    "org.uk",
}


@dataclass(frozen=True)
class PlacementConfig:
    """Material settings for archived outbound-link extraction."""

    max_html_bytes: int = 5_000_000
    max_links_per_page: int = 20_000
    retries: int = 2
    retry_backoff: float = 1.0


@dataclass(frozen=True)
class PlacementSummary:
    """Counters for a resumable placement-graph build."""

    input_pages: int
    scheduled_pages: int
    completed_pages: int
    successful_pages: int
    services: int
    outbound_links: int
    placements: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _compact(value: str | None, limit: int) -> str:
    return re.sub(r"\s+", " ", value or "").strip()[:limit]


def _registered_domain(url_or_host: str) -> str:
    parsed = urlparse(url_or_host if "://" in url_or_host else f"//{url_or_host}")
    host = (parsed.hostname or "").lower().rstrip(".")
    if not host:
        return ""
    if _TLD_EXTRACT is not None:
        result = _TLD_EXTRACT(host)
        return result.top_domain_under_public_suffix or host
    labels = host.split(".")
    suffix_length = 2 if ".".join(labels[-2:]) in _COMMON_SECOND_LEVEL_SUFFIXES else 1
    return (
        ".".join(labels[-(suffix_length + 1) :])
        if len(labels) > suffix_length
        else host
    )


def _normalized_url(value: str, base_url: str) -> str:
    absolute, _ = urldefrag(urljoin(base_url, value.strip()))
    parsed = urlparse(absolute)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        return ""
    return parsed._replace(
        scheme=parsed.scheme.lower(), netloc=parsed.netloc.lower()
    ).geturl()


def _stable_id(prefix: str, *values: str) -> str:
    payload = "\x1f".join(values).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(payload).hexdigest()[:24]}"


def _classify_link(
    *,
    same_domain: bool,
    destination_domain: str,
    anchor: str,
    context: str,
    external_service_context: bool,
) -> tuple[str, float, list[str]]:
    combined = f"{anchor} {context}"
    reasons: list[str] = []
    if destination_domain in SOCIAL_DOMAINS:
        return "social", 0.99, ["known_social_domain"]
    if same_domain:
        return "self_hosted", 0.90, ["same_registered_domain"]
    if external_service_context and EXAMPLE_RE.search(combined):
        reasons.append("explicit_example_context")
        return "placement_example", 0.90, reasons
    if external_service_context and INVENTORY_RE.search(combined):
        reasons.extend(["publisher_inventory_context", "external_service_context"])
        return "publisher_inventory", 0.80, reasons
    if CONTACT_RE.search(anchor) and len(anchor) <= 80:
        return "contact", 0.75, ["contact_or_policy_anchor"]
    return "reference", 0.40, ["external_link_without_placement_evidence"]


def _model_from_evidence(
    *, text: str, terms: Mapping[str, Any], links: Sequence[Mapping[str, Any]]
) -> tuple[str, float, float, float, list[str], list[dict[str, str]]]:
    external_score = 0.0
    self_score = 0.0
    reasons: list[str] = []
    evidence: list[dict[str, str]] = []
    external_match = EXTERNAL_SERVICE_RE.search(text)
    if external_match:
        external_score += 5.0
        reasons.append("external_service_language")
        evidence.append(
            {
                "signal": "external_service_language",
                "snippet": text[
                    max(0, external_match.start() - 120) : external_match.end() + 180
                ],
            }
        )
    self_match = SELF_HOSTED_RE.search(text)
    if self_match:
        self_score += 4.0
        reasons.append("self_hosted_publication_language")
        evidence.append(
            {
                "signal": "self_hosted_publication_language",
                "snippet": text[
                    max(0, self_match.start() - 120) : self_match.end() + 180
                ],
            }
        )
    placement_types = terms.get("placement_types")
    if isinstance(placement_types, str):
        try:
            placement_types = json.loads(placement_types)
        except json.JSONDecodeError:
            placement_types = []
    explicit_terms = str(terms.get("promise_level") or "unclear") != "unclear" or bool(
        placement_types
    )
    if explicit_terms:
        self_score += 1.5
        reasons.append("explicit_publication_terms")
    example_domains = {
        str(link.get("destination_registered_domain") or "")
        for link in links
        if link.get("link_role") == "placement_example"
    }
    example_domains.discard("")
    inventory_domains = {
        str(link.get("destination_registered_domain") or "")
        for link in links
        if link.get("link_role") == "publisher_inventory"
    }
    inventory_domains.discard("")
    if example_domains:
        external_score += min(4.0, 2.0 + len(example_domains) * 0.5)
        reasons.append(f"external_example_domains:{len(example_domains)}")
    if inventory_domains:
        external_score += min(4.0, 2.0 + len(inventory_domains) * 0.25)
        reasons.append(f"publisher_inventory_domains:{len(inventory_domains)}")
    if external_score >= 4.0 and self_score >= 4.0:
        model = "hybrid"
    elif external_score >= 4.0 and external_score > self_score:
        model = "external_service"
    elif self_score >= 2.5 and self_score > external_score:
        model = "self_hosted"
    else:
        model = "unknown"
    confidence = min(0.98, 0.45 + max(external_score, self_score) * 0.07)
    if model == "unknown":
        confidence = 0.35
        reasons.append("insufficient_model_evidence")
    elif abs(external_score - self_score) < 1.0 and model != "hybrid":
        confidence = min(confidence, 0.60)
        reasons.append("model_scores_close")
    return (
        model,
        confidence,
        external_score,
        self_score,
        sorted(set(reasons)),
        evidence,
    )


def extract_placement_graph(
    html: str,
    *,
    page_url: str,
    registered_domain: str,
    terms: Mapping[str, Any] | None = None,
    max_links: int = 20_000,
) -> dict[str, Any]:
    """Extract bounded outbound evidence and classify the placement model."""
    parser = lxml_html.HTMLParser(recover=True, remove_comments=True)
    try:
        document = lxml_html.fromstring(html, parser=parser, base_url=page_url)
    except (etree.ParserError, ValueError):
        document = lxml_html.fromstring("<html></html>", parser=parser)
    for node in document.xpath("//script|//style|//noscript|//svg"):
        parent = node.getparent()
        if parent is not None:
            parent.remove(node)
    text = _compact(" ".join(document.itertext()), 3_000_000)
    external_service_context = bool(EXTERNAL_SERVICE_RE.search(text))
    links: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for anchor_node in document.xpath("//a[@href]")[:max_links]:
        destination = _normalized_url(anchor_node.get("href") or "", page_url)
        if not destination:
            continue
        destination_domain = _registered_domain(destination)
        same_domain = destination_domain == registered_domain.lower()
        anchor = _compact(anchor_node.text_content(), 300)
        context_node = next(
            (
                ancestor
                for ancestor in anchor_node.iterancestors()
                if str(ancestor.tag).lower() in {"p", "li", "article", "section", "div"}
            ),
            anchor_node.getparent(),
        )
        context = _compact(
            context_node.text_content() if context_node is not None else anchor, 700
        )
        section = next(
            (
                str(ancestor.tag).lower()
                for ancestor in anchor_node.iterancestors()
                if str(ancestor.tag).lower()
                in {"article", "main", "aside", "nav", "footer", "header"}
            ),
            "body",
        )
        rel = _compact(anchor_node.get("rel"), 200).lower()
        key = (destination, anchor, rel)
        if key in seen:
            continue
        seen.add(key)
        role, role_confidence, role_reasons = _classify_link(
            same_domain=same_domain,
            destination_domain=destination_domain,
            anchor=anchor,
            context=context,
            external_service_context=external_service_context,
        )
        links.append(
            {
                "destination_url": destination,
                "destination_registered_domain": destination_domain or None,
                "same_registered_domain": int(same_domain),
                "anchor_text": anchor or None,
                "rel": rel or None,
                "context_text": context or None,
                "dom_section": section,
                "link_role": role,
                "role_confidence": role_confidence,
                "role_reasons": role_reasons,
            }
        )
    (
        model,
        confidence,
        external_score,
        self_score,
        model_reasons,
        model_evidence,
    ) = _model_from_evidence(text=text, terms=terms or {}, links=links)
    return {
        "placement_model": model,
        "model_confidence": confidence,
        "external_score": external_score,
        "self_hosted_score": self_score,
        "model_reasons": model_reasons,
        "model_evidence": model_evidence,
        "links": links,
    }


def _input_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_rows(
    source_db: Path, terms_db: Path, scores_db: Path
) -> list[dict[str, Any]]:
    source = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    terms = sqlite3.connect(f"file:{terms_db}?mode=ro", uri=True)
    terms.row_factory = sqlite3.Row
    scores = sqlite3.connect(f"file:{scores_db}?mode=ro", uri=True)
    scores.row_factory = sqlite3.Row
    try:
        term_rows = {
            str(row["url"]): dict(row)
            for row in terms.execute("SELECT * FROM placement_terms")
        }
        score_rows = {
            str(row["url"]): dict(row)
            for row in scores.execute("SELECT * FROM page_scores")
        }
        rows: list[dict[str, Any]] = []
        for row in source.execute(
            "SELECT * FROM outreach_pages ORDER BY registered_domain,url"
        ):
            value = dict(row)
            value["terms"] = term_rows.get(str(row["url"]), {})
            value["scores"] = score_rows.get(str(row["url"]), {})
            rows.append(value)
        return rows
    finally:
        source.close()
        terms.close()
        scores.close()


def _init_db(
    path: Path, identities: Mapping[str, str], config: PlacementConfig
) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path), timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(PLACEMENT_SCHEMA)
    identity = {
        "schema_version": str(SCHEMA_VERSION),
        "extractor_version": str(EXTRACTOR_VERSION),
        "config": _json(asdict(config)),
        **identities,
    }
    existing = dict(connection.execute("SELECT key,value FROM placement_meta"))
    if existing:
        mismatches = {
            key: (existing.get(key), value)
            for key, value in identity.items()
            if existing.get(key) != value
        }
        if mismatches:
            connection.close()
            raise ValueError(f"placement graph resume identity mismatch: {mismatches}")
    else:
        with connection:
            connection.executemany(
                "INSERT INTO placement_meta(key,value) VALUES (?,?)",
                sorted(identity.items()),
            )
    return connection


def _fetch_page(row: Mapping[str, Any], config: PlacementConfig) -> dict[str, Any]:
    from botocore.exceptions import (  # type: ignore[import-untyped]
        BotoCoreError,
        ClientError,
    )
    from requests.exceptions import RequestException
    from warcio.exceptions import ArchiveLoadFailed  # type: ignore[import-untyped]

    from cc_links.fetch import fetch_warc_record
    from cc_links.outreach_enrich import parse_warc_html

    last_error: Exception | None = None
    for attempt in range(config.retries + 1):
        try:
            raw = fetch_warc_record(
                str(row["warc_filename"]),
                int(row["warc_record_offset"]),
                int(row["warc_record_length"]),
            )
            page_html, _, _ = parse_warc_html(raw, config.max_html_bytes)
            if page_html is None:
                raise ValueError("WARC record is not HTML")
            return {
                "fetch_status": "ok",
                **extract_placement_graph(
                    page_html,
                    page_url=str(row["url"]),
                    registered_domain=str(row["registered_domain"]),
                    terms=row.get("terms")
                    if isinstance(row.get("terms"), Mapping)
                    else {},
                    max_links=config.max_links_per_page,
                ),
            }
        except (
            ArchiveLoadFailed,
            BotoCoreError,
            ClientError,
            RequestException,
            EOFError,
            TimeoutError,
            OSError,
            ValueError,
        ) as exc:
            last_error = exc
            if attempt < config.retries:
                time.sleep(config.retry_backoff * (2**attempt))
    assert last_error is not None
    return {
        "fetch_status": "error",
        "placement_model": "unknown",
        "model_confidence": 0.0,
        "external_score": 0.0,
        "self_hosted_score": 0.0,
        "model_reasons": ["fetch_error"],
        "model_evidence": [],
        "links": [],
        "error_kind": type(last_error).__name__,
        "error_detail": str(last_error)[:500],
    }


def _service_id(domain: str) -> str:
    return _stable_id("svc", domain.lower())


def _save_page(
    connection: sqlite3.Connection, row: Mapping[str, Any], result: Mapping[str, Any]
) -> None:
    service_id = _service_id(str(row["registered_domain"]))
    raw_terms = row.get("terms")
    raw_scores = row.get("scores")
    terms: Mapping[str, Any] = raw_terms if isinstance(raw_terms, Mapping) else {}
    scores: Mapping[str, Any] = raw_scores if isinstance(raw_scores, Mapping) else {}
    observed_at = str(row.get("fetch_time") or _utc_now())
    with connection:
        connection.execute(
            """INSERT OR IGNORE INTO services(
                   service_id,registered_domain,canonical_url,placement_model,
                   model_confidence,model_reasons,page_count,successful_page_count,
                   example_placement_count,publisher_domain_count,reliability_status,
                   reliability_score,updated_at)
               VALUES (?,?,?,'unknown',0,'[]',0,0,0,0,'pending_domain_metrics',NULL,?)""",
            (service_id, row["registered_domain"], row["url"], _utc_now()),
        )
        connection.execute(
            """INSERT OR REPLACE INTO service_pages VALUES (
                   ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                row["url"],
                service_id,
                row["registered_domain"],
                row["crawl"],
                row.get("fetch_time"),
                result["fetch_status"],
                result["placement_model"],
                result["model_confidence"],
                result["external_score"],
                result["self_hosted_score"],
                _json(result.get("model_reasons", [])),
                _json(result.get("model_evidence", [])),
                terms.get("promise_level"),
                terms.get("promise_probability"),
                terms.get("placement_types", "[]"),
                terms.get("commercial_model"),
                terms.get("price_status"),
                terms.get("price_min"),
                terms.get("price_max"),
                terms.get("currency"),
                scores.get("combined_score"),
                scores.get("score_band"),
                scores.get("best_lastmod"),
                scores.get("best_lastmod_source"),
                result.get("error_kind"),
                result.get("error_detail"),
            ),
        )
        connection.execute(
            "DELETE FROM outbound_links WHERE source_url=?", (row["url"],)
        )
        connection.execute("DELETE FROM placements WHERE evidence_url=?", (row["url"],))
        for link in result.get("links", []):
            link_id = _stable_id(
                "lnk",
                str(row["url"]),
                str(link["destination_url"]),
                str(link.get("anchor_text") or ""),
            )
            connection.execute(
                """INSERT OR REPLACE INTO outbound_links VALUES (
                       ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    link_id,
                    service_id,
                    row["url"],
                    link["destination_url"],
                    link.get("destination_registered_domain"),
                    link["same_registered_domain"],
                    link.get("anchor_text"),
                    link.get("rel"),
                    link.get("context_text"),
                    link.get("dom_section"),
                    link["link_role"],
                    link["role_confidence"],
                    _json(link.get("role_reasons", [])),
                    "warc",
                    scores.get("best_lastmod"),
                    observed_at,
                ),
            )
            if link["link_role"] not in {"placement_example", "publisher_inventory"}:
                continue
            placement_id = _stable_id("plc", service_id, str(link["destination_url"]))
            connection.execute(
                """INSERT OR REPLACE INTO placements VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    placement_id,
                    service_id,
                    row["url"],
                    link["destination_url"],
                    link.get("destination_registered_domain"),
                    link["link_role"],
                    link["role_confidence"],
                    _json(link.get("role_reasons", [])),
                    observed_at,
                    observed_at,
                ),
            )


def _refresh_services(connection: sqlite3.Connection) -> None:
    service_ids = [
        str(row[0])
        for row in connection.execute(
            "SELECT service_id FROM services ORDER BY service_id"
        )
    ]
    with connection:
        for service_id in service_ids:
            pages = list(
                connection.execute(
                    "SELECT url,placement_model,model_confidence,model_reasons,fetch_status,page_quality_score FROM service_pages WHERE service_id=? ORDER BY url",
                    (service_id,),
                )
            )
            counts = Counter(str(row[1]) for row in pages if str(row[4]) == "ok")
            if counts.get("hybrid") or (
                counts.get("external_service") and counts.get("self_hosted")
            ):
                model = "hybrid"
            elif counts:
                model = counts.most_common(1)[0][0]
            else:
                model = "unknown"
            relevant = [float(row[2]) for row in pages if str(row[1]) == model]
            confidence = sum(relevant) / len(relevant) if relevant else 0.0
            reasons = sorted(
                {reason for row in pages for reason in json.loads(str(row[3]) or "[]")}
            )
            placement_stats = connection.execute(
                "SELECT COUNT(*),COUNT(DISTINCT publisher_registered_domain) FROM placements WHERE service_id=?",
                (service_id,),
            ).fetchone()
            canonical = min(
                (str(row[0]) for row in pages), key=lambda value: (len(value), value)
            )
            reliability_status = (
                "pending_domain_metrics"
                if model in {"external_service", "hybrid"}
                else "offline_page_evidence_only"
            )
            connection.execute(
                """UPDATE services SET canonical_url=?,placement_model=?,model_confidence=?,
                       model_reasons=?,page_count=?,successful_page_count=?,
                       example_placement_count=?,publisher_domain_count=?,
                       reliability_status=?,reliability_score=NULL,updated_at=?
                   WHERE service_id=?""",
                (
                    canonical,
                    model,
                    confidence,
                    _json(reasons),
                    len(pages),
                    sum(str(row[4]) == "ok" for row in pages),
                    int(placement_stats[0] or 0),
                    int(placement_stats[1] or 0),
                    reliability_status,
                    _utc_now(),
                    service_id,
                ),
            )


def _refresh_evaluations(connection: sqlite3.Connection) -> None:
    """Build truthful offline-only evaluations without borrowing provider metrics."""
    rows = list(
        connection.execute(
            """SELECT p.url,p.service_id,s.placement_model,p.page_quality_score,
                      p.promise_probability,p.price_status,p.price_min,p.price_max,
                      p.currency
               FROM service_pages p JOIN services s USING(service_id)
               ORDER BY p.url"""
        )
    )
    with connection:
        for row in rows:
            (
                url,
                service_id,
                model,
                page_quality,
                probability,
                price_status,
                price_min,
                price_max,
                currency,
            ) = row
            reasons = ["service_metrics_not_supplied"]
            utility: float | None = None
            cost_per_utility: float | None = None
            advertised_price: float | None = None
            if price_min is not None and price_max is not None and currency != "MIXED":
                advertised_price = (float(price_min) + float(price_max)) / 2.0
            elif str(price_status) == "free":
                advertised_price = 0.0
            if model == "self_hosted" and page_quality is not None:
                probability_value = float(probability or 0.15)
                utility = round(float(page_quality) * probability_value, 4)
                reasons.extend(
                    ["self_hosted_page_quality", "publication_probability_applied"]
                )
                if advertised_price is not None and utility > 0:
                    cost_per_utility = round(advertised_price / utility, 6)
                    status = "offline_partial_priced"
                else:
                    status = "offline_partial_unpriced"
            elif model in {"external_service", "hybrid"}:
                status = "pending_placement_metrics"
                reasons.append("provider_page_not_used_as_placement_quality")
            else:
                status = "pending_model_review"
                reasons.append("placement_model_unknown")
            connection.execute(
                """INSERT OR REPLACE INTO placement_evaluations VALUES (
                       ?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    _stable_id("eval", str(url)),
                    service_id,
                    url,
                    model,
                    None,
                    page_quality if model == "self_hosted" else None,
                    probability,
                    advertised_price,
                    currency,
                    utility,
                    cost_per_utility,
                    status,
                    _json(reasons),
                    _utc_now(),
                ),
            )


def build_placement_graph(
    *,
    source_db: str | Path,
    terms_db: str | Path,
    scores_db: str | Path,
    out_db: str | Path,
    workers: int = 24,
    max_pages: int | None = None,
    fetch_source: str = "s3",
    config: PlacementConfig | None = None,
    progress: Callable[[str], None] | None = None,
) -> PlacementSummary:
    """Build a resumable graph from exact archived outreach-page HTML."""
    if workers < 1:
        raise ValueError("workers must be positive")
    if max_pages is not None and max_pages < 1:
        raise ValueError("max_pages must be positive")
    if fetch_source not in {"s3", "https"}:
        raise ValueError("fetch_source must be s3 or https")
    config = config or PlacementConfig()
    source_path, terms_path, scores_path = map(Path, (source_db, terms_db, scores_db))
    rows = _read_rows(source_path, terms_path, scores_path)
    scheduled = rows[:max_pages] if max_pages is not None else rows
    identities = {
        "source_sha256": _input_digest(source_path),
        "terms_sha256": _input_digest(terms_path),
        "scores_sha256": _input_digest(scores_path),
    }
    connection = _init_db(Path(out_db), identities, config)
    if fetch_source == "s3":
        from cc_links.fetch import enable_s3

        enable_s3(pool_size=max(32, workers * 2))
    completed = {
        str(row[0])
        for row in connection.execute(
            "SELECT url FROM service_pages WHERE fetch_status='ok'"
        )
    }
    pending = [row for row in scheduled if str(row["url"]) not in completed]
    if progress:
        progress(f"placements: resume={len(completed)} pending={len(pending)}")
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_page, row, config): row for row in pending}
        for index, future in enumerate(as_completed(futures), start=1):
            row = futures[future]
            _save_page(connection, row, future.result())
            if progress and (index % 100 == 0 or index == len(pending)):
                elapsed = max(0.001, time.monotonic() - started)
                progress(
                    f"placements: {len(completed) + index}/{len(scheduled)} rate={index / elapsed * 60:.1f}/min"
                )
    _refresh_services(connection)
    _refresh_evaluations(connection)
    counts = connection.execute(
        """SELECT
             (SELECT COUNT(*) FROM service_pages),
             (SELECT SUM(fetch_status='ok') FROM service_pages),
             (SELECT COUNT(*) FROM services),
             (SELECT COUNT(*) FROM outbound_links),
             (SELECT COUNT(*) FROM placements)"""
    ).fetchone()
    connection.close()
    return PlacementSummary(
        input_pages=len(rows),
        scheduled_pages=len(scheduled),
        completed_pages=int(counts[0] or 0),
        successful_pages=int(counts[1] or 0),
        services=int(counts[2] or 0),
        outbound_links=int(counts[3] or 0),
        placements=int(counts[4] or 0),
    )


def build_placement_report(out_db: str | Path) -> dict[str, Any]:
    """Return deterministic graph coverage and evidence distributions."""
    connection = sqlite3.connect(f"file:{Path(out_db)}?mode=ro", uri=True)
    try:
        distribution = lambda query: {
            str(row[0]): int(row[1]) for row in connection.execute(query)
        }
        return {
            "schema_version": SCHEMA_VERSION,
            "services": int(
                connection.execute("SELECT COUNT(*) FROM services").fetchone()[0]
            ),
            "pages": int(
                connection.execute("SELECT COUNT(*) FROM service_pages").fetchone()[0]
            ),
            "outbound_links": int(
                connection.execute("SELECT COUNT(*) FROM outbound_links").fetchone()[0]
            ),
            "placements": int(
                connection.execute("SELECT COUNT(*) FROM placements").fetchone()[0]
            ),
            "evaluations": int(
                connection.execute(
                    "SELECT COUNT(*) FROM placement_evaluations"
                ).fetchone()[0]
            ),
            "service_models": distribution(
                "SELECT placement_model,COUNT(*) FROM services GROUP BY 1 ORDER BY 1"
            ),
            "page_models": distribution(
                "SELECT placement_model,COUNT(*) FROM service_pages GROUP BY 1 ORDER BY 1"
            ),
            "link_roles": distribution(
                "SELECT link_role,COUNT(*) FROM outbound_links GROUP BY 1 ORDER BY 1"
            ),
            "reliability_status": distribution(
                "SELECT reliability_status,COUNT(*) FROM services GROUP BY 1 ORDER BY 1"
            ),
            "evaluation_status": distribution(
                "SELECT evaluation_status,COUNT(*) FROM placement_evaluations "
                "GROUP BY 1 ORDER BY 1"
            ),
            "quick_check": str(connection.execute("PRAGMA quick_check").fetchone()[0]),
        }
    finally:
        connection.close()


def write_placement_outputs(
    out_db: str | Path,
    *,
    report_path: str | Path,
    export_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Write a JSON report and deterministic service/link/placement CSV files."""
    report = build_placement_report(out_db)
    report_target = Path(report_path)
    report_target.parent.mkdir(parents=True, exist_ok=True)
    report_target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if export_dir:
        target = Path(export_dir)
        target.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(f"file:{Path(out_db)}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            for table, order_by in (
                ("services", "registered_domain"),
                ("service_pages", "registered_domain,url"),
                ("outbound_links", "service_id,source_url,destination_url"),
                ("placements", "service_id,publisher_registered_domain,placement_url"),
                ("placement_evaluations", "service_id,evidence_url"),
            ):
                rows = list(
                    connection.execute(f"SELECT * FROM {table} ORDER BY {order_by}")
                )
                with (target / f"{table}.csv").open(
                    "w", encoding="utf-8", newline=""
                ) as handle:
                    if rows:
                        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
                        writer.writeheader()
                        writer.writerows(dict(row) for row in rows)
        finally:
            connection.close()
    return report


def write_impact_template(out_db: str | Path, out_csv: str | Path) -> int:
    """Export blank 7/30/90/180-day rows for later manual observations."""
    connection = sqlite3.connect(f"file:{Path(out_db)}?mode=ro", uri=True)
    try:
        placements = list(
            connection.execute(
                "SELECT placement_id,service_id,placement_url FROM placements ORDER BY placement_id"
            )
        )
    finally:
        connection.close()
    path = Path(out_csv)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "placement_id",
        "service_id",
        "placement_url",
        "project_id",
        "target_url",
        "placed_at",
        "cost",
        "currency",
        "window_days",
        "observed_at",
        "link_live",
        "indexed",
        "target_rating",
        "target_organic_traffic",
        "target_keywords",
        "target_referring_domains",
        "observation_source",
        "control_label",
        "notes",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for placement_id, service_id, placement_url in placements:
            for window in (7, 30, 90, 180):
                writer.writerow(
                    {
                        "placement_id": placement_id,
                        "service_id": service_id,
                        "placement_url": placement_url,
                        "window_days": window,
                        "observation_source": "manual",
                    }
                )
    return len(placements) * 4
