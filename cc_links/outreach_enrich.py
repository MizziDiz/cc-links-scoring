"""Resumable HTML, WARC and sitemap enrichment for outreach candidates."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import logging
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

import requests
from botocore.exceptions import BotoCoreError, ClientError
from lxml import etree
from lxml import html as lxml_html
from requests.exceptions import (
    ChunkedEncodingError,
    ContentDecodingError,
    RequestException,
    SSLError,
    Timeout,
    TooManyRedirects,
)
from requests.exceptions import (
    ConnectionError as RequestsConnectionError,
)
from warcio.archiveiterator import ArchiveIterator
from warcio.exceptions import ArchiveLoadFailed

from cc_links.engines import classify_engine, get_generator
from cc_links.fetch import enable_s3, fetch_warc_record
from cc_links.outreach_live import (
    CAREERS_RE,
    CHALLENGE_RE,
    CONTACT_LINK_RE,
    DEMO_HOST_RE,
    PARKED_RE,
    POSITIVE_RE,
    SOFT_404_RE,
    SUBMISSION_FIELD_RE,
    TAXONOMY_PATH_RE,
)

LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = 1
EXTRACTOR_VERSION = 1
DEFAULT_MAX_HTML_BYTES = 5_000_000
DEFAULT_MAX_SITEMAP_BYTES = 5_000_000

ENRICHMENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS enrichment_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS page_snapshots (
    url TEXT NOT NULL,
    source TEXT NOT NULL CHECK(source IN ('warc', 'live')),
    registered_domain TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    fetch_status TEXT NOT NULL,
    http_status INTEGER,
    final_url TEXT,
    content_type TEXT,
    response_last_modified TEXT,
    etag TEXT,
    bytes_read INTEGER NOT NULL DEFAULT 0,
    truncated INTEGER NOT NULL DEFAULT 0,
    title TEXT,
    meta_description TEXT,
    h1 TEXT,
    html_lang TEXT,
    canonical_url TEXT,
    meta_robots TEXT,
    generator TEXT,
    engine_category TEXT,
    engine_name TEXT,
    engine_signal TEXT,
    word_count INTEGER NOT NULL DEFAULT 0,
    form_count INTEGER NOT NULL DEFAULT 0,
    submission_form_count INTEGER NOT NULL DEFAULT 0,
    contact_link_count INTEGER NOT NULL DEFAULT 0,
    email_link_count INTEGER NOT NULL DEFAULT 0,
    internal_link_count INTEGER NOT NULL DEFAULT 0,
    external_link_count INTEGER NOT NULL DEFAULT 0,
    nofollow_external_count INTEGER NOT NULL DEFAULT 0,
    invitation_phrases TEXT NOT NULL DEFAULT '[]',
    form_actions TEXT NOT NULL DEFAULT '[]',
    form_fields TEXT NOT NULL DEFAULT '[]',
    contact_urls TEXT NOT NULL DEFAULT '[]',
    guideline_signals TEXT NOT NULL DEFAULT '[]',
    risk_flags TEXT NOT NULL DEFAULT '[]',
    date_modified TEXT,
    date_modified_source TEXT,
    date_published TEXT,
    date_published_source TEXT,
    date_candidates TEXT NOT NULL DEFAULT '[]',
    error_kind TEXT,
    error_detail TEXT,
    PRIMARY KEY (url, source)
);

CREATE INDEX IF NOT EXISTS idx_page_snapshots_domain
    ON page_snapshots(registered_domain);
CREATE INDEX IF NOT EXISTS idx_page_snapshots_source
    ON page_snapshots(source);
CREATE INDEX IF NOT EXISTS idx_page_snapshots_modified
    ON page_snapshots(date_modified);

CREATE TABLE IF NOT EXISTS sitemap_domains (
    registered_domain TEXT PRIMARY KEY,
    checked_at TEXT NOT NULL,
    status TEXT NOT NULL,
    documents_fetched INTEGER NOT NULL DEFAULT 0,
    sitemap_urls TEXT NOT NULL DEFAULT '[]',
    domain_latest_lastmod TEXT,
    error_kind TEXT,
    error_detail TEXT
);

CREATE TABLE IF NOT EXISTS sitemap_pages (
    url TEXT PRIMARY KEY,
    registered_domain TEXT NOT NULL,
    sitemap_url TEXT,
    lastmod TEXT,
    confidence TEXT NOT NULL,
    checked_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sitemap_pages_domain
    ON sitemap_pages(registered_domain);

CREATE VIEW IF NOT EXISTS scoring_features AS
SELECT
    w.url,
    w.registered_domain,
    CASE WHEN l.fetch_status = 'ok' THEN 'live' ELSE 'warc' END AS selected_source,
    COALESCE(NULLIF(l.title, ''), w.title) AS title,
    COALESCE(NULLIF(l.meta_description, ''), w.meta_description)
        AS meta_description,
    COALESCE(NULLIF(l.h1, ''), w.h1) AS h1,
    COALESCE(NULLIF(l.html_lang, ''), w.html_lang) AS html_lang,
    COALESCE(NULLIF(l.canonical_url, ''), w.canonical_url) AS canonical_url,
    COALESCE(NULLIF(l.meta_robots, ''), w.meta_robots) AS meta_robots,
    COALESCE(NULLIF(l.generator, ''), w.generator) AS generator,
    COALESCE(NULLIF(l.engine_category, ''), w.engine_category)
        AS engine_category,
    COALESCE(NULLIF(l.engine_name, ''), w.engine_name) AS engine_name,
    COALESCE(NULLIF(l.engine_signal, ''), w.engine_signal) AS engine_signal,
    CASE WHEN l.fetch_status = 'ok' THEN l.word_count ELSE w.word_count END
        AS word_count,
    CASE WHEN l.fetch_status = 'ok' THEN l.form_count ELSE w.form_count END
        AS form_count,
    CASE WHEN l.fetch_status = 'ok'
        THEN l.submission_form_count ELSE w.submission_form_count END
        AS submission_form_count,
    CASE WHEN l.fetch_status = 'ok'
        THEN l.contact_link_count ELSE w.contact_link_count END
        AS contact_link_count,
    CASE WHEN l.fetch_status = 'ok'
        THEN l.email_link_count ELSE w.email_link_count END AS email_link_count,
    CASE WHEN l.fetch_status = 'ok'
        THEN l.internal_link_count ELSE w.internal_link_count END
        AS internal_link_count,
    CASE WHEN l.fetch_status = 'ok'
        THEN l.external_link_count ELSE w.external_link_count END
        AS external_link_count,
    CASE WHEN l.fetch_status = 'ok'
        THEN l.nofollow_external_count ELSE w.nofollow_external_count END
        AS nofollow_external_count,
    CASE WHEN l.fetch_status = 'ok'
        THEN l.invitation_phrases ELSE w.invitation_phrases END
        AS invitation_phrases,
    CASE WHEN l.fetch_status = 'ok' THEN l.form_actions ELSE w.form_actions END
        AS form_actions,
    CASE WHEN l.fetch_status = 'ok' THEN l.form_fields ELSE w.form_fields END
        AS form_fields,
    CASE WHEN l.fetch_status = 'ok' THEN l.contact_urls ELSE w.contact_urls END
        AS contact_urls,
    CASE WHEN l.fetch_status = 'ok'
        THEN l.guideline_signals ELSE w.guideline_signals END
        AS guideline_signals,
    CASE WHEN l.fetch_status = 'ok' THEN l.risk_flags ELSE w.risk_flags END
        AS risk_flags,
    COALESCE(sp.lastmod, l.date_modified, w.date_modified)
        AS best_lastmod,
    CASE
        WHEN sp.lastmod IS NOT NULL THEN 'sitemap_exact'
        WHEN l.date_modified IS NOT NULL THEN 'live_html_or_header'
        WHEN w.date_modified IS NOT NULL THEN 'warc_html_or_header'
    END AS best_lastmod_source,
    sp.confidence AS sitemap_lastmod_confidence,
    COALESCE(l.date_published, w.date_published) AS date_published,
    l.http_status AS live_http_status,
    l.response_last_modified AS live_http_last_modified,
    w.response_last_modified AS warc_http_last_modified,
    l.fetch_status AS live_fetch_status,
    w.fetch_status AS warc_fetch_status
FROM page_snapshots w
LEFT JOIN page_snapshots l ON l.url = w.url AND l.source = 'live'
LEFT JOIN sitemap_pages sp ON sp.url = w.url
WHERE w.source = 'warc';
"""

GUIDELINE_TERMS = (
    "word count",
    "original content",
    "editorial review",
    "submission guidelines",
    "author bio",
    "backlink",
    "do-follow",
    "dofollow",
    "sponsored post",
    "guest post fee",
    "plagiarism",
    "ai-generated",
)
MODIFIED_META_KEYS = {
    "article:modified_time",
    "og:updated_time",
    "last-modified",
    "last_modified",
    "datemodified",
    "date.modified",
}
PUBLISHED_META_KEYS = {
    "article:published_time",
    "datepublished",
    "date",
    "publish-date",
    "publish_date",
}


@dataclass(frozen=True)
class EnrichmentConfig:
    """Material settings for a resumable enrichment run."""

    max_html_bytes: int = DEFAULT_MAX_HTML_BYTES
    max_sitemap_bytes: int = DEFAULT_MAX_SITEMAP_BYTES
    timeout: float = 15.0
    retries: int = 2
    retry_backoff: float = 1.0
    max_sitemap_documents: int = 4
    user_agent: str = (
        "cc-outreach-enrichment/1.0 "
        "(read-only research crawler; no forms or authentication)"
    )


@dataclass(frozen=True)
class EnrichmentSummary:
    """Counters returned by the enrichment pipeline."""

    input_pages: int
    warc_snapshots: int
    live_snapshots: int
    live_success: int
    sitemap_domains: int
    exact_sitemap_lastmods: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _bounded_text(value: str | None, limit: int) -> str | None:
    if not value:
        return None
    compact = re.sub(r"\s+", " ", value).strip()
    return compact[:limit] or None


def _normalize_url(value: str) -> str:
    clean, _ = urldefrag(value.strip())
    parsed = urlparse(clean)
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        path=path,
        query="",
        fragment="",
    ).geturl()


def _parse_date(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or len(text) > 100:
        return None
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        parsed = None
    if parsed is None:
        candidate = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            match = re.search(r"\b(19|20)\d{2}-\d{2}-\d{2}\b", text)
            if not match:
                return None
            try:
                parsed = datetime.fromisoformat(match.group(0))
            except ValueError:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    if not 1990 <= parsed.year <= datetime.now(timezone.utc).year + 1:
        return None
    return parsed.replace(microsecond=0).isoformat()


def _walk_json(value: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            yield str(key), child
            yield from _walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_json(child)


def _best_date(
    candidates: Sequence[dict[str, str]], kind: str
) -> tuple[str | None, str | None]:
    relevant = [
        row for row in candidates if row.get("kind") == kind and row.get("value")
    ]
    if not relevant:
        return None, None
    priority = {
        "jsonld": 5,
        "meta": 4,
        "time": 3,
        "http_header": 2,
    }
    relevant.sort(
        key=lambda row: (priority.get(row.get("source", ""), 0), row["value"]),
        reverse=True,
    )
    return relevant[0]["value"], relevant[0]["source"]


def extract_html_features(
    html: str,
    *,
    page_url: str,
    registered_domain: str,
    response_headers: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Extract bounded scoring evidence without retaining the HTML body."""
    headers = {key.lower(): value for key, value in (response_headers or {}).items()}
    parser = lxml_html.HTMLParser(recover=True, remove_comments=True)
    try:
        document = lxml_html.fromstring(html, parser=parser, base_url=page_url)
    except (etree.ParserError, ValueError):
        document = lxml_html.fromstring("<html></html>", parser=parser)

    def first_text(xpath: str, limit: int) -> str | None:
        values = document.xpath(xpath)
        if not values:
            return None
        value = values[0]
        if hasattr(value, "text_content"):
            value = value.text_content()
        return _bounded_text(str(value), limit)

    title = first_text("//title", 500)
    h1 = first_text("//h1", 500)
    html_lang = first_text("/html/@lang", 40)
    canonical = first_text(
        "//link[contains(translate(@rel,'CANONICAL','canonical'),'canonical')]/@href",
        2_000,
    )
    if canonical:
        canonical = urljoin(page_url, canonical)

    metas: dict[str, list[str]] = defaultdict(list)
    for element in document.xpath("//meta[@content]"):
        key = (
            element.get("property")
            or element.get("name")
            or element.get("itemprop")
            or ""
        ).strip().lower()
        content = (element.get("content") or "").strip()
        if key and content:
            metas[key].append(content)
    description = _bounded_text(
        (metas.get("description") or metas.get("og:description") or [""])[0], 1_000
    )
    meta_robots = _bounded_text(
        ",".join(metas.get("robots", []) + metas.get("googlebot", [])), 500
    )

    date_candidates: list[dict[str, str]] = []
    header_lastmod = _parse_date(headers.get("last-modified"))
    if header_lastmod:
        date_candidates.append(
            {"kind": "modified", "source": "http_header", "value": header_lastmod}
        )
    for key, values in metas.items():
        kind = (
            "modified"
            if key in MODIFIED_META_KEYS
            else "published"
            if key in PUBLISHED_META_KEYS
            else None
        )
        if kind:
            for value in values:
                parsed = _parse_date(value)
                if parsed:
                    date_candidates.append(
                        {"kind": kind, "source": "meta", "value": parsed, "key": key}
                    )
    for script in document.xpath(
        "//script[contains(translate(@type,'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
        "'abcdefghijklmnopqrstuvwxyz'),'ld+json')]"
    ):
        raw = script.text or ""
        if len(raw) > 1_000_000:
            continue
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        for key, value in _walk_json(payload):
            lowered = key.lower()
            kind = (
                "modified"
                if lowered == "datemodified"
                else "published"
                if lowered == "datepublished"
                else None
            )
            if kind:
                parsed = _parse_date(value)
                if parsed:
                    date_candidates.append(
                        {
                            "kind": kind,
                            "source": "jsonld",
                            "value": parsed,
                            "key": key,
                        }
                    )
    for element in document.xpath("//time[@datetime]")[:50]:
        parsed = _parse_date(element.get("datetime"))
        if not parsed:
            continue
        context = " ".join(
            filter(
                None,
                [
                    element.get("class"),
                    element.get("itemprop"),
                    element.get("title"),
                ],
            )
        ).lower()
        kind = "modified" if "modif" in context or "updated" in context else "published"
        date_candidates.append({"kind": kind, "source": "time", "value": parsed})
    date_modified, date_modified_source = _best_date(date_candidates, "modified")
    date_published, date_published_source = _best_date(date_candidates, "published")

    positive_phrase_set: set[str] = set()
    for match in POSITIVE_RE.finditer(html[:5_000_000]):
        phrase = _bounded_text(match.group(0).lower(), 120)
        if phrase:
            positive_phrase_set.add(phrase)
    positive_phrases = sorted(positive_phrase_set)
    lower_html = html[:5_000_000].lower()
    guideline_signals = sorted(term for term in GUIDELINE_TERMS if term in lower_html)
    risk_flags: list[str] = []
    checks = (
        ("parked_or_for_sale", PARKED_RE.search(html)),
        ("soft_404", SOFT_404_RE.search(html)),
        ("careers_context", CAREERS_RE.search(html)),
        ("anti_bot_challenge", CHALLENGE_RE.search(html[:200_000])),
        ("taxonomy_path", TAXONOMY_PATH_RE.search(urlparse(page_url).path)),
        ("demo_host", DEMO_HOST_RE.search(urlparse(page_url).hostname or "")),
    )
    risk_flags.extend(name for name, matched in checks if matched)

    forms = document.xpath("//form")
    form_actions: set[str] = set()
    form_fields: set[str] = set()
    submission_forms = 0
    for form in forms[:30]:
        action = form.get("action")
        if action:
            form_actions.add(urljoin(page_url, action)[:2_000])
        fields = {
            (node.get("name") or node.get("id") or "").strip().lower()
            for node in form.xpath(".//input|.//textarea|.//select")
        }
        fields.discard("")
        form_fields.update(list(fields)[:100])
        serialized = etree.tostring(form, encoding="unicode")[:200_000]
        if SUBMISSION_FIELD_RE.search(serialized):
            submission_forms += 1

    contact_urls: set[str] = set()
    email_links = 0
    internal_links = 0
    external_links = 0
    nofollow_external = 0
    for anchor in document.xpath("//a[@href]")[:20_000]:
        href = (anchor.get("href") or "").strip()
        if href.lower().startswith("mailto:"):
            email_links += 1
            continue
        if not href or href.startswith(("#", "javascript:")):
            continue
        absolute = urljoin(page_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            continue
        host = (parsed.hostname or "").lower()
        same_domain = host == registered_domain or host.endswith(
            "." + registered_domain
        )
        if same_domain:
            internal_links += 1
        else:
            external_links += 1
            rel = (anchor.get("rel") or "").lower()
            if "nofollow" in rel:
                nofollow_external += 1
        if CONTACT_LINK_RE.search(f'href="{absolute}"'):
            contact_urls.add(absolute[:2_000])

    visible_text = _bounded_text(document.text_content(), 2_000_000) or ""
    word_count = len(re.findall(r"\b[\w'-]+\b", visible_text, re.UNICODE))
    generator = get_generator(html)
    engine_category, engine_name, engine_signal = classify_engine(html, page_url)
    return {
        "title": title,
        "meta_description": description,
        "h1": h1,
        "html_lang": html_lang,
        "canonical_url": canonical,
        "meta_robots": meta_robots,
        "generator": generator or None,
        "engine_category": engine_category,
        "engine_name": engine_name,
        "engine_signal": engine_signal,
        "word_count": word_count,
        "form_count": len(forms),
        "submission_form_count": submission_forms,
        "contact_link_count": len(contact_urls),
        "email_link_count": email_links,
        "internal_link_count": internal_links,
        "external_link_count": external_links,
        "nofollow_external_count": nofollow_external,
        "invitation_phrases": positive_phrases,
        "form_actions": sorted(form_actions)[:30],
        "form_fields": sorted(form_fields)[:100],
        "contact_urls": sorted(contact_urls)[:30],
        "guideline_signals": guideline_signals,
        "risk_flags": sorted(set(risk_flags)),
        "date_modified": date_modified,
        "date_modified_source": date_modified_source,
        "date_published": date_published,
        "date_published_source": date_published_source,
        "date_candidates": date_candidates[:100],
    }


def parse_warc_html(
    raw: bytes, max_html_bytes: int
) -> tuple[str | None, dict[str, str], bool]:
    """Extract one bounded HTML response and its HTTP headers from a WARC record."""
    for record in ArchiveIterator(io.BytesIO(raw)):
        if record.rec_type != "response":
            continue
        headers = {
            str(name): str(value)
            for name, value in (record.http_headers.headers if record.http_headers else [])
        }
        if record.http_headers:
            headers["X-WARC-HTTP-Status"] = (
                record.http_headers.get_statuscode() or ""
            )
        content_type = headers.get("Content-Type", headers.get("content-type", ""))
        stream = record.content_stream()
        body = stream.read(max_html_bytes + 1)
        truncated = len(body) > max_html_bytes
        body = body[:max_html_bytes]
        if "html" not in content_type.lower() and b"<html" not in body[:4_096].lower():
            return None, headers, truncated
        charset_match = re.search(r"charset=([\w.-]+)", content_type, re.IGNORECASE)
        encoding = charset_match.group(1) if charset_match else "utf-8"
        try:
            decoded = body.decode(encoding, errors="replace")
        except LookupError:
            decoded = body.decode("utf-8", errors="replace")
        return decoded, headers, truncated
    return None, {}, False


def _snapshot_failure(
    row: Mapping[str, Any],
    source: str,
    error: Exception | str,
    *,
    status: str = "error",
) -> dict[str, Any]:
    error_kind = type(error).__name__ if isinstance(error, Exception) else status
    return {
        "url": str(row["url"]),
        "source": source,
        "registered_domain": str(row["registered_domain"]),
        "fetched_at": _utc_now(),
        "fetch_status": status,
        "bytes_read": 0,
        "truncated": 0,
        "error_kind": error_kind,
        "error_detail": str(error)[:500],
    }


def fetch_warc_snapshot(
    row: Mapping[str, Any], config: EnrichmentConfig
) -> dict[str, Any]:
    """Fetch and extract one archived snapshot by exact S3 WARC range."""
    try:
        raw = fetch_warc_record(
            str(row["warc_filename"]),
            int(row["warc_record_offset"]),
            int(row["warc_record_length"]),
        )
        page_html, headers, truncated = parse_warc_html(
            raw, config.max_html_bytes
        )
        if page_html is None:
            return _snapshot_failure(row, "warc", "non_html_warc", status="non_html")
        features = extract_html_features(
            page_html,
            page_url=str(row["url"]),
            registered_domain=str(row["registered_domain"]),
            response_headers=headers,
        )
        return {
            "url": str(row["url"]),
            "source": "warc",
            "registered_domain": str(row["registered_domain"]),
            "fetched_at": str(row.get("fetch_time") or _utc_now()),
            "fetch_status": "ok",
            "http_status": (
                int(headers["X-WARC-HTTP-Status"])
                if headers.get("X-WARC-HTTP-Status", "").isdigit()
                else None
            ),
            "final_url": str(row["url"]),
            "content_type": headers.get("Content-Type"),
            "response_last_modified": headers.get("Last-Modified"),
            "etag": headers.get("ETag"),
            "bytes_read": len(page_html.encode("utf-8", errors="replace")),
            "truncated": int(truncated),
            **features,
        }
    except (
        ArchiveLoadFailed,
        BotoCoreError,
        ClientError,
        EOFError,
        TimeoutError,
        OSError,
        ValueError,
    ) as exc:
        return _snapshot_failure(row, "warc", exc)


def _live_request(
    row: Mapping[str, Any], config: EnrichmentConfig
) -> dict[str, Any]:
    session = requests.Session()
    last_error: RequestException | None = None
    started = time.monotonic()
    try:
        for attempt in range(1, config.retries + 2):
            try:
                response = session.get(
                    str(row["url"]),
                    timeout=(min(10.0, config.timeout), config.timeout),
                    allow_redirects=True,
                    headers={
                        "User-Agent": config.user_agent,
                        "Accept": "text/html,application/xhtml+xml",
                    },
                    stream=True,
                )
                chunks: list[bytes] = []
                total = 0
                truncated = False
                try:
                    for chunk in response.iter_content(65_536):
                        if not chunk:
                            continue
                        remaining = config.max_html_bytes - total
                        if remaining <= 0:
                            truncated = True
                            break
                        chunks.append(chunk[:remaining])
                        total += min(len(chunk), remaining)
                        if len(chunk) > remaining:
                            truncated = True
                            break
                finally:
                    response.close()
                payload = b"".join(chunks)
                content_type = response.headers.get("content-type", "")
                if (
                    not 200 <= response.status_code < 300
                    or (
                        "html" not in content_type.lower()
                        and b"<html" not in payload[:4_096].lower()
                    )
                ):
                    return {
                        **_snapshot_failure(
                            row,
                            "live",
                            f"http_{response.status_code}:{content_type}",
                            status="http_or_non_html",
                        ),
                        "http_status": response.status_code,
                        "final_url": response.url,
                        "content_type": content_type,
                        "bytes_read": len(payload),
                        "truncated": int(truncated),
                    }
                encoding = response.encoding or "utf-8"
                try:
                    page_html = payload.decode(encoding, errors="replace")
                except LookupError:
                    page_html = payload.decode("utf-8", errors="replace")
                features = extract_html_features(
                    page_html,
                    page_url=response.url,
                    registered_domain=str(row["registered_domain"]),
                    response_headers=response.headers,
                )
                return {
                    "url": str(row["url"]),
                    "source": "live",
                    "registered_domain": str(row["registered_domain"]),
                    "fetched_at": _utc_now(),
                    "fetch_status": "ok",
                    "http_status": response.status_code,
                    "final_url": response.url,
                    "content_type": content_type,
                    "response_last_modified": response.headers.get("last-modified"),
                    "etag": response.headers.get("etag"),
                    "bytes_read": len(payload),
                    "truncated": int(truncated),
                    **features,
                }
            except (
                Timeout,
                RequestsConnectionError,
                TooManyRedirects,
                ChunkedEncodingError,
                ContentDecodingError,
                SSLError,
            ) as exc:
                last_error = exc
                if attempt <= config.retries:
                    time.sleep(config.retry_backoff * (2 ** (attempt - 1)))
        assert last_error is not None
        return _snapshot_failure(row, "live", last_error)
    finally:
        session.close()
        LOGGER.debug(
            "live HTML %s completed in %.2fs",
            row["registered_domain"],
            time.monotonic() - started,
        )


def _decode_xml(payload: bytes, max_bytes: int) -> tuple[bytes, bool]:
    if payload[:2] != b"\x1f\x8b":
        return payload[:max_bytes], len(payload) > max_bytes
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as handle:
            decoded = handle.read(max_bytes + 1)
    except (gzip.BadGzipFile, EOFError, OSError):
        return b"", False
    return decoded[:max_bytes], len(decoded) > max_bytes


def parse_sitemap_document(
    payload: bytes, *, max_bytes: int
) -> tuple[str, list[tuple[str, str | None]], bool]:
    """Return sitemap kind, URL/lastmod rows and truncation status."""
    decoded, truncated = _decode_xml(payload, max_bytes)
    if not decoded:
        return "invalid", [], truncated
    try:
        root = ET.fromstring(decoded)
    except ET.ParseError:
        return "invalid", [], truncated
    kind = root.tag.rsplit("}", 1)[-1].lower()
    rows: list[tuple[str, str | None]] = []
    child_name = "sitemap" if kind == "sitemapindex" else "url"
    for child in root:
        if child.tag.rsplit("}", 1)[-1].lower() != child_name:
            continue
        location = None
        lastmod = None
        for value in child:
            name = value.tag.rsplit("}", 1)[-1].lower()
            if name == "loc":
                location = (value.text or "").strip()
            elif name == "lastmod":
                lastmod = _parse_date(value.text)
        if location:
            rows.append((location, lastmod))
    return kind, rows, truncated


def _get_limited(
    session: requests.Session,
    url: str,
    config: EnrichmentConfig,
    limit: int,
) -> tuple[int, str, bytes]:
    response = session.get(
        url,
        timeout=(min(10.0, config.timeout), config.timeout),
        allow_redirects=True,
        headers={
            "User-Agent": config.user_agent,
            "Accept": "application/xml,text/xml,text/plain;q=0.8",
        },
        stream=True,
    )
    try:
        payload = response.raw.read(limit + 1, decode_content=True)
    finally:
        response.close()
    return response.status_code, response.url, payload


def check_domain_sitemaps(
    domain: str,
    urls: Sequence[str],
    config: EnrichmentConfig,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Inspect a bounded set of sitemap documents for exact page lastmods."""
    origins = sorted(
        {
            f"{parsed.scheme}://{parsed.netloc}"
            for parsed in map(urlparse, urls)
            if parsed.scheme in ("http", "https") and parsed.netloc
        }
    )
    session = requests.Session()
    candidates: list[str] = []
    errors: list[str] = []
    fetched = 0
    all_rows: list[tuple[str, str | None, str]] = []
    try:
        for origin in origins:
            try:
                status, _, robots = _get_limited(
                    session, origin + "/robots.txt", config, 256_000
                )
                if status == 200:
                    text = robots.decode("utf-8", errors="replace")
                    candidates.extend(
                        line.split(":", 1)[1].strip()
                        for line in text.splitlines()
                        if line.lower().startswith("sitemap:") and ":" in line
                    )
            except (
                Timeout,
                RequestsConnectionError,
                TooManyRedirects,
                ChunkedEncodingError,
                ContentDecodingError,
                SSLError,
            ) as exc:
                errors.append(f"robots:{type(exc).__name__}")
            candidates.extend(
                [origin + "/sitemap.xml", origin + "/sitemap_index.xml"]
            )
        queue = list(dict.fromkeys(candidates))
        seen: set[str] = set()
        document_limit = config.max_sitemap_documents * max(1, len(origins))
        attempted = 0
        while queue and attempted < document_limit:
            sitemap_url = queue.pop(0)
            if sitemap_url in seen:
                continue
            seen.add(sitemap_url)
            attempted += 1
            try:
                status, final_url, payload = _get_limited(
                    session,
                    sitemap_url,
                    config,
                    config.max_sitemap_bytes,
                )
            except (
                Timeout,
                RequestsConnectionError,
                TooManyRedirects,
                ChunkedEncodingError,
                ContentDecodingError,
                SSLError,
            ) as exc:
                errors.append(f"{type(exc).__name__}:{sitemap_url[:120]}")
                continue
            if status != 200:
                continue
            kind, rows, truncated = parse_sitemap_document(
                payload, max_bytes=config.max_sitemap_bytes
            )
            if kind == "invalid":
                continue
            fetched += 1
            all_rows.extend((url, lastmod, final_url) for url, lastmod in rows)
            if kind == "sitemapindex":
                children = [url for url, _ in rows]
                children.sort(
                    key=lambda value: (
                        not bool(
                            re.search(
                                r"(page|post|blog|article|news)", value, re.IGNORECASE
                            )
                        ),
                        value,
                    )
                )
                queue.extend(children)
            if truncated:
                errors.append(f"truncated:{final_url[:120]}")
    finally:
        session.close()

    target_map = {_normalize_url(url): url for url in urls}
    page_results = []
    domain_dates: list[str] = []
    for found_url, lastmod, sitemap_url in all_rows:
        if lastmod:
            domain_dates.append(lastmod)
        normalized = _normalize_url(found_url)
        if normalized in target_map and lastmod:
            page_results.append(
                {
                    "url": target_map[normalized],
                    "registered_domain": domain,
                    "sitemap_url": sitemap_url,
                    "lastmod": lastmod,
                    "confidence": "high_exact_url",
                    "checked_at": _utc_now(),
                }
            )
    domain_result = {
        "registered_domain": domain,
        "checked_at": _utc_now(),
        "status": "ok" if fetched else "not_found_or_unreachable",
        "documents_fetched": fetched,
        "sitemap_urls": sorted(seen),
        "domain_latest_lastmod": max(domain_dates) if domain_dates else None,
        "error_kind": "partial" if errors else None,
        "error_detail": ";".join(errors)[:1_000] or None,
    }
    return domain_result, page_results


SNAPSHOT_COLUMNS = (
    "url",
    "source",
    "registered_domain",
    "fetched_at",
    "fetch_status",
    "http_status",
    "final_url",
    "content_type",
    "response_last_modified",
    "etag",
    "bytes_read",
    "truncated",
    "title",
    "meta_description",
    "h1",
    "html_lang",
    "canonical_url",
    "meta_robots",
    "generator",
    "engine_category",
    "engine_name",
    "engine_signal",
    "word_count",
    "form_count",
    "submission_form_count",
    "contact_link_count",
    "email_link_count",
    "internal_link_count",
    "external_link_count",
    "nofollow_external_count",
    "invitation_phrases",
    "form_actions",
    "form_fields",
    "contact_urls",
    "guideline_signals",
    "risk_flags",
    "date_modified",
    "date_modified_source",
    "date_published",
    "date_published_source",
    "date_candidates",
    "error_kind",
    "error_detail",
)


def _save_snapshot(connection: sqlite3.Connection, row: Mapping[str, Any]) -> None:
    values = dict(row)
    for key in (
        "bytes_read",
        "truncated",
        "word_count",
        "form_count",
        "submission_form_count",
        "contact_link_count",
        "email_link_count",
        "internal_link_count",
        "external_link_count",
        "nofollow_external_count",
    ):
        values.setdefault(key, 0)
    for key in (
        "invitation_phrases",
        "form_actions",
        "form_fields",
        "contact_urls",
        "guideline_signals",
        "risk_flags",
        "date_candidates",
    ):
        values[key] = _json(values.get(key, []))
    placeholders = ",".join("?" for _ in SNAPSHOT_COLUMNS)
    with connection:
        connection.execute(
            f"INSERT OR REPLACE INTO page_snapshots "
            f"({','.join(SNAPSHOT_COLUMNS)}) VALUES ({placeholders})",
            tuple(values.get(column) for column in SNAPSHOT_COLUMNS),
        )


def _input_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _init_db(
    path: Path,
    *,
    input_db: Path,
    validation_db: Path,
    config: EnrichmentConfig,
) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path), timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.executescript(ENRICHMENT_SCHEMA)
    identity = {
        "schema_version": str(SCHEMA_VERSION),
        "extractor_version": str(EXTRACTOR_VERSION),
        "input_sha256": _input_digest(input_db),
        "validation_sha256": _input_digest(validation_db),
        "config": _json(asdict(config)),
    }
    existing = dict(connection.execute("SELECT key,value FROM enrichment_meta"))
    if existing:
        mismatches = {
            key: (existing.get(key), value)
            for key, value in identity.items()
            if existing.get(key) != value
        }
        if mismatches:
            connection.close()
            raise ValueError(f"enrichment resume identity mismatch: {mismatches}")
    else:
        with connection:
            connection.executemany(
                "INSERT INTO enrichment_meta(key,value) VALUES (?,?)",
                sorted(identity.items()),
            )
    return connection


def _load_rows(
    input_db: Path, validation_db: Path
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    source = sqlite3.connect(f"file:{input_db}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    try:
        pages = [
            dict(row)
            for row in source.execute(
                """
                SELECT url, registered_domain, fetch_time, warc_filename,
                       warc_record_offset, warc_record_length
                FROM outreach_pages ORDER BY registered_domain,url
                """
            )
        ]
    finally:
        source.close()
    live = sqlite3.connect(f"file:{validation_db}?mode=ro", uri=True)
    live.row_factory = sqlite3.Row
    try:
        checks = {
            str(row["url"]): dict(row)
            for row in live.execute(
                "SELECT url,robots_status,http_status,outcome FROM page_checks"
            )
        }
    finally:
        live.close()
    return pages, checks


def _run_snapshot_phase(
    *,
    connection: sqlite3.Connection,
    rows: Sequence[dict[str, Any]],
    source: str,
    workers: int,
    worker: Callable[[Mapping[str, Any], EnrichmentConfig], dict[str, Any]],
    config: EnrichmentConfig,
    progress: Callable[[str], None] | None,
) -> None:
    completed = {
        str(row[0])
        for row in connection.execute(
            "SELECT url FROM page_snapshots WHERE source=?", (source,)
        )
    }
    pending = [row for row in rows if str(row["url"]) not in completed]
    if progress:
        progress(f"{source}: resume={len(completed)} pending={len(pending)}")
    started = time.monotonic()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(worker, row, config): row for row in pending}
        for future in as_completed(futures):
            result = future.result()
            _save_snapshot(connection, result)
            done += 1
            if progress and (done == len(pending) or done % 100 == 0):
                elapsed = max(0.001, time.monotonic() - started)
                progress(
                    f"{source}: {len(completed)+done}/{len(rows)} "
                    f"rate={done/elapsed*60:.1f}/min"
                )


def _run_sitemap_phase(
    *,
    connection: sqlite3.Connection,
    rows: Sequence[dict[str, Any]],
    validation: Mapping[str, Mapping[str, Any]],
    workers: int,
    config: EnrichmentConfig,
    progress: Callable[[str], None] | None,
) -> None:
    grouped: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        check = validation.get(str(row["url"]), {})
        if str(check.get("robots_status", "")).startswith("allowed"):
            grouped[str(row["registered_domain"])].append(str(row["url"]))
    completed = {
        str(row[0]) for row in connection.execute("SELECT registered_domain FROM sitemap_domains")
    }
    pending = {
        domain: urls for domain, urls in grouped.items() if domain not in completed
    }
    if progress:
        progress(
            f"sitemap: resume={len(completed)} pending={len(pending)} domains"
        )
    started = time.monotonic()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(check_domain_sitemaps, domain, urls, config): domain
            for domain, urls in pending.items()
        }
        for future in as_completed(futures):
            domain_result, page_results = future.result()
            with connection:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO sitemap_domains (
                        registered_domain,checked_at,status,documents_fetched,
                        sitemap_urls,domain_latest_lastmod,error_kind,error_detail
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        domain_result["registered_domain"],
                        domain_result["checked_at"],
                        domain_result["status"],
                        domain_result["documents_fetched"],
                        _json(domain_result["sitemap_urls"]),
                        domain_result["domain_latest_lastmod"],
                        domain_result["error_kind"],
                        domain_result["error_detail"],
                    ),
                )
                connection.executemany(
                    """
                    INSERT OR REPLACE INTO sitemap_pages (
                        url,registered_domain,sitemap_url,lastmod,confidence,checked_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    [
                        (
                            row["url"],
                            row["registered_domain"],
                            row["sitemap_url"],
                            row["lastmod"],
                            row["confidence"],
                            row["checked_at"],
                        )
                        for row in page_results
                    ],
                )
            done += 1
            if progress and (done == len(pending) or done % 100 == 0):
                elapsed = max(0.001, time.monotonic() - started)
                progress(
                    f"sitemap: {len(completed)+done}/{len(grouped)} "
                    f"rate={done/elapsed*60:.1f} domains/min"
                )


def enrich_outreach(
    *,
    input_db: str | Path,
    validation_db: str | Path,
    out_db: str | Path,
    warc_workers: int = 24,
    live_workers: int = 20,
    sitemap_workers: int = 20,
    fetch_source: str = "s3",
    config: EnrichmentConfig | None = None,
    progress: Callable[[str], None] | None = None,
) -> EnrichmentSummary:
    """Extract archived/current HTML evidence and exact sitemap lastmods."""
    if min(warc_workers, live_workers, sitemap_workers) < 1:
        raise ValueError("worker counts must be positive")
    if fetch_source not in ("s3", "https"):
        raise ValueError("fetch_source must be s3 or https")
    config = config or EnrichmentConfig()
    input_path = Path(input_db)
    validation_path = Path(validation_db)
    rows, validation = _load_rows(input_path, validation_path)
    connection = _init_db(
        Path(out_db),
        input_db=input_path,
        validation_db=validation_path,
        config=config,
    )
    if fetch_source == "s3":
        enable_s3(pool_size=max(32, warc_workers * 2))
    _run_snapshot_phase(
        connection=connection,
        rows=rows,
        source="warc",
        workers=warc_workers,
        worker=fetch_warc_snapshot,
        config=config,
        progress=progress,
    )
    live_rows = [
        row
        for row in rows
        if str(validation.get(str(row["url"]), {}).get("robots_status", "")).startswith(
            "allowed"
        )
    ]
    _run_snapshot_phase(
        connection=connection,
        rows=live_rows,
        source="live",
        workers=live_workers,
        worker=_live_request,
        config=config,
        progress=progress,
    )
    _run_sitemap_phase(
        connection=connection,
        rows=rows,
        validation=validation,
        workers=sitemap_workers,
        config=config,
        progress=progress,
    )
    summary = EnrichmentSummary(
        input_pages=len(rows),
        warc_snapshots=int(
            connection.execute(
                "SELECT COUNT(*) FROM page_snapshots WHERE source='warc'"
            ).fetchone()[0]
        ),
        live_snapshots=int(
            connection.execute(
                "SELECT COUNT(*) FROM page_snapshots WHERE source='live'"
            ).fetchone()[0]
        ),
        live_success=int(
            connection.execute(
                "SELECT COUNT(*) FROM page_snapshots "
                "WHERE source='live' AND fetch_status='ok'"
            ).fetchone()[0]
        ),
        sitemap_domains=int(
            connection.execute("SELECT COUNT(*) FROM sitemap_domains").fetchone()[0]
        ),
        exact_sitemap_lastmods=int(
            connection.execute("SELECT COUNT(*) FROM sitemap_pages").fetchone()[0]
        ),
    )
    connection.close()
    return summary


def write_enrichment_report(
    db_path: str | Path,
    report_path: str | Path,
    export_path: str | Path | None = None,
) -> dict[str, Any]:
    """Write extraction coverage, date provenance and a scoring-ready CSV."""
    connection = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        def count(query: str) -> int:
            return int(connection.execute(query).fetchone()[0])

        report = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "quick_check": connection.execute("PRAGMA quick_check").fetchone()[0],
            "warc": {
                "total": count(
                    "SELECT COUNT(*) FROM page_snapshots WHERE source='warc'"
                ),
                "ok": count(
                    "SELECT COUNT(*) FROM page_snapshots "
                    "WHERE source='warc' AND fetch_status='ok'"
                ),
                "truncated": count(
                    "SELECT COUNT(*) FROM page_snapshots "
                    "WHERE source='warc' AND truncated=1"
                ),
            },
            "live": {
                "total": count(
                    "SELECT COUNT(*) FROM page_snapshots WHERE source='live'"
                ),
                "ok": count(
                    "SELECT COUNT(*) FROM page_snapshots "
                    "WHERE source='live' AND fetch_status='ok'"
                ),
                "truncated": count(
                    "SELECT COUNT(*) FROM page_snapshots "
                    "WHERE source='live' AND truncated=1"
                ),
            },
            "features": {
                "title": count(
                    "SELECT COUNT(*) FROM scoring_features WHERE title IS NOT NULL"
                ),
                "description": count(
                    "SELECT COUNT(*) FROM scoring_features "
                    "WHERE meta_description IS NOT NULL"
                ),
                "h1": count(
                    "SELECT COUNT(*) FROM scoring_features WHERE h1 IS NOT NULL"
                ),
                "submission_form": count(
                    "SELECT COUNT(*) FROM scoring_features "
                    "WHERE submission_form_count>0"
                ),
                "contact_link": count(
                    "SELECT COUNT(*) FROM scoring_features WHERE contact_link_count>0"
                ),
                "engine": count(
                    "SELECT COUNT(*) FROM scoring_features "
                    "WHERE engine_name IS NOT NULL"
                ),
                "best_lastmod": count(
                    "SELECT COUNT(*) FROM scoring_features "
                    "WHERE best_lastmod IS NOT NULL"
                ),
                "sitemap_exact_lastmod": count(
                    "SELECT COUNT(*) FROM sitemap_pages WHERE lastmod IS NOT NULL"
                ),
                "published_date": count(
                    "SELECT COUNT(*) FROM scoring_features "
                    "WHERE date_published IS NOT NULL"
                ),
            },
            "lastmod_sources": dict(
                connection.execute(
                    """
                    SELECT COALESCE(best_lastmod_source,'missing'),COUNT(*)
                    FROM scoring_features GROUP BY best_lastmod_source
                    """
                )
            ),
            "engine_distribution": [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT COALESCE(engine_name,'unknown') engine,COUNT(*) pages
                    FROM scoring_features GROUP BY engine_name
                    ORDER BY pages DESC LIMIT 30
                    """
                )
            ],
            "sitemap_status": dict(
                connection.execute(
                    "SELECT status,COUNT(*) FROM sitemap_domains GROUP BY status"
                )
            ),
        }
        target = Path(report_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        if export_path is not None:
            export = Path(export_path)
            export.parent.mkdir(parents=True, exist_ok=True)
            rows = connection.execute(
                "SELECT * FROM scoring_features ORDER BY registered_domain,url"
            )
            with export.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow([column[0] for column in rows.description])
                writer.writerows(rows)
        return report
    finally:
        connection.close()
