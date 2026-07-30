"""Polite, resumable GET-only qualification for outreach candidates."""

from __future__ import annotations

import csv
import hashlib
import html as html_module
import json
import logging
import re
import sqlite3
import time
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from requests import Response
from requests.exceptions import (
    ChunkedEncodingError,
    ContentDecodingError,
    RequestException,
    Timeout,
    TooManyRedirects,
)
from requests.exceptions import (
    ConnectionError as RequestsConnectionError,
)

LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = 1
CLASSIFIER_VERSION = 1
DEFAULT_USER_AGENT = (
    "cc-outreach-validator/1.0 "
    "(read-only research crawler; no forms or authentication)"
)

LIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS validation_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS page_checks (
    url TEXT PRIMARY KEY,
    registered_domain TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    elapsed_ms INTEGER NOT NULL,
    robots_status TEXT NOT NULL,
    http_status INTEGER,
    final_url TEXT,
    content_type TEXT,
    bytes_read INTEGER NOT NULL DEFAULT 0,
    outcome TEXT NOT NULL CHECK(outcome IN (
        'approved', 'review', 'rejected', 'unreachable'
    )),
    qualification_score REAL NOT NULL,
    reason_codes TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    error_kind TEXT,
    error_detail TEXT
);

CREATE INDEX IF NOT EXISTS idx_page_checks_domain
    ON page_checks(registered_domain);
CREATE INDEX IF NOT EXISTS idx_page_checks_outcome
    ON page_checks(outcome);

CREATE TABLE IF NOT EXISTS domain_checks (
    registered_domain TEXT PRIMARY KEY,
    checked_at TEXT NOT NULL,
    checked_urls INTEGER NOT NULL,
    best_url TEXT,
    status TEXT NOT NULL CHECK(status IN (
        'approved', 'review', 'rejected', 'unreachable'
    )),
    qualification_score REAL NOT NULL,
    reason_codes TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_domain_checks_status
    ON domain_checks(status);
"""

POSITIVE_PHRASES = (
    r"\bwrite\s+for\s+us\b",
    r"\bsubmit\s+(?:an?\s+)?article\b",
    r"\bbecome\s+(?:an?\s+)?contributor\b",
    r"\bguest\s+post\s+guidelines\b",
    r"\bcontributor\s+guidelines\b",
    r"\bcontribute\s+to\s+(?:our|the)\b",
    r"\bpitch\s+us\b",
    r"\bsend\s+us\s+your\s+(?:pitch|article|story)\b",
    r"\bescribe\s+para\s+nosotros\b",
    r"\benviar\s+(?:un\s+)?articulo\b",
    r"\bguia\s+para\s+autores\b",
    r"\bcolabore\s+conosco\b",
    r"\bescreva\s+para\s+nos\b",
    r"\benvie\s+(?:um\s+)?artigo\b",
)
POSITIVE_RE = re.compile("|".join(POSITIVE_PHRASES), re.IGNORECASE)
PARKED_RE = re.compile(
    r"\b(domain\s+(?:is\s+)?for\s+sale|buy\s+this\s+domain|"
    r"domain\s+parking|parkingcrew|sedo\s+domain|expired\s+domain)\b",
    re.IGNORECASE,
)
SOFT_404_RE = re.compile(
    r"\b(page\s+not\s+found|the\s+page\s+you\s+(?:requested|are\s+looking\s+for)"
    r"|content\s+is\s+no\s+longer\s+available)\b",
    re.IGNORECASE,
)
CAREERS_RE = re.compile(
    r"\b(careers?|job\s+openings?|employment\s+opportunities|join\s+our\s+team)\b",
    re.IGNORECASE,
)
FORM_RE = re.compile(r"<form\b", re.IGNORECASE)
SUBMISSION_FIELD_RE = re.compile(
    r"<(?:textarea|input)\b[^>]*(?:name|id)\s*=\s*[\"'][^\"']*"
    r"(?:article|content|story|pitch|bio|website|email)[^\"']*[\"']",
    re.IGNORECASE,
)
CONTACT_LINK_RE = re.compile(
    r"href\s*=\s*[\"'][^\"']*/(?:contact|about|editorial|team)(?:[/#?\"'])",
    re.IGNORECASE,
)
TAXONOMY_PATH_RE = re.compile(r"/(?:tag|category|author|search)(?:/|$)", re.IGNORECASE)
DEMO_HOST_RE = re.compile(r"^(?:demo|preview|themes?)\.", re.IGNORECASE)
CHALLENGE_RE = re.compile(
    r"\b(cloudflare|checking\s+your\s+browser|verify\s+you\s+are\s+human|captcha)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ValidationConfig:
    """Material settings that define a resumable validation run."""

    timeout: float = 15.0
    retries: int = 2
    retry_backoff: float = 1.0
    max_bytes: int = 1_500_000
    robots_max_bytes: int = 256_000
    user_agent: str = DEFAULT_USER_AGENT


@dataclass
class PageCheck:
    """One compact live check; no response body is retained."""

    url: str
    registered_domain: str
    checked_at: str
    attempts: int
    elapsed_ms: int
    robots_status: str
    http_status: int | None
    final_url: str | None
    content_type: str | None
    bytes_read: int
    outcome: str
    qualification_score: float
    reason_codes: list[str]
    evidence: dict[str, Any]
    error_kind: str | None = None
    error_detail: str | None = None


@dataclass(frozen=True)
class QualificationSummary:
    """Counters returned after a complete or resumed qualification run."""

    input_pages: int
    input_domains: int
    checked_pages: int
    checked_domains: int
    page_outcomes: dict[str, int]
    domain_outcomes: dict[str, int]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_visible_text(value: str) -> str:
    decoded = html_module.unescape(value)
    normalized = unicodedata.normalize("NFKD", decoded)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _same_domain(first: str, second: str, registered_domain: str) -> bool:
    first_host = (urlparse(first).hostname or "").lower()
    second_host = (urlparse(second).hostname or "").lower()
    domain = registered_domain.lower()
    return (
        (first_host == domain or first_host.endswith("." + domain))
        and (second_host == domain or second_host.endswith("." + domain))
    )


def classify_live_html(
    *,
    original_url: str,
    final_url: str,
    registered_domain: str,
    html: str,
    matched_expression: str,
) -> tuple[str, float, list[str], dict[str, Any]]:
    """Classify current HTML using explicit, auditable evidence."""
    normalized = _normalize_visible_text(html)
    final_path = urlparse(final_url).path.lower()
    final_host = (urlparse(final_url).hostname or "").lower()
    expression = matched_expression.lower().replace("_", "-")
    comparable_path = final_path.replace("_", "-")

    evidence = {
        "positive_phrase": bool(POSITIVE_RE.search(normalized)),
        "path_signal_retained": bool(expression and expression in comparable_path),
        "submission_form": bool(
            FORM_RE.search(html) and SUBMISSION_FIELD_RE.search(html)
        ),
        "contact_link": bool(CONTACT_LINK_RE.search(html)),
        "same_domain_redirect": _same_domain(
            original_url, final_url, registered_domain
        ),
        "taxonomy_path": bool(TAXONOMY_PATH_RE.search(final_path)),
        "demo_host": bool(DEMO_HOST_RE.search(final_host)),
        "careers_context": bool(CAREERS_RE.search(normalized)),
        "parked": bool(PARKED_RE.search(normalized)),
        "soft_404": bool(SOFT_404_RE.search(normalized)),
        "challenge": bool(CHALLENGE_RE.search(normalized[:100_000])),
    }

    score = 25.0
    reasons: list[str] = ["live_html_2xx"]
    if evidence["positive_phrase"]:
        score += 35
        reasons.append("current_invitation_phrase")
    if evidence["path_signal_retained"]:
        score += 20
        reasons.append("outreach_path_retained")
    if evidence["submission_form"]:
        score += 10
        reasons.append("submission_form_visible")
    if evidence["contact_link"]:
        score += 5
        reasons.append("contact_path_visible")
    if evidence["same_domain_redirect"]:
        score += 5
    else:
        score -= 15
        reasons.append("off_domain_redirect")
    if evidence["taxonomy_path"]:
        score -= 40
        reasons.append("taxonomy_or_archive_path")
    if evidence["demo_host"]:
        score -= 50
        reasons.append("demo_or_template_host")
    if evidence["careers_context"] and not evidence["positive_phrase"]:
        score -= 30
        reasons.append("careers_context")
    if evidence["parked"]:
        score -= 60
        reasons.append("parked_or_for_sale")
    if evidence["soft_404"]:
        score -= 40
        reasons.append("soft_404")
    if evidence["challenge"]:
        score -= 15
        reasons.append("anti_bot_challenge")

    score = max(0.0, min(100.0, score))
    hard_reject = any(
        evidence[key] for key in ("taxonomy_path", "demo_host", "parked", "soft_404")
    )
    if hard_reject:
        outcome = "rejected"
    elif evidence["positive_phrase"] and score >= 65:
        outcome = "approved"
    else:
        outcome = "review"
        if not evidence["positive_phrase"]:
            reasons.append("no_current_invitation_phrase")
    return outcome, score, sorted(set(reasons)), evidence


def _read_limited(response: Response, max_bytes: int) -> tuple[bytes, bool]:
    chunks: list[bytes] = []
    total = 0
    truncated = False
    for chunk in response.iter_content(chunk_size=65_536):
        if not chunk:
            continue
        remaining = max_bytes - total
        if remaining <= 0:
            truncated = True
            break
        chunks.append(chunk[:remaining])
        total += min(len(chunk), remaining)
        if len(chunk) > remaining:
            truncated = True
            break
    return b"".join(chunks), truncated


def _decode_html(payload: bytes, response: Response) -> str:
    encoding = response.encoding or "utf-8"
    try:
        return payload.decode(encoding, errors="replace")
    except LookupError:
        return payload.decode("utf-8", errors="replace")


def _request(
    session: requests.Session,
    url: str,
    config: ValidationConfig,
    *,
    max_bytes: int,
) -> tuple[Response, bytes, int]:
    last_error: RequestException | None = None
    for attempt in range(1, config.retries + 2):
        try:
            response = session.get(
                url,
                timeout=(min(10.0, config.timeout), config.timeout),
                allow_redirects=True,
                headers={
                    "User-Agent": config.user_agent,
                    "Accept": "text/html,application/xhtml+xml,text/plain;q=0.8",
                },
                stream=True,
            )
            try:
                body, _ = _read_limited(response, max_bytes)
            finally:
                response.close()
            return response, body, attempt
        except (
            Timeout,
            RequestsConnectionError,
            TooManyRedirects,
            ChunkedEncodingError,
            ContentDecodingError,
        ) as exc:
            last_error = exc
            if attempt <= config.retries:
                time.sleep(config.retry_backoff * (2 ** (attempt - 1)))
    assert last_error is not None
    raise last_error


def _robots_decision(
    session: requests.Session,
    url: str,
    config: ValidationConfig,
    cache: dict[str, tuple[str, RobotFileParser | None, int]] | None = None,
) -> tuple[str, int]:
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    cached = cache.get(origin) if cache is not None else None
    if cached is None:
        try:
            response, payload, attempts = _request(
                session, robots_url, config, max_bytes=config.robots_max_bytes
            )
        except (
            Timeout,
            RequestsConnectionError,
            TooManyRedirects,
            ChunkedEncodingError,
            ContentDecodingError,
        ) as exc:
            cached = (f"unknown:{type(exc).__name__}", None, config.retries + 1)
        else:
            if response.status_code in (401, 403):
                cached = ("disallowed:robots_unavailable", None, attempts)
            elif response.status_code == 429 or response.status_code >= 500:
                cached = (f"unknown:http_{response.status_code}", None, attempts)
            elif response.status_code != 200:
                cached = ("allowed:no_robots", None, attempts)
            else:
                robots_parser = RobotFileParser()
                robots_parser.set_url(robots_url)
                robots_parser.parse(_decode_html(payload, response).splitlines())
                cached = ("rules", robots_parser, attempts)
        if cache is not None:
            cache[origin] = cached
    base_status, parser, attempts = cached
    if parser is None:
        return base_status, attempts
    if parser.can_fetch(config.user_agent, url):
        return "allowed", attempts
    return "disallowed", attempts


def _terminal_check(
    row: Mapping[str, Any],
    *,
    started: float,
    attempts: int,
    robots_status: str,
    outcome: str,
    score: float,
    reasons: Sequence[str],
    http_status: int | None = None,
    final_url: str | None = None,
    content_type: str | None = None,
    bytes_read: int = 0,
    evidence: Mapping[str, Any] | None = None,
    error_kind: str | None = None,
    error_detail: str | None = None,
) -> PageCheck:
    return PageCheck(
        url=str(row["url"]),
        registered_domain=str(row["registered_domain"]),
        checked_at=_utc_now(),
        attempts=attempts,
        elapsed_ms=int((time.monotonic() - started) * 1000),
        robots_status=robots_status,
        http_status=http_status,
        final_url=final_url,
        content_type=content_type,
        bytes_read=bytes_read,
        outcome=outcome,
        qualification_score=score,
        reason_codes=sorted(set(reasons)),
        evidence=dict(evidence or {}),
        error_kind=error_kind,
        error_detail=(error_detail or "")[:300] or None,
    )


def check_domain(
    rows: Sequence[Mapping[str, Any]], config: ValidationConfig
) -> list[PageCheck]:
    """Check one domain sequentially, enforcing per-domain concurrency of one."""
    session = requests.Session()
    results: list[PageCheck] = []
    robots_cache: dict[str, tuple[str, RobotFileParser | None, int]] = {}
    try:
        for row in rows:
            started = time.monotonic()
            robots_status, robots_attempts = _robots_decision(
                session, str(row["url"]), config, robots_cache
            )
            if robots_status.startswith("disallowed"):
                results.append(
                    _terminal_check(
                        row,
                        started=started,
                        attempts=robots_attempts,
                        robots_status=robots_status,
                        outcome="review",
                        score=0,
                        reasons=["robots_disallowed"],
                    )
                )
                continue
            if robots_status.startswith("unknown"):
                results.append(
                    _terminal_check(
                        row,
                        started=started,
                        attempts=robots_attempts,
                        robots_status=robots_status,
                        outcome="review",
                        score=0,
                        reasons=["robots_status_unknown"],
                    )
                )
                continue
            try:
                response, payload, attempts = _request(
                    session, str(row["url"]), config, max_bytes=config.max_bytes
                )
            except (
                Timeout,
                RequestsConnectionError,
                TooManyRedirects,
                ChunkedEncodingError,
                ContentDecodingError,
            ) as exc:
                results.append(
                    _terminal_check(
                        row,
                        started=started,
                        attempts=config.retries + 1,
                        robots_status=robots_status,
                        outcome="unreachable",
                        score=0,
                        reasons=["unreachable_from_worker"],
                        error_kind=type(exc).__name__,
                        error_detail=str(exc),
                    )
                )
                continue

            content_type = response.headers.get("content-type", "")
            if response.status_code in (404, 410):
                results.append(
                    _terminal_check(
                        row,
                        started=started,
                        attempts=attempts,
                        robots_status=robots_status,
                        http_status=response.status_code,
                        final_url=response.url,
                        content_type=content_type[:160],
                        bytes_read=len(payload),
                        outcome="rejected",
                        score=0,
                        reasons=["http_gone_or_not_found"],
                    )
                )
                continue
            if response.status_code in (401, 403, 429) or response.status_code >= 500:
                results.append(
                    _terminal_check(
                        row,
                        started=started,
                        attempts=attempts,
                        robots_status=robots_status,
                        http_status=response.status_code,
                        final_url=response.url,
                        content_type=content_type[:160],
                        bytes_read=len(payload),
                        outcome="review",
                        score=0,
                        reasons=["blocked_rate_limited_or_transient_http"],
                    )
                )
                continue
            if not 200 <= response.status_code < 300:
                results.append(
                    _terminal_check(
                        row,
                        started=started,
                        attempts=attempts,
                        robots_status=robots_status,
                        http_status=response.status_code,
                        final_url=response.url,
                        content_type=content_type[:160],
                        bytes_read=len(payload),
                        outcome="review",
                        score=0,
                        reasons=["unexpected_http_status"],
                    )
                )
                continue
            looks_like_html = b"<html" in payload[:4_096].lower()
            if "html" not in content_type.lower() and not (
                not content_type and looks_like_html
            ):
                results.append(
                    _terminal_check(
                        row,
                        started=started,
                        attempts=attempts,
                        robots_status=robots_status,
                        http_status=response.status_code,
                        final_url=response.url,
                        content_type=content_type[:160],
                        bytes_read=len(payload),
                        outcome="rejected",
                        score=5,
                        reasons=["non_html_response"],
                    )
                )
                continue
            page_html = _decode_html(payload, response)
            outcome, score, reasons, evidence = classify_live_html(
                original_url=str(row["url"]),
                final_url=response.url,
                registered_domain=str(row["registered_domain"]),
                html=page_html,
                matched_expression=str(row.get("matched_expression", "")),
            )
            results.append(
                _terminal_check(
                    row,
                    started=started,
                    attempts=attempts,
                    robots_status=robots_status,
                    http_status=response.status_code,
                    final_url=response.url,
                    content_type=content_type[:160],
                    bytes_read=len(payload),
                    outcome=outcome,
                    score=score,
                    reasons=reasons,
                    evidence=evidence,
                )
            )
    finally:
        session.close()
    return results


def _input_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _init_result_db(
    path: Path, *, input_db: Path, config: ValidationConfig, resume: bool
) -> sqlite3.Connection:
    if path.exists() and not resume:
        raise FileExistsError(f"validation database already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.executescript(LIVE_SCHEMA)
    identity = {
        "schema_version": str(SCHEMA_VERSION),
        "classifier_version": str(CLASSIFIER_VERSION),
        "input_sha256": _input_digest(input_db),
        "config": json.dumps(asdict(config), sort_keys=True, separators=(",", ":")),
    }
    existing = dict(connection.execute("SELECT key, value FROM validation_meta"))
    if existing:
        mismatches = {
            key: (existing.get(key), value)
            for key, value in identity.items()
            if existing.get(key) != value
        }
        if mismatches:
            connection.close()
            raise ValueError(f"validation resume identity mismatch: {mismatches}")
    else:
        with connection:
            connection.executemany(
                "INSERT INTO validation_meta(key, value) VALUES (?, ?)",
                sorted(identity.items()),
            )
    return connection


def _load_input_pages(input_db: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{input_db}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [
            dict(row)
            for row in connection.execute(
                """
                SELECT url, registered_domain, pattern_id, pattern_language,
                       matched_expression, pattern_weight, path_specificity
                FROM outreach_pages
                ORDER BY registered_domain, pattern_weight DESC,
                         path_specificity DESC, url
                """
            )
        ]
    finally:
        connection.close()


def _save_page_checks(
    connection: sqlite3.Connection, checks: Iterable[PageCheck]
) -> None:
    rows = []
    for check in checks:
        values = asdict(check)
        rows.append(
            (
                values["url"],
                values["registered_domain"],
                values["checked_at"],
                values["attempts"],
                values["elapsed_ms"],
                values["robots_status"],
                values["http_status"],
                values["final_url"],
                values["content_type"],
                values["bytes_read"],
                values["outcome"],
                values["qualification_score"],
                json.dumps(values["reason_codes"], separators=(",", ":")),
                json.dumps(values["evidence"], sort_keys=True, separators=(",", ":")),
                values["error_kind"],
                values["error_detail"],
            )
        )
    with connection:
        connection.executemany(
            """
            INSERT OR REPLACE INTO page_checks (
                url, registered_domain, checked_at, attempts, elapsed_ms,
                robots_status, http_status, final_url, content_type, bytes_read,
                outcome, qualification_score, reason_codes, evidence_json,
                error_kind, error_detail
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def _refresh_domain_checks(connection: sqlite3.Connection) -> None:
    connection.row_factory = sqlite3.Row
    domains = [
        str(row[0])
        for row in connection.execute(
            "SELECT DISTINCT registered_domain FROM page_checks ORDER BY 1"
        )
    ]
    output = []
    for domain in domains:
        rows = list(
            connection.execute(
                """
                SELECT * FROM page_checks
                WHERE registered_domain = ?
                ORDER BY qualification_score DESC, url
                """,
                (domain,),
            )
        )
        best = rows[0]
        statuses = {str(row["outcome"]) for row in rows}
        if "approved" in statuses:
            status = "approved"
        elif "review" in statuses or len(statuses) > 1:
            status = "review"
        elif "rejected" in statuses:
            status = "rejected"
        else:
            status = "unreachable"
        reasons = sorted(
            {
                reason
                for row in rows
                for reason in json.loads(str(row["reason_codes"]))
            }
        )
        output.append(
            (
                domain,
                _utc_now(),
                len(rows),
                str(best["url"]),
                status,
                float(best["qualification_score"]),
                json.dumps(reasons, separators=(",", ":")),
            )
        )
    with connection:
        connection.execute("DELETE FROM domain_checks")
        connection.executemany(
            """
            INSERT INTO domain_checks (
                registered_domain, checked_at, checked_urls, best_url, status,
                qualification_score, reason_codes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            output,
        )


def _build_summary(
    connection: sqlite3.Connection, *, input_pages: int, input_domains: int
) -> QualificationSummary:
    page_outcomes = {
        str(name): int(count)
        for name, count in connection.execute(
            "SELECT outcome, COUNT(*) FROM page_checks GROUP BY outcome"
        )
    }
    domain_outcomes = {
        str(name): int(count)
        for name, count in connection.execute(
            "SELECT status, COUNT(*) FROM domain_checks GROUP BY status"
        )
    }
    return QualificationSummary(
        input_pages=input_pages,
        input_domains=input_domains,
        checked_pages=sum(page_outcomes.values()),
        checked_domains=sum(domain_outcomes.values()),
        page_outcomes=page_outcomes,
        domain_outcomes=domain_outcomes,
    )


def qualify_outreach(
    *,
    input_db: str | Path,
    out_db: str | Path,
    workers: int = 20,
    config: ValidationConfig | None = None,
    resume: bool = True,
    progress: Callable[[str], None] | None = None,
    domain_checker: Callable[
        [Sequence[Mapping[str, Any]], ValidationConfig], list[PageCheck]
    ] = check_domain,
) -> QualificationSummary:
    """Validate every discovered page and aggregate decisions by domain."""
    if workers < 1:
        raise ValueError("workers must be positive")
    config = config or ValidationConfig()
    input_path, output_path = Path(input_db), Path(out_db)
    pages = _load_input_pages(input_path)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in pages:
        grouped[str(row["registered_domain"])].append(row)

    connection = _init_result_db(
        output_path, input_db=input_path, config=config, resume=resume
    )
    completed = {
        str(row[0]) for row in connection.execute("SELECT url FROM page_checks")
    }
    pending = {
        domain: [row for row in rows if str(row["url"]) not in completed]
        for domain, rows in grouped.items()
    }
    pending = {domain: rows for domain, rows in pending.items() if rows}
    total_pending = sum(len(rows) for rows in pending.values())
    if progress:
        progress(
            f"resume={len(completed)} checked; pending={total_pending} URLs "
            f"across {len(pending)} domains"
        )

    checked_this_run = 0
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(domain_checker, rows, config): domain
            for domain, rows in pending.items()
        }
        for future in as_completed(futures):
            domain = futures[future]
            try:
                checks = future.result()
            except RequestException as exc:
                LOGGER.warning("domain check failed for %s: %s", domain, exc)
                continue
            _save_page_checks(connection, checks)
            checked_this_run += len(checks)
            if progress and (
                checked_this_run == total_pending or checked_this_run % 100 < len(checks)
            ):
                elapsed = max(0.001, time.monotonic() - started)
                progress(
                    f"checked={len(completed) + checked_this_run}/{len(pages)} "
                    f"rate={checked_this_run / elapsed * 60:.1f} URLs/min"
                )

    _refresh_domain_checks(connection)
    summary = _build_summary(
        connection, input_pages=len(pages), input_domains=len(grouped)
    )
    connection.close()
    return summary


def write_qualification_outputs(
    result_db: str | Path,
    report_path: str | Path,
    export_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Write summary JSON and optional status-separated domain CSV files."""
    connection = sqlite3.connect(f"file:{Path(result_db)}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        summary = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "pages": int(
                connection.execute("SELECT COUNT(*) FROM page_checks").fetchone()[0]
            ),
            "domains": int(
                connection.execute("SELECT COUNT(*) FROM domain_checks").fetchone()[0]
            ),
            "page_outcomes": dict(
                connection.execute(
                    "SELECT outcome, COUNT(*) FROM page_checks GROUP BY outcome"
                )
            ),
            "domain_outcomes": dict(
                connection.execute(
                    "SELECT status, COUNT(*) FROM domain_checks GROUP BY status"
                )
            ),
            "http_statuses": dict(
                connection.execute(
                    """
                    SELECT COALESCE(CAST(http_status AS TEXT), 'network_error'),
                           COUNT(*)
                    FROM page_checks GROUP BY http_status
                    """
                )
            ),
            "robots_statuses": dict(
                connection.execute(
                    "SELECT robots_status, COUNT(*) FROM page_checks GROUP BY 1"
                )
            ),
            "top_reasons": Counter(
                reason
                for row in connection.execute(
                    "SELECT reason_codes FROM page_checks"
                )
                for reason in json.loads(str(row[0]))
            ).most_common(30),
        }
        target = Path(report_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

        if export_dir is not None:
            output = Path(export_dir)
            output.mkdir(parents=True, exist_ok=True)
            columns = (
                "registered_domain",
                "best_url",
                "status",
                "qualification_score",
                "checked_urls",
                "reason_codes",
            )
            for status in ("approved", "review", "rejected", "unreachable"):
                with (output / f"{status}.csv").open(
                    "w", encoding="utf-8-sig", newline=""
                ) as handle:
                    writer = csv.DictWriter(handle, fieldnames=columns)
                    writer.writeheader()
                    for row in connection.execute(
                        """
                        SELECT registered_domain, best_url, status,
                               qualification_score, checked_urls, reason_codes
                        FROM domain_checks WHERE status = ?
                        ORDER BY qualification_score DESC, registered_domain
                        """,
                        (status,),
                    ):
                        writer.writerow(dict(row))
        return summary
    finally:
        connection.close()
