"""Resumable URL-only outreach discovery over Common Crawl Parquet parts."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence
from urllib.parse import urlsplit

import duckdb

from cc_links.cc_index import _connect, get_index_parts
from cc_links.outreach import (
    OutreachPattern,
    best_outreach_match,
    compile_discovery_regex,
    load_outreach_patterns,
    match_outreach_path,
    pattern_registry_digest,
)
from cc_links.outreach_db import (
    init_outreach_db,
    iter_selected_pages,
    save_outreach_pages,
)
from cc_links.partmap import load_part_map, select_parts, surt_prefix_for_tld

LOGGER = logging.getLogger(__name__)
STATE_SCHEMA_VERSION = 1
QUERY_SCHEMA_VERSION = 1


class OutreachDiscoveryError(RuntimeError):
    """Base error for outreach discovery."""


class OutreachDiscoveryIncomplete(OutreachDiscoveryError):
    """Raised after a run leaves one or more selected parts incomplete."""


@dataclass(frozen=True)
class DiscoverySummary:
    """Final counters for one discovery invocation."""

    selected_parts: int
    completed_parts: int
    failed_parts: int
    matched_rows: int
    inserted_rows: int
    domains: int
    output_rows: int


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _identity_digest(identity: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _part_name(part_url: str) -> str:
    return urlsplit(part_url).path.rsplit("/", 1)[-1] or part_url


def _spool_path(spool_dir: Path, part_index: int, part_url: str) -> Path:
    suffix = hashlib.sha256(part_url.encode("utf-8")).hexdigest()[:12]
    return spool_dir / f"{part_index:05d}-{suffix}.jsonl"


def _select_part_urls(
    crawl: str,
    tlds: Sequence[str],
    part_map_path: Optional[str | Path],
    part_urls: Optional[Sequence[str]],
    max_parts: Optional[int],
    part_shard: Optional[tuple[int, int]],
    index_source: str,
) -> list[str]:
    if part_urls is not None:
        selected = list(part_urls)
    elif part_map_path is not None:
        part_map = load_part_map(part_map_path)
        if part_map["crawl"] != crawl:
            raise OutreachDiscoveryError(
                f"part map is for {part_map['crawl']}, not {crawl}"
            )
        selected = [row.part_url for row in select_parts(part_map, tlds)]
    else:
        selected = list(get_index_parts(crawl, index_source=index_source))

    if max_parts is not None:
        if max_parts < 1:
            raise ValueError("max_parts must be at least one")
        if max_parts < len(selected):
            stride = len(selected) / max_parts
            selected = [selected[int(index * stride)] for index in range(max_parts)]
    if part_shard is not None:
        shard_index, shard_count = part_shard
        if shard_count < 1 or not 0 <= shard_index < shard_count:
            raise ValueError("part shard must satisfy 0 <= index < count")
        selected = [
            part_url
            for index, part_url in enumerate(selected)
            if index % shard_count == shard_index
        ]
    if not selected:
        raise OutreachDiscoveryError("no Parquet parts selected")
    return selected


def _infer_index_source(part_urls: Sequence[str]) -> str:
    """Infer one DuckDB index source from a homogeneous part list."""
    sources = {
        "s3" if urlsplit(part_url).scheme.lower() == "s3" else "https"
        for part_url in part_urls
    }
    if len(sources) != 1:
        raise OutreachDiscoveryError("selected parts mix S3 and HTTPS sources")
    return sources.pop()


def _load_state(path: Path, identity: Mapping[str, Any]) -> dict[str, Any]:
    digest = _identity_digest(identity)
    if not path.exists():
        return {
            "schema_version": STATE_SCHEMA_VERSION,
            "identity": dict(identity),
            "identity_digest": digest,
            "completed_parts": [],
            "failed_parts": {},
            "matched_rows": 0,
            "inserted_rows": 0,
        }
    with path.open("r", encoding="utf-8") as handle:
        state = json.load(handle)
    if state.get("schema_version") != STATE_SCHEMA_VERSION:
        raise OutreachDiscoveryError("checkpoint schema mismatch")
    if state.get("identity_digest") != digest or state.get("identity") != identity:
        raise OutreachDiscoveryError(
            "checkpoint identity mismatch; use a new output/state path"
        )
    return state


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise OutreachDiscoveryError(
                    f"invalid JSONL in {path.name} at line {line_number}"
                ) from exc
            if not isinstance(value, dict):
                raise OutreachDiscoveryError(
                    f"non-object JSONL record in {path.name} at line {line_number}"
                )
            yield value


def _import_spool(
    connection: sqlite3.Connection, spool: Path, max_per_domain: int
) -> int:
    return save_outreach_pages(
        connection, _iter_jsonl(spool), max_per_domain=max_per_domain
    )


def _write_export(
    connection: sqlite3.Connection, out_path: Path, max_per_domain: int
) -> int:
    temporary = out_path.with_name(out_path.name + ".tmp")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        for row in iter_selected_pages(connection, max_per_domain=max_per_domain):
            row.pop("domain_rank", None)
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            count += 1
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, out_path)
    return count


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _discovery_query(part_url: str, tlds: Sequence[str], discovery_regex: str) -> str:
    tld_sql = ", ".join(_sql_literal(tld) for tld in tlds)
    part_sql = _sql_literal(part_url)
    regex_sql = _sql_literal(discovery_regex)
    return f"""
        SELECT url, url_path, url_host_registered_domain, url_host_tld,
               content_languages, CAST(fetch_time AS VARCHAR) AS fetch_time,
               warc_filename,
               warc_record_offset, warc_record_length
        FROM read_parquet({part_sql})
        WHERE url_host_tld IN ({tld_sql})
          AND fetch_status = 200
          AND content_mime_detected = 'text/html'
          AND REGEXP_MATCHES(
              LOWER(COALESCE(url_path, '')),
              {regex_sql}
          )
    """


def _record_from_row(
    row: Sequence[Any],
    crawl: str,
    source_part: str,
    patterns: Sequence[OutreachPattern],
) -> Optional[dict[str, Any]]:
    (
        url,
        url_path,
        registered_domain,
        tld,
        content_languages,
        fetch_time,
        warc_filename,
        warc_offset,
        warc_length,
    ) = row
    if not url or not url_path or not registered_domain:
        return None
    language_value = "" if content_languages is None else str(content_languages)
    matches = match_outreach_path(str(url_path), patterns)
    best = best_outreach_match(str(url_path), patterns, language_value)
    if best is None:
        return None
    return {
        "url": str(url),
        "crawl": crawl,
        "query_schema_version": QUERY_SCHEMA_VERSION,
        "url_path": str(url_path),
        "registered_domain": str(registered_domain).lower(),
        "tld": None if tld is None else str(tld).lower(),
        "content_languages": language_value or None,
        "fetch_time": None if fetch_time is None else str(fetch_time),
        "pattern_id": best.pattern_id,
        "pattern_language": best.language,
        "matched_expression": best.expression,
        "matched_pattern_ids": sorted({match.pattern_id for match in matches}),
        "pattern_weight": best.weight,
        "path_specificity": best.specificity,
        "source_part": source_part,
        "warc_filename": None if warc_filename is None else str(warc_filename),
        "warc_record_offset": (None if warc_offset is None else int(warc_offset)),
        "warc_record_length": (None if warc_length is None else int(warc_length)),
    }


def discover_outreach(
    *,
    crawl: str,
    tlds: Sequence[str],
    out_path: str | Path,
    db_path: str | Path,
    patterns_path: Optional[str | Path] = None,
    part_map_path: Optional[str | Path] = None,
    part_urls: Optional[Sequence[str]] = None,
    max_parts: Optional[int] = None,
    max_per_domain: int = 2,
    part_shard: Optional[tuple[int, int]] = None,
    index_source: str = "auto",
    reconnect_every: int = 15,
    resume: bool = True,
    max_retries: int = 3,
    retry_backoff: float = 2.0,
    fetch_batch_size: int = 1000,
    connection_factory: Optional[Callable[[], Any]] = None,
    retry_exceptions: tuple[type[BaseException], ...] = (duckdb.Error,),
    is_excluded: Optional[Callable[[str], bool]] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> DiscoverySummary:
    """Run URL-only outreach discovery with per-part atomic spools.

    Completed spool files are authoritative recovery units.  If a crash happens
    after a part query but before its checkpoint is updated, resume imports the
    existing spool instead of reading the Parquet part again.
    """
    if max_per_domain < 1:
        raise ValueError("max_per_domain must be at least one")
    if index_source not in {"auto", "https", "s3"}:
        raise ValueError(f"unsupported index source: {index_source}")
    if reconnect_every < 1:
        raise ValueError("reconnect_every must be at least one")
    if max_retries < 1:
        raise ValueError("max_retries must be at least one")
    if fetch_batch_size < 1:
        raise ValueError("fetch_batch_size must be at least one")

    normalized_tlds = sorted(
        {str(tld).strip().lower().lstrip(".") for tld in tlds if str(tld).strip()}
    )
    if not normalized_tlds:
        raise ValueError("at least one TLD is required")
    for tld in normalized_tlds:
        surt_prefix_for_tld(tld)
    patterns = load_outreach_patterns(patterns_path)
    selected_parts = _select_part_urls(
        crawl,
        normalized_tlds,
        part_map_path,
        part_urls,
        max_parts,
        part_shard,
        "https" if index_source == "auto" else index_source,
    )
    resolved_index_source = (
        _infer_index_source(selected_parts)
        if index_source == "auto"
        else index_source
    )
    pattern_digest = pattern_registry_digest(patterns)
    identity = {
        "crawl": crawl,
        "tlds": normalized_tlds,
        "part_urls": selected_parts,
        "index_source": resolved_index_source,
        "pattern_digest": pattern_digest,
        "max_per_domain": max_per_domain,
        "part_shard": list(part_shard) if part_shard is not None else None,
        "db_path": str(Path(db_path).resolve()),
    }

    output = Path(out_path)
    state_path = output.with_name(output.name + ".state.json")
    spool_dir = output.with_name(output.name + ".parts")
    spool_dir.mkdir(parents=True, exist_ok=True)
    if not resume and (state_path.exists() or any(spool_dir.iterdir())):
        raise OutreachDiscoveryError(
            "non-resume run requires a new output path without checkpoints"
        )
    if state_path.exists() and not Path(db_path).exists():
        raise OutreachDiscoveryError(
            "checkpoint exists but its outreach database is missing"
        )
    state = _load_state(state_path, identity)
    completed = set(state.get("completed_parts", []))
    failed: dict[str, Any] = dict(state.get("failed_parts", {}))
    connection = init_outreach_db(db_path)
    seen = {
        (str(url), str(row_crawl))
        for url, row_crawl in connection.execute(
            "SELECT url, crawl FROM outreach_pages"
        )
    }
    discovery_regex = compile_discovery_regex(patterns)
    make_connection = connection_factory or (
        lambda: _connect(index_source=resolved_index_source)
    )
    parquet_connection: Any = None
    completed_on_connection = 0

    try:
        for index, part_url in enumerate(selected_parts):
            spool = _spool_path(spool_dir, index, part_url)
            if part_url in completed:
                if not spool.exists():
                    raise OutreachDiscoveryError(
                        f"completed part has no recovery spool: {_part_name(part_url)}"
                    )
                continue
            if spool.exists():
                inserted = _import_spool(connection, spool, max_per_domain)
                seen.update(
                    (str(url), str(row_crawl))
                    for url, row_crawl in connection.execute(
                        "SELECT url, crawl FROM outreach_pages"
                    )
                )
                completed.add(part_url)
                failed.pop(part_url, None)
                state["inserted_rows"] = int(state.get("inserted_rows", 0)) + inserted
                state["completed_parts"] = sorted(completed)
                state["failed_parts"] = failed
                _atomic_json(state_path, state)
                if progress:
                    progress(
                        f"part {index + 1}/{len(selected_parts)} recovered from spool"
                    )
                continue

            query = _discovery_query(part_url, normalized_tlds, discovery_regex)
            last_error: Optional[BaseException] = None
            query_succeeded = False
            temporary = spool.with_name(spool.name + ".tmp")
            matched_this_part = 0
            for attempt in range(1, max_retries + 1):
                try:
                    if parquet_connection is None:
                        parquet_connection = make_connection()
                    cursor = parquet_connection.execute(query)
                    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
                        while True:
                            rows = cursor.fetchmany(fetch_batch_size)
                            if not rows:
                                break
                            for row in rows:
                                record = _record_from_row(
                                    row, crawl, _part_name(part_url), patterns
                                )
                                if record is None:
                                    continue
                                domain = record["registered_domain"]
                                key = (record["url"], crawl)
                                if key in seen:
                                    continue
                                if is_excluded is not None and is_excluded(domain):
                                    continue
                                handle.write(
                                    json.dumps(
                                        record,
                                        ensure_ascii=False,
                                        sort_keys=True,
                                    )
                                    + "\n"
                                )
                                seen.add(key)
                                matched_this_part += 1
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(temporary, spool)
                    query_succeeded = True
                    break
                except retry_exceptions as exc:
                    last_error = exc
                    LOGGER.warning(
                        "outreach query failed for %s (attempt %d/%d): %s",
                        _part_name(part_url),
                        attempt,
                        max_retries,
                        exc,
                    )
                    if temporary.exists():
                        temporary.unlink()
                    # A driver error may happen after some rows were written.
                    # Those rows are not durable until the spool is renamed and
                    # imported, so discard their in-memory seen markers too.
                    seen = {
                        (str(url), str(row_crawl))
                        for url, row_crawl in connection.execute(
                            "SELECT url, crawl FROM outreach_pages"
                        )
                    }
                    if parquet_connection is not None:
                        try:
                            parquet_connection.close()
                        except duckdb.Error:
                            LOGGER.debug(
                                "failed to close DuckDB connection", exc_info=True
                            )
                        parquet_connection = None
                    if attempt < max_retries:
                        time.sleep(retry_backoff * (2 ** (attempt - 1)))

            if not query_succeeded:
                failed[part_url] = {
                    "attempts": max_retries,
                    "error": type(last_error).__name__ if last_error else "unknown",
                    "message": str(last_error)[:500] if last_error else "",
                }
                state["failed_parts"] = failed
                _atomic_json(state_path, state)
                if progress:
                    progress(
                        f"part {index + 1}/{len(selected_parts)} failed and remains incomplete"
                    )
                # Restore seen because rows from the failed attempt were never
                # committed and its temporary spool was discarded.
                seen = {
                    (str(url), str(row_crawl))
                    for url, row_crawl in connection.execute(
                        "SELECT url, crawl FROM outreach_pages"
                    )
                }
                continue

            inserted = _import_spool(connection, spool, max_per_domain)
            completed.add(part_url)
            completed_on_connection += 1
            failed.pop(part_url, None)
            state["matched_rows"] = (
                int(state.get("matched_rows", 0)) + matched_this_part
            )
            state["inserted_rows"] = int(state.get("inserted_rows", 0)) + inserted
            state["completed_parts"] = sorted(completed)
            state["failed_parts"] = failed
            state["updated_at"] = datetime.now(timezone.utc).isoformat()
            _atomic_json(state_path, state)
            if progress:
                progress(
                    f"part {index + 1}/{len(selected_parts)}: "
                    f"{matched_this_part} matched, {inserted} inserted"
                )
            if completed_on_connection >= reconnect_every:
                if parquet_connection is not None:
                    try:
                        parquet_connection.close()
                    except duckdb.Error:
                        LOGGER.debug(
                            "failed to close recycled DuckDB connection",
                            exc_info=True,
                        )
                    parquet_connection = None
                completed_on_connection = 0
    finally:
        if parquet_connection is not None:
            parquet_connection.close()

    output_rows = _write_export(connection, output, max_per_domain)
    domains = int(
        connection.execute("SELECT COUNT(*) FROM outreach_prospects").fetchone()[0]
    )
    connection.close()
    summary = DiscoverySummary(
        selected_parts=len(selected_parts),
        completed_parts=len(completed),
        failed_parts=len(failed),
        matched_rows=int(state.get("matched_rows", 0)),
        inserted_rows=int(state.get("inserted_rows", 0)),
        domains=domains,
        output_rows=output_rows,
    )
    if failed:
        raise OutreachDiscoveryIncomplete(
            f"{len(failed)} of {len(selected_parts)} selected parts remain incomplete"
        )
    return summary
