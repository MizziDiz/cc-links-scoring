"""Crawl-specific Common Crawl index part maps.

The cc-index is split into many Parquet files.  A map of each file's minimum
and maximum ``url_surtkey`` lets a targeted ccTLD run skip files whose ranges
cannot contain the requested SURT prefixes.  Selection is deliberately
conservative: unknown or malformed ranges are always scanned.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence
from urllib.parse import urlsplit

import duckdb

from cc_links.cc_index import _connect, get_index_parts

LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = 1


class PartMapError(ValueError):
    """Raised when a part-map artifact is invalid or incomplete."""


@dataclass(frozen=True)
class PartRange:
    """The observed SURT range for one Parquet part."""

    part: str
    part_url: str
    min_url_surtkey: Optional[str]
    max_url_surtkey: Optional[str]
    row_count: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "PartRange":
        """Validate and construct a part range from serialized data."""
        part = str(value.get("part", "")).strip()
        part_url = str(value.get("part_url", "")).strip()
        if not part or not part_url:
            raise PartMapError("each part-map row needs part and part_url")
        try:
            row_count = int(value.get("row_count", 0))
        except (TypeError, ValueError) as exc:
            raise PartMapError(f"invalid row_count for {part}") from exc
        if row_count < 0:
            raise PartMapError(f"negative row_count for {part}")
        minimum = value.get("min_url_surtkey")
        maximum = value.get("max_url_surtkey")
        return cls(
            part=part,
            part_url=part_url,
            min_url_surtkey=None if minimum is None else str(minimum),
            max_url_surtkey=None if maximum is None else str(maximum),
            row_count=row_count,
        )


def prefix_successor(prefix: str) -> str:
    """Return the smallest Unicode string strictly above every string with prefix.

    For the ASCII SURT prefix ``"co,"`` this returns ``"co-"``.  The function
    avoids relying on an encoding-specific ``\xff`` sentinel.
    """
    if not prefix:
        raise ValueError("prefix must not be empty")
    codepoints = [ord(char) for char in prefix]
    for index in range(len(codepoints) - 1, -1, -1):
        if codepoints[index] < 0x10FFFF:
            return "".join(chr(value) for value in codepoints[:index]) + chr(
                codepoints[index] + 1
            )
    raise ValueError("prefix has no finite Unicode successor")


def surt_prefix_for_tld(tld: str) -> str:
    """Convert a validated TLD to the prefix used by reversed SURT host keys."""
    normalized = tld.strip().lower().lstrip(".")
    if not normalized or any(
        not (char.isascii() and (char.isalnum() or char == "-")) for char in normalized
    ):
        raise ValueError(f"invalid TLD: {tld!r}")
    return normalized + ","


def range_overlaps_prefix(part: PartRange, prefix: str) -> bool:
    """Return whether a part may contain the prefix.

    Missing or contradictory metadata is treated as unknown and therefore
    included.  This function is a pruning hint, never a source of false
    negatives.
    """
    minimum = part.min_url_surtkey
    maximum = part.max_url_surtkey
    if minimum is None or maximum is None or minimum > maximum:
        return True
    upper = prefix_successor(prefix)
    return maximum >= prefix and minimum < upper


def select_parts(part_map: Mapping[str, Any], tlds: Iterable[str]) -> list[PartRange]:
    """Select parts whose SURT ranges may contain at least one requested TLD."""
    prefixes = tuple(surt_prefix_for_tld(tld) for tld in tlds)
    if not prefixes:
        raise ValueError("at least one TLD is required")
    rows = [PartRange.from_mapping(value) for value in part_map.get("parts", [])]
    return [
        row
        for row in rows
        if any(range_overlaps_prefix(row, prefix) for prefix in prefixes)
    ]


def _canonical_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    canonical = dict(payload)
    canonical.pop("generated_at", None)
    canonical.pop("digest", None)
    return canonical


def part_map_digest(payload: Mapping[str, Any]) -> str:
    """Calculate a stable SHA-256 digest for identity/checkpoint validation."""
    encoded = json.dumps(
        _canonical_payload(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def save_part_map(path: str | Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate, digest and atomically save a complete part map."""
    output = dict(payload)
    output["digest"] = part_map_digest(output)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(output, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    return output


def load_part_map(path: str | Path) -> dict[str, Any]:
    """Load a complete part map and verify its schema and digest."""
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise PartMapError(
            f"unsupported part-map schema: {payload.get('schema_version')!r}"
        )
    if not str(payload.get("crawl", "")).strip():
        raise PartMapError("part map has no crawl")
    rows = payload.get("parts")
    if not isinstance(rows, list) or not rows:
        raise PartMapError("part map has no parts")
    for row in rows:
        PartRange.from_mapping(row)
    expected = str(payload.get("digest", ""))
    actual = part_map_digest(payload)
    if not expected or expected != actual:
        raise PartMapError("part-map digest mismatch")
    return payload


def _part_name(part_url: str) -> str:
    path = urlsplit(part_url).path
    return path.rsplit("/", 1)[-1] or part_url


def _atomic_save_progress(
    path: Path,
    crawl: str,
    part_urls: Sequence[str],
    completed: Mapping[str, PartRange],
) -> None:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "crawl": crawl,
        "part_urls": list(part_urls),
        "parts": [asdict(completed[url]) for url in part_urls if url in completed],
    }
    temporary = path.with_name(path.name + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _load_progress(
    path: Path, crawl: str, part_urls: Sequence[str]
) -> dict[str, PartRange]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise PartMapError("part-map checkpoint schema mismatch")
    if payload.get("crawl") != crawl:
        raise PartMapError("part-map checkpoint crawl mismatch")
    if payload.get("part_urls") != list(part_urls):
        raise PartMapError("part-map checkpoint part list mismatch")
    completed: dict[str, PartRange] = {}
    for value in payload.get("parts", []):
        row = PartRange.from_mapping(value)
        completed[row.part_url] = row
    return completed


def build_part_map(
    crawl: str,
    out_path: str | Path,
    *,
    part_urls: Optional[Sequence[str]] = None,
    index_source: str = "https",
    resume: bool = True,
    max_retries: int = 3,
    reconnect_every: int = 15,
    connection_factory: Optional[Callable[[], Any]] = None,
    retry_exceptions: tuple[type[BaseException], ...] = (duckdb.Error,),
    progress: Optional[Callable[[str], None]] = None,
) -> dict[str, Any]:
    """Build a resumable part map and atomically write the final artifact.

    A failed part is never added to the checkpoint.  If retries are exhausted,
    the function raises and leaves a resumable ``.state.json`` file.
    """
    if max_retries < 1:
        raise ValueError("max_retries must be at least one")
    if reconnect_every < 1:
        raise ValueError("reconnect_every must be at least one")
    if index_source not in {"https", "s3"}:
        raise ValueError(f"unsupported index source: {index_source}")
    urls = (
        list(part_urls)
        if part_urls is not None
        else get_index_parts(crawl, index_source=index_source)
    )
    if not urls:
        raise PartMapError(f"no WARC index parts found for {crawl}")

    target = Path(out_path)
    state_path = target.with_name(target.name + ".state.json")
    completed = _load_progress(state_path, crawl, urls) if resume else {}
    make_connection = connection_factory or (
        lambda: _connect(index_source=index_source)
    )
    connection: Any = None
    mapped_on_connection = 0

    try:
        for index, part_url in enumerate(urls):
            if part_url in completed:
                continue
            last_error: Optional[BaseException] = None
            for attempt in range(1, max_retries + 1):
                try:
                    if connection is None:
                        connection = make_connection()
                    escaped = part_url.replace("'", "''")
                    row = connection.execute(
                        "SELECT MIN(url_surtkey), MAX(url_surtkey), COUNT(*) "
                        f"FROM read_parquet('{escaped}')"
                    ).fetchone()
                    minimum, maximum, row_count = row
                    part_range = PartRange(
                        part=_part_name(part_url),
                        part_url=part_url,
                        min_url_surtkey=None if minimum is None else str(minimum),
                        max_url_surtkey=None if maximum is None else str(maximum),
                        row_count=int(row_count),
                    )
                    completed[part_url] = part_range
                    _atomic_save_progress(state_path, crawl, urls, completed)
                    mapped_on_connection += 1
                    if progress:
                        progress(
                            f"part {index + 1}/{len(urls)} mapped: {part_range.part}"
                        )
                    if mapped_on_connection >= reconnect_every:
                        try:
                            connection.close()
                        except duckdb.Error:
                            LOGGER.debug(
                                "failed to close recycled DuckDB connection",
                                exc_info=True,
                            )
                        connection = None
                        mapped_on_connection = 0
                    break
                except retry_exceptions as exc:
                    last_error = exc
                    LOGGER.warning(
                        "part-map read failed for %s (attempt %d/%d): %s",
                        part_url,
                        attempt,
                        max_retries,
                        exc,
                    )
                    if connection is not None:
                        try:
                            connection.close()
                        except duckdb.Error:
                            LOGGER.debug(
                                "failed to close DuckDB connection", exc_info=True
                            )
                        connection = None
                        mapped_on_connection = 0
            else:
                raise PartMapError(
                    f"failed to map {_part_name(part_url)} after "
                    f"{max_retries} attempts: {last_error}"
                ) from last_error
    finally:
        if connection is not None:
            try:
                connection.close()
            except duckdb.Error:
                LOGGER.debug("failed to close DuckDB connection", exc_info=True)

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "crawl": crawl,
        "index_source": index_source,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parts": [asdict(completed[url]) for url in urls],
    }
    saved = save_part_map(target, payload)
    if state_path.exists():
        state_path.unlink()
    return saved
