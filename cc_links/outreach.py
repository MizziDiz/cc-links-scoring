"""Outreach invitation URL matching and ranking.

The module intentionally operates on URL paths only.  It does not classify the
existing forum/comment/wiki prospect families and it does not fetch HTML.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence
from urllib.parse import urlsplit

PATTERNS_PATH = Path(__file__).with_name("outreach_patterns.json")
SCHEMA_VERSION = 1
_ALLOWED_SCOPES = {"path_segment"}
_SEGMENT_END = r"(?:/|$|\.(?:html?|shtml|php\d*|aspx?)(?:/|$))"
_ISO3_TO_ISO2 = {"eng": "en", "spa": "es", "por": "pt"}


class OutreachPatternError(ValueError):
    """Raised when the outreach pattern registry is invalid."""


@dataclass(frozen=True)
class OutreachPattern:
    """One versioned outreach URL-path rule."""

    pattern_id: str
    language: str
    scope: str
    expressions: tuple[str, ...]
    weight: int


@dataclass(frozen=True)
class OutreachMatch:
    """A concrete path match with its ranking evidence."""

    pattern_id: str
    language: str
    expression: str
    weight: int
    specificity: int


def _normalize_expression(value: str) -> str:
    expression = value.strip().lower().strip("/")
    if not expression:
        raise OutreachPatternError("empty outreach expression")
    if "/" in expression or "?" in expression or "#" in expression:
        raise OutreachPatternError(
            f"outreach expression must be one path segment: {value!r}"
        )
    return expression


def _parse_pattern(value: Mapping[str, Any]) -> OutreachPattern:
    pattern_id = str(value.get("id", "")).strip()
    language = str(value.get("language", "")).strip().lower()
    scope = str(value.get("scope", "")).strip()
    raw_expressions = value.get("expressions")
    if not pattern_id:
        raise OutreachPatternError("outreach pattern has no id")
    if not re.fullmatch(r"[a-z0-9_.-]+", pattern_id):
        raise OutreachPatternError(f"invalid outreach pattern id: {pattern_id!r}")
    if not re.fullmatch(r"[a-z]{2,3}", language):
        raise OutreachPatternError(
            f"invalid language for outreach pattern {pattern_id!r}"
        )
    if scope not in _ALLOWED_SCOPES:
        raise OutreachPatternError(
            f"unsupported scope {scope!r} for outreach pattern {pattern_id!r}"
        )
    if not isinstance(raw_expressions, list) or not raw_expressions:
        raise OutreachPatternError(
            f"outreach pattern {pattern_id!r} has no expressions"
        )
    expressions = tuple(_normalize_expression(str(item)) for item in raw_expressions)
    if len(set(expressions)) != len(expressions):
        raise OutreachPatternError(
            f"duplicate expression inside outreach pattern {pattern_id!r}"
        )
    raw_weight = value.get("weight")
    try:
        if raw_weight is None:
            raise TypeError
        weight = int(raw_weight)
    except (TypeError, ValueError) as exc:
        raise OutreachPatternError(
            f"invalid weight for outreach pattern {pattern_id!r}"
        ) from exc
    if not 1 <= weight <= 100:
        raise OutreachPatternError(
            f"weight for outreach pattern {pattern_id!r} must be 1..100"
        )
    return OutreachPattern(pattern_id, language, scope, expressions, weight)


def load_outreach_patterns(
    path: Optional[str | Path] = None,
) -> tuple[OutreachPattern, ...]:
    """Load and validate the versioned outreach pattern registry."""
    source = Path(path) if path is not None else PATTERNS_PATH
    with source.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise OutreachPatternError(
            f"unsupported outreach pattern schema: {payload.get('schema_version')!r}"
        )
    raw_patterns = payload.get("patterns")
    if not isinstance(raw_patterns, list) or not raw_patterns:
        raise OutreachPatternError("outreach pattern registry has no patterns")
    patterns = tuple(_parse_pattern(value) for value in raw_patterns)
    ids = [pattern.pattern_id for pattern in patterns]
    if len(ids) != len(set(ids)):
        raise OutreachPatternError("duplicate outreach pattern id")
    expressions: dict[tuple[str, str, str], str] = {}
    for pattern in patterns:
        for expression in pattern.expressions:
            # The same literal can be valid in more than one language (for
            # example ``guia-para-autores`` in Spanish and Portuguese).  It is
            # ambiguous until cc-index language metadata or later page evidence
            # is available, but it is not a malformed registry.
            key = (pattern.language, pattern.scope, expression)
            previous = expressions.get(key)
            if previous:
                raise OutreachPatternError(
                    f"expression {expression!r} is shared by {previous!r} "
                    f"and {pattern.pattern_id!r}"
                )
            expressions[key] = pattern.pattern_id
    return patterns


def pattern_registry_digest(patterns: Sequence[OutreachPattern]) -> str:
    """Return a stable digest used to bind checkpoints to pattern behavior."""
    serializable = [
        {
            "id": pattern.pattern_id,
            "language": pattern.language,
            "scope": pattern.scope,
            "expressions": list(pattern.expressions),
            "weight": pattern.weight,
        }
        for pattern in patterns
    ]
    encoded = json.dumps(
        serializable, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _expression_regex(expression: str, scope: str) -> str:
    escaped = re.escape(expression)
    if scope != "path_segment":
        raise OutreachPatternError(f"unsupported outreach scope: {scope!r}")
    return rf"(?:^|/){escaped}{_SEGMENT_END}"


def compile_discovery_regex(patterns: Sequence[OutreachPattern]) -> str:
    """Compile one lowercase RE2-compatible predicate for DuckDB."""
    alternatives = [
        _expression_regex(expression, pattern.scope)
        for pattern in patterns
        for expression in pattern.expressions
    ]
    if not alternatives:
        raise OutreachPatternError("cannot compile an empty outreach registry")
    return "(?:" + "|".join(alternatives) + ")"


def _path_for_matching(url_or_path: str) -> str:
    if "://" in url_or_path or url_or_path.startswith("//"):
        return urlsplit(url_or_path).path.lower()
    return url_or_path.split("?", 1)[0].split("#", 1)[0].lower()


def match_outreach_path(
    url_or_path: str, patterns: Sequence[OutreachPattern]
) -> list[OutreachMatch]:
    """Return all outreach patterns matching a URL path, best evidence first."""
    path = _path_for_matching(url_or_path)
    matches: list[OutreachMatch] = []
    for pattern in patterns:
        for expression in pattern.expressions:
            if re.search(_expression_regex(expression, pattern.scope), path):
                matches.append(
                    OutreachMatch(
                        pattern_id=pattern.pattern_id,
                        language=pattern.language,
                        expression=expression,
                        weight=pattern.weight,
                        specificity=len(expression),
                    )
                )
    return sorted(
        matches,
        key=lambda match: (
            -match.weight,
            -match.specificity,
            match.pattern_id,
            match.expression,
        ),
    )


def best_outreach_match(
    url_or_path: str,
    patterns: Sequence[OutreachPattern],
    content_languages: Optional[str] = None,
) -> Optional[OutreachMatch]:
    """Return the strongest match, preferring compatible cc-index language data."""
    matches = match_outreach_path(url_or_path, patterns)
    if not matches:
        return None
    preferred: set[str] = set()
    if content_languages:
        for raw in re.split(r"[,;\\s]+", content_languages.lower()):
            language = raw.strip()
            if not language:
                continue
            preferred.add(_ISO3_TO_ISO2.get(language, language[:2]))
    if preferred:
        matches.sort(
            key=lambda match: (
                match.language not in preferred,
                -match.weight,
                -match.specificity,
                match.pattern_id,
                match.expression,
            )
        )
    return matches[0]


def languages_for_patterns(patterns: Iterable[OutreachPattern]) -> set[str]:
    """Return languages represented by a pattern collection."""
    return {pattern.language for pattern in patterns}
