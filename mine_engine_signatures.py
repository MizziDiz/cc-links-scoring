#!/usr/bin/env python3
"""Extract reviewable classifier/search signatures from owner-provided engine INIs.

The tool never edits the production taxonomy. It creates a compact JSON report
that can be reviewed and curated into ``prospect_footprints.json``.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

FIELD_RE = re.compile(
    r"^(engine type|page must have\d*|url must have\d*|search term)\s*=(.*)$",
    re.IGNORECASE,
)


def read_engine_text(path: Path) -> str:
    payload = path.read_bytes()
    for encoding in ("utf-8-sig", "cp1252", "latin1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", "replace")


def _clean_signature(value: str) -> str | None:
    value = value.strip().strip('"').strip()
    if value.startswith("!"):
        return None
    if not 3 <= len(value) <= 240:
        return None
    if "%" in value or value.startswith(";"):
        return None
    return value


def parse_engine_file(path: Path) -> dict[str, Any]:
    engine_type = ""
    html_signals: set[str] = set()
    url_signals: set[str] = set()
    search_footprints: set[str] = set()

    for raw_line in read_engine_text(path).splitlines():
        match = FIELD_RE.match(raw_line.strip())
        if not match:
            continue
        field, raw_value = match.groups()
        field = field.lower()
        if field == "engine type":
            engine_type = raw_value.strip()
            continue
        destination = (
            search_footprints if field == "search term"
            else url_signals if field.startswith("url must have")
            else html_signals
        )
        for part in raw_value.split("|"):
            cleaned = _clean_signature(part)
            if cleaned:
                destination.add(cleaned)

    return {
        "engine": path.stem,
        "engine_type": engine_type,
        "source_file": path.name,
        "html_signals": sorted(html_signals, key=str.lower),
        "url_signals": sorted(url_signals, key=str.lower),
        "search_footprints": sorted(search_footprints, key=str.lower),
    }


def mine_engine_directory(path: str) -> dict[str, Any]:
    root = Path(path)
    if not root.is_dir():
        raise ValueError(f"engine directory not found: {root}")
    engines = []
    type_counts: Counter[str] = Counter()
    for source in sorted(root.rglob("*.ini")):
        parsed = parse_engine_file(source)
        if not any(
            parsed[key]
            for key in ("html_signals", "url_signals", "search_footprints")
        ):
            continue
        engines.append(parsed)
        type_counts[parsed["engine_type"] or "Unknown"] += 1
    return {
        "source": str(root.resolve()),
        "ini_files": len(list(root.rglob("*.ini"))),
        "engines_with_signatures": len(engines),
        "engine_types": dict(type_counts.most_common()),
        "engines": engines,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mine reviewable signatures from a GSA Engines directory"
    )
    parser.add_argument("--engines-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    report = mine_engine_directory(args.engines_dir)
    destination = Path(args.out)
    destination.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        f"ini={report['ini_files']} "
        f"with_signatures={report['engines_with_signatures']} "
        f"types={len(report['engine_types'])} -> {destination}"
    )


if __name__ == "__main__":
    main()
