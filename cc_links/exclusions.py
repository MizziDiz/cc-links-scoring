"""Exclusion of global mega-platforms from engine scoring.

These are single, centrally-hosted services (not installable CMS/forum/blog
"engines"), so keeping them out of the classification keeps the platform-market
statistics meaningful and avoids sending any traffic their way.
"""
import json
import os
from typing import Optional, Set

_DEFAULT_PATH = os.path.join(os.path.dirname(__file__), "exclusions.json")


def load_excluded_domains(extra_path: Optional[str] = None) -> Set[str]:
    domains: Set[str] = set()
    with open(_DEFAULT_PATH, "r", encoding="utf-8") as f:
        domains.update(d.lower() for d in json.load(f)["domains"])

    if extra_path and os.path.exists(extra_path):
        with open(extra_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            domains.update(d.lower() for d in data.get("domains", []))

    return domains


def is_excluded(domain: Optional[str], excluded: Set[str]) -> bool:
    domain = (domain or "").lower()
    return any(domain == d or domain.endswith("." + d) for d in excluded)
