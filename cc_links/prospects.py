"""Scored classifier for Common Crawl link-prospect candidates."""
import json
import os
from dataclasses import dataclass, asdict
from typing import List, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from cc_links.engines import get_generator

DEFAULT_FOOTPRINTS = os.path.join(os.path.dirname(__file__), "prospect_footprints.json")
TRACKING_PARAMS = {"fbclid", "gclid", "yclid", "mc_cid", "mc_eid"}
WEIGHTS = {"url": 55, "generator": 55, "html": 25}
STRONG_HTML_MARKERS = (
    "powered by", "wp-comments-post.php", "comment_post_id", "mw-content-text",
    "simple machines forum", "coppermine photo gallery", "trackback url",
)


@dataclass
class ProspectMatch:
    rule_id: str
    family: str
    platform: Optional[str]
    score: int
    signal_types: int
    signals: List[str]

    def to_dict(self):
        return asdict(self)


def load_prospect_rules(path: Optional[str] = None):
    with open(path or DEFAULT_FOOTPRINTS, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("defaults", {}), data["rules"]


def discovery_url_terms(path: Optional[str] = None) -> List[str]:
    """Specific URL terms safe enough to prefilter the Common Crawl index."""
    _, rules = load_prospect_rules(path)
    terms = []
    for rule in rules:
        terms.extend(rule.get("signals", {}).get("url_contains", []))
    # Fragments never reach the index; very broad path words create too much noise.
    blocked = {"#respond", "/comment/", "/forum/", "/forums/", "/threads/",
               "/directory/", "submit.php"}
    return sorted({t.lower() for t in terms if t.lower() not in blocked})


def normalize_url(url: str) -> str:
    """Conservative URL normalization for deduplication."""
    try:
        p = urlsplit(url.strip())
    except ValueError:
        return url.strip()
    scheme = p.scheme.lower()
    host = (p.hostname or "").lower()
    if not scheme or not host:
        return url.strip()
    port = p.port
    netloc = host
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{host}:{port}"
    path = p.path or "/"
    query = urlencode([(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
                       if not k.lower().startswith("utm_") and k.lower() not in TRACKING_PARAMS])
    return urlunsplit((scheme, netloc, path, query, ""))


def classify_prospect(html: str, url: str, footprints_path: Optional[str] = None,
                      minimum_score: Optional[int] = None) -> List[ProspectMatch]:
    defaults, rules = load_prospect_rules(footprints_path)
    threshold = minimum_score if minimum_score is not None else defaults.get("minimum_score", 50)
    min_types = defaults.get("minimum_signal_types", 2)
    url_lower = url.lower()
    html_lower = html.lower()
    generator = get_generator(html)
    matches = []

    for rule in rules:
        found = []
        types = set()
        signals = rule.get("signals", {})
        for term in signals.get("url_contains", []):
            if term.lower() in url_lower:
                found.append(f"url:{term}")
                types.add("url")
        for term in signals.get("generator_contains", []):
            if term.lower() in generator:
                found.append(f"generator:{term}")
                types.add("generator")
        for term in signals.get("html_contains", []):
            if term.lower() in html_lower:
                found.append(f"html:{term}")
                types.add("html")
        type_scores = {t: WEIGHTS[t] for t in types}
        if "html" in types and any(
                marker in html_lower for marker in STRONG_HTML_MARKERS):
            type_scores["html"] = 50
        score = min(100, sum(type_scores.values()) + max(0, len(found) - len(types)) * 5)
        required_types = rule.get("minimum_signal_types", min_types)
        # The CLI threshold is a hard floor. A rule may demand more, never less.
        rule_threshold = max(threshold, rule.get("minimum_score", 0))
        if score >= rule_threshold and len(types) >= required_types:
            matches.append(ProspectMatch(rule["id"], rule["family"], rule.get("platform"),
                                         score, len(types), found))
    return sorted(matches, key=lambda m: (-m.score, -m.signal_types, m.rule_id))
