"""Heuristic engine/platform classifier for the market-share analysis.

Looks at the <meta name="generator"> tag, page text and URL path against a
table of known footprints (cc_links/footprints.json) and returns the best
matching (category, engine_name) pair, e.g. ("Forum", "phpBB").

This is intentionally a simple, extensible heuristic (similar in spirit to
W3Techs/Wappalyzer detection) -- not a guarantee of exact CMS identification.
"""
import json
import os

_DEFAULT_PATH = os.path.join(os.path.dirname(__file__), "footprints.json")

with open(_DEFAULT_PATH, "r", encoding="utf-8") as _f:
    _FOOTPRINTS = json.load(_f)["engines"]


def get_generator(soup) -> str:
    tag = soup.find("meta", attrs={"name": lambda v: v and v.lower() == "generator"})
    if tag and tag.get("content"):
        return tag["content"].lower()
    return ""


def classify_engine(html: str, url: str, soup=None):
    """Return (category, engine_name, matched_signal) or (None, None, None)."""
    if soup is None:
        from cc_links.fetch import make_soup
        soup = make_soup(html)

    generator = get_generator(soup)
    html_lower = html.lower()
    url_lower = url.lower()

    for engine in _FOOTPRINTS:
        for g in engine.get("generator", []):
            if g in generator:
                return engine["category"], engine["name"], f"generator:{g}"
        for p in engine.get("url_path_contains", []):
            if p in url_lower:
                return engine["category"], engine["name"], f"url:{p}"
        for h in engine.get("html_contains", []):
            if h in html_lower:
                return engine["category"], engine["name"], f"html:{h}"

    return None, None, None


def known_categories():
    return sorted({e["category"] for e in _FOOTPRINTS})
