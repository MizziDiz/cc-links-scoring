"""Scored classifier for Common Crawl link-prospect candidates."""
import hashlib
import json
import os
import re
from itertools import permutations
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit

from cc_links.engines import get_generator

DEFAULT_FOOTPRINTS = os.path.join(os.path.dirname(__file__), "prospect_footprints.json")
TRACKING_PARAMS = {"fbclid", "gclid", "yclid", "mc_cid", "mc_eid"}
WEIGHTS = {"url": 55, "generator": 55, "html": 25}
STRONG_HTML_MARKERS = (
    "powered by", "wp-comments-post.php", "comment_post_id", "mw-content-text",
    "simple machines forum", "coppermine photo gallery", "trackback url",
    "wpforo", "jcommentsinitializeform", "dokuwiki__top", "tikiwiki cms",
    "power by dedecms",
)

# Broad but structural URL markers used only to open the discovery funnel.
# They do not grant a final score: every resulting page is still fetched from
# WARC and passed through the normal classifier/minimum-score threshold.
BROAD_DISCOVERY_TERMS = (
    "/forum/", "/forums/", "/community/", "/board/", "/boards/",
    "/thread/", "/threads/", "/topic/", "/topics/",
    "viewtopic", "showthread", "viewthread", "forumdisplay",
    "memberlist", "/members/", "/member/", "/users/", "/user/",
    "/profile/", "profile.php", "member.php", "register.php",
    "/comment/", "/comments/", "comment.php", "reply.php", "#respond",
    "guestbook", "gbook.php", "signbook", "addentry",
    "/wiki/", "index.php?title=", "special:", "special%3a",
    "submit-article", "submit_article", "add-article", "add_article",
    "submit-link", "submit-site", "add-url", "add-listing",
    "/directory/", "displayimage.php", "addcomment",
    "/trackback/", "wp-trackback.php", "mt-tb.cgi", "pingback",
    "nv_redirect=", "/redirect", "goto=", "/go.php", "/proxy.php",
    "/opener.php", "oadest", "bo_table=", "wr_id=",
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


@dataclass
class DiscoveryMatch:
    pattern_id: str
    family: str
    platform: Optional[str]
    tier: int
    score: int
    signals: List[str]

    def to_dict(self):
        return asdict(self)


def load_taxonomy(path: Optional[str] = None):
    with open(path or DEFAULT_FOOTPRINTS, "r", encoding="utf-8") as f:
        return json.load(f)


def load_prospect_rules(path: Optional[str] = None):
    data = load_taxonomy(path)
    return data.get("defaults", {}), data["rules"]


def taxonomy_hash(path: Optional[str] = None) -> str:
    """Stable short hash used to bind checkpoints to an exact ruleset."""
    encoded = json.dumps(
        load_taxonomy(path), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def taxonomy_version(path: Optional[str] = None) -> int:
    return int(load_taxonomy(path).get("version", 1))


def discovery_ruleset_identity(
        crawl: str, path: Optional[str] = None,
        profile: str = "precise",
        broad_sample_fraction: Optional[float] = None,
        priority_profile_hash: Optional[str] = None) -> dict:
    """Stable identity shared by checkpoints and completion markers."""
    identity = {
        "crawl": crawl,
        "taxonomy_hash": taxonomy_hash(path),
        "discovery_profile": profile,
    }
    if profile == "broad":
        identity["broad_sample_fraction"] = float(
            broad_sample_fraction if broad_sample_fraction is not None else 0.0
        )
    if priority_profile_hash:
        identity["priority_profile_hash"] = priority_profile_hash
    return identity


def broad_discovery_terms(path: Optional[str] = None) -> Tuple[str, ...]:
    """Broad structural terms from the taxonomy, with legacy-file fallback."""
    configured = (
        load_taxonomy(path).get("discovery", {}).get("broad_terms", [])
    )
    terms = configured or BROAD_DISCOVERY_TERMS
    return tuple(dict.fromkeys(str(term).lower() for term in terms if term))


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


def discovery_url_patterns(path: Optional[str] = None,
                           family: Optional[str] = None) -> List[Tuple[str, ...]]:
    """Return selective URL clauses; terms within a clause must all match.

    Rules may provide an explicit ``discovery`` list of string lists. This allows
    precise index filters such as (``/bitrix/redirect.php`` AND ``goto=``) without
    making either broad term a global OR condition. Legacy rules fall back to one
    clause per selective ``url_contains`` term.
    """
    _, rules = load_prospect_rules(path)
    legacy_terms = set(discovery_url_terms(path))
    patterns = set()
    for rule in rules:
        if family is not None and rule.get("family") != family:
            continue
        explicit = rule.get("discovery")
        if explicit:
            for clause in explicit:
                normalized = tuple(dict.fromkeys(str(term).lower() for term in clause if term))
                if normalized:
                    patterns.add(normalized)
            continue
        for term in rule.get("signals", {}).get("url_contains", []):
            if term.lower() in legacy_terms:
                patterns.add((term.lower(),))
    return sorted(patterns)


def classify_discovery_url(
        url: str, path: Optional[str] = None,
        include_broad: bool = False) -> List[DiscoveryMatch]:
    """Attribute an index URL to precise rules or broad structural terms.

    This stage never claims that a page is a final prospect. Its score only
    prioritizes WARC fetching; ``classify_prospect`` remains authoritative.
    """
    _, rules = load_prospect_rules(path)
    legacy_terms = set(discovery_url_terms(path))
    url_lower = url.lower()
    source_lower = source_url_for_matching(url)
    matches: List[DiscoveryMatch] = []

    for rule in rules:
        rule_url = (
            url_lower
            if rule.get("family") == "redirect_backlink"
            else source_lower
        )
        explicit = rule.get("discovery")
        if explicit:
            clauses = [
                tuple(str(term).lower() for term in clause if term)
                for clause in explicit
            ]
        else:
            clauses = [
                (str(term).lower(),)
                for term in rule.get("signals", {}).get("url_contains", [])
                if str(term).lower() in legacy_terms
            ]
        for index, clause in enumerate(clauses):
            if clause and all(term in rule_url for term in clause):
                score = int(
                    rule.get("prefetch_score", 65 if len(clause) > 1 else 55)
                )
                matches.append(DiscoveryMatch(
                    pattern_id=f"{rule['id']}:{index}",
                    family=rule["family"],
                    platform=rule.get("platform"),
                    tier=0,
                    score=min(100, score),
                    signals=[f"url:{term}" for term in clause],
                ))

    if not matches and include_broad:
        terms = [term for term in broad_discovery_terms(path) if term in source_lower]
        if terms:
            score = min(45, 20 + max(0, len(terms) - 1) * 5)
            matches.append(DiscoveryMatch(
                pattern_id=f"broad:{terms[0]}",
                family="",
                platform=None,
                tier=1,
                score=score,
                signals=[f"broad_url:{term}" for term in terms],
            ))

    return sorted(
        matches,
        key=lambda match: (
            match.tier, -match.score, -len(match.signals), match.pattern_id
        ),
    )


def _patterns_regex(patterns) -> str:
    branches = set()
    for pattern in patterns:
        # Existing compound clauses mean "all terms, in any order". Expressing
        # them as a small set of ordered regex branches keeps that meaning while
        # letting DuckDB evaluate one vectorized expression instead of dozens of
        # LOWER/INSTR calls per row.
        orders = permutations(pattern) if len(pattern) > 1 else (pattern,)
        for order in orders:
            branches.add(".*".join(re.escape(term.lower()) for term in order))
    return "(?:" + "|".join(sorted(branches, key=lambda term: (-len(term), term))) + ")"


def precise_discovery_regex(path: Optional[str] = None) -> str:
    """Compile the selective URL clauses into one equivalent regex."""
    return _patterns_regex(discovery_url_patterns(path))


def broad_discovery_regex(path: Optional[str] = None) -> str:
    """One vectorized regex covering precise rules plus broad structural paths.

    This is a recall-oriented prefilter only; broad-only matches are sampled
    and quota-limited before WARC fetching, and final classification is unchanged.
    """
    patterns = list(discovery_url_patterns(path))
    patterns.extend((term,) for term in broad_discovery_terms(path))
    return _patterns_regex(patterns)


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


def source_url_for_matching(url: str) -> str:
    """Mask embedded destination URLs before classifying the source page.

    Redirect URLs frequently contain a complete target URL. Platform terms in that
    target (for example ``viewtopic.php``) describe the destination, not the source.
    Redirect-family rules deliberately use the original URL; all other rules use
    this masked representation.
    """
    try:
        parsed = urlsplit(url)
    except ValueError:
        return url.lower()
    pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        decoded = unquote(value).strip().lower()
        if "http://" in decoded or "https://" in decoded:
            value = "{target}"
        pairs.append((key, value))
    path = parsed.path
    path_lower = unquote(path).lower()
    positions = [position for marker in ("http://", "https://")
                 if (position := path_lower.find(marker)) >= 0]
    if positions:
        path = path[:min(positions)] + "{target}"
    return urlunsplit((parsed.scheme, parsed.netloc, path, urlencode(pairs), "")).lower()


def classify_prospect(html: str, url: str, footprints_path: Optional[str] = None,
                      minimum_score: Optional[int] = None) -> List[ProspectMatch]:
    defaults, rules = load_prospect_rules(footprints_path)
    threshold = minimum_score if minimum_score is not None else defaults.get("minimum_score", 50)
    min_types = defaults.get("minimum_signal_types", 2)
    url_lower = url.lower()
    source_url_lower = source_url_for_matching(url)
    html_lower = html.lower()
    generator = get_generator(html)
    matches = []

    for rule in rules:
        found = []
        types = set()
        signals = rule.get("signals", {})
        rule_url = url_lower if rule.get("family") == "redirect_backlink" else source_url_lower
        for term in signals.get("url_contains", []):
            if term.lower() in rule_url:
                found.append(f"url:{term}")
                types.add("url")
        for clause in signals.get("url_all", []):
            if clause and all(term.lower() in rule_url for term in clause):
                found.append("url_all:" + " + ".join(clause))
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
