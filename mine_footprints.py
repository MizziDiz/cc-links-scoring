#!/usr/bin/env python3
"""Mine footprint evidence from our own databases, read-only.

Two sources, either or both:

* ``--db prospects.db`` (Common Crawl collector): yield of every discovery
  pattern, which classifier rules and signals actually fire, and URL tokens
  that separate stored candidates from pages the classifier rejected.
* ``--gsa-db gsabases.db`` (our GSA verified/success bases, one row per URL
  with the engine name): engines ranked by unique hosts, URL tokens specific
  to each engine, and the engines the Common Crawl taxonomy does not cover.

Nothing here edits the taxonomy. The report is evidence for curating
``cc_links/prospect_footprints.json``; every proposed pattern should still be
checked against sampled HTML (``sample_html_markers.py``) before it ships.

    python mine_footprints.py --db prospects.db --since 2026-07-25 \
        --taxonomy cc_links/prospect_footprints.json --out report.json
    python mine_footprints.py --gsa-db gsabases.db --out gsa-report.json
"""
from __future__ import annotations

import argparse
import collections
import json
import re
import sqlite3
from typing import Any, Iterable
from urllib.parse import urlsplit

DECISION_OUTCOMES = ("stored", "unmatched", "below_threshold")
NOT_PLACEMENT_ENGINES = ("fast indexer", "url redirect", "url shortener", "pingback",
                         "trackback", "indexer", "exploit", "whois", "referrer")


def read_only(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


# ---------------------------------------------------------------- tokens ---

def url_tokens(url_or_path: str) -> set[str]:
    """Stable URL pieces: path segments, file names, query keys and key=value."""
    if "://" not in url_or_path:
        url_or_path = "http://x" + (url_or_path if url_or_path.startswith("/")
                                     else "/" + url_or_path)
    parts = urlsplit(url_or_path.lower())
    out: set[str] = set()
    segments = [s for s in parts.path.split("/") if s]
    for segment in segments:
        if segment.isdigit() or len(segment) > 40 or re.search(r"\d{4,}", segment):
            continue
        out.add("seg:" + segment)
    if segments and "." in segments[-1]:
        out.add("file:" + segments[-1][:40])
    for pair in parts.query.split("&"):
        if not pair:
            continue
        key, _, value = pair.partition("=")
        if key and len(key) <= 30:
            out.add("qk:" + key)
        if value and not value.isdigit() and len(value) <= 30 \
                and not re.search(r"\d{3,}", value):
            out.add("qkv:" + key + "=" + value)
    return out


def token_term(token: str) -> str:
    """The URL substring a token stands for, as a discovery term."""
    kind, _, body = token.partition(":")
    if kind == "seg":
        return "/" + body + ("/" if "." not in body else "")
    if kind == "file":
        return body
    if kind == "qk":
        return body + "="
    if kind == "qkv":
        return body
    return body


# -------------------------------------------------------------- taxonomy ---

def load_taxonomy(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with open(path, encoding="utf-8") as source:
        return json.load(source)


def taxonomy_terms(taxonomy: dict[str, Any]) -> set[str]:
    terms: set[str] = set()
    for rule in taxonomy.get("rules", []):
        for clause in rule.get("discovery", []) or []:
            terms.update(str(t).lower() for t in clause)
        terms.update(str(t).lower() for t in rule.get("signals", {}).get("url_contains", []))
    terms.update(str(t).lower() for t in taxonomy.get("discovery", {}).get("broad_terms", []))
    return terms


def term_covered(term: str, known: Iterable[str]) -> bool:
    term = term.lower()
    return any(term in k or k in term for k in known if len(k) >= 4)


# GSA engine names that do not contain the taxonomy platform/rule name.
GSA_ENGINE_ALIASES = {
    "general blogs": "wordpress",
    "wordpress article": "wordpress",
    "wordpress directory": "wordpress",
    "wordpress forum": "wpforo",
    "general bbs": "generic_forum",
    "gnuboard": "generic_board_post",
    "datalife cms": "datalife",
    "dedeeims": "dedecms",
    "bbpress (forum profile)": "bbpress",
    "aska bbs": "cgi_bbs_jp",
    "yyboard": "cgi_bbs_jp",
    "easybook reloaded": "easybook",
    "kideshoutbox": "kide",
    "php link directory": "phplinkdirectory",
    "general url shortener": "nukeviet",
    "drupal - comment": "drupal",
    "drupal - blog": "drupal",
    "vbulletin - blog": "vbulletin",
    "trackback-format2": "trackback",
}


def taxonomy_platforms(taxonomy: dict[str, Any]) -> set[str]:
    names = set()
    for rule in taxonomy.get("rules", []):
        for value in (rule.get("platform"), rule.get("id")):
            if value:
                names.add(re.sub(r"[^a-z0-9]", "", str(value).lower()))
    return names


def engine_key(engine: str) -> str:
    alias = GSA_ENGINE_ALIASES.get(engine.lower().strip(), engine)
    return re.sub(r"[^a-z0-9]", "", alias.lower())


# -------------------------------------------------------- prospects.db ---

def pattern_yield(conn: sqlite3.Connection, since: str | None) -> list[dict[str, Any]]:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(processed_urls)")}
    if "pattern_id" not in columns:
        return []
    where = "outcome IN (%s) AND pattern_id IS NOT NULL AND pattern_id <> ''" % (
        ", ".join("?" for _ in DECISION_OUTCOMES))
    params: list[Any] = list(DECISION_OUTCOMES)
    if since and "processed_at" in columns:
        where += " AND processed_at >= ?"
        params.append(since)
    rows = conn.execute(
        f"""SELECT pattern_id, discovery_tier, COUNT(*),
                   SUM(CASE WHEN outcome='stored' THEN 1 ELSE 0 END),
                   COUNT(DISTINCT CASE WHEN outcome='stored' THEN registered_domain END)
            FROM processed_urls WHERE {where}
            GROUP BY pattern_id, discovery_tier ORDER BY 3 DESC""", params).fetchall()
    return [{"pattern_id": p, "tier": t, "decisions": d, "stored": s or 0,
             "yield": round((s or 0) / d, 4) if d else 0.0, "stored_domains": sd or 0}
            for p, t, d, s, sd in rows]


def rule_activity(conn: sqlite3.Connection, taxonomy: dict[str, Any],
                  sample_every: int = 9, limit: int = 250000) -> dict[str, Any]:
    finals = conn.execute(
        """SELECT COALESCE(final_rule_id, ''), COUNT(*), COUNT(DISTINCT registered_domain)
           FROM processed_urls WHERE outcome='stored' GROUP BY 1 ORDER BY 2 DESC""").fetchall()
    observed: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    sampled = 0
    for (blob,) in conn.execute(
            "SELECT matched_signals FROM candidates WHERE rowid % ? = 0 LIMIT ?",
            (sample_every, limit)):
        try:
            matches = json.loads(blob or "[]")
        except ValueError:
            continue
        sampled += 1
        for match in matches:
            for signal in match.get("signals", []):
                observed[match.get("rule_id", "")][signal] += 1
    dead_signals = []
    silent_rules = []
    for rule in taxonomy.get("rules", []):
        rid = rule["id"]
        seen = observed.get(rid, collections.Counter())
        if not seen:
            silent_rules.append(rid)
        signals = rule.get("signals", {})
        declared = ([f"url:{t}" for t in signals.get("url_contains", [])]
                    + [f"generator:{t}" for t in signals.get("generator_contains", [])]
                    + [f"html:{t}" for t in signals.get("html_contains", [])])
        for name in declared:
            if name.lower() not in {k.lower() for k in seen}:
                dead_signals.append({"rule": rid, "signal": name})
    return {
        "sampled_candidates": sampled,
        "final_rules": [{"rule": r, "stored": n, "domains": d} for r, n, d in finals],
        "signals_by_rule": {rid: cnt.most_common(12) for rid, cnt in observed.items()},
        "silent_rules": silent_rules,
        "dead_signals": dead_signals,
    }


def token_precision(conn: sqlite3.Connection, since: str | None, min_support: int,
                    taxonomy: dict[str, Any]) -> dict[str, Any]:
    stored: collections.Counter = collections.Counter()
    stored_domains: dict[str, set] = collections.defaultdict(set)
    unmatched: collections.Counter = collections.Counter()
    unmatched_domains: dict[str, set] = collections.defaultdict(set)
    n_stored = n_unmatched = 0
    for url, domain in conn.execute("SELECT url, registered_domain FROM candidates"):
        n_stored += 1
        for token in url_tokens(url):
            stored[token] += 1
            stored_domains[token].add(domain)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(processed_urls)")}
    where = "outcome='unmatched'"
    params: list[Any] = []
    if since and "processed_at" in columns:
        where += " AND processed_at >= ?"
        params.append(since)
    for url, domain in conn.execute(
            f"SELECT url, registered_domain FROM processed_urls WHERE {where}", params):
        n_unmatched += 1
        for token in url_tokens(url):
            unmatched[token] += 1
            unmatched_domains[token].add(domain)
    known = taxonomy_terms(taxonomy)
    rows = []
    # Union of both sides: a trap can be a token that never reaches candidates.
    for token in set(stored) | set(unmatched):
        s = stored.get(token, 0)
        u = unmatched.get(token, 0)
        if s + u < min_support:
            continue
        term = token_term(token)
        rows.append({
            "token": token, "term": term, "stored": s, "unmatched": u,
            "precision": round(s / (s + u), 3),
            "stored_domains": len(stored_domains[token]),
            "unmatched_domains": len(unmatched_domains.get(token, ())),
            "in_taxonomy": term_covered(term, known),
        })
    rows.sort(key=lambda r: -r["stored_domains"])
    domain_floor = max(10, min_support)
    proposals = [r for r in rows if r["precision"] >= 0.85
                 and r["stored_domains"] >= domain_floor and not r["in_taxonomy"]]
    traps = sorted([r for r in rows if r["precision"] < 0.15
                    and r["unmatched_domains"] >= domain_floor],
                   key=lambda r: -r["unmatched"])
    return {"stored_urls": n_stored, "unmatched_urls": n_unmatched,
            "tokens": rows[:2000], "proposals": proposals[:200], "traps": traps[:100]}


# ---------------------------------------------------------- gsabases.db ---

def gsa_engines(conn: sqlite3.Connection, taxonomy: dict[str, Any], min_hosts: int,
                min_token_hosts: int) -> dict[str, Any]:
    kinds = ("verified", "success")
    engine_hosts: dict[str, set] = collections.defaultdict(set)
    engine_type: dict[str, str] = {}
    token_hosts: dict[str, set] = collections.defaultdict(set)
    engine_token_hosts: dict[str, dict[str, set]] = collections.defaultdict(
        lambda: collections.defaultdict(set))
    placeholders = ", ".join("?" for _ in kinds)
    for engine, etype, host, path in conn.execute(
            f"SELECT engine, type, host, path FROM rows WHERE kind IN ({placeholders})", kinds):
        engine = engine or "?"
        engine_type[engine] = etype or ""
        engine_hosts[engine].add(host)
        for token in url_tokens(path or "/"):
            token_hosts[token].add(host)
            engine_token_hosts[engine][token].add(host)
    known_platforms = taxonomy_platforms(taxonomy)
    known_terms = taxonomy_terms(taxonomy)
    engines = []
    for engine, hosts in sorted(engine_hosts.items(), key=lambda kv: -len(kv[1])):
        if len(hosts) < min_hosts:
            continue
        key = engine_key(engine)
        placement = not any(bad in engine.lower() for bad in NOT_PLACEMENT_ENGINES)
        covered = any(key and (key in p or p in key) for p in known_platforms if len(p) >= 4)
        tokens = []
        for token, th in engine_token_hosts[engine].items():
            if len(th) < min_token_hosts:
                continue
            specificity = len(th) / len(token_hosts[token])
            if specificity < 0.5:
                continue
            term = token_term(token)
            tokens.append({"token": token, "term": term, "hosts": len(th),
                           "specificity": round(specificity, 2),
                           "coverage": round(len(th) / len(hosts), 3),
                           "in_taxonomy": term_covered(term, known_terms)})
        tokens.sort(key=lambda t: -(t["hosts"] * t["specificity"]))
        engines.append({"engine": engine, "type": engine_type.get(engine, ""),
                        "hosts": len(hosts), "placement": placement,
                        "covered_by_taxonomy": covered, "tokens": tokens[:20]})
    gaps = [e for e in engines if e["placement"] and not e["covered_by_taxonomy"]]
    return {"engines": engines, "coverage_gaps": gaps}


# ----------------------------------------------------------------- main ---

def render_markdown(report: dict[str, Any]) -> str:
    lines = ["# Footprint mining report", ""]
    cc = report.get("prospects")
    if cc:
        lines += ["## Discovery pattern yield", "",
                  "| pattern | tier | decisions | stored | yield | domains |", "|---|---|---:|---:|---:|---:|"]
        for r in cc["pattern_yield"][:60]:
            lines.append(f"| `{r['pattern_id']}` | {r['tier']} | {r['decisions']} | {r['stored']} | "
                         f"{r['yield']:.1%} | {r['stored_domains']} |")
        act = cc["rule_activity"]
        lines += ["", f"Silent rules (no signal observed in {act['sampled_candidates']} sampled candidates): "
                  + (", ".join(act["silent_rules"]) or "none"), ""]
        lines += ["Declared signals never observed:", ""]
        for d in act["dead_signals"][:80]:
            lines.append(f"- `{d['rule']}`: `{d['signal']}`")
        tp = cc["tokens"]
        lines += ["", f"## URL tokens ({tp['stored_urls']} stored vs {tp['unmatched_urls']} unmatched URLs)", "",
                  "Proposed new discovery terms (precision >= 85%, >= 200 stored domains, not in taxonomy):", "",
                  "| term | stored | unmatched | precision | domains |", "|---|---:|---:|---:|---:|"]
        for r in tp["proposals"][:60]:
            lines.append(f"| `{r['term']}` | {r['stored']} | {r['unmatched']} | {r['precision']:.1%} | {r['stored_domains']} |")
        lines += ["", "Traps (precision < 15%, many unmatched domains):", "",
                  "| term | stored | unmatched | domains hit |", "|---|---:|---:|---:|"]
        for r in tp["traps"][:40]:
            lines.append(f"| `{r['term']}` | {r['stored']} | {r['unmatched']} | {r['unmatched_domains']} |")
    gsa = report.get("gsa")
    if gsa:
        lines += ["", "## GSA engines (verified + success, unique hosts)", "",
                  "| engine | type | hosts | placement | covered | top specific terms |", "|---|---|---:|---|---|---|"]
        for e in gsa["engines"][:80]:
            terms = ", ".join(f"`{t['term']}`({t['hosts']})" for t in e["tokens"][:5])
            lines.append(f"| {e['engine']} | {e['type']} | {e['hosts']} | {'yes' if e['placement'] else 'no'} | "
                         f"{'yes' if e['covered_by_taxonomy'] else 'NO'} | {terms} |")
        lines += ["", "### Coverage gaps (placement engines without a taxonomy rule)", ""]
        for e in gsa["coverage_gaps"][:60]:
            terms = ", ".join(f"`{t['term']}`({t['hosts']}, spec {t['specificity']})" for t in e["tokens"][:6])
            lines.append(f"- **{e['engine']}** ({e['type']}, {e['hosts']} hosts): {terms or 'no specific URL term'}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", help="Common Crawl prospects SQLite (read-only)")
    parser.add_argument("--gsa-db", help="gsabases.db with rows(kind, engine, type, host, path)")
    parser.add_argument("--taxonomy", help="prospect_footprints.json to check coverage against")
    parser.add_argument("--since", help="Only processed_urls rows from this date (YYYY-MM-DD)")
    parser.add_argument("--min-support", type=int, default=150)
    parser.add_argument("--min-engine-hosts", type=int, default=20)
    parser.add_argument("--min-token-hosts", type=int, default=20)
    parser.add_argument("--out", help="JSON report path")
    parser.add_argument("--markdown", help="Markdown report path")
    args = parser.parse_args()
    if not args.db and not args.gsa_db:
        parser.error("give --db and/or --gsa-db")
    taxonomy = load_taxonomy(args.taxonomy)
    report: dict[str, Any] = {"since": args.since}
    if args.db:
        conn = read_only(args.db)
        try:
            report["prospects"] = {
                "pattern_yield": pattern_yield(conn, args.since),
                "rule_activity": rule_activity(conn, taxonomy),
                "tokens": token_precision(conn, args.since, args.min_support, taxonomy),
            }
        finally:
            conn.close()
    if args.gsa_db:
        conn = read_only(args.gsa_db)
        try:
            report["gsa"] = gsa_engines(conn, taxonomy, args.min_engine_hosts, args.min_token_hosts)
        finally:
            conn.close()
    markdown = render_markdown(report)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as output:
            json.dump(report, output, ensure_ascii=False, indent=1)
    if args.markdown:
        with open(args.markdown, "w", encoding="utf-8") as output:
            output.write(markdown)
    print(markdown)


if __name__ == "__main__":
    main()
