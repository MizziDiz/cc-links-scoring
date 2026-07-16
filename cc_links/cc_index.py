"""Country-wide URL discovery via the public Common Crawl columnar index.

This is the direct replacement for Athena: Athena normally runs SQL over the
same `cc-index` Parquet table stored at s3://commoncrawl/cc-index/table/...
Since that table is also mirrored over plain HTTPS at data.commoncrawl.org,
DuckDB (with the httpfs extension) can query it directly -- no AWS account,
no credentials, no cluster. Each Parquet part already contains the WARC
filename/offset/length, so results here can be fetched immediately without a
separate CDX lookup.

For large budgets (hundreds of thousands of URLs per country) the scan itself
can take a while, so results stream to a JSONL file as they're found and
progress is checkpointed part-by-part -- an interrupted scan can be resumed
instead of starting over.
"""
import gc
import gzip
import io
import json
import os
import time

import duckdb
import requests

BASE_URL = "https://data.commoncrawl.org/"
PATHS_URL = BASE_URL + "crawl-data/{crawl}/cc-index-table.paths.gz"

# DuckDB's httpfs / parquet reader appears to retain buffers across queries on a
# long-lived connection -- scanning all ~300 index parts on one connection was
# observed to grow RSS to 12GB+ within 30 minutes. Recreating the connection
# periodically and capping how many rows a single query can materialize keeps
# memory bounded.
_RECONNECT_EVERY = 15


def get_index_parts(crawl: str):
    """Return HTTPS URLs of the cc-index Parquet parts (subset=warc) for a crawl."""
    resp = requests.get(PATHS_URL.format(crawl=crawl), timeout=30)
    resp.raise_for_status()
    with gzip.open(io.BytesIO(resp.content), "rt") as f:
        paths = [line.strip() for line in f if line.strip()]
    return [BASE_URL + p for p in paths if "/subset=warc/" in p]


def _connect(proxy=None):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET memory_limit = '1GB';")
    con.execute("SET enable_object_cache = false;")
    if proxy:
        host, port, user, pw = proxy
        con.execute(f"SET http_proxy = '{host}:{port}'")
        if user:
            con.execute(f"SET http_proxy_username = '{user}'")
            con.execute(f"SET http_proxy_password = '{pw}'")
    return con


def _parse_proxy_line(line: str):
    parts = line.strip().split(":")
    if len(parts) == 4:
        return (parts[0], parts[1], parts[2], parts[3])
    if len(parts) == 2:
        return (parts[0], parts[1], None, None)
    return None


def load_proxies(path: str):
    """Read `host:port:user:pass` (or `host:port`) lines into (host,port,user,pw) tuples."""
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            p = _parse_proxy_line(line)
            if p:
                out.append(p)
    return out


def _load_state(state_path):
    if os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def _save_state(state_path, scanned_parts, remaining, domain_counts):
    tmp = state_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"scanned_parts": sorted(scanned_parts), "remaining": remaining,
                    "domain_counts": domain_counts}, f)
    os.replace(tmp, state_path)


def discover_by_countries(crawl: str, category_budgets: dict, tld_to_category: dict,
                           is_excluded_fn, out_path: str,
                           max_parts=None, per_tld_cap: int = 250_000,
                           max_per_domain: int = None, progress=None, resume: bool = True,
                           max_retries: int = 6, retry_backoff: float = 8.0, proxies=None,
                           part_delay: float = 0.0, url_terms=None, part_shard=None):
    """Scan Parquet index parts, streaming matches to out_path (JSONL) as they're found.

    category_budgets: {"Colombia": 100000, "Other Africa": 100000, ...} -- max pages
    to collect per *category*. A category can span several ccTLDs (a regional bucket),
    which then share one combined budget rather than getting one budget each.
    tld_to_category: {"co": "Colombia", "eg": "Other Africa", "ng": "Other Africa", ...}
    -- maps every ccTLD being scanned to the category whose budget it draws from. For a
    plain per-ccTLD run this is just the identity map {"co": "co", ...}.
    is_excluded_fn: callable(domain) -> bool, used to drop global mega-platforms.
    per_tld_cap: safety ceiling on rows taken *per ccTLD* from a single part -- the
    actual per-part cap used is min(largest remaining budget among active categories,
    this ceiling), so a category can pull its entire remaining budget from one "hot"
    part in a single shot (observed: one part alone held 6.3M matches for one
    ccTLD; Ecuador's whole budget once came from a single part). Once a part is
    scanned it's never revisited, so under-capping here silently and permanently
    throws away everything past the cap. Per-query memory is bounded separately,
    by recycling the DuckDB connection periodically (see _RECONNECT_EVERY), not by
    shrinking this.
    max_per_domain: cap pages taken from any single registered domain (e.g. a single
    high-volume news/e-commerce site can otherwise fill a whole ccTLD's budget by
    itself, skewing the engine-market-share stats towards that one site's CMS).
    Resumable: progress (scanned part indices + remaining budget + per-domain counts)
    is checkpointed to `<out_path>.state.json` after every part.
    Returns the final `remaining` dict (0 for categories that were fully filled).
    """
    state_path = out_path + ".state.json"
    state = _load_state(state_path) if resume else None
    scanned_parts = set(state["scanned_parts"]) if state else set()
    remaining = state["remaining"] if state else dict(category_budgets)
    domain_counts = state["domain_counts"] if state and "domain_counts" in state else {}

    parts = get_index_parts(crawl)
    if max_parts and max_parts < len(parts):
        stride = len(parts) / max_parts
        allowed_idx = {int(i * stride) for i in range(max_parts)}
    else:
        allowed_idx = set(range(len(parts)))
    if part_shard:
        shard_index, shard_count = part_shard
        allowed_idx = {i for i in allowed_idx if i % shard_count == shard_index}

    # When proxies are supplied, each new connection binds to the next proxy so
    # parquet reads rotate across exit IPs -- the fix for the CloudFront throttling
    # that killed 192/300 parts on the un-proxied run. Reconnect every part (vs
    # every _RECONNECT_EVERY) so the IP actually rotates.
    _prox_idx = [0]

    def make_conn():
        p = None
        if proxies:
            p = proxies[_prox_idx[0] % len(proxies)]
            _prox_idx[0] += 1
        return _connect(p)

    reconnect_every = 1 if proxies else _RECONNECT_EVERY
    con = make_conn()
    out_mode = "a" if (state and os.path.exists(out_path)) else "w"
    out_f = open(out_path, out_mode, encoding="utf-8")

    try:
        for i, part_url in enumerate(parts):
            if i not in allowed_idx or i in scanned_parts:
                continue
            active_cats = {c for c, v in remaining.items() if v > 0}
            if not active_cats:
                break
            # Query every ccTLD that still feeds an unfilled category. Several
            # ccTLDs can map to the same category (a regional bucket) and draw
            # down its shared budget together.
            active_tlds = [t for t, c in tld_to_category.items() if c in active_cats]
            if not active_tlds:
                break

            tld_list_sql = ", ".join(f"'{t}'" for t in active_tlds)
            # A single shared LIMIT lets whichever ccTLD is most common in this part
            # crowd out the others entirely (observed: Ecuador got 0 matches across
            # all 300 parts because Colombia/Chile/Peru filled the row cap first in
            # every part they shared). Cap rows per-tld instead so every active
            # country gets a fair slice of this part regardless of relative volume.
            #
            # The per-tld cap must scale with what's still needed: a ccTLD's matches
            # are often concentrated in just one or two "hot" parts (observed: one
            # part alone held 6.3M matches for one ccTLD; Ecuador's entire budget
            # came from a single part). A cap fixed below the true budget (e.g. a
            # flat 50k against a 200k target) silently throws away everything past
            # the cap in that hot part and there's no second chance -- once a part
            # is marked scanned it's never revisited. So size the cap to the largest
            # remaining need among active ccTLDs (bounded by a sane ceiling so one
            # freak part can't blow up memory).
            effective_cap = min(max(remaining[c] for c in active_cats), per_tld_cap)
            url_filter = ""
            if url_terms:
                escaped_terms = [str(t).lower().replace("'", "''") for t in url_terms]
                clauses = [f"INSTR(LOWER(url), '{t}') > 0" for t in escaped_terms]
                url_filter = "AND (" + " OR ".join(clauses) + ")"
            query = f"""
                SELECT url, url_host_tld, url_host_registered_domain,
                       warc_filename, warc_record_offset, warc_record_length
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY url_host_tld) AS rn
                    FROM read_parquet('{part_url}')
                    WHERE fetch_status = 200
                      AND content_mime_detected = 'text/html'
                      AND url_host_tld IN ({tld_list_sql})
                      {url_filter}
                )
                WHERE rn <= {effective_cap}
            """
            # Reading the parquet parts pulls them straight from data.commoncrawl.org
            # over HTTPS with no proxy, so a long fast scan gets CloudFront-throttled
            # (surfaces as DuckDB "HTTP 0 Internal Server Error"). Retry with backoff
            # on a fresh connection; the throttle is transient and clears on cooldown.
            rows = None
            last_err = None
            for attempt in range(max_retries):
                try:
                    rows = con.execute(query).fetchall()
                    break
                except Exception as e:
                    last_err = e
                    try:
                        con.close()
                    except Exception:
                        pass
                    gc.collect()
                    con = make_conn()  # rotate to a different exit IP on throttle
                    if attempt < max_retries - 1:
                        sleep_s = min(retry_backoff * (2 ** attempt), 300.0)
                        if progress:
                            progress(f"part {i} read failed ({e}); "
                                     f"retry {attempt + 1}/{max_retries - 1} in {sleep_s:.0f}s")
                        time.sleep(sleep_s)
            if rows is None:
                if progress:
                    progress(f"skip part {i} after {max_retries} attempts ({last_err}) "
                             f"-- left UNscanned so a later resume retries it")
                # Deliberately NOT added to scanned_parts: marking a throttled part
                # scanned would permanently discard everything in it (this is exactly
                # how Japan/Malaysia/Singapore/Ecuador/Korea came back empty).
                continue

            found_this_part = 0
            for url, tld, reg_domain, warc_filename, offset, length in rows:
                cat = tld_to_category.get(tld)
                if cat is None or remaining.get(cat, 0) <= 0:
                    continue
                if is_excluded_fn(reg_domain):
                    continue
                if max_per_domain:
                    dcount = domain_counts.get(reg_domain, 0)
                    if dcount >= max_per_domain:
                        continue
                    domain_counts[reg_domain] = dcount + 1
                out_f.write(json.dumps({
                    "url": url, "url_host_tld": tld, "url_host_registered_domain": reg_domain,
                    "bucket": cat,
                    "filename": warc_filename, "offset": offset, "length": length,
                }) + "\n")
                remaining[cat] -= 1
                found_this_part += 1
            out_f.flush()

            scanned_parts.add(i)
            _save_state(state_path, scanned_parts, remaining, domain_counts)

            if progress:
                progress(f"part {i+1}/{len(parts)}: +{found_this_part} matches; remaining: {remaining}")

            del rows
            if len(scanned_parts) % reconnect_every == 0:
                con.close()
                gc.collect()
                con = make_conn()
            # Pace direct (un-proxied) reads so the sustained request rate stays under
            # CloudFront's throttle threshold -- a fast unpaced scan is what got 192
            # parts HTTP-0'd on the first run.
            if part_delay:
                time.sleep(part_delay)
    finally:
        out_f.close()
        con.close()

    return remaining


def load_candidates(path: str):
    """Stream candidate records back out of a JSONL file written by discover_by_countries."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def load_candidates_shuffled(path: str, seed: int = 42):
    """Like load_candidates, but interleaved randomly instead of in discovery order.

    Discovery writes results in contiguous per-ccTLD blocks (each country is filled
    from whichever index parts happen to match it), so a naive sequential read hits
    one country at a time. If the source ever gets rate-limited partway through a
    run, that would wipe out whichever countries are still unprocessed rather than
    spreading the loss evenly -- so shuffle by byte offset (cheap: only offsets are
    held in memory, not the records themselves) before reading.
    """
    import random

    offsets = []
    with open(path, "rb") as f:
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            if line.strip():
                offsets.append(pos)

    random.Random(seed).shuffle(offsets)

    with open(path, "r", encoding="utf-8") as f:
        for pos in offsets:
            f.seek(pos)
            line = f.readline()
            yield json.loads(line)
