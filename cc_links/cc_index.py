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


def _connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET memory_limit = '1GB';")
    con.execute("SET enable_object_cache = false;")
    return con


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


def discover_by_countries(crawl: str, tld_budgets: dict, is_excluded_fn, out_path: str,
                           max_parts=None, per_tld_cap: int = 250_000,
                           max_per_domain: int = None, progress=None, resume: bool = True):
    """Scan Parquet index parts, streaming matches to out_path (JSONL) as they're found.

    tld_budgets: {"co": 200000, "mx": 200000, ...} -- max pages to collect per ccTLD.
    is_excluded_fn: callable(domain) -> bool, used to drop global mega-platforms.
    per_tld_cap: safety ceiling on rows taken *per ccTLD* from a single part -- the
    actual per-part cap used is min(largest remaining budget among active ccTLDs,
    this ceiling), so a ccTLD can pull its entire remaining budget from one "hot"
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
    Returns the final `remaining` dict (0 for ccTLDs that were fully filled).
    """
    state_path = out_path + ".state.json"
    state = _load_state(state_path) if resume else None
    scanned_parts = set(state["scanned_parts"]) if state else set()
    remaining = state["remaining"] if state else dict(tld_budgets)
    domain_counts = state["domain_counts"] if state and "domain_counts" in state else {}

    parts = get_index_parts(crawl)
    if max_parts and max_parts < len(parts):
        stride = len(parts) / max_parts
        allowed_idx = {int(i * stride) for i in range(max_parts)}
    else:
        allowed_idx = set(range(len(parts)))

    con = _connect()
    out_mode = "a" if (state and os.path.exists(out_path)) else "w"
    out_f = open(out_path, out_mode, encoding="utf-8")

    try:
        for i, part_url in enumerate(parts):
            if i not in allowed_idx or i in scanned_parts:
                continue
            active_tlds = [t for t, v in remaining.items() if v > 0]
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
            effective_cap = min(max(remaining[t] for t in active_tlds), per_tld_cap)
            query = f"""
                SELECT url, url_host_tld, url_host_registered_domain,
                       warc_filename, warc_record_offset, warc_record_length
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY url_host_tld) AS rn
                    FROM read_parquet('{part_url}')
                    WHERE fetch_status = 200
                      AND content_mime_detected = 'text/html'
                      AND url_host_tld IN ({tld_list_sql})
                )
                WHERE rn <= {effective_cap}
            """
            try:
                rows = con.execute(query).fetchall()
            except Exception as e:
                if progress:
                    progress(f"skip part {i} ({e})")
                scanned_parts.add(i)
                continue

            found_this_part = 0
            for url, tld, reg_domain, warc_filename, offset, length in rows:
                if remaining.get(tld, 0) <= 0:
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
                    "filename": warc_filename, "offset": offset, "length": length,
                }) + "\n")
                remaining[tld] -= 1
                found_this_part += 1
            out_f.flush()

            scanned_parts.add(i)
            _save_state(state_path, scanned_parts, remaining, domain_counts)

            if progress:
                progress(f"part {i+1}/{len(parts)}: +{found_this_part} matches; remaining: {remaining}")

            del rows
            if len(scanned_parts) % _RECONNECT_EVERY == 0:
                con.close()
                gc.collect()
                con = _connect()
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
