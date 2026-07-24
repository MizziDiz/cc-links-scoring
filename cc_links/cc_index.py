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
S3_BASE_URL = "s3://commoncrawl/"
PATHS_URL = BASE_URL + "crawl-data/{crawl}/cc-index-table.paths.gz"

# DuckDB's httpfs / parquet reader appears to retain buffers across queries on a
# long-lived connection -- scanning all ~300 index parts on one connection was
# observed to grow RSS to 12GB+ within 30 minutes. Recreating the connection
# periodically and capping how many rows a single query can materialize keeps
# memory bounded.
_RECONNECT_EVERY = 15


def get_index_parts(crawl: str, index_source: str = "https"):
    """Return URLs of the cc-index Parquet parts (subset=warc) for a crawl."""
    if index_source not in {"https", "s3"}:
        raise ValueError(f"unsupported index source: {index_source}")
    resp = requests.get(PATHS_URL.format(crawl=crawl), timeout=30)
    resp.raise_for_status()
    with gzip.open(io.BytesIO(resp.content), "rt") as f:
        paths = [line.strip() for line in f if line.strip()]
    base_url = S3_BASE_URL if index_source == "s3" else BASE_URL
    return [base_url + p for p in paths if "/subset=warc/" in p]


def _connect(proxy=None, index_source: str = "https"):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET memory_limit = '1GB';")
    con.execute("SET enable_object_cache = false;")
    if index_source == "s3":
        # On EC2, DuckDB obtains short-lived credentials from the instance role.
        # A named secret avoids embedding credentials and is recreated with each
        # recycled connection.
        con.execute("INSTALL aws; LOAD aws;")
        con.execute("""
            CREATE SECRET cc_index_s3 (
                TYPE s3,
                PROVIDER credential_chain,
                REGION 'us-east-1'
            )
        """)
    if proxy:
        if index_source == "s3":
            raise ValueError("discovery proxies cannot be used with the S3 index source")
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


def _save_state(state_path, scanned_parts, remaining, domain_counts,
                allowed_parts_count=None, broad_remaining=None, metrics=None,
                checkpoint_identity=None):
    tmp = state_path + ".tmp"
    payload = {"scanned_parts": sorted(scanned_parts), "remaining": remaining,
               "domain_counts": domain_counts}
    if allowed_parts_count is not None:
        payload["allowed_parts_count"] = allowed_parts_count
    if broad_remaining is not None:
        payload["broad_remaining"] = broad_remaining
    if metrics:
        payload["metrics"] = metrics
    if checkpoint_identity:
        payload["checkpoint_identity"] = checkpoint_identity
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.replace(tmp, state_path)


def _parquet_row_count(con, part_url: str) -> int:
    """Read a Parquet part's row count from footer metadata without scanning columns."""
    row = con.execute(
        "SELECT COALESCE(SUM(row_group_num_rows), 0) "
        "FROM parquet_file_metadata(?)",
        [part_url],
    ).fetchone()
    return int(row[0] or 0)


def _validate_checkpoint_identity(state, expected) -> None:
    """Reject incompatible modern checkpoints instead of silently rescanning."""
    if not state or not expected:
        return
    actual = state.get("checkpoint_identity")
    if actual is None:
        # Legacy checkpoints remain resumable and are never relabeled as modern.
        return
    if actual != expected:
        raise ValueError(
            "discovery checkpoint identity mismatch; use the original taxonomy/"
            "profile or a new --state-dir for an explicit backfill "
            f"(checkpoint={actual}, requested={expected})"
        )


def _url_match_sql(url_terms=None, url_patterns=None, url_regex=None):
    if url_regex:
        escaped = str(url_regex).replace("'", "''")
        return f"REGEXP_MATCHES(LOWER(url), '{escaped}')"
    if url_patterns:
        clauses = []
        for pattern in url_patterns:
            terms = [str(term).lower().replace("'", "''") for term in pattern]
            if terms:
                clauses.append("(" + " AND ".join(
                    f"INSTR(LOWER(url), '{term}') > 0" for term in terms) + ")")
        return "(" + " OR ".join(clauses) + ")" if clauses else ""
    if url_terms:
        escaped_terms = [str(term).lower().replace("'", "''") for term in url_terms]
        clauses = [f"INSTR(LOWER(url), '{term}') > 0" for term in escaped_terms]
        return "(" + " OR ".join(clauses) + ")"
    return ""


def _url_filter_sql(url_terms=None, url_patterns=None, url_regex=None):
    expression = _url_match_sql(
        url_terms=url_terms, url_patterns=url_patterns, url_regex=url_regex)
    return "AND " + expression if expression else ""


def discover_by_countries(crawl: str, category_budgets: dict, tld_to_category: dict,
                           is_excluded_fn, out_path: str,
                           max_parts=None, per_tld_cap: int = 250_000,
                           max_per_domain: int = None, progress=None, resume: bool = True,
                           max_retries: int = 6, retry_backoff: float = 8.0, proxies=None,
                           part_delay: float = 0.0, url_terms=None, url_patterns=None,
                           redirect_url_patterns=None, part_shard=None,
                           index_source: str = "https", url_regex=None,
                           priority_url_patterns=None,
                           broad_category_budgets=None,
                           priority_url_regex=None,
                           broad_sample_fraction=None,
                           collect_metrics: bool = False,
                           candidate_metadata_fn=None,
                           checkpoint_identity=None,
                           initial_domain_counts=None):
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
    broad_remaining = None
    if broad_category_budgets is not None:
        unknown = set(broad_category_budgets) - set(category_budgets)
        if unknown:
            raise ValueError(f"unknown broad-budget categories: {sorted(unknown)}")
        broad_remaining = {
            category: min(int(broad_category_budgets.get(category, 0)),
                          int(category_budgets[category]))
            for category in category_budgets
        }
        if state and "broad_remaining" in state:
            broad_remaining = {
                category: min(int(state["broad_remaining"].get(category, 0)),
                              int(broad_remaining[category]))
                for category in category_budgets
            }

    if index_source not in {"https", "s3"}:
        raise ValueError(f"unsupported index source: {index_source}")
    if index_source == "s3" and proxies:
        raise ValueError("discovery proxies cannot be used with the S3 index source")
    if (broad_sample_fraction is not None
            and not 0.0 <= float(broad_sample_fraction) <= 1.0):
        raise ValueError("broad_sample_fraction must be between 0 and 1")

    parts = get_index_parts(crawl, index_source=index_source)
    if max_parts and max_parts < len(parts):
        stride = len(parts) / max_parts
        allowed_idx = {int(i * stride) for i in range(max_parts)}
    else:
        allowed_idx = set(range(len(parts)))
    if part_shard:
        shard_index, shard_count = part_shard
        allowed_idx = {i for i in allowed_idx if i % shard_count == shard_index}

    requested_identity = dict(checkpoint_identity or {})
    requested_identity.update({
        "max_parts": max_parts,
        "part_shard": (
            f"{part_shard[0]}/{part_shard[1]}" if part_shard else None
        ),
    })
    _validate_checkpoint_identity(state, requested_identity)
    # Preserve legacy checkpoints as legacy; assigning the current identity to
    # already-scanned historical parts would make the claim untrue.
    saved_identity = (
        None if state and state.get("checkpoint_identity") is None
        else requested_identity
    )

    scanned_parts = set(state["scanned_parts"]) if state else set()
    remaining = state["remaining"] if state else dict(category_budgets)
    domain_counts = state["domain_counts"] if state and "domain_counts" in state else {}
    for domain, count in (initial_domain_counts or {}).items():
        domain_counts[domain] = max(int(count), int(domain_counts.get(domain, 0)))
    metrics = dict(state.get("metrics", {})) if state else {}

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
        return _connect(p, index_source=index_source)

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
            tld_caps = {
                tld: min(remaining[tld_to_category[tld]], per_tld_cap)
                for tld in active_tlds
            }
            tld_cap_sql = "CASE url_host_tld " + " ".join(
                f"WHEN '{tld}' THEN {cap}" for tld, cap in tld_caps.items()
            ) + " ELSE 0 END"
            url_match = _url_match_sql(
                url_terms=url_terms, url_patterns=url_patterns, url_regex=url_regex)
            priority_match = _url_match_sql(
                url_patterns=priority_url_patterns, url_regex=priority_url_regex)
            discovery_tier_sql = (
                f"CASE WHEN {priority_match} THEN 0 ELSE 1 END"
                if priority_match else "0"
            )
            broad_sample_match = ""
            if priority_match and broad_sample_fraction is not None:
                sample_threshold = int(round(float(broad_sample_fraction) * 10_000))
                broad_sample_match = (
                    f"AND (({priority_match}) OR "
                    f"(HASH(url) % 10000 < {sample_threshold}))"
                )
            redirect_match = _url_match_sql(url_patterns=redirect_url_patterns)
            if url_match:
                status_filter = (
                    "AND (((fetch_status = 200 AND content_mime_detected = 'text/html') "
                    f"AND {url_match} {broad_sample_match})"
                )
                if redirect_match:
                    status_filter += (
                        " OR ((fetch_status BETWEEN 300 AND 399) "
                        f"AND {redirect_match})"
                    )
                status_filter += ")"
            else:
                status_filter = (
                    "AND fetch_status = 200 "
                    "AND content_mime_detected = 'text/html'"
                )
            filtered_source = f"""
                SELECT *, {discovery_tier_sql} AS discovery_tier
                FROM read_parquet('{part_url}')
                WHERE url_host_tld IN ({tld_list_sql})
                  {status_filter}
            """
            if max_per_domain:
                ranked_source = f"""
                    SELECT *
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY url_host_registered_domain
                            ORDER BY discovery_tier
                        ) AS domain_rn
                        FROM ({filtered_source})
                    )
                    WHERE domain_rn <= {int(max_per_domain)}
                """
            else:
                ranked_source = filtered_source
            query = f"""
                SELECT url, url_host_tld, url_host_registered_domain,
                       warc_filename, warc_record_offset, warc_record_length,
                       fetch_status, content_mime_detected, discovery_tier
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY url_host_tld
                        ORDER BY discovery_tier
                    ) AS rn
                    FROM ({ranked_source})
                )
                WHERE rn <= ({tld_cap_sql})
            """
            # Remote Parquet reads can fail transiently on either CloudFront or S3.
            # Retry with backoff on a fresh DuckDB connection. For HTTPS this also
            # rotates the configured proxy; for S3 it refreshes the IAM credentials.
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

            if collect_metrics:
                metrics["parts_scanned"] = int(metrics.get("parts_scanned", 0)) + 1
                metrics["matches_returned"] = (
                    int(metrics.get("matches_returned", 0)) + len(rows)
                )
                try:
                    metrics["index_rows_scanned"] = (
                        int(metrics.get("index_rows_scanned", 0))
                        + _parquet_row_count(con, part_url)
                    )
                except Exception as exc:
                    # Metrics are optional and must never make discovery fail.
                    metrics["index_row_count_errors"] = (
                        int(metrics.get("index_row_count_errors", 0)) + 1
                    )
                    if progress:
                        progress(f"part {i} row-count metric unavailable ({exc})")

            found_this_part = 0
            for (url, tld, reg_domain, warc_filename, offset, length,
                 fetch_status, content_mime, discovery_tier) in rows:
                cat = tld_to_category.get(tld)
                if cat is None or remaining.get(cat, 0) <= 0:
                    if collect_metrics:
                        metrics["category_quota_dropped"] = (
                            int(metrics.get("category_quota_dropped", 0)) + 1
                        )
                    continue
                if (int(discovery_tier) > 0 and broad_remaining is not None
                        and broad_remaining.get(cat, 0) <= 0):
                    if collect_metrics:
                        metrics["broad_quota_dropped"] = (
                            int(metrics.get("broad_quota_dropped", 0)) + 1
                        )
                    continue
                if is_excluded_fn(reg_domain):
                    if collect_metrics:
                        metrics["excluded_domain_dropped"] = (
                            int(metrics.get("excluded_domain_dropped", 0)) + 1
                        )
                    continue
                if max_per_domain:
                    dcount = domain_counts.get(reg_domain, 0)
                    if dcount >= max_per_domain:
                        if collect_metrics:
                            metrics["domain_cap_dropped"] = (
                                int(metrics.get("domain_cap_dropped", 0)) + 1
                            )
                        continue
                    domain_counts[reg_domain] = dcount + 1
                record = {
                    "url": url, "url_host_tld": tld, "url_host_registered_domain": reg_domain,
                    "bucket": cat,
                    "filename": warc_filename, "offset": offset, "length": length,
                    "fetch_status": fetch_status, "content_mime": content_mime,
                    "discovery_tier": int(discovery_tier),
                }
                if candidate_metadata_fn:
                    metadata = candidate_metadata_fn(
                        url, int(discovery_tier), reg_domain, cat) or {}
                    record.update(metadata)
                out_f.write(json.dumps(record) + "\n")
                remaining[cat] -= 1
                if int(discovery_tier) > 0 and broad_remaining is not None:
                    broad_remaining[cat] -= 1
                if collect_metrics:
                    metrics["candidates_written"] = (
                        int(metrics.get("candidates_written", 0)) + 1
                    )
                    tier_key = (
                        "broad_candidates_written"
                        if int(discovery_tier) > 0
                        else "precise_candidates_written"
                    )
                    metrics[tier_key] = int(metrics.get(tier_key, 0)) + 1
                found_this_part += 1
            out_f.flush()

            scanned_parts.add(i)
            _save_state(state_path, scanned_parts, remaining, domain_counts,
                        len(allowed_idx), broad_remaining=broad_remaining,
                        metrics=metrics, checkpoint_identity=saved_identity)

            if progress:
                progress(f"part {i+1}/{len(parts)}: +{found_this_part} matches; remaining: {remaining}")

            del rows
            if len(scanned_parts) % reconnect_every == 0:
                con.close()
                gc.collect()
                con = make_conn()
            # Pacing is only useful for direct CloudFront reads. S3 handles request
            # concurrency and applies its own adaptive throttling/retries.
            if part_delay and index_source == "https":
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


def load_candidates_prioritized(
        path: str, seed: int = 42, priority_profile=None):
    """Prioritize precise/high-score candidates, randomizing ties deterministically.

    Legacy manifests without prefetch metadata retain the old shuffled behavior.
    Only byte offsets and small numeric keys are held in memory.
    """
    import random

    ranked = []
    rng = random.Random(seed)
    adjustment_fn = None
    if priority_profile:
        from cc_links.feedback import priority_adjustment
        adjustment_fn = priority_adjustment
    from cc_links.prospects import classify_discovery_url
    with open(path, "rb") as source:
        while True:
            position = source.tell()
            line = source.readline()
            if not line:
                break
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            tier = int(record.get("discovery_tier", 0))
            score = int(record.get("prefetch_score", 0))
            pattern_id = str(record.get("pattern_id", ""))
            if not pattern_id:
                evidence = classify_discovery_url(
                    str(record.get("url", "")), include_broad=True
                )
                if evidence:
                    pattern_id = evidence[0].pattern_id
            if adjustment_fn:
                score += adjustment_fn(
                    priority_profile,
                    pattern_id,
                    record.get("bucket"),
                )
            ranked.append((tier, -score, rng.random(), position, pattern_id))
    ranked.sort()

    with open(path, "r", encoding="utf-8") as source:
        for _tier, _score, _tie, position, pattern_id in ranked:
            source.seek(position)
            record = json.loads(source.readline())
            if pattern_id and not record.get("pattern_id"):
                record["pattern_id"] = pattern_id
            yield record
