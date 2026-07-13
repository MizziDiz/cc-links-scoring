"""Fetch a single WARC record via HTTP range request and extract outbound links.

This replaces the Athena approach: instead of scanning S3 with SQL, we use the
offset/length from the CDX index to pull only the relevant bytes over HTTPS,
then parse that one record with warcio + BeautifulSoup.
"""
import threading
import time
from io import BytesIO
from urllib.parse import urljoin, urlparse

import warnings

import requests
from bs4 import BeautifulSoup

# Some fetched pages are really XML (RSS/sitemaps) served as text/html; bs4 warns
# about parsing XML with an HTML parser. It's harmless here (they just don't match
# any CMS footprint), so silence the noise across millions of pages.
try:
    from bs4 import XMLParsedAsHTMLWarning
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
except Exception:
    pass
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from warcio.archiveiterator import ArchiveIterator

DATA_BASE_URL = "https://data.commoncrawl.org/"

_thread_local = threading.local()

# lxml parses HTML several times faster than the pure-Python html.parser; the
# whole fetch is CPU-bound on parsing, so this is the single biggest speedup.
try:
    import lxml  # noqa: F401
    _BS_PARSER = "lxml"
except Exception:
    _BS_PARSER = "html.parser"


def make_soup(html):
    """BeautifulSoup with the fastest available parser (lxml, else html.parser)."""
    return BeautifulSoup(html, _BS_PARSER)


class RateLimiter:
    """Global pacing shared across all worker threads.

    data.commoncrawl.org (CloudFront) starts returning 403 after a burst of
    ~2900 requests in ~30s from one client (observed empirically) -- this
    caps the aggregate request rate regardless of how many threads are fetching.
    """

    def __init__(self, rate_per_sec: float):
        self.set_rate(rate_per_sec)
        self.lock = threading.Lock()
        self.next_time = time.monotonic()

    def set_rate(self, rate_per_sec: float):
        self.min_interval = 1.0 / max(rate_per_sec, 0.1)

    def wait(self):
        with self.lock:
            now = time.monotonic()
            start = max(self.next_time, now)
            self.next_time = start + self.min_interval
        delay = start - now
        if delay > 0:
            time.sleep(delay)

    def try_acquire(self) -> bool:
        """Non-blocking: grant a slot only if one is due right now, else False.
        Used by the hybrid path to keep the direct IP busy up to its safe rate
        and overflow the rest to proxies."""
        with self.lock:
            now = time.monotonic()
            if now >= self.next_time:
                self.next_time = now + self.min_interval
                return True
            return False


rate_limiter = RateLimiter(rate_per_sec=15)


class ProxyPool:
    """Round-robins requests across many proxy addresses, thread-safe, with
    health-based eviction.

    data.commoncrawl.org's CloudFront throttling is per source IP, so spreading
    requests across a pool of proxy exit points lifts the per-IP ceiling. Free
    public proxies die constantly, so a proxy that fails `fail_threshold` times
    in a row is dropped from rotation; when the live pool empties, next() returns
    None and the caller falls back to a direct (un-proxied) request.
    """

    def __init__(self, proxy_urls, fail_threshold: int = 3):
        if not proxy_urls:
            raise ValueError("proxy pool needs at least one proxy URL")
        self.live = list(dict.fromkeys(proxy_urls))
        self.fail_threshold = fail_threshold
        self.fails = {}
        self.lock = threading.Lock()
        self.idx = 0

    def next(self):
        with self.lock:
            if not self.live:
                return None
            url = self.live[self.idx % len(self.live)]
            self.idx += 1
            return url

    def report(self, url, ok: bool):
        if url is None:
            return
        with self.lock:
            if ok:
                self.fails.pop(url, None)
            else:
                n = self.fails.get(url, 0) + 1
                self.fails[url] = n
                if n >= self.fail_threshold and url in self.live:
                    self.live.remove(url)

    def size(self) -> int:
        with self.lock:
            return len(self.live)

    def add(self, proxy_urls):
        with self.lock:
            for u in proxy_urls:
                if u not in self.live:
                    self.live.append(u)
                    self.fails.pop(u, None)


_proxy_pool = None
_gateway = None


def set_proxy(proxy_url: str):
    """Route ALL requests through a single rotating-gateway endpoint.

    A rotating gateway exits from a fresh IP on every request, so CloudFront's
    per-IP throttle never accumulates -- unlike our own single IP. So in this
    mode every request goes through the gateway (no direct fallback, no per-proxy
    eviction); rate_limiter just paces total throughput."""
    global _gateway
    _gateway = proxy_url


def set_proxy_pool(proxy_urls):
    """Round-robin requests across a list of proxy URLs."""
    global _proxy_pool
    _proxy_pool = ProxyPool(proxy_urls)


def start_proxy_refresher(path: str, interval: float = 120.0):
    """Background daemon that periodically re-reads the proxy file and folds any
    new entries into the live pool -- so an external harvester can keep the pool
    topped up (free proxies die constantly) without restarting the fetch."""
    def loop():
        while True:
            time.sleep(interval)
            try:
                urls = []
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        parts = line.split(":")
                        if len(parts) == 4:
                            h, p, u, pw = parts
                            urls.append(f"http://{u}:{pw}@{h}:{p}")
                        elif len(parts) == 2:
                            urls.append(f"http://{parts[0]}:{parts[1]}")
                if _proxy_pool is not None and urls:
                    before = _proxy_pool.size()
                    _proxy_pool.add(urls)
                    after = _proxy_pool.size()
                    if after != before:
                        print(f"[proxy-refresh] pool {before} -> {after} live", flush=True)
            except Exception as e:
                print(f"[proxy-refresh] {e}", flush=True)

    t = threading.Thread(target=loop, daemon=True)
    t.start()


def load_proxy_file(path: str) -> int:
    """Load a pool from lines of `host:port:user:pass` (or `host:port`). Returns pool size."""
    urls = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) == 4:
                host, port, user, pw = parts
                urls.append(f"http://{user}:{pw}@{host}:{port}")
            elif len(parts) == 2:
                host, port = parts
                urls.append(f"http://{host}:{port}")
    set_proxy_pool(urls)
    return len(urls)


def get_session(pool_size: int = 32) -> requests.Session:
    """One pooled, retrying session per thread -- reused across many range requests."""
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        retry = Retry(
            total=3, backoff_factor=0.5, connect=2, read=2,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _thread_local.session = session
    return session


_S3_BUCKET = "commoncrawl"
_s3_client = None


def enable_s3(pool_size: int = 64):
    """Fetch WARC records straight from the CommonCrawl S3 bucket instead of the
    CloudFront mirror (data.commoncrawl.org). S3 has no per-IP throttle, so this
    is the high-throughput path -- but the bucket denies anonymous access from
    outside AWS, so it only works when running inside AWS (e.g. an EC2 instance
    whose IAM role can read S3). Requests are signed with the instance's default
    credential chain; no CloudFront, no proxy, no rate limiter needed.
    """
    global _s3_client
    import boto3
    from botocore.config import Config
    _s3_client = boto3.client(
        "s3", region_name="us-east-1",
        config=Config(max_pool_connections=pool_size,
                      retries={"max_attempts": 5, "mode": "adaptive"}),
    )


def fetch_warc_record(filename: str, offset: int, length: int, timeout: int = 20) -> bytes:
    """Download the raw bytes of one WARC record using a byte-range request."""
    end = int(offset) + int(length) - 1
    if _s3_client is not None:
        # S3 GetObject with a Range: no rate limiter (S3 handles high concurrency),
        # signed by the instance role.
        resp = _s3_client.get_object(Bucket=_S3_BUCKET, Key=filename,
                                     Range=f"bytes={offset}-{end}")
        return resp["Body"].read()
    headers = {"Range": f"bytes={offset}-{end}"}
    url = DATA_BASE_URL + filename
    if _gateway is not None:
        # Rotating gateway: all traffic through it (fresh exit IP per request),
        # rate_limiter paces total. No direct fallback, no eviction.
        rate_limiter.wait()
        resp = get_session().get(url, headers=headers, timeout=timeout,
                                 proxies={"http": _gateway, "https": _gateway})
        resp.raise_for_status()
        return resp.content
    proxies = None
    p = None
    if _proxy_pool is None:
        rate_limiter.wait()  # pure-direct: the limiter caps our own IP's rate
    else:
        # Hybrid: run our own IP flat-out up to its safe rate (rate_limiter), and
        # overflow every other request onto the proxy pool. If no proxy is free,
        # block for a direct slot so our IP never exceeds the safe rate.
        if rate_limiter.try_acquire():
            proxies = None  # this request goes direct
        else:
            p = _proxy_pool.next()
            if p:
                proxies = {"http": p, "https": p}
            else:
                rate_limiter.wait()
    try:
        resp = get_session().get(url, headers=headers, timeout=timeout, proxies=proxies)
        resp.raise_for_status()
    except Exception:
        if _proxy_pool is not None:
            _proxy_pool.report(p, False)  # a dead free proxy gets evicted after a few of these
        raise
    if _proxy_pool is not None:
        _proxy_pool.report(p, True)
    return resp.content


def parse_html_record(raw_bytes: bytes):
    """Return the decoded HTML body of the first HTML response record in a WARC chunk, or None."""
    stream = BytesIO(raw_bytes)
    for record in ArchiveIterator(stream):
        if record.rec_type != "response":
            continue
        content_type = record.http_headers.get_header("Content-Type", "") if record.http_headers else ""
        if "html" not in content_type:
            continue
        try:
            body = record.content_stream().read()
        except Exception:
            continue
        try:
            return body.decode("utf-8", errors="replace")
        except Exception:
            return None
    return None


def extract_links_from_html(html: str, page_url: str, soup=None):
    """Return a list of (target_url, anchor_text) tuples found in the HTML."""
    if soup is None:
        soup = make_soup(html)
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        try:
            absolute = urljoin(page_url, href)
        except ValueError:
            # Malformed hrefs (bad IPv6 literals, etc.) are common in the wild
            # across millions of real pages -- skip just this one link.
            continue
        if not absolute.startswith(("http://", "https://")):
            continue
        links.append((absolute, a.get_text(strip=True)[:200]))
    return links


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except ValueError:
        return ""
