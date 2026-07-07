"""Fetch a single WARC record via HTTP range request and extract outbound links.

This replaces the Athena approach: instead of scanning S3 with SQL, we use the
offset/length from the CDX index to pull only the relevant bytes over HTTPS,
then parse that one record with warcio + BeautifulSoup.
"""
import threading
import time
from io import BytesIO
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from warcio.archiveiterator import ArchiveIterator

DATA_BASE_URL = "https://data.commoncrawl.org/"

_thread_local = threading.local()


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


rate_limiter = RateLimiter(rate_per_sec=15)


class ProxyPool:
    """Round-robins requests across many proxy addresses, thread-safe.

    data.commoncrawl.org's CloudFront throttling is per source IP, so spreading
    requests across a pool of proxy exit points (rather than one static proxy,
    and rather than our own single IP) is what actually lifts the per-IP ceiling.
    """

    def __init__(self, proxy_urls):
        if not proxy_urls:
            raise ValueError("proxy pool needs at least one proxy URL")
        self.proxy_urls = list(proxy_urls)
        self.lock = threading.Lock()
        self.idx = 0

    def next(self) -> str:
        with self.lock:
            url = self.proxy_urls[self.idx % len(self.proxy_urls)]
            self.idx += 1
        return url


_proxy_pool = None


def set_proxy(proxy_url: str):
    """Route all requests through a single proxy URL (e.g. a rotating-gateway endpoint)."""
    global _proxy_pool
    _proxy_pool = ProxyPool([proxy_url])


def set_proxy_pool(proxy_urls):
    """Round-robin requests across a list of proxy URLs."""
    global _proxy_pool
    _proxy_pool = ProxyPool(proxy_urls)


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
            total=6, backoff_factor=2.0,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _thread_local.session = session
    return session


def fetch_warc_record(filename: str, offset: int, length: int, timeout: int = 30) -> bytes:
    """Download the raw bytes of one WARC record using a byte-range request."""
    rate_limiter.wait()
    end = int(offset) + int(length) - 1
    headers = {"Range": f"bytes={offset}-{end}"}
    url = DATA_BASE_URL + filename
    proxies = None
    if _proxy_pool is not None:
        p = _proxy_pool.next()
        proxies = {"http": p, "https": p}
    resp = get_session().get(url, headers=headers, timeout=timeout, proxies=proxies)
    resp.raise_for_status()
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
        soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        absolute = urljoin(page_url, href)
        if not absolute.startswith(("http://", "https://")):
            continue
        links.append((absolute, a.get_text(strip=True)[:200]))
    return links


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except ValueError:
        return ""
