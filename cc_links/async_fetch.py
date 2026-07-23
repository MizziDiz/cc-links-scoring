"""Bounded aiohttp Range fetches feeding CPU work to a process pool."""

import asyncio
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Iterable, Optional, Set, Tuple

import aiohttp

from cc_links.fetch import DATA_BASE_URL
from cc_links.processing import (
    Candidate,
    PageResult,
    classify_in_cpu_worker,
    initialize_cpu_worker,
)

RETRYABLE_STATUSES = {403, 408, 425, 429, 500, 502, 503, 504}
NETWORK_ERRORS = (
    asyncio.TimeoutError,
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
    aiohttp.ServerDisconnectedError,
)

ResultCallback = Callable[
    [PageResult],
    Awaitable[Optional[Tuple[float, float]]],
]


class RangeFetchError(Exception):
    """A WARC range could not be fetched safely."""


class RetryableRangeFetchError(RangeFetchError):
    """A transient HTTP response should be retried."""


ASYNC_RETRY_ERRORS = (*NETWORK_ERRORS, RetryableRangeFetchError)


class AsyncRateLimiter:
    """Coroutine-safe aggregate request pacing."""

    def __init__(self, rate_per_sec: float) -> None:
        self._lock = asyncio.Lock()
        self._next_time = time.monotonic()
        self.set_rate(rate_per_sec)

    def set_rate(self, rate_per_sec: float) -> None:
        self._min_interval = 1.0 / max(rate_per_sec, 0.1)

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            start = max(self._next_time, now)
            self._next_time = start + self._min_interval
        delay = start - now
        if delay > 0:
            await asyncio.sleep(delay)


@dataclass(frozen=True)
class AsyncFetchSettings:
    """Network controls for the async CloudFront/gateway path."""

    concurrency: int
    rate_limit: float
    timeout: float = 20.0
    connect_timeout: float = 10.0
    retries: int = 3
    retry_backoff: float = 0.5
    chunk_size: int = 64 * 1024


class AsyncWarcFetcher:
    """Stream exact byte ranges with bounded concurrency and retries."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        settings: AsyncFetchSettings,
        proxy: Optional[str] = None,
    ) -> None:
        self._session = session
        self._settings = settings
        self._proxy = proxy
        self._semaphore = asyncio.Semaphore(max(settings.concurrency, 1))
        self.rate_limiter = AsyncRateLimiter(settings.rate_limit)

    async def fetch(self, filename: str, offset: int, length: int) -> bytes:
        """Fetch one exact Range; never request or retain the whole WARC file."""
        offset = int(offset)
        length = int(length)
        if offset < 0 or length < 1:
            raise RangeFetchError("offset must be non-negative and length must be positive")

        end = offset + length - 1
        url = DATA_BASE_URL + filename
        headers = {"Range": f"bytes={offset}-{end}"}
        attempts = max(self._settings.retries, 0) + 1
        last_error: Optional[BaseException] = None

        for attempt in range(attempts):
            try:
                await self.rate_limiter.wait()
                async with self._semaphore:
                    return await self._fetch_once(url, headers, offset, end, length)
            except ASYNC_RETRY_ERRORS as exc:
                last_error = exc
                if attempt + 1 >= attempts:
                    break
                await asyncio.sleep(self._settings.retry_backoff * (2**attempt))

        if last_error is not None:
            detail = f"{type(last_error).__name__}: {last_error}"
            raise RangeFetchError(
                f"range fetch failed after {attempts} attempts: {detail}"
            ) from last_error
        raise RangeFetchError("range fetch failed without an exception")

    async def _fetch_once(
        self,
        url: str,
        headers: Dict[str, str],
        offset: int,
        end: int,
        expected_length: int,
    ) -> bytes:
        async with self._session.get(
            url,
            headers=headers,
            proxy=self._proxy,
        ) as response:
            if response.status in RETRYABLE_STATUSES:
                raise RetryableRangeFetchError(f"transient HTTP {response.status}")
            if response.status != 206:
                raise RangeFetchError(f"expected HTTP 206 for Range request, got {response.status}")

            content_range = response.headers.get("Content-Range", "")
            expected_prefix = f"bytes {offset}-{end}/"
            if content_range and not content_range.startswith(expected_prefix):
                raise RangeFetchError("server returned a different Content-Range")

            payload = bytearray()
            async for chunk in response.content.iter_chunked(self._settings.chunk_size):
                payload.extend(chunk)
                if len(payload) > expected_length:
                    raise RangeFetchError("Range response exceeded requested length")
            if len(payload) != expected_length:
                raise RetryableRangeFetchError(
                    f"incomplete Range response: expected {expected_length}, got {len(payload)}"
                )
            return bytes(payload)


async def run_async_fetch(
    records: Iterable[Candidate],
    excluded: Set[str],
    extract_links: bool,
    settings: AsyncFetchSettings,
    cpu_workers: int,
    on_result: ResultCallback,
    proxy: Optional[str] = None,
) -> None:
    """Run bounded network workers, process workers, and one result collector."""
    concurrency = max(settings.concurrency, 1)
    process_count = max(cpu_workers, 1)
    record_queue: asyncio.Queue[Optional[Candidate]] = asyncio.Queue(maxsize=concurrency * 2)
    cpu_queue: asyncio.Queue[Optional[Tuple[Candidate, bytes]]] = asyncio.Queue(
        maxsize=process_count * 2
    )
    result_queue: asyncio.Queue[Optional[PageResult]] = asyncio.Queue(
        maxsize=max(concurrency, process_count) * 2
    )

    timeout = aiohttp.ClientTimeout(
        total=settings.timeout,
        connect=settings.connect_timeout,
        sock_read=settings.timeout,
    )
    connector = aiohttp.TCPConnector(limit=concurrency, ttl_dns_cache=300)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        auto_decompress=False,
    ) as session:
        fetcher = AsyncWarcFetcher(session, settings, proxy=proxy)
        loop = asyncio.get_running_loop()

        with ProcessPoolExecutor(
            max_workers=process_count,
            initializer=initialize_cpu_worker,
            initargs=(excluded, extract_links),
        ) as process_pool:

            async def produce() -> None:
                for record in records:
                    await record_queue.put(record)
                for _ in range(concurrency):
                    await record_queue.put(None)

            async def fetch_worker() -> None:
                while True:
                    record = await record_queue.get()
                    if record is None:
                        return
                    try:
                        raw_bytes = await fetcher.fetch(
                            record["filename"],
                            record["offset"],
                            record["length"],
                        )
                    except RangeFetchError as exc:
                        await result_queue.put(
                            {
                                "url": record["url"],
                                "ok": False,
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )
                    else:
                        await cpu_queue.put((record, raw_bytes))

            async def cpu_worker() -> None:
                while True:
                    item = await cpu_queue.get()
                    if item is None:
                        return
                    record, raw_bytes = item
                    result = await loop.run_in_executor(
                        process_pool,
                        classify_in_cpu_worker,
                        record,
                        raw_bytes,
                    )
                    await result_queue.put(result)

            async def collect() -> None:
                while True:
                    result = await result_queue.get()
                    if result is None:
                        return
                    throttle = await on_result(result)
                    if throttle is not None:
                        pause_seconds, new_rate = throttle
                        fetcher.rate_limiter.set_rate(new_rate)
                        await asyncio.sleep(pause_seconds)

            producer_task = asyncio.create_task(produce())
            fetch_tasks = [asyncio.create_task(fetch_worker()) for _ in range(concurrency)]
            cpu_tasks = [asyncio.create_task(cpu_worker()) for _ in range(process_count)]
            collector_task = asyncio.create_task(collect())

            async def finish_fetch_stage() -> None:
                await asyncio.gather(*fetch_tasks)
                for _ in range(process_count):
                    await cpu_queue.put(None)

            async def finish_cpu_stage() -> None:
                await asyncio.gather(*cpu_tasks)
                await result_queue.put(None)

            fetch_stage_task = asyncio.create_task(finish_fetch_stage())
            cpu_stage_task = asyncio.create_task(finish_cpu_stage())
            all_tasks = [
                producer_task,
                *fetch_tasks,
                fetch_stage_task,
                *cpu_tasks,
                cpu_stage_task,
                collector_task,
            ]
            try:
                await asyncio.gather(
                    producer_task,
                    fetch_stage_task,
                    cpu_stage_task,
                    collector_task,
                )
            finally:
                for task in all_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*all_tasks, return_exceptions=True)
