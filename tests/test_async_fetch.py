import asyncio
import unittest
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Any, Dict, List, Optional
from unittest.mock import patch

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

import pipeline
from cc_links.async_fetch import (
    AsyncFetchSettings,
    AsyncWarcFetcher,
    RangeFetchError,
    run_async_fetch,
)
from cc_links.processing import PageResult, classify_warc_bytes


def make_warc(html: str) -> bytes:
    output = BytesIO()
    writer = WARCWriter(output, gzip=True)
    headers = StatusAndHeaders(
        "200 OK",
        [("Content-Type", "text/html; charset=utf-8")],
        protocol="HTTP/1.1",
    )
    record = writer.create_warc_record(
        "https://fixture.invalid/page",
        "response",
        payload=BytesIO(html.encode()),
        http_headers=headers,
    )
    writer.write_record(record)
    record.raw_stream.close()
    return output.getvalue()


class FakeContent:
    def __init__(self, chunks: List[bytes], tracker: Dict[str, int]) -> None:
        self._chunks = chunks
        self._tracker = tracker

    async def iter_chunked(self, _size: int) -> Any:
        for chunk in self._chunks:
            await asyncio.sleep(0)
            yield chunk


class FakeResponse:
    def __init__(
        self,
        status: int,
        chunks: List[bytes],
        content_range: str = "",
        tracker: Optional[Dict[str, int]] = None,
    ) -> None:
        self.status = status
        self.headers = {"Content-Range": content_range} if content_range else {}
        self._tracker = tracker or {"active": 0, "maximum": 0}
        self.content = FakeContent(chunks, self._tracker)

    async def __aenter__(self) -> "FakeResponse":
        self._tracker["active"] += 1
        self._tracker["maximum"] = max(
            self._tracker["maximum"],
            self._tracker["active"],
        )
        return self

    async def __aexit__(self, *_args: object) -> None:
        self._tracker["active"] -= 1


class FakeSession:
    def __init__(self, responses: List[Any]) -> None:
        self._responses = iter(responses)
        self.calls: List[Dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        response = next(self._responses)
        if isinstance(response, BaseException):
            raise response
        return response


class AsyncWarcFetcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_streams_an_exact_range(self) -> None:
        session = FakeSession([FakeResponse(206, [b"ab", b"cd"], "bytes 10-13/100")])
        fetcher = AsyncWarcFetcher(
            session,  # type: ignore[arg-type]
            AsyncFetchSettings(concurrency=2, rate_limit=1_000_000),
        )

        payload = await fetcher.fetch("fixture.warc.gz", 10, 4)

        self.assertEqual(payload, b"abcd")
        self.assertEqual(session.calls[0]["headers"], {"Range": "bytes=10-13"})

    async def test_fetch_retries_transient_status(self) -> None:
        session = FakeSession(
            [
                FakeResponse(503, []),
                FakeResponse(206, [b"ok"], "bytes 0-1/2"),
            ]
        )
        fetcher = AsyncWarcFetcher(
            session,  # type: ignore[arg-type]
            AsyncFetchSettings(
                concurrency=1,
                rate_limit=1_000_000,
                retries=1,
                retry_backoff=0,
            ),
        )

        self.assertEqual(await fetcher.fetch("fixture.warc.gz", 0, 2), b"ok")
        self.assertEqual(len(session.calls), 2)

    async def test_fetch_retries_a_timeout(self) -> None:
        session = FakeSession(
            [
                asyncio.TimeoutError(),
                FakeResponse(206, [b"ok"], "bytes 0-1/2"),
            ]
        )
        fetcher = AsyncWarcFetcher(
            session,  # type: ignore[arg-type]
            AsyncFetchSettings(
                concurrency=1,
                rate_limit=1_000_000,
                retries=1,
                retry_backoff=0,
            ),
        )

        self.assertEqual(await fetcher.fetch("fixture.warc.gz", 0, 2), b"ok")
        self.assertEqual(len(session.calls), 2)

    async def test_fetch_rejects_a_full_file_response(self) -> None:
        session = FakeSession([FakeResponse(200, [b"whole file"])])
        fetcher = AsyncWarcFetcher(
            session,  # type: ignore[arg-type]
            AsyncFetchSettings(concurrency=1, rate_limit=1_000_000),
        )

        with self.assertRaises(RangeFetchError):
            await fetcher.fetch("fixture.warc.gz", 0, 4)

    async def test_semaphore_bounds_in_flight_responses(self) -> None:
        tracker = {"active": 0, "maximum": 0}
        session = FakeSession(
            [FakeResponse(206, [b"x"], f"bytes {i}-{i}/10", tracker) for i in range(4)]
        )
        fetcher = AsyncWarcFetcher(
            session,  # type: ignore[arg-type]
            AsyncFetchSettings(concurrency=2, rate_limit=1_000_000),
        )

        await asyncio.gather(*(fetcher.fetch("fixture.warc.gz", i, 1) for i in range(4)))

        self.assertLessEqual(tracker["maximum"], 2)


class ProcessingTests(unittest.TestCase):
    def test_cpu_stage_decompresses_and_classifies_warc(self) -> None:
        raw = make_warc('<html><head><meta name="generator" content="WordPress 6"></head></html>')
        record = {
            "url": "https://fixture.invalid/page",
            "filename": "fixture.warc.gz",
            "offset": 0,
            "length": len(raw),
            "url_host_tld": "invalid",
            "bucket": "fixture",
        }

        result = classify_warc_bytes(record, raw, set(), extract_links=False)

        self.assertTrue(result["ok"])
        self.assertEqual(result["engine_name"], "WordPress")
        self.assertEqual(result["links"], [])

    def test_threads_path_still_uses_the_shared_cpu_stage(self) -> None:
        raw = make_warc("<html><head></head><body>fixture</body></html>")
        record = {
            "url": "https://fixture.invalid/page",
            "filename": "fixture.warc.gz",
            "offset": 0,
            "length": len(raw),
        }

        with patch.object(pipeline, "fetch_warc_record", return_value=raw):
            result = pipeline._fetch_and_classify(
                record,
                set(),
                extract_links=False,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["links"], [])

    def test_async_mode_rejects_the_s3_fetch_implementation(self) -> None:
        with self.assertRaisesRegex(ValueError, "not signed S3"):
            pipeline.run_countries(
                countries=["test"],
                crawl="crawl",
                total_limit=1,
                per_country_limit=None,
                priorities_file=None,
                db_path="unused.db",
                workers=1,
                max_parts=None,
                exclude_file=None,
                candidates_file=None,
                commit_every=1,
                skip_discovery=True,
                rate_limit=1,
                max_per_domain=None,
                proxy=None,
                proxy_file=None,
                store_links=False,
                categories_file=None,
                per_category_limit=None,
                discovery_only=False,
                discover_delay=0,
                source="s3",
                fetch_mode="async",
            )


class AsyncPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_runner_passes_cpu_work_to_an_executor(self) -> None:
        raw = make_warc("<html><head></head><body>fixture</body></html>")
        records = [
            {
                "url": "https://fixture.invalid/page",
                "filename": "fixture.warc.gz",
                "offset": 0,
                "length": len(raw),
                "url_host_tld": "invalid",
                "bucket": "fixture",
            }
        ]
        results: List[PageResult] = []
        pool_arguments: List[Dict[str, Any]] = []

        async def fake_fetch(
            _self: AsyncWarcFetcher,
            _filename: str,
            _offset: int,
            _length: int,
        ) -> bytes:
            return raw

        async def collect(result: PageResult) -> None:
            results.append(result)
            return None

        def make_pool(**kwargs: Any) -> ThreadPoolExecutor:
            pool_arguments.append(kwargs)
            initializer = kwargs.pop("initializer")
            initargs = kwargs.pop("initargs")
            initializer(*initargs)
            return ThreadPoolExecutor(**kwargs)

        with (
            patch.object(AsyncWarcFetcher, "fetch", new=fake_fetch),
            patch("cc_links.async_fetch.ProcessPoolExecutor", side_effect=make_pool),
        ):
            await run_async_fetch(
                records,
                set(),
                False,
                AsyncFetchSettings(concurrency=2, rate_limit=1_000_000),
                cpu_workers=1,
                on_result=collect,
            )

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0]["ok"])
        self.assertEqual(pool_arguments[0]["max_workers"], 1)

    async def test_runner_cancels_all_stages_when_callback_fails(self) -> None:
        raw = make_warc("<html><head></head><body>fixture</body></html>")
        records = [
            {
                "url": f"https://fixture.invalid/page/{index}",
                "filename": "fixture.warc.gz",
                "offset": 0,
                "length": len(raw),
            }
            for index in range(20)
        ]

        async def fake_fetch(
            _self: AsyncWarcFetcher,
            _filename: str,
            _offset: int,
            _length: int,
        ) -> bytes:
            return raw

        async def fail_callback(_result: PageResult) -> None:
            raise RuntimeError("callback failure")

        def make_pool(**kwargs: Any) -> ThreadPoolExecutor:
            initializer = kwargs.pop("initializer")
            initargs = kwargs.pop("initargs")
            initializer(*initargs)
            return ThreadPoolExecutor(**kwargs)

        with (
            patch.object(AsyncWarcFetcher, "fetch", new=fake_fetch),
            patch("cc_links.async_fetch.ProcessPoolExecutor", side_effect=make_pool),
        ):
            with self.assertRaisesRegex(RuntimeError, "callback failure"):
                await asyncio.wait_for(
                    run_async_fetch(
                        records,
                        set(),
                        False,
                        AsyncFetchSettings(concurrency=2, rate_limit=1_000_000),
                        cpu_workers=1,
                        on_result=fail_callback,
                    ),
                    timeout=2,
                )


if __name__ == "__main__":
    unittest.main()
