import json
import tempfile
import unittest
from pathlib import Path

from cc_links.outreach_discovery import (
    OutreachDiscoveryError,
    OutreachDiscoveryIncomplete,
    discover_outreach,
)


class FakeCursor:
    def __init__(self, rows):
        self.rows = list(rows)

    def fetchmany(self, size):
        batch = self.rows[:size]
        self.rows = self.rows[size:]
        return batch


class FailingCursor(FakeCursor):
    def __init__(self, rows):
        super().__init__(rows)
        self.calls = 0

    def fetchmany(self, size):
        self.calls += 1
        if self.calls == 2:
            raise RuntimeError("mid-stream")
        return super().fetchmany(1)


class FakeConnection:
    def __init__(self, responses):
        self.responses = list(responses)
        self.queries = []
        self.closed = False

    def execute(self, query):
        self.queries.append(query)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response if isinstance(response, FakeCursor) else FakeCursor(response)

    def close(self):
        self.closed = True


def row(url, path, domain="example.co", tld="co", languages="eng"):
    return (
        url,
        path,
        domain,
        tld,
        languages,
        "2026-01-01T00:00:00Z",
        "crawl-data/test.warc.gz",
        10,
        20,
    )


class OutreachDiscoveryTests(unittest.TestCase):
    def test_discovers_path_matches_and_enforces_domain_cap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake = FakeConnection(
                [
                    [
                        row("https://example.co/write-for-us", "/write-for-us"),
                        row("https://example.co/submit", "/submit-an-article"),
                        row("https://example.co/third", "/guest-post-guidelines"),
                        row(
                            "https://otro.co/escribe",
                            "/escribe-para-nosotros",
                            "otro.co",
                            languages="spa",
                        ),
                    ]
                ]
            )
            output = root / "out.jsonl"
            summary = discover_outreach(
                crawl="CC-TEST",
                tlds=["co"],
                out_path=output,
                db_path=root / "outreach.db",
                part_urls=["https://example/part-a.parquet"],
                max_per_domain=2,
                connection_factory=lambda: fake,
                retry_exceptions=(RuntimeError,),
                retry_backoff=0,
            )
            records = [
                json.loads(line)
                for line in output.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(summary.output_rows, 3)
            self.assertEqual(summary.domains, 2)
            self.assertEqual(
                sum(row["registered_domain"] == "example.co" for row in records), 2
            )
            self.assertNotIn(
                "https://example.co/submit", {record["url"] for record in records}
            )
            spanish = next(
                row for row in records if row["registered_domain"] == "otro.co"
            )
            self.assertEqual(spanish["pattern_language"], "es")
            self.assertIn("LOWER(COALESCE(url_path", fake.queries[0])
            self.assertIn("CAST(fetch_time AS VARCHAR)", fake.queries[0])

    def test_completed_spool_is_recovered_without_parquet_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root / "out.jsonl"
            part_url = "https://example/part-a.parquet"
            failed = FakeConnection(
                [[row("https://example.co/write-for-us", "/write-for-us")]]
            )
            summary = discover_outreach(
                crawl="CC-TEST",
                tlds=["co"],
                out_path=output,
                db_path=root / "outreach.db",
                part_urls=[part_url],
                connection_factory=lambda: failed,
                retry_exceptions=(RuntimeError,),
                retry_backoff=0,
            )
            self.assertEqual(summary.completed_parts, 1)

            # Remove the completed marker while retaining the atomic spool.
            state_path = Path(str(output) + ".state.json")
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["completed_parts"] = []
            state_path.write_text(json.dumps(state), encoding="utf-8")

            def should_not_connect():
                self.fail("Parquet should not be read")

            resumed = discover_outreach(
                crawl="CC-TEST",
                tlds=["co"],
                out_path=output,
                db_path=root / "outreach.db",
                part_urls=[part_url],
                connection_factory=should_not_connect,
            )
            self.assertEqual(resumed.completed_parts, 1)
            self.assertEqual(resumed.inserted_rows, 1)

    def test_failed_part_stays_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            connections = [
                FakeConnection([RuntimeError("one")]),
                FakeConnection([RuntimeError("two")]),
            ]
            with self.assertRaises(OutreachDiscoveryIncomplete):
                discover_outreach(
                    crawl="CC-TEST",
                    tlds=["co"],
                    out_path=root / "out.jsonl",
                    db_path=root / "outreach.db",
                    part_urls=["https://example/part-a.parquet"],
                    max_retries=2,
                    connection_factory=lambda: connections.pop(0),
                    retry_exceptions=(RuntimeError,),
                    retry_backoff=0,
                )
            state = json.loads(
                (root / "out.jsonl.state.json").read_text(encoding="utf-8")
            )
            self.assertEqual(state["completed_parts"], [])
            self.assertEqual(len(state["failed_parts"]), 1)

    def test_retry_discards_partial_seen_markers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = FakeConnection(
                [
                    FailingCursor(
                        [
                            row(
                                "https://example.co/write-for-us",
                                "/write-for-us",
                            ),
                            row(
                                "https://other.co/write-for-us",
                                "/write-for-us",
                                domain="other.co",
                            ),
                        ]
                    )
                ]
            )
            second = FakeConnection(
                [
                    [
                        row(
                            "https://example.co/write-for-us",
                            "/write-for-us",
                        ),
                        row(
                            "https://other.co/write-for-us",
                            "/write-for-us",
                            domain="other.co",
                        ),
                    ]
                ]
            )
            connections = [first, second]
            output = root / "out.jsonl"
            summary = discover_outreach(
                crawl="CC-TEST",
                tlds=["co"],
                out_path=output,
                db_path=root / "outreach.db",
                part_urls=["https://example/part-a.parquet"],
                max_retries=2,
                max_per_domain=1,
                connection_factory=lambda: connections.pop(0),
                retry_exceptions=(RuntimeError,),
                retry_backoff=0,
            )
            self.assertEqual(summary.output_rows, 2)

    def test_identity_change_requires_new_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root / "out.jsonl"
            first = FakeConnection([[]])
            discover_outreach(
                crawl="CC-TEST",
                tlds=["co"],
                out_path=output,
                db_path=root / "outreach.db",
                part_urls=["https://example/part-a.parquet"],
                connection_factory=lambda: first,
            )
            with self.assertRaises(OutreachDiscoveryError):
                discover_outreach(
                    crawl="CC-TEST",
                    tlds=["mx"],
                    out_path=output,
                    db_path=root / "outreach.db",
                    part_urls=["https://example/part-a.parquet"],
                    connection_factory=lambda: FakeConnection([[]]),
                )


if __name__ == "__main__":
    unittest.main()
