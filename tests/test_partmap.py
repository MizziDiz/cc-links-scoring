import json
import tempfile
import unittest
from pathlib import Path

from cc_links.partmap import (
    PartMapError,
    PartRange,
    build_part_map,
    load_part_map,
    part_map_digest,
    prefix_successor,
    range_overlaps_prefix,
    save_part_map,
    select_parts,
    surt_prefix_for_tld,
)


class FakeResult:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, rows, failures=None):
        self.rows = iter(rows)
        self.failures = failures if failures is not None else []
        self.closed = False

    def execute(self, query):
        if self.failures:
            failure = self.failures.pop(0)
            if failure:
                raise RuntimeError("transient")
        return FakeResult(next(self.rows))

    def close(self):
        self.closed = True


class PartMapTests(unittest.TestCase):
    def test_prefix_successor(self):
        self.assertEqual(prefix_successor("co,"), "co-")
        self.assertEqual(surt_prefix_for_tld(".MX"), "mx,")
        with self.assertRaises(ValueError):
            surt_prefix_for_tld("co')")

    def test_overlap_is_half_open_and_conservative(self):
        co = PartRange("a", "https://example/a", "co,a", "co,z", 2)
        before = PartRange("b", "https://example/b", "cn,a", "cn,z", 2)
        after = PartRange("c", "https://example/c", "co-", "de,a", 2)
        unknown = PartRange("d", "https://example/d", None, None, 0)
        invalid = PartRange("e", "https://example/e", "z", "a", 0)
        self.assertTrue(range_overlaps_prefix(co, "co,"))
        self.assertFalse(range_overlaps_prefix(before, "co,"))
        self.assertFalse(range_overlaps_prefix(after, "co,"))
        self.assertTrue(range_overlaps_prefix(unknown, "co,"))
        self.assertTrue(range_overlaps_prefix(invalid, "co,"))

    def test_select_parts_for_multiple_tlds(self):
        payload = {
            "parts": [
                {
                    "part": "a",
                    "part_url": "https://example/a",
                    "min_url_surtkey": "cl,a",
                    "max_url_surtkey": "cl,z",
                    "row_count": 2,
                },
                {
                    "part": "b",
                    "part_url": "https://example/b",
                    "min_url_surtkey": "mx,a",
                    "max_url_surtkey": "mx,z",
                    "row_count": 2,
                },
                {
                    "part": "c",
                    "part_url": "https://example/c",
                    "min_url_surtkey": "za,a",
                    "max_url_surtkey": "za,z",
                    "row_count": 2,
                },
            ]
        }
        self.assertEqual(
            [row.part for row in select_parts(payload, ["cl", "mx"])], ["a", "b"]
        )

    def test_digest_ignores_generation_time(self):
        first = {"schema_version": 1, "crawl": "CC-TEST", "generated_at": "a"}
        second = {"schema_version": 1, "crawl": "CC-TEST", "generated_at": "b"}
        self.assertEqual(part_map_digest(first), part_map_digest(second))

    def test_save_and_load_validates_digest(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "map.json"
            save_part_map(
                path,
                {
                    "schema_version": 1,
                    "crawl": "CC-TEST",
                    "parts": [
                        {
                            "part": "a",
                            "part_url": "https://example/a",
                            "min_url_surtkey": "a",
                            "max_url_surtkey": "z",
                            "row_count": 3,
                        }
                    ],
                },
            )
            self.assertEqual(load_part_map(path)["crawl"], "CC-TEST")
            data = json.loads(path.read_text(encoding="utf-8"))
            data["crawl"] = "CC-TAMPERED"
            path.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaises(PartMapError):
                load_part_map(path)

    def test_builder_writes_complete_map_and_removes_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "map.json"
            connection = FakeConnection([("cl,a", "cl,z", 10), ("mx,a", "mx,z", 20)])
            payload = build_part_map(
                "CC-TEST",
                path,
                part_urls=["https://example/a.parquet", "https://example/b.parquet"],
                connection_factory=lambda: connection,
            )
            self.assertEqual(len(payload["parts"]), 2)
            self.assertEqual(payload["parts"][1]["row_count"], 20)
            self.assertFalse(Path(str(path) + ".state.json").exists())
            self.assertTrue(connection.closed)

    def test_builder_recycles_connections_and_records_index_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "map.json"
            connections = [
                FakeConnection([("cl,a", "cl,z", 10)]),
                FakeConnection([("mx,a", "mx,z", 20)]),
            ]
            payload = build_part_map(
                "CC-TEST",
                path,
                part_urls=["s3://example/a.parquet", "s3://example/b.parquet"],
                index_source="s3",
                reconnect_every=1,
                connection_factory=lambda: connections.pop(0),
            )
            self.assertEqual(payload["index_source"], "s3")
            self.assertEqual(payload["parts"][1]["row_count"], 20)
            self.assertEqual(connections, [])

    def test_builder_validates_source_and_reconnect_interval(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "map.json"
            with self.assertRaises(ValueError):
                build_part_map(
                    "CC-TEST",
                    path,
                    part_urls=["https://example/a.parquet"],
                    index_source="ftp",
                )
            with self.assertRaises(ValueError):
                build_part_map(
                    "CC-TEST",
                    path,
                    part_urls=["https://example/a.parquet"],
                    reconnect_every=0,
                )

    def test_failed_part_is_not_checkpointed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "map.json"
            connections = [
                FakeConnection([], failures=[True]),
                FakeConnection([], failures=[True]),
            ]
            with self.assertRaises(PartMapError):
                build_part_map(
                    "CC-TEST",
                    path,
                    part_urls=["https://example/a.parquet"],
                    max_retries=2,
                    connection_factory=lambda: connections.pop(0),
                    retry_exceptions=(RuntimeError,),
                )
            self.assertFalse(path.exists())
            self.assertFalse(Path(str(path) + ".state.json").exists())

    def test_successful_part_remains_resumable_when_next_part_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "map.json"
            first = FakeConnection([("cl,a", "cl,z", 10)], failures=[False, True])
            second = FakeConnection([], failures=[True])
            connections = [first, second]
            with self.assertRaises(PartMapError):
                build_part_map(
                    "CC-TEST",
                    path,
                    part_urls=[
                        "https://example/a.parquet",
                        "https://example/b.parquet",
                    ],
                    max_retries=2,
                    connection_factory=lambda: connections.pop(0),
                    retry_exceptions=(RuntimeError,),
                )
            state_path = Path(str(path) + ".state.json")
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                [row["part_url"] for row in state["parts"]],
                ["https://example/a.parquet"],
            )


if __name__ == "__main__":
    unittest.main()
