import argparse
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cc_links.db import init_db
from multi_crawl import build_command, candidate_count, merge_candidate_files


class MultiCrawlTests(unittest.TestCase):
    def test_candidate_count_handles_new_and_initialized_databases(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.db"
            self.assertEqual(candidate_count(str(path)), 0)
            conn = init_db(str(path))
            conn.close()
            self.assertEqual(candidate_count(str(path)), 0)

    def test_command_uses_crawl_specific_candidate_file(self):
        args = argparse.Namespace(
            categories_file="categories.json", per_category_limit=5000,
            db="prospects.db", min_score=50, workers=64, max_parts=300,
            max_per_domain=10, source="s3", progress_interval=60,
            footprints=None, exclude_file=None, proxy=None, proxy_file=None)
        command = build_command(args, "CC-MAIN-2026-21", "/tmp/2026-21.jsonl")
        self.assertIn("CC-MAIN-2026-21", command)
        self.assertIn("/tmp/2026-21.jsonl", command)

    def test_merge_deduplicates_normalized_urls(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = Path(tmp) / "first.jsonl"
            second = Path(tmp) / "second.jsonl"
            output = Path(tmp) / "merged.jsonl"
            first.write_text(json.dumps({"url": "https://Example.com/x?utm_source=a"}) + "\n",
                             encoding="utf-8")
            second.write_text(json.dumps({"url": "https://example.com/x"}) + "\n",
                              encoding="utf-8")
            self.assertEqual(merge_candidate_files(
                [str(first), str(second)], str(output)), 1)
            self.assertEqual(len(output.read_text(encoding="utf-8").splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
