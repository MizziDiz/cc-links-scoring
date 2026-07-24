import argparse
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cc_links.db import init_db
from multi_crawl import (
    build_command,
    candidate_count,
    discovery_marker,
    discovery_state_complete,
    existing_shard_count,
    mark_discovery_complete,
    merge_candidate_files,
    read_discovery_marker,
    resolve_discovery_shards,
)


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
            max_per_domain=10, source="s3", index_source="auto", progress_interval=60,
            footprints=None, category_limits=None, discovery_profile="precise",
            broad_quota_fraction=0.25, broad_index_sample=0.02,
            exclude_file=None, proxy=None, proxy_file=None)
        command = build_command(args, "CC-MAIN-2026-21", "/tmp/2026-21.jsonl")
        self.assertIn("CC-MAIN-2026-21", command)
        self.assertIn("/tmp/2026-21.jsonl", command)
        self.assertEqual(command[command.index("--index-source") + 1], "auto")
        self.assertEqual(
            command[command.index("--broad-quota-fraction") + 1], "0.25")

    def test_category_limits_are_divided_across_discovery_shards(self):
        args = argparse.Namespace(
            categories_file="categories.json", per_category_limit=5000,
            db="prospects.db", min_score=50, workers=64, max_parts=300,
            max_per_domain=10, source="s3", index_source="auto", progress_interval=60,
            footprints=None, category_limits="category_limits.small.json",
            discovery_profile="broad", broad_quota_fraction=0.25,
            broad_index_sample=0.02,
            exclude_file=None, proxy=None, proxy_file=None)
        command = build_command(
            args, "CC-MAIN-2026-21", "/tmp/shard.jsonl",
            category_limit_divisor=4)
        self.assertEqual(
            command[command.index("--category-limit-divisor") + 1], "4")

    def test_optional_discovery_metrics_are_forwarded(self):
        args = argparse.Namespace(
            categories_file="categories.json", per_category_limit=5000,
            db="prospects.db", min_score=50, workers=64, max_parts=300,
            max_per_domain=10, source="s3", index_source="auto",
            progress_interval=60, footprints=None, category_limits=None,
            discovery_profile="precise", broad_quota_fraction=0.25,
            broad_index_sample=0.02, discovery_metrics=True,
            exclude_file=None, proxy=None, proxy_file=None)
        command = build_command(
            args, "CC-MAIN-2026-21", "/tmp/metrics.jsonl")
        self.assertIn("--discovery-metrics", command)

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

    def test_discovery_state_complete_supports_legacy_and_sharded_states(self):
        with tempfile.TemporaryDirectory() as tmp:
            candidates = Path(tmp) / "crawl.jsonl"
            state = Path(str(candidates) + ".state.json")
            state.write_text(json.dumps({
                "scanned_parts": [0, 1, 2],
                "remaining": {"A": 10},
            }), encoding="utf-8")
            self.assertTrue(discovery_state_complete(str(candidates), 3))
            self.assertFalse(discovery_state_complete(str(candidates), 4))

            state.write_text(json.dumps({
                "scanned_parts": [0, 4],
                "allowed_parts_count": 2,
                "remaining": {"A": 10},
            }), encoding="utf-8")
            self.assertTrue(discovery_state_complete(str(candidates), 300))

    def test_zero_remaining_marks_early_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            candidates = Path(tmp) / "crawl.jsonl"
            Path(str(candidates) + ".state.json").write_text(json.dumps({
                "scanned_parts": [0],
                "remaining": {"A": 0, "B": 0},
            }), encoding="utf-8")
            self.assertTrue(discovery_state_complete(str(candidates), 300))

    def test_completion_marker_is_atomic_and_discoverable(self):
        with tempfile.TemporaryDirectory() as tmp:
            candidates = str(Path(tmp) / "crawl.jsonl")
            identity = {
                "crawl": "CC-TEST",
                "taxonomy_hash": "abc",
                "discovery_profile": "precise",
            }
            mark_discovery_complete(candidates, identity)
            self.assertTrue(Path(discovery_marker(candidates)).is_file())
            self.assertEqual(read_discovery_marker(candidates), identity)

    def test_auto_shards_follow_available_cpu_with_ceiling(self):
        self.assertEqual(resolve_discovery_shards(0, cpu_count=1), 1)
        self.assertEqual(resolve_discovery_shards(0, cpu_count=3), 3)
        self.assertEqual(resolve_discovery_shards(0, cpu_count=16), 4)
        self.assertEqual(resolve_discovery_shards(6, cpu_count=1), 6)

    def test_existing_shard_layout_is_detected_for_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            for index in (0, 2, 3):
                Path(tmp, f"crawl.shard-{index}-of-4.jsonl.state.json").touch()
            self.assertEqual(existing_shard_count(tmp, "crawl"), 4)
            self.assertIsNone(existing_shard_count(tmp, "other"))


if __name__ == "__main__":
    unittest.main()
