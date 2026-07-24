import gzip
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

try:
    import duckdb  # noqa: F401
except ModuleNotFoundError:
    sys.modules["duckdb"] = Mock()
try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    sys.modules["requests"] = Mock()

from cc_links.cc_index import (_save_state, _validate_checkpoint_identity,
                               get_index_parts,
                               load_candidates_prioritized)


class ColumnarIndexSourceTests(unittest.TestCase):
    def _response(self):
        body = (
            "cc-index/table/cc-main/warc/crawl=CC-MAIN-TEST/subset=warc/part-0.parquet\n"
            "cc-index/table/cc-main/warc/crawl=CC-MAIN-TEST/subset=robotstxt/part-1.parquet\n"
        )
        response = Mock()
        response.content = gzip.compress(body.encode())
        response.raise_for_status.return_value = None
        return response

    @patch("cc_links.cc_index.requests.get")
    def test_https_index_urls(self, get):
        get.return_value = self._response()
        self.assertEqual(
            get_index_parts("CC-MAIN-TEST", index_source="https"),
            ["https://data.commoncrawl.org/cc-index/table/cc-main/warc/"
             "crawl=CC-MAIN-TEST/subset=warc/part-0.parquet"],
        )

    @patch("cc_links.cc_index.requests.get")
    def test_s3_index_urls(self, get):
        get.return_value = self._response()
        self.assertEqual(
            get_index_parts("CC-MAIN-TEST", index_source="s3"),
            ["s3://commoncrawl/cc-index/table/cc-main/warc/"
             "crawl=CC-MAIN-TEST/subset=warc/part-0.parquet"],
        )

    def test_rejects_unknown_source(self):
        with self.assertRaises(ValueError):
            get_index_parts("CC-MAIN-TEST", index_source="ftp")

    def test_checkpoint_preserves_broad_quota(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "state.json")
            _save_state(
                path, {3}, {"Example": 9}, {"example.test": 1},
                allowed_parts_count=5, broad_remaining={"Example": 2},
                metrics={"index_rows_scanned": 123})
            with open(path, encoding="utf-8") as source:
                state = json.load(source)
            self.assertEqual(state["broad_remaining"], {"Example": 2})
            self.assertEqual(state["metrics"]["index_rows_scanned"], 123)

    def test_prefetch_priority_orders_precise_before_broad(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "manifest.jsonl")
            with open(path, "w", encoding="utf-8") as output:
                for record in [
                    {"url": "https://b.test/community/1",
                     "discovery_tier": 1, "prefetch_score": 25},
                    {"url": "https://a.test/viewtopic.php?t=1",
                     "discovery_tier": 0, "prefetch_score": 55},
                    {"url": "https://c.test/bitrix/redirect.php?goto=x",
                     "discovery_tier": 0, "prefetch_score": 65},
                ]:
                    output.write(json.dumps(record) + "\n")
            urls = [
                record["url"] for record in load_candidates_prioritized(path)
            ]
            self.assertEqual(urls[0], "https://c.test/bitrix/redirect.php?goto=x")
            self.assertEqual(urls[-1], "https://b.test/community/1")

    def test_feedback_profile_reorders_existing_manifest_without_rescan(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "manifest.jsonl")
            with open(path, "w", encoding="utf-8") as output:
                for record in [
                    {
                        "url": "https://weak.test/forum",
                        "bucket": "latam",
                        "discovery_tier": 0,
                        "prefetch_score": 60,
                        "pattern_id": "rule:weak",
                    },
                    {
                        "url": "https://good.test/forum",
                        "bucket": "latam",
                        "discovery_tier": 0,
                        "prefetch_score": 55,
                        "pattern_id": "rule:good",
                    },
                ]:
                    output.write(json.dumps(record) + "\n")
            profile = {
                "patterns": {
                    "rule:weak": {"score_adjustment": -10},
                    "rule:good": {"score_adjustment": 10},
                },
                "pattern_buckets": {},
            }
            urls = [
                record["url"] for record in load_candidates_prioritized(
                    path, priority_profile=profile
                )
            ]
            self.assertEqual(urls[0], "https://good.test/forum")

    def test_checkpoint_identity_rejects_silent_ruleset_change(self):
        expected = {
            "crawl": "CC-A",
            "taxonomy_hash": "new",
            "discovery_profile": "precise",
        }
        with self.assertRaises(ValueError):
            _validate_checkpoint_identity(
                {"checkpoint_identity": {
                    "crawl": "CC-A",
                    "taxonomy_hash": "old",
                    "discovery_profile": "precise",
                }},
                expected,
            )
        # Legacy checkpoints remain resumable and are not relabeled.
        _validate_checkpoint_identity({"scanned_parts": [1]}, expected)


if __name__ == "__main__":
    unittest.main()
