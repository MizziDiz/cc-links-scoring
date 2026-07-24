import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cc_links.db import init_db, mark_url_processed
from cc_links.feedback import (
    collect_pattern_feedback,
    load_priority_adjustments,
    priority_adjustment,
    priority_profile_hash,
)


class PatternFeedbackTests(unittest.TestCase):
    def test_feedback_separates_productive_and_noisy_patterns(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "prospects.db")
            conn = init_db(db)
            for index in range(30):
                url = f"https://good-{index}.test/topic"
                mark_url_processed(
                    conn, url, url, "CC-TEST", "stored", score=80,
                    registered_domain=f"good-{index}.test", bucket="latam",
                    discovery_tier=0, pattern_id="rule:good",
                    final_family="forum", final_rule_id="forum_good",
                )
                noisy = f"https://noise-{index}.test/community"
                mark_url_processed(
                    conn, noisy, noisy, "CC-TEST", "unmatched",
                    registered_domain=f"noise-{index}.test", bucket="latam",
                    discovery_tier=1, pattern_id="broad:community",
                )
            conn.commit()
            conn.close()

            report = collect_pattern_feedback(db, minimum_samples=10)
            self.assertEqual(report["summary"]["attributed_decisions"], 60)
            self.assertEqual(report["summary"]["baseline_qualified_rate"], 0.5)
            self.assertGreater(
                report["patterns"]["rule:good"]["score_adjustment"], 0
            )
            self.assertLess(
                report["patterns"]["broad:community"]["score_adjustment"], 0
            )
            self.assertEqual(
                priority_adjustment(report, "rule:good", "latam"),
                report["pattern_buckets"]["rule:good|latam"][
                    "score_adjustment"
                ],
            )

    def test_small_samples_keep_exploration_and_profile_is_hashable(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "weights.json"
            profile = {
                "version": 1,
                "patterns": {
                    "rule:new": {
                        "score_adjustment": 0,
                        "exploration": True,
                    }
                },
                "pattern_buckets": {},
            }
            path.write_text(json.dumps(profile), encoding="utf-8")
            loaded = load_priority_adjustments(str(path))
            self.assertEqual(priority_adjustment(loaded, "rule:new"), 0)
            self.assertEqual(len(priority_profile_hash(str(path))), 16)

    def test_legacy_database_can_use_manifest_attribution_read_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "legacy.db"
            conn = sqlite3.connect(db)
            conn.execute(
                """CREATE TABLE processed_urls (
                       normalized_url TEXT PRIMARY KEY,
                       url TEXT,
                       crawl TEXT,
                       outcome TEXT,
                       score INTEGER
                   )"""
            )
            url = "https://example.test/viewtopic.php?t=1"
            conn.execute(
                "INSERT INTO processed_urls VALUES (?, ?, ?, ?, ?)",
                (url, url, "CC-TEST", "stored", 80),
            )
            conn.commit()
            conn.close()
            manifest = Path(tmp) / "manifest.jsonl"
            manifest.write_text(json.dumps({
                "url": url,
                "url_host_registered_domain": "example.test",
                "bucket": "latam",
                "discovery_tier": 0,
            }) + "\n", encoding="utf-8")

            report = collect_pattern_feedback(
                str(db), minimum_samples=1,
                manifest_paths=[str(manifest)],
            )
            self.assertEqual(report["summary"]["attributed_decisions"], 1)
            self.assertEqual(len(report["patterns"]), 1)
            self.assertEqual(
                next(iter(report["patterns"].values()))["qualified"], 1
            )
            # The source DB remains on its legacy schema.
            conn = sqlite3.connect(db)
            self.assertNotIn(
                "pattern_id",
                {row[1] for row in conn.execute(
                    "PRAGMA table_info(processed_urls)"
                )},
            )
            conn.close()


if __name__ == "__main__":
    unittest.main()
