import csv
import json
import tempfile
import unittest
from pathlib import Path

from baseline_report import (
    collect_checkpoint_metrics,
    collect_database_metrics,
    collect_manifest_metrics,
    collect_validation_metrics,
)
from cc_links.db import init_db


class BaselineReportTests(unittest.TestCase):
    def test_database_metrics_are_read_only_and_include_outcomes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "prospects.db")
            conn = init_db(path)
            conn.executemany(
                """
                INSERT INTO candidates
                (normalized_url, url, domain, registered_domain, crawl, family,
                 score, matched_signals)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("https://a.test/1", "https://a.test/1", "a.test", "a.test",
                     "CC-A", "forum", 90, "[]"),
                    ("https://a.test/2", "https://a.test/2", "a.test", "a.test",
                     "CC-A", "forum", 55, "[]"),
                    ("https://b.test/1", "https://b.test/1", "b.test", "b.test",
                     "CC-B", "wiki", 75, "[]"),
                ],
            )
            conn.executemany(
                """
                INSERT INTO processed_urls
                (normalized_url, url, crawl, outcome, score)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    ("a", "https://a.test/1", "CC-A", "stored", 90),
                    ("b", "https://b.test/1", "CC-B", "domain_cap", 75),
                    ("c", "https://c.test/1", "CC-B", "unmatched", 0),
                ],
            )
            conn.commit()
            conn.close()

            metrics = collect_database_metrics(path)
            self.assertEqual(metrics["summary"]["candidates"], 3)
            self.assertEqual(metrics["summary"]["domains"], 2)
            self.assertEqual(metrics["summary"]["max_urls_per_domain"], 2)
            self.assertEqual(metrics["processing"]["qualified_decisions"], 2)
            self.assertAlmostEqual(metrics["processing"]["qualified_rate"], 2 / 3, 6)

    def test_manifest_and_checkpoint_metrics_preserve_discovery_tiers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "crawl.jsonl"
            manifest.write_text(
                "\n".join([
                    json.dumps({
                        "url": "https://a.test/forum/1",
                        "url_host_registered_domain": "a.test",
                        "bucket": "A",
                        "discovery_tier": 0,
                    }),
                    json.dumps({
                        "url": "https://b.test/community/1",
                        "url_host_registered_domain": "b.test",
                        "bucket": "B",
                        "discovery_tier": 1,
                    }),
                ]) + "\n",
                encoding="utf-8",
            )
            state = root / "crawl.jsonl.state.json"
            state.write_text(json.dumps({
                "scanned_parts": [0, 2],
                "allowed_parts_count": 4,
                "remaining": {"A": 2},
                "metrics": {
                    "index_rows_scanned": 2_000_000,
                    "candidates_written": 2,
                },
            }), encoding="utf-8")

            manifest_metrics = collect_manifest_metrics([str(manifest)])
            self.assertEqual(manifest_metrics["tiers"], {"0": 1, "1": 1})
            checkpoint_metrics = collect_checkpoint_metrics(str(root))
            self.assertEqual(checkpoint_metrics["scanned_parts"], 2)
            self.assertEqual(
                checkpoint_metrics["candidates_per_million_index_rows"], 1.0
            )

    def test_validation_metrics_report_live_family_agreement(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "validation.csv"
            with path.open("w", encoding="utf-8", newline="") as output:
                writer = csv.DictWriter(
                    output,
                    fieldnames=[
                        "family", "live_http_status", "live_family", "verdict"
                    ],
                )
                writer.writeheader()
                writer.writerow({
                    "family": "forum", "live_http_status": "200",
                    "live_family": "forum", "verdict": "good",
                })
                writer.writerow({
                    "family": "wiki", "live_http_status": "404",
                    "live_family": "", "verdict": "bad",
                })

            metrics = collect_validation_metrics(str(path))
            self.assertEqual(metrics["rows"], 2)
            self.assertEqual(metrics["live_2xx"], 1)
            self.assertEqual(metrics["family_agreement_rate"], 1.0)
            self.assertEqual(metrics["verdicts"], {"good": 1, "bad": 1})


if __name__ == "__main__":
    unittest.main()
