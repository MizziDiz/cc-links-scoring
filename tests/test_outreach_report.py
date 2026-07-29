import csv
import tempfile
import unittest
from pathlib import Path

from cc_links.outreach_db import init_outreach_db, save_outreach_pages
from cc_links.outreach_report import (
    build_pilot_report,
    evaluate_review_rows,
    write_review_sample,
)
from tests.test_outreach_db import page


class OutreachReportTests(unittest.TestCase):
    def test_report_and_stratified_sample(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "outreach.db"
            connection = init_outreach_db(db)
            save_outreach_pages(
                connection,
                [
                    page("https://a.example/write", domain="a.example"),
                    page("https://b.example/write", domain="b.example"),
                ],
            )
            connection.close()
            report = build_pilot_report(db)
            self.assertEqual(report["pages"], 2)
            self.assertEqual(report["domains"], 2)
            sample = root / "sample.csv"
            self.assertEqual(write_review_sample(db, sample, size=1), 1)
            with sample.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["label"], "")

    def test_gate_passes_below_strict_noise_threshold(self):
        rows = [
            {
                "pattern_id": "en.write_for_us",
                "label": "noise" if index < 5 else "relevant",
            }
            for index in range(50)
        ]
        result = evaluate_review_rows(rows)
        self.assertTrue(result["gate"]["passed"])
        self.assertEqual(result["noise_rate"], 0.1)

    def test_gate_fails_on_one_noisy_pattern(self):
        rows = [{"pattern_id": "good", "label": "relevant"} for _ in range(40)] + [
            {"pattern_id": "bad", "label": "noise"} for _ in range(10)
        ]
        result = evaluate_review_rows(rows)
        self.assertFalse(result["gate"]["passed"])
        self.assertEqual(result["noisy_patterns"], ["bad"])


if __name__ == "__main__":
    unittest.main()
