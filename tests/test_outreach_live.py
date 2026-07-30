import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cc_links.outreach_db import init_outreach_db, save_outreach_pages
from cc_links.outreach_live import (
    PageCheck,
    ValidationConfig,
    classify_live_html,
    qualify_outreach,
    write_qualification_outputs,
)
from tests.test_outreach_db import page


class OutreachLiveClassificationTests(unittest.TestCase):
    def test_current_invitation_with_submission_form_is_approved(self):
        outcome, score, reasons, evidence = classify_live_html(
            original_url="https://example.com/write-for-us/",
            final_url="https://example.com/write-for-us/",
            registered_domain="example.com",
            matched_expression="write-for-us",
            html="""
                <html><body>
                  <h1>Write for us</h1>
                  <a href="/contact/">Contact</a>
                  <form><textarea name="article_content"></textarea></form>
                </body></html>
            """,
        )
        self.assertEqual(outcome, "approved")
        self.assertGreaterEqual(score, 90)
        self.assertIn("current_invitation_phrase", reasons)
        self.assertTrue(evidence["submission_form"])

    def test_taxonomy_archive_is_rejected(self):
        outcome, score, reasons, _ = classify_live_html(
            original_url="https://example.com/tag/guide-for-authors/",
            final_url="https://example.com/tag/guide-for-authors/",
            registered_domain="example.com",
            matched_expression="guide-for-authors",
            html="<html><title>Guide for authors archive</title></html>",
        )
        self.assertEqual(outcome, "rejected")
        self.assertLess(score, 40)
        self.assertIn("taxonomy_or_archive_path", reasons)

    def test_stale_path_without_current_phrase_requires_review(self):
        outcome, _score, reasons, _ = classify_live_html(
            original_url="https://example.com/write-for-us/",
            final_url="https://example.com/write-for-us/",
            registered_domain="example.com",
            matched_expression="write-for-us",
            html="<html><body>Welcome to our company website.</body></html>",
        )
        self.assertEqual(outcome, "review")
        self.assertIn("no_current_invitation_phrase", reasons)


class OutreachLiveRunTests(unittest.TestCase):
    def test_run_is_resumable_and_exports_domain_statuses(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "outreach.db"
            connection = init_outreach_db(source)
            save_outreach_pages(
                connection,
                [
                    page(
                        "https://a.example/write-for-us/",
                        domain="a.example",
                    ),
                    page(
                        "https://b.example/submit-article/",
                        domain="b.example",
                    ),
                ],
            )
            connection.close()
            calls = []

            def fake_checker(rows, config):
                calls.extend(str(row["url"]) for row in rows)
                return [
                    PageCheck(
                        url=str(row["url"]),
                        registered_domain=str(row["registered_domain"]),
                        checked_at="2026-01-01T00:00:00+00:00",
                        attempts=1,
                        elapsed_ms=10,
                        robots_status="allowed",
                        http_status=200,
                        final_url=str(row["url"]),
                        content_type="text/html",
                        bytes_read=100,
                        outcome="approved",
                        qualification_score=90,
                        reason_codes=["current_invitation_phrase"],
                        evidence={"positive_phrase": True},
                    )
                    for row in rows
                ]

            result_db = root / "live.db"
            first = qualify_outreach(
                input_db=source,
                out_db=result_db,
                workers=2,
                config=ValidationConfig(),
                domain_checker=fake_checker,
            )
            self.assertEqual(first.checked_pages, 2)
            self.assertEqual(first.domain_outcomes, {"approved": 2})
            self.assertEqual(len(calls), 2)

            calls.clear()
            second = qualify_outreach(
                input_db=source,
                out_db=result_db,
                workers=2,
                config=ValidationConfig(),
                domain_checker=fake_checker,
            )
            self.assertEqual(second.checked_pages, 2)
            self.assertEqual(calls, [])

            report = root / "report.json"
            exports = root / "exports"
            payload = write_qualification_outputs(result_db, report, exports)
            self.assertEqual(payload["domain_outcomes"], {"approved": 2})
            self.assertTrue((exports / "approved.csv").exists())
            self.assertEqual(json.loads(report.read_text())["domains"], 2)

            check = sqlite3.connect(result_db)
            self.assertEqual(
                check.execute("SELECT COUNT(*) FROM page_checks").fetchone()[0], 2
            )
            check.close()


if __name__ == "__main__":
    unittest.main()
