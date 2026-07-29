import tempfile
import unittest
from pathlib import Path

from cc_links.outreach_db import (
    domain_page_counts,
    init_outreach_db,
    iter_selected_pages,
    save_outreach_page,
    save_outreach_pages,
)


def page(url, *, weight=90, specificity=12, crawl="CC-TEST", domain="example.com"):
    return {
        "url": url,
        "crawl": crawl,
        "url_path": "/" + url.rsplit("/", 1)[-1],
        "registered_domain": domain,
        "tld": "com",
        "content_languages": "eng",
        "fetch_time": "2026-01-01T00:00:00Z",
        "pattern_id": "en.test",
        "pattern_language": "en",
        "matched_expression": "write-for-us",
        "matched_pattern_ids": ["en.test"],
        "pattern_weight": weight,
        "path_specificity": specificity,
        "source_part": "part-a",
        "warc_filename": "x.warc.gz",
        "warc_record_offset": 1,
        "warc_record_length": 2,
    }


class OutreachDatabaseTests(unittest.TestCase):
    def test_replay_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            connection = init_outreach_db(Path(tmp) / "outreach.db")
            record = page("https://example.com/write-for-us")
            self.assertTrue(save_outreach_page(connection, record))
            self.assertFalse(save_outreach_page(connection, record))
            connection.commit()
            self.assertEqual(
                connection.execute(
                    "SELECT match_count FROM outreach_prospects"
                ).fetchone()[0],
                1,
            )
            connection.close()

    def test_best_url_uses_weight_and_specificity(self):
        with tempfile.TemporaryDirectory() as tmp:
            connection = init_outreach_db(Path(tmp) / "outreach.db")
            save_outreach_pages(
                connection,
                [
                    page("https://example.com/low", weight=70, specificity=20),
                    page("https://example.com/high", weight=95, specificity=10),
                    page("https://example.com/specific", weight=95, specificity=30),
                ],
            )
            best_url, weight, count = connection.execute(
                "SELECT best_url, best_pattern_weight, match_count "
                "FROM outreach_prospects"
            ).fetchone()
            self.assertEqual(best_url, "https://example.com/specific")
            self.assertEqual(weight, 95)
            self.assertEqual(count, 3)
            connection.close()

    def test_domain_cap_is_applied_on_selection_not_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            connection = init_outreach_db(Path(tmp) / "outreach.db")
            save_outreach_pages(
                connection,
                [
                    page("https://example.com/a", weight=70),
                    page("https://example.com/b", weight=90),
                    page("https://example.com/c", weight=80),
                    page(
                        "https://other.test/write",
                        weight=95,
                        domain="other.test",
                    ),
                ],
            )
            selected = list(iter_selected_pages(connection, max_per_domain=2))
            self.assertEqual(len(selected), 3)
            self.assertEqual(
                [
                    row["url"]
                    for row in selected
                    if row["registered_domain"] == "example.com"
                ],
                ["https://example.com/b", "https://example.com/c"],
            )
            self.assertEqual(
                domain_page_counts(connection), {"example.com": 3, "other.test": 1}
            )
            connection.close()

    def test_bulk_cap_replaces_early_low_quality_page(self):
        with tempfile.TemporaryDirectory() as tmp:
            connection = init_outreach_db(Path(tmp) / "outreach.db")
            save_outreach_pages(
                connection,
                [
                    page("https://example.com/low", weight=60),
                    page("https://example.com/mid", weight=80),
                ],
                max_per_domain=2,
            )
            save_outreach_pages(
                connection,
                [page("https://example.com/high", weight=100)],
                max_per_domain=2,
            )
            urls = [
                row[0]
                for row in connection.execute(
                    "SELECT url FROM outreach_pages ORDER BY pattern_weight DESC"
                )
            ]
            self.assertEqual(
                urls,
                ["https://example.com/high", "https://example.com/mid"],
            )
            self.assertEqual(
                connection.execute(
                    "SELECT best_url FROM outreach_prospects"
                ).fetchone()[0],
                "https://example.com/high",
            )
            connection.close()

    def test_url_can_be_observed_in_multiple_crawls(self):
        with tempfile.TemporaryDirectory() as tmp:
            connection = init_outreach_db(Path(tmp) / "outreach.db")
            first = page("https://example.com/write-for-us", crawl="CC-TEST-1")
            second = page("https://example.com/write-for-us", crawl="CC-TEST-2")
            self.assertEqual(save_outreach_pages(connection, [first, second]), 2)
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM outreach_pages").fetchone()[0],
                2,
            )
            connection.close()


if __name__ == "__main__":
    unittest.main()
