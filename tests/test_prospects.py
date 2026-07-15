import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cc_links.db import init_db, upsert_candidate
from cc_links.prospects import classify_prospect, discovery_url_terms, normalize_url


class ProspectClassifierTests(unittest.TestCase):
    def test_wordpress_comment_needs_page_evidence(self):
        homepage = '<meta name="generator" content="WordPress 6.7">'
        self.assertEqual(classify_prospect(homepage, "https://example.com/"), [])
        html = homepage + '<form id="comment-form" action="/wp-comments-post.php"></form>'
        match = classify_prospect(html, "https://example.com/post/")[0]
        self.assertEqual(match.family, "blog_comment")
        self.assertEqual(match.platform, "WordPress")
        self.assertGreaterEqual(match.score, 60)

    def test_exact_forum_platform_signal(self):
        matches = classify_prospect("<footer>Powered by phpBB</footer>",
                                    "https://forum.example.net/index.php")
        self.assertEqual(matches[0].rule_id, "phpbb_forum")

    def test_broad_phrase_alone_is_rejected(self):
        self.assertEqual(classify_prospect("Please leave a comment", "https://example.org/post"), [])

    def test_normalization_removes_tracking_and_fragment(self):
        value = normalize_url("HTTPS://Example.COM:443/post?utm_source=x&id=7#comments")
        self.assertEqual(value, "https://example.com/post?id=7")

    def test_discovery_terms_are_selective(self):
        terms = discovery_url_terms()
        self.assertIn("wp-comments-post.php", terms)
        self.assertNotIn("/forum/", terms)


class ProspectDatabaseTests(unittest.TestCase):
    def test_upsert_keeps_highest_score(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "test.db"
            conn = init_db(str(db))
            common = dict(normalized_url="https://example.com/post", url="https://example.com/post",
                          domain="example.com", registered_domain="example.com", crawl="CC-TEST",
                          tld="com", country="", bucket="test", family="blog_comment",
                          platform="WordPress", warc_filename="x", warc_offset=1, warc_length=2)
            upsert_candidate(conn, **common, score=80, matched_signals=json.dumps(["strong"]))
            upsert_candidate(conn, **common, score=50, matched_signals=json.dumps(["weak"]))
            score, signals = conn.execute(
                "SELECT score, matched_signals FROM candidates").fetchone()
            self.assertEqual(score, 80)
            self.assertIn("strong", signals)
            conn.close()

    def test_upsert_replaces_classification_when_score_is_higher(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = init_db(str(Path(tmp) / "test.db"))
            common = dict(normalized_url="https://example.com/x", url="https://example.com/x",
                          domain="example.com", registered_domain="example.com", crawl="CC-TEST",
                          tld="com", country="", bucket="test", platform=None,
                          warc_filename="x", warc_offset=1, warc_length=2)
            upsert_candidate(conn, **common, family="directory", score=50,
                             matched_signals="[]")
            upsert_candidate(conn, **common, family="forum", score=90,
                             matched_signals="[]")
            self.assertEqual(conn.execute("SELECT family FROM candidates").fetchone()[0], "forum")
            conn.close()


if __name__ == "__main__":
    unittest.main()
