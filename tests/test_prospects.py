import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cc_links.db import (enforce_candidate_floor, enforce_domain_cap, init_db,
                         mark_url_processed, upsert_candidate)
from cc_links.prospects import (classify_prospect, discovery_url_patterns,
                                discovery_url_terms, normalize_url)


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

    def test_cli_minimum_score_is_hard_floor(self):
        matches = classify_prospect("<footer>Powered by phpBB</footer>",
                                    "https://forum.example.net/index.php",
                                    minimum_score=60)
        self.assertEqual(matches, [])

    def test_selective_url_signal_survives_default_threshold(self):
        matches = classify_prospect("<html></html>",
                                    "https://forum.example.net/viewtopic.php?t=7")
        self.assertEqual(matches[0].rule_id, "phpbb_forum")
        self.assertGreaterEqual(matches[0].score, 50)

    def test_broad_phrase_alone_is_rejected(self):
        self.assertEqual(classify_prospect("Please leave a comment", "https://example.org/post"), [])

    def test_normalization_removes_tracking_and_fragment(self):
        value = normalize_url("HTTPS://Example.COM:443/post?utm_source=x&id=7#comments")
        self.assertEqual(value, "https://example.com/post?id=7")

    def test_discovery_terms_are_selective(self):
        terms = discovery_url_terms()
        self.assertIn("wp-comments-post.php", terms)
        self.assertNotIn("/forum/", terms)

    def test_compound_discovery_patterns_preserve_conjunction(self):
        patterns = discovery_url_patterns()
        self.assertIn(("/bitrix/redirect.php", "goto="), patterns)
        self.assertNotIn(("goto=",), patterns)

    def test_known_positive_redirect_patterns(self):
        cases = [
            "https://example.vn/index.php?nv=statistics&nv_redirect=aHR0cHM6Ly90LmV4YW1wbGU=",
            "https://example.ru/bitrix/rk.php?goto=https://target.example/page",
            "https://www.google.co.zm/url?q=https://target.example/",
            "https://example.org/proxy.php?link=https://target.example/",
        ]
        for url in cases:
            with self.subTest(url=url):
                matches = classify_prospect("<html></html>", url)
                self.assertEqual(matches[0].family, "redirect_backlink")

    def test_broad_redirect_parameter_alone_is_rejected(self):
        self.assertEqual(classify_prospect(
            "<html></html>", "https://example.org/article?url=https://target.example/"), [])

    def test_embedded_target_does_not_classify_the_source_as_forum(self):
        matches = classify_prospect(
            "<html></html>",
            "https://www.google.co.zm/url?q=https://target.example/viewtopic.php?t=7")
        self.assertEqual([match.family for match in matches], ["redirect_backlink"])

    def test_known_positive_page_roles(self):
        wiki = classify_prospect(
            '<meta name="generator" content="MediaWiki 1.41">',
            "https://example.org/index.php?title=User:Example")
        self.assertEqual(wiki[0].family, "profile_page")
        discuz = classify_prospect(
            "<html></html>", "https://example.cn/forum.php?mod=viewthread&tid=7")
        self.assertEqual(discuz[0].platform, "Discuz")


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

    def test_processed_urls_are_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = init_db(str(Path(tmp) / "test.db"))
            mark_url_processed(conn, "https://example.com/x", "https://example.com/x",
                               "CC-TEST", "unmatched")
            self.assertEqual(
                conn.execute("SELECT outcome FROM processed_urls").fetchone()[0], "unmatched")
            conn.close()

    def test_score_floor_archives_old_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = init_db(str(Path(tmp) / "test.db"))
            common = dict(normalized_url="https://example.com/x", url="https://example.com/x",
                          domain="example.com", registered_domain="example.com", crawl="CC-TEST",
                          tld="com", country="", bucket="test", family="social_bookmark",
                          platform="Pligg", warc_filename="x", warc_offset=1, warc_length=2)
            upsert_candidate(conn, **common, score=35, matched_signals="[]")
            self.assertEqual(enforce_candidate_floor(conn, 50), 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT outcome FROM processed_urls").fetchone()[0], "below_threshold")
            conn.close()

    def test_domain_cap_keeps_highest_scores(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = init_db(str(Path(tmp) / "test.db"))
            for i, score in enumerate((50, 90, 70)):
                upsert_candidate(
                    conn, normalized_url=f"https://example.com/{i}",
                    url=f"https://example.com/{i}", domain="example.com",
                    registered_domain="example.com", crawl="CC-TEST", tld="com",
                    country="", bucket="test", family="forum", platform="phpBB",
                    score=score, matched_signals="[]", warc_filename="x",
                    warc_offset=i, warc_length=2)
            self.assertEqual(enforce_domain_cap(conn, 2), 1)
            self.assertEqual([r[0] for r in conn.execute(
                "SELECT score FROM candidates ORDER BY score DESC")], [90, 70])
            self.assertEqual(conn.execute(
                "SELECT outcome FROM processed_urls").fetchone()[0], "domain_cap")
            conn.close()


if __name__ == "__main__":
    unittest.main()
