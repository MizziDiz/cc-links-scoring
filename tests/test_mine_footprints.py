import json
import os
import sqlite3
import tempfile
import unittest

import mine_footprints as mf
from cc_links.db import init_db, mark_url_processed, upsert_candidate


class TokenTests(unittest.TestCase):
    def test_url_tokens_and_terms(self):
        tokens = mf.url_tokens("https://x.test/bbs/board.php?bo_table=free&wr_id=12&mode=view")
        self.assertIn("seg:bbs", tokens)
        self.assertIn("file:board.php", tokens)
        self.assertIn("qk:bo_table", tokens)
        self.assertIn("qkv:bo_table=free", tokens)
        self.assertNotIn("qkv:wr_id=12", tokens)
        self.assertEqual(mf.token_term("seg:bbs"), "/bbs/")
        self.assertEqual(mf.token_term("file:board.php"), "board.php")
        self.assertEqual(mf.token_term("qk:bo_table"), "bo_table=")
        self.assertEqual(mf.token_term("qkv:mode=view"), "mode=view")

    def test_path_only_input(self):
        self.assertIn("seg:guestbook", mf.url_tokens("/guestbook/index.php"))


class ProspectsReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "p.db")
        conn = init_db(self.db)
        for i in range(30):
            url = f"http://f{i}.test/viewtopic.php?t={i}"
            upsert_candidate(
                conn, normalized_url=url, url=url, domain=f"f{i}.test",
                registered_domain=f"f{i}.test", crawl="c", tld="test", country=None,
                bucket=None, family="forum", platform="phpBB", score=80,
                matched_signals=json.dumps([{"rule_id": "phpbb_forum",
                                             "signals": ["url:viewtopic.php", "html:phpbb"]}]),
                warc_filename="w", warc_offset=0, warc_length=1)
            mark_url_processed(conn, url, url, "c", "stored", 80, pattern_id="phpbb_forum:0",
                               discovery_tier=0, registered_domain=f"f{i}.test",
                               final_rule_id="phpbb_forum")
        for i in range(40):
            url = f"http://g{i}.test/guestbook/"
            mark_url_processed(conn, url, url, "c", "unmatched", pattern_id="guestbook:0",
                               discovery_tier=0, registered_domain=f"g{i}.test")
        conn.commit()
        conn.close()
        self.taxonomy = {"rules": [
            {"id": "phpbb_forum", "platform": "phpBB", "signals": {
                "url_contains": ["viewtopic.php"], "html_contains": ["phpbb", "never-seen"]}},
            {"id": "silent_rule", "platform": "Nothing", "signals": {"html_contains": ["x"]}},
        ], "discovery": {"broad_terms": ["guestbook"]}}

    def tearDown(self):
        self.tmp.cleanup()

    def test_pattern_yield_and_rule_activity(self):
        conn = mf.read_only(self.db)
        try:
            rows = mf.pattern_yield(conn, None)
            by_id = {r["pattern_id"]: r for r in rows}
            self.assertEqual(by_id["guestbook:0"]["yield"], 0.0)
            self.assertEqual(by_id["phpbb_forum:0"]["stored"], 30)
            activity = mf.rule_activity(conn, self.taxonomy, sample_every=1)
            self.assertIn("silent_rule", activity["silent_rules"])
            self.assertIn({"rule": "phpbb_forum", "signal": "html:never-seen"},
                          activity["dead_signals"])
            tokens = mf.token_precision(conn, None, 10, self.taxonomy)
        finally:
            conn.close()
        by_term = {r["term"]: r for r in tokens["tokens"]}
        self.assertEqual(by_term["viewtopic.php"]["precision"], 1.0)
        self.assertTrue(by_term["viewtopic.php"]["in_taxonomy"])
        self.assertEqual(by_term["/guestbook/"]["precision"], 0.0)
        self.assertTrue(any(r["term"] == "/guestbook/" for r in tokens["traps"]))

    def test_database_opened_read_only(self):
        missing = os.path.join(self.tmp.name, "absent.db")
        with self.assertRaises(sqlite3.OperationalError):
            mf.read_only(missing).execute("SELECT 1")
        self.assertFalse(os.path.exists(missing))


class GsaReportTests(unittest.TestCase):
    def test_engine_tokens_and_coverage_gap(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "g.db")
            conn = sqlite3.connect(path)
            conn.execute("CREATE TABLE rows(base TEXT, kind TEXT, type TEXT, engine TEXT, "
                         "host TEXT, tld TEXT, cc TEXT, ip TEXT, path TEXT, gsa_cc TEXT)")
            rows = []
            for i in range(25):
                rows.append(("b", "verified", "Forum", "YYBoard", f"h{i}.jp", "jp", "jp", "",
                             "/cgi-bin/yybbs/yybbs.cgi?list=thread", ""))
                rows.append(("b", "verified", "Blog Comment", "General Blogs", f"w{i}.de", "de", "de", "",
                             "/hallo-welt/", ""))
                rows.append(("b", "success", "Forum", "phpBB", f"p{i}.de", "de", "de", "",
                             f"/viewtopic.php?t={i}", ""))
                rows.append(("b", "verified", "Indexer", "Fast Indexer", f"i{i}.com", "com", "", "",
                             "/__media__/js/netsoltrademark.php?d=x", ""))
            conn.executemany("INSERT INTO rows VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
            conn.commit()
            conn.close()
            taxonomy = {"rules": [{"id": "phpbb_forum", "platform": "phpBB",
                                   "signals": {"url_contains": ["viewtopic.php"]}},
                                  {"id": "wordpress_comment", "platform": "WordPress",
                                   "signals": {"url_contains": ["replytocom="]}}]}
            report = mf.gsa_engines(mf.read_only(path), taxonomy, min_hosts=10, min_token_hosts=10)
        engines = {e["engine"]: e for e in report["engines"]}
        self.assertTrue(engines["phpBB"]["covered_by_taxonomy"])
        # "General Blogs" is GSA's name for WordPress comments: alias resolves it.
        self.assertTrue(engines["General Blogs"]["covered_by_taxonomy"])
        self.assertFalse(engines["YYBoard"]["covered_by_taxonomy"])
        self.assertFalse(engines["Fast Indexer"]["placement"])
        self.assertEqual([e["engine"] for e in report["coverage_gaps"]], ["YYBoard"])
        yy_terms = {t["term"] for t in engines["YYBoard"]["tokens"]}
        self.assertIn("yybbs.cgi", yy_terms)


if __name__ == "__main__":
    unittest.main()
