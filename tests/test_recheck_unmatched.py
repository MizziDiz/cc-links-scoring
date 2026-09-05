import argparse
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import recheck_unmatched
from cc_links.db import init_db, mark_url_processed
from cc_links.prospects import ProspectMatch


def _args(db, state_dir, **overrides):
    values = dict(db=db, state_dir=[state_dir], group=[], since=None, until=None,
                  limit=None, footprints=None, min_score=50, max_per_domain=10,
                  source="cloudfront", rate_limit=1000, workers=2, commit_every=2,
                  progress_interval=3600, log=None)
    values.update(overrides)
    return argparse.Namespace(**values)


def _manifest(path, rows):
    with open(path, "w", encoding="utf-8") as out:
        for i, (url, pattern_id) in enumerate(rows):
            out.write(json.dumps({
                "url": url, "url_host_tld": "test",
                "url_host_registered_domain": url.split("/")[2], "bucket": "T",
                "filename": "w.warc.gz", "offset": i * 10, "length": 10,
                "fetch_status": 200, "discovery_tier": 0, "pattern_id": pattern_id,
                "prefetch_score": 55,
            }) + "\n")


def _fake_fetch(record, footprints, minimum_score):
    # The new taxonomy "recognizes" guestbook pages; user pages stay rejected.
    if "broken" in record["url"]:
        return {"ok": False, "record": record, "error": "ReadTimeout: x"}
    if "guestbook" in record["url"]:
        return {"ok": True, "record": record, "matches": [
            ProspectMatch("guestbook", "guestbook", None, 80, 2, ["url:guestbook", "html:gästebuch"])]}
    return {"ok": True, "record": record, "matches": []}


class RecheckTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "p.db")
        self.state = os.path.join(self.tmp.name, "states")
        os.makedirs(self.state)
        conn = init_db(self.db)
        self.urls = [(f"http://g{i}.test/guestbook/", "guestbook:0") for i in range(4)]
        self.urls += [(f"http://u{i}.test/user/x", "broad:/user/") for i in range(3)]
        self.urls += [("http://broken.test/guestbook/", "guestbook:0")]
        for url, pattern_id in self.urls:
            mark_url_processed(conn, url, url, "CC-MAIN-2026-21", "unmatched",
                               pattern_id=pattern_id, discovery_tier=0,
                               registered_domain=url.split("/")[2])
        # A rejection with no manifest line must simply be reported as missing.
        mark_url_processed(conn, "http://nomanifest.test/guestbook/",
                           "http://nomanifest.test/guestbook/", "CC-MAIN-2026-21",
                           "unmatched", pattern_id="guestbook:0", discovery_tier=0)
        conn.commit()
        conn.close()
        _manifest(os.path.join(self.state, "CC-MAIN-2026-21.jsonl"), self.urls)

    def tearDown(self):
        self.tmp.cleanup()

    def test_recheck_flips_recognized_pages_and_keeps_the_rest(self):
        log = os.path.join(self.tmp.name, "recheck.jsonl")
        with mock.patch.object(recheck_unmatched, "fetch_and_classify", _fake_fetch):
            code = recheck_unmatched.run(_args(self.db, self.state, log=log))
        self.assertEqual(code, 0)
        conn = sqlite3.connect(self.db)
        outcomes = dict(conn.execute(
            "SELECT outcome, COUNT(*) FROM processed_urls GROUP BY outcome").fetchall())
        stored = {r[0] for r in conn.execute("SELECT url FROM candidates")}
        rule = conn.execute(
            "SELECT final_rule_id FROM processed_urls WHERE url='http://g0.test/guestbook/'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(outcomes["stored"], 4)
        # 3 user pages + the fetch error + the one without a manifest line stay unmatched.
        self.assertEqual(outcomes["unmatched"], 5)
        self.assertEqual(len(stored), 4)
        self.assertEqual(rule, "guestbook")
        with open(log, encoding="utf-8") as source:
            lines = [json.loads(l) for l in source]
        self.assertEqual(len(lines), 8)
        self.assertEqual(sum(l["after"].startswith("stored:") for l in lines), 4)
        self.assertEqual(sum(l["after"] == "error" for l in lines), 1)

    def test_group_filter_and_limit(self):
        with mock.patch.object(recheck_unmatched, "fetch_and_classify", _fake_fetch):
            recheck_unmatched.run(_args(self.db, self.state, group=["broad:/user/"], limit=2))
        conn = sqlite3.connect(self.db)
        stored = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
        conn.close()
        self.assertEqual(stored, 0)

    def test_until_selects_old_decisions_only(self):
        conn = init_db(self.db)
        targets = recheck_unmatched.select_targets(conn, [], None, "2000-01-01", None)
        conn.close()
        self.assertEqual(targets, {})


if __name__ == "__main__":
    unittest.main()
