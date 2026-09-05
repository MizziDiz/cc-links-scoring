"""End-to-end run() of prospect_pipeline with the network replaced by a stub.

Covers the two behaviours that only show up in the fetch loop itself: the
periodic SQLite commit must fire on every Nth result whatever its outcome, and
the pre-fetch gates must keep low-yield URLs out of the queue entirely.
"""
import argparse
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import prospect_pipeline
from cc_links.db import init_db, mark_url_processed


class CountingConnection:
    """Delegates to a sqlite3 connection and counts commit() calls."""

    def __init__(self, conn):
        self._conn = conn
        self.commits = 0

    def commit(self):
        self.commits += 1
        return self._conn.commit()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _args(db, manifest, **overrides):
    values = dict(
        categories_file="categories.json", category_limits=None,
        category_limit_divisor=1, footprints=None, pattern_priorities=None,
        fetch_limit=None, per_category_limit=10, crawl="CC-MAIN-2026-21", db=db,
        candidates_file=manifest, min_score=50, workers=2, rate_limit=1000,
        max_parts=None, max_per_domain=10, discover_delay=0.0,
        discovery_metrics=False, discovery_profile="precise",
        broad_quota_fraction=0.25, broad_index_sample=0.02, source="cloudfront",
        index_source="https", proxy=None, proxy_file=None, exclude_file=None,
        skip_discovery=True, discovery_only=False, part_shard=None,
        commit_every=3, progress_interval=3600,
        pattern_min_yield=None, pattern_yield_file=None, pattern_yield_since=None,
        pattern_min_decisions=20, pattern_explore_share=0.0,
        new_domains_only=False, per_run_domain_cap=0,
    )
    values.update(overrides)
    return argparse.Namespace(**values)


def _manifest(path, rows):
    with open(path, "w", encoding="utf-8") as out:
        for i, (url, pattern_id, tier, domain) in enumerate(rows):
            out.write(json.dumps({
                "url": url, "url_host_tld": "test",
                "url_host_registered_domain": domain, "bucket": "T",
                "filename": "w.warc.gz", "offset": i * 10, "length": 10,
                "fetch_status": 200, "discovery_tier": tier,
                "pattern_id": pattern_id, "prefetch_score": 50,
            }) + "\n")


def _unmatched(record, footprints, minimum_score):
    return {"ok": True, "record": record, "matches": []}


class RunLoopTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "p.db")
        self.manifest = os.path.join(self.tmp.name, "m.jsonl")
        self.connections = []
        real_init = init_db

        def counting_init(path):
            conn = CountingConnection(real_init(path))
            self.connections.append(conn)
            return conn

        self.init_patch = mock.patch.object(prospect_pipeline, "init_db", counting_init)
        self.init_patch.start()
        self.fetch_patch = mock.patch.object(
            prospect_pipeline, "fetch_and_classify", _unmatched)
        self.fetch_patch.start()

    def tearDown(self):
        self.fetch_patch.stop()
        self.init_patch.stop()
        self.tmp.cleanup()

    def test_commit_fires_on_unmatched_results(self):
        rows = [(f"http://d{i}.test/p", "phpbb_forum:0", 0, f"d{i}.test")
                for i in range(10)]
        _manifest(self.manifest, rows)
        prospect_pipeline.run(_args(self.db, self.manifest, commit_every=3))
        loop_conn = self.connections[-1]
        # 10 results at commit_every=3 -> commits after #3, #6, #9, plus the final one.
        self.assertGreaterEqual(loop_conn.commits, 4)
        conn = sqlite3.connect(self.db)
        outcome_count = conn.execute(
            "SELECT COUNT(*) FROM processed_urls WHERE outcome='unmatched'").fetchone()[0]
        conn.close()
        self.assertEqual(outcome_count, 10)

    def test_yield_gate_keeps_low_yield_urls_out_of_the_queue(self):
        conn = init_db(self.db)
        for i in range(30):
            mark_url_processed(conn, f"http://h{i}.test/gb", f"http://h{i}.test/gb",
                               "CC-MAIN-2026-17", "stored" if i < 2 else "unmatched",
                               pattern_id="guestbook:0", discovery_tier=0,
                               registered_domain=f"h{i}.test")
        conn.commit()
        conn.close()
        rows = [(f"http://g{i}.test/gb", "guestbook:0", 0, f"g{i}.test") for i in range(5)]
        rows += [(f"http://f{i}.test/t", "phpbb_forum:0", 0, f"f{i}.test") for i in range(4)]
        _manifest(self.manifest, rows)
        prospect_pipeline.run(_args(self.db, self.manifest, pattern_min_yield=0.2))
        conn = sqlite3.connect(self.db)
        fetched = {r[0] for r in conn.execute(
            "SELECT url FROM processed_urls WHERE crawl='CC-MAIN-2026-21'")}
        conn.close()
        self.assertEqual(fetched, {f"http://f{i}.test/t" for i in range(4)})

    def test_domain_gates_limit_scheduling(self):
        conn = init_db(self.db)
        conn.execute(
            """INSERT INTO candidates (normalized_url, url, domain, registered_domain,
               crawl, tld, family, score, matched_signals, warc_filename, warc_offset,
               warc_length) VALUES ('http://known.test/a', 'http://known.test/a',
               'known.test', 'known.test', 'c', 'test', 'forum', 90, '[]', 'w', 0, 1)""")
        conn.commit()
        conn.close()
        rows = [("http://known.test/b", "phpbb_forum:0", 0, "known.test"),
                ("http://new.test/1", "phpbb_forum:0", 0, "new.test"),
                ("http://new.test/2", "phpbb_forum:0", 0, "new.test"),
                ("http://new.test/3", "phpbb_forum:0", 0, "new.test")]
        _manifest(self.manifest, rows)
        prospect_pipeline.run(_args(self.db, self.manifest, new_domains_only=True,
                                    per_run_domain_cap=2))
        conn = sqlite3.connect(self.db)
        fetched = sorted(r[0] for r in conn.execute(
            "SELECT url FROM processed_urls WHERE crawl='CC-MAIN-2026-21'"))
        conn.close()
        # Ties in the priority order are broken randomly, so only the shape is
        # deterministic: nothing from the known domain, two of the three new URLs.
        self.assertEqual(len(fetched), 2)
        self.assertTrue(all(url.startswith("http://new.test/") for url in fetched))


if __name__ == "__main__":
    unittest.main()
