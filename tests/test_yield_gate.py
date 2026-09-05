import os
import sqlite3
import tempfile
import unittest

from cc_links.db import init_db, mark_url_processed, upsert_candidate
from cc_links.yield_gate import (DomainGate, YieldGate, apply_gates,
                                 collect_pattern_yield, group_key,
                                 load_known_domains, load_yield_profile,
                                 save_yield_profile)


def _record(url, pattern_id, tier, domain):
    return {"url": url, "pattern_id": pattern_id, "discovery_tier": tier,
            "url_host_registered_domain": domain}


class YieldProfileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "p.db")
        conn = init_db(self.db)
        # guestbook:0 -> 30 decisions, 3 stored (10%)
        for i in range(30):
            mark_url_processed(conn, f"http://g{i}.test/gb", f"http://g{i}.test/gb",
                               "CC-MAIN-2026-21", "stored" if i < 3 else "unmatched",
                               pattern_id="guestbook:0", discovery_tier=0,
                               registered_domain=f"g{i}.test")
        # phpbb:0 -> 25 decisions, 24 stored (96%)
        for i in range(25):
            mark_url_processed(conn, f"http://f{i}.test/t", f"http://f{i}.test/t",
                               "CC-MAIN-2026-21", "stored" if i < 24 else "unmatched",
                               pattern_id="phpbb_forum:0", discovery_tier=0,
                               registered_domain=f"f{i}.test")
        # rare:1 -> 5 decisions, 0 stored (too few to judge)
        for i in range(5):
            mark_url_processed(conn, f"http://r{i}.test/u", f"http://r{i}.test/u",
                               "CC-MAIN-2026-21", "unmatched",
                               pattern_id="broad:/user/", discovery_tier=1,
                               registered_domain=f"r{i}.test")
        # domain_cap rows must not count as decisions
        mark_url_processed(conn, "http://c.test/x", "http://c.test/x",
                           "CC-MAIN-2026-21", "domain_cap",
                           pattern_id="guestbook:0", discovery_tier=0,
                           registered_domain="c.test")
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def test_collect_pattern_yield_counts_only_fetch_decisions(self):
        profile = collect_pattern_yield(self.db)
        gb = profile["groups"][group_key("guestbook:0", 0)]
        self.assertEqual(gb["decisions"], 30)
        self.assertEqual(gb["stored"], 3)
        self.assertAlmostEqual(gb["yield"], 0.1)
        self.assertEqual(gb["stored_domains"], 3)
        self.assertEqual(profile["groups"][group_key("phpbb_forum:0", 0)]["decisions"], 25)

    def test_profile_roundtrip(self):
        profile = collect_pattern_yield(self.db, since="2000-01-01")
        path = os.path.join(self.tmp.name, "yield.json")
        save_yield_profile(profile, path)
        loaded = load_yield_profile(path)
        self.assertEqual(loaded["groups"].keys(), profile["groups"].keys())
        self.assertEqual(loaded["since"], "2000-01-01")

    def test_since_is_ignored_when_processed_at_is_missing(self):
        legacy = os.path.join(self.tmp.name, "legacy.db")
        conn = sqlite3.connect(legacy)
        conn.execute("""CREATE TABLE processed_urls (
            normalized_url TEXT PRIMARY KEY, url TEXT, crawl TEXT, outcome TEXT,
            score INTEGER, registered_domain TEXT, discovery_tier INTEGER,
            pattern_id TEXT)""")
        conn.executemany(
            "INSERT INTO processed_urls VALUES (?, ?, 'c', ?, 60, ?, 0, 'guestbook:0')",
            [(f"http://l{i}.test/", f"http://l{i}.test/",
              "stored" if i < 2 else "unmatched", f"l{i}.test") for i in range(25)])
        conn.commit()
        conn.close()
        profile = collect_pattern_yield(legacy, since="2026-07-25")
        self.assertIsNone(profile["since"])
        self.assertIn("processed_at", profile["note"])
        self.assertEqual(profile["groups"][group_key("guestbook:0", 0)]["decisions"], 25)

    def test_database_is_opened_read_only(self):
        # A missing file must not be created by the profile query.
        missing = os.path.join(self.tmp.name, "absent.db")
        with self.assertRaises(sqlite3.OperationalError):
            collect_pattern_yield(missing)
        self.assertFalse(os.path.exists(missing))

    def test_gate_skips_low_yield_but_explores_unknown(self):
        profile = collect_pattern_yield(self.db)
        gate = YieldGate(profile, minimum_yield=0.2, minimum_decisions=20,
                         explore_share=0.0)
        self.assertFalse(gate.decide(_record("http://a.test/gb", "guestbook:0", 0, "a.test")))
        self.assertTrue(gate.decide(_record("http://b.test/t", "phpbb_forum:0", 0, "b.test")))
        # Too few decisions: explore.
        self.assertTrue(gate.decide(_record("http://c.test/u", "broad:/user/", 1, "c.test")))
        # Never seen: explore.
        self.assertTrue(gate.decide(_record("http://d.test/n", "new:0", 0, "d.test")))
        self.assertEqual(gate.stats, {"allowed": 1, "explored": 0, "skipped": 1, "unknown": 2})
        self.assertEqual([g["pattern_id"] for g in gate.low_yield_groups()], ["guestbook:0"])

    def test_explore_share_is_deterministic_and_bounded(self):
        profile = collect_pattern_yield(self.db)
        urls = [f"http://site{i}.test/guestbook" for i in range(2000)]
        first = YieldGate(profile, 0.2, 20, explore_share=0.05)
        second = YieldGate(profile, 0.2, 20, explore_share=0.05)
        a = [first.decide(_record(u, "guestbook:0", 0, "x")) for u in urls]
        b = [second.decide(_record(u, "guestbook:0", 0, "x")) for u in urls]
        self.assertEqual(a, b)
        explored = sum(a)
        self.assertGreater(explored, 40)
        self.assertLess(explored, 160)
        self.assertEqual(first.stats["explored"], explored)

    def test_gate_without_profile_allows_everything(self):
        gate = YieldGate(None, 0.2)
        self.assertTrue(gate.decide(_record("http://a.test/gb", "guestbook:0", 0, "a")))
        self.assertEqual(gate.low_yield_groups(), [])


class DomainGateTests(unittest.TestCase):
    def test_known_domains_and_per_run_cap(self):
        gate = DomainGate({"known.test"}, skip_known=True, per_run_cap=2)
        self.assertFalse(gate.decide(_record("http://known.test/1", "p", 0, "known.test")))
        self.assertTrue(gate.decide(_record("http://new.test/1", "p", 0, "new.test")))
        self.assertTrue(gate.decide(_record("http://new.test/2", "p", 0, "new.test")))
        self.assertFalse(gate.decide(_record("http://new.test/3", "p", 0, "new.test")))
        # Unknown domain field: never blocks.
        self.assertTrue(gate.decide(_record("http://x/", "p", 0, None)))
        self.assertEqual(gate.stats, {"allowed": 3, "skipped_known": 1, "skipped_cap": 1})

    def test_cap_off_and_known_not_skipped_by_default(self):
        gate = DomainGate({"known.test"})
        for i in range(50):
            self.assertTrue(gate.decide(_record(f"http://known.test/{i}", "p", 0, "known.test")))

    def test_load_known_domains(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = init_db(os.path.join(tmp, "k.db"))
            upsert_candidate(
                conn, normalized_url="http://a.test/x", url="http://a.test/x",
                domain="a.test", registered_domain="A.test", crawl="c", tld="test",
                country=None, bucket=None, family="forum", platform=None, score=80,
                matched_signals="[]", warc_filename="w", warc_offset=0, warc_length=1)
            conn.commit()
            self.assertEqual(load_known_domains(conn), {"a.test"})
            conn.close()


class ApplyGatesTests(unittest.TestCase):
    def test_all_gates_must_accept_and_order_is_kept(self):
        records = [
            _record("http://a.test/1", "good:0", 0, "a.test"),
            _record("http://a.test/2", "good:0", 0, "a.test"),
            _record("http://b.test/1", "good:0", 0, "b.test"),
        ]
        out = list(apply_gates(records, [YieldGate(None, 0.2),
                                         DomainGate(per_run_cap=1)]))
        self.assertEqual([r["url"] for r in out], ["http://a.test/1", "http://b.test/1"])


if __name__ == "__main__":
    unittest.main()
