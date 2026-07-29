import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from cc_links.outreach import (
    OutreachPattern,
    OutreachPatternError,
    best_outreach_match,
    compile_discovery_regex,
    load_outreach_patterns,
    match_outreach_path,
    pattern_registry_digest,
)


class OutreachPatternTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.patterns = load_outreach_patterns()

    def test_registry_has_pilot_languages(self):
        self.assertTrue(
            {"en", "es", "pt"}.issubset({pattern.language for pattern in self.patterns})
        )

    def test_exact_segment_and_extension_match(self):
        cases = [
            ("https://example.com/write-for-us/", "en.write_for_us"),
            (
                "https://example.com/es/escribe-para-nosotros.html",
                "es.escribe_para_nosotros",
            ),
            ("https://example.com/escreva-conosco.php", "pt.escreva_para_nos"),
        ]
        for url, expected in cases:
            with self.subTest(url=url):
                self.assertEqual(
                    best_outreach_match(url, self.patterns).pattern_id, expected
                )

    def test_segment_boundaries_reject_lexical_noise(self):
        cases = [
            "https://example.com/not-write-for-us/",
            "https://example.com/write-for-useless/",
            "https://example.com/archive/escribe-para-nosotros-ayer/",
            "https://example.com/?next=/write-for-us",
        ]
        for url in cases:
            with self.subTest(url=url):
                self.assertEqual(match_outreach_path(url, self.patterns), [])

    def test_query_and_fragment_are_not_part_of_path_matching(self):
        self.assertEqual(
            best_outreach_match(
                "/write-for-us?utm_source=x#top", self.patterns
            ).pattern_id,
            "en.write_for_us",
        )

    def test_best_match_uses_weight_then_specificity(self):
        patterns = (
            OutreachPattern("en.low", "en", "path_segment", ("write-for-us",), 70),
            OutreachPattern("en.high", "en", "path_segment", ("write-for-us",), 90),
        )
        self.assertEqual(
            best_outreach_match("/write-for-us", patterns).pattern_id, "en.high"
        )

    def test_language_metadata_resolves_shared_literal(self):
        match = best_outreach_match(
            "/guia-para-autores", self.patterns, content_languages="por"
        )
        self.assertEqual(match.pattern_id, "pt.guia_autores")

    def test_compiled_regex_matches_python_boundary_behavior(self):
        regex = compile_discovery_regex(self.patterns)
        self.assertIn("write\\-for\\-us", regex)
        self.assertNotIn("(?P<", regex)
        connection = duckdb.connect()
        try:
            for path, expected in (
                ("/write-for-us/", True),
                ("/es/escribe-para-nosotros.html", True),
                ("/write-for-useless/", False),
                ("/not-write-for-us/", False),
            ):
                with self.subTest(path=path):
                    actual = connection.execute(
                        "SELECT REGEXP_MATCHES(LOWER(?), ?)", [path, regex]
                    ).fetchone()[0]
                    self.assertEqual(actual, expected)
        finally:
            connection.close()

    def test_digest_is_deterministic(self):
        self.assertEqual(
            pattern_registry_digest(self.patterns),
            pattern_registry_digest(load_outreach_patterns()),
        )

    def test_duplicate_ids_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "patterns.json"
            pattern = {
                "id": "en.test",
                "language": "en",
                "scope": "path_segment",
                "expressions": ["write-here"],
                "weight": 80,
            }
            path.write_text(
                json.dumps({"schema_version": 1, "patterns": [pattern, pattern]}),
                encoding="utf-8",
            )
            with self.assertRaises(OutreachPatternError):
                load_outreach_patterns(path)


if __name__ == "__main__":
    unittest.main()
