from __future__ import annotations

import unittest
from datetime import datetime, timezone

from cc_links.outreach_score import (
    content_component,
    content_component_v2,
    discovery_component_v2,
    freshness_component,
    score_page,
)


class OutreachScoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 7, 30, tzinfo=timezone.utc)

    def test_content_rewards_actionable_signals(self) -> None:
        score, reasons = content_component(
            {
                "invitation_phrases": '["write for us"]',
                "guideline_signals": '["word count"]',
                "risk_flags": "[]",
                "submission_form_count": 1,
                "contact_link_count": 1,
                "email_link_count": 1,
                "form_count": 1,
                "word_count": 900,
                "engine_name": "WordPress",
                "meta_robots": "",
            }
        )
        self.assertEqual(score, 100)
        self.assertIn("submission_form", reasons)

    def test_warc_lastmod_is_confidence_shrunk(self) -> None:
        raw, confidence, effective, _ = freshness_component(
            "2026-07-01T00:00:00+00:00",
            "warc_html_or_header",
            now=self.now,
        )
        self.assertEqual(raw, 100)
        self.assertEqual(confidence, 0.45)
        self.assertEqual(effective, 72.5)

    def test_missing_lastmod_is_neutral_not_fresh(self) -> None:
        raw, confidence, effective, reasons = freshness_component(
            None, None, now=self.now
        )
        self.assertEqual((raw, confidence, effective), (50, 0, 50))
        self.assertEqual(reasons, ["freshness_unknown"])

    def test_rejected_live_result_caps_combined_score(self) -> None:
        components = score_page(
            {
                "invitation_phrases": '["write for us"]',
                "guideline_signals": '["word count"]',
                "risk_flags": "[]",
                "submission_form_count": 1,
                "contact_link_count": 1,
                "email_link_count": 1,
                "form_count": 1,
                "word_count": 900,
                "engine_name": "WordPress",
                "meta_robots": "",
                "best_lastmod": "2026-07-01",
                "best_lastmod_source": "sitemap_exact",
            },
            discovery_score=100,
            qualification_score=100,
            live_outcome="rejected",
            now=self.now,
        )
        self.assertEqual(components.combined, 35)
        self.assertEqual(components.band, "low")

    def test_v2_ignores_weak_challenge_on_editorial_page(self) -> None:
        score, reasons = content_component_v2(
            {
                "title": "Write for us",
                "h1": "Contributor guidelines",
                "invitation_phrases": '["write for us"]',
                "guideline_signals": '["word count"]',
                "risk_flags": '["anti_bot_challenge"]',
                "submission_form_count": 0,
                "contact_link_count": 1,
                "email_link_count": 0,
                "form_count": 0,
                "word_count": 800,
                "engine_name": "WordPress",
                "meta_robots": "",
            }
        )
        self.assertGreaterEqual(score, 80)
        self.assertIn("challenge_signal_ignored_as_weak", reasons)

    def test_v2_recognizes_strong_spanish_editorial_title(self) -> None:
        score, reasons = content_component_v2(
            {
                "title": "Publica con nosotros",
                "h1": "",
                "invitation_phrases": "[]",
                "guideline_signals": "[]",
                "risk_flags": "[]",
                "submission_form_count": 0,
                "contact_link_count": 0,
                "email_link_count": 0,
                "form_count": 0,
                "word_count": 300,
                "engine_name": None,
                "meta_robots": "",
            }
        )
        self.assertEqual(score, 38)
        self.assertIn("multilingual_editorial_title", reasons)

    def test_v2_does_not_promote_broad_collaboration_title(self) -> None:
        score, reasons = content_component_v2(
            {
                "title": "Colabora con nosotros",
                "h1": "",
                "invitation_phrases": "[]",
                "guideline_signals": "[]",
                "risk_flags": "[]",
                "submission_form_count": 0,
                "contact_link_count": 0,
                "email_link_count": 0,
                "form_count": 0,
                "word_count": 300,
                "engine_name": None,
                "meta_robots": "",
            }
        )
        self.assertEqual(score, 18)
        self.assertNotIn("multilingual_editorial_title", reasons)

    def test_v2_discovery_rescales_narrow_registry_range(self) -> None:
        self.assertEqual(
            discovery_component_v2(pattern_weight=88, path_specificity=12),
            62,
        )
        self.assertEqual(
            discovery_component_v2(pattern_weight=100, path_specificity=12),
            86,
        )


if __name__ == "__main__":
    unittest.main()
