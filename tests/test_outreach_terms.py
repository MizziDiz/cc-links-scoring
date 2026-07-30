import unittest

from cc_links.outreach_terms import extract_placement_terms


class PlacementTermsTests(unittest.TestCase):
    def test_extracts_guaranteed_paid_contextual_placement(self) -> None:
        result = extract_placement_terms(
            """
            <html><body>
              <h1>Sponsored guest post</h1>
              <p>We will publish your article on our blog within 3-5 days.</p>
              <p>The publication fee is USD 120. You must provide the article.</p>
              <p>Your permanent dofollow contextual link stays within the article.</p>
            </body></html>
            """
        )

        self.assertEqual(result["promise_level"], "guaranteed")
        self.assertEqual(result["promise_probability"], 0.85)
        self.assertEqual(result["commercial_model"], "paid")
        self.assertEqual(result["price_min"], 120)
        self.assertEqual(result["price_max"], 120)
        self.assertEqual(result["currency"], "USD")
        self.assertIn("guest_post", result["placement_types"])
        self.assertIn("sponsored_post", result["placement_types"])
        self.assertIn("dofollow", result["link_attributes"])
        self.assertIn("editorial_body", result["placement_locations"])
        self.assertEqual(result["permanence"], "permanent")
        self.assertEqual(result["content_responsibility"], "author_provides")
        self.assertEqual(result["turnaround_days_max"], 5)
        self.assertTrue(result["evidence_json"])

    def test_conditional_policy_overrides_guarantee_wording(self) -> None:
        result = extract_placement_terms(
            """
            <p>Submit your article. Submissions are reviewed and publication
            is not guaranteed. If accepted, the editorial fee is €75.</p>
            <p>Links are nofollow and the article stays for 12 months.</p>
            """
        )

        self.assertEqual(result["promise_level"], "conditional_review")
        self.assertEqual(result["price_min"], 75)
        self.assertEqual(result["currency"], "EUR")
        self.assertIn("nofollow", result["link_attributes"])
        self.assertEqual(result["permanence"], "temporary")

    def test_broad_collaboration_does_not_imply_editorial_placement(self) -> None:
        result = extract_placement_terms(
            "<h1>Colabora con nosotros</h1><p>Buscamos voluntarios para eventos.</p>"
        )

        self.assertEqual(result["promise_level"], "unclear")
        self.assertEqual(result["placement_types"], [])
        self.assertEqual(result["price_status"], "unknown")

    def test_extracts_free_submission_without_inventing_currency(self) -> None:
        result = extract_placement_terms(
            "<h1>Write for us</h1><p>Free guest post publication. No publication fee.</p>"
        )

        self.assertEqual(result["promise_level"], "open_submission")
        self.assertEqual(result["commercial_model"], "free")
        self.assertEqual(result["price_status"], "free")
        self.assertIsNone(result["currency"])


if __name__ == "__main__":
    unittest.main()
