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

    def test_approval_or_selection_makes_future_publication_conditional(self) -> None:
        for body in (
            "Once approved, your article will be published with a byline.",
            "If your content meets our requirements, we will publish your article.",
            "Writers are notified when their article was chosen and will be published.",
            "Una vez aprobado, el articulo sera publicado.",
            "Apos a aprovacao, o artigo sera publicado.",
        ):
            with self.subTest(body=body):
                result = extract_placement_terms(f"<p>{body}</p>")
                self.assertEqual(result["promise_level"], "conditional_review")

    def test_closed_submission_overrides_open_invitation(self) -> None:
        result = extract_placement_terms(
            "<h1>Write for us</h1><p>We are no longer accepting guest post "
            "submissions.</p>"
        )

        self.assertEqual(result["promise_level"], "closed")
        self.assertEqual(result["promise_probability"], 0.01)

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

    def test_ignores_market_prices_donations_and_contributor_payouts(self) -> None:
        for body in (
            "Write for us. Bitcoin is $64,372. Submit your article.",
            "Submit an article. Donations of $10,000 need a declaration.",
            "Become a contributor and earn $99 for each approved profile.",
        ):
            with self.subTest(body=body):
                result = extract_placement_terms(f"<p>{body}</p>")
                self.assertEqual(result["price_status"], "unknown")
                self.assertIsNone(result["price_min"])

    def test_marks_free_and_paid_policy_as_conflicting(self) -> None:
        result = extract_placement_terms(
            """
            <p>Free guest post submissions are welcome.</p>
            <p>We also offer paid guest post opportunities.</p>
            """
        )

        self.assertEqual(result["price_status"], "conflicting")
        self.assertEqual(result["commercial_model"], "mixed")

    def test_navigation_words_do_not_imply_placement_location(self) -> None:
        result = extract_placement_terms(
            """
            <nav>Home Page | Social Media | Redes sociales</nav>
            <h1>Write for us</h1>
            """
        )

        self.assertEqual(result["placement_locations"], [])

    def test_additional_link_fee_is_not_the_base_publication_price(self) -> None:
        result = extract_placement_terms(
            """
            <p>Pricing Details: Standard article $9.99 per article.
            Additional links: $7.99 each.</p>
            """
        )

        self.assertEqual(result["price_min"], 9.99)
        self.assertEqual(result["price_max"], 9.99)

    def test_price_does_not_consume_following_word_count(self) -> None:
        result = extract_placement_terms(
            "<p>Guest post packages start at $170 800–1,200 words.</p>"
        )

        self.assertEqual(result["price_status"], "advertised")
        self.assertEqual(result["price_min"], 170)
        self.assertEqual(result["price_max"], 170)

    def test_labeled_post_options_are_direct_prices(self) -> None:
        result = extract_placement_terms(
            "<div><p>Single Post - $30</p><p>Multiple Posts (up to 5): $99</p></div>"
        )

        self.assertEqual(result["price_status"], "advertised")
        self.assertEqual(result["price_min"], 30)
        self.assertEqual(result["price_max"], 99)

    def test_weak_price_context_requires_review(self) -> None:
        result = extract_placement_terms(
            "<p>Guest post topic: tell us how you sold a painting for $50,000.</p>"
        )

        self.assertEqual(result["price_status"], "advertised_review")
        self.assertEqual(result["price_min"], 50_000)

    def test_recurring_plan_is_not_used_as_placement_fee(self) -> None:
        result = extract_placement_terms(
            """
            <div>Guest Post Submission $399 One-time</div>
            <div>Monthly contributor plans: $799 per month</div>
            """
        )

        self.assertEqual(result["price_status"], "advertised")
        self.assertEqual(result["price_min"], 399)
        self.assertEqual(result["price_max"], 399)

    def test_mixed_currency_prices_are_not_silently_compared(self) -> None:
        result = extract_placement_terms(
            "<p>Guest post publication fee: USD 50 or EUR 45 per article.</p>"
        )

        self.assertEqual(result["price_status"], "advertised_mixed_currency")
        self.assertEqual(result["currency"], "MIXED")
        self.assertIsNone(result["price_min"])


if __name__ == "__main__":
    unittest.main()
