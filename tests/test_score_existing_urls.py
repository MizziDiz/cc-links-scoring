import unittest

from score_existing_urls import build_url_terms, score_url


class ExistingUrlScoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.terms = build_url_terms()

    def test_confirmed_matching_target_scores_high(self):
        score = score_url("https://example.com/wp-comments-post.php", "Blog Comment", 120,
                          self.terms)
        self.assertEqual(score, 100)

    def test_url_only_candidate_scores_for_review(self):
        score = score_url("https://example.com/guestbook", None, 0, self.terms)
        self.assertEqual(score, 55)

    def test_generic_cms_page_stays_low(self):
        score = score_url("https://example.com/about", "CMS", 30, self.terms)
        self.assertEqual(score, 23)


if __name__ == "__main__":
    unittest.main()
