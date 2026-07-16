import unittest
from unittest.mock import patch

from validate_sample import validate


class FakeResponse:
    status_code = 200
    url = "https://example.com/viewtopic.php?t=1"
    text = "<footer>Powered by phpBB</footer>"
    headers = {"content-type": "text/html; charset=utf-8"}


class ValidateSampleTests(unittest.TestCase):
    @patch("validate_sample.fetch_url", return_value=FakeResponse())
    def test_live_page_is_reclassified(self, _get):
        row = validate({"url": "http://example.com/x"}, timeout=1, minimum_score=50)
        self.assertEqual(row["live_http_status"], 200)
        self.assertEqual(row["live_family"], "forum")
        self.assertGreaterEqual(row["live_score"], 50)


if __name__ == "__main__":
    unittest.main()
