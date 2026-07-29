import tempfile
import unittest
from pathlib import Path

from mine_engine_signatures import mine_engine_directory, parse_engine_file


class EngineSignatureMiningTests(unittest.TestCase):
    def test_extracts_positive_fields_and_drops_negative_signatures(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "Example.ini"
            source.write_text(
                "\n".join([
                    "engine type=Guestbook",
                    "page must have1=Powered by Example|!registration disabled",
                    "url must have1=*/guestbook.php*",
                    'search term="Powered by Example"|"Sign Guestbook"',
                    "submit failed=Wrong captcha",
                ]),
                encoding="utf-8",
            )
            parsed = parse_engine_file(source)
            self.assertEqual(parsed["engine_type"], "Guestbook")
            self.assertEqual(parsed["html_signals"], ["Powered by Example"])
            self.assertEqual(parsed["url_signals"], ["*/guestbook.php*"])
            self.assertEqual(
                parsed["search_footprints"],
                ["Powered by Example", "Sign Guestbook"],
            )

    def test_directory_report_counts_ini_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "A.ini").write_text(
                "engine type=Forum\npage must have=Powered by A\n",
                encoding="utf-8",
            )
            (root / "ignored.txt").write_text("not an engine", encoding="utf-8")
            report = mine_engine_directory(str(root))
            self.assertEqual(report["ini_files"], 1)
            self.assertEqual(report["engines_with_signatures"], 1)
            self.assertEqual(report["engine_types"], {"Forum": 1})


if __name__ == "__main__":
    unittest.main()
