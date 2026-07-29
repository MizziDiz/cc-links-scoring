import unittest
from pathlib import Path

from cc_links.countries import country_name, load_category_map


ROOT = Path(__file__).resolve().parents[1]
REQUESTED_TLDS = frozenset(
    """
    ad ae af al am ao ar at au az ba bd be bf bg bh bi bj bn bo br bt bw by bz
    ca cd cg ch ci cl cm cn cr cu cv cy cz de dj dk do dz ec ee eg eh er es et
    fi fr ga ge gh gm gn gr gt gw gy hk hn hr ht hu id ie il in iq ir is it jm
    jo jp ke kg kh km kp kr kw kz la lb lk lr ls lt lu lv ly ma mc md mg mk ml
    mm mn mo mp mr mt mu mv mw mx my mz na ne ng ni nl no np nz om pa pe pg ph
    pk pl pr ps pt py qa re ro rs ru rw sa sc sd se sg si sk sl sm sn so sr ss
    sv sy sz td tg th tj tk tl tm tn tr tt tw tz ua ug uk us uy uz va ve vn ye
    yt za zm zw
    """.split()
)


class CountryTaxonomyTests(unittest.TestCase):
    def test_requested_geo_set_is_covered(self):
        _, tld_to_category = load_category_map(str(ROOT / "categories.json"))

        self.assertEqual(REQUESTED_TLDS - set(tld_to_category), set())

    def test_requested_oceania_tlds_are_discoverable(self):
        categories, tld_to_category = load_category_map(
            str(ROOT / "categories.json")
        )
        oceania = next(
            name for name in categories if "Австралия" in name
        )

        for tld in ("mp", "pg", "tk"):
            with self.subTest(tld=tld):
                self.assertEqual(tld_to_category[tld], oceania)
                self.assertNotEqual(country_name(tld), tld.upper())


if __name__ == "__main__":
    unittest.main()
