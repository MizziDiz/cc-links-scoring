import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cc_links.outreach_terms import TERMS_SCHEMA
from cc_links.outreach_value import (
    CostConfig,
    aggregate_domain_metrics,
    build_value_scores,
    domain_strength_components,
    evaluate_page_value,
    import_domain_metrics,
    placement_quality_score,
)


class ValueComponentTests(unittest.TestCase):
    def test_domain_strength_uses_flow_balance_and_traffic(self) -> None:
        metrics = aggregate_domain_metrics(
            [
                {
                    "domain_rating": 70,
                    "domain_authority": None,
                    "authority_score": None,
                    "trust_flow": 40,
                    "citation_flow": 50,
                    "organic_traffic": 100_000,
                    "referring_domains": 1_000,
                    "spam_score": 10,
                    "topical_relevance": 80,
                    "geo_relevance": 60,
                }
            ]
        )
        authority, flow, traffic, relevance, strength, coverage, reasons = (
            domain_strength_components(metrics)
        )

        self.assertEqual(authority, 70)
        self.assertEqual(flow, 52)
        self.assertGreater(traffic, 80)
        self.assertEqual(relevance, 70)
        self.assertIsNotNone(strength)
        self.assertEqual(coverage, 5)
        self.assertIn("flow_balance_included", reasons)
        self.assertIn("spam_penalty", reasons)

    def test_placement_quality_rewards_dofollow_body_and_permanence(self) -> None:
        score, reasons = placement_quality_score(
            {
                "link_attributes": '["dofollow"]',
                "placement_locations": '["editorial_body"]',
                "permanence": "permanent",
            }
        )

        self.assertEqual(score, 100)
        self.assertIn("link:dofollow", reasons)

    def test_missing_metrics_remains_explicit(self) -> None:
        result = evaluate_page_value(
            page_quality_score=80,
            terms={
                "promise_level": "conditional_review",
                "promise_probability": 0.5,
                "link_attributes": "[]",
                "placement_locations": "[]",
                "permanence": "unspecified",
                "content_responsibility": "author_provides",
            },
            metrics={},
            normalized_price=100,
            cost_config=CostConfig(contact_cost=5, content_cost=40),
        )

        self.assertIsNone(result.domain_strength)
        self.assertIsNone(result.expected_effectiveness)
        self.assertEqual(result.expected_total_cost, 150)
        self.assertEqual(result.status, "missing_metrics")


class ValuePipelineTests(unittest.TestCase):
    def test_import_and_build_value_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scores = root / "scores.db"
            terms = root / "terms.db"
            output = root / "value.db"
            metrics_csv = root / "metrics.csv"

            score_connection = sqlite3.connect(scores)
            score_connection.execute(
                """
                CREATE TABLE page_scores (
                    url TEXT,registered_domain TEXT,score_band TEXT,
                    combined_score REAL
                )
                """
            )
            score_connection.execute(
                "INSERT INTO page_scores VALUES (?,?,?,?)",
                ("https://example.com/write-for-us", "example.com", "high", 82),
            )
            score_connection.commit()
            score_connection.close()

            terms_connection = sqlite3.connect(terms)
            terms_connection.executescript(TERMS_SCHEMA)
            terms_connection.execute(
                """
                INSERT INTO placement_terms (
                    url,registered_domain,source,analyzed_at,fetch_status,
                    placement_types,promise_level,promise_probability,
                    commercial_model,price_status,price_min,price_max,currency,
                    link_attributes,placement_locations,permanence,
                    content_responsibility,evidence_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "https://example.com/write-for-us",
                    "example.com",
                    "warc",
                    "2026-07-30T00:00:00+00:00",
                    "ok",
                    '["guest_post"]',
                    "guaranteed",
                    0.85,
                    "paid",
                    "advertised",
                    100,
                    100,
                    "USD",
                    '["dofollow"]',
                    '["editorial_body"]',
                    "permanent",
                    "author_provides",
                    "[]",
                ),
            )
            terms_connection.commit()
            terms_connection.close()

            with metrics_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "domain",
                        "provider",
                        "DR",
                        "Trust Flow",
                        "Citation Flow",
                        "Organic Traffic",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "domain": "example.com",
                        "provider": "fixture",
                        "DR": "70",
                        "Trust Flow": "40",
                        "Citation Flow": "50",
                        "Organic Traffic": "100K",
                    }
                )
            imported = import_domain_metrics(metrics_csv, output)
            self.assertEqual(imported["imported"], 1)

            report = build_value_scores(
                scores_db=scores,
                terms_db=terms,
                out_db=output,
                cost_config=CostConfig(contact_cost=5, content_cost=40),
            )
            self.assertEqual(report["pages"], 1)
            self.assertEqual(report["quick_check"], "ok")

            connection = sqlite3.connect(output)
            row = connection.execute(
                """
                SELECT domain_strength_score,placement_quality_score,
                       expected_total_cost,evaluation_status,value_reasons
                FROM page_values
                """
            ).fetchone()
            connection.close()
            self.assertIsNotNone(row[0])
            self.assertEqual(row[1], 100)
            self.assertGreater(row[2], 140)
            self.assertEqual(row[3], "complete")
            self.assertIn("promise:guaranteed", json.loads(row[4]))


if __name__ == "__main__":
    unittest.main()
