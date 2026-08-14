import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cc_links.outreach_placements import (
    PLACEMENT_SCHEMA,
    _refresh_evaluations,
    _refresh_services,
    _save_page,
    extract_placement_graph,
    import_model_reviews,
    import_verified_placements,
    write_impact_template,
    write_placement_outputs,
)


class PlacementGraphExtractionTests(unittest.TestCase):
    def test_classifies_self_hosted_publisher(self) -> None:
        result = extract_placement_graph(
            """
            <html><body><main>
              <h1>Write for us</h1>
              <p>We publish your article on our blog after editorial review.</p>
              <a href="/stories/example">Recent article</a>
            </main></body></html>
            """,
            page_url="https://example.co.uk/write-for-us",
            registered_domain="example.co.uk",
            terms={"promise_level": "conditional_review", "placement_types": "[]"},
        )

        self.assertEqual(result["placement_model"], "self_hosted")
        self.assertGreaterEqual(result["model_confidence"], 0.7)
        self.assertEqual(result["links"][0]["link_role"], "self_hosted")
        self.assertEqual(
            result["links"][0]["destination_registered_domain"], "example.co.uk"
        )

    def test_classifies_external_service_and_explicit_example(self) -> None:
        result = extract_placement_graph(
            """
            <html><body><main>
              <h1>Guest posting service</h1>
              <p>We place your links on publisher sites in our network.</p>
              <p>Recent placement example:
                <a rel="nofollow" href="https://publisher.example/article?id=2#top">
                  Published on Publisher
                </a>
              </p>
            </main></body></html>
            """,
            page_url="https://agency.test/services",
            registered_domain="agency.test",
        )

        self.assertEqual(result["placement_model"], "external_service")
        example = result["links"][0]
        self.assertEqual(example["link_role"], "placement_example")
        self.assertEqual(example["rel"], "nofollow")
        self.assertNotIn("#top", example["destination_url"])

    def test_marks_service_with_both_models_as_hybrid(self) -> None:
        result = extract_placement_graph(
            """
            <h1>Write for us</h1>
            <p>We publish your article on our blog.</p>
            <p>We place your links across thousands of sites.</p>
            """,
            page_url="https://hybrid.test/write",
            registered_domain="hybrid.test",
            terms={
                "promise_level": "open_submission",
                "placement_types": ["guest_post"],
            },
        )

        self.assertEqual(result["placement_model"], "hybrid")

    def test_requires_operational_external_service_language(self) -> None:
        for body in (
            "You confirm rights to publish images on our network of websites.",
            "Keywords: paid guest post | link building service | niche edit.",
            "Powered by SEO Link Building Service.",
        ):
            with self.subTest(body=body):
                result = extract_placement_graph(
                    f"<h1>Write for us</h1><p>{body}</p>",
                    page_url="https://publisher.test/write",
                    registered_domain="publisher.test",
                )
                self.assertEqual(result["placement_model"], "self_hosted")

    def test_detects_publisher_inventory_workflow(self) -> None:
        result = extract_placement_graph(
            """
            <h1>Guest posting marketplace</h1>
            <p>Find Publisher: search for a proper site in our list using filters.</p>
            <p>Choose a sample from our inventory:
              <a href="https://publisher.test/example">Recent placement</a>
            </p>
            """,
            page_url="https://broker.test/marketplace",
            registered_domain="broker.test",
        )

        self.assertEqual(result["placement_model"], "external_service")
        self.assertEqual(result["links"][0]["link_role"], "placement_example")

    def test_buy_guest_post_on_write_for_us_is_not_external_network_proof(
        self,
    ) -> None:
        result = extract_placement_graph(
            """
            <h1>Write for us</h1>
            <p>Submit your article to us for editorial review.</p>
            <aside><a href="https://seller.test/order">Buy guest posts</a></aside>
            """,
            page_url="https://publisher.test/write-for-us",
            registered_domain="publisher.test",
        )

        self.assertEqual(result["placement_model"], "self_hosted")
        self.assertEqual(result["links"][0]["link_role"], "reference")

    def test_guest_post_service_wording_is_not_external_network_proof(self) -> None:
        result = extract_placement_graph(
            """
            <h1>Write for us</h1>
            <p>Submit your article to us. We offer our guest posting service free.</p>
            """,
            page_url="https://publisher.test/write-for-us",
            registered_domain="publisher.test",
        )

        self.assertEqual(result["placement_model"], "self_hosted")
        self.assertIn("commercial_placement_language", result["model_reasons"])

    def test_generic_site_context_does_not_create_inventory_placement(self) -> None:
        result = extract_placement_graph(
            """
            <h1>Guest posting service</h1>
            <p>We are a guest posting service provider.</p>
            <div>This site uses an external
              <a href="https://docs.test/reference">documentation example</a>.
            </div>
            """,
            page_url="https://agency.test/service",
            registered_domain="agency.test",
        )

        self.assertEqual(result["placement_model"], "external_service")
        self.assertEqual(result["links"][0]["link_role"], "reference")

    def test_evidence_records_exact_matched_expression(self) -> None:
        result = extract_placement_graph(
            "<p>We are a reputed link building service provider.</p>",
            page_url="https://agency.test/service",
            registered_domain="agency.test",
        )

        self.assertEqual(
            result["model_evidence"][0]["matched_expression"],
            "We are a reputed link building service provider",
        )

    def test_does_not_treat_arbitrary_external_reference_as_placement(self) -> None:
        result = extract_placement_graph(
            '<p>Read the <a href="https://docs.example/reference">documentation</a>.</p>',
            page_url="https://service.test/info",
            registered_domain="service.test",
        )

        self.assertEqual(result["placement_model"], "unknown")
        self.assertEqual(result["links"][0]["link_role"], "reference")

    def test_case_study_and_license_links_are_not_examples_without_service_context(
        self,
    ) -> None:
        result = extract_placement_graph(
            """
            <h1>Write for us</h1>
            <p><a href="/internal-case-study">Our product case study</a></p>
            <p>Published under <a href="https://creativecommons.org/licenses/by/4.0/">
              Creative Commons License example</a>.</p>
            """,
            page_url="https://publisher.com.in/write-for-us",
            registered_domain="publisher.com.in",
        )

        roles = [link["link_role"] for link in result["links"]]
        self.assertEqual(roles, ["self_hosted", "reference"])
        self.assertEqual(
            result["links"][0]["destination_registered_domain"], "publisher.com.in"
        )


class ImpactTemplateTests(unittest.TestCase):
    def test_exports_services_waiting_for_placement_examples(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "placements.db"
            report_path = Path(directory) / "report.json"
            export_dir = Path(directory) / "exports"
            connection = sqlite3.connect(db_path)
            connection.executescript(PLACEMENT_SCHEMA)
            connection.execute(
                """INSERT INTO services VALUES (
                    'svc_1','broker.test','https://broker.test','external_service',
                    0.9,'[]',1,0,0,0,'pending_domain_metrics',NULL,'2026-01-01')"""
            )
            connection.execute(
                """INSERT INTO service_pages (
                       url,service_id,registered_domain,crawl,fetch_status,
                       placement_model,model_confidence,external_score,
                       self_hosted_score,model_reasons,model_evidence
                   ) VALUES (
                       'https://broker.test','svc_1','broker.test','CC-TEST','ok',
                       'external_service',0.9,5,0,'[]','[]')"""
            )
            connection.execute(
                """INSERT INTO services VALUES (
                    'svc_2','unknown.test','https://unknown.test','unknown',
                    0.35,'[]',1,1,0,0,'offline_page_evidence_only',NULL,'2026-01-01')"""
            )
            connection.execute(
                """INSERT INTO service_pages (
                       url,service_id,registered_domain,crawl,fetch_status,
                       placement_model,model_confidence,external_score,
                       self_hosted_score,model_reasons,model_evidence,
                       promise_level,promise_probability,price_status,
                       page_quality_score,score_band,best_lastmod
                   ) VALUES (
                       'https://unknown.test/write','svc_2','unknown.test','CC-TEST','ok',
                       'unknown',0.35,0,1.5,'[]','[]','open_submission',0.5,
                       'unknown',80,'high','2026-01-01')"""
            )
            connection.commit()
            connection.close()

            write_placement_outputs(
                db_path, report_path=report_path, export_dir=export_dir
            )

            with (export_dir / "placement_requests.csv").open(
                encoding="utf-8", newline=""
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["registered_domain"], "broker.test")
            self.assertEqual(rows[0]["request_status"], "pending_example_request")
            self.assertEqual(rows[0]["placement_url"], "")
            with (export_dir / "model_review.csv").open(
                encoding="utf-8", newline=""
            ) as handle:
                review_rows = list(csv.DictReader(handle))
            self.assertEqual(len(review_rows), 1)
            self.assertEqual(review_rows[0]["registered_domain"], "unknown.test")
            self.assertEqual(
                review_rows[0]["suggested_action"], "confirm_self_hosted_terms"
            )
            self.assertEqual(review_rows[0]["manual_model"], "")

    def test_imports_manual_model_review_and_preserves_override(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "placements.db"
            input_path = Path(directory) / "model_review.csv"
            report_path = Path(directory) / "report.json"
            export_dir = Path(directory) / "exports"
            connection = sqlite3.connect(db_path)
            connection.executescript(PLACEMENT_SCHEMA)
            connection.execute(
                """INSERT INTO services VALUES (
                    'svc_1','publisher.test','https://publisher.test/write','unknown',
                    0.35,'[]',1,1,0,0,'offline_page_evidence_only',NULL,'2026-01-01')"""
            )
            connection.execute(
                """INSERT INTO service_pages (
                       url,service_id,registered_domain,crawl,fetch_status,
                       placement_model,model_confidence,external_score,
                       self_hosted_score,model_reasons,model_evidence,
                       promise_level,promise_probability,page_quality_score
                   ) VALUES (
                       'https://publisher.test/write','svc_1','publisher.test',
                       'CC-TEST','ok','unknown',0.35,0,1.5,
                       '["insufficient_model_evidence"]','[]',
                       'conditional_review',0.5,80)"""
            )
            connection.commit()
            connection.close()
            with input_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["service_id", "manual_model", "reviewed_at", "notes"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "service_id": "svc_1",
                        "manual_model": "self_hosted",
                        "reviewed_at": "2026-08-14T00:00:00Z",
                        "notes": "archived evidence reviewed",
                    }
                )

            imported = import_model_reviews(db_path, input_path)

            connection = sqlite3.connect(db_path)
            service = connection.execute(
                "SELECT placement_model,model_confidence,model_reasons FROM services"
            ).fetchone()
            review = connection.execute(
                "SELECT manual_model,reviewed_at,notes,length(source_digest) "
                "FROM service_model_reviews"
            ).fetchone()
            evaluation = connection.execute(
                "SELECT placement_model,evaluation_status FROM placement_evaluations"
            ).fetchone()
            connection.close()
            self.assertEqual(imported, 1)
            self.assertEqual(service[0:2], ("self_hosted", 1.0))
            self.assertIn("manual_model_review", service[2])
            self.assertEqual(
                review,
                (
                    "self_hosted",
                    "2026-08-14T00:00:00Z",
                    "archived evidence reviewed",
                    64,
                ),
            )
            self.assertEqual(evaluation, ("self_hosted", "offline_partial_unpriced"))
            report = write_placement_outputs(
                db_path, report_path=report_path, export_dir=export_dir
            )
            with (export_dir / "service_model_reviews.csv").open(
                encoding="utf-8", newline=""
            ) as handle:
                audit_rows = list(csv.DictReader(handle))
            self.assertEqual(report["model_reviews"], 1)
            self.assertEqual(len(audit_rows), 1)
            self.assertEqual(audit_rows[0]["manual_model"], "self_hosted")

    def test_imports_verified_example_and_refreshes_service(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "placements.db"
            input_path = Path(directory) / "verified.csv"
            connection = sqlite3.connect(db_path)
            connection.executescript(PLACEMENT_SCHEMA)
            connection.execute(
                """INSERT INTO services VALUES (
                    'svc_1','broker.test','https://broker.test','external_service',
                    0.9,'[]',1,0,0,0,'pending_domain_metrics',NULL,'2026-01-01')"""
            )
            connection.execute(
                """INSERT INTO service_pages (
                       url,service_id,registered_domain,crawl,fetch_status,
                       placement_model,model_confidence,external_score,
                       self_hosted_score,model_reasons,model_evidence
                   ) VALUES (
                       'https://broker.test','svc_1','broker.test','CC-TEST','ok',
                       'external_service',0.9,5,0,'[]','[]')"""
            )
            connection.commit()
            connection.close()
            with input_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "service_id",
                        "canonical_url",
                        "placement_url",
                        "publisher_registered_domain",
                        "verification_source",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "service_id": "svc_1",
                        "canonical_url": "https://broker.test",
                        "placement_url": "https://publisher.example/post#section",
                        "publisher_registered_domain": "publisher.example",
                        "verification_source": "provider_email",
                    }
                )

            imported = import_verified_placements(db_path, input_path)

            connection = sqlite3.connect(db_path)
            placement = connection.execute(
                "SELECT placement_url,relationship FROM placements"
            ).fetchone()
            service = connection.execute(
                "SELECT example_placement_count,publisher_domain_count FROM services"
            ).fetchone()
            connection.close()
            self.assertEqual(imported, 1)
            self.assertEqual(
                placement,
                ("https://publisher.example/post", "verified_example"),
            )
            self.assertEqual(service, (1, 1))

    def test_only_successful_pages_are_resume_checkpoints(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.executescript(PLACEMENT_SCHEMA)
        connection.execute(
            """INSERT INTO services VALUES (
                'svc_1','example.test','https://example.test','unknown',0,'[]',
                0,0,0,0,'pending_domain_metrics',NULL,'2026-01-01')"""
        )
        base = (
            "https://example.test/ok",
            "svc_1",
            "example.test",
            "CC-TEST",
            None,
            "ok",
            "unknown",
            0,
            0,
            0,
            "[]",
            "[]",
            None,
            None,
            "[]",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        connection.execute(
            "INSERT INTO service_pages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            base,
        )
        failed = list(base)
        failed[0] = "https://example.test/error"
        failed[5] = "error"
        connection.execute(
            "INSERT INTO service_pages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            failed,
        )

        completed = {
            str(row[0])
            for row in connection.execute(
                "SELECT url FROM service_pages WHERE fetch_status='ok'"
            )
        }

        self.assertEqual(completed, {"https://example.test/ok"})
        connection.close()

    def test_persists_page_links_placements_and_service_rollup(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.executescript(PLACEMENT_SCHEMA)
        row = {
            "url": "https://agency.test/service",
            "registered_domain": "agency.test",
            "crawl": "CC-TEST",
            "fetch_time": "2026-01-01T00:00:00+00:00",
            "terms": {
                "promise_level": "guaranteed",
                "promise_probability": 0.85,
                "placement_types": '["guest_post"]',
            },
            "scores": {"combined_score": 77.0, "score_band": "high"},
        }
        result = {
            "fetch_status": "ok",
            "placement_model": "external_service",
            "model_confidence": 0.9,
            "external_score": 7.0,
            "self_hosted_score": 0.0,
            "model_reasons": ["external_service_language"],
            "links": [
                {
                    "destination_url": "https://publisher.test/post",
                    "destination_registered_domain": "publisher.test",
                    "same_registered_domain": 0,
                    "anchor_text": "Example placement",
                    "rel": "nofollow",
                    "context_text": "Recent example placement",
                    "dom_section": "main",
                    "link_role": "placement_example",
                    "role_confidence": 0.9,
                    "role_reasons": ["explicit_example_context"],
                }
            ],
        }

        _save_page(connection, row, result)
        _refresh_services(connection)
        _refresh_evaluations(connection)

        self.assertEqual(
            connection.execute("SELECT COUNT(*) FROM outbound_links").fetchone()[0], 1
        )
        self.assertEqual(
            connection.execute("SELECT COUNT(*) FROM placements").fetchone()[0], 1
        )
        service = connection.execute(
            "SELECT placement_model,example_placement_count,reliability_status FROM services"
        ).fetchone()
        self.assertEqual(service, ("external_service", 1, "pending_domain_metrics"))
        evaluation = connection.execute(
            "SELECT placement_quality_score,expected_utility_points,evaluation_status "
            "FROM placement_evaluations"
        ).fetchone()
        self.assertEqual(evaluation, (None, None, "pending_placement_metrics"))
        connection.close()

    def test_service_rollup_does_not_let_unknown_page_hide_known_model(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.executescript(PLACEMENT_SCHEMA)
        connection.execute(
            """INSERT INTO services VALUES (
                'svc_1','publisher.test','https://publisher.test/a','unknown',
                0.35,'[]',2,2,0,0,'offline_page_evidence_only',NULL,'2026-01-01')"""
        )
        page_values = [
            (
                "https://publisher.test/a",
                "unknown",
                0.35,
                '["insufficient_model_evidence"]',
            ),
            (
                "https://publisher.test/write-for-us",
                "self_hosted",
                0.83,
                '["self_hosted_publication_language"]',
            ),
        ]
        connection.executemany(
            """INSERT INTO service_pages (
                   url,service_id,registered_domain,crawl,fetch_status,
                   placement_model,model_confidence,external_score,
                   self_hosted_score,model_reasons,model_evidence
               ) VALUES (?,'svc_1','publisher.test','CC-TEST','ok',?,?,0,0,?,'[]')""",
            page_values,
        )

        _refresh_services(connection)

        service = connection.execute(
            "SELECT placement_model,model_confidence FROM services"
        ).fetchone()
        self.assertEqual(service, ("self_hosted", 0.83))
        connection.close()

    def test_self_hosted_offline_evaluation_uses_page_not_provider_metrics(
        self,
    ) -> None:
        connection = sqlite3.connect(":memory:")
        connection.executescript(PLACEMENT_SCHEMA)
        row = {
            "url": "https://publisher.test/write",
            "registered_domain": "publisher.test",
            "crawl": "CC-TEST",
            "terms": {
                "promise_level": "open_submission",
                "promise_probability": 0.3,
                "placement_types": '["guest_post"]',
                "price_status": "free",
            },
            "scores": {"combined_score": 80.0, "score_band": "high"},
        }
        result = {
            "fetch_status": "ok",
            "placement_model": "self_hosted",
            "model_confidence": 0.9,
            "external_score": 0.0,
            "self_hosted_score": 5.5,
            "model_reasons": ["self_hosted_publication_language"],
            "links": [],
        }

        _save_page(connection, row, result)
        _refresh_services(connection)
        _refresh_evaluations(connection)

        evaluation = connection.execute(
            "SELECT service_reliability_score,placement_quality_score,"
            "expected_utility_points,cost_per_utility_point,evaluation_status "
            "FROM placement_evaluations"
        ).fetchone()
        self.assertEqual(evaluation, (None, 80.0, 24.0, 0.0, "offline_partial_priced"))
        connection.close()

    def test_writes_four_observation_windows_per_placement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "placements.db"
            out_path = Path(directory) / "impact.csv"
            connection = sqlite3.connect(db_path)
            connection.executescript(PLACEMENT_SCHEMA)
            connection.execute(
                """INSERT INTO placements VALUES (
                    'plc_1','svc_1','https://service.test/examples',
                    'https://publisher.test/post','publisher.test',
                    'placement_example',0.9,'[]','2026-01-01','2026-01-01')"""
            )
            connection.commit()
            connection.close()

            count = write_impact_template(db_path, out_path)

            self.assertEqual(count, 4)
            with out_path.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(
                [int(row["window_days"]) for row in rows], [7, 30, 90, 180]
            )


if __name__ == "__main__":
    unittest.main()
