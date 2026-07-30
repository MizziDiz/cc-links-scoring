import io
import sqlite3
import unittest
from unittest.mock import patch

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from cc_links.outreach_enrich import (
    ENRICHMENT_SCHEMA,
    EnrichmentConfig,
    _save_snapshot,
    check_domain_sitemaps,
    extract_html_features,
    parse_sitemap_document,
    parse_warc_html,
)


class HtmlFeatureTests(unittest.TestCase):
    def test_extracts_dates_forms_contacts_and_engine(self):
        page = """
        <html lang="en">
          <head>
            <title>Write for us</title>
            <meta name="description" content="Contributor guidelines">
            <meta name="generator" content="WordPress 6.8">
            <meta property="article:modified_time"
                  content="2026-07-20T12:00:00Z">
            <meta property="article:published_time"
                  content="2025-01-10T08:00:00Z">
            <link rel="canonical" href="/write-for-us/">
            <script type="application/ld+json">
              {"@type":"Article","dateModified":"2026-07-21T09:30:00Z"}
            </script>
          </head>
          <body>
            <h1>Write for us</h1>
            <p>Submit original content. Word count: 1200.</p>
            <a href="/contact/">Contact editors</a>
            <a href="https://outside.example/story" rel="nofollow">Example</a>
            <a href="mailto:editor@example.com">Email</a>
            <form action="/submit" method="post">
              <textarea name="article_content"></textarea>
              <input name="author_bio">
            </form>
          </body>
        </html>
        """
        result = extract_html_features(
            page,
            page_url="https://example.com/write-for-us/",
            registered_domain="example.com",
            response_headers={"Last-Modified": "Wed, 22 Jul 2026 10:00:00 GMT"},
        )
        self.assertEqual(result["title"], "Write for us")
        self.assertEqual(result["h1"], "Write for us")
        self.assertEqual(result["html_lang"], "en")
        self.assertEqual(
            result["canonical_url"], "https://example.com/write-for-us/"
        )
        self.assertEqual(result["generator"], "wordpress 6.8")
        self.assertEqual(result["submission_form_count"], 1)
        self.assertEqual(result["contact_link_count"], 1)
        self.assertEqual(result["email_link_count"], 1)
        self.assertEqual(result["external_link_count"], 1)
        self.assertEqual(result["nofollow_external_count"], 1)
        self.assertIn("write for us", result["invitation_phrases"])
        self.assertIn("word count", result["guideline_signals"])
        self.assertEqual(
            result["date_modified"], "2026-07-21T09:30:00+00:00"
        )
        self.assertEqual(result["date_modified_source"], "jsonld")
        self.assertEqual(
            result["date_published"], "2025-01-10T08:00:00+00:00"
        )

    def test_flags_taxonomy_soft404_and_parked_content(self):
        result = extract_html_features(
            "<html><body>Page not found. This domain is for sale.</body></html>",
            page_url="https://demo.example.com/tag/write-for-us/",
            registered_domain="example.com",
        )
        self.assertIn("soft_404", result["risk_flags"])
        self.assertIn("parked_or_for_sale", result["risk_flags"])
        self.assertIn("taxonomy_path", result["risk_flags"])
        self.assertIn("demo_host", result["risk_flags"])


class WarcAndSitemapTests(unittest.TestCase):
    def test_relative_robots_sitemap_is_resolved_against_origin(self):
        calls: list[str] = []

        def fake_get(session, url, config, max_bytes):
            calls.append(url)
            if url.endswith("/robots.txt"):
                return 200, url, b"Sitemap: /sitemap.xml\n"
            return 404, url, b""

        with patch(
            "cc_links.outreach_enrich._get_limited", side_effect=fake_get
        ):
            check_domain_sitemaps(
                "example.com",
                ["https://example.com/write-for-us/"],
                EnrichmentConfig(max_sitemap_documents=2),
            )

        self.assertIn("https://example.com/sitemap.xml", calls)
        self.assertNotIn("/sitemap.xml", calls)

    def test_sitemap_attempts_are_bounded_when_robots_lists_dead_maps(self):
        robots = "\n".join(
            f"Sitemap: https://example.com/dead-{index}.xml"
            for index in range(50)
        ).encode()
        calls: list[str] = []
        request_configs: list[EnrichmentConfig] = []

        def fake_get(session, url, config, max_bytes):
            calls.append(url)
            request_configs.append(config)
            if url.endswith("/robots.txt"):
                return 200, url, robots
            return 404, url, b""

        with patch(
            "cc_links.outreach_enrich._get_limited", side_effect=fake_get
        ):
            domain, pages = check_domain_sitemaps(
                "example.com",
                ["https://example.com/write-for-us/"],
                EnrichmentConfig(max_sitemap_documents=4),
            )

        sitemap_calls = [url for url in calls if not url.endswith("/robots.txt")]
        self.assertEqual(len(sitemap_calls), 4)
        self.assertTrue(all(config.retries == 0 for config in request_configs))
        self.assertTrue(all(config.timeout == 8 for config in request_configs))
        self.assertEqual(domain["documents_fetched"], 0)
        self.assertEqual(pages, [])

    def test_failure_snapshot_satisfies_schema_defaults(self):
        connection = sqlite3.connect(":memory:")
        connection.executescript(ENRICHMENT_SCHEMA)
        _save_snapshot(
            connection,
            {
                "url": "https://example.com/write-for-us/",
                "source": "warc",
                "registered_domain": "example.com",
                "fetched_at": "2026-07-30T00:00:00+00:00",
                "fetch_status": "error",
                "error_kind": "ArchiveLoadFailed",
                "error_detail": "bad record",
            },
        )
        row = connection.execute(
            "SELECT bytes_read,invitation_phrases FROM page_snapshots"
        ).fetchone()
        self.assertEqual(row, (0, "[]"))
        connection.close()

    def test_parses_html_and_headers_from_gzipped_warc(self):
        stream = io.BytesIO()
        writer = WARCWriter(stream, gzip=True)
        http_headers = StatusAndHeaders(
            "200 OK",
            [
                ("Content-Type", "text/html; charset=utf-8"),
                ("Last-Modified", "Wed, 22 Jul 2026 10:00:00 GMT"),
            ],
            protocol="HTTP/1.1",
        )
        record = writer.create_warc_record(
            "https://example.com/write-for-us/",
            "response",
            payload=io.BytesIO(b"<html><title>Current</title></html>"),
            http_headers=http_headers,
        )
        writer.write_record(record)
        record.raw_stream.close()
        page, headers, truncated = parse_warc_html(stream.getvalue(), 1_000_000)
        self.assertIn("<title>Current</title>", page)
        self.assertEqual(headers["Content-Type"], "text/html; charset=utf-8")
        self.assertEqual(headers["X-WARC-HTTP-Status"], "200")
        self.assertFalse(truncated)

    def test_parses_urlset_and_normalizes_lastmod(self):
        payload = b"""<?xml version="1.0"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url>
            <loc>https://example.com/write-for-us/</loc>
            <lastmod>2026-07-22</lastmod>
          </url>
        </urlset>
        """
        kind, rows, truncated = parse_sitemap_document(
            payload, max_bytes=1_000_000
        )
        self.assertEqual(kind, "urlset")
        self.assertEqual(
            rows,
            [
                (
                    "https://example.com/write-for-us/",
                    "2026-07-22T00:00:00+00:00",
                )
            ],
        )
        self.assertFalse(truncated)

    def test_parses_sitemap_index(self):
        payload = b"""<sitemapindex>
          <sitemap><loc>https://example.com/page-sitemap.xml</loc>
          <lastmod>2026-07-22T01:00:00Z</lastmod></sitemap>
        </sitemapindex>"""
        kind, rows, _ = parse_sitemap_document(payload, max_bytes=1_000_000)
        self.assertEqual(kind, "sitemapindex")
        self.assertEqual(rows[0][0], "https://example.com/page-sitemap.xml")


if __name__ == "__main__":
    unittest.main()
