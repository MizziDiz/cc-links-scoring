import os
import sys
import unittest
from unittest.mock import patch

import pipeline
from cc_links.storage import SQLiteStorage


class PipelineStorageTests(unittest.TestCase):
    def test_process_page_uses_storage_contract(self) -> None:
        storage = SQLiteStorage(":memory:")
        try:
            with (
                patch.object(pipeline, "fetch_warc_record", return_value=b"warc"),
                patch.object(pipeline, "parse_html_record", return_value="<html></html>"),
                patch.object(pipeline, "make_soup", return_value=object()),
                patch.object(
                    pipeline,
                    "classify_engine",
                    return_value=("Forum", "Example", "signal"),
                ),
                patch.object(pipeline, "extract_links_from_html", return_value=[]),
            ):
                pipeline.process_page(
                    storage,
                    "crawl",
                    "https://example.test/",
                    "warc/file",
                    0,
                    1,
                    set(),
                )
            self.assertEqual(storage.query("SELECT COUNT(*) FROM pages").rows, [(1,)])
        finally:
            storage.close()

    def test_cli_backend_defaults_to_sqlite(self) -> None:
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(sys, "argv", ["pipeline.py", "countries", "--config", "run.config.json"]),
            patch.object(pipeline, "run_countries") as run,
        ):
            pipeline.main()
        self.assertEqual(run.call_args.args[-3:], ("sqlite", "threads", None))

    def test_cli_backend_can_come_from_environment(self) -> None:
        with (
            patch.dict(os.environ, {"DB_BACKEND": "mysql"}, clear=True),
            patch.object(sys, "argv", ["pipeline.py", "countries", "--config", "run.config.json"]),
            patch.object(pipeline, "run_countries") as run,
        ):
            pipeline.main()
        self.assertEqual(run.call_args.args[-3:], ("mysql", "threads", None))

    def test_cli_can_select_async_fetch_and_cpu_workers(self) -> None:
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(
                sys,
                "argv",
                [
                    "pipeline.py",
                    "countries",
                    "--config",
                    "run.config.json",
                    "--fetch-mode",
                    "async",
                    "--cpu-workers",
                    "3",
                ],
            ),
            patch.object(pipeline, "run_countries") as run,
        ):
            pipeline.main()
        self.assertEqual(run.call_args.args[-3:], ("sqlite", "async", 3))


if __name__ == "__main__":
    unittest.main()
