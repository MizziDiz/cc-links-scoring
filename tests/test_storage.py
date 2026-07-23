import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cc_links.storage import (
    MYSQL_SCHEMA_STATEMENTS,
    MySQLStorage,
    PageRecord,
    SQLiteStorage,
    create_storage,
    mysql_config_from_dsn,
)
from migrate_sqlite_to_mysql import migrate_pages


class SQLiteStorageTests(unittest.TestCase):
    def test_save_query_and_duplicate_page(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = SQLiteStorage(str(Path(temp_dir) / "pages.db"))
            try:
                page = PageRecord(
                    url="https://example.test/page",
                    domain="example.test",
                    crawl="CC-MAIN-TEST",
                    timestamp="",
                    bucket="sample",
                    engine_name="ExampleEngine",
                )
                storage.save_pages([page, page])
                storage.save_links(page.url, [("https://target.test/x", "target")])
                storage.commit()

                self.assertEqual(storage.query("SELECT COUNT(*) FROM pages").rows, [(1,)])
                self.assertEqual(storage.query("SELECT COUNT(*) FROM links").rows, [(1,)])
            finally:
                storage.close()

    def test_factory_defaults_to_sqlite(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            storage = create_storage(":memory:")
            self.assertIsInstance(storage, SQLiteStorage)
            storage.close()


class MySQLConfigTests(unittest.TestCase):
    def test_dsn_parser_decodes_credentials_without_exposing_them(self) -> None:
        config = mysql_config_from_dsn(
            "mysql://worker:p%40ss@mysql.example:3307/cc_links"
        )
        self.assertEqual(config["host"], "mysql.example")
        self.assertEqual(config["port"], 3307)
        self.assertEqual(config["user"], "worker")
        self.assertEqual(config["password"], "p@ss")
        self.assertEqual(config["database"], "cc_links")

    def test_mysql_schema_has_requested_indexes_and_full_url_hash_key(self) -> None:
        ddl = "\n".join(MYSQL_SCHEMA_STATEMENTS)
        self.assertIn("idx_pages_domain", ddl)
        self.assertIn("idx_pages_bucket", ddl)
        self.assertIn("idx_pages_engine_name", ddl)
        self.assertIn("url_hash BINARY(32)", ddl)
        self.assertIn("url TEXT NOT NULL", ddl)

    def test_mysql_storage_hashes_full_urls_for_writes(self) -> None:
        connection = _FakeMySQLConnection()
        connector = _FakeMySQLConnector(connection)
        with patch("cc_links.storage._load_mysql_connector", return_value=connector):
            storage = MySQLStorage(
                "mysql://worker:password@mysql.example/cc_links",
                connect_retries=1,
                retry_delay=0,
            )
            page = PageRecord(
                url="https://example.test/" + ("x" * 3000),
                domain="example.test",
                crawl="crawl",
                timestamp="",
            )
            storage.save_pages([page])
            storage.save_links(page.url, [("https://target.test/", "target")])
            storage.commit()
            storage.close()

        page_values = connection.executemany_calls[0][1][0]
        link_values = connection.executemany_calls[1][1][0]
        self.assertEqual(len(page_values[0]), 32)
        self.assertEqual(page_values[1], page.url)
        self.assertEqual(link_values[0], page_values[0])
        self.assertEqual(link_values[1], page.url)


class MigrationTests(unittest.TestCase):
    def test_pages_migration_is_idempotent(self) -> None:
        source = sqlite3.connect(":memory:")
        source.executescript(
            """
            CREATE TABLE pages (
                url TEXT PRIMARY KEY,
                domain TEXT,
                crawl TEXT,
                timestamp TEXT,
                tld TEXT,
                country TEXT,
                bucket TEXT,
                engine_category TEXT,
                engine_name TEXT,
                outlink_count INTEGER,
                fetched_at TEXT
            );
            INSERT INTO pages VALUES (
                'https://example.test/', 'example.test', 'crawl', '', 'test',
                'Test', 'sample', 'Forum', 'Example', 0, CURRENT_TIMESTAMP
            );
            """
        )
        target = SQLiteStorage(":memory:")
        try:
            self.assertEqual(migrate_pages(source, target, batch_size=1), 1)
            self.assertEqual(migrate_pages(source, target, batch_size=1), 1)
            self.assertEqual(target.query("SELECT COUNT(*) FROM pages").rows, [(1,)])
        finally:
            source.close()
            target.close()


class _FakeMySQLCursor:
    def __init__(self, connection: "_FakeMySQLConnection") -> None:
        self.connection = connection
        self.description = None

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
        self.connection.execute_calls.append((sql, params))

    def executemany(
        self,
        sql: str,
        values: list[tuple[object, ...]],
    ) -> None:
        self.connection.executemany_calls.append((sql, values))

    def fetchall(self) -> list[tuple[object, ...]]:
        return []

    def close(self) -> None:
        return None


class _FakeMySQLConnection:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.commits = 0
        self.closed = False

    def cursor(self) -> _FakeMySQLCursor:
        return _FakeMySQLCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


class _FakeMySQLConnector:
    Error = RuntimeError

    def __init__(self, connection: _FakeMySQLConnection) -> None:
        self.connection = connection

    def connect(self, **_kwargs: object) -> _FakeMySQLConnection:
        return self.connection


if __name__ == "__main__":
    unittest.main()
