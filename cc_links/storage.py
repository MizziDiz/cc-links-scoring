"""Optional storage backends for pipeline writes and analysis queries."""

import hashlib
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Protocol, Sequence, Tuple
from urllib.parse import unquote, urlparse

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS pages (
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
    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url TEXT NOT NULL,
    target_url TEXT NOT NULL,
    target_domain TEXT,
    anchor_text TEXT,
    FOREIGN KEY (source_url) REFERENCES pages(url)
);

CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_url);
CREATE INDEX IF NOT EXISTS idx_links_target_domain ON links(target_domain);
CREATE INDEX IF NOT EXISTS idx_pages_engine ON pages(engine_category);
CREATE INDEX IF NOT EXISTS idx_pages_country ON pages(country);
CREATE INDEX IF NOT EXISTS idx_pages_bucket ON pages(bucket);
"""

MYSQL_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS pages (
        url_hash BINARY(32) NOT NULL,
        url TEXT NOT NULL,
        domain VARCHAR(253),
        crawl VARCHAR(64),
        timestamp VARCHAR(32),
        tld VARCHAR(32),
        country VARCHAR(255),
        bucket VARCHAR(255),
        engine_category VARCHAR(255),
        engine_name VARCHAR(255),
        outlink_count INT,
        fetched_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (url_hash),
        INDEX idx_pages_domain (domain),
        INDEX idx_pages_bucket (bucket),
        INDEX idx_pages_engine_name (engine_name),
        INDEX idx_pages_engine_category (engine_category),
        INDEX idx_pages_country (country)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS links (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        source_url_hash BINARY(32) NOT NULL,
        source_url TEXT NOT NULL,
        target_url TEXT NOT NULL,
        target_domain VARCHAR(253),
        anchor_text TEXT,
        PRIMARY KEY (id),
        INDEX idx_links_source (source_url_hash),
        INDEX idx_links_target_domain (target_domain),
        CONSTRAINT fk_links_page
            FOREIGN KEY (source_url_hash) REFERENCES pages (url_hash)
            ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
)


@dataclass(frozen=True)
class PageRecord:
    """One row in the pages table."""

    url: str
    domain: str
    crawl: str
    timestamp: str
    tld: Optional[str] = None
    country: Optional[str] = None
    bucket: Optional[str] = None
    engine_category: Optional[str] = None
    engine_name: Optional[str] = None
    outlink_count: Optional[int] = None
    fetched_at: Optional[str] = None


@dataclass(frozen=True)
class QueryResult:
    """Backend-neutral materialized query output."""

    columns: Tuple[str, ...]
    rows: List[Tuple[Any, ...]]


class Storage(Protocol):
    """Minimal contract used by the pipeline and analyze.py."""

    def save_pages(self, pages: Iterable[PageRecord]) -> None:
        """Insert pages, ignoring URLs already present."""

    def save_links(self, source_url: str, links: Iterable[Tuple[str, str]]) -> None:
        """Insert outbound links for a stored page."""

    def query(self, sql: str, params: Sequence[Any] = ()) -> QueryResult:
        """Execute a query and materialize its result."""

    def commit(self) -> None:
        """Commit the current transaction."""

    def close(self) -> None:
        """Close the backend connection."""


class SQLiteStorage:
    """Default local SQLite backend."""

    def __init__(self, path: str) -> None:
        self.connection = sqlite3.connect(path)
        self.connection.executescript(SQLITE_SCHEMA)
        self.connection.commit()

    def save_pages(self, pages: Iterable[PageRecord]) -> None:
        values = [_page_values(page) for page in pages]
        if not values:
            return
        self.connection.executemany(
            """INSERT OR IGNORE INTO pages
               (url, domain, crawl, timestamp, tld, country, bucket, engine_category,
                engine_name, outlink_count, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))""",
            values,
        )

    def save_links(self, source_url: str, links: Iterable[Tuple[str, str]]) -> None:
        values = [
            (source_url, target, _extract_domain(target), anchor)
            for target, anchor in links
        ]
        if values:
            self.connection.executemany(
                """INSERT INTO links
                   (source_url, target_url, target_domain, anchor_text)
                   VALUES (?, ?, ?, ?)""",
                values,
            )

    def query(self, sql: str, params: Sequence[Any] = ()) -> QueryResult:
        cursor = self.connection.execute(sql, tuple(params))
        columns = tuple(item[0] for item in (cursor.description or ()))
        return QueryResult(columns=columns, rows=cursor.fetchall())

    def commit(self) -> None:
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


class MySQLStorage:
    """Shared MySQL backend using mysql-connector-python."""

    def __init__(
        self,
        dsn: str,
        connect_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
    ) -> None:
        self._connector = _load_mysql_connector()
        self._config = mysql_config_from_dsn(dsn)
        retries = (
            connect_retries
            if connect_retries is not None
            else int(os.getenv("MYSQL_CONNECT_RETRIES", "30"))
        )
        delay = (
            retry_delay
            if retry_delay is not None
            else float(os.getenv("MYSQL_CONNECT_RETRY_DELAY", "2"))
        )
        self.connection = self._connect(retries, delay)
        self._initialize_schema()

    def _connect(self, retries: int, retry_delay: float) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(1, max(retries, 1) + 1):
            try:
                return self._connector.connect(**self._config)
            except self._connector.Error as exc:
                last_error = exc
                if attempt < retries:
                    time.sleep(retry_delay)
        if last_error is not None:
            raise last_error
        raise RuntimeError("MySQL connection failed without an exception")

    def _initialize_schema(self) -> None:
        cursor = self.connection.cursor()
        try:
            for statement in MYSQL_SCHEMA_STATEMENTS:
                cursor.execute(statement)
            self.connection.commit()
        finally:
            cursor.close()

    def save_pages(self, pages: Iterable[PageRecord]) -> None:
        values = [(_url_hash(page.url), *_page_values(page)) for page in pages]
        if not values:
            return
        cursor = self.connection.cursor()
        try:
            cursor.executemany(
                """INSERT INTO pages
                   (url_hash, url, domain, crawl, timestamp, tld, country, bucket, engine_category,
                    engine_name, outlink_count, fetched_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           COALESCE(%s, CURRENT_TIMESTAMP))
                   ON DUPLICATE KEY UPDATE url_hash = url_hash""",
                values,
            )
        finally:
            cursor.close()

    def save_links(self, source_url: str, links: Iterable[Tuple[str, str]]) -> None:
        values = [
            (_url_hash(source_url), source_url, target, _extract_domain(target), anchor)
            for target, anchor in links
        ]
        if not values:
            return
        cursor = self.connection.cursor()
        try:
            cursor.executemany(
                """INSERT INTO links
                   (source_url_hash, source_url, target_url, target_domain, anchor_text)
                   VALUES (%s, %s, %s, %s, %s)""",
                values,
            )
        finally:
            cursor.close()

    def query(self, sql: str, params: Sequence[Any] = ()) -> QueryResult:
        cursor = self.connection.cursor()
        try:
            prepared_sql = _mysql_placeholders(sql) if params else sql
            cursor.execute(prepared_sql, tuple(params))
            columns = tuple(item[0] for item in (cursor.description or ()))
            rows = cursor.fetchall() if cursor.description else []
            return QueryResult(columns=columns, rows=rows)
        finally:
            cursor.close()

    def commit(self) -> None:
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def create_storage(
    sqlite_path: str,
    backend: Optional[str] = None,
    mysql_dsn: Optional[str] = None,
) -> Storage:
    """Create the selected backend; SQLite remains the default."""
    selected = (backend or os.getenv("DB_BACKEND") or "sqlite").strip().lower()
    if selected == "sqlite":
        return SQLiteStorage(sqlite_path)
    if selected == "mysql":
        dsn = mysql_dsn or os.getenv("MYSQL_DSN")
        if not dsn:
            raise ValueError("DB_BACKEND=mysql requires MYSQL_DSN in the environment")
        return MySQLStorage(dsn)
    raise ValueError("DB_BACKEND must be sqlite or mysql")


def mysql_config_from_dsn(dsn: str) -> dict[str, Any]:
    """Parse a mysql:// DSN without logging or persisting its credentials."""
    parsed = urlparse(dsn)
    if parsed.scheme not in {"mysql", "mysql+mysqlconnector"}:
        raise ValueError("MYSQL_DSN must start with mysql://")
    path = parsed.path or ""
    if not parsed.hostname or not path.strip("/"):
        raise ValueError("MYSQL_DSN must include host and database")
    return {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": unquote(path.lstrip("/")),
        "charset": "utf8mb4",
        "autocommit": False,
        "connection_timeout": int(os.getenv("MYSQL_CONNECT_TIMEOUT", "10")),
    }


def _load_mysql_connector() -> Any:
    import mysql.connector

    return mysql.connector


def _page_values(page: PageRecord) -> Tuple[Any, ...]:
    return (
        page.url,
        page.domain,
        page.crawl,
        page.timestamp,
        page.tld,
        page.country,
        page.bucket,
        page.engine_category,
        page.engine_name,
        page.outlink_count,
        page.fetched_at,
    )


def _mysql_placeholders(sql: str) -> str:
    return sql.replace("?", "%s")


def _url_hash(url: str) -> bytes:
    return hashlib.sha256(url.encode("utf-8")).digest()


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except ValueError:
        return ""
