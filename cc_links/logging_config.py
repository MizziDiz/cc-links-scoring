"""Shared logging configuration for command-line entry points."""

import logging
import os

DEFAULT_LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
KNOWN_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}


def configure_logging() -> None:
    """Configure standard logging from ``LOG_LEVEL`` once per process."""
    requested = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    level = getattr(logging, requested, logging.INFO)

    logging.basicConfig(level=level, format=LOG_FORMAT)
    if requested not in KNOWN_LOG_LEVELS:
        logging.getLogger(__name__).warning(
            "Unknown LOG_LEVEL=%s; falling back to %s",
            requested,
            DEFAULT_LOG_LEVEL,
        )
