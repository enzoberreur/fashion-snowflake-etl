"""Centralised structured logging for the Monogram Paris ETL.

Sets up a Python logger with a consistent format so Airflow / Docker logs are
greppable. Idempotent - call ``configure_logging()`` from any entry point.
"""
from __future__ import annotations

import logging
import os
import sys

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s :: %(message)s"
_configured = False


def configure_logging(level: str | None = None) -> None:
    global _configured
    if _configured:
        return

    log_level = (level or os.getenv("MONOGRAM_LOG_LEVEL") or "INFO").upper()

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))

    root = logging.getLogger()
    root.setLevel(log_level)
    # Replace any pre-existing handlers so reload-safe in Airflow workers.
    root.handlers = [handler]

    # Tame noisy third-parties.
    for noisy in ("snowflake.connector", "botocore", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger. Call once per module: ``logger = get_logger(__name__)``."""
    configure_logging()
    return logging.getLogger(name)
