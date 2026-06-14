"""Snowflake connection helper for the Monogram Paris ETL.

A thin wrapper over snowflake-connector-python that:

- loads the private key from either the PRIVATE_KEY env var (raw PEM) or the
  SNOWFLAKE_PRIVATE_KEY_PATH file path (priority: env var first),
- exposes the connection as a context manager so callers never forget to close,
- accepts an optional ``query_tag`` so each caller (direct ingester, snowpipe
  ingester, dbt, diagnostics) shows up distinctly in Snowflake QUERY_HISTORY.

Designed to replace the inline ``connect_snow()`` blocks that previously lived
in the snowpipe ingester and the diagnostics module.
"""
from __future__ import annotations

import contextlib
import logging
import os
from collections.abc import Iterable
from typing import Any

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

logger = logging.getLogger(__name__)


class SnowflakeConnection:
    def __init__(
        self,
        *,
        role: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        query_tag: str | None = None,
    ):
        self.connection: Any = None
        self.cursor: Any = None
        self._role = role or os.getenv("SNOWFLAKE_ROLE", "INGEST")
        self._warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "INGEST")
        self._database = database or os.getenv("SNOWFLAKE_DATABASE", "INGEST")
        self._schema = schema or os.getenv("SNOWFLAKE_SCHEMA", "INGEST")
        self._query_tag = query_tag
        self.connect()

    @staticmethod
    def load_private_key(
        path: str | None = None,
        passphrase: str | None = None,
    ) -> bytes:
        """Resolve the Snowflake private key from env var first, then file path.

        Returns DER-encoded bytes accepted by snowflake.connector.connect.
        Raises if no usable key is found.
        """
        private_key_content = os.getenv("PRIVATE_KEY")
        if private_key_content:
            try:
                key = serialization.load_pem_private_key(
                    private_key_content.encode(),
                    password=passphrase.encode() if passphrase else None,
                    backend=default_backend(),
                )
                return key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            except Exception as exc:  # noqa: BLE001 - surfaced as a warning
                logger.warning("PRIVATE_KEY env var present but unusable: %s", exc)

        if path and os.path.exists(path):
            try:
                with open(path, "rb") as fh:
                    key = serialization.load_pem_private_key(
                        fh.read(),
                        password=passphrase.encode() if passphrase else None,
                        backend=default_backend(),
                    )
                return key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Key file %s present but unusable: %s", path, exc)

        raise Exception("Aucune clé privée valide trouvée dans PRIVATE_KEY ou SNOWFLAKE_PRIVATE_KEY_PATH")

    def connect(self) -> None:
        private_key = self.load_private_key(
            os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
            os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        )

        session_parameters: dict[str, str] = {}
        if self._query_tag:
            session_parameters["QUERY_TAG"] = self._query_tag

        self.connection = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            private_key=private_key,
            role=self._role,
            warehouse=self._warehouse,
            database=self._database,
            schema=self._schema,
            session_parameters=session_parameters or None,
        )
        self.cursor = self.connection.cursor()
        logger.info(
            "Snowflake connected: role=%s warehouse=%s db=%s schema=%s query_tag=%s",
            self._role, self._warehouse, self._database, self._schema, self._query_tag,
        )

    def execute_query(self, query: str) -> list[tuple]:
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def execute_batch(self, query: str, data: Iterable[tuple]) -> int:
        self.cursor.executemany(query, list(data))
        return self.cursor.rowcount

    def close(self) -> None:
        if self.cursor:
            with contextlib.suppress(Exception):
                self.cursor.close()
        if self.connection:
            with contextlib.suppress(Exception):
                self.connection.close()

    def __enter__(self) -> SnowflakeConnection:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
