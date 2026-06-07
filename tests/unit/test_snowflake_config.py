"""Unit tests for snowflake key loading.

We avoid hitting Snowflake by mocking out snowflake.connector.connect and the
cryptography PEM loader.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit


@pytest.fixture
def fake_connect():
    """Patch snowflake.connector.connect so SnowflakeConnection.__init__ doesn't dial out."""
    with patch("monogram_etl.config.snowflake.snowflake.connector.connect") as connect:
        connect.return_value = MagicMock()
        connect.return_value.cursor.return_value = MagicMock()
        yield connect


def test_load_private_key_from_env_var(monkeypatch: pytest.MonkeyPatch, fake_connect) -> None:
    """When PRIVATE_KEY is set, the file-path branch should not be touched."""
    fake_pem = b"-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----"
    monkeypatch.setenv("PRIVATE_KEY", fake_pem.decode())
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", raising=False)

    fake_key = MagicMock()
    fake_key.private_bytes.return_value = b"DER-bytes"

    with patch(
        "monogram_etl.config.snowflake.serialization.load_pem_private_key",
        return_value=fake_key,
    ) as loader:
        from monogram_etl.config.snowflake import SnowflakeConnection

        sf = SnowflakeConnection()
        loader.assert_called_once()
        # Ensure the connection was opened with the DER bytes from our fake key
        assert fake_connect.call_args.kwargs["private_key"] == b"DER-bytes"
        sf.close()


def test_load_private_key_missing_everywhere_raises(monkeypatch: pytest.MonkeyPatch, fake_connect) -> None:
    monkeypatch.delenv("PRIVATE_KEY", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PATH", raising=False)

    from monogram_etl.config.snowflake import SnowflakeConnection

    with pytest.raises(Exception, match="Aucune clé privée"):
        SnowflakeConnection()


def test_context_manager_closes_cursor_and_connection(monkeypatch: pytest.MonkeyPatch, fake_connect) -> None:
    monkeypatch.setenv("PRIVATE_KEY", "-----BEGIN-----X-----END-----")
    fake_key = MagicMock()
    fake_key.private_bytes.return_value = b"X"

    with patch(
        "monogram_etl.config.snowflake.serialization.load_pem_private_key",
        return_value=fake_key,
    ):
        from monogram_etl.config.snowflake import SnowflakeConnection

        with SnowflakeConnection() as sf:
            mock_cursor = sf.cursor
            mock_conn = sf.connection

        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
