from __future__ import annotations

import json
import logging
from functools import lru_cache
from typing import Any

import mysql.connector

from app.core.config import get_settings

logger = logging.getLogger(__name__)

try:
    from scrapling.core.custom_types import AnySelectorElement
    from scrapling.core.storage_adaptors import StorageSystemMixin
    from scrapling.core.utils import _StorageTools
except ImportError:  # pragma: no cover - optional dependency for local imports
    AnySelectorElement = Any
    StorageSystemMixin = object  # type: ignore[assignment]
    _StorageTools = None


@lru_cache(maxsize=None)
class MySQLAdaptiveStorage(StorageSystemMixin):  # type: ignore[misc]
    def __init__(
        self,
        *,
        url: str | None = None,
        table_name: str = "scrapling_adaptive_elements",
        version: str = "v1",
        **_: object,
    ) -> None:
        if hasattr(super(), "__init__"):
            super().__init__(url=url)  # type: ignore[misc]
        self._url = url or ""
        self._table_name = table_name
        self._version = version
        self._ensure_schema()

    def _settings(self) -> dict[str, object]:
        settings = get_settings()
        password = settings.mysql_root_password if settings.mysql_user == "root" else settings.mysql_password
        return {
            "host": settings.mysql_host,
            "port": settings.mysql_port,
            "user": settings.mysql_user,
            "password": password,
            "database": settings.mysql_database,
            "charset": "utf8mb4",
            "use_unicode": True,
        }

    def _connect(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(**self._settings())

    def _ensure_schema(self) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._table_name} (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        target_url VARCHAR(1024) NOT NULL,
                        identifier VARCHAR(255) NOT NULL,
                        selector_version VARCHAR(32) NOT NULL DEFAULT 'v1',
                        payload_json LONGTEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_scrapling_adaptive_target (
                            target_url(255),
                            identifier,
                            selector_version
                        )
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
            connection.commit()
        finally:
            connection.close()

    def save(self, element: AnySelectorElement, identifier: str) -> None:
        if _StorageTools is None:
            raise RuntimeError("Scrapling is not installed. Adaptive storage is unavailable.")

        payload = json.dumps(_StorageTools.element_to_dict(element), ensure_ascii=False)
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self._table_name} (
                        target_url,
                        identifier,
                        selector_version,
                        payload_json
                    ) VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        payload_json = VALUES(payload_json),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (self._url, identifier, self._version, payload),
                )
            connection.commit()
        finally:
            connection.close()

    def retrieve(self, identifier: str) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT payload_json
                    FROM {self._table_name}
                    WHERE target_url = %s
                      AND identifier = %s
                      AND selector_version = %s
                    LIMIT 1
                    """,
                    (self._url, identifier, self._version),
                )
                row = cursor.fetchone()
        finally:
            connection.close()

        if row is None:
            return None

        payload = row[0]
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            logger.warning(
                "Failed to decode Scrapling adaptive payload",
                extra={"target_url": self._url, "identifier": identifier},
            )
            return None
