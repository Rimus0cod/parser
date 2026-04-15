from __future__ import annotations

import unittest
from types import SimpleNamespace

from app.core.config import validate_runtime_settings


def _settings(**overrides: object) -> SimpleNamespace:
    base = {
        "app_env": "prod",
        "mysql_user": "app_user",
        "mysql_password": "safe-db-password",
        "mysql_root_password": "",
        "streamlit_cookie_key": "safe-cookie-key-123",
        "streamlit_jwt_secret": "safe-jwt-secret-1234567890",
        "voice_enabled": False,
        "voice_public_base_url": "",
        "voice_ws_public_url": "",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class RuntimeConfigTests(unittest.TestCase):
    def test_prod_rejects_placeholder_cookie_secret(self) -> None:
        with self.assertRaises(RuntimeError):
            validate_runtime_settings(
                _settings(streamlit_cookie_key="change_me_cookie_key_please"),
                component="api",
            )

    def test_prod_rejects_voice_without_public_urls(self) -> None:
        with self.assertRaises(RuntimeError):
            validate_runtime_settings(
                _settings(voice_enabled=True, voice_public_base_url="", voice_ws_public_url=""),
                component="api",
            )

    def test_non_prod_allows_placeholders(self) -> None:
        validate_runtime_settings(
            _settings(app_env="dev", mysql_password="app_password"),
            component="api",
        )


if __name__ == "__main__":
    unittest.main()
