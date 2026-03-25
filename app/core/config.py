from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Real Estate SaaS Core"
    app_env: Literal["dev", "prod"] = "dev"
    app_debug: bool = False

    mysql_host: str = "mysql"
    mysql_port: int = 3306
    mysql_user: str = "app_user"
    mysql_password: str = "app_password"
    mysql_database: str = "imoti"

    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0

    scrape_base_url: str = "https://imoti.bg/наеми/page:{page}"
    scrape_max_pages: int = 10
    scrape_timeout_seconds: float = 30.0
    scrape_interval_seconds: int = 900
    scrape_concurrency: int = 8
    city_filter: str | None = None

    streamlit_cookie_name: str = "lead_scanner_cookie"
    streamlit_cookie_key: str = "change_me_cookie_key"
    streamlit_cookie_expiry_days: int = 7
    streamlit_jwt_secret: str = Field(default="change_me_jwt_secret", min_length=16)
    streamlit_users_yaml_path: str = "app/ui/users.yaml"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
