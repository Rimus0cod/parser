from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _default_user_agents() -> list[str]:
    return [
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) "
            "Gecko/20100101 Firefox/138.0"
        ),
    ]


class SiteConfig(BaseModel):
    name: str
    base_url: str
    max_pages: int = 5
    selectors: dict[str, str] = Field(default_factory=dict)
    timeout: float = 30.0
    concurrency: int = 4
    enabled: bool = True
    verify_ssl: bool = True
    detail_pages_enabled: bool = True
    listing_path_keywords: list[str] = Field(default_factory=list)
    allowed_domains: list[str] = Field(default_factory=list)


class AmoCrmConfig(BaseModel):
    enabled: bool = False
    base_url: str = ""
    access_token: str = ""
    refresh_token: str = ""
    client_id: str = ""
    client_secret: str = ""
    pipeline_id: str = ""
    status_id: str = ""


class Bitrix24Config(BaseModel):
    enabled: bool = False
    webhook_url: str = ""
    domain: str = ""
    client_id: str = ""
    client_secret: str = ""


class WebhookConfig(BaseModel):
    enabled: bool = False
    urls: list[str] = Field(default_factory=list)


def _default_sites() -> list[SiteConfig]:
    return [
        SiteConfig(
            name="imoti.bg",
            base_url="https://imoti.bg/наеми/page:{page}",
            max_pages=10,
            selectors={
                "card": "article, li, section, div",
                "title": "h4 a[href*='/наеми/'], h3 a[href*='/наеми/'], a[href*='/наеми/']",
                "link": "h4 a[href*='/наеми/'], h3 a[href*='/наеми/'], a[href*='/наеми/']",
                "price": "[class*='price'], strong, b",
                "location": "[class*='location'], [class*='region'], p, span",
                "size": "[class*='area'], [class*='size'], li, span",
                "seller": "[class*='agency'], [class*='broker'], [class*='owner']",
            },
            listing_path_keywords=["/наеми/"],
            allowed_domains=["imoti.bg"],
        ),
        SiteConfig(
            name="alo.bg",
            base_url="https://www.alo.bg/obiavi/imoti-naemi/?page={page}",
            max_pages=10,
            selectors={
                "card": ".list, .listvip, article, .item, .list-obj",
                "title": "h2, h3, .title, a[title], a[href]",
                "link": "a[href]",
                "price": ".price, .item-price, [class*='price']",
                "location": ".location, .list-location, .item-location, [class*='location']",
                "size": ".size, [class*='area'], [class*='size']",
                "seller": ".seller, [class*='agency'], [class*='owner']",
            },
            listing_path_keywords=["/obiava/", "/obiavi/"],
            allowed_domains=["alo.bg", "www.alo.bg"],
        ),
        SiteConfig(
            name="dom.ria.com",
            base_url="https://dom.ria.com/uk/arenda-kvartir/?page={page}",
            max_pages=5,
            verify_ssl=False,
            selectors={
                "card": "article, section, div",
                "title": "a[href*='/uk/arenda-kvartir/'], a[href*='/arenda-kvartir/']",
                "link": "a[href*='/uk/arenda-kvartir/'], a[href*='/arenda-kvartir/']",
            },
            listing_path_keywords=["/uk/arenda-kvartir/", "/arenda-kvartir/"],
            allowed_domains=["dom.ria.com"],
        ),
        SiteConfig(
            name="olx.ua",
            base_url=(
                "https://www.olx.ua/uk/nedvizhimost/kvartiry/"
                "dolgosrochnaya-arenda-kvartir/?page={page}"
            ),
            max_pages=5,
            selectors={
                "card": "article, li, div",
                "title": "a[href]",
                "link": "a[href]",
            },
            listing_path_keywords=["/d/uk/obyavlenie/", "/obyavlenie/"],
            allowed_domains=["olx.ua", "www.olx.ua"],
        ),
        SiteConfig(
            name="lun.ua",
            base_url="https://lun.ua/rent/kyiv/flats?page={page}",
            max_pages=3,
            selectors={
                "card": "article, section, div",
                "title": "a[href*='/rent/kyiv/flats/']",
                "link": "a[href*='/rent/kyiv/flats/']",
            },
            listing_path_keywords=["/rent/kyiv/flats/"],
            allowed_domains=["lun.ua"],
        ),
    ]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_name: str = "Real Estate SaaS Core"
    app_env: Literal["dev", "prod", "test"] = "dev"
    app_debug: bool = False

    mysql_host: str = "mysql"
    mysql_port: int = 3306
    mysql_user: str = "app_user"
    mysql_password: str = "app_password"
    mysql_database: str = "imoti"
    mysql_root_password: str = ""

    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0

    voice_enabled: bool = False
    voice_public_base_url: str = ""
    voice_ws_public_url: str = ""
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_from_number: str = ""
    twilio_validate: bool = True
    google_application_credentials: str = ""

    sites: list[SiteConfig] = Field(default_factory=_default_sites)

    scrape_timeout_seconds: float = 30.0
    scrape_interval_seconds: int = 900
    scrape_concurrency: int = 8
    scrape_retry_count: int = 3
    scrape_backoff_base_seconds: float = 1.5
    scrape_backoff_cap_seconds: float = 12.0
    scrape_delay_min_seconds: float = 0.15
    scrape_delay_max_seconds: float = 0.8
    scrape_follow_redirects: bool = True
    scrape_verify_ssl: bool = True
    scrape_detail_pages: bool = True
    http_max_connections: int = 30
    http_max_keepalive_connections: int = 10
    city_filter: str | None = None
    scraper_sites: str = ""

    user_agent: str = _default_user_agents()[0]
    user_agents: list[str] = Field(default_factory=_default_user_agents)

    proxy_enabled: bool = False
    proxy_list: str = ""
    proxy_rotation_strategy: Literal["random", "round_robin", "failover"] = "random"
    proxy_max_retries: int = 3

    streamlit_cookie_name: str = "lead_scanner_cookie"
    streamlit_cookie_key: str = "change_me_cookie_key_please"
    streamlit_cookie_expiry_days: int = 7
    streamlit_jwt_secret: str = Field(default="change_me_jwt_secret_32chars_min", min_length=16)
    streamlit_users_yaml_path: str = "app/ui/users.yaml"

    sentry_dsn: str = ""
    sentry_environment: str = "production"
    sentry_traces_sample_rate: float = 0.1

    log_level: str = "INFO"
    log_format: Literal["console", "json"] = "json"
    log_to_file: bool = False
    log_dir: str = "logs"

    scheduler_enabled: bool = False
    scheduler_cron: str = "*/15 * * * *"
    scheduler_timezone: str = "Europe/Kyiv"

    webhooks_enabled: bool = False
    webhooks_urls: str = ""

    amocrm_enabled: bool = False
    amocrm_base_url: str = ""
    amocrm_access_token: str = ""
    amocrm_refresh_token: str = ""
    amocrm_client_id: str = ""
    amocrm_client_secret: str = ""
    amocrm_pipeline_id: str = ""
    amocrm_status_id: str = ""

    bitrix24_enabled: bool = False
    bitrix24_webhook_url: str = ""
    bitrix24_domain: str = ""
    bitrix24_client_id: str = ""
    bitrix24_client_secret: str = ""

    google_sheet_id: str = ""
    service_account_json: str = "./google.json"
    sheet_name: str = "Imoti_BG_Rentals"
    email_from: str = ""
    email_to: str = ""
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    max_pages: int = 26
    request_delay_min: float = 2.0
    request_delay_max: float = 5.0
    log_file: str = "./parser.log"
    agencies_csv_path: str = "./agencies.csv"
    mysql_enabled: bool = True
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_allowed_chat_ids: str = ""
    telegram_history_chunk_size: int = 20
    telegram_default_history_days: int = 7
    telegram_startup_preview_count: int = 3

    @property
    def proxy_pool(self) -> list[str]:
        return _split_csv(self.proxy_list)

    @property
    def webhooks(self) -> WebhookConfig:
        return WebhookConfig(enabled=self.webhooks_enabled, urls=_split_csv(self.webhooks_urls))

    @property
    def amocrm(self) -> AmoCrmConfig:
        return AmoCrmConfig(
            enabled=self.amocrm_enabled,
            base_url=self.amocrm_base_url,
            access_token=self.amocrm_access_token,
            refresh_token=self.amocrm_refresh_token,
            client_id=self.amocrm_client_id,
            client_secret=self.amocrm_client_secret,
            pipeline_id=self.amocrm_pipeline_id,
            status_id=self.amocrm_status_id,
        )

    @property
    def bitrix24(self) -> Bitrix24Config:
        return Bitrix24Config(
            enabled=self.bitrix24_enabled,
            webhook_url=self.bitrix24_webhook_url,
            domain=self.bitrix24_domain,
            client_id=self.bitrix24_client_id,
            client_secret=self.bitrix24_client_secret,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()

    if settings.proxy_enabled and not settings.proxy_pool:
        settings.proxy_enabled = False

    if settings.scraper_sites:
        try:
            sites_data = json.loads(settings.scraper_sites)
            settings.sites = [SiteConfig.model_validate(site) for site in sites_data]
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    for site in settings.sites:
        if not site.timeout:
            site.timeout = settings.scrape_timeout_seconds
        if not site.concurrency:
            site.concurrency = settings.scrape_concurrency

    return settings
