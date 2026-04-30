from __future__ import annotations

import json
from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ScrapeModeConfig = Literal["http", "browser", "ai", "dynamic", "stealth"]


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
        ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) " "Gecko/20100101 Firefox/138.0"),
    ]


def _default_scrapling_blocked_markers() -> list[str]:
    return [
        "access denied",
        "blocked",
        "captcha",
        "cf-challenge",
        "cloudflare",
        "ddos protection",
        "enable javascript and cookies",
        "human verification",
        "just a moment",
        "please verify you are a human",
        "request unsuccessful",
        "security check",
        "turnstile",
        "unusual traffic",
        "verify you are human",
    ]


def _default_mode_order() -> list[ScrapeModeConfig]:
    return ["http", "browser", "ai"]


class SiteConfig(BaseModel):
    name: str
    base_url: str
    max_pages: int = 5
    selectors: dict[str, str] = Field(default_factory=dict)
    selector_version: str = "v1"
    timeout: float = 30.0
    concurrency: int = 4
    enabled: bool = True
    verify_ssl: bool = True
    detail_pages_enabled: bool = True
    mode_order: list[ScrapeModeConfig] = Field(default_factory=_default_mode_order)
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
    scrape_data_version: str = "v2"
    scrape_stale_strategy: Literal["mark", "delete"] = "mark"
    scrape_backoff_base_seconds: float = 1.5
    scrape_backoff_cap_seconds: float = 12.0
    scrape_delay_min_seconds: float = 0.15
    scrape_delay_max_seconds: float = 0.8
    scrape_follow_redirects: bool = True
    scrape_verify_ssl: bool = True
    scrape_detail_pages: bool = True
    http_max_connections: int = 30
    http_max_keepalive_connections: int = 10
    browser_strategy_enabled: bool = False
    browser_concurrency: int = 2
    browser_headless: bool = True
    browser_stealth: bool = True
    playwright_browser: Literal["chromium", "firefox", "webkit"] = "chromium"
    ai_strategy_enabled: bool = False
    ai_strategy_concurrency: int = 1
    scrapling_dynamic_enabled: bool = False
    scrapling_stealth_enabled: bool = False
    scrapling_dynamic_concurrency: int = 2
    scrapling_stealth_concurrency: int = 1
    scrapling_http_impersonate: str = "chrome"
    scrapling_http3: bool = True
    scrapling_disable_resources: bool = True
    scrapling_disable_ads: bool = True
    scrapling_block_webrtc: bool = True
    scrapling_network_idle: bool = True
    scrapling_solve_cloudflare: bool = True
    scrapling_stealth_humanize: bool = True
    scrapling_wait_selector_timeout_ms: int = 12000
    scrapling_blocked_markers: list[str] = Field(default_factory=_default_scrapling_blocked_markers)
    scrapling_storage_table: str = "scrapling_adaptive_elements"
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


def _looks_like_placeholder_secret(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in {
        "",
        "app_password",
        "change_me_cookie_key_please",
        "change_me_jwt_secret_32chars_min",
        "change_me_mysql_password",
        "change_me_root_password",
    } or normalized.startswith(("change_me", "replace_me", "replace_with"))


def validate_runtime_settings(settings: Settings, *, component: str) -> None:
    if settings.app_env != "prod":
        return

    issues: list[str] = []
    database_secret = (
        settings.mysql_root_password
        if settings.mysql_user == "root" and settings.mysql_root_password
        else settings.mysql_password
    )

    if _looks_like_placeholder_secret(database_secret):
        issues.append("database password is empty or still uses a placeholder value")
    if _looks_like_placeholder_secret(settings.streamlit_cookie_key):
        issues.append("STREAMLIT_COOKIE_KEY must be replaced for production")
    if _looks_like_placeholder_secret(settings.streamlit_jwt_secret):
        issues.append("STREAMLIT_JWT_SECRET must be replaced for production")
    if settings.voice_enabled and not settings.voice_public_base_url:
        issues.append("VOICE_PUBLIC_BASE_URL is required when voice integration is enabled")
    if settings.voice_enabled and not settings.voice_ws_public_url:
        issues.append("VOICE_WS_PUBLIC_URL is required when voice integration is enabled")

    if issues:
        joined_issues = "; ".join(issues)
        raise RuntimeError(f"Unsafe production configuration for {component}: {joined_issues}.")
