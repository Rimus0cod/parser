from __future__ import annotations

import os
from functools import lru_cache
from typing import Dict, List, Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SiteConfig(BaseModel):
    name: str
    base_url: str
    max_pages: int = 10
    selectors: Dict[str, str] = {}
    timeout: float = 30.0
    concurrency: int = 8


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
    urls: List[str] = []


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

    # Конфигурация сайтов
    sites: List[SiteConfig] = [
        SiteConfig(
            name="imoti.bg",
            base_url="https://imoti.bg/наеми/page:{page}",
            max_pages=10,
            selectors={
                "card": "div.item, article, .property-box",
                "title": "h2, h3, .title, a[title]",
                "price": ".price, .item-price, [class*='price']",
                "location": ".location, .item-location, [class*='location']",
                "size": ".size, [class*='area'], [class*='size']",
                "link": "a[href]",
                "seller": ".seller, [class*='agency'], [class*='owner']",
            },
        ),
        SiteConfig(
            name="domria.com",
            base_url="https://domria.com/search?page={page}",
            max_pages=20,
            selectors={
                "card": ".property-card, .listing-item",
                "title": ".property-title, .listing-title",
                "price": ".price, .cost",
                "location": ".address, .location",
                "size": ".area, .size",
                "link": "a.property-link",
                "seller": ".agent-name, .agency-name",
            },
        ),
        SiteConfig(
            name="olx.ua",
            base_url="https://olx.ua/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/?page={page}",
            max_pages=15,
            selectors={
                "card": ".offer-wrapper, .listing-item",
                "title": ".title-cell, .offer-title",
                "price": ".price, .offer-price",
                "location": ".location, .show-map-link",
                "size": ".params, .additional-info",
                "link": ".offer-titlebox > a",
                "seller": ".contact-person",
            },
        ),
        SiteConfig(
            name="lun.ua",
            base_url="https://lun.ua/uk/kvartiry/dolgosrochnyj-arendy?page={page}",
            max_pages=12,
            selectors={
                "card": ".listing-card, .property-item",
                "title": ".title, .property-title",
                "price": ".price, .cost",
                "location": ".location, .address",
                "size": ".area, .info-block",
                "link": "a.property-link",
                "seller": ".contact-info, .agency",
            },
        ),
        SiteConfig(
            name="flats.ua",
            base_url="https://flats.ua/kvartira-arenda/page/{page}",
            max_pages=15,
            selectors={
                "card": ".card, .property-card",
                "title": ".title, .card-title",
                "price": ".price, .cost",
                "location": ".location, .address",
                "size": ".area, .property-area",
                "link": "a.card-link",
                "seller": ".contact, .agency",
            },
        ),
    ]

    # Вебхуки
    webhooks: WebhookConfig = WebhookConfig()

    # Интеграции
    amocrm: AmoCrmConfig = AmoCrmConfig()
    bitrix24: Bitrix24Config = Bitrix24Config()

    # Остальные настройки
    scrape_timeout_seconds: float = 30.0
    scrape_interval_seconds: int = 900  # 15 минут
    scrape_concurrency: int = 8
    city_filter: str | None = None
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    streamlit_cookie_name: str = "lead_scanner_cookie"
    streamlit_cookie_key: str = "change_me_cookie_key"
    streamlit_cookie_expiry_days: int = 7
    streamlit_jwt_secret: str = Field(default="change_me_jwt_secret", min_length=16)
    streamlit_users_yaml_path: str = "app/ui/users.yaml"

    # Sentry
    sentry_dsn: str = ""
    sentry_environment: str = "production"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()

    # Читаем из переменных окружения списки сайтов
    sites_env = os.getenv("SCRAPER_SITES")
    if sites_env:
        import json
        from typing import cast

        try:
            sites_data = json.loads(sites_env)
            settings.sites = [SiteConfig(**site) for site in sites_data]
        except Exception:
            pass  # Оставляем значения по умолчанию

    return settings
