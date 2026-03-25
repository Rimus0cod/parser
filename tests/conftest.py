import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.config import Settings, SiteConfig
from app.services.async_scraper import ScrapedListing


@pytest.fixture
def sample_settings():
    """Sample settings for testing."""
    settings = Settings(
        app_env="test",
        mysql_host="localhost",
        redis_host="localhost",
        sites=[
            SiteConfig(
                name="test_site",
                base_url="http://test.com/page:{page}",
                max_pages=3,
                selectors={
                    "card": ".property-card",
                    "title": ".title",
                    "price": ".price",
                    "location": ".location",
                    "size": ".size",
                    "link": "a",
                    "seller": ".seller",
                },
            )
        ],
    )
    return settings


@pytest.fixture
def sample_listing():
    """Sample listing for testing."""
    return ScrapedListing(
        ad_id="123456",
        title="Test Apartment",
        price="500 EUR",
        location="Sofia, Center",
        size="60 sqm",
        link="http://test.com/ad/123456",
        source_site="test_site",
    )


@pytest.fixture
def mock_httpx_client():
    """Mock HTTPX client for testing."""
    with patch("httpx.AsyncClient") as mock_client:
        yield mock_client


@pytest.fixture
def mock_mysql_pool():
    """Mock MySQL pool for testing."""
    with patch("app.db.mysql.mysql_pool") as mock_pool:
        mock_connection = AsyncMock()
        mock_cursor = AsyncMock()
        mock_pool.return_value.__aenter__.return_value = mock_connection
        mock_connection.acquire.return_value.__aenter__.return_value = mock_cursor
        yield mock_cursor


@pytest.fixture
def event_loop():
    """Create a new event loop for each test."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
