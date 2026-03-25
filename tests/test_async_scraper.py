import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from bs4 import BeautifulSoup

from app.core.config import Settings, SiteConfig
from app.services.async_scraper import BaseScraper, MultiSiteScraper, ScrapedListing


@pytest.mark.asyncio
async def test_base_scraper_parse_card(sample_settings):
    """Test BaseScraper card parsing."""
    site_config = sample_settings.sites[0]
    scraper = BaseScraper(site_config, sample_settings)

    # Create mock HTML card
    html = """
    <div class="property-card">
        <a href="/ad/123456">Test Apartment</a>
        <span class="price">500 EUR</span>
        <span class="location">Sofia, Center</span>
        <span class="size">60 sqm</span>
        <span class="seller">Private Owner</span>
    </div>
    """

    soup = BeautifulSoup(html, "html.parser")
    card = soup.find("div", class_="property-card")

    listing = scraper._parse_card(card, "http://test.com")

    assert listing is not None
    assert listing.ad_id == "123456"
    assert listing.title == "Test Apartment"
    assert listing.price == "500 EUR"
    assert listing.location == "Sofia, Center"
    assert listing.size == "60 sqm"
    assert listing.link == "http://test.com/ad/123456"
    assert listing.seller_name == "Private Owner"


@pytest.mark.asyncio
async def test_base_scraper_parse_card_no_href():
    """Test BaseScraper card parsing when no href found."""
    site_config = SiteConfig(
        name="test_site",
        base_url="http://test.com/page:{page}",
        max_pages=1,
        selectors={
            "card": ".property-card",
            "link": "a",
            "title": ".title",
            "price": ".price",
            "location": ".location",
            "size": ".size",
            "seller": ".seller",
        },
    )

    settings = Settings(app_env="test")
    scraper = BaseScraper(site_config, settings)

    # Create mock HTML card without href
    html = """
    <div class="property-card">
        <span class="title">Test Apartment</span>
    </div>
    """

    soup = BeautifulSoup(html, "html.parser")
    card = soup.find("div", class_="property-card")

    listing = scraper._parse_card(card, "http://test.com")

    assert listing is None


@pytest.mark.asyncio
async def test_base_scraper_scrape_page_success(mock_httpx_client, sample_settings):
    """Test BaseScraper page scraping with successful response."""
    site_config = sample_settings.sites[0]
    scraper = BaseScraper(site_config, sample_settings)

    # Mock HTTP response
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.text = """
    <html>
        <body>
            <div class="property-card">
                <a href="/ad/123456">Test Apartment</a>
                <span class="price">500 EUR</span>
                <span class="location">Sofia, Center</span>
                <span class="size">60 sqm</span>
            </div>
        </body>
    </html>
    """

    mock_client_instance = AsyncMock()
    mock_client_instance.get.return_value = mock_response
    mock_httpx_client.return_value.__aenter__.return_value = mock_client_instance

    listings = await scraper._scrape_page(mock_client_instance, 1)

    assert len(listings) == 1
    assert listings[0].ad_id == "123456"
    assert listings[0].title == "Test Apartment"


@pytest.mark.asyncio
async def test_base_scraper_scrape_page_http_error(mock_httpx_client, sample_settings):
    """Test BaseScraper page scraping with HTTP error."""
    site_config = sample_settings.sites[0]
    scraper = BaseScraper(site_config, sample_settings)

    # Mock HTTP response with error
    mock_response = AsyncMock()
    mock_response.status_code = 404

    mock_client_instance = AsyncMock()
    mock_client_instance.get.return_value = mock_response
    mock_httpx_client.return_value.__aenter__.return_value = mock_client_instance

    listings = await scraper._scrape_page(mock_client_instance, 1)

    assert listings == []  # Should return empty list on HTTP error


@pytest.mark.asyncio
async def test_base_scraper_scrape_page_request_error(mock_httpx_client, sample_settings):
    """Test BaseScraper page scraping with request error."""
    site_config = sample_settings.sites[0]
    scraper = BaseScraper(site_config, sample_settings)

    # Mock client that raises RequestError
    mock_client_instance = AsyncMock()
    mock_client_instance.get.side_effect = httpx.RequestError("Connection failed")
    mock_httpx_client.return_value.__aenter__.return_value = mock_client_instance

    listings = await scraper._scrape_page(mock_client_instance, 1)

    assert listings == []  # Should return empty list on request error


@pytest.mark.asyncio
async def test_multisite_scraper_initialization(sample_settings):
    """Test MultiSiteScraper initialization."""
    multisite_scraper = MultiSiteScraper(sample_settings)

    assert len(multisite_scraper.scrapers) == len(sample_settings.sites)
    assert all(isinstance(scraper, BaseScraper) for scraper in multisite_scraper.scrapers)


@pytest.mark.asyncio
async def test_multisite_scraper_scrape_all_sites(mock_httpx_client, sample_settings):
    """Test MultiSiteScraper scraping all sites."""
    multisite_scraper = MultiSiteScraper(sample_settings)

    # Mock successful responses from all scrapers
    for scraper in multisite_scraper.scrapers:
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html>
            <body>
                <div class="property-card">
                    <a href="/ad/123456">Test Apartment</a>
                    <span class="price">500 EUR</span>
                    <span class="location">Sofia, Center</span>
                    <span class="size">60 sqm</span>
                </div>
            </body>
        </html>
        """

        mock_client_instance = AsyncMock()
        mock_client_instance.get.return_value = mock_response
        mock_httpx_client.return_value.__aenter__.return_value = mock_client_instance

        # Patch the individual scraper's methods
        original_scrape = scraper.scrape
        scraper.scrape = AsyncMock(
            return_value=[
                ScrapedListing(
                    ad_id="123456",
                    title="Test Apartment",
                    price="500 EUR",
                    location="Sofia, Center",
                    size="60 sqm",
                    link="/ad/123456",
                    source_site=scraper.site_config.name,
                )
            ]
        )

    all_listings = await multisite_scraper.scrape_all_sites()

    # Should get listings from all sites
    assert len(all_listings) == len(sample_settings.sites)


@pytest.mark.asyncio
async def test_multisite_scraper_scrape_with_errors(mock_httpx_client, sample_settings):
    """Test MultiSiteScraper handling errors from some sites."""
    # Add another site to the configuration
    sample_settings.sites.append(
        SiteConfig(
            name="failing_site",
            base_url="http://fail.com/page:{page}",
            max_pages=1,
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
    )

    multisite_scraper = MultiSiteScraper(sample_settings)

    # Mock successful response for first site
    mock_response_success = AsyncMock()
    mock_response_success.status_code = 200
    mock_response_success.text = """
    <html>
        <body>
            <div class="property-card">
                <a href="/ad/123456">Test Apartment</a>
                <span class="price">500 EUR</span>
                <span class="location">Sofia, Center</span>
                <span class="size">60 sqm</span>
            </div>
        </body>
    </html>
    """

    # Mock error for second site
    mock_response_error = AsyncMock()
    mock_response_error.status_code = 500

    # Setup HTTP client mock
    mock_client_instance = AsyncMock()
    mock_httpx_client.return_value.__aenter__.return_value = mock_client_instance

    # Patch the individual scrapers' methods
    multisite_scraper.scrapers[0].scrape = AsyncMock(
        return_value=[
            ScrapedListing(
                ad_id="123456",
                title="Test Apartment",
                price="500 EUR",
                location="Sofia, Center",
                size="60 sqm",
                link="/ad/123456",
                source_site=multisite_scraper.scrapers[0].site_config.name,
            )
        ]
    )

    multisite_scraper.scrapers[1].scrape = AsyncMock(side_effect=Exception("Test error"))

    all_listings = await multisite_scraper.scrape_all_sites()

    # Should get listings from only the successful site
    assert len(all_listings) == 1
    assert all_listings[0].ad_id == "123456"
