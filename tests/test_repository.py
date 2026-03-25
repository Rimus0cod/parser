from unittest.mock import AsyncMock

import pytest

from app.services.async_scraper import ScrapedListing
from app.services.repository import upsert_leads


@pytest.mark.asyncio
async def test_upsert_leads_empty_list():
    """Test upsert_leads with empty list."""
    result = await upsert_leads([])
    assert result == 0


@pytest.mark.asyncio
async def test_upsert_leads_single_item(mock_mysql_pool, sample_listing):
    """Test upsert_leads with single item."""
    # Mock the cursor execution
    mock_mysql_pool.executemany = AsyncMock()

    result = await upsert_leads([sample_listing])

    # Should have called executemany once
    mock_mysql_pool.executemany.assert_called_once()

    # Should return the number of listings
    assert result == 1


@pytest.mark.asyncio
async def test_upsert_leads_multiple_items(mock_mysql_pool, sample_listing):
    """Test upsert_leads with multiple items."""
    # Create multiple listings
    listings = [
        sample_listing,
        ScrapedListing(
            ad_id="789012",
            title="Another Apartment",
            price="600 EUR",
            location="Plovdiv, Center",
            size="70 sqm",
            link="http://test.com/ad/789012",
            source_site="test_site",
        ),
    ]

    # Mock the cursor execution
    mock_mysql_pool.executemany = AsyncMock()

    result = await upsert_leads(listings)

    # Should have called executemany once
    mock_mysql_pool.executemany.assert_called_once()

    # Should return the number of listings
    assert result == 2


@pytest.mark.asyncio
async def test_upsert_leads_error_handling(mock_mysql_pool, sample_listing):
    """Test upsert_leads error handling."""
    # Mock the cursor to raise an exception
    mock_mysql_pool.executemany.side_effect = Exception("Database error")

    with pytest.raises(Exception, match="Database error"):
        await upsert_leads([sample_listing])
