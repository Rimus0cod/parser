from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.scraping.contracts import ListingEnvelope, ScrapeExecutionResult, ScrapeSiteResult
from app.scraping.models import ScrapedListing
from app.services import repository
from tests.helpers import AsyncCursorStub, AsyncPoolStub


def _listing(*, ad_id: str = "A1", source_site: str = "example.com") -> ScrapedListing:
    return ScrapedListing(
        ad_id=ad_id,
        date_seen="2026-04-19",
        title="Apartment for rent in the center",
        price="1200 EUR",
        location="Kyiv",
        size="54 m2",
        link=f"https://{source_site}/listing/{ad_id}",
        source_site=source_site,
        phone="+380991112233",
        seller_name="Broker",
        ad_type="agency",
        contact_name="Owner",
        contact_email="owner@example.com",
    )


class ScrapingRepositoryTests(unittest.TestCase):
    def test_refresh_leads_marks_previous_site_rows_as_stale(self) -> None:
        cursor = AsyncCursorStub()
        execution = ScrapeExecutionResult(
            site_results=[
                ScrapeSiteResult(
                    site_name="example.com",
                    strategy_name="http_strategy",
                    accepted=[ListingEnvelope(listing=_listing())],
                )
            ]
        )

        async def run_test() -> None:
            with patch("app.services.repository.mysql_pool", return_value=AsyncPoolStub(cursor)):
                written = await repository.refresh_leads(
                    execution,
                    parser_version="parser-v2",
                    stale_strategy="mark",
                )
            self.assertEqual(written, 1)

        asyncio.run(run_test())

        self.assertEqual(len(cursor.executed), 2)
        upsert_sql, upsert_params = cursor.executed[0]
        cleanup_sql, cleanup_params = cursor.executed[1]

        self.assertIn("parser_version", upsert_sql)
        first_row = upsert_params[0]
        self.assertEqual(first_row[0], "A1")
        self.assertEqual(first_row[7], "example.com")
        self.assertEqual(first_row[8], "parser-v2")
        self.assertEqual(first_row[9], repository.LISTING_STATUS_ACTIVE)

        self.assertIn("SET record_status = %s", cleanup_sql)
        self.assertEqual(
            cleanup_params,
            (repository.LISTING_STATUS_STALE, "example.com", "parser-v2", "A1"),
        )

    def test_refresh_leads_can_delete_stale_rows_when_requested(self) -> None:
        cursor = AsyncCursorStub()
        execution = ScrapeExecutionResult(
            site_results=[
                ScrapeSiteResult(
                    site_name="example.com",
                    strategy_name="browser_strategy",
                    accepted=[],
                )
            ]
        )

        async def run_test() -> None:
            with patch("app.services.repository.mysql_pool", return_value=AsyncPoolStub(cursor)):
                written = await repository.refresh_leads(
                    execution,
                    parser_version="parser-v2",
                    stale_strategy="delete",
                )
            self.assertEqual(written, 0)

        asyncio.run(run_test())

        self.assertEqual(len(cursor.executed), 1)
        cleanup_sql, cleanup_params = cursor.executed[0]
        self.assertIn("DELETE FROM listings", cleanup_sql)
        self.assertEqual(cleanup_params, ("example.com",))

    def test_list_leads_uses_only_active_rows_for_current_parser_version(self) -> None:
        cursor = AsyncCursorStub()

        async def run_test() -> None:
            with (
                patch("app.services.repository.mysql_pool", return_value=AsyncPoolStub(cursor)),
                patch(
                    "app.services.repository.get_settings",
                    return_value=SimpleNamespace(scrape_data_version="parser-v2"),
                ),
            ):
                await repository.list_leads(limit=5)

        asyncio.run(run_test())

        self.assertEqual(len(cursor.executed), 1)
        sql, params = cursor.executed[0]
        self.assertIn("WHERE record_status = %s", sql)
        self.assertIn("AND parser_version = %s", sql)
        self.assertEqual(params, (repository.LISTING_STATUS_ACTIVE, "parser-v2", 5))


if __name__ == "__main__":
    unittest.main()
