from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

from app.core.config import Settings, SiteConfig
from app.core.logging import get_logger
from app.scraping.extractor import ListingExtractor
from app.scraping.fetchers import StrategyBlockedError, build_session_client
from app.scraping.models import ScrapedListing
from app.scraping.site_profiles import SiteProfile, get_site_profile

logger = get_logger("scraping_strategies")


class SessionStrategy:
    name = "scrapling_session"
    mode = "http"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def supports(self, site_config: SiteConfig) -> bool:
        profile = get_site_profile(site_config, self.settings)
        return site_config.enabled and self.mode in profile.mode_order

    async def scrape_site(self, site_config: SiteConfig) -> list[ScrapedListing]:
        site_profile = get_site_profile(site_config, self.settings)
        extractor = ListingExtractor(site_config, site_profile)
        page_sem = asyncio.Semaphore(self._strategy_concurrency())
        detail_sem = asyncio.Semaphore(self._detail_concurrency())
        today = date.today().isoformat()

        async with build_session_client(
            self.mode,
            settings=self.settings,
            site_config=site_config,
            site_profile=site_profile,
        ) as session:
            list_pages = await asyncio.gather(
                *(
                    self._scrape_list_page(
                        session=session,
                        site_config=site_config,
                        site_profile=site_profile,
                        page_number=page_number,
                        extractor=extractor,
                        page_sem=page_sem,
                    )
                    for page_number in range(1, site_config.max_pages + 1)
                ),
                return_exceptions=True,
            )

            listings = self._merge_listings(list_pages, site_config=site_config, today=today)
            if listings and self.settings.scrape_detail_pages and site_config.detail_pages_enabled:
                await self._enrich_listings(
                    session=session,
                    site_config=site_config,
                    site_profile=site_profile,
                    listings=listings,
                    extractor=extractor,
                    detail_sem=detail_sem,
                )

        logger.info(
            "Strategy site scrape completed",
            site=site_config.name,
            strategy=self.name,
            mode=self.mode,
            extracted=len(listings),
        )
        return listings

    def _strategy_concurrency(self) -> int:
        return max(1, self.settings.scrape_concurrency)

    def _detail_concurrency(self) -> int:
        return max(1, min(self._strategy_concurrency(), 4))

    def _build_page_url(self, site_config: SiteConfig, page: int) -> str:
        base_url = site_config.base_url
        if "{page}" not in base_url:
            return base_url if page == 1 else f"{base_url}?page={page}"
        if page == 1 and site_config.name == "imoti.bg":
            return base_url.replace("/page:{page}", "")
        return base_url.format(page=page)

    async def _scrape_list_page(
        self,
        *,
        session: Any,
        site_config: SiteConfig,
        site_profile: SiteProfile,
        page_number: int,
        extractor: ListingExtractor,
        page_sem: asyncio.Semaphore,
    ) -> list[ScrapedListing]:
        url = self._build_page_url(site_config, page_number)
        async with page_sem:
            outcome = await session.fetch(
                url,
                page_kind="list",
                wait_selector=site_profile.list_wait_selector,
            )

        self._raise_for_transport_or_block(
            site_name=site_config.name,
            url=outcome.url,
            status_code=outcome.status_code,
            reason=extractor.detect_list_page_issue(outcome.page) if outcome.page is not None else "empty_response",
        )
        if outcome.page is None:
            return []
        return extractor.extract_listings(outcome.page, base_url=outcome.url)

    async def _enrich_listings(
        self,
        *,
        session: Any,
        site_config: SiteConfig,
        site_profile: SiteProfile,
        listings: list[ScrapedListing],
        extractor: ListingExtractor,
        detail_sem: asyncio.Semaphore,
    ) -> None:
        results = await asyncio.gather(
            *(
                self._enrich_listing(
                    session=session,
                    site_config=site_config,
                    site_profile=site_profile,
                    listing=listing,
                    extractor=extractor,
                    detail_sem=detail_sem,
                )
                for listing in listings
            ),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, StrategyBlockedError):
                raise result
            if isinstance(result, Exception):
                logger.warning(
                    "Detail enrichment failed",
                    site=site_config.name,
                    strategy=self.name,
                    mode=self.mode,
                    error=str(result),
                )

    async def _enrich_listing(
        self,
        *,
        session: Any,
        site_config: SiteConfig,
        site_profile: SiteProfile,
        listing: ScrapedListing,
        extractor: ListingExtractor,
        detail_sem: asyncio.Semaphore,
    ) -> ScrapedListing:
        async with detail_sem:
            outcome = await session.fetch(
                listing.link,
                page_kind="detail",
                wait_selector=site_profile.detail_wait_selector,
            )

        detail_issue = extractor.detect_detail_page_issue(outcome.page) if outcome.page is not None else "empty_response"
        self._raise_for_transport_or_block(
            site_name=site_config.name,
            url=outcome.url,
            status_code=outcome.status_code,
            reason=detail_issue,
        )
        if outcome.page is None:
            return listing
        extractor.enrich_listing(outcome.page, listing)
        logger.info(
            "Listing detail enriched",
            site=site_config.name,
            strategy=self.name,
            mode=self.mode,
            ad_id=listing.ad_id,
        )
        return listing

    def _merge_listings(
        self,
        page_results: list[list[ScrapedListing] | BaseException],
        *,
        site_config: SiteConfig,
        today: str,
    ) -> list[ScrapedListing]:
        listings: list[ScrapedListing] = []
        seen: set[str] = set()
        blocked_error: StrategyBlockedError | None = None

        for result in page_results:
            if isinstance(result, StrategyBlockedError):
                blocked_error = result
                continue
            if isinstance(result, Exception):
                logger.warning(
                    "List page scrape failed",
                    site=site_config.name,
                    strategy=self.name,
                    mode=self.mode,
                    error=str(result),
                )
                continue
            for listing in result:
                dedupe_key = f"{site_config.name}:{listing.ad_id}"
                if dedupe_key in seen:
                    continue
                listing.date_seen = today
                listing.source_site = site_config.name
                seen.add(dedupe_key)
                listings.append(listing)

        if blocked_error is not None and not listings:
            raise blocked_error
        return listings

    def _raise_for_transport_or_block(
        self,
        *,
        site_name: str,
        url: str,
        status_code: int | None,
        reason: str | None,
    ) -> None:
        if status_code == 404:
            return
        if status_code in {403, 408, 425, 429, 500, 502, 503, 504}:
            raise StrategyBlockedError(
                site_name=site_name,
                mode=self.mode,
                url=url,
                reason=f"http_status:{status_code}",
            )
        if reason:
            raise StrategyBlockedError(
                site_name=site_name,
                mode=self.mode,
                url=url,
                reason=reason,
            )


class ScraplingHttpStrategy(SessionStrategy):
    name = "scrapling_http"
    mode = "http"

    def _strategy_concurrency(self) -> int:
        return max(1, self.settings.scrape_concurrency)


class ScraplingDynamicStrategy(SessionStrategy):
    name = "scrapling_dynamic"
    mode = "dynamic"

    def _strategy_concurrency(self) -> int:
        return max(1, self.settings.scrapling_dynamic_concurrency)


class ScraplingStealthStrategy(SessionStrategy):
    name = "scrapling_stealth"
    mode = "stealth"

    def _strategy_concurrency(self) -> int:
        return max(1, self.settings.scrapling_stealth_concurrency)
