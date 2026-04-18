from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from app.core.logging import get_logger
from app.scraping.contracts import (
    ListingEnvelope,
    ScrapeExecutionResult,
    ScrapeSiteResult,
    SiteScrapeStrategy,
)
from app.scraping.fallback import FallbackAction, FallbackManager
from app.scraping.fetchers import StrategyBlockedError
from app.scraping.validation import RealEstateValidationLayer, ValidationLayer

if TYPE_CHECKING:
    from app.core.config import Settings, SiteConfig
    from app.scraping.models import ScrapedListing

logger = get_logger("scraping_engine")


class ScrapingEngine:
    def __init__(
        self,
        *,
        settings: Settings,
        strategies: list[SiteScrapeStrategy],
        validator: ValidationLayer | None = None,
        fallback_manager: FallbackManager | None = None,
    ) -> None:
        self.settings = settings
        self.strategies = strategies
        self.validator = validator or RealEstateValidationLayer()
        self.fallback_manager = fallback_manager or FallbackManager()

    async def scrape_site(self, site_config: SiteConfig) -> ScrapeSiteResult:
        site_errors: list[str] = []

        supported_strategies = [strategy for strategy in self.strategies if strategy.supports(site_config)]
        if not supported_strategies:
            return ScrapeSiteResult(
                site_name=site_config.name,
                errors=["No scraping strategy supports this site configuration."],
            )

        for index, strategy in enumerate(supported_strategies):
            has_next_strategy = index < len(supported_strategies) - 1
            try:
                logger.info(
                    "Running site strategy",
                    site=site_config.name,
                    strategy=strategy.name,
                    mode=strategy.mode,
                    attempt=index + 1,
                    total_attempts=len(supported_strategies),
                )
                listings = await strategy.scrape_site(site_config)
            except StrategyBlockedError as exc:
                message = f"{strategy.name} blocked: {exc.reason}"
                site_errors.append(message)
                logger.warning(
                    "Site strategy blocked",
                    site=site_config.name,
                    strategy=strategy.name,
                    mode=strategy.mode,
                    attempt=index + 1,
                    blocked_reason=exc.reason,
                    blocked_url=exc.url,
                )
                continue
            except Exception as exc:  # noqa: BLE001
                message = f"{strategy.name} failed: {exc}"
                site_errors.append(message)
                logger.warning(
                    "Site strategy failed",
                    site=site_config.name,
                    strategy=strategy.name,
                    mode=strategy.mode,
                    attempt=index + 1,
                    error=str(exc),
                )
                continue

            accepted: list[ListingEnvelope] = []
            rejected: list[ListingEnvelope] = []

            for listing in listings:
                issues = self.validator.validate(listing)
                decision = self.fallback_manager.decide(
                    issues=issues,
                    has_next_strategy=has_next_strategy,
                )
                envelope = ListingEnvelope(
                    listing=listing,
                    issues=issues,
                    strategy_name=strategy.name,
                    mode=strategy.mode,
                    fallback_action=decision.action.value,
                )

                if decision.action == FallbackAction.drop:
                    rejected.append(envelope)
                    continue
                if decision.action == FallbackAction.retry_next_strategy:
                    rejected.append(envelope)
                    continue
                accepted.append(envelope)

            if accepted or not has_next_strategy:
                return ScrapeSiteResult(
                    site_name=site_config.name,
                    accepted=accepted,
                    rejected=rejected,
                    strategy_name=strategy.name,
                    mode_used=strategy.mode,
                    errors=site_errors,
                )

            logger.info(
                "Retrying site with next strategy",
                site=site_config.name,
                strategy=strategy.name,
                mode=strategy.mode,
                attempt=index + 1,
                rejected=len(rejected),
            )

        return ScrapeSiteResult(
            site_name=site_config.name,
            errors=site_errors or ["All scraping strategies failed."],
        )

    async def scrape_all_sites(self) -> ScrapeExecutionResult:
        enabled_sites = [site for site in self.settings.sites if site.enabled]
        site_semaphore = asyncio.Semaphore(max(1, int(getattr(self.settings, "scrape_concurrency", 8))))

        async def _bounded_scrape(site_config: SiteConfig) -> ScrapeSiteResult:
            async with site_semaphore:
                return await self.scrape_site(site_config)

        results = await asyncio.gather(
            *(_bounded_scrape(site) for site in enabled_sites),
            return_exceptions=True,
        )

        site_results: list[ScrapeSiteResult] = []
        execution_errors: list[str] = []
        deduplicated: dict[str, ListingEnvelope] = {}

        for site_config, result in zip(enabled_sites, results, strict=False):
            if isinstance(result, Exception):
                message = f"{site_config.name} failed: {result}"
                execution_errors.append(message)
                logger.exception("Site scrape crashed", site=site_config.name, error=str(result))
                continue

            site_results.append(result)
            for envelope in result.accepted:
                dedupe_key = self._dedupe_key(envelope.listing)
                deduplicated[dedupe_key] = envelope

            logger.info(
                "Site scrape validated",
                site=result.site_name,
                strategy=result.strategy_name,
                mode=result.mode_used,
                accepted=result.accepted_count,
                rejected=result.rejected_count,
            )

        return ScrapeExecutionResult(
            site_results=[
                ScrapeSiteResult(
                    site_name=result.site_name,
                    accepted=[
                        envelope
                        for envelope in result.accepted
                        if self._dedupe_key(envelope.listing) in deduplicated
                        and deduplicated[self._dedupe_key(envelope.listing)] is envelope
                    ],
                    rejected=result.rejected,
                    strategy_name=result.strategy_name,
                    mode_used=result.mode_used,
                    errors=result.errors,
                )
                for result in site_results
            ],
            errors=execution_errors,
        )

    def _dedupe_key(self, listing: ScrapedListing) -> str:
        site = (listing.source_site or "").strip().lower()
        return f"{site}:{listing.ad_id}"
