from __future__ import annotations

from app.core.config import Settings
from app.scraping.engine import ScrapingEngine
from app.scraping.fallback import FallbackManager
from app.scraping.models import ScrapedListing
from app.scraping.validation import RealEstateValidationLayer
from app.scraping.strategies import (
    ScraplingDynamicStrategy,
    ScraplingHttpStrategy,
    ScraplingStealthStrategy,
)


def build_scraping_engine(settings: Settings) -> ScrapingEngine:
    strategies = [ScraplingHttpStrategy(settings)]
    if settings.scrapling_dynamic_enabled:
        strategies.append(ScraplingDynamicStrategy(settings))
    if settings.scrapling_stealth_enabled:
        strategies.append(ScraplingStealthStrategy(settings))

    return ScrapingEngine(
        settings=settings,
        strategies=strategies,
        validator=RealEstateValidationLayer(),
        fallback_manager=FallbackManager(),
    )


__all__ = ["ScrapedListing", "ScrapingEngine", "build_scraping_engine"]
