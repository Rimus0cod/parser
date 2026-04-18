from __future__ import annotations

from app.core.config import Settings
from app.scraping.engine import ScrapingEngine
from app.scraping.fallback import FallbackManager
from app.scraping.models import ScrapedListing
from app.scraping.strategies import (
    AIStrategy,
    BrowserStrategy,
    HttpStrategy,
    ScraplingDynamicStrategy,
    ScraplingHttpStrategy,
    ScraplingStealthStrategy,
)
from app.scraping.validation import RealEstateValidationLayer


def build_scraping_engine(settings: Settings) -> ScrapingEngine:
    strategies = [HttpStrategy(settings)]
    if bool(getattr(settings, "browser_strategy_enabled", False) or getattr(settings, "scrapling_dynamic_enabled", False)):
        strategies.append(BrowserStrategy(settings))
    if bool(getattr(settings, "ai_strategy_enabled", False) or getattr(settings, "scrapling_stealth_enabled", False)):
        strategies.append(AIStrategy(settings))

    return ScrapingEngine(
        settings=settings,
        strategies=strategies,
        validator=RealEstateValidationLayer(),
        fallback_manager=FallbackManager(),
    )


__all__ = [
    "AIStrategy",
    "BrowserStrategy",
    "HttpStrategy",
    "ScrapedListing",
    "ScrapingEngine",
    "ScraplingDynamicStrategy",
    "ScraplingHttpStrategy",
    "ScraplingStealthStrategy",
    "build_scraping_engine",
]
