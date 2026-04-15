from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from app.core.config import SiteConfig
    from app.scraping.models import ScrapedListing


class IssueSeverity(str, Enum):
    warning = "warning"
    error = "error"


@dataclass(slots=True)
class ValidationIssue:
    code: str
    message: str
    field_name: str = ""
    severity: IssueSeverity = IssueSeverity.warning


@dataclass(slots=True)
class ListingEnvelope:
    listing: "ScrapedListing"
    issues: list[ValidationIssue] = field(default_factory=list)
    strategy_name: str = ""
    mode: str = "http"
    fallback_action: str = "accept"

    @property
    def is_valid(self) -> bool:
        return not any(issue.severity == IssueSeverity.error for issue in self.issues)


@dataclass(slots=True)
class ScrapeSiteResult:
    site_name: str
    accepted: list[ListingEnvelope] = field(default_factory=list)
    rejected: list[ListingEnvelope] = field(default_factory=list)
    strategy_name: str = ""
    mode_used: str = "http"
    errors: list[str] = field(default_factory=list)

    @property
    def accepted_count(self) -> int:
        return len(self.accepted)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)


@dataclass(slots=True)
class ScrapeExecutionResult:
    site_results: list[ScrapeSiteResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def listings(self) -> list["ScrapedListing"]:
        return [envelope.listing for result in self.site_results for envelope in result.accepted]

    @property
    def accepted_count(self) -> int:
        return sum(result.accepted_count for result in self.site_results)

    @property
    def rejected_count(self) -> int:
        return sum(result.rejected_count for result in self.site_results)


class SiteScrapeStrategy(Protocol):
    name: str
    mode: str

    def supports(self, site_config: "SiteConfig") -> bool: ...

    async def scrape_site(self, site_config: "SiteConfig") -> list["ScrapedListing"]: ...
