from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from app.scraping.contracts import IssueSeverity, ValidationIssue


class FallbackAction(str, Enum):
    accept = "accept"
    drop = "drop"
    retry_next_strategy = "retry_next_strategy"
    manual_review = "manual_review"


@dataclass(slots=True)
class FallbackDecision:
    action: FallbackAction
    reason: str


class FallbackManager:
    _manual_review_warning_codes = {
        "missing_price",
        "invalid_price",
        "unnormalized_price",
        "missing_location",
        "invalid_size",
        "unnormalized_size",
        "invalid_phone",
        "missing_phone",
    }

    def decide(
        self,
        *,
        issues: list[ValidationIssue],
        has_next_strategy: bool,
    ) -> FallbackDecision:
        has_errors = any(issue.severity == IssueSeverity.error for issue in issues)
        if not has_errors:
            if any(issue.code in self._manual_review_warning_codes for issue in issues):
                return FallbackDecision(
                    action=FallbackAction.manual_review,
                    reason="validation_warning_requires_review",
                )
            return FallbackDecision(action=FallbackAction.accept, reason="validation_passed")

        if has_next_strategy:
            return FallbackDecision(
                action=FallbackAction.retry_next_strategy,
                reason="hard_validation_error_retrying_next_strategy",
            )

        return FallbackDecision(action=FallbackAction.drop, reason="hard_validation_error")
