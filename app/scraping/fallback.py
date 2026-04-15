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
    def decide(
        self,
        *,
        issues: list[ValidationIssue],
        has_next_strategy: bool,
    ) -> FallbackDecision:
        has_errors = any(issue.severity == IssueSeverity.error for issue in issues)
        if not has_errors:
            return FallbackDecision(action=FallbackAction.accept, reason="validation_passed")

        if has_next_strategy:
            return FallbackDecision(
                action=FallbackAction.retry_next_strategy,
                reason="hard_validation_error_retrying_next_strategy",
            )

        return FallbackDecision(action=FallbackAction.drop, reason="hard_validation_error")
