from __future__ import annotations

import re
from typing import Protocol
from urllib.parse import urlparse

from app.scraping.contracts import IssueSeverity, ValidationIssue


class ValidationLayer(Protocol):
    def validate(self, listing: object) -> list[ValidationIssue]: ...


class RealEstateValidationLayer:
    def validate(self, listing: object) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        ad_id = getattr(listing, "ad_id", "")
        title = getattr(listing, "title", "")
        link = getattr(listing, "link", "")
        price = getattr(listing, "price", "")
        location = getattr(listing, "location", "")
        size = getattr(listing, "size", "")
        phone = getattr(listing, "phone", "")
        price_amount = getattr(listing, "price_amount", None)
        currency = getattr(listing, "currency", "")
        area_m2 = getattr(listing, "area_m2", None)

        if not ad_id:
            issues.append(
                ValidationIssue(
                    code="missing_ad_id",
                    message="Listing has no stable external identifier.",
                    field_name="ad_id",
                    severity=IssueSeverity.error,
                )
            )

        if not title:
            issues.append(
                ValidationIssue(
                    code="missing_title",
                    message="Listing title is empty.",
                    field_name="title",
                    severity=IssueSeverity.error,
                )
            )
        elif len(title.strip()) < 8:
            issues.append(
                ValidationIssue(
                    code="short_title",
                    message="Listing title is unexpectedly short.",
                    field_name="title",
                )
            )

        parsed = urlparse(link or "")
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            issues.append(
                ValidationIssue(
                    code="invalid_link",
                    message="Listing link is missing or invalid.",
                    field_name="link",
                    severity=IssueSeverity.error,
                )
            )

        if not price:
            issues.append(
                ValidationIssue(
                    code="missing_price",
                    message="Price was not extracted.",
                    field_name="price",
                )
            )
        elif re.search(r"\d", price) is None:
            issues.append(
                ValidationIssue(
                    code="invalid_price",
                    message="Price value does not contain digits.",
                    field_name="price",
                )
            )
        elif price_amount is None or not currency:
            issues.append(
                ValidationIssue(
                    code="unnormalized_price",
                    message="Price was extracted but amount or currency normalization failed.",
                    field_name="price",
                )
            )

        if not location:
            issues.append(
                ValidationIssue(
                    code="missing_location",
                    message="Location was not extracted.",
                    field_name="location",
                )
            )

        if size and re.search(r"\d+(?:[.,]\d+)?", size) is None:
            issues.append(
                ValidationIssue(
                    code="invalid_size",
                    message="Size value does not contain a numeric area.",
                    field_name="size",
                )
            )
        elif size and area_m2 is None:
            issues.append(
                ValidationIssue(
                    code="unnormalized_size",
                    message="Size was extracted but area normalization failed.",
                    field_name="size",
                )
            )

        if phone and re.search(r"\d", phone) is None:
            issues.append(
                ValidationIssue(
                    code="invalid_phone",
                    message="Phone value does not contain digits.",
                    field_name="phone",
                )
            )
        if not phone:
            issues.append(
                ValidationIssue(
                    code="missing_phone",
                    message="Phone was not extracted.",
                    field_name="phone",
                )
            )

        return issues
