from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence


@dataclass(frozen=True, slots=True)
class ListingIdentity:
    source_site: str
    external_id: str

    @classmethod
    def from_listing(cls, listing: "ScrapedListing") -> "ListingIdentity":
        return cls(
            source_site=(listing.source_site or "").strip().lower(),
            external_id=(listing.ad_id or "").strip(),
        )

    @property
    def storage_key(self) -> str:
        return f"{self.source_site}:{self.external_id}"

    @property
    def is_complete(self) -> bool:
        return bool(self.source_site and self.external_id)


@dataclass(slots=True)
class ScrapedListing:
    ad_id: str
    title: str
    price: str
    location: str
    size: str
    link: str
    image_url: str = ""
    source_site: str = ""
    phone: str = ""
    seller_name: str = ""
    ad_type: str = ""
    contact_name: str = "-"
    contact_email: str = "-"
    date_seen: str = ""
    price_raw: str = ""
    price_amount: Decimal | None = None
    currency: str = ""
    location_raw: str = ""
    size_raw: str = ""
    area_m2: Decimal | None = None

    @property
    def identity(self) -> ListingIdentity:
        return ListingIdentity.from_listing(self)


def to_listing_rows(rows: Sequence[ScrapedListing]) -> list[tuple[str, ...]]:
    return [
        (
            row.ad_id,
            row.date_seen,
            row.title,
            row.price,
            row.location,
            row.size,
            row.link,
            row.source_site,
            row.phone,
            row.seller_name,
            row.ad_type,
            row.contact_name,
            row.contact_email,
            row.price_raw,
            str(row.price_amount) if row.price_amount is not None else "",
            row.currency,
            row.location_raw,
            row.size_raw,
            str(row.area_m2) if row.area_m2 is not None else "",
        )
        for row in rows
    ]
