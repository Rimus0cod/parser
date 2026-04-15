from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


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
        )
        for row in rows
    ]
