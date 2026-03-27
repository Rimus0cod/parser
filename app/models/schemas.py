from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, HttpUrl


class Lead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ad_id: str
    date_seen: date | None = None
    title: str
    price: str
    location: str
    size: str
    link: HttpUrl | str
    source_site: str = ""
    phone: str
    seller_name: str
    ad_type: str
    contact_name: str
    contact_email: str
    updated_at: datetime | None = None


class Agency(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    agency_name: str
    phones: str
    city: str
    email: str
    contact_name: str
    updated_at: datetime | None = None


class TriggerScrapeResponse(BaseModel):
    status: str
    message: str
