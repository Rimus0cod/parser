from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


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


class VoiceCallCreateRequest(BaseModel):
    listing_ad_id: str
    initiated_by: str = "api"


class VoiceCall(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source_type: str
    listing_ad_id: str | None = None
    tenant_contact_id: int | None = None
    twilio_call_sid: str | None = None
    contact_name: str = ""
    phone_raw: str = ""
    phone_e164: str = ""
    status: str
    script_name: str
    answers_json: dict[str, str] = Field(default_factory=dict)
    transcript: str | None = None
    recording_url: HttpUrl | str | None = None
    last_error: str | None = None
    initiated_by: str = ""
    started_at: datetime | None = None
    answered_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    listing_title: str | None = None
    listing_link: HttpUrl | str | None = None


class TenantContact(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    full_name: str = ""
    phone_raw: str
    phone_normalized: str
    phone_e164: str = ""
    notes: str = ""
    import_source: str = ""
    active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None


class TenantContactImportResponse(BaseModel):
    imported: int
    skipped: int
