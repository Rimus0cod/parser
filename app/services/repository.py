from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import select, update, desc, cast, String
from sqlalchemy.dialects.mysql import insert

from app.db.mysql import AsyncSessionLocal
from app.models.domain import Listing, Agency, VoiceCall, TenantContact
from app.services.async_scraper import ScrapedListing

logger = logging.getLogger(__name__)
_MISSING = object()


async def upsert_leads(rows: list[ScrapedListing]) -> int:
    if not rows:
        return 0

    values = []
    for row in rows:
        values.append({
            "ad_id": str(row.ad_id),
            "date_seen": str(getattr(row, 'date', '')),
            "title": getattr(row, 'title', ''),
            "price": str(getattr(row, 'price', '')),
            "location": getattr(row, 'location', ''),
            "size": str(getattr(row, 'size', '')),
            "link": getattr(row, 'link', ''),
            "source_site": getattr(row, 'source_site', ''),
            "phone": getattr(row, 'phone', ''),
            "seller_name": getattr(row, 'seller_name', ''),
            "ad_type": getattr(row, 'ad_type', ''),
            "contact_name": getattr(row, 'contact_name', ''),
            "contact_email": getattr(row, 'contact_email', ''),
        })

    async with AsyncSessionLocal() as session:
        stmt = insert(Listing).values(values)
        stmt = stmt.on_duplicate_key_update(
            date_seen=stmt.inserted.date_seen,
            title=stmt.inserted.title,
            price=stmt.inserted.price,
            location=stmt.inserted.location,
            size=stmt.inserted.size,
            link=stmt.inserted.link,
            source_site=stmt.inserted.source_site,
            phone=stmt.inserted.phone,
            seller_name=stmt.inserted.seller_name,
            ad_type=stmt.inserted.ad_type,
            contact_name=stmt.inserted.contact_name,
            contact_email=stmt.inserted.contact_email,
        )
        await session.execute(stmt)
        await session.commit()
    return len(rows)


async def list_leads(limit: int = 100) -> list[dict[str, Any]]:
    async with AsyncSessionLocal() as session:
        stmt = select(Listing).order_by(desc(Listing.date_seen), desc(Listing.updated_at)).limit(limit)
        result = await session.execute(stmt)
        listings = result.scalars().all()
        return [
            {
                "ad_id": l.ad_id,
                "date_seen": l.date_seen,
                "title": l.title,
                "price": l.price,
                "location": l.location,
                "size": l.size,
                "link": l.link,
                "source_site": getattr(l, "source_site", ""),
                "phone": l.phone,
                "seller_name": l.seller_name,
                "ad_type": l.ad_type,
                "contact_name": l.contact_name,
                "contact_email": l.contact_email,
                "updated_at": l.updated_at,
            } for l in listings
        ]


async def list_agencies(limit: int = 100) -> list[dict[str, Any]]:
    async with AsyncSessionLocal() as session:
        stmt = select(Agency).order_by(Agency.name).limit(limit)
        result = await session.execute(stmt)
        agencies = result.scalars().all()
        return [
            {
                "id": a.id,
                "agency_name": a.name,
                "phones": a.phone,
                "city": getattr(a, "city", ""),
                "email": a.email,
                "contact_name": getattr(a, "contact_name", ""),
                "updated_at": a.last_updated,
            } for a in agencies
        ]


async def list_leads_by_city_and_days(city: str | None, days: int) -> list[dict[str, Any]]:
    start_date = date.today() - timedelta(days=max(1, days))
    async with AsyncSessionLocal() as session:
        stmt = select(Listing).where(Listing.date_seen >= str(start_date))
        if city:
            stmt = stmt.where(Listing.location.ilike(f"%{city}%"))
        stmt = stmt.order_by(desc(Listing.date_seen), desc(Listing.updated_at))
        
        result = await session.execute(stmt)
        listings = result.scalars().all()
        return [
            {
                "ad_id": l.ad_id,
                "date_seen": l.date_seen,
                "title": l.title,
                "price": l.price,
                "location": l.location,
                "size": l.size,
                "link": l.link,
                "source_site": getattr(l, "source_site", ""),
                "phone": l.phone,
                "seller_name": l.seller_name,
                "ad_type": l.ad_type,
                "contact_name": l.contact_name,
                "contact_email": l.contact_email,
                "updated_at": l.updated_at,
            } for l in listings
        ]


async def get_listing_by_ad_id(ad_id: str) -> dict[str, Any] | None:
    async with AsyncSessionLocal() as session:
        stmt = select(Listing).where(Listing.ad_id == ad_id)
        result = await session.execute(stmt)
        l = result.scalar_one_or_none()
        if not l:
            return None
        return {
            "ad_id": l.ad_id,
            "date_seen": l.date_seen,
            "title": l.title,
            "price": l.price,
            "location": l.location,
            "size": l.size,
            "link": l.link,
            "source_site": getattr(l, "source_site", ""),
            "phone": l.phone,
            "seller_name": l.seller_name,
            "ad_type": l.ad_type,
            "contact_name": l.contact_name,
            "contact_email": l.contact_email,
            "updated_at": l.updated_at,
        }


async def create_voice_call(
    *,
    source_type: str,
    listing_ad_id: str | None,
    tenant_contact_id: int | None,
    contact_name: str,
    phone_raw: str,
    phone_e164: str,
    status: str,
    script_name: str,
    initiated_by: str,
) -> int:
    async with AsyncSessionLocal() as session:
        call = VoiceCall(
            source_type=source_type,
            listing_ad_id=listing_ad_id,
            tenant_contact_id=tenant_contact_id,
            contact_name=contact_name,
            phone_raw=phone_raw,
            phone_e164=phone_e164,
            status=status,
            script_name=script_name,
            initiated_by=initiated_by
        )
        session.add(call)
        await session.commit()
        await session.refresh(call)
        return call.id


async def patch_voice_call(
    *,
    voice_call_id: int | None = None,
    twilio_call_sid: str | None | object = _MISSING,
    status: str | object = _MISSING,
    recording_url: str | None | object = _MISSING,
    transcript: str | None | object = _MISSING,
    answers_json: dict[str, str] | None | object = _MISSING,
    last_error: str | None | object = _MISSING,
    started_at: datetime | None | object = _MISSING,
    answered_at: datetime | None | object = _MISSING,
    completed_at: datetime | None | object = _MISSING,
) -> None:
    values = {}
    if status is not _MISSING:
        values["status"] = status
    if twilio_call_sid is not _MISSING:
        values["twilio_call_sid"] = twilio_call_sid
    if recording_url is not _MISSING:
        values["recording_url"] = recording_url
    if transcript is not _MISSING:
        values["transcript"] = transcript
    if last_error is not _MISSING:
        values["last_error"] = last_error
    if started_at is not _MISSING:
        values["started_at"] = started_at
    if answered_at is not _MISSING:
        values["answered_at"] = answered_at
    if completed_at is not _MISSING:
        values["completed_at"] = completed_at
    if answers_json is not _MISSING:
        values["answers_json"] = answers_json or {}
        
    if not values:
        return
        
    async with AsyncSessionLocal() as session:
        stmt = update(VoiceCall)
        if voice_call_id is not None:
            stmt = stmt.where(VoiceCall.id == voice_call_id)
        elif twilio_call_sid is not _MISSING and twilio_call_sid is not None:
            stmt = stmt.where(VoiceCall.twilio_call_sid == twilio_call_sid)
        else:
            raise ValueError("Either voice_call_id or twilio_call_sid must be provided.")
            
        stmt = stmt.values(**values)
        await session.execute(stmt)
        await session.commit()


def _voice_call_to_dict(vc: VoiceCall, listing_title: str | None = None, listing_link: str | None = None) -> dict[str, Any]:
    return {
        "id": vc.id,
        "source_type": vc.source_type,
        "listing_ad_id": vc.listing_ad_id,
        "tenant_contact_id": vc.tenant_contact_id,
        "twilio_call_sid": vc.twilio_call_sid,
        "contact_name": vc.contact_name,
        "phone_raw": vc.phone_raw,
        "phone_e164": vc.phone_e164,
        "status": vc.status,
        "script_name": vc.script_name,
        "answers_json": vc.answers_json or {},
        "transcript": vc.transcript,
        "recording_url": vc.recording_url,
        "last_error": getattr(vc, "last_error", None),
        "initiated_by": getattr(vc, "initiated_by", getattr(vc, "source_type", "")),
        "started_at": getattr(vc, "started_at", None),
        "answered_at": getattr(vc, "answered_at", None),
        "completed_at": getattr(vc, "completed_at", None),
        "created_at": getattr(vc, "created_at", None),
        "updated_at": getattr(vc, "updated_at", None),
        "listing_title": listing_title,
        "listing_link": listing_link
    }


async def list_voice_calls(limit: int = 100) -> list[dict[str, Any]]:
    async with AsyncSessionLocal() as session:
        stmt = select(VoiceCall, Listing).outerjoin(
            Listing, VoiceCall.listing_ad_id == Listing.ad_id
        ).order_by(desc(VoiceCall.created_at), desc(VoiceCall.id)).limit(limit)
        
        result = await session.execute(stmt)
        
        calls = []
        for vc, listing in result.all():
            l_title = listing.title if listing else None
            l_link = listing.link if listing else None
            calls.append(_voice_call_to_dict(vc, l_title, l_link))
        return calls


async def get_voice_call(voice_call_id: int) -> dict[str, Any] | None:
    async with AsyncSessionLocal() as session:
        stmt = select(VoiceCall, Listing).outerjoin(
            Listing, VoiceCall.listing_ad_id == Listing.ad_id
        ).where(VoiceCall.id == voice_call_id)
        
        result = await session.execute(stmt)
        row = result.first()
        if not row:
            return None
        vc, listing = row
        l_title = listing.title if listing else None
        l_link = listing.link if listing else None
        return _voice_call_to_dict(vc, l_title, l_link)


async def upsert_tenant_contacts(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    values = [
        {
            "full_name": row.get("full_name", ""),
            "phone_raw": row.get("phone_raw", ""),
            "phone_normalized": row.get("phone_normalized", ""),
            "phone_e164": row.get("phone_e164", ""),
            "notes": row.get("notes", ""),
            "import_source": row.get("import_source", ""),
            "active": bool(row.get("active", True)),
        }
        for row in rows
        if row.get("phone_normalized")
    ]
    if not values:
        return 0

    async with AsyncSessionLocal() as session:
        stmt = insert(TenantContact).values(values)
        stmt = stmt.on_duplicate_key_update(
            full_name=stmt.inserted.full_name,
            phone_raw=stmt.inserted.phone_raw,
            phone_e164=stmt.inserted.phone_e164,
            notes=stmt.inserted.notes,
            import_source=stmt.inserted.import_source,
            active=stmt.inserted.active
        )
        await session.execute(stmt)
        await session.commit()
    return len(values)


async def list_tenant_contacts(limit: int = 100) -> list[dict[str, Any]]:
    async with AsyncSessionLocal() as session:
        stmt = select(TenantContact).order_by(desc(TenantContact.updated_at), desc(TenantContact.id)).limit(limit)
        result = await session.execute(stmt)
        contacts = result.scalars().all()
        return [
            {
                "id": c.id,
                "full_name": c.full_name,
                "phone_raw": getattr(c, "phone_raw", ""),
                "phone_normalized": getattr(c, "phone_normalized", ""),
                "phone_e164": c.phone_e164,
                "notes": getattr(c, "notes", ""),
                "import_source": getattr(c, "import_source", ""),
                "active": getattr(c, "active", True),
                "created_at": c.created_at,
                "updated_at": c.updated_at,
            } for c in contacts
        ]
