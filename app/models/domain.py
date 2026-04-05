from __future__ import annotations

import datetime
from sqlalchemy import String, Text, Date, DateTime, Integer, Boolean, JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class Listing(Base):
    __tablename__ = "listings"

    ad_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    date_seen: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    title: Mapped[str | None] = mapped_column(Text)
    price: Mapped[str | None] = mapped_column(String(100))
    location: Mapped[str | None] = mapped_column(String(255))
    size: Mapped[str | None] = mapped_column(String(50))
    link: Mapped[str | None] = mapped_column(Text)
    source_site: Mapped[str] = mapped_column(String(120), default="", server_default="")
    phone: Mapped[str | None] = mapped_column(String(50))
    seller_name: Mapped[str | None] = mapped_column(String(255))
    ad_type: Mapped[str | None] = mapped_column(String(50))
    contact_name: Mapped[str | None] = mapped_column(String(255))
    contact_email: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Agency(Base):
    __tablename__ = "agencies"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    agency_name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    phones: Mapped[str | None] = mapped_column(Text)
    city: Mapped[str | None] = mapped_column(String(100))
    email: Mapped[str | None] = mapped_column(String(255))
    contact_name: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class TenantContact(Base):
    __tablename__ = "tenant_contacts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    full_name: Mapped[str] = mapped_column(String(255), default="", server_default="")
    phone_raw: Mapped[str] = mapped_column(String(64), nullable=False)
    phone_normalized: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    phone_e164: Mapped[str] = mapped_column(String(32), default="", server_default="")
    notes: Mapped[str | None] = mapped_column(Text)
    import_source: Mapped[str] = mapped_column(String(255), default="", server_default="")
    active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="1")
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class VoiceCall(Base):
    __tablename__ = "voice_calls"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    listing_ad_id: Mapped[str | None] = mapped_column(String(50), index=True)
    tenant_contact_id: Mapped[int | None] = mapped_column(Integer, index=True)
    twilio_call_sid: Mapped[str | None] = mapped_column(String(64), unique=True)
    contact_name: Mapped[str] = mapped_column(String(255), default="", server_default="")
    phone_raw: Mapped[str] = mapped_column(String(64), default="", server_default="")
    phone_e164: Mapped[str] = mapped_column(String(32), default="", server_default="")
    status: Mapped[str] = mapped_column(String(50), default="queued", server_default="queued", index=True)
    script_name: Mapped[str] = mapped_column(String(120), default="", server_default="")
    answers_json: Mapped[dict | None] = mapped_column(JSON)
    transcript: Mapped[str | None] = mapped_column(Text)
    recording_url: Mapped[str | None] = mapped_column(Text)
    last_error: Mapped[str | None] = mapped_column(Text)
    initiated_by: Mapped[str] = mapped_column(String(120), default="", server_default="")
    started_at: Mapped[datetime.datetime | None] = mapped_column(DateTime)
    answered_at: Mapped[datetime.datetime | None] = mapped_column(DateTime)
    completed_at: Mapped[datetime.datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class ProcessedId(Base):
    __tablename__ = "processed_ids"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ad_id: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    processed_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
