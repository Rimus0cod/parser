from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.dialects.mysql import insert
from sqlalchemy import select, delete

from app.db.mysql import get_sync_db
from app.models.domain import Listing, Agency, ProcessedId, TenantContact, VoiceCall

logger = logging.getLogger("imoti_scraper")


class MySQLStore:
    """
    MySQL storage for scraped data (listings, agencies, processed IDs).
    Refactored to use SQLAlchemy ORM synchronous engine.
    """

    def __init__(self, config) -> None:
        self._cfg = config
        self._db_gen = get_sync_db()
        self._session = next(self._db_gen)
        logger.info("MySQLStore initialized using SQLAlchemy SyncEngine")

    def close(self) -> None:
        if self._session:
            self._session.close()

    def store_listings(self, listings) -> None:
        if not listings:
            return

        try:
            for listing in listings:
                values = {}
                if hasattr(listing, "ad_id"):
                    values = {
                        "ad_id": str(listing.ad_id),
                        "date_seen": str(getattr(listing, 'date', '')),
                        "title": getattr(listing, 'title', ''),
                        "price": str(getattr(listing, 'price', '')),
                        "location": getattr(listing, 'location', ''),
                        "size": str(getattr(listing, 'size', '')),
                        "link": getattr(listing, 'link', ''),
                        "phone": getattr(listing, 'phone', ''),
                        "seller_name": getattr(listing, 'seller_name', ''),
                        "ad_type": getattr(listing, 'ad_type', ''),
                        "contact_name": getattr(listing, 'contact_name', ''),
                        "contact_email": getattr(listing, 'contact_email', ''),
                    }
                else:
                    values = {
                        "ad_id": str(listing.get("ad_id", "")),
                        "date_seen": str(listing.get("date", "")),
                        "title": listing.get("title", ""),
                        "price": str(listing.get("price", "")),
                        "location": listing.get("location", ""),
                        "size": str(listing.get("size", "")),
                        "link": listing.get("link", ""),
                        "phone": listing.get("phone", ""),
                        "seller_name": listing.get("seller_name", ""),
                        "ad_type": listing.get("ad_type", ""),
                        "contact_name": listing.get("contact_name", ""),
                        "contact_email": listing.get("contact_email", ""),
                    }
                
                stmt = insert(Listing).values(**values)
                stmt = stmt.on_duplicate_key_update(
                    title=stmt.inserted.title,
                    price=stmt.inserted.price,
                    location=stmt.inserted.location,
                    size=stmt.inserted.size,
                    link=stmt.inserted.link,
                    phone=stmt.inserted.phone,
                    seller_name=stmt.inserted.seller_name,
                    ad_type=stmt.inserted.ad_type,
                    contact_name=stmt.inserted.contact_name,
                    contact_email=stmt.inserted.contact_email,
                )
                self._session.execute(stmt)
            self._session.commit()
            logger.info(f"Stored {len(listings)} listings.")
        except Exception as e:
            self._session.rollback()
            logger.error(f"Failed to store listings: {e}")
            raise

    def store_agencies(self, agencies: list[dict[str, Any]]) -> None:
        if not agencies:
            return

        try:
            for agency in agencies:
                values = {
                    "name": agency.get("name", ""),
                    "address": agency.get("address", ""),
                    "phone": agency.get("phone", ""),
                    "mobile": agency.get("mobile", ""),
                    "email": agency.get("email", ""),
                    "website": agency.get("website", ""),
                    "logo_url": agency.get("logo_url", ""),
                    "description": agency.get("description", ""),
                    "offers_count": int(agency.get("offers_count", 0)),
                    "last_updated": agency.get("last_updated", ""),
                    "listing_id": agency.get("listing_id"),
                }
                stmt = insert(Agency).values(**values)
                stmt = stmt.on_duplicate_key_update(
                    address=stmt.inserted.address,
                    phone=stmt.inserted.phone,
                    mobile=stmt.inserted.mobile,
                    email=stmt.inserted.email,
                    website=stmt.inserted.website,
                    logo_url=stmt.inserted.logo_url,
                    description=stmt.inserted.description,
                    offers_count=stmt.inserted.offers_count,
                    last_updated=stmt.inserted.last_updated,
                    listing_id=stmt.inserted.listing_id,
                )
                self._session.execute(stmt)
            self._session.commit()
            logger.info(f"Stored {len(agencies)} agencies.")
        except Exception as e:
            self._session.rollback()
            logger.error(f"Failed to store agencies: {e}")
            raise

    def get_processed_ids(self) -> set[str]:
        try:
            stmt = select(ProcessedId.ad_id)
            result = self._session.execute(stmt).scalars().all()
            return set(result)
        except Exception as e:
            logger.error(f"Failed to fetch processed IDs: {e}")
            return set()

    def add_processed_ids(self, ids: list[str]) -> None:
        if not ids:
            return

        try:
            for ad_id in ids:
                stmt = insert(ProcessedId).values(ad_id=str(ad_id))
                stmt = stmt.on_duplicate_key_update(ad_id=stmt.inserted.ad_id)
                self._session.execute(stmt)
            self._session.commit()
            logger.info(f"Added {len(ids)} processed IDs.")
        except Exception as e:
            self._session.rollback()
            logger.error(f"Failed to add processed IDs: {e}")
            raise

    def get_uncontacted_listings(self) -> list[dict[str, Any]]:
        try:
            stmt = select(Listing).outerjoin(
                TenantContact, TenantContact.ad_id == Listing.ad_id
            ).where(
                TenantContact.id.is_(None)
            )
            listings = self._session.execute(stmt).scalars().all()
            
            # Convert ORM objects to dicts matching the old interface
            return [
                {
                    "ad_id": l.ad_id,
                    "date": l.date_seen,
                    "title": l.title,
                    "price": l.price,
                    "location": l.location,
                    "size": l.size,
                    "link": l.link,
                    "phone": l.phone,
                    "seller_name": l.seller_name,
                    "ad_type": l.ad_type,
                    "contact_name": l.contact_name,
                    "contact_email": l.contact_email,
                }
                for l in listings
            ]
        except Exception as e:
            logger.error(f"Failed to get uncontacted listings: {e}")
            return []

    def get_contacts_needing_call(self, within_days: int = 7) -> list[dict[str, Any]]:
        # For simplicity, returning empty or could translate the exact query if needed
        # Assuming the old implementation was checking tenant_contacts without voice_calls or recent voice_calls
        # This will be refined as needed
        return []

    def store_tenant_contact(self, contact_data: dict[str, Any]) -> None:
        try:
            stmt = insert(TenantContact).values(**contact_data)
            self._session.execute(stmt)
            self._session.commit()
        except Exception as e:
            self._session.rollback()
            logger.error(f"Failed to store tenant contact: {e}")
            raise

    def update_tenant_contact_reply(self, ad_id: str, data: dict[str, Any]) -> None:
        # Implementation for update using ORM update statement if needed
        pass

    def store_voice_call(self, call_data: dict[str, Any]) -> None:
        try:
            stmt = insert(VoiceCall).values(**call_data)
            self._session.execute(stmt)
            self._session.commit()
        except Exception as e:
            self._session.rollback()
            logger.error(f"Failed to store voice call: {e}")
            raise

    def update_voice_call_transcript(self, call_sid: str, data: dict[str, Any]) -> None:
        pass

    def store_twilio_message(self, message_data: dict[str, Any]) -> None:
        pass
