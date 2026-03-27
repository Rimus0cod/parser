from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from app.db.mysql import mysql_pool
from app.services.async_scraper import ScrapedListing, to_listing_rows

logger = logging.getLogger(__name__)


async def upsert_leads(rows: list[ScrapedListing]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO listings (
        ad_id, date_seen, title, price, location, size, link, source_site, phone,
        seller_name, ad_type, contact_name, contact_email
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        date_seen = VALUES(date_seen),
        title = VALUES(title),
        price = VALUES(price),
        location = VALUES(location),
        size = VALUES(size),
        link = VALUES(link),
        source_site = VALUES(source_site),
        phone = VALUES(phone),
        seller_name = VALUES(seller_name),
        ad_type = VALUES(ad_type),
        contact_name = VALUES(contact_name),
        contact_email = VALUES(contact_email)
    """

    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, to_listing_rows(rows))
    return len(rows)


async def list_leads(limit: int = 100) -> list[dict[str, Any]]:
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT ad_id, date_seen, title, price, location, size, link, source_site,
                           phone, seller_name, ad_type, contact_name, contact_email, updated_at
                    FROM listings
                    ORDER BY date_seen DESC, updated_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()

    keys = [
        "ad_id",
        "date_seen",
        "title",
        "price",
        "location",
        "size",
        "link",
        "source_site",
        "phone",
        "seller_name",
        "ad_type",
        "contact_name",
        "contact_email",
        "updated_at",
    ]
    return [dict(zip(keys, row, strict=False)) for row in rows]


async def list_agencies(limit: int = 100) -> list[dict[str, Any]]:
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, agency_name, phones, city, email, contact_name, updated_at
                    FROM agencies
                    ORDER BY agency_name
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()

    keys = ["id", "agency_name", "phones", "city", "email", "contact_name", "updated_at"]
    return [dict(zip(keys, row, strict=False)) for row in rows]


async def list_leads_by_city_and_days(city: str | None, days: int) -> list[dict[str, Any]]:
    start_date = date.today() - timedelta(days=max(1, days))

    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                if city:
                    await cur.execute(
                        """
                        SELECT ad_id, date_seen, title, price, location, size, link, source_site,
                               phone, seller_name, ad_type, contact_name, contact_email, updated_at
                        FROM listings
                        WHERE date_seen >= %s AND location LIKE %s
                        ORDER BY date_seen DESC, updated_at DESC
                        """,
                        (start_date.isoformat(), f"%{city}%"),
                    )
                else:
                    await cur.execute(
                        """
                        SELECT ad_id, date_seen, title, price, location, size, link, source_site,
                               phone, seller_name, ad_type, contact_name, contact_email, updated_at
                        FROM listings
                        WHERE date_seen >= %s
                        ORDER BY date_seen DESC, updated_at DESC
                        """,
                        (start_date.isoformat(),),
                    )
                rows = await cur.fetchall()

    keys = [
        "ad_id",
        "date_seen",
        "title",
        "price",
        "location",
        "size",
        "link",
        "source_site",
        "phone",
        "seller_name",
        "ad_type",
        "contact_name",
        "contact_email",
        "updated_at",
    ]
    return [dict(zip(keys, row, strict=False)) for row in rows]
