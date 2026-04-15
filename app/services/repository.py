from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from app.db.mysql import mysql_pool

if TYPE_CHECKING:
    from app.scraping.contracts import ScrapeExecutionResult
    from app.scraping.models import ScrapedListing

logger = logging.getLogger(__name__)
_MISSING = object()


async def upsert_leads(rows: list["ScrapedListing"]) -> int:
    if not rows:
        return 0

    from app.scraping.models import to_listing_rows

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


async def record_scrape_execution(execution: "ScrapeExecutionResult") -> None:
    if not execution.site_results:
        return

    sql = """
    INSERT INTO scrape_runs (
        source_site,
        strategy_name,
        mode_used,
        accepted_count,
        rejected_count,
        status,
        error_summary
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    rows = [
        (
            result.site_name,
            result.strategy_name,
            result.mode_used,
            result.accepted_count,
            result.rejected_count,
            "error" if result.errors else "ok",
            "; ".join(result.errors)[:2000] if result.errors else None,
        )
        for result in execution.site_results
    ]

    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, rows)


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
    return [dict(zip(keys, row)) for row in rows]


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
    return [dict(zip(keys, row)) for row in rows]


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
    return [dict(zip(keys, row)) for row in rows]


async def get_listing_by_ad_id(ad_id: str) -> dict[str, Any] | None:
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT ad_id, date_seen, title, price, location, size, link, source_site,
                           phone, seller_name, ad_type, contact_name, contact_email, updated_at
                    FROM listings
                    WHERE ad_id = %s
                    LIMIT 1
                    """,
                    (ad_id,),
                )
                row = await cur.fetchone()

    if row is None:
        return None

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
    return dict(zip(keys, row))


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
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO voice_calls (
                        source_type,
                        listing_ad_id,
                        tenant_contact_id,
                        contact_name,
                        phone_raw,
                        phone_e164,
                        status,
                        script_name,
                        initiated_by
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        source_type,
                        listing_ad_id,
                        tenant_contact_id,
                        contact_name,
                        phone_raw,
                        phone_e164,
                        status,
                        script_name,
                        initiated_by,
                    ),
                )
                return int(cur.lastrowid)


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
    assignments: list[str] = []
    values: list[Any] = []
    for column, value in (
        ("status", status),
        ("recording_url", recording_url),
        ("transcript", transcript),
        ("last_error", last_error),
        ("started_at", started_at),
        ("answered_at", answered_at),
        ("completed_at", completed_at),
        ("twilio_call_sid", twilio_call_sid),
    ):
        if value is _MISSING:
            continue
        assignments.append(f"{column} = %s")
        values.append(value)

    if answers_json is not _MISSING:
        assignments.append("answers_json = %s")
        values.append(json.dumps(answers_json or {}, ensure_ascii=False))

    if not assignments:
        return

    if voice_call_id is not None:
        where_clause = "id = %s"
        values.append(voice_call_id)
    elif twilio_call_sid is not _MISSING and twilio_call_sid is not None:
        where_clause = "twilio_call_sid = %s"
        values.append(twilio_call_sid)
    else:
        raise ValueError("Either voice_call_id or twilio_call_sid must be provided.")

    sql = f"UPDATE voice_calls SET {', '.join(assignments)} WHERE {where_clause}"
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, tuple(values))


def _voice_call_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    keys = [
        "id",
        "source_type",
        "listing_ad_id",
        "tenant_contact_id",
        "twilio_call_sid",
        "contact_name",
        "phone_raw",
        "phone_e164",
        "status",
        "script_name",
        "answers_json",
        "transcript",
        "recording_url",
        "last_error",
        "initiated_by",
        "started_at",
        "answered_at",
        "completed_at",
        "created_at",
        "updated_at",
        "listing_title",
        "listing_link",
    ]
    data = dict(zip(keys, row))
    raw_answers = data.get("answers_json")
    if isinstance(raw_answers, (bytes, bytearray)):
        raw_answers = raw_answers.decode("utf-8")
    if isinstance(raw_answers, str) and raw_answers:
        try:
            data["answers_json"] = json.loads(raw_answers)
        except json.JSONDecodeError:
            data["answers_json"] = {}
    else:
        data["answers_json"] = raw_answers or {}
    return data


async def list_voice_calls(limit: int = 100) -> list[dict[str, Any]]:
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT vc.id, vc.source_type, vc.listing_ad_id, vc.tenant_contact_id,
                           vc.twilio_call_sid, vc.contact_name, vc.phone_raw, vc.phone_e164,
                           vc.status, vc.script_name, vc.answers_json, vc.transcript,
                           vc.recording_url, vc.last_error, vc.initiated_by,
                           vc.started_at, vc.answered_at, vc.completed_at,
                           vc.created_at, vc.updated_at,
                           l.title AS listing_title, l.link AS listing_link
                    FROM voice_calls vc
                    LEFT JOIN listings l ON l.ad_id = vc.listing_ad_id
                    ORDER BY vc.created_at DESC, vc.id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
    return [_voice_call_from_row(row) for row in rows]


async def get_voice_call(voice_call_id: int) -> dict[str, Any] | None:
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT vc.id, vc.source_type, vc.listing_ad_id, vc.tenant_contact_id,
                           vc.twilio_call_sid, vc.contact_name, vc.phone_raw, vc.phone_e164,
                           vc.status, vc.script_name, vc.answers_json, vc.transcript,
                           vc.recording_url, vc.last_error, vc.initiated_by,
                           vc.started_at, vc.answered_at, vc.completed_at,
                           vc.created_at, vc.updated_at,
                           l.title AS listing_title, l.link AS listing_link
                    FROM voice_calls vc
                    LEFT JOIN listings l ON l.ad_id = vc.listing_ad_id
                    WHERE vc.id = %s
                    LIMIT 1
                    """,
                    (voice_call_id,),
                )
                row = await cur.fetchone()
    return _voice_call_from_row(row) if row else None


async def upsert_tenant_contacts(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO tenant_contacts (
        full_name,
        phone_raw,
        phone_normalized,
        phone_e164,
        notes,
        import_source,
        active
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        full_name = COALESCE(NULLIF(VALUES(full_name), ''), full_name),
        phone_raw = COALESCE(NULLIF(VALUES(phone_raw), ''), phone_raw),
        phone_e164 = COALESCE(NULLIF(VALUES(phone_e164), ''), phone_e164),
        notes = COALESCE(NULLIF(VALUES(notes), ''), notes),
        import_source = COALESCE(NULLIF(VALUES(import_source), ''), import_source),
        active = VALUES(active)
    """

    values = [
        (
            row.get("full_name", ""),
            row.get("phone_raw", ""),
            row.get("phone_normalized", ""),
            row.get("phone_e164", ""),
            row.get("notes", ""),
            row.get("import_source", ""),
            bool(row.get("active", True)),
        )
        for row in rows
        if row.get("phone_normalized")
    ]
    if not values:
        return 0

    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, values)
    return len(values)


async def list_tenant_contacts(limit: int = 100) -> list[dict[str, Any]]:
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, full_name, phone_raw, phone_normalized, phone_e164,
                           notes, import_source, active, created_at, updated_at
                    FROM tenant_contacts
                    ORDER BY updated_at DESC, id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()

    keys = [
        "id",
        "full_name",
        "phone_raw",
        "phone_normalized",
        "phone_e164",
        "notes",
        "import_source",
        "active",
        "created_at",
        "updated_at",
    ]
    return [dict(zip(keys, row)) for row in rows]
