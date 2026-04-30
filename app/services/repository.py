from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from app.core.config import get_settings
from app.db.mysql import mysql_pool

if TYPE_CHECKING:
    from app.scraping.contracts import ListingEnvelope, ScrapeExecutionResult, ScrapeSiteResult
    from app.scraping.models import ScrapedListing

logger = logging.getLogger(__name__)
_MISSING = object()
LISTING_STATUS_ACTIVE = "active"
LISTING_STATUS_MANUAL_REVIEW = "manual_review"
LISTING_STATUS_STALE = "stale"
LISTING_STATUS_REJECTED = "rejected"
STALE_STRATEGIES = {"mark", "delete"}
VISIBLE_LISTING_COLUMNS = """
    ad_id, date_seen, title, price, location, size, link, source_site,
    parser_version, record_status, phone, seller_name, ad_type, contact_name,
    contact_email, price_raw, price_amount, currency, location_raw, size_raw,
    area_m2, updated_at
"""
VISIBLE_LISTING_KEYS = [
    "ad_id",
    "date_seen",
    "title",
    "price",
    "location",
    "size",
    "link",
    "source_site",
    "parser_version",
    "record_status",
    "phone",
    "seller_name",
    "ad_type",
    "contact_name",
    "contact_email",
    "price_raw",
    "price_amount",
    "currency",
    "location_raw",
    "size_raw",
    "area_m2",
    "updated_at",
]


def _current_parser_version() -> str:
    return get_settings().scrape_data_version.strip() or "v2"


def _optional_decimal(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def _normalize_listing(
    row: "ScrapedListing", *, default_site: str | None = None
) -> "ScrapedListing | None":
    from app.scraping.models import ScrapedListing

    ad_id = str(row.ad_id or "").strip()
    source_site = str(row.source_site or default_site or "").strip().lower()
    if not ad_id or not source_site:
        logger.warning(
            "Skipping listing without stable storage identity",
            extra={"ad_id": ad_id, "source_site": source_site},
        )
        return None

    return ScrapedListing(
        ad_id=ad_id,
        date_seen=str(row.date_seen or date.today().isoformat()).strip()
        or date.today().isoformat(),
        title=str(row.title or "").strip(),
        price=str(row.price or "").strip(),
        location=str(row.location or "").strip(),
        size=str(row.size or "").strip(),
        link=str(row.link or "").strip(),
        image_url=str(row.image_url or "").strip(),
        source_site=source_site,
        phone=str(row.phone or "").strip(),
        seller_name=str(row.seller_name or "").strip(),
        ad_type=str(row.ad_type or "").strip(),
        contact_name=str(row.contact_name or "").strip() or "-",
        contact_email=str(row.contact_email or "").strip() or "-",
        price_raw=str(row.price_raw or row.price or "").strip(),
        price_amount=_optional_decimal(row.price_amount),
        currency=str(row.currency or "").strip().upper(),
        location_raw=str(row.location_raw or row.location or "").strip(),
        size_raw=str(row.size_raw or row.size or "").strip(),
        area_m2=_optional_decimal(row.area_m2),
    )


def _listing_row_values(
    normalized: "ScrapedListing",
    *,
    parser_version: str,
    record_status: str,
) -> tuple[Any, ...]:
    return (
        normalized.ad_id,
        normalized.date_seen,
        normalized.title,
        normalized.price,
        normalized.location,
        normalized.size,
        normalized.link,
        normalized.source_site,
        parser_version.strip() or _current_parser_version(),
        record_status,
        normalized.phone,
        normalized.seller_name,
        normalized.ad_type,
        normalized.contact_name,
        normalized.contact_email,
        normalized.price_raw or normalized.price,
        normalized.price_amount,
        normalized.currency,
        normalized.location_raw or normalized.location,
        normalized.size_raw or normalized.size,
        normalized.area_m2,
    )


def _to_listing_rows(
    rows: list["ScrapedListing"],
    *,
    parser_version: str,
    record_status: str = LISTING_STATUS_ACTIVE,
) -> list[tuple[Any, ...]]:
    normalized_rows: list[tuple[Any, ...]] = []
    active_parser_version = parser_version.strip() or _current_parser_version()
    for row in rows:
        normalized = _normalize_listing(row)
        if normalized is None:
            continue
        normalized_rows.append(
            _listing_row_values(
                normalized,
                parser_version=active_parser_version,
                record_status=record_status,
            )
        )
    return normalized_rows


def _envelope_record_status(envelope: "ListingEnvelope") -> str:
    if envelope.fallback_action == "manual_review":
        return LISTING_STATUS_MANUAL_REVIEW
    return LISTING_STATUS_ACTIVE


def _site_scrape_completed(result: "ScrapeSiteResult") -> bool:
    return bool(result.strategy_name)


def _deduplicated_ad_ids(rows: list["ScrapedListing"], *, site_name: str) -> list[str]:
    ad_ids: list[str] = []
    for row in rows:
        normalized = _normalize_listing(row, default_site=site_name)
        if normalized is None:
            continue
        ad_ids.append(normalized.ad_id)
    return list(dict.fromkeys(ad_ids))


async def upsert_leads(rows: list["ScrapedListing"], *, parser_version: str | None = None) -> int:
    if not rows:
        return 0

    active_parser_version = (
        parser_version or _current_parser_version()
    ).strip() or _current_parser_version()
    values = _to_listing_rows(rows, parser_version=active_parser_version)
    return await _upsert_listing_values(values)


async def _upsert_listing_values(values: list[tuple[Any, ...]]) -> int:
    if not values:
        return 0

    sql = """
    INSERT INTO listings (
        ad_id, date_seen, title, price, location, size, link, source_site,
        parser_version, record_status, phone, seller_name, ad_type, contact_name, contact_email,
        price_raw, price_amount, currency, location_raw, size_raw, area_m2
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        date_seen = VALUES(date_seen),
        title = VALUES(title),
        price = VALUES(price),
        location = VALUES(location),
        size = VALUES(size),
        link = VALUES(link),
        source_site = VALUES(source_site),
        parser_version = VALUES(parser_version),
        record_status = VALUES(record_status),
        phone = VALUES(phone),
        seller_name = VALUES(seller_name),
        ad_type = VALUES(ad_type),
        contact_name = VALUES(contact_name),
        contact_email = VALUES(contact_email),
        price_raw = VALUES(price_raw),
        price_amount = VALUES(price_amount),
        currency = VALUES(currency),
        location_raw = VALUES(location_raw),
        size_raw = VALUES(size_raw),
        area_m2 = VALUES(area_m2)
    """

    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, values)
    return len(values)


async def refresh_leads(
    execution: "ScrapeExecutionResult",
    *,
    parser_version: str | None = None,
    stale_strategy: str = "mark",
) -> int:
    active_parser_version = (
        parser_version or _current_parser_version()
    ).strip() or _current_parser_version()
    normalized_strategy = stale_strategy.strip().lower()
    if normalized_strategy not in STALE_STRATEGIES:
        raise ValueError(f"Unsupported stale strategy '{stale_strategy}'.")

    written = 0
    for result in execution.site_results:
        if not _site_scrape_completed(result):
            continue

        site_rows: list[ScrapedListing] = []
        site_values: list[tuple[Any, ...]] = []
        for envelope in result.accepted:
            normalized = _normalize_listing(envelope.listing, default_site=result.site_name)
            if normalized is None:
                continue
            site_rows.append(normalized)
            site_values.append(
                _listing_row_values(
                    normalized,
                    parser_version=active_parser_version,
                    record_status=_envelope_record_status(envelope),
                )
            )

        written += await _upsert_listing_values(site_values)
        await _replace_listing_issues(result, parser_version=active_parser_version)
        await _cleanup_site_listings(
            site_name=result.site_name,
            active_ad_ids=_deduplicated_ad_ids(site_rows, site_name=result.site_name),
            parser_version=active_parser_version,
            stale_strategy=normalized_strategy,
        )

    return written


async def _cleanup_site_listings(
    *,
    site_name: str,
    active_ad_ids: list[str],
    parser_version: str,
    stale_strategy: str,
) -> None:
    normalized_site = site_name.strip().lower()
    if not normalized_site:
        return

    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                if active_ad_ids:
                    placeholders = ", ".join(["%s"] * len(active_ad_ids))
                    if stale_strategy == "delete":
                        sql = f"""
                        DELETE FROM listings
                        WHERE source_site = %s
                          AND (
                              parser_version <> %s
                              OR ad_id NOT IN ({placeholders})
                          )
                        """
                        params = (normalized_site, parser_version, *active_ad_ids)
                    else:
                        sql = f"""
                        UPDATE listings
                        SET record_status = %s
                        WHERE source_site = %s
                          AND (
                              parser_version <> %s
                              OR ad_id NOT IN ({placeholders})
                          )
                        """
                        params = (
                            LISTING_STATUS_STALE,
                            normalized_site,
                            parser_version,
                            *active_ad_ids,
                        )
                elif stale_strategy == "delete":
                    sql = "DELETE FROM listings WHERE source_site = %s"
                    params = (normalized_site,)
                else:
                    sql = """
                    UPDATE listings
                    SET record_status = %s
                    WHERE source_site = %s
                    """
                    params = (LISTING_STATUS_STALE, normalized_site)

                await cur.execute(sql, params)


async def _replace_listing_issues(
    result: "ScrapeSiteResult",
    *,
    parser_version: str,
) -> None:
    normalized_site = result.site_name.strip().lower()
    if not normalized_site:
        return

    issue_rows: list[tuple[Any, ...]] = []
    for status, envelopes in (
        (LISTING_STATUS_ACTIVE, result.accepted),
        (LISTING_STATUS_REJECTED, result.rejected),
    ):
        for envelope in envelopes:
            normalized = _normalize_listing(envelope.listing, default_site=result.site_name)
            if normalized is None:
                continue
            effective_status = (
                _envelope_record_status(envelope)
                if status == LISTING_STATUS_ACTIVE
                else LISTING_STATUS_REJECTED
            )
            for issue in envelope.issues:
                issue_rows.append(
                    (
                        normalized.source_site,
                        normalized.ad_id,
                        parser_version,
                        result.strategy_name,
                        result.mode_used,
                        effective_status,
                        envelope.fallback_action,
                        issue.code,
                        issue.field_name,
                        issue.severity.value,
                        issue.message,
                    )
                )

    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM listing_extraction_issues
                    WHERE source_site = %s
                      AND parser_version = %s
                    """,
                    (normalized_site, parser_version),
                )
                if issue_rows:
                    await cur.executemany(
                        """
                        INSERT INTO listing_extraction_issues (
                            source_site, ad_id, parser_version, strategy_name, mode_used,
                            record_status, fallback_action, issue_code, field_name,
                            severity, message
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        issue_rows,
                    )


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
    parser_version = _current_parser_version()
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT {VISIBLE_LISTING_COLUMNS}
                    FROM listings
                    WHERE record_status = %s
                      AND parser_version = %s
                    ORDER BY date_seen DESC, updated_at DESC
                    LIMIT %s
                    """,
                    (LISTING_STATUS_ACTIVE, parser_version, limit),
                )
                rows = await cur.fetchall()

    return [dict(zip(VISIBLE_LISTING_KEYS, row)) for row in rows]


async def list_review_leads(limit: int = 100) -> list[dict[str, Any]]:
    parser_version = _current_parser_version()
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT {VISIBLE_LISTING_COLUMNS}
                    FROM listings
                    WHERE record_status = %s
                      AND parser_version = %s
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """,
                    (LISTING_STATUS_MANUAL_REVIEW, parser_version, limit),
                )
                rows = await cur.fetchall()

    return [dict(zip(VISIBLE_LISTING_KEYS, row)) for row in rows]


async def list_listing_issues(limit: int = 250) -> list[dict[str, Any]]:
    parser_version = _current_parser_version()
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT source_site, ad_id, parser_version, strategy_name, mode_used,
                           record_status, fallback_action, issue_code, field_name,
                           severity, message, created_at
                    FROM listing_extraction_issues
                    WHERE parser_version = %s
                    ORDER BY created_at DESC, source_site, ad_id
                    LIMIT %s
                    """,
                    (parser_version, limit),
                )
                rows = await cur.fetchall()

    keys = [
        "source_site",
        "ad_id",
        "parser_version",
        "strategy_name",
        "mode_used",
        "record_status",
        "fallback_action",
        "issue_code",
        "field_name",
        "severity",
        "message",
        "created_at",
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
    parser_version = _current_parser_version()

    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                if city:
                    await cur.execute(
                        f"""
                        SELECT {VISIBLE_LISTING_COLUMNS}
                        FROM listings
                        WHERE record_status = %s
                          AND parser_version = %s
                          AND date_seen >= %s
                          AND location LIKE %s
                        ORDER BY date_seen DESC, updated_at DESC
                        """,
                        (
                            LISTING_STATUS_ACTIVE,
                            parser_version,
                            start_date.isoformat(),
                            f"%{city}%",
                        ),
                    )
                else:
                    await cur.execute(
                        f"""
                        SELECT {VISIBLE_LISTING_COLUMNS}
                        FROM listings
                        WHERE record_status = %s
                          AND parser_version = %s
                          AND date_seen >= %s
                        ORDER BY date_seen DESC, updated_at DESC
                        """,
                        (LISTING_STATUS_ACTIVE, parser_version, start_date.isoformat()),
                    )
                rows = await cur.fetchall()

    return [dict(zip(VISIBLE_LISTING_KEYS, row)) for row in rows]


async def get_listing(ad_id: str, *, source_site: str | None = None) -> dict[str, Any] | None:
    parser_version = _current_parser_version()
    normalized_site = (source_site or "").strip().lower()
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                if normalized_site:
                    await cur.execute(
                        f"""
                        SELECT {VISIBLE_LISTING_COLUMNS}
                        FROM listings
                        WHERE ad_id = %s
                          AND source_site = %s
                          AND record_status = %s
                          AND parser_version = %s
                        LIMIT 1
                        """,
                        (ad_id, normalized_site, LISTING_STATUS_ACTIVE, parser_version),
                    )
                    row = await cur.fetchone()
                else:
                    await cur.execute(
                        f"""
                        SELECT {VISIBLE_LISTING_COLUMNS}
                        FROM listings
                        WHERE ad_id = %s
                          AND record_status = %s
                          AND parser_version = %s
                        LIMIT 2
                        """,
                        (ad_id, LISTING_STATUS_ACTIVE, parser_version),
                    )
                    rows = await cur.fetchall()
                    if len(rows) > 1:
                        raise ValueError(
                            f"Listing ad_id '{ad_id}' is ambiguous. Provide source_site."
                        )
                    row = rows[0] if rows else None

    if row is None:
        return None

    return dict(zip(VISIBLE_LISTING_KEYS, row))


async def get_listing_by_ad_id(ad_id: str) -> dict[str, Any] | None:
    return await get_listing(ad_id)


async def create_voice_call(
    *,
    source_type: str,
    listing_ad_id: str | None,
    listing_source_site: str | None,
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
                        listing_source_site,
                        tenant_contact_id,
                        contact_name,
                        phone_raw,
                        phone_e164,
                        status,
                        script_name,
                        initiated_by
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        source_type,
                        listing_ad_id,
                        listing_source_site,
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
        "listing_source_site",
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
    parser_version = _current_parser_version()
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT vc.id, vc.source_type, vc.listing_ad_id, vc.listing_source_site,
                           vc.tenant_contact_id,
                           vc.twilio_call_sid, vc.contact_name, vc.phone_raw, vc.phone_e164,
                           vc.status, vc.script_name, vc.answers_json, vc.transcript,
                           vc.recording_url, vc.last_error, vc.initiated_by,
                           vc.started_at, vc.answered_at, vc.completed_at,
                           vc.created_at, vc.updated_at,
                           l.title AS listing_title, l.link AS listing_link
                    FROM voice_calls vc
                    LEFT JOIN listings l
                      ON l.ad_id = vc.listing_ad_id
                     AND (vc.listing_source_site IS NULL OR l.source_site = vc.listing_source_site)
                     AND l.record_status = %s
                     AND l.parser_version = %s
                    ORDER BY vc.created_at DESC, vc.id DESC
                    LIMIT %s
                    """,
                    (LISTING_STATUS_ACTIVE, parser_version, limit),
                )
                rows = await cur.fetchall()
    return [_voice_call_from_row(row) for row in rows]


async def get_voice_call(voice_call_id: int) -> dict[str, Any] | None:
    parser_version = _current_parser_version()
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT vc.id, vc.source_type, vc.listing_ad_id, vc.listing_source_site,
                           vc.tenant_contact_id,
                           vc.twilio_call_sid, vc.contact_name, vc.phone_raw, vc.phone_e164,
                           vc.status, vc.script_name, vc.answers_json, vc.transcript,
                           vc.recording_url, vc.last_error, vc.initiated_by,
                           vc.started_at, vc.answered_at, vc.completed_at,
                           vc.created_at, vc.updated_at,
                           l.title AS listing_title, l.link AS listing_link
                    FROM voice_calls vc
                    LEFT JOIN listings l
                      ON l.ad_id = vc.listing_ad_id
                     AND (vc.listing_source_site IS NULL OR l.source_site = vc.listing_source_site)
                     AND l.record_status = %s
                     AND l.parser_version = %s
                    WHERE vc.id = %s
                    LIMIT 1
                    """,
                    (LISTING_STATUS_ACTIVE, parser_version, voice_call_id),
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
