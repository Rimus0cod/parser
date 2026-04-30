from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

try:
    import aiomysql
except ImportError:  # pragma: no cover - optional dependency path for test imports
    aiomysql = None

try:
    from pymysql.err import OperationalError  # type: ignore[import-untyped, unused-ignore]
except ImportError:  # pragma: no cover - optional dependency path for test imports
    OperationalError = RuntimeError

logger = logging.getLogger(__name__)
MYSQL_DUPLICATE_KEY_ERROR = 1061
MYSQL_DUPLICATE_COLUMN_ERROR = 1060


def _effective_mysql_password() -> str:
    from app.core.config import get_settings

    settings = get_settings()
    if settings.mysql_user == "root" and settings.mysql_root_password:
        if settings.mysql_password and settings.mysql_password != settings.mysql_root_password:
            logger.warning(
                "MYSQL_USER is root; using MYSQL_ROOT_PASSWORD for the connection. "
                "For production, switch to a dedicated non-root DB user."
            )
        return settings.mysql_root_password
    return settings.mysql_password


@asynccontextmanager
async def mysql_pool() -> AsyncIterator[Any]:
    if aiomysql is None:
        raise RuntimeError("aiomysql is not installed.")
    from app.core.config import get_settings

    settings = get_settings()
    pool = await aiomysql.create_pool(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=_effective_mysql_password(),
        db=settings.mysql_database,
        autocommit=True,
        minsize=1,
        maxsize=10,
        connect_timeout=10,
        charset="utf8mb4",
    )
    try:
        yield pool
    finally:
        pool.close()
        await pool.wait_closed()


async def init_schema() -> None:
    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS listings (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        ad_id VARCHAR(50) NOT NULL,
                        date_seen DATE NOT NULL,
                        title TEXT,
                        price VARCHAR(100),
                        location VARCHAR(255),
                        size VARCHAR(50),
                        link TEXT,
                        source_site VARCHAR(120) NOT NULL DEFAULT '',
                        parser_version VARCHAR(64) NOT NULL DEFAULT 'legacy',
                        record_status VARCHAR(32) NOT NULL DEFAULT 'active',
                        phone VARCHAR(50),
                        seller_name VARCHAR(255),
                        ad_type VARCHAR(50),
                        contact_name VARCHAR(255),
                        contact_email VARCHAR(255),
                        price_raw VARCHAR(100) NOT NULL DEFAULT '',
                        price_amount DECIMAL(14, 2) NULL,
                        currency VARCHAR(8) NOT NULL DEFAULT '',
                        location_raw VARCHAR(255) NOT NULL DEFAULT '',
                        size_raw VARCHAR(50) NOT NULL DEFAULT '',
                        area_m2 DECIMAL(10, 2) NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_listings_source_ad_id (source_site, ad_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
                await cur.execute("SHOW COLUMNS FROM listings LIKE 'id'")
                if await cur.fetchone() is None:
                    try:
                        await cur.execute(
                            """
                            ALTER TABLE listings
                            DROP PRIMARY KEY,
                            ADD COLUMN id BIGINT AUTO_INCREMENT PRIMARY KEY FIRST
                            """
                        )
                    except OperationalError as exc:
                        if exc.args and exc.args[0] not in {
                            MYSQL_DUPLICATE_COLUMN_ERROR,
                            MYSQL_DUPLICATE_KEY_ERROR,
                        }:
                            raise
                await cur.execute("SHOW COLUMNS FROM listings LIKE 'source_site'")
                if await cur.fetchone() is None:
                    try:
                        await cur.execute(
                            """
                            ALTER TABLE listings
                            ADD COLUMN source_site VARCHAR(120) NOT NULL DEFAULT '' AFTER link
                            """
                        )
                    except OperationalError as exc:
                        if exc.args and exc.args[0] != MYSQL_DUPLICATE_COLUMN_ERROR:
                            raise

                await cur.execute("SHOW COLUMNS FROM listings LIKE 'parser_version'")
                if await cur.fetchone() is None:
                    try:
                        await cur.execute(
                            """
                            ALTER TABLE listings
                            ADD COLUMN parser_version VARCHAR(64) NOT NULL DEFAULT 'legacy' AFTER source_site
                            """
                        )
                    except OperationalError as exc:
                        if exc.args and exc.args[0] != MYSQL_DUPLICATE_COLUMN_ERROR:
                            raise

                await cur.execute("SHOW COLUMNS FROM listings LIKE 'record_status'")
                if await cur.fetchone() is None:
                    try:
                        await cur.execute(
                            """
                            ALTER TABLE listings
                            ADD COLUMN record_status VARCHAR(32) NOT NULL DEFAULT 'active' AFTER parser_version
                            """
                        )
                    except OperationalError as exc:
                        if exc.args and exc.args[0] != MYSQL_DUPLICATE_COLUMN_ERROR:
                            raise

                for column_name, column_definition, after_column in (
                    ("price_raw", "VARCHAR(100) NOT NULL DEFAULT ''", "contact_email"),
                    ("price_amount", "DECIMAL(14, 2) NULL", "price_raw"),
                    ("currency", "VARCHAR(8) NOT NULL DEFAULT ''", "price_amount"),
                    ("location_raw", "VARCHAR(255) NOT NULL DEFAULT ''", "currency"),
                    ("size_raw", "VARCHAR(50) NOT NULL DEFAULT ''", "location_raw"),
                    ("area_m2", "DECIMAL(10, 2) NULL", "size_raw"),
                ):
                    await cur.execute(f"SHOW COLUMNS FROM listings LIKE '{column_name}'")
                    if await cur.fetchone() is None:
                        try:
                            await cur.execute(
                                f"""
                                ALTER TABLE listings
                                ADD COLUMN {column_name} {column_definition} AFTER {after_column}
                                """
                            )
                        except OperationalError as exc:
                            if exc.args and exc.args[0] != MYSQL_DUPLICATE_COLUMN_ERROR:
                                raise

                await cur.execute(
                    """
                    UPDATE listings
                    SET parser_version = 'legacy'
                    WHERE parser_version IS NULL OR parser_version = ''
                    """
                )
                await cur.execute(
                    """
                    UPDATE listings
                    SET record_status = 'active'
                    WHERE record_status IS NULL OR record_status = ''
                    """
                )
                await cur.execute(
                    """
                    UPDATE listings
                    SET price_raw = COALESCE(NULLIF(price_raw, ''), COALESCE(price, '')),
                        location_raw = COALESCE(NULLIF(location_raw, ''), COALESCE(location, '')),
                        size_raw = COALESCE(NULLIF(size_raw, ''), COALESCE(size, ''))
                    """
                )

                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agencies (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        agency_name VARCHAR(255) NOT NULL,
                        phones TEXT,
                        city VARCHAR(100),
                        email VARCHAR(255),
                        contact_name VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
                await cur.execute(
                    "SHOW INDEX FROM listings WHERE Key_name = 'idx_listings_date_seen'"
                )
                if await cur.fetchone() is None:
                    try:
                        await cur.execute(
                            "CREATE INDEX idx_listings_date_seen ON listings (date_seen)"
                        )
                    except OperationalError as exc:
                        if exc.args and exc.args[0] != MYSQL_DUPLICATE_KEY_ERROR:
                            raise

                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tenant_contacts (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        full_name VARCHAR(255) NOT NULL DEFAULT '',
                        phone_raw VARCHAR(64) NOT NULL,
                        phone_normalized VARCHAR(32) NOT NULL,
                        phone_e164 VARCHAR(32) NOT NULL DEFAULT '',
                        notes TEXT,
                        import_source VARCHAR(255) NOT NULL DEFAULT '',
                        active BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_tenant_phone_normalized (phone_normalized)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )

                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS voice_calls (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        source_type VARCHAR(50) NOT NULL,
                        listing_ad_id VARCHAR(50) NULL,
                        listing_source_site VARCHAR(120) NULL,
                        tenant_contact_id INT NULL,
                        twilio_call_sid VARCHAR(64) NULL,
                        contact_name VARCHAR(255) NOT NULL DEFAULT '',
                        phone_raw VARCHAR(64) NOT NULL DEFAULT '',
                        phone_e164 VARCHAR(32) NOT NULL DEFAULT '',
                        status VARCHAR(50) NOT NULL DEFAULT 'queued',
                        script_name VARCHAR(120) NOT NULL DEFAULT '',
                        answers_json JSON NULL,
                        transcript LONGTEXT NULL,
                        recording_url TEXT NULL,
                        last_error TEXT NULL,
                        initiated_by VARCHAR(120) NOT NULL DEFAULT '',
                        started_at DATETIME NULL,
                        answered_at DATETIME NULL,
                        completed_at DATETIME NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY idx_voice_calls_status (status),
                        KEY idx_voice_calls_listing_ad_id (listing_ad_id),
                        KEY idx_voice_calls_listing_identity (listing_source_site, listing_ad_id),
                        KEY idx_voice_calls_tenant_contact_id (tenant_contact_id),
                        UNIQUE KEY uniq_voice_calls_twilio_call_sid (twilio_call_sid)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
                await cur.execute("SHOW COLUMNS FROM voice_calls LIKE 'listing_source_site'")
                if await cur.fetchone() is None:
                    try:
                        await cur.execute(
                            """
                            ALTER TABLE voice_calls
                            ADD COLUMN listing_source_site VARCHAR(120) NULL AFTER listing_ad_id
                            """
                        )
                    except OperationalError as exc:
                        if exc.args and exc.args[0] != MYSQL_DUPLICATE_COLUMN_ERROR:
                            raise
                await cur.execute(
                    "SHOW INDEX FROM voice_calls WHERE Key_name = 'idx_voice_calls_listing_identity'"
                )
                if await cur.fetchone() is None:
                    try:
                        await cur.execute(
                            """
                            CREATE INDEX idx_voice_calls_listing_identity
                            ON voice_calls (listing_source_site, listing_ad_id)
                            """
                        )
                    except OperationalError as exc:
                        if exc.args and exc.args[0] != MYSQL_DUPLICATE_KEY_ERROR:
                            raise

                await cur.execute(
                    "SHOW INDEX FROM listings WHERE Key_name = 'idx_listings_source_site'"
                )
                if await cur.fetchone() is None:
                    try:
                        await cur.execute(
                            "CREATE INDEX idx_listings_source_site ON listings (source_site)"
                        )
                    except OperationalError as exc:
                        if exc.args and exc.args[0] != MYSQL_DUPLICATE_KEY_ERROR:
                            raise

                await cur.execute(
                    "SHOW INDEX FROM listings WHERE Key_name = 'uniq_listings_source_ad_id'"
                )
                if await cur.fetchone() is None:
                    try:
                        await cur.execute(
                            """
                            CREATE UNIQUE INDEX uniq_listings_source_ad_id
                            ON listings (source_site, ad_id)
                            """
                        )
                    except OperationalError as exc:
                        if exc.args and exc.args[0] != MYSQL_DUPLICATE_KEY_ERROR:
                            raise

                await cur.execute(
                    "SHOW INDEX FROM listings WHERE Key_name = 'idx_listings_visibility'"
                )
                if await cur.fetchone() is None:
                    try:
                        await cur.execute(
                            """
                            CREATE INDEX idx_listings_visibility
                            ON listings (record_status, parser_version, date_seen)
                            """
                        )
                    except OperationalError as exc:
                        if exc.args and exc.args[0] != MYSQL_DUPLICATE_KEY_ERROR:
                            raise

                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scrapling_adaptive_elements (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        target_url VARCHAR(1024) NOT NULL,
                        identifier VARCHAR(255) NOT NULL,
                        selector_version VARCHAR(32) NOT NULL DEFAULT 'v1',
                        payload_json LONGTEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_scrapling_adaptive_target (
                            target_url(255),
                            identifier,
                            selector_version
                        )
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )

                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scrape_runs (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        source_site VARCHAR(120) NOT NULL,
                        strategy_name VARCHAR(120) NOT NULL DEFAULT '',
                        mode_used VARCHAR(32) NOT NULL DEFAULT 'http',
                        accepted_count INT NOT NULL DEFAULT 0,
                        rejected_count INT NOT NULL DEFAULT 0,
                        status VARCHAR(32) NOT NULL DEFAULT 'ok',
                        error_summary TEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS listing_extraction_issues (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        source_site VARCHAR(120) NOT NULL,
                        ad_id VARCHAR(50) NOT NULL,
                        parser_version VARCHAR(64) NOT NULL,
                        strategy_name VARCHAR(120) NOT NULL DEFAULT '',
                        mode_used VARCHAR(32) NOT NULL DEFAULT 'http',
                        record_status VARCHAR(32) NOT NULL DEFAULT '',
                        fallback_action VARCHAR(32) NOT NULL DEFAULT '',
                        issue_code VARCHAR(120) NOT NULL,
                        field_name VARCHAR(120) NOT NULL DEFAULT '',
                        severity VARCHAR(32) NOT NULL,
                        message TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        KEY idx_listing_issues_identity (
                            source_site,
                            ad_id,
                            parser_version
                        ),
                        KEY idx_listing_issues_code (issue_code, severity)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )


async def ping_mysql() -> bool:
    try:
        async with mysql_pool() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    row = await cur.fetchone()
                    return bool(row)
    except Exception:  # noqa: BLE001
        return False
