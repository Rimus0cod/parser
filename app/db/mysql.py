from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import aiomysql

from app.core.config import get_settings


@asynccontextmanager
async def mysql_pool() -> AsyncIterator[aiomysql.Pool]:
    settings = get_settings()
    pool = await aiomysql.create_pool(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        db=settings.mysql_database,
        autocommit=True,
        minsize=1,
        maxsize=10,
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
                        ad_id VARCHAR(32) PRIMARY KEY,
                        date_seen DATE NULL,
                        title TEXT,
                        price VARCHAR(128),
                        location VARCHAR(255),
                        size VARCHAR(64),
                        link TEXT,
                        phone VARCHAR(64),
                        seller_name VARCHAR(255),
                        ad_type VARCHAR(64),
                        contact_name VARCHAR(255),
                        contact_email VARCHAR(255),
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_listings_date (date_seen),
                        INDEX idx_listings_location (location(191))
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agencies (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        agency_name VARCHAR(255) NOT NULL,
                        phones TEXT,
                        city VARCHAR(128),
                        email VARCHAR(255),
                        contact_name VARCHAR(255),
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uq_agency_name (agency_name)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
