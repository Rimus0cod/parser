from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import aiomysql

from app.core.config import get_settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def mysql_pool() -> AsyncIterator[aiomysql.Pool]:
    settings = get_settings()
    try:
        pool = await aiomysql.create_pool(
            host=settings.mysql_host,
            port=settings.mysql_port,
            user=settings.mysql_user,
            password=settings.mysql_password,
            db=settings.mysql_database,
            autocommit=True,
            minsize=1,
            maxsize=10,
            connect_timeout=10,
            echo=False,
        )
    except Exception as e:
        logger.error(f"Failed to create MySQL pool: {e}")
        raise

    try:
        yield pool
    except Exception as e:
        logger.error(f"MySQL pool error: {e}")
        raise
    finally:
        pool.close()
        await pool.wait_closed()


async def init_schema() -> None:
    try:
        async with mysql_pool() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Create listings table
                    await cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS listings (
                            ad_id VARCHAR(50) PRIMARY KEY,
                            date_seen DATE NOT NULL,
                            title TEXT,
                            price VARCHAR(100),
                            location VARCHAR(255),
                            size VARCHAR(50),
                            link TEXT,
                            phone VARCHAR(50),
                            seller_name VARCHAR(255),
                            ad_type VARCHAR(50),
                            contact_name VARCHAR(255),
                            contact_email VARCHAR(255),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
                        """
                    )
                    # Create agencies table
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
    except Exception as e:
        logger.error(f"Failed to initialize schema: {e}")
        raise
