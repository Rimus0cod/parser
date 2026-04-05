from __future__ import annotations

import logging
from typing import AsyncGenerator, Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.core.config import get_settings

logger = logging.getLogger(__name__)

def _effective_mysql_password() -> str:
    settings = get_settings()
    if settings.mysql_user == "root" and settings.mysql_root_password:
        if settings.mysql_password and settings.mysql_password != settings.mysql_root_password:
            logger.warning(
                "MYSQL_USER is root; using MYSQL_ROOT_PASSWORD for the connection. "
                "For production, switch to a dedicated non-root DB user."
            )
        return settings.mysql_root_password
    return settings.mysql_password

def get_async_database_url() -> str:
    settings = get_settings()
    password = _effective_mysql_password()
    return f"mysql+aiomysql://{settings.mysql_user}:{password}@{settings.mysql_host}:{settings.mysql_port}/{settings.mysql_database}?charset=utf8mb4"

def get_sync_database_url() -> str:
    settings = get_settings()
    password = _effective_mysql_password()
    return f"mysql+pymysql://{settings.mysql_user}:{password}@{settings.mysql_host}:{settings.mysql_port}/{settings.mysql_database}?charset=utf8mb4"

# Global engines
async_engine = create_async_engine(get_async_database_url(), pool_pre_ping=True, pool_size=10, max_overflow=20)
sync_engine = create_engine(get_sync_database_url(), pool_pre_ping=True, pool_size=10, max_overflow=20)

AsyncSessionLocal = async_sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)
SessionLocal = sessionmaker(sync_engine, expire_on_commit=False, class_=Session)

async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

def get_sync_db() -> Generator[Session, None, None]:
    with SessionLocal() as session:
        yield session

