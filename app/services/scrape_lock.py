from __future__ import annotations

import secrets
from typing import Any

SCRAPE_LOCK_KEY = "scrape:run_lock"
DEFAULT_SCRAPE_LOCK_TTL_SECONDS = 3600


def scrape_lock_ttl_seconds(scrape_interval_seconds: int) -> int:
    return max(DEFAULT_SCRAPE_LOCK_TTL_SECONDS, max(60, scrape_interval_seconds) * 2)


def acquire_scrape_lock(
    redis_client: Any,
    *,
    owner: str,
    ttl_seconds: int = DEFAULT_SCRAPE_LOCK_TTL_SECONDS,
) -> str | None:
    token = f"{owner}:{secrets.token_urlsafe(12)}"
    acquired = redis_client.set(
        SCRAPE_LOCK_KEY,
        token,
        ex=max(60, ttl_seconds),
        nx=True,
    )
    return token if acquired else None


def release_scrape_lock(redis_client: Any, token: str) -> bool:
    current = redis_client.get(SCRAPE_LOCK_KEY)
    if isinstance(current, bytes):
        current = current.decode("utf-8")
    if current != token:
        return False
    return bool(redis_client.delete(SCRAPE_LOCK_KEY))
