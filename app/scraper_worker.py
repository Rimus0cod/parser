from __future__ import annotations

import asyncio
import logging

from redis import Redis

from app.core.config import get_settings
from app.db.mysql import init_schema
from app.services.async_scraper import AsyncImotiScraper
from app.services.repository import upsert_leads

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scraper_worker")


async def run_once() -> int:
    settings = get_settings()
    scraper = AsyncImotiScraper(settings)
    leads = await scraper.scrape()
    written = await upsert_leads(leads)
    logger.info("Scrape completed. Parsed=%d, Upserted=%d", len(leads), written)
    redis = Redis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db, decode_responses=True)
    redis.set("scrape:last_worker_written", str(written))
    return written


async def run_forever() -> None:
    settings = get_settings()
    await init_schema()
    while True:
        try:
            await run_once()
        except Exception as exc:  # noqa: BLE001
            logger.exception("Worker loop failed: %s", exc)
        await asyncio.sleep(max(60, settings.scrape_interval_seconds))


if __name__ == "__main__":
    asyncio.run(run_forever())
