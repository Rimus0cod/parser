from __future__ import annotations

import asyncio
import signal
import sys
from datetime import datetime, timezone

from redis import Redis

from app.core.config import Settings, get_settings
from app.core.logging import capture_exception, configure_logging, get_logger
from app.db.mysql import init_schema
from app.services.async_scraper import MultiSiteScraper, ScrapedListing
from app.services.repository import upsert_leads
from integrations.amocrm import get_amocrm_integration
from integrations.bitrix24 import get_bitrix24_integration
from integrations.webhooks import get_webhook_integration

logger = get_logger("scraper_worker")


def _redis(settings: Settings) -> Redis:
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
    )


def _set_redis_state(redis: Redis | None, **values: str) -> None:
    if redis is None:
        return
    try:
        for key, value in values.items():
            redis.set(key, value)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to update Redis state", error=str(exc))


async def send_to_integrations(listings: list[ScrapedListing]) -> None:
    if not listings:
        return

    webhook_integration = get_webhook_integration()
    if webhook_integration is not None:
        try:
            results = await webhook_integration.send_batch_leads(listings)
            logger.info("Webhook batch send completed", count=len(listings), success_count=sum(results))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Webhook integration failed", error=str(exc))

    amocrm_integration = get_amocrm_integration()
    if amocrm_integration is not None:
        try:
            successful_ids = await amocrm_integration.send_batch_leads(listings)
            logger.info(
                "AmoCRM batch send completed",
                count=len(listings),
                success_count=len(successful_ids),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("AmoCRM integration failed", error=str(exc))

    bitrix24_integration = get_bitrix24_integration()
    if bitrix24_integration is not None:
        try:
            successful_ids = await bitrix24_integration.send_batch_deals(listings)
            logger.info(
                "Bitrix24 batch send completed",
                count=len(listings),
                success_count=len(successful_ids),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Bitrix24 integration failed", error=str(exc))


async def run_once(settings: Settings | None = None) -> int:
    settings = settings or get_settings()
    redis = None
    try:
        redis = _redis(settings)
    except Exception:  # noqa: BLE001
        redis = None

    started_at = datetime.now(timezone.utc).isoformat()
    _set_redis_state(
        redis,
        **{
            "scrape:worker_status": "running",
            "scrape:last_started_at": started_at,
        },
    )

    logger.info("Starting scrape cycle", site_count=len(settings.sites))
    scraper = MultiSiteScraper(settings)
    listings = await scraper.scrape_all_sites()
    written = await upsert_leads(listings)
    await send_to_integrations(listings)

    _set_redis_state(
        redis,
        **{
            "scrape:last_status": "ok",
            "scrape:last_worker_written": str(written),
            "scrape:last_total_scraped": str(len(listings)),
            "scrape:last_execution_time": datetime.now(timezone.utc).isoformat(),
        },
    )
    logger.info("Scrape cycle finished", parsed=len(listings), upserted=written)
    return written


async def run_forever() -> None:
    settings = get_settings()
    configure_logging(
        sentry_dsn=settings.sentry_dsn,
        environment=settings.sentry_environment,
        debug=settings.app_debug,
        log_level=settings.log_level,
        log_format=settings.log_format,
        log_to_file=settings.log_to_file,
        log_dir=settings.log_dir,
        sentry_traces_sample_rate=settings.sentry_traces_sample_rate,
    )

    shutdown_event = asyncio.Event()

    def _request_shutdown(signum: int, _frame: object | None) -> None:
        logger.info("Shutdown signal received", signal=signum)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    redis = None
    try:
        await init_schema()
        redis = _redis(settings)
        _set_redis_state(
            redis,
            **{
                "scrape:worker_start_time": datetime.now(timezone.utc).isoformat(),
                "scrape:worker_status": "idle",
            },
        )
    except Exception as exc:  # noqa: BLE001
        capture_exception(exc)
        logger.exception("Worker initialization failed", error=str(exc))
        raise

    while not shutdown_event.is_set():
        started = datetime.now(timezone.utc)
        try:
            await run_once(settings)
            _set_redis_state(redis, **{"scrape:worker_status": "idle"})
        except Exception as exc:  # noqa: BLE001
            capture_exception(exc)
            logger.exception("Scrape iteration failed", error=str(exc))
            _set_redis_state(
                redis,
                **{
                    "scrape:last_status": "error",
                    "scrape:worker_status": "error",
                    "scrape:last_error_time": datetime.now(timezone.utc).isoformat(),
                    "scrape:last_error_message": str(exc),
                },
            )

        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        sleep_seconds = max(0.0, settings.scrape_interval_seconds - elapsed)
        if sleep_seconds <= 0:
            continue

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_seconds)
        except TimeoutError:
            continue

    _set_redis_state(
        redis,
        **{
            "scrape:worker_status": "stopped",
            "scrape:last_stopped_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    logger.info("Scraper worker stopped")


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as exc:  # noqa: BLE001
        logger.critical("Fatal worker error", error=str(exc))
        sys.exit(1)
