from __future__ import annotations

import asyncio
import logging
import signal
import sys
from datetime import datetime
from typing import NoReturn

from redis import Redis

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.mysql import init_schema
from app.integrations.amocrm import get_amocrm_integration
from app.integrations.bitrix24 import get_bitrix24_integration
from app.integrations.webhooks import get_webhook_integration
from app.services.async_scraper import MultiSiteScraper
from app.services.repository import upsert_leads

logger = get_logger("scraper_worker")


async def run_once() -> int:
    """
    Выполняет один цикл скрапинга.
    """
    settings = get_settings()
    try:
        logger.info("Starting scrape cycle", site_count=len(settings.sites))

        scraper = MultiSiteScraper(settings)
        all_listings = await scraper.scrape_all_sites()

        # Сохраняем данные
        written = await upsert_leads(all_listings)
        logger.info("Scrape completed", parsed=len(all_listings), upserted=written)

        # Отправляем данные в интеграции
        await send_to_integrations(all_listings)

        # Обновляем Redis
        redis = Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
        )
        redis.set("scrape:last_worker_written", str(written))
        redis.set("scrape:last_execution_time", datetime.now().isoformat())

        return written
    except Exception as exc:
        logger.exception("Scraping cycle failed", error=str(exc))
        # Обновляем информацию о последней ошибке
        try:
            redis = Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                decode_responses=True,
            )
            redis.set("scrape:last_error_time", datetime.now().isoformat())
            redis.set("scrape:last_error_message", str(exc))
        except Exception as redis_exc:
            logger.error("Failed to update Redis with error info", error=str(redis_exc))
        raise  # Re-raise exception to handle it at higher level


async def send_to_integrations(listings: list) -> None:
    """Send listings to all configured integrations."""
    # Send to webhooks
    webhook_integration = get_webhook_integration()
    if webhook_integration:
        results = await webhook_integration.send_batch_leads(listings)
        logger.info("Webhook batch send completed", count=len(listings), success_count=sum(results))

    # Send to AmoCRM
    amocrm_integration = get_amocrm_integration()
    if amocrm_integration:
        try:
            successful_ids = await amocrm_integration.send_batch_leads(listings)
            logger.info(
                "AmoCRM batch send completed",
                count=len(listings),
                success_count=len(successful_ids),
            )
        except Exception as e:
            logger.error("AmoCRM batch send failed", error=str(e))

    # Send to Bitrix24
    bitrix24_integration = get_bitrix24_integration()
    if bitrix24_integration:
        try:
            successful_ids = await bitrix24_integration.send_batch_deals(listings)
            logger.info(
                "Bitrix24 batch send completed",
                count=len(listings),
                success_count=len(successful_ids),
            )
        except Exception as e:
            logger.error("Bitrix24 batch send failed", error=str(e))


async def run_forever() -> NoReturn:
    """
    Бесконечный цикл работы скрапера.
    """
    settings = get_settings()

    # Настройка логирования
    configure_logging(
        sentry_dsn=settings.sentry_dsn, environment=settings.app_env, debug=settings.app_debug
    )

    logger.info(
        "Initializing scraper worker",
        sites=[site.name for site in settings.sites],
        interval_seconds=settings.scrape_interval_seconds,
    )

    # Инициализация схемы БД
    try:
        await init_schema()
        logger.info("Database schema initialized successfully")
    except Exception as e:
        logger.error("Failed to initialize database schema", error=str(e))
        # Не завершаем процесс, но логируем ошибку
        return

    # Подключение к Redis и установка статуса
    try:
        redis = Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
        )
        redis.set("scrape:worker_start_time", datetime.now().isoformat())
        redis.set("scrape:worker_status", "running")
        logger.info("Redis connection established and worker status updated")
    except Exception as e:
        logger.error("Failed to connect to Redis", error=str(e))
        return

    # Регистрация сигналов завершения для корректного завершения
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal", signal=signum)
        redis.set("scrape:worker_status", "stopped")
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Бесконечный цикл
    iteration = 0
    while True:
        iteration += 1
        start_time = datetime.now()
        logger.info("Starting iteration", iteration=iteration)

        try:
            result = await run_once()
            logger.info(
                "Iteration completed",
                iteration=iteration,
                records_created=result,
                duration_seconds=(datetime.now() - start_time).total_seconds(),
            )
        except Exception as exc:
            logger.exception("Iteration failed", iteration=iteration, error=str(exc))

            # Обновляем статус в Redis в случае ошибки
            try:
                redis.set("scrape:worker_status", "error")
                redis.set("scrape:last_error_time", datetime.now().isoformat())
                redis.set("scrape:last_error_message", str(exc))
            except Exception as redis_exc:
                logger.error("Failed to update Redis status after error", error=str(redis_exc))

        # Вычисляем время, которое занял цикл
        elapsed = (datetime.now() - start_time).total_seconds()
        remaining_sleep = settings.scrape_interval_seconds - elapsed

        # Логируем статистику выполнения
        logger.info(
            "Iteration completed",
            iteration=iteration,
            duration_seconds=elapsed,
            planned_interval=settings.scrape_interval_seconds,
            sleep_duration=max(0, remaining_sleep),
        )

        if remaining_sleep > 0:
            logger.info("Waiting until next scrape cycle", sleep_seconds=remaining_sleep)
            await asyncio.sleep(remaining_sleep)
        else:
            # Если цикл занял больше времени, чем интервал, предупреждаем
            logger.warning(
                "Scraping cycle exceeded planned interval, skipping sleep",
                duration=elapsed,
                planned_interval=settings.scrape_interval_seconds,
            )
            # В этом случае мы просто переходим к следующей итерации без задержки


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.critical("Critical error in main loop", error=str(e))
        sys.exit(1)
