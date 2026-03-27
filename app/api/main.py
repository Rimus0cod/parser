from __future__ import annotations

from datetime import datetime, timezone

from fastapi import BackgroundTasks, FastAPI, Query
from redis import Redis

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.mysql import init_schema
from app.models.schemas import Agency, Lead, TriggerScrapeResponse
from app.services.async_scraper import MultiSiteScraper
from app.services.repository import list_agencies, list_leads, upsert_leads

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
logger = get_logger("api")
app = FastAPI(title=settings.app_name, version="0.2.0")


def _redis() -> Redis:
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
    )


async def _run_scrape_job() -> None:
    redis = _redis()
    redis.set("scrape:last_status", "running")
    redis.set("scrape:last_started_at", datetime.now(timezone.utc).isoformat())
    try:
        scraper = MultiSiteScraper(settings)
        leads = await scraper.scrape_all_sites()
        written = await upsert_leads(leads)
        redis.set("scrape:last_status", "ok")
        redis.set("scrape:last_written", str(written))
        redis.set("scrape:last_total_scraped", str(len(leads)))
        logger.info("Background scrape finished", parsed=len(leads), written=written)
    except Exception as exc:  # noqa: BLE001
        redis.set("scrape:last_status", "error")
        redis.set("scrape:last_error", str(exc))
        logger.exception("Background scrape failed", error=str(exc))
    finally:
        redis.set("scrape:last_finished_at", datetime.now(timezone.utc).isoformat())


@app.on_event("startup")
async def startup() -> None:
    await init_schema()


@app.get("/health")
async def health() -> dict[str, str]:
    status = {"status": "ok", "app": settings.app_name}
    try:
        status["redis"] = "ok" if _redis().ping() else "error"
    except Exception:  # noqa: BLE001
        status["redis"] = "error"
    return status


@app.get("/leads", response_model=list[Lead])
async def get_leads(limit: int = Query(default=100, ge=1, le=1000)) -> list[Lead]:
    rows = await list_leads(limit=limit)
    return [Lead.model_validate(row) for row in rows]


@app.get("/agencies", response_model=list[Agency])
async def get_agencies(limit: int = Query(default=100, ge=1, le=1000)) -> list[Agency]:
    rows = await list_agencies(limit=limit)
    return [Agency.model_validate(row) for row in rows]


@app.post("/trigger-scrape", response_model=TriggerScrapeResponse)
async def trigger_scrape(background_tasks: BackgroundTasks) -> TriggerScrapeResponse:
    background_tasks.add_task(_run_scrape_job)
    return TriggerScrapeResponse(status="queued", message="Scrape task has been queued.")
