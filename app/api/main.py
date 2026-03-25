from __future__ import annotations

from datetime import datetime, timezone

from fastapi import BackgroundTasks, FastAPI, Query
from redis import Redis

from app.core.config import get_settings
from app.db.mysql import init_schema
from app.models.schemas import Agency, Lead, TriggerScrapeResponse
from app.services.async_scraper import AsyncImotiScraper
from app.services.repository import list_agencies, list_leads, upsert_leads

settings = get_settings()
app = FastAPI(title=settings.app_name, version="0.1.0")


def _redis() -> Redis:
    return Redis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db, decode_responses=True)


async def _run_scrape_job() -> None:
    redis = _redis()
    redis.set("scrape:last_status", "running")
    redis.set("scrape:last_started_at", datetime.now(timezone.utc).isoformat())
    try:
        scraper = AsyncImotiScraper(settings)
        leads = await scraper.scrape()
        written = await upsert_leads(leads)
        redis.set("scrape:last_status", "ok")
        redis.set("scrape:last_written", str(written))
    except Exception as exc:  # noqa: BLE001
        redis.set("scrape:last_status", "error")
        redis.set("scrape:last_error", str(exc))
    finally:
        redis.set("scrape:last_finished_at", datetime.now(timezone.utc).isoformat())


@app.on_event("startup")
async def startup() -> None:
    await init_schema()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/leads", response_model=list[Lead])
async def get_leads(limit: int = Query(default=100, ge=1, le=1000)) -> list[Lead]:
    rows = await list_leads(limit=limit)
    return [Lead.model_validate(r) for r in rows]


@app.get("/agencies", response_model=list[Agency])
async def get_agencies(limit: int = Query(default=100, ge=1, le=1000)) -> list[Agency]:
    rows = await list_agencies(limit=limit)
    return [Agency.model_validate(r) for r in rows]


@app.post("/trigger-scrape", response_model=TriggerScrapeResponse)
async def trigger_scrape(background_tasks: BackgroundTasks) -> TriggerScrapeResponse:
    background_tasks.add_task(_run_scrape_job)
    return TriggerScrapeResponse(status="queued", message="Scrape task has been queued.")
