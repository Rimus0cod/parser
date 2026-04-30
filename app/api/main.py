from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from redis import Redis

from app.core.config import get_settings, validate_runtime_settings
from app.core.logging import configure_logging, get_logger
from app.db.mysql import init_schema, ping_mysql
from app.models.schemas import Agency, Lead, ListingIssue, TriggerScrapeResponse
from app.scraping import build_scraping_engine
from app.services.scrape_lock import (
    acquire_scrape_lock,
    release_scrape_lock,
    scrape_lock_ttl_seconds,
)
from app.services.repository import (
    list_agencies,
    list_leads,
    list_listing_issues,
    list_review_leads,
    record_scrape_execution,
    refresh_leads,
)
from app.voice.router import router as voice_router
from app.voice.runtime import prepare_voice_runtime

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


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    validate_runtime_settings(settings, component="api")
    await init_schema()
    prepare_voice_runtime()
    yield


app = FastAPI(title=settings.app_name, version="0.2.0", lifespan=lifespan)


def _redis() -> Redis:
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
    )


async def _run_scrape_job(lock_token: str) -> None:
    redis = _redis()
    redis.set("scrape:last_status", "running")
    redis.set("scrape:last_started_at", datetime.now(timezone.utc).isoformat())
    try:
        engine = build_scraping_engine(settings)
        execution = await engine.scrape_all_sites()
        leads = execution.listings
        written = await refresh_leads(
            execution,
            parser_version=settings.scrape_data_version,
            stale_strategy=settings.scrape_stale_strategy,
        )
        await record_scrape_execution(execution)
        redis.set("scrape:last_status", "ok")
        redis.set("scrape:last_written", str(written))
        redis.set("scrape:last_total_scraped", str(len(leads)))
        redis.set("scrape:last_rejected", str(execution.rejected_count))
        logger.info(
            "Background scrape finished",
            parsed=len(leads),
            rejected=execution.rejected_count,
            written=written,
        )
    except Exception as exc:  # noqa: BLE001
        redis.set("scrape:last_status", "error")
        redis.set("scrape:last_error", str(exc))
        logger.exception("Background scrape failed", error=str(exc))
    finally:
        try:
            release_scrape_lock(redis, lock_token)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to release scrape lock", error=str(exc))
        redis.set("scrape:last_finished_at", datetime.now(timezone.utc).isoformat())


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "app": settings.app_name}


@app.get("/readyz")
async def readiness() -> JSONResponse:
    try:
        redis_ok = bool(_redis().ping())
    except Exception:  # noqa: BLE001
        redis_ok = False

    mysql_ok = await ping_mysql()
    payload = {
        "status": "ok" if redis_ok and mysql_ok else "error",
        "app": settings.app_name,
        "redis": "ok" if redis_ok else "error",
        "mysql": "ok" if mysql_ok else "error",
    }
    return JSONResponse(status_code=200 if payload["status"] == "ok" else 503, content=payload)


@app.get("/leads", response_model=list[Lead])
async def get_leads(limit: int = Query(default=100, ge=1, le=1000)) -> list[Lead]:
    rows = await list_leads(limit=limit)
    return [Lead.model_validate(row) for row in rows]


@app.get("/leads/review", response_model=list[Lead])
async def get_review_leads(limit: int = Query(default=100, ge=1, le=1000)) -> list[Lead]:
    rows = await list_review_leads(limit=limit)
    return [Lead.model_validate(row) for row in rows]


@app.get("/leads/issues", response_model=list[ListingIssue])
async def get_listing_issues(limit: int = Query(default=250, ge=1, le=1000)) -> list[ListingIssue]:
    rows = await list_listing_issues(limit=limit)
    return [ListingIssue.model_validate(row) for row in rows]


@app.get("/agencies", response_model=list[Agency])
async def get_agencies(limit: int = Query(default=100, ge=1, le=1000)) -> list[Agency]:
    rows = await list_agencies(limit=limit)
    return [Agency.model_validate(row) for row in rows]


@app.post("/trigger-scrape", response_model=TriggerScrapeResponse)
async def trigger_scrape(background_tasks: BackgroundTasks) -> TriggerScrapeResponse:
    try:
        redis = _redis()
        lock_token = acquire_scrape_lock(
            redis,
            owner="api-trigger",
            ttl_seconds=scrape_lock_ttl_seconds(settings.scrape_interval_seconds),
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Scrape trigger failed because Redis is unavailable", error=str(exc))
        raise HTTPException(
            status_code=503, detail="Redis is unavailable; scrape trigger was rejected."
        ) from exc

    if lock_token is None:
        return TriggerScrapeResponse(
            status="busy",
            message="A scrape job is already running. Wait for it to finish before triggering another one.",
        )

    background_tasks.add_task(_run_scrape_job, lock_token)
    return TriggerScrapeResponse(status="queued", message="Scrape task has been queued.")


app.include_router(voice_router)
