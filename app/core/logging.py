from __future__ import annotations

import logging
import sys
from pathlib import Path

import structlog

try:
    import sentry_sdk
    from sentry_sdk.integrations.aiohttp import AioHttpIntegration
    from sentry_sdk.integrations.httpx import HttpxIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration

    SENTRY_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency path
    sentry_sdk = None
    LoggingIntegration = None
    AioHttpIntegration = None
    HttpxIntegration = None
    SENTRY_AVAILABLE = False


def configure_logging(
    sentry_dsn: str = "",
    environment: str = "development",
    debug: bool = False,
    log_level: str = "INFO",
    log_format: str = "json",
    log_to_file: bool = False,
    log_dir: str = "logs",
    sentry_traces_sample_rate: float = 0.1,
) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    if debug:
        level = logging.DEBUG

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer: structlog.types.Processor
    if log_format == "console":
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    if log_to_file:
        try:
            log_path = Path(log_dir)
            log_path.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_path / "app.log", encoding="utf-8")
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        except OSError as exc:
            root_logger.warning("File logging is disabled because log directory is not writable: %s", exc)

    if sentry_dsn and SENTRY_AVAILABLE:
        sentry_logging = LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)
        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=environment,
            integrations=[
                sentry_logging,
                AioHttpIntegration(),
                HttpxIntegration(),
            ],
            traces_sample_rate=sentry_traces_sample_rate,
        )

    if sentry_dsn and not SENTRY_AVAILABLE:
        root_logger.warning("Sentry DSN provided but sentry-sdk is not installed")


def capture_exception(exc: BaseException) -> None:
    if SENTRY_AVAILABLE and sentry_sdk is not None:
        sentry_sdk.capture_exception(exc)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
