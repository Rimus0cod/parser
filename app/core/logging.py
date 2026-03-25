import logging
from typing import Any, Dict

import sentry_sdk
import structlog
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.httpx import HttpxIntegration
from sentry_sdk.integrations.logging import LoggingIntegration


def configure_logging(
    sentry_dsn: str = "", environment: str = "development", debug: bool = False
) -> None:
    """Configure structured logging with optional Sentry integration."""

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.processors.time_stamper(fmt="iso"),
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if debug else logging.INFO)

    # Clear any existing handlers
    root_logger.handlers.clear()

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(processor=structlog.dev.ConsoleRenderer(colors=True))
    )
    root_logger.addHandler(console_handler)

    # Configure Sentry if DSN provided
    if sentry_dsn:
        sentry_logging = LoggingIntegration(
            level=logging.INFO,  # Capture info and above as breadcrumbs
            event_level=logging.ERROR,  # Send errors as events
        )

        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=environment,
            integrations=[
                sentry_logging,
                AioHttpIntegration(),
                HttpxIntegration(),
            ],
            traces_sample_rate=1.0,
        )

        # Set up logging to send unhandled exceptions to Sentry
        def handle_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                # Don't report KeyboardInterrupt
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return

            sentry_sdk.capture_exception(exc_value)

        import sys

        sys.excepthook = handle_exception


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a configured structlog logger."""
    return structlog.wrap_logger(logging.getLogger(name))


# Initialize logger for the core application
logger = get_logger(__name__)
