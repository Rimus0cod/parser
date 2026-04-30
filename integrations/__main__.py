"""Main module to test integrations."""

import asyncio
import logging

from app.core.config import get_settings
from integrations.amocrm import AmoCrmIntegration
from integrations.bitrix24 import Bitrix24Integration
from integrations.webhooks import WebhookIntegration

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def test_integrations() -> None:
    """Test all configured integrations."""
    settings = get_settings()

    # Test AmoCRM
    if settings.amocrm.enabled:
        logger.info("Testing AmoCRM integration...")
        amocrm = AmoCrmIntegration(settings.amocrm)
        success = await amocrm.authenticate()
        logger.info(f"AmoCRM authentication: {'SUCCESS' if success else 'FAILED'}")

    # Test Bitrix24
    if settings.bitrix24.enabled:
        logger.info("Testing Bitrix24 integration...")
        bitrix24 = Bitrix24Integration(settings.bitrix24)
        success = await bitrix24.authenticate()
        logger.info(f"Bitrix24 authentication: {'SUCCESS' if success else 'FAILED'}")

    # Test Webhooks
    if settings.webhooks.enabled and settings.webhooks.urls:
        logger.info("Testing Webhook integration...")
        WebhookIntegration(settings.webhooks)
        logger.info(f"Webhook integration configured with {len(settings.webhooks.urls)} URLs")

    logger.info("Integration testing completed.")


if __name__ == "__main__":
    asyncio.run(test_integrations())
