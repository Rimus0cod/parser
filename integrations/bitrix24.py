import logging
from typing import List, Optional

import httpx

from app.core.config import Bitrix24Config, get_settings
from app.scraping.models import ScrapedListing

logger = logging.getLogger(__name__)


class Bitrix24Integration:
    def __init__(self, config: Bitrix24Config):
        self.config = config

    async def authenticate(self) -> bool:
        """Best-effort auth check for Bitrix24 webhook integration."""
        if not self.config.enabled:
            return False
        if not self.config.webhook_url:
            logger.warning("Bitrix24 enabled but webhook_url is empty")
            return False
        return True

    async def send_batch_deals(self, listings: List[ScrapedListing]) -> List[str]:
        """Send leads to Bitrix24 as deals and return successful ad ids."""
        if not self.config.enabled or not self.config.webhook_url or not listings:
            return []

        successful_ids: List[str] = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for listing in listings:
                payload = {
                    "fields": {
                        "TITLE": listing.title[:120],
                        "COMMENTS": (
                            f"Цена: {listing.price}\n"
                            f"Локация: {listing.location}\n"
                            f"Площ: {listing.size}\n"
                            f"Линк: {listing.link}\n"
                            f"Източник: {listing.source_site}"
                        ),
                        "PHONE": [{"VALUE": listing.phone, "VALUE_TYPE": "WORK"}]
                        if listing.phone
                        else [],
                        "EMAIL": [{"VALUE": listing.contact_email, "VALUE_TYPE": "WORK"}]
                        if listing.contact_email and listing.contact_email != "-"
                        else [],
                    }
                }
                try:
                    response = await client.post(self.config.webhook_url, json=payload)
                    if response.status_code in (200, 201):
                        successful_ids.append(listing.ad_id)
                    else:
                        logger.error(
                            "Bitrix24 deal creation failed",
                            extra={"status_code": response.status_code, "ad_id": listing.ad_id},
                        )
                except Exception as exc:
                    logger.error("Bitrix24 request failed: %s", exc)

        return successful_ids


def get_bitrix24_integration() -> Optional[Bitrix24Integration]:
    settings = get_settings()
    if not settings.bitrix24.enabled:
        return None
    return Bitrix24Integration(settings.bitrix24)
