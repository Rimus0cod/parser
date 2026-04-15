import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

import httpx

from app.core.config import WebhookConfig
from app.scraping.models import ScrapedListing

logger = logging.getLogger(__name__)


class WebhookIntegration:
    def __init__(self, config: WebhookConfig):
        self.config = config

    async def send_to_webhook(self, url: str, payload: Dict[str, Any]) -> bool:
        """Send a payload to a webhook URL."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    url, json=payload, headers={"Content-Type": "application/json"}
                )

            if response.status_code in [200, 201, 202]:
                logger.info(f"Successfully sent data to webhook: {url}")
                return True
            else:
                logger.error(f"Webhook failed with status {response.status_code}: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error sending to webhook {url}: {e}")
            return False

    async def send_new_lead(self, listing: ScrapedListing) -> List[bool]:
        """Send a new lead to all configured webhooks."""
        if not self.config.enabled or not self.config.urls:
            return []

        payload = {
            "event": "new_lead",
            "timestamp": listing.date_seen,
            "data": {
                "ad_id": listing.ad_id,
                "title": listing.title,
                "price": listing.price,
                "location": listing.location,
                "size": listing.size,
                "link": listing.link,
                "source_site": listing.source_site,
                "phone": listing.phone,
                "seller_name": listing.seller_name,
                "ad_type": listing.ad_type,
                "contact_name": listing.contact_name,
                "contact_email": listing.contact_email,
            },
        }

        # Send to all webhooks concurrently
        tasks = [self.send_to_webhook(url, payload) for url in self.config.urls]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error sending to webhook {self.config.urls[i]}: {result}")
                success_results.append(False)
            else:
                success_results.append(result)

        return success_results

    async def send_batch_leads(self, listings: List[ScrapedListing]) -> List[bool]:
        """Send multiple leads to webhooks."""
        if not self.config.enabled or not self.config.urls or not listings:
            return []

        payload = {
            "event": "batch_new_leads",
            "timestamp": listings[0].date_seen if listings else "",
            "count": len(listings),
            "data": [
                {
                    "ad_id": listing.ad_id,
                    "title": listing.title,
                    "price": listing.price,
                    "location": listing.location,
                    "size": listing.size,
                    "link": listing.link,
                    "source_site": listing.source_site,
                    "phone": listing.phone,
                    "seller_name": listing.seller_name,
                    "ad_type": listing.ad_type,
                    "contact_name": listing.contact_name,
                    "contact_email": listing.contact_email,
                }
                for listing in listings
            ],
        }

        # Send to all webhooks concurrently
        tasks = [self.send_to_webhook(url, payload) for url in self.config.urls]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error sending to webhook {self.config.urls[i]}: {result}")
                success_results.append(False)
            else:
                success_results.append(result)

        return success_results


# Global instance for singleton pattern
_webhook_instance: Optional[WebhookIntegration] = None


def get_webhook_integration() -> Optional[WebhookIntegration]:
    """Get global webhook integration instance."""
    global _webhook_instance
    if _webhook_instance is None:
        from app.core.config import get_settings

        settings = get_settings()
        _webhook_instance = WebhookIntegration(settings.webhooks)
    return _webhook_instance
