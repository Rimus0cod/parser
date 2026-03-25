import asyncio
import logging
from typing import Any, Dict, List, Optional

import httpx

from app.core.config import AmoCrmConfig
from app.services.async_scraper import ScrapedListing

logger = logging.getLogger(__name__)


class AmoCrmIntegration:
    def __init__(self, config: AmoCrmConfig):
        self.config = config
        self.base_url = config.base_url
        self.access_token = config.access_token
        self.pipeline_id = config.pipeline_id
        self.status_id = config.status_id
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    async def authenticate(self) -> bool:
        """Authenticate with AmoCRM API and refresh tokens if needed."""
        if not self.config.enabled:
            logger.info("AmoCRM integration is disabled")
            return False

        try:
            # Check if access token is still valid
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(f"{self.base_url}/api/v4/account", headers=self.headers)

            if response.status_code == 200:
                logger.info("Successfully authenticated with AmoCRM")
                return True
            elif response.status_code == 401:
                # Token expired, try to refresh
                await self.refresh_token()
                return True
            else:
                logger.error(f"Authentication failed with status {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Error authenticating with AmoCRM: {e}")
            return False

    async def refresh_token(self) -> bool:
        """Refresh access token using refresh token."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                data = {
                    "grant_type": "refresh_token",
                    "client_id": self.config.client_id,
                    "client_secret": self.config.client_secret,
                    "refresh_token": self.config.refresh_token,
                }

                response = await client.post(f"{self.base_url}/oauth2/access_token", json=data)

            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data["access_token"]
                self.config.access_token = token_data["access_token"]
                self.config.refresh_token = token_data["refresh_token"]

                self.headers["Authorization"] = f"Bearer {self.access_token}"
                logger.info("Successfully refreshed AmoCRM token")
                return True
            else:
                logger.error(f"Failed to refresh token: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Error refreshing AmoCRM token: {e}")
            return False

    async def create_lead(self, listing: ScrapedListing) -> Optional[int]:
        """Create a lead in AmoCRM for the given listing."""
        if not self.config.enabled:
            return None

        try:
            lead_data = {
                "add": [
                    {
                        "name": f"Property: {listing.title[:50]}...",
                        "pipeline_id": self.pipeline_id,
                        "status_id": self.status_id,
                        "custom_fields_values": [
                            {
                                "field_id": 123,  # Example field ID - should be configured
                                "values": [{"value": listing.price}],
                            },
                            {
                                "field_id": 124,  # Location field
                                "values": [{"value": listing.location}],
                            },
                            {
                                "field_id": 125,  # Size field
                                "values": [{"value": listing.size}],
                            },
                            {
                                "field_id": 126,  # Link field
                                "values": [{"value": listing.link}],
                            },
                            {
                                "field_id": 127,  # Source site
                                "values": [{"value": listing.source_site}],
                            },
                        ],
                        "contacts": [
                            {
                                "first_name": listing.contact_name,
                                "custom_fields_values": [
                                    {
                                        "field_id": 128,  # Phone
                                        "values": [{"value": listing.phone}],
                                    },
                                    {
                                        "field_id": 129,  # Email
                                        "values": [{"value": listing.contact_email}],
                                    },
                                ],
                            }
                        ],
                    }
                ]
            }

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.base_url}/api/v4/leads", headers=self.headers, json=lead_data
                )

            if response.status_code in [200, 201]:
                result = response.json()
                if "id" in result["add"][0]:
                    lead_id = result["add"][0]["id"]
                    logger.info(f"Successfully created AmoCRM lead with ID: {lead_id}")
                    return lead_id
                else:
                    logger.error(f"Lead creation failed: {result}")
                    return None
            else:
                logger.error(f"Failed to create lead: {response.status_code}, {response.text}")
                return None

        except Exception as e:
            logger.error(f"Error creating AmoCRM lead: {e}")
            return None

    async def send_batch_leads(self, listings: List[ScrapedListing]) -> List[int]:
        """Send multiple listings as leads to AmoCRM."""
        if not self.config.enabled or not listings:
            return []

        successful_ids = []
        for listing in listings:
            try:
                lead_id = await self.create_lead(listing)
                if lead_id:
                    successful_ids.append(lead_id)

                # Small delay to avoid rate limiting
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error sending listing {listing.ad_id} to AmoCRM: {e}")
                continue

        return successful_ids


# Global instance for singleton pattern
_amocrm_instance: Optional[AmoCrmIntegration] = None


def get_amocrm_integration() -> Optional[AmoCrmIntegration]:
    """Get global AmoCRM integration instance."""
    global _amocrm_instance
    if _amocrm_instance is None:
        from app.core.config import get_settings

        settings = get_settings()
        _amocrm_instance = AmoCrmIntegration(settings.amocrm)
    return _amocrm_instance
