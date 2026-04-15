from __future__ import annotations

import unittest

from app.services.scrape_lock import SCRAPE_LOCK_KEY, acquire_scrape_lock, release_scrape_lock
from tests.helpers import MemoryRedis


class ScrapeLockTests(unittest.TestCase):
    def test_acquire_prevents_parallel_run_until_release(self) -> None:
        redis = MemoryRedis()

        first_token = acquire_scrape_lock(redis, owner="api", ttl_seconds=120)
        second_token = acquire_scrape_lock(redis, owner="worker", ttl_seconds=120)

        self.assertIsNotNone(first_token)
        self.assertIsNone(second_token)
        self.assertEqual(redis.get(SCRAPE_LOCK_KEY), first_token)

        self.assertTrue(release_scrape_lock(redis, first_token or ""))
        self.assertIsNone(redis.get(SCRAPE_LOCK_KEY))

    def test_release_ignores_stale_token(self) -> None:
        redis = MemoryRedis()

        first_token = acquire_scrape_lock(redis, owner="api", ttl_seconds=120)

        self.assertIsNotNone(first_token)
        self.assertFalse(release_scrape_lock(redis, "worker:stale-token"))
        self.assertEqual(redis.get(SCRAPE_LOCK_KEY), first_token)


if __name__ == "__main__":
    unittest.main()
