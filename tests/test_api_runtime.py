from __future__ import annotations

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import BackgroundTasks

from app.api import main


class _FakeRedis:
    def __init__(self, *, ping_ok: bool = True) -> None:
        self._ping_ok = ping_ok

    def ping(self) -> bool:
        return self._ping_ok


class ApiRuntimeTests(unittest.TestCase):
    def test_readiness_returns_503_when_dependencies_are_down(self) -> None:
        async def run_test() -> None:
            with (
                patch("app.api.main._redis", return_value=_FakeRedis(ping_ok=False)),
                patch("app.api.main.ping_mysql", return_value=False),
            ):
                response = await main.readiness()

            payload = json.loads(response.body)
            self.assertEqual(response.status_code, 503)
            self.assertEqual(payload["status"], "error")
            self.assertEqual(payload["redis"], "error")
            self.assertEqual(payload["mysql"], "error")

        asyncio.run(run_test())

    def test_trigger_scrape_returns_busy_when_lock_is_held(self) -> None:
        async def run_test() -> None:
            tasks = BackgroundTasks()
            with (
                patch("app.api.main._redis", return_value=_FakeRedis()),
                patch("app.api.main.acquire_scrape_lock", return_value=None),
            ):
                result = await main.trigger_scrape(tasks)

            self.assertEqual(result.status, "busy")
            self.assertEqual(len(tasks.tasks), 0)

        asyncio.run(run_test())

    def test_trigger_scrape_queues_background_task_when_lock_is_free(self) -> None:
        async def run_test() -> None:
            tasks = BackgroundTasks()
            with (
                patch("app.api.main._redis", return_value=_FakeRedis()),
                patch("app.api.main.acquire_scrape_lock", return_value="lock-token"),
            ):
                result = await main.trigger_scrape(tasks)

            self.assertEqual(result.status, "queued")
            self.assertEqual(len(tasks.tasks), 1)
            task = tasks.tasks[0]
            self.assertEqual(task.func, main._run_scrape_job)
            self.assertEqual(task.args, ("lock-token",))

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
