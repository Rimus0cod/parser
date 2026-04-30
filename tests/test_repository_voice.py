from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from app.services import repository
from tests.helpers import AsyncCursorStub, AsyncPoolStub


class VoiceRepositoryTests(unittest.TestCase):
    def test_patch_voice_call_does_not_clear_twilio_sid_when_not_provided(self) -> None:
        cursor = AsyncCursorStub()

        async def run_test() -> None:
            with patch("app.services.repository.mysql_pool", return_value=AsyncPoolStub(cursor)):
                await repository.patch_voice_call(voice_call_id=7, status="completed")

        asyncio.run(run_test())

        self.assertEqual(len(cursor.executed), 1)
        sql, params = cursor.executed[0]
        self.assertIn("status = %s", sql)
        self.assertNotIn("twilio_call_sid = %s", sql)
        self.assertEqual(params, ("completed", 7))

    def test_voice_call_row_parses_answers_json(self) -> None:
        row = (
            1,
            "listing",
            "A1",
            "example.com",
            None,
            "CA123",
            "Broker",
            "0888123456",
            "+359888123456",
            "completed",
            "bg_listing_v1",
            '{"price":"900 евро"}',
            "900 евро",
            "https://recording.example.com",
            None,
            "tester",
            None,
            None,
            None,
            None,
            None,
            "Listing title",
            "https://listing.example.com",
        )

        parsed = repository._voice_call_from_row(row)
        self.assertEqual(parsed["listing_source_site"], "example.com")
        self.assertEqual(parsed["answers_json"]["price"], "900 евро")


if __name__ == "__main__":
    unittest.main()
