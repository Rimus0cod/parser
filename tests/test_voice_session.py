from __future__ import annotations

import unittest

from app.voice.extractors import extract_answers, next_question
from app.voice.session import VoiceSessionStore
from tests.helpers import MemoryRedis


class VoiceSessionTests(unittest.TestCase):
    def test_session_tracks_transcript_and_next_question(self) -> None:
        store = VoiceSessionStore(MemoryRedis())
        store.create_session(
            call_sid="CA123",
            voice_call_id=1,
            source_type="listing",
            listing_ad_id="L1",
            listing_source_site="example.com",
            tenant_contact_id=None,
            contact_name="Broker",
            phone_raw="0888 123 456",
            phone_e164="+359888123456",
            apartment_details={"ad_id": "L1"},
            script_name="bg_listing_v1",
        )
        store.append_transcript("CA123", "900 евро")

        state, new_text = store.consume_pending_transcript("CA123")
        self.assertIsNotNone(state)
        self.assertEqual(new_text, "900 евро")

        answers = extract_answers(new_text, {}, "price")
        self.assertEqual(answers["price"], "900 евро")
        question = next_question(answers, {})
        self.assertIsNotNone(question)
        self.assertEqual(question.key, "negotiation")

    def test_marks_answered_and_completed(self) -> None:
        store = VoiceSessionStore(MemoryRedis())
        store.create_session(
            call_sid="CA456",
            voice_call_id=2,
            source_type="listing",
            listing_ad_id="L2",
            listing_source_site="example.com",
            tenant_contact_id=None,
            contact_name="Owner",
            phone_raw="0899 123 456",
            phone_e164="+359899123456",
            apartment_details={},
            script_name="bg_listing_v1",
        )

        store.mark_answered("CA456")
        store.mark_completed("CA456", "completed")
        state = store.get_session("CA456")

        self.assertIsNotNone(state)
        self.assertEqual(state["status"], "completed")
        self.assertIsNotNone(state["answered_at"])
        self.assertIsNotNone(state["completed_at"])


if __name__ == "__main__":
    unittest.main()
