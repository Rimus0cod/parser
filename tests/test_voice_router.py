from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode
from xml.etree import ElementTree as ET

from starlette.requests import Request

from app.models.schemas import VoiceCallCreateRequest
from app.voice.router import (
    create_voice_call,
    voice_recording_callback,
    voice_status_callback,
    voice_twiml_next,
    voice_twiml_start,
)


def _build_request(
    path: str,
    *,
    method: str = "POST",
    query: str = "",
    form: dict[str, str] | None = None,
) -> Request:
    body = urlencode(form or {}).encode("utf-8")
    headers = [(b"content-type", b"application/x-www-form-urlencoded")]
    sent = False

    async def receive() -> dict[str, object]:
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": "https",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": query.encode("utf-8"),
        "headers": headers,
        "client": ("testclient", 50000),
        "server": ("testserver", 443),
    }
    return Request(scope, receive)


class _FakeSessionStore:
    def __init__(self) -> None:
        self._state: dict[str, dict] = {}

    def consume_pending_transcript(self, call_sid: str) -> tuple[dict | None, str]:
        state = self._state.get(call_sid)
        if state is None:
            return None, ""
        chunks = state.get("transcript_chunks", [])
        index = state.get("last_processed_index", 0)
        text = " ".join(chunks[index:]).strip()
        state["last_processed_index"] = len(chunks)
        return state, text

    def save_session(self, state: dict) -> None:
        self._state[state["call_sid"]] = state

    def get_session(self, call_sid: str) -> dict | None:
        return self._state.get(call_sid)

    def create_state(self, call_sid: str, *, transcript_chunks: list[str] | None = None) -> dict:
        state = {
            "call_sid": call_sid,
            "voice_call_id": 1,
            "answers": {},
            "transcript_chunks": transcript_chunks or [],
            "last_processed_index": 0,
            "current_question": "price",
            "question_attempts": {},
            "asked_questions": [],
            "recording_url": None,
            "last_error": None,
            "status": "initiated",
            "answered_at": None,
            "completed_at": None,
        }
        self._state[call_sid] = state
        return state

    def mark_answered(self, call_sid: str) -> None:
        state = self._state.setdefault(call_sid, self.create_state(call_sid))
        state["status"] = "in-progress"
        state["answered_at"] = "2026-03-29T10:00:00+00:00"

    def mark_completed(self, call_sid: str, status: str) -> None:
        state = self._state.setdefault(call_sid, self.create_state(call_sid))
        state["status"] = status
        state["completed_at"] = "2026-03-29T10:05:00+00:00"

    def set_status(self, call_sid: str, status: str) -> None:
        state = self._state.setdefault(call_sid, self.create_state(call_sid))
        state["status"] = status

    def set_recording_url(self, call_sid: str, recording_url: str) -> None:
        state = self._state.setdefault(call_sid, self.create_state(call_sid))
        state["recording_url"] = recording_url


class _FakeVoiceService:
    def __init__(self, session_store: _FakeSessionStore) -> None:
        self._store = session_store
        self.persist_session_snapshot = AsyncMock()

    async def start_listing_call(self, listing_ad_id: str, initiated_by: str) -> dict:
        return {
            "id": 1,
            "source_type": "listing",
            "listing_ad_id": listing_ad_id,
            "tenant_contact_id": None,
            "twilio_call_sid": "CA123",
            "contact_name": "Broker",
            "phone_raw": "0888123456",
            "phone_e164": "+359888123456",
            "status": "initiated",
            "script_name": "bg_listing_v1",
            "answers_json": {},
            "transcript": None,
            "recording_url": None,
            "last_error": None,
            "initiated_by": initiated_by,
            "started_at": None,
            "answered_at": None,
            "completed_at": None,
            "created_at": None,
            "updated_at": None,
            "listing_title": "Test listing",
            "listing_link": "https://listing.example.com",
        }

    async def bootstrap_session(self, voice_call_id: int, call_sid: str) -> dict:
        return self._store.create_state(call_sid)

    def validate_twilio_request(self, url: str, params: dict[str, str], signature: str) -> bool:  # noqa: ARG002
        return True


class VoiceRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = _FakeSessionStore()
        self.service = _FakeVoiceService(self.store)
        self.settings = SimpleNamespace(
            voice_public_base_url="https://voice.example.com",
            voice_ws_public_url="wss://voice.example.com/voice/media",
            twilio_validate=False,
        )

    def test_create_voice_call_endpoint(self) -> None:
        payload = VoiceCallCreateRequest(listing_ad_id="A1", initiated_by="ui")
        with (
            patch("app.voice.router.get_voice_service", return_value=self.service),
            patch("app.voice.router.get_settings", return_value=self.settings),
        ):
            result = asyncio.run(create_voice_call(payload))

        self.assertEqual(result.listing_ad_id, "A1")
        self.assertEqual(result.phone_e164, "+359888123456")

    def test_start_twiml_contains_stream_play_pause_and_redirect(self) -> None:
        request = _build_request(
            "/voice/twiml/start",
            query="voice_call_id=1",
            form={"CallSid": "CA123"},
        )
        with (
            patch("app.voice.router.get_voice_service", return_value=self.service),
            patch("app.voice.router.get_settings", return_value=self.settings),
        ):
            response = asyncio.run(voice_twiml_start(request, voice_call_id=1))

        xml = ET.fromstring(response.body.decode("utf-8"))
        stream = xml.find("./Start/Stream")
        self.assertIsNotNone(stream)
        self.assertEqual(stream.attrib["track"], "inbound_track")
        self.assertEqual(stream.attrib["url"], "wss://voice.example.com/voice/media")
        self.assertEqual(xml.findtext("./Play"), "https://voice.example.com/voice/tts/intro.mp3")
        self.assertEqual(xml.find("./Pause").attrib["length"], "4")
        self.assertIn("/voice/twiml/next?call_sid=CA123", xml.findtext("./Redirect"))

    def test_next_twiml_moves_to_negotiation_after_price(self) -> None:
        self.store.create_state("CA999", transcript_chunks=["900 евро"])
        request = _build_request(
            "/voice/twiml/next",
            query="call_sid=CA999",
            form={"CallSid": "CA999"},
        )
        with (
            patch("app.voice.router.get_voice_service", return_value=self.service),
            patch("app.voice.router.get_voice_session_store", return_value=self.store),
            patch("app.voice.router.get_settings", return_value=self.settings),
        ):
            response = asyncio.run(voice_twiml_next(request, call_sid="CA999"))

        xml = ET.fromstring(response.body.decode("utf-8"))
        self.assertEqual(
            xml.findtext("./Play"),
            "https://voice.example.com/voice/tts/negotiation.mp3",
        )

    def test_status_and_recording_callbacks_patch_database_and_snapshot(self) -> None:
        self.store.create_state("CA777")
        status_request = _build_request(
            "/voice/twilio/status",
            query="voice_call_id=1",
            form={"CallSid": "CA777", "CallStatus": "completed"},
        )
        recording_request = _build_request(
            "/voice/twilio/recording",
            query="voice_call_id=1",
            form={"CallSid": "CA777", "RecordingUrl": "https://recording.example.com"},
        )
        with (
            patch("app.voice.router.get_voice_service", return_value=self.service),
            patch("app.voice.router.get_voice_session_store", return_value=self.store),
            patch("app.voice.router.get_settings", return_value=self.settings),
            patch("app.voice.router.repository.patch_voice_call", new=AsyncMock()) as patch_voice_call,
        ):
            status_response = asyncio.run(voice_status_callback(status_request, voice_call_id=1))
            recording_response = asyncio.run(
                voice_recording_callback(recording_request, voice_call_id=1)
            )

        self.assertEqual(status_response.status_code, 204)
        self.assertEqual(recording_response.status_code, 204)
        self.assertGreaterEqual(patch_voice_call.await_count, 2)
        self.assertEqual(self.service.persist_session_snapshot.await_count, 2)


if __name__ == "__main__":
    unittest.main()
