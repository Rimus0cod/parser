from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, cast


class VoiceSessionStore:
    def __init__(self, redis_client: Any, ttl_seconds: int = 60 * 60 * 24 * 2) -> None:
        self._redis = redis_client
        self._ttl_seconds = ttl_seconds

    def _key(self, call_sid: str) -> str:
        return f"voice:session:{call_sid}"

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def create_session(
        self,
        *,
        call_sid: str,
        voice_call_id: int,
        source_type: str,
        listing_ad_id: str | None,
        listing_source_site: str | None,
        tenant_contact_id: int | None,
        contact_name: str,
        phone_raw: str,
        phone_e164: str,
        apartment_details: dict[str, Any],
        script_name: str,
    ) -> dict[str, Any]:
        state: dict[str, Any] = {
            "call_sid": call_sid,
            "voice_call_id": voice_call_id,
            "source_type": source_type,
            "listing_ad_id": listing_ad_id,
            "listing_source_site": listing_source_site,
            "tenant_contact_id": tenant_contact_id,
            "contact_name": contact_name,
            "phone_raw": phone_raw,
            "phone_e164": phone_e164,
            "apartment_details": apartment_details,
            "script_name": script_name,
            "status": "initiated",
            "recording_url": None,
            "stream_sid": None,
            "answers": {},
            "transcript_chunks": [],
            "last_processed_index": 0,
            "current_question": None,
            "question_attempts": {},
            "asked_questions": [],
            "last_error": None,
            "started_at": self._now(),
            "answered_at": None,
            "completed_at": None,
            "updated_at": self._now(),
        }
        self.save_session(state)
        return state

    def get_session(self, call_sid: str) -> dict[str, Any] | None:
        raw = self._redis.get(self._key(call_sid))
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cast(dict[str, Any], json.loads(raw))

    def save_session(self, state: dict[str, Any]) -> None:
        state["updated_at"] = self._now()
        self._redis.set(self._key(state["call_sid"]), json.dumps(state), ex=self._ttl_seconds)

    def attach_stream(
        self,
        call_sid: str,
        *,
        stream_sid: str | None = None,
        custom_parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        state = self.get_session(call_sid)
        if state is None:
            return None
        if stream_sid:
            state["stream_sid"] = stream_sid
        if custom_parameters:
            state["custom_parameters"] = custom_parameters
        self.save_session(state)
        return state

    def append_transcript(self, call_sid: str, text: str) -> dict[str, Any] | None:
        state = self.get_session(call_sid)
        if state is None or not text.strip():
            return state
        state.setdefault("transcript_chunks", []).append(text.strip())
        self.save_session(state)
        return state

    def consume_pending_transcript(self, call_sid: str) -> tuple[dict[str, Any] | None, str]:
        state = self.get_session(call_sid)
        if state is None:
            return None, ""
        chunks = state.get("transcript_chunks", [])
        last_processed_index = int(state.get("last_processed_index", 0))
        new_text = " ".join(chunks[last_processed_index:]).strip()
        state["last_processed_index"] = len(chunks)
        self.save_session(state)
        return state, new_text

    def set_status(self, call_sid: str, status: str, *, last_error: str | None = None) -> None:
        state = self.get_session(call_sid)
        if state is None:
            return
        state["status"] = status
        if last_error is not None:
            state["last_error"] = last_error
        self.save_session(state)

    def mark_answered(self, call_sid: str) -> None:
        state = self.get_session(call_sid)
        if state is None:
            return
        if not state.get("answered_at"):
            state["answered_at"] = self._now()
        state["status"] = "in-progress"
        self.save_session(state)

    def mark_completed(self, call_sid: str, status: str) -> None:
        state = self.get_session(call_sid)
        if state is None:
            return
        state["status"] = status
        state["completed_at"] = self._now()
        self.save_session(state)

    def set_recording_url(self, call_sid: str, recording_url: str) -> None:
        state = self.get_session(call_sid)
        if state is None:
            return
        state["recording_url"] = recording_url
        self.save_session(state)

    def set_error(self, call_sid: str, message: str) -> None:
        self.set_status(call_sid, "error", last_error=message)
