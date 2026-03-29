from __future__ import annotations

import csv
from datetime import UTC, datetime, timezone
from io import StringIO
from typing import Any

from app.core.config import get_settings
from app.core.logging import get_logger
from app.services import repository
from app.voice.phone import normalize_bulgarian_phone, to_bulgarian_e164
from app.voice.prompts import SCRIPT_NAME

try:
    from twilio.request_validator import RequestValidator
    from twilio.rest import Client as TwilioClient
except ImportError:  # pragma: no cover - optional dependency path
    RequestValidator = None
    TwilioClient = None

logger = get_logger("voice.service")

TERMINAL_CALL_STATUSES = {"busy", "canceled", "completed", "failed", "no-answer"}


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _parse_session_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _decode_csv(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _normalize_bool(value: str) -> bool:
    return value.strip().lower() not in {"0", "false", "no", "n", "off"}


def parse_tenant_contacts_csv(content: bytes, filename: str = "upload.csv") -> list[dict[str, Any]]:
    decoded = _decode_csv(content)
    sample = decoded[:1024]
    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except csv.Error:
        pass

    reader = csv.DictReader(StringIO(decoded), delimiter=delimiter)
    rows: list[dict[str, Any]] = []
    for row in reader:
        normalized_row = {str(key or "").strip().lower(): (value or "").strip() for key, value in row.items()}
        full_name = normalized_row.get("full_name") or normalized_row.get("name") or normalized_row.get("contact_name") or ""
        phone_raw = normalized_row.get("phone_raw") or normalized_row.get("phone") or normalized_row.get("mobile") or ""
        phone_normalized = normalize_bulgarian_phone(phone_raw)
        if not phone_normalized:
            continue
        rows.append(
            {
                "full_name": full_name,
                "phone_raw": phone_raw,
                "phone_normalized": phone_normalized,
                "phone_e164": to_bulgarian_e164(phone_raw),
                "notes": normalized_row.get("notes") or normalized_row.get("comment") or "",
                "import_source": normalized_row.get("import_source") or filename,
                "active": _normalize_bool(normalized_row.get("active", "true") or "true"),
            }
        )
    return rows


class VoiceService:
    def __init__(self, session_store: Any) -> None:
        self._settings = get_settings()
        self._session_store = session_store

    @property
    def settings(self) -> Any:
        return self._settings

    def is_enabled(self) -> bool:
        return self._settings.voice_enabled

    def _ensure_enabled(self) -> None:
        if not self.is_enabled():
            raise RuntimeError("Voice integration is disabled. Set VOICE_ENABLED=true to use it.")

    def _ensure_twilio_client(self) -> Any:
        self._ensure_enabled()
        if TwilioClient is None:
            raise RuntimeError("twilio is not installed.")
        if not self._settings.twilio_account_sid or not self._settings.twilio_auth_token:
            raise RuntimeError("Twilio credentials are not configured.")
        if not self._settings.twilio_from_number:
            raise RuntimeError("TWILIO_FROM_NUMBER is not configured.")
        if not self._settings.voice_public_base_url:
            raise RuntimeError("VOICE_PUBLIC_BASE_URL must be configured.")
        if not self._settings.voice_ws_public_url:
            raise RuntimeError("VOICE_WS_PUBLIC_URL must be configured.")
        return TwilioClient(self._settings.twilio_account_sid, self._settings.twilio_auth_token)

    def validate_twilio_request(self, url: str, params: dict[str, str], signature: str) -> bool:
        if not self._settings.twilio_validate:
            return True
        if RequestValidator is None:
            logger.warning("Twilio request validation skipped because twilio is not installed.")
            return True
        validator = RequestValidator(self._settings.twilio_auth_token)
        return validator.validate(url, params, signature)

    async def start_listing_call(self, listing_ad_id: str, initiated_by: str = "api") -> dict[str, Any]:
        client = self._ensure_twilio_client()
        listing = await repository.get_listing_by_ad_id(listing_ad_id)
        if listing is None:
            raise LookupError(f"Listing {listing_ad_id} was not found.")
        if not listing.get("phone"):
            raise ValueError(f"Listing {listing_ad_id} does not have a phone number.")

        phone_e164 = to_bulgarian_e164(listing["phone"])
        if not phone_e164:
            raise ValueError(
                f"Listing {listing_ad_id} has an invalid Bulgarian phone for Twilio: {listing['phone']}"
            )

        voice_call_id = await repository.create_voice_call(
            source_type="listing",
            listing_ad_id=listing["ad_id"],
            tenant_contact_id=None,
            contact_name=listing.get("contact_name") or listing.get("seller_name") or "",
            phone_raw=listing.get("phone") or "",
            phone_e164=phone_e164,
            status="queued",
            script_name=SCRIPT_NAME,
            initiated_by=initiated_by,
        )
        started_at = _utcnow()
        base_url = self._settings.voice_public_base_url.rstrip("/")

        try:
            call = client.calls.create(
                to=phone_e164,
                from_=self._settings.twilio_from_number,
                url=f"{base_url}/voice/twiml/start?voice_call_id={voice_call_id}",
                status_callback=f"{base_url}/voice/twilio/status?voice_call_id={voice_call_id}",
                status_callback_event=["initiated", "ringing", "answered", "completed"],
                record=True,
                recording_status_callback=(
                    f"{base_url}/voice/twilio/recording?voice_call_id={voice_call_id}"
                ),
            )
            await repository.patch_voice_call(
                voice_call_id=voice_call_id,
                twilio_call_sid=call.sid,
                status="initiated",
                started_at=started_at,
                last_error=None,
            )
        except Exception as exc:  # noqa: BLE001
            await repository.patch_voice_call(
                voice_call_id=voice_call_id,
                status="failed",
                started_at=started_at,
                last_error=str(exc),
            )
            raise

        voice_call = await repository.get_voice_call(voice_call_id)
        if voice_call is None:
            raise RuntimeError("Voice call was created but could not be reloaded from the database.")
        return voice_call

    async def bootstrap_session(self, voice_call_id: int, call_sid: str) -> dict[str, Any]:
        existing = self._session_store.get_session(call_sid)
        if existing is not None:
            return existing

        voice_call = await repository.get_voice_call(voice_call_id)
        if voice_call is None:
            raise LookupError(f"Voice call {voice_call_id} was not found.")

        listing_details: dict[str, Any] = {}
        if voice_call.get("listing_ad_id"):
            listing = await repository.get_listing_by_ad_id(voice_call["listing_ad_id"])
            if listing:
                listing_details = {
                    "ad_id": listing.get("ad_id"),
                    "title": listing.get("title"),
                    "price": listing.get("price"),
                    "location": listing.get("location"),
                    "size": listing.get("size"),
                    "link": listing.get("link"),
                }

        self._session_store.create_session(
            call_sid=call_sid,
            voice_call_id=voice_call["id"],
            source_type=voice_call["source_type"],
            listing_ad_id=voice_call.get("listing_ad_id"),
            tenant_contact_id=voice_call.get("tenant_contact_id"),
            contact_name=voice_call.get("contact_name") or "",
            phone_raw=voice_call.get("phone_raw") or "",
            phone_e164=voice_call.get("phone_e164") or "",
            apartment_details=listing_details,
            script_name=voice_call.get("script_name") or SCRIPT_NAME,
        )
        await repository.patch_voice_call(
            voice_call_id=voice_call["id"],
            twilio_call_sid=call_sid,
            status="initiated",
        )
        session = self._session_store.get_session(call_sid)
        if session is None:
            raise RuntimeError("Voice session was not created.")
        return session

    async def persist_session_snapshot(self, call_sid: str, *, status: str | None = None) -> None:
        session = self._session_store.get_session(call_sid)
        if session is None:
            return

        transcript = "\n".join(session.get("transcript_chunks", []))
        await repository.patch_voice_call(
            voice_call_id=int(session["voice_call_id"]),
            status=status or session.get("status"),
            transcript=transcript,
            answers_json=session.get("answers", {}),
            recording_url=session.get("recording_url"),
            last_error=session.get("last_error"),
            answered_at=_parse_session_datetime(session.get("answered_at")),
            completed_at=_parse_session_datetime(session.get("completed_at")),
        )
