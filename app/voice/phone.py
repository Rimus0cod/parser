from __future__ import annotations

import re

from utils import normalize_phone_number


def normalize_bulgarian_phone(phone: str) -> str:
    return normalize_phone_number(phone)


def to_bulgarian_e164(phone: str) -> str:
    normalized = normalize_bulgarian_phone(phone)
    if not normalized:
        return ""
    if not normalized.startswith("0"):
        return ""
    return f"+359{normalized[1:]}"


def looks_like_e164(phone: str) -> bool:
    return bool(re.fullmatch(r"\+\d{8,15}", phone or ""))
