from __future__ import annotations

import re
from dataclasses import dataclass

MAX_QUESTION_ATTEMPTS = 2


@dataclass(frozen=True)
class VoiceQuestion:
    key: str
    text: str
    audio_file: str


QUESTION_SEQUENCE: tuple[VoiceQuestion, ...] = (
    VoiceQuestion("price", "Каква е цената на апартамента?", "price.mp3"),
    VoiceQuestion("negotiation", "Има ли възможност за договаряне на цената?", "negotiation.mp3"),
    VoiceQuestion("size", "Каква е квадратурата?", "size.mp3"),
    VoiceQuestion("location", "Къде точно се намира?", "location.mp3"),
    VoiceQuestion("condition", "Какво е състоянието и обзавеждането?", "condition.mp3"),
    VoiceQuestion("availability", "От кога е наличен за наем или продажба?", "availability.mp3"),
)

_LOCATION_HINTS = ("квартал", "улица", "адрес", "район", "център", "до ", "бул.")
_CONDITION_HINTS = ("ремонт", "обзавед", "ново", "старо", "състояние", "лукс", "след")
_AVAILABILITY_HINTS = ("свобод", "налич", "дата", "от ", "сега", "веднага", "след ")
_POSITIVE_NEGOTIATION = ("да", "има", "възможно", "може", "разбира се", "донякъде")
_NEGATIVE_NEGOTIATION = ("не", "твърда", "крайна", "фиксирана", "без")


def detect_price(text: str) -> str | None:
    match = re.search(r"(\d[\d\s.,]{1,10})\s*(лв|лева|eur|евро|€)", text, re.IGNORECASE)
    return " ".join(match.group(0).split()) if match else None


def detect_size(text: str) -> str | None:
    match = re.search(r"(\d{1,4})\s*(кв\.?\s?м|квм|m2|м2)", text, re.IGNORECASE)
    return " ".join(match.group(0).split()) if match else None


def detect_location(text: str) -> str | None:
    return text.strip() if any(token in text.lower() for token in _LOCATION_HINTS) else None


def detect_condition(text: str) -> str | None:
    return text.strip() if any(token in text.lower() for token in _CONDITION_HINTS) else None


def detect_availability(text: str) -> str | None:
    return text.strip() if any(token in text.lower() for token in _AVAILABILITY_HINTS) else None


def detect_negotiation(text: str) -> str | None:
    lowered = text.lower()
    if any(token in lowered for token in _POSITIVE_NEGOTIATION):
        return text.strip()
    if any(token in lowered for token in _NEGATIVE_NEGOTIATION):
        return text.strip()
    return None


def extract_answers(
    text: str,
    existing_answers: dict[str, str] | None = None,
    current_question: str | None = None,
) -> dict[str, str]:
    text = (text or "").strip()
    if not text:
        return {}

    answers = dict(existing_answers or {})
    extracted: dict[str, str] = {}

    if "price" not in answers:
        price = detect_price(text)
        if price:
            extracted["price"] = price

    if "size" not in answers:
        size = detect_size(text)
        if size:
            extracted["size"] = size

    if "location" not in answers:
        location = detect_location(text)
        if location:
            extracted["location"] = location

    if "condition" not in answers:
        condition = detect_condition(text)
        if condition:
            extracted["condition"] = condition

    if "availability" not in answers:
        availability = detect_availability(text)
        if availability:
            extracted["availability"] = availability

    if "negotiation" not in answers:
        negotiation = detect_negotiation(text)
        if negotiation:
            extracted["negotiation"] = negotiation

    if current_question and current_question not in answers and current_question not in extracted:
        extracted[current_question] = text

    return extracted


def next_question(
    answers: dict[str, str] | None,
    question_attempts: dict[str, int] | None,
) -> VoiceQuestion | None:
    known_answers = answers or {}
    attempts = question_attempts or {}

    for question in QUESTION_SEQUENCE:
        if question.key == "negotiation" and "price" not in known_answers:
            continue
        if question.key in known_answers:
            continue
        if attempts.get(question.key, 0) >= MAX_QUESTION_ATTEMPTS:
            continue
        return question
    return None
