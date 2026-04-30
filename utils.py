"""
Utility functions for parsing names and phone numbers from web content.
Contains enhanced regular expressions and validation logic.
"""

import re
import unicodedata
from typing import List

BULGARIAN_MOBILE_PREFIXES = ("086", "087", "088", "089", "098", "099")
BULGARIAN_LANDLINE_PREFIXES = ("02", "03", "04", "05", "06", "07")


def normalize_phone_number(phone: str) -> str:
    """
    Enhanced phone number normalization for Bulgarian numbers.

    Args:
        phone: Raw phone number string

    Returns:
        Normalized phone number in format starting with 0
    """
    if not phone:
        return ""

    # Remove all non-numeric characters except + at the beginning
    digits_only = "".join(
        c for c in phone.strip() if c.isdigit() or (c == "+" and phone.index(c) == 0)
    )

    if not digits_only:
        return ""

    # Handle international formats
    if digits_only.startswith("00359"):
        digits_only = "359" + digits_only[5:]
    elif digits_only.startswith("++359"):
        digits_only = "+359" + digits_only[5:]
    elif digits_only.startswith("00"):
        digits_only = "+" + digits_only[2:]

    # Convert Bulgarian international format to national format for internal storage.
    if digits_only.startswith("+359"):
        subscriber_number = digits_only[4:]
        digits_only = subscriber_number if subscriber_number.startswith("0") else f"0{subscriber_number}"
    elif digits_only.startswith("359"):
        subscriber_number = digits_only[3:]
        digits_only = subscriber_number if subscriber_number.startswith("0") else f"0{subscriber_number}"

    # Validate Bulgarian phone format
    if not digits_only.startswith("0"):
        return ""
    if len(set(digits_only)) == 1:
        return ""
    if len(digits_only) == 10 and digits_only.startswith(BULGARIAN_MOBILE_PREFIXES):
        return digits_only
    if len(digits_only) in {9, 10} and digits_only.startswith(BULGARIAN_LANDLINE_PREFIXES):
        return digits_only

    return ""


def extract_phone_numbers(text: str) -> List[str]:
    """
    Extract phone numbers from text using multiple regex patterns.

    Args:
        text: Input text to search for phone numbers

    Returns:
        List of normalized phone numbers found
    """
    if not text:
        return []

    phone_patterns = [
        # Pattern for +359 XX XXX XXX or +359-XX-XXX-XXX or +359 XXX XXX XXX
        r"(\+359(?:[-\s]*[0-9]){8,9})",
        r"(\+359\s*[0-9]{2}\s*[0-9]{3}\s*[0-9]{3})",
        r"(\+359[-\s]*[0-9]{2}[-\s]*[0-9]{3}[-\s]*[0-9]{3})",
        r"(\+359[0-9]{8,9})",
        # Pattern for 0XX XXX XXX or 0XX-XXX-XXX or 0XX XXXXXXX
        r"(0(?:[-\s]*[0-9]){8,9})",
        r"(0[0-9]{2}\s*[0-9]{3}\s*[0-9]{3})",
        r"(0[0-9]{2}[-\s]*[0-9]{3}[-\s]*[0-9]{3})",
        r"(0[0-9]{8,9})",
        # Pattern for 359 XX XXX XXX
        r"(359(?:[-\s]*[0-9]){8,9})",
        r"(359\s*[0-9]{2}\s*[0-9]{3}\s*[0-9]{3})",
        r"(359[-\s]*[0-9]{2}[-\s]*[0-9]{3}[-\s]*[0-9]{3})",
        # More flexible pattern that captures anything that looks like a Bulgarian number
        r"(?:тел\.?\s*:?\s*|\+)?(?:359|0)?[-\s\.]*([0-9][-\s\.]*){8,12}[0-9]",
        # Direct tel: links
        r"tel:\+?([\d\s\-\+]+)",
    ]

    phones = set()
    for pattern in phone_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            # The match could be the full number or a group within the match
            if isinstance(match, tuple):
                phone_str = "".join(match)
            else:
                phone_str = match
            # Clean the phone string and normalize it
            clean_phone = re.sub(r"[^\d\+\s]", "", phone_str).strip()
            normalized = normalize_phone_number(clean_phone)
            if normalized:
                phones.add(normalized)

    # Additional pattern: extract any sequence of digits that looks like a phone
    digit_sequences = re.findall(r"\b\d{8,12}\b", text)
    for seq in digit_sequences:
        # Try adding '0' prefix if it looks like a Bulgarian number
        if len(seq) == 8 and (
            seq.startswith(("86", "87", "88", "89", "98", "99")) or seq[0] in "23456789"
        ):
            test_phone = "0" + seq
        else:
            test_phone = seq

        normalized = normalize_phone_number(test_phone)
        if normalized:
            phones.add(normalized)

    return sorted(phones, key=lambda phone: (-len(phone), phone))


def clean_name(name: str) -> str:
    """
    Clean and standardize a person's name.

    Args:
        name: Raw name string

    Returns:
        Cleaned name string
    """
    if not name:
        return ""

    # Remove extra whitespace and normalize unicode
    cleaned = unicodedata.normalize("NFKD", name.strip())

    # Remove common prefixes and suffixes
    prefixes = ["г-н ", "г-жа ", "инж. ", "доц. ", "проф. ", "др. ", "господин ", "госпожа "]
    suffixes = [" моб.", " тел.", " tel", " mobile"]

    for prefix in prefixes:
        if cleaned.lower().startswith(prefix.lower()):
            cleaned = cleaned[len(prefix) :].strip()

    for suffix in suffixes:
        if cleaned.lower().endswith(suffix.lower()):
            cleaned = cleaned[: -len(suffix)].strip()

    # Remove email addresses and phone numbers that might be part of the name
    email_pattern = r"[\w\.-]+@[\w\.-]+\.\w+"
    phone_pattern = r"(\+?\d{1,3}[-.\s]?)?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}"

    cleaned = re.sub(email_pattern, "", cleaned).strip()
    cleaned = re.sub(phone_pattern, "", cleaned).strip()

    # Clean up remaining separators
    cleaned = re.sub(r"[,\|\/\\;:]+", " ", cleaned).strip()

    # Remove any extra spaces
    cleaned = " ".join(cleaned.split())

    return cleaned


def looks_like_person_name(text: str) -> bool:
    """
    Determine if text looks like a person's name.

    Args:
        text: Text to evaluate

    Returns:
        True if text likely represents a person's name
    """
    if not text:
        return False

    name = clean_name(text)

    if not name:
        return False

    # Check length
    if len(name) < 2:
        return False

    # Split into parts (words)
    parts = name.split()
    if len(parts) < 2 and len(name) < 4:
        # Allow short full names like "Ivan Petrov" but not single letters
        return False

    # Check for indicators that this is NOT a person name
    lower_name = name.lower()

    # Common terms that indicate it's not a personal name
    non_personal_terms = [
        "частно лице",
        "агент",
        "агентство",
        "имоти",
        "агенция",
        "ооо",
        "еоод",
        "ад",
        "ooo",
        "eood",
        "juristic",
        "business",
        "company",
        "firm",
        "агентства",
        "estate",
    ]

    if any(term in lower_name for term in non_personal_terms):
        return False

    # Check for email-like patterns
    email_pattern = r"[\w\.-]+@[\w\.-]+\.\w+"
    if re.search(email_pattern, name):
        return False

    # Check for phone-like patterns
    phone_pattern = r"(\+?\d{1,3}[-.\s]?)?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}"
    if re.search(phone_pattern, name):
        return False

    # Count numbers in the name - if more than 30% are digits, it's probably not a name
    digits = sum(1 for char in name if char.isdigit())
    if len(name) > 0 and digits / len(name) > 0.3:
        return False

    # Check if it contains mostly letters and spaces
    alpha_chars = sum(1 for char in name if char.isalpha() or char.isspace())
    if alpha_chars / len(name) < 0.6:  # At least 60% should be letters/spaces
        return False

    return True


def extract_names(text: str) -> List[str]:
    """
    Extract potential person names from text based on common patterns.

    Args:
        text: Input text to search for names

    Returns:
        List of potential names found
    """
    if not text:
        return []

    names = set()

    # Patterns that often precede names
    name_indicators = [
        r"(?:контактно лице|лице за контакти|контакт|contact|агент|мениджър|брокер)\s*[:\-\—\|]?\s*([^\n\r.,;()<>]+)",
        r"(?:г-н|г-жа|инж\.|доц\.|проф\.|др\.|господин|госпожа)\s+([A-ZА-Я][a-zа-я]+(?:\s+[A-ZА-Я][a-zа-я]+)?)",
        r"([A-ZА-Я][a-zа-я]+\s+[A-ZА-Я][a-zа-я]+(?:\s+[A-ZА-Я][a-zа-я]+)?)",  # Two or more capitalized words
    ]

    for pattern in name_indicators:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                # Take the first capturing group if it's a tuple
                actual_match = match[0] if match else ""
            else:
                actual_match = match

            # Clean and validate the potential name
            clean_name_val = clean_name(actual_match.strip())
            if looks_like_person_name(clean_name_val):
                names.add(clean_name_val)

    # Additional check for names appearing near common labels
    # Look for patterns like "Име: Ivan Petrov" or "Name: John Doe"
    labeled_names = re.findall(
        r"(?:име|name|contact)\s*[:\-\—]?\s*([^\n\r.,;()<>]+)", text, re.IGNORECASE
    )
    for potential_name in labeled_names:
        clean_name_val = clean_name(potential_name.strip())
        if looks_like_person_name(clean_name_val):
            names.add(clean_name_val)

    return list(names)


def extract_email(text: str) -> List[str]:
    """
    Extract email addresses from text.

    Args:
        text: Input text to search for emails

    Returns:
        List of email addresses found
    """
    if not text:
        return []

    email_pattern = r"[\w\.-]+@[\w\.-]+\.\w+"
    emails = re.findall(email_pattern, text, re.IGNORECASE)

    # Return unique emails, cleaning whitespace
    return list(set(email.strip() for email in emails if email.strip()))
