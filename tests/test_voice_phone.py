from __future__ import annotations

import unittest

from app.voice.phone import normalize_bulgarian_phone, to_bulgarian_e164


class VoicePhoneTests(unittest.TestCase):
    def test_normalizes_bulgarian_phone(self) -> None:
        self.assertEqual(normalize_bulgarian_phone("+359 888 123 456"), "0888123456")

    def test_converts_local_phone_to_e164(self) -> None:
        self.assertEqual(to_bulgarian_e164("0888 123 456"), "+359888123456")

    def test_invalid_phone_returns_empty_e164(self) -> None:
        self.assertEqual(to_bulgarian_e164("12345"), "")


if __name__ == "__main__":
    unittest.main()
