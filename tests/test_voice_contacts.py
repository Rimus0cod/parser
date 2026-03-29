from __future__ import annotations

import unittest

from app.voice.service import parse_tenant_contacts_csv


class VoiceContactImportTests(unittest.TestCase):
    def test_parses_semicolon_csv_and_normalizes_phone(self) -> None:
        content = (
            "full_name;phone;notes;active\n"
            "Иван Иванов;0888 123 456;Търси двустаен;true\n"
            "Bad Contact;12345;Ignore;true\n"
        ).encode("utf-8")

        rows = parse_tenant_contacts_csv(content, filename="tenants.csv")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["full_name"], "Иван Иванов")
        self.assertEqual(rows[0]["phone_normalized"], "0888123456")
        self.assertEqual(rows[0]["phone_e164"], "+359888123456")


if __name__ == "__main__":
    unittest.main()
