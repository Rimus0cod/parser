from __future__ import annotations

import unittest

from app.voice.extractors import extract_answers, next_question


class VoiceExtractorTests(unittest.TestCase):
    def test_extracts_price_and_negotiation(self) -> None:
        self.assertEqual(extract_answers("900 евро", {}, "price")["price"], "900 евро")
        negotiation = extract_answers("Да, може да се коментира.", {"price": "900 евро"}, "negotiation")
        self.assertIn("negotiation", negotiation)

    def test_falls_back_to_current_question_text(self) -> None:
        answers = extract_answers("Свободен е веднага.", {}, "availability")
        self.assertEqual(answers["availability"], "Свободен е веднага.")

    def test_next_question_waits_for_price_before_negotiation(self) -> None:
        question = next_question({}, {})
        self.assertIsNotNone(question)
        self.assertEqual(question.key, "price")

        question = next_question({"price": "900 евро"}, {})
        self.assertIsNotNone(question)
        self.assertEqual(question.key, "negotiation")

    def test_next_question_skips_exhausted_prompt(self) -> None:
        question = next_question({}, {"price": 2})
        self.assertIsNotNone(question)
        self.assertEqual(question.key, "size")


if __name__ == "__main__":
    unittest.main()
