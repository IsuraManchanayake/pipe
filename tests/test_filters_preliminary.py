import unittest
from pathlib import Path

from pipelib.components.filters.preliminary import PreliminaryFilter
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class TestPreliminaryFilter(unittest.TestCase):
    def setUp(self):
        self.config = PipelineConfig(
            input_path=Path(''),
            output_dir=Path(''),
            min_char_len=5,
            max_char_len=20,
            min_ascii_ratio=0.8,
            max_symbol_ratio=0.2,
        )
        self.filter = PreliminaryFilter(self.config)

    def _make_record(
        self,
        text: str,
        char_count: int | None = None,
        ascii_ratio: float = 1.0,
        symbol_ratio: float = 0.0,
    ) -> Record:
        record = Record(text, url="https://example.com")
        record.cleaned = text
        record.char_count = len(text) if char_count is None else char_count
        record.ascii_ratio = ascii_ratio
        record.symbol_ratio = symbol_ratio
        return record

    def test_omit_too_short(self):
        record = self._make_record("abcd", char_count=4)
        record = self.filter.process(record)
        self.assertTrue(record.omit)
        self.assertEqual(record.omit_reason, "too_short")

    def test_omit_too_long(self):
        record = self._make_record("a" * 21, char_count=21)
        record = self.filter.process(record)
        self.assertTrue(record.omit)
        self.assertEqual(record.omit_reason, "too_long")

    def test_omit_non_ascii(self):
        record = self._make_record("valid text", ascii_ratio=0.5)
        record = self.filter.process(record)
        self.assertTrue(record.omit)
        self.assertEqual(record.omit_reason, "non_ascii_heavy")

    def test_omit_symbol_heavy(self):
        record = self._make_record("valid text", symbol_ratio=0.3)
        record = self.filter.process(record)
        self.assertTrue(record.omit)
        self.assertEqual(record.omit_reason, "symbol_heavy")

    def test_omit_long_repeat(self):
        record = self._make_record("aaaaaaaaaa", char_count=10)
        record = self.filter.process(record)
        self.assertTrue(record.omit)
        self.assertEqual(record.omit_reason, "long_repeat")

    def test_keep_valid(self):
        record = self._make_record(
            "This is acceptable text.",
            char_count=15,
            ascii_ratio=1.0,
            symbol_ratio=0.0,
        )
        record = self.filter.process(record)
        self.assertFalse(record.omit)
