import unittest
from pathlib import Path

from pipelib.components.core.settings import PipelineConfig
from pipelib.components.core.record import Record
from pipelib.components.modifiers.normalize import NormalizeModifier


class TestNormalizeModifier(unittest.TestCase):
    def test_normalizes_text(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''))
        modifier = NormalizeModifier(config)

        raw = "Hello\u201cworld\u201d &amp; \x0btest\t\t\n\n\nNext"
        record = Record(raw, url="https://example.com")

        modifier.process(record)

        self.assertEqual(record.cleaned, "Hello\"world\" & test \nNext")