import unittest
from pathlib import Path

from pipelib.components.core.settings import PipelineConfig
from pipelib.components.core.record import Record
from pipelib.components.filters.toxicity import ToxicityFilter


class TestToxicityFilter(unittest.TestCase):
    def test_toxicity_filter_omit(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''))
        filter_step = ToxicityFilter(config)

        record = Record("You are very very ugly!!", url="https://example.com")
        record = filter_step.process(record)

        self.assertTrue(record.omit)
        self.assertEqual(record.omit_reason, "toxic_content")

    def test_toxicity_filter_keep(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''), toxicity_threshold=0.7)
        filter_step = ToxicityFilter(config)

        record = Record("You are a very nice person", url="https://example.com")
        record = filter_step.process(record)

        self.assertFalse(record.omit)