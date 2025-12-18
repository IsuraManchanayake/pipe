import unittest
from pathlib import Path

from pipelib.components.filters.dedup import DedupFilter, hash_fingerprint
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class TestDedupFilter(unittest.TestCase):
    def setUp(self):
        self.config = PipelineConfig(input_path=Path(''), output_dir=Path(''), workers=1)

    def test_hash_fingerprint_canonicalizes(self):
        self.assertEqual(hash_fingerprint("Hello   world"), hash_fingerprint("hello world"))

    def test_dedup_detects_duplicates(self):
        filter_step = DedupFilter(self.config)
        record_1 = Record("Hello   world", url="https://example.com")
        record_2 = Record("hello world", url="https://example.com/2")

        record_1 = filter_step.process(record_1)
        record_2 = filter_step.process(record_2)

        self.assertFalse(record_1.omit)
        self.assertTrue(record_2.omit)
        self.assertEqual(record_2.omit_reason, "duplicate")
