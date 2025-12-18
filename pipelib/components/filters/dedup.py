import re
import hashlib
import unittest
from pathlib import Path

from pipelib.components.core import Filter, FilterResult
from pipelib.components.core import Record
from pipelib.components.core.settings import PipelineConfig


class DedupFilter(Filter):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.dedup_hashes: set[str] = set()
        self._lock = None
        if config.workers > 1:
            import threading
            self._lock = threading.Lock()

    def _filter(self, record: Record) -> FilterResult:
        fingerprint = hash_fingerprint(record.cleaned)
        if self._lock:
            with self._lock:
                if fingerprint in self.dedup_hashes:
                    return FilterResult.omit('duplicate')
                self.dedup_hashes.add(fingerprint)
        else:
            if fingerprint in self.dedup_hashes:
                return FilterResult.omit('duplicate')
            self.dedup_hashes.add(fingerprint)
        return FilterResult.keep()


def hash_fingerprint(text: str) -> str:
    canonical = re.sub(r"\s+", " ", text.lower()).strip()
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


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
