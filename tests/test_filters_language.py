import unittest
from pathlib import Path
from unittest import mock

from pipelib.components.filters.language import LanguageFilter
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class TestLanguageFilter(unittest.TestCase):
    def test_skip_when_not_required(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''), require_english=False)
        filter_step = LanguageFilter(config)
        filter_step.lang_detect_model = mock.Mock()

        record = Record("hola mundo", url="https://example.com")
        record = filter_step.process(record)

        self.assertFalse(record.omit)
        filter_step.lang_detect_model.detect.assert_not_called()

    def test_omit_non_english(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''), require_english=True)
        filter_step = LanguageFilter(config)

        class DummyDetector:
            def detect(self, text, k=1):
                return [{'lang': 'es'}]

        filter_step.lang_detect_model = DummyDetector()
        record = Record("hola mundo", url="https://example.com")
        record = filter_step.process(record)

        self.assertTrue(record.omit)
        self.assertEqual(record.omit_reason, "non_english")
        self.assertEqual(record.lang, "es")

    def test_keep_english(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''), require_english=True)
        filter_step = LanguageFilter(config)

        class DummyDetector:
            def detect(self, text, k=1):
                return [{'lang': 'en'}]

        filter_step.lang_detect_model = DummyDetector()
        record = Record("hello world", url="https://example.com")
        record = filter_step.process(record)

        self.assertFalse(record.omit)
        self.assertEqual(record.lang, "en")
