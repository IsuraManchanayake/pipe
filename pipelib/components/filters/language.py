import logging
import unittest
from pathlib import Path
from unittest import mock

logging.getLogger('fast_langdetect.infer').setLevel(logging.ERROR)

from fast_langdetect import LangDetectConfig, LangDetector

from pipelib.components.core import Filter, FilterResult
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class LanguageFilter(Filter):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.lang_detect_config = LangDetectConfig(model='auto')
        self.lang_detect_model = LangDetector(self.lang_detect_config)

    def _filter(self, record: Record) -> FilterResult:
        if not self.config.require_english:
            return FilterResult.keep()
        record.lang = self.lang_detect_model.detect(record.cleaned, k=1)[0]['lang']
        if record.lang != 'en':
            return FilterResult.omit('non_english')
        return FilterResult.keep()


class TestLanguageFilter(unittest.TestCase):
    def test_skip_when_not_required(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''), require_english=False)
        filter_step = LanguageFilter(config)
        filter_step.lang_detect_model = mock.Mock()

        record = Record("Mi familia no es muy grande, somos solo cuatro personas: mi padre, mi madre, mi hermana y yo. También tenemos un perro.", url="https://example.com")
        record = filter_step.process(record)

        self.assertFalse(record.omit)
        filter_step.lang_detect_model.detect.assert_not_called()

    def test_omit_non_english(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''), require_english=True)
        filter_step = LanguageFilter(config)

        record = Record("Mi familia no es muy grande, somos solo cuatro personas: mi padre, mi madre, mi hermana y yo. También tenemos un perro.", url="https://example.com")
        record = filter_step.process(record)

        self.assertTrue(record.omit)
        self.assertEqual(record.omit_reason, "non_english")
        self.assertEqual(record.lang, "es")

    def test_keep_english(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''), require_english=True)
        filter_step = LanguageFilter(config)

        record = Record("hello world", url="https://example.com")
        record = filter_step.process(record)

        self.assertFalse(record.omit)
        self.assertEqual(record.lang, "en")


# Attempt downloading from a single thread and cold start
_lang_detect_config = LangDetectConfig(model='auto')
_lang_detect_model = LangDetector(_lang_detect_config)
_lang = _lang_detect_model.detect('hello', k=1)[0]['lang']
del _lang
del _lang_detect_model
del _lang_detect_config
