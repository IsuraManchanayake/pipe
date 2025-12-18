from typing import Iterable
import unittest
from pathlib import Path
from unittest import mock

from detoxify import Detoxify

from pipelib.components.core import BatchFilter, FilterResult, Filter
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class ToxicityBatchFilter(BatchFilter):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.detoxify_model = Detoxify('original-small')

    def _batch_filter(self, records: Iterable[Record]) -> Iterable[FilterResult]:
        record_list = list(records)
        tox_scores = self.detoxify_model.predict([record.cleaned for record in record_list])['toxicity']
        return [
            FilterResult.omit('toxic_content') if tox > self.config.toxicity_threshold else FilterResult.keep()
            for tox in tox_scores
        ]


class ToxicityFilter(Filter):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.detoxify_model = Detoxify('original-small')

    def _filter(self, record: Record) -> FilterResult:
        tox_score = self.detoxify_model.predict(record.cleaned)['toxicity']
        return FilterResult.omit('toxic_content') \
            if tox_score > self.config.toxicity_threshold else FilterResult.keep()


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


# Attempt downloading from a single thread cold start
_detoxify_model = Detoxify('original-small')
_tox_score = _detoxify_model.predict('hello')['toxicity']
del _tox_score
del _detoxify_model
