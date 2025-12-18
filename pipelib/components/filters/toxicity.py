from typing import Iterable

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


# Attempt downloading from a single thread cold start
_detoxify_model = Detoxify('original-small')
_tox_score = _detoxify_model.predict('hello')['toxicity']
del _tox_score
del _detoxify_model
