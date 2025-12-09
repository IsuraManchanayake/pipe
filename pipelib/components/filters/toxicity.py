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
        tox_scores =  self.detoxify_model.predict([record.cleaned for record in records])['toxicity']
        return [
            FilterResult.omit('toxic_content') if tox > PipelineConfig.toxicity_threshold else FilterResult.keep()
            for tox in tox_scores
        ]


class ToxicityFilter(Filter):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.detoxify_model = Detoxify('original-small')

    def _filter(self, record: Record) -> FilterResult:
        tox_score = self.detoxify_model.predict(record.cleaned)['toxicity']
        return FilterResult.omit('toxic_content') \
            if tox_score > PipelineConfig.toxicity_threshold else FilterResult.keep()
