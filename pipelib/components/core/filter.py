from dataclasses import dataclass
from typing import Tuple, Iterable
from enum import Enum

from pipelib.components.core.step import Step, BatchStep
from pipelib.components.core.settings import PipelineConfig
from pipelib.components.core.record import Record


class FilterStatus(Enum):
    KEEP = "keep"
    OMIT = "omit"


@dataclass
class FilterResult:
    status: FilterStatus = FilterStatus.KEEP
    reason: str|None = None

    @staticmethod
    def omit(reason: str):
        return FilterResult(FilterStatus.OMIT, reason)

    @staticmethod
    def keep():
        return FilterResult(FilterStatus.KEEP, None)


class Filter(Step):
    """
    Expected to modify the record.omit and record.omit_reason
    """
    def __init__(self, config: PipelineConfig):
        super().__init__(config)

    def process(self, record: Record) -> Record:
        result = self._filter(record)
        if result.status is FilterStatus.OMIT:
            record.omit = True
            record.omit_reason = result.reason
        return record

    def _filter(self, record: Record) -> FilterResult:
        raise NotImplementedError()


class BatchFilter(BatchStep):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)

    def batch_process(self, records: Iterable[Record]) -> Iterable[Record]:
        for record, filter_result in zip(records, self._batch_filter(records)):
            if filter_result.status is FilterStatus.OMIT:
                record.omit = True
                record.omit_reason = filter_result.reason
        return records

    def _batch_filter(self, records: Iterable[Record]) -> Iterable[FilterResult]:
        raise NotImplementedError()