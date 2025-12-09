from typing import Iterable

from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class Step:
    def __init__(self, config: PipelineConfig):
        self.config = config

    def process(self, record: Record) -> Record:
        raise NotImplementedError()


class BatchStep:
    def __init__(self, config: PipelineConfig):
        self.config = config

    def process(self, records: Iterable[Record]) -> Iterable[Record]:
        raise NotImplementedError()
