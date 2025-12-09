from pipelib.components.core.step import Step
from pipelib.components.core.settings import PipelineConfig
from pipelib.components.core.record import Record


class Modifier(Step):
    """
    Expected to modify the record.cleaned
    """
    def __init__(self, config: PipelineConfig):
        super().__init__(config)

    def process(self, record: Record) -> Record:
        self._modify(record)
        return record

    def _modify(self, record: Record) -> None:
        raise NotImplementedError()
