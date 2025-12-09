from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig
from pipelib.components.core.step import Step

class AttributeModifier(Step):
    """
    Expected to modify the attribute of a record except record.cleaned, record.omit, record.omit_reason
    """
    def __init__(self, config: PipelineConfig):
        super().__init__(config)

    def process(self, record: Record) -> Record:
        self._modify_attributes(record)
        return record

    def _modify_attributes(self, record: Record) -> None:
        raise NotImplementedError()