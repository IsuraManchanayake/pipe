import re
from typing import List

from pipelib.components.core.attribute_modifier import AttributeModifier
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class AttributeEvaluationStep(AttributeModifier):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)

    def _modify_attributes(self, record: Record) -> None:
        record.tokens = tokenize(record.cleaned)
        record.char_count = len(record.cleaned)
        record.token_count = len(record.tokens)
        record.ascii_ratio = compute_ascii_ratio(record.cleaned)
        record.symbol_ratio = compute_symbol_ratio(record.cleaned)


def tokenize(text) -> List[str]:
    return re.findall(r"[A-Za-z']+", text.lower())


def compute_ascii_ratio(text: str) -> float:
    if not text:
        return 0.0
    ascii_chars = sum(1 for c in text if ord(c) < 128)
    return ascii_chars / len(text)


def compute_symbol_ratio(text: str) -> float:
    if not text:
        return 0.0
    symbols = sum(1 for c in text if not c.isalnum() and not c.isspace())
    return symbols / len(text)
