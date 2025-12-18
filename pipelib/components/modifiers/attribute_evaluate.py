import re
from typing import List
import unittest
from pathlib import Path

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

        ascii_count, symbol_count = compute_char_counts(record.cleaned)
        record.ascii_ratio = ascii_count / record.char_count if record.char_count else 0.0
        record.symbol_ratio = symbol_count / record.char_count if record.char_count else 0.0


TOKEN_RE = re.compile(r"[A-Za-z']+")


def tokenize(text) -> List[str]:
    return TOKEN_RE.findall(text.lower())


def compute_char_counts(text: str) -> tuple[int, int]:
    ascii_count = 0
    symbol_count = 0
    for c in text:
        if ord(c) < 128:
            ascii_count += 1
        if not c.isalnum() and not c.isspace():
            symbol_count += 1
    return ascii_count, symbol_count


class TestAttributeEvaluationStep(unittest.TestCase):
    def test_computes_attributes(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''))
        step = AttributeEvaluationStep(config)

        record = Record("Hi there!!!", url="https://example.com")
        step.process(record)

        self.assertEqual(record.tokens, ["hi", "there"])
        self.assertEqual(record.char_count, len("Hi there!!!"))
        self.assertEqual(record.token_count, 2)
        self.assertAlmostEqual(record.ascii_ratio, 1.0)
        self.assertAlmostEqual(record.symbol_ratio, 3 / len("Hi there!!!"))
