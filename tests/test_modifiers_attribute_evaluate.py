import unittest
from pathlib import Path

from pipelib.components.core.settings import PipelineConfig
from pipelib.components.core.record import Record
from pipelib.components.modifiers.attribute_evaluate import AttributeEvaluationStep


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