import unittest
from pathlib import Path

from pipelib.components.filters.code_snippet import CodeSnippetFilter
from pipelib.components.filters.preliminary import PreliminaryFilter
from pipelib.components.modifiers.attribute_evaluate import AttributeEvaluationStep
from pipelib.components.modifiers.normalize import NormalizeModifier
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class TestCodeSnippetFilter(unittest.TestCase):
    def test_code_snippet_filter_keeps_natural_text(self):
        record = Record(
            """
            This was an unexpected pleasure. When I started reading, my mental picture
            of Brazil could not have been more different than what I found in this story.
            Descriptions of the northern countryside, a hot dry scrub land, contrast with
            the lush humidity of the regional capital and parallel the storylines of two
            sisters separated by unusual circumstances. Both girls raised and trained as
            seamstresses by their widowed aunt, are eventually drawn in different directions.
            The younger sister Luzia's story of hardship and love within a band of outlaws
            captured my imagination. I was fascinated reading about a group of people
            living outdoors, at the mercy of the elements and the violent struggles that
            accompany that choice. Eliza's life in the city becomes dominated by politics
            inside and outside her home.
            """,
            url="https://example.com",
        )

        pipeline_config = PipelineConfig(input_path=Path(''), output_dir=Path(''))
        record = NormalizeModifier(pipeline_config).process(record)
        record = AttributeEvaluationStep(pipeline_config).process(record)
        record = PreliminaryFilter(pipeline_config).process(record)

        code_snippet = CodeSnippetFilter(pipeline_config)
        record = code_snippet.process(record)
        self.assertFalse(record.omit)

    def test_detects_function_definition(self):
        record = Record("def greet(name):\n    return f\"Hello {name}\"", url="https://example.com")
        pipeline_config = PipelineConfig(input_path=Path(''), output_dir=Path(''))
        code_snippet = CodeSnippetFilter(pipeline_config)
        record = code_snippet.process(record)
        self.assertTrue(record.omit)
        self.assertEqual(record.omit_reason, "code_snippet")
