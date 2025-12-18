import re
import html
import unittest
from pathlib import Path

from pipelib.components.core.record import Record
from pipelib.components.core.modifier import Modifier
from pipelib.components.core.settings import PipelineConfig


class NormalizeModifier(Modifier):
    CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
    WHITESPACE_AND_NEWLINE_RE = re.compile(r"[ \t]+|\n{2,}")
    QUOTE_TRANSLATION_TABLE = str.maketrans({
        "\u201c": '"',
        "\u201d": '"',
        "\u2018": "'",
        "\u2019": "'",
    })
    def __init__(self, config: PipelineConfig):
        super().__init__(config)

    def _modify(self, record: Record) -> None:
        text = record.cleaned
        text = html.unescape(text or "")
        text = NormalizeModifier.CONTROL_CHAR_RE.sub(" ", text)
        text = text.translate(NormalizeModifier.QUOTE_TRANSLATION_TABLE)
        text = NormalizeModifier.WHITESPACE_AND_NEWLINE_RE.sub(_collapse_whitespace, text)
        record.cleaned = text


def _collapse_whitespace(match: re.Match[str]) -> str:
    return "\n" if "\n" in match.group(0) else " "


class TestNormalizeModifier(unittest.TestCase):
    def test_normalizes_text(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''))
        modifier = NormalizeModifier(config)

        raw = "Hello\u201cworld\u201d &amp; \x0btest\t\t\n\n\nNext"
        record = Record(raw, url="https://example.com")

        modifier.process(record)

        self.assertEqual(record.cleaned, "Hello\"world\" & test \nNext")
