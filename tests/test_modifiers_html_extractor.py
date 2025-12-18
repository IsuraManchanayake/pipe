import unittest
from pathlib import Path

from pipelib.components.core.settings import PipelineConfig
from pipelib.components.core.record import Record
from pipelib.components.modifiers.html_extractor import HTMLExtractorModifier


class TestHTMLExtractorModifier(unittest.TestCase):
    def test_extracts_content_and_skips_noise(self):
        html = (
            "<html><body>"
            "<nav>Menu</nav>"
            "<p>Hello <a href=\"#\">world</a>!</p>"
            "<div class=\"ad\">Buy now</div>"
            "</body></html>"
        )
        record = Record(html, url="https://example.com")
        modifier = HTMLExtractorModifier(PipelineConfig(input_path=Path(''), output_dir=Path(''), min_char_len=1))
        modifier.process(record)

        self.assertTrue(record.html_extracted)
        self.assertIn("Hello", record.cleaned)
        self.assertIn("world", record.cleaned)
        self.assertNotIn("Menu", record.cleaned)
        self.assertNotIn("Buy now", record.cleaned)

    def test_skips_non_html_input(self):
        record = Record("Just plain text", url="https://example.com")
        modifier = HTMLExtractorModifier(PipelineConfig(input_path=Path(''), output_dir=Path(''), min_char_len=1))
        modifier.process(record)

        self.assertFalse(record.html_extracted)
        self.assertEqual(record.cleaned, "Just plain text")
