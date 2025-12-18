import re
from html.parser import HTMLParser
from typing import List, Set
import unittest
from pathlib import Path

from pipelib.components.core import Modifier
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class HTMLExtractorModifier(Modifier):
    """
    Extracts clean text content from HTML documents for LLM training.
    Removes scripts, styles, navigation, ads, and other noise while
    preserving semantic structure and readability.
    """

    # Tags to completely skip (including their content)
    SKIP_TAGS = {
        'script', 'style', 'noscript', 'iframe', 'object', 'embed',
        'svg', 'canvas', 'audio', 'video', 'map', 'area', 'pre', 'code'
    }

    # Tags that typically contain non-content (skip content but continue parsing)
    NOISE_TAGS = {
        'nav', 'header', 'footer', 'aside', 'menu', 'menuitem',
        'form', 'button', 'input', 'select', 'textarea', 'label'
    }

    # Tags that indicate main content areas (prioritize these)
    CONTENT_TAGS = {
        'article', 'main', 'section', 'div', 'p', 'h1', 'h2', 'h3',
        'h4', 'h5', 'h6', 'blockquote', 'pre', 'ul', 'ol', 'li',
        'table', 'tr', 'td', 'th', 'dl', 'dt', 'dd'
    }

    # Inline tags that should preserve spacing
    INLINE_TAGS = {'span', 'a', 'strong', 'em', 'b', 'i', 'small', 'mark'}

    # Common class/id patterns for ads, navigation, and other noise
    NOISE_PATTERNS = re.compile(
        r'(ad|advertisement|banner|sidebar|nav|navigation|menu|footer|header|'
        r'cookie|popup|modal|promo|sponsor|related|comment|share|social|'
        r'widget|subscribe|newsletter)',
        re.IGNORECASE
    )

    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.parser = CleanHTMLParser()

    def _modify(self, record: Record) -> None:
        """Extract clean text from HTML if the record contains HTML."""
        text = record.cleaned

        # Quick check if this looks like HTML
        if not self._is_html(text):
            return

        # Extract clean text
        try:
            clean_text = self.parser.extract_text(text)

            # Post-process the extracted text
            clean_text = self._post_process(clean_text)

            # Only update if we got meaningful content
            if clean_text and len(clean_text.strip()) >= self.config.min_char_len:
                record.cleaned = clean_text
                record.html_extracted = True
        except Exception as e:
            # If parsing fails, leave the record unchanged
            # Could log this: print(f"HTML parsing failed: {e}")
            record.cleaned = text

    @staticmethod
    def _is_html(text: str) -> bool:
        """Quick check if text contains HTML tags."""
        return bool(re.search(r'<[a-zA-Z][^>]*>', text))

    @staticmethod
    def _post_process(text: str) -> str:
        """Clean up extracted text."""
        # Remove excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)  # Multiple spaces to single
        text = re.sub(r'\t+', ' ', text)  # Tabs to spaces

        # Remove lines that are just punctuation or very short
        lines = text.split('\n')
        lines = [
            line.strip()
            for line in lines
            if line.strip() and (len(line.strip()) > 2 or line.strip().isalpha())
        ]

        # Remove duplicate consecutive lines (often from headers/footers)
        deduplicated = []
        prev = None
        for line in lines:
            if line != prev:
                deduplicated.append(line)
            prev = line

        return '\n'.join(deduplicated).strip()


class CleanHTMLParser(HTMLParser):
    """
    Custom HTML parser that extracts clean text while filtering noise.
    """

    def __init__(self):
        super().__init__()
        self.reset_state()

    def reset_state(self):
        """Reset parser state for new document."""
        self.text_chunks: List[str] = []
        self.skip_level = 0  # Track nested skip tags
        self.noise_level = 0  # Track nested noise tags
        self.current_tag_stack: List[str] = []

    def extract_text(self, html: str) -> str:
        """Main entry point to extract text from HTML."""
        self.reset_state()
        self.feed(html)
        return ''.join(self.text_chunks)

    def handle_starttag(self, tag, attrs):
        """Handle opening tags."""
        self.current_tag_stack.append(tag)

        # Check if we should skip this tag and its content
        if tag in HTMLExtractorModifier.SKIP_TAGS:
            self.skip_level += 1
            return

        # Check if this is a noise tag (navigation, forms, etc.)
        if tag in HTMLExtractorModifier.NOISE_TAGS:
            self.noise_level += 1
            return

        # Check class/id for noise patterns
        if self._has_noise_attributes(attrs):
            self.noise_level += 1
            return

        # # Handle specific tags
        # if tag == 'pre':
        #     self.in_pre = True

        # Add spacing for block-level elements
        if tag in HTMLExtractorModifier.CONTENT_TAGS and tag not in HTMLExtractorModifier.INLINE_TAGS:
            # Add newlines before headings and paragraphs for structure
            if tag in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'article', 'section'):
                self._add_text('\n\n')
            elif tag in ('li', 'tr', 'dd'):
                self._add_text('\n')
            elif tag == 'blockquote':
                self._add_text('\n')

        # Handle list markers
        if tag == 'li' and self.skip_level == 0 and self.noise_level == 0:
            self._add_text('• ')

        # Add space before inline tags to prevent word merging
        if tag in HTMLExtractorModifier.INLINE_TAGS and self.skip_level == 0 and self.noise_level == 0:
            # Check if we need a space (if last char isn't already whitespace)
            if self.text_chunks and self.text_chunks[-1] and not self.text_chunks[-1][-1].isspace():
                self._add_text('  ')

    def handle_endtag(self, tag):
        """Handle closing tags."""
        if self.current_tag_stack and self.current_tag_stack[-1] == tag:
            self.current_tag_stack.pop()

        if tag in HTMLExtractorModifier.SKIP_TAGS:
            self.skip_level = max(0, self.skip_level - 1)
            return

        if tag in HTMLExtractorModifier.NOISE_TAGS:
            self.noise_level = max(0, self.noise_level - 1)
            return

        # if tag == 'pre':
        #     self.in_pre = False

        # Add space after inline tags to prevent word merging
        if tag in HTMLExtractorModifier.INLINE_TAGS and self.skip_level == 0 and self.noise_level == 0:
            # Check if we need a space (if last char isn't already whitespace)
            if self.text_chunks and self.text_chunks[-1] and not self.text_chunks[-1][-1].isspace():
                self._add_text(' ')

        # Add spacing after block elements
        if tag in ('p', 'div', 'article', 'section', 'blockquote'):
            self._add_text('\n')
        elif tag in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6'):
            self._add_text('\n\n')

    def handle_data(self, data):
        """Handle text content."""
        # Skip if we're in a skip or noise tag
        if self.skip_level > 0 or self.noise_level > 0:
            return

        # Skip empty or whitespace-only data
        if not data or not data.strip():
            return

        clean_data = ' '.join(data.split())

        self._add_text(clean_data)

    def handle_entityref(self, name):
        """Handle HTML entities like &nbsp;"""
        if self.skip_level > 0 or self.noise_level > 0:
            return

        # Common entities
        entities = {
            'nbsp': ' ',
            'lt': '<',
            'gt': '>',
            'amp': '&',
            'quot': '"',
            'apos': "'",
        }
        self._add_text(entities.get(name, f'&{name};'))

    def handle_charref(self, name):
        """Handle numeric character references like &#123;"""
        if self.skip_level > 0 or self.noise_level > 0:
            return

        try:
            if name.startswith('x'):
                char = chr(int(name[1:], 16))
            else:
                char = chr(int(name))
            self._add_text(char)
        except (ValueError, OverflowError):
            pass

    def _add_text(self, text: str):
        """Add text to the output buffer."""
        if text:
            self.text_chunks.append(text)

    @staticmethod
    def _has_noise_attributes(attrs) -> bool:
        """Check if element has class/id indicating noise content."""
        for attr_name, attr_value in attrs:
            if attr_name in ('class', 'id') and attr_value:
                if HTMLExtractorModifier.NOISE_PATTERNS.search(attr_value):
                    return True
        return False


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

#
# # Example usage and test
# if __name__ == '__main__':
#     from unittest.mock import MagicMock
#
#     # Test HTML
#     test_html = """
#     <p>Ubuntu 9.10</p>
#
# <p>Silly question time: When the clamav-freshclam service is running, how often is clamav checking for updates? Or do I have to manually run freshclam via a cronjob? </p>
#
# <blockquote>
# <p>how often is clamav checking for updates?</p>
# </blockquote>
# <p>Unless you setup a cronjob it will not check for updates.</p>
# <blockquote>
# <p>Do I have to manually run freshclam via a cronjob?</p>
# </blockquote>
# <p>The purpose of a cronjob is to automate the process. You can decide to run it in the following:</p>
# <pre><code>/etc/cron.daily
# /etc/cron.hourly
# /etc/cron.weekly
# /etc/cron.monthly
# </code></pre>
# <p>I recommend <strong>cron.daily</strong> and set it up via a shell script.</p>
# <pre><code>sudo gedit /etc/cron.daily/freshclam.sh
# </code></pre>
# <p>add the lines:</p>
# <pre><code>#!/bin/sh
# /usr/bin/freshclam --quiet
# </code></pre>
# <p>This will now run with all your other cron.daily jobs</p>
# <p>Save and exit</p>
# <pre><code>sudo chmod 755 /etc/cron.daily/freshclam.sh
# </code></pre>
#
#     """
#
#     # Create modifier
#     config = MagicMock()
#     modifier = HTMLExtractorModifier(config)
#
#     # Create record
#     record = Record('', '')
#     record.cleaned = test_html
#
#     # Apply modifier
#     modifier._modify(record)
#
#     print("Extracted text:")
#     print("=" * 50)
#     print(record.cleaned)
#     print("=" * 50)
