import unittest
from pathlib import Path

from pipelib.components.core.settings import PipelineConfig
from pipelib.components.core.record import Record
from pipelib.components.modifiers.pii import PIIModifier


class TestPIIModifier(unittest.TestCase):
    def test_neutralize_pronouns(self):
        record = Record("He told her it was his.", url="https://example.com")
        PIIModifier.neutralize_pronouns(record)

        self.assertTrue(record.anonymized)
        self.assertEqual(record.cleaned, "<HE/SHE> told <HIS/HER> it was <HIS/HER>.")

    def test_anonimize_sets_anonymized(self):
        config = PipelineConfig(input_path=Path(''), output_dir=Path(''))
        modifier = PIIModifier(config)
        record = Record("John Doe email john@example.com", url="https://example.com")

        modifier.anonimize(record)

        self.assertTrue(record.anonymized)
        self.assertIn('<PERSON>', record.cleaned)
        self.assertIn('<EMAIL_ADDRESS>', record.cleaned)
        self.assertNotIn('John Doe', record.cleaned)
        self.assertNotIn('john@example.com', record.cleaned)
        self.assertNotIn('<PHONE_NUMBER>', record.cleaned)