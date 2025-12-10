import re
import logging

logging.getLogger('presidio-analyzer').setLevel(logging.ERROR)
logging.getLogger('presidio-anonymizer').setLevel(logging.ERROR)


from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine

from pipelib.components.core import Modifier
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class PIIModifier(Modifier):
    PRONOUN_MAP = {
        "his": "<HIS/HER>",
        "he": "<HE/SHE>",
        "him": "<HIM/HER>",
        "himself": "<HIMSELF/HERSELF>",
        "she": "<HE/SHE>",
        "her": "<HIS/HER>",
        "hers": "<HIS/HER>",
        "herself": "<HIMSELF/HERSELF>",
    }
    PRONOUN_RE = re.compile(
        r"\b(he|she|his|him|hers|her|himself|herself|He|She|His|Him|Hers|Her|Himself|Herself)\b"
    )

    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.pii_analyzer = AnalyzerEngine()
        self.pii_anonymizer = AnonymizerEngine()

    def _modify(self, record: Record) -> None:
        self.anonimize(record)
        self.neutralize_pronouns(record)

    def anonimize(self, record: Record) -> None:
        analyzer_results = self.pii_analyzer.analyze(
            text=record.cleaned,
            language='en',
            entities=['PERSON', 'EMAIL_ADDRESS', 'LOCATION', 'PHONE_NUMBER', 'IP_ADDRESS'],
        )
        if analyzer_results:
            anon_result = self.pii_anonymizer.anonymize(
                text=record.cleaned,
                analyzer_results=analyzer_results,
            )
            record.cleaned = anon_result.text
            record.anonymized = True

    @staticmethod
    def neutralize_pronouns(record: Record) -> None:
        def repl(match: re.Match[str]) -> str:
            word = match.group(0)
            lower = word.lower()
            return PIIModifier.PRONOUN_MAP.get(lower, word)

        record.cleaned = PIIModifier.PRONOUN_RE.sub(repl, record.cleaned)


# Attempt downloading from a single thread code start
_temp_analyzer_engine = AnalyzerEngine()
_temp_anonymizer = AnonymizerEngine()
_temp_analyzer_results = _temp_analyzer_engine.analyze(
    text='hello',
    language='en',
    entities=['PERSON', 'EMAIL_ADDRESS', 'LOCATION', 'PHONE_NUMBER', 'IP_ADDRESS'],
)
_temp_anon_result = _temp_anonymizer.anonymize(
    text='hello',
    analyzer_results=_temp_analyzer_results,
)
del _temp_anon_result
del _temp_analyzer_results
del _temp_analyzer_engine
del _temp_anonymizer
