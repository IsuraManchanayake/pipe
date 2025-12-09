from fast_langdetect import LangDetectConfig, LangDetector

from pipelib.components.core import Filter, FilterResult
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class LanguageFilter(Filter):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.lang_detect_config = LangDetectConfig(model='auto')
        self.lang_detect_model = LangDetector(self.lang_detect_config)

    def _filter(self, record: Record) -> FilterResult:
        if not self.config.require_english:
            return FilterResult.keep()
        record.lang = self.lang_detect_model.detect(record.cleaned, k=1)[0]['lang']
        if record.lang != 'en':
            return FilterResult.omit('non_english')
        return FilterResult.keep()
