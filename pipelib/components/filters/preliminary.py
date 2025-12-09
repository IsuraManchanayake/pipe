import re

from pipelib.components.core import Filter, FilterResult
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class PreliminaryFilter(Filter):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)

    def _filter(self, record: Record) -> FilterResult:
        if record.char_count < self.config.min_char_len:
            return FilterResult.omit('too_short')
        if record.char_count > self.config.max_char_len:
            return FilterResult.omit('too_long')
        if record.ascii_ratio < self.config.min_ascii_ratio:
            return FilterResult.omit('non_ascii_heavy')
        if record.symbol_ratio > self.config.max_symbol_ratio:
            return FilterResult.omit('symbol_heavy')
        # has_long_repeat = bool(re.search(r"(.)\1{9,}", record.cleaned))
        # if has_long_repeat:
        #     return FilterResult.omit('long_repeat')
        return FilterResult.keep()
