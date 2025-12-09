import re
import hashlib

from pipelib.components.core import Filter, FilterResult
from pipelib.components.core import Record
from pipelib.components.core.settings import PipelineConfig


class DedupFilter(Filter):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.dedup_hashes: set[str] = set()

    def _filter(self, record: Record) -> FilterResult:
        fingerprint = hash_fingerprint(record.cleaned)
        if fingerprint in self.dedup_hashes:
            self.dedup_hashes.add(fingerprint)
            return FilterResult.omit('duplicate')
        return FilterResult.keep()


def hash_fingerprint(text: str) -> str:
    canonical = re.sub(r"\s+", " ", text.lower()).strip()
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()
