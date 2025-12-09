import re
import hashlib

from pipelib.components.core import Filter, FilterResult
from pipelib.components.core import Record
from pipelib.components.core.settings import PipelineConfig


class DedupFilter(Filter):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.dedup_hashes: set[str] = set()
        self._lock = None
        if config.workers > 1:
            import threading
            self._lock = threading.Lock()

    def _filter(self, record: Record) -> FilterResult:
        fingerprint = hash_fingerprint(record.cleaned)
        if self._lock:
            with self._lock:
                if fingerprint in self.dedup_hashes:
                    return FilterResult.omit('duplicate')
                self.dedup_hashes.add(fingerprint)
        else:
            if fingerprint in self.dedup_hashes:
                return FilterResult.omit('duplicate')
            self.dedup_hashes.add(fingerprint)
        return FilterResult.keep()


def hash_fingerprint(text: str) -> str:
    canonical = re.sub(r"\s+", " ", text.lower()).strip()
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()
