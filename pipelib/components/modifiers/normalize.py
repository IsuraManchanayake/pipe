import re
import html

from pipelib.components.core.record import Record
from pipelib.components.core.modifier import Modifier
from pipelib.components.core.settings import PipelineConfig


class NormalizeModifier(Modifier):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)

    def _modify(self, record: Record) -> None:
        text = record.cleaned
        text = html.unescape(text or "")
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", text)
        text = text.replace("\u201c", '"').replace("\u201d", '"')
        text = text.replace("\u2018", "'").replace("\u2019", "'")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{2,}", "\n", text)
        record.cleaned = text
