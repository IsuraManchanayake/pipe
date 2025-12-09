import json


class Record:
    _next_id = 1

    def __init__(self, original: str, url: str):
        self.original: str = original
        self.url: str = url
        self.id: int = Record._next_id

        Record._next_id += 1

        # Derived
        self.cleaned: str = self.original
        self.lang: str|None = None
        self.char_count: int|None = None
        self.token_count: int|None = None
        self.ascii_ratio: float|None = None
        self.symbol_ratio: float|None = None
        self.tokens: list[str]|None = None

        self.omit: bool = False
        self.omit_reason: str|None = None

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'cleaned': self.cleaned,
            'lang': self.lang,
            'original': self.original,
        }

    def write_jsonl(self, handle):
        d = self.to_dict()
        handle.write(json.dumps(d))
        handle.write('\n')
