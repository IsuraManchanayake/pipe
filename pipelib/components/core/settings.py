from pathlib import Path
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfigDefaults:
    SHARD_SIZE = 10_000
    MIN_CHAR_LEN = 50
    MIN_TOKEN_LEN = 30
    MAX_CHAR_LEN = 20_000
    MIN_ASCII_RATIO = 0.65
    MAX_SYMBOL_RATIO = 0.25
    MIN_STOPWORD_HITS = 3
    REQUIRE_ENGLISH = True
    TOXICITY_THRESHOLD = 0.7
    TOXICITY_BATCH_SIZE = 1000
    WORKERS = 6


@dataclass
class PipelineConfig:
    input_path: Path
    output_dir: Path
    debug_info: bool = False
    input_limit: int = 0
    workers: int = PipelineConfigDefaults.WORKERS
    shard_size: int = PipelineConfigDefaults.SHARD_SIZE
    min_char_len: int = PipelineConfigDefaults.MIN_CHAR_LEN
    min_token_len: int = PipelineConfigDefaults.MIN_TOKEN_LEN
    max_char_len: int = PipelineConfigDefaults.MAX_CHAR_LEN
    min_ascii_ratio: float = PipelineConfigDefaults.MIN_ASCII_RATIO
    max_symbol_ratio: float = PipelineConfigDefaults.MAX_SYMBOL_RATIO
    min_stopword_hits: int = PipelineConfigDefaults.MIN_STOPWORD_HITS

    # Language filter
    require_english: bool = PipelineConfigDefaults.REQUIRE_ENGLISH

    # Toxicity filter
    toxicity_threshold: float = PipelineConfigDefaults.TOXICITY_THRESHOLD
    toxicity_batch_size: int = PipelineConfigDefaults.TOXICITY_BATCH_SIZE

#
# try:
#     nltk.data.find("corpora/stopwords")
# except LookupError:
#     nltk.download("stopwords")
# _english_stopwords = set(nltk.corpus.stopwords.words("english"))


# @dataclass(frozen=True)
# class Constant:
#     PRONOUN_MAP = {
#         "his": "<HIS/HER>",
#         "he": "<HE/SHE>",
#         "him": "<HIM/HER>",
#         "himself": "<HIMSELF/HERSELF>",
#         "she": "<HE/SHE>",
#         "her": "<HIS/HER>",
#         "hers": "<HIS/HER>",
#         "herself": "<HIMSELF/HERSELF>",
#     }
#     PRONOUN_RE = re.compile(
#         r"\b(he|she|his|him|hers|her|himself|herself|He|She|His|Him|Hers|Her|Himself|Herself)\b"
#     )
#     # ENGLISH_STOP_WORDS = _english_stopwords
#     # HTML_CODE_HINTS = (
#     #     "<!doctype html", "<html", "<head", "<body", "<script",
#     #     "</html>", "</body>", "</script>",
#     # )
