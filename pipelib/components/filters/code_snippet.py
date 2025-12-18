import re

from pipelib.components.core import Filter, FilterResult
from pipelib.components.core.record import Record
from pipelib.components.core.settings import PipelineConfig


class CodeSnippetFilter(Filter):
    """
    Filter to detect and remove code snippets from text.
    Optimized for catching Python, JavaScript, and HTML while avoiding false positives.
    """

    # Code-specific punctuation patterns (reduced set)
    CODE_PUNCT_CHARS = set("{}[]();=<>")

    # Expanded keyword sets by language
    CODE_KEYWORDS = {
        # Python
        "def", "lambda", "import", "from", "try", "except", "finally",
        "with", "yield", "async", "await", "self", "cls", "None", "True", "False",
        "print", "range", "len", "str", "int", "float", "list", "dict", "tuple",
        "isinstance", "enumerate", "zip", "__init__", "__name__", "__main__",
        "elif", "pass", "raise", "assert", "global", "nonlocal",

        # JavaScript
        "function", "var", "let", "const", "console", "document", "window",
        "onclick", "typeof", "instanceof", "prototype", "constructor",
        "getElementById", "querySelector", "addEventListener", "setTimeout",
        "undefined", "require", "module", "export",

        # Common across languages
        "void", "bool", "array", "public", "private", "protected", "static",
        "final", "class", "new", "this",
    }

    # Common code patterns
    FUNCTION_DEF_PATTERN = re.compile(r'\b(def|function|const|let|var)\s+\w+\s*\(')
    ASSIGNMENT_PATTERN = re.compile(r'^\s*\w+\s*=\s*[\w\[\{\(\'"]', re.MULTILINE)
    METHOD_CALL_PATTERN = re.compile(r'\.\w+\s*\(')

    # Common code line starters (must be at start of line after whitespace)
    CODE_LINE_STARTERS = (
        '//', '#', '/*', '*/', '--', '<!--',
        'import ', 'from ', 'const ', 'let ', 'var ', 'def ',
        'function ', 'class ', 'async ', 'export ', 'return ',
        'public ', 'private ', 'protected '
    )

    ENGLISH_STOPWORDS = {
        "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "you're", "you've", "you'll", "you'd",
        "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "she's", "her", "hers",
        "herself", "it", "it's", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which",
        "who", "whom", "this", "that", "that'll", "these", "those", "am", "is", "are", "was", "were", "be", "been",
        "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if",
        "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into",
        "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off",
        "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all",
        "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own",
        "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "don't", "should", "should've",
        "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren", "aren't", "couldn", "couldn't", "didn", "didn't",
        "doesn", "doesn't", "hadn", "hadn't", "hasn", "hasn't", "haven", "haven't", "isn", "isn't", "ma", "mightn",
        "mightn't", "mustn", "mustn't", "needn", "needn't", "shan", "shan't", "shouldn", "shouldn't", "wasn", "wasn't",
        "weren", "weren't", "won", "won't", "wouldn", "wouldn't"
    }

    # Common code line enders
    CODE_LINE_ENDERS = ('{', '}', ';', '=>', ');', ']);', '};', '},')

    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.english_stopwords = CodeSnippetFilter.ENGLISH_STOPWORDS

    def _has_strong_code_patterns(self, text: str) -> bool:
        """Check for strong indicators of code structure."""
        # Function definitions are very strong signals
        if self.FUNCTION_DEF_PATTERN.search(text):
            return True

        # Multiple method calls (object.method())
        if len(self.METHOD_CALL_PATTERN.findall(text)) >= 3:
            return True

        # Multiple assignments at line starts
        if len(self.ASSIGNMENT_PATTERN.findall(text)) >= 4:
            return True

        return False

    @staticmethod
    def _count_bracket_pairs(text: str) -> int:
        """Count matched bracket pairs (indicates structured code)."""
        pairs = 0
        pairs += min(text.count('('), text.count(')'))
        pairs += min(text.count('['), text.count(']'))
        pairs += min(text.count('{'), text.count('}'))
        return pairs

    @staticmethod
    def _found_code_snippet():
        return FilterResult.omit('code_snippet')

    def _filter(self, record: Record) -> FilterResult:
        text = record.cleaned
        low = text.lower()

        # Strong pattern check - immediate rejection
        if self._has_strong_code_patterns(text):
            return CodeSnippetFilter._found_code_snippet()

        # Analyze lines
        lines = [ln for ln in low.splitlines() if ln.strip()]
        if not lines:
            return FilterResult.keep()

        total_lines = len(lines)
        codey_lines = 0
        indent_lines = 0

        for ln in lines:
            stripped = ln.lstrip()
            if not stripped:
                continue

            # Count heavily indented lines (8+ spaces or 2+ tabs)
            indent_amount = len(ln) - len(stripped)
            if indent_amount >= 8 or ln.startswith('\t\t'):
                indent_lines += 1

            # Check line starters (must be at beginning of stripped line)
            if stripped.startswith(self.CODE_LINE_STARTERS):
                codey_lines += 1
            # Check line enders
            elif any(stripped.endswith(ender) for ender in self.CODE_LINE_ENDERS):
                codey_lines += 1

        # Calculate ratios
        codey_ratio = codey_lines / total_lines
        indent_ratio = indent_lines / total_lines

        # High proportion of code-like lines (stricter threshold)
        if codey_ratio > 0.4:
            return CodeSnippetFilter._found_code_snippet()

        # Combined high indent + code lines
        if indent_ratio > 0.5 and codey_ratio > 0.25:
            return CodeSnippetFilter._found_code_snippet()

        # Character-level analysis
        code_punct_count = sum(1 for c in text if c in self.CODE_PUNCT_CHARS)
        code_punct_ratio = code_punct_count / max(len(text), 1)

        # Bracket pair analysis
        bracket_pairs = self._count_bracket_pairs(text)
        bracket_density = bracket_pairs / max(len(text), 1) * 100  # per 100 chars

        # Token analysis
        if not record.tokens:
            tokens = low.split()
        else:
            tokens = record.tokens

        if len(tokens) == 0:
            return FilterResult.keep()

        stop_hits = sum(1 for t in tokens if t in self.english_stopwords)
        stop_ratio = stop_hits / len(tokens)

        code_kw_hits = sum(1 for t in tokens if t in self.CODE_KEYWORDS)
        code_kw_ratio = code_kw_hits / len(tokens)

        # Natural text typically has 20-40% stopwords, so we use much lower thresholds

        # Very strong signal: extremely low stopwords + high code indicators
        if stop_ratio < 0.05 and code_kw_hits >= 3 and code_punct_ratio > 0.15:
            return CodeSnippetFilter._found_code_snippet()

        # High bracket density with very low stopwords
        if bracket_density > 3.0 and stop_ratio < 0.08:
            return CodeSnippetFilter._found_code_snippet()

        # High keyword concentration
        if code_kw_ratio > 0.20 and stop_ratio < 0.10:
            return CodeSnippetFilter._found_code_snippet()

        # Very short snippets need stricter rules
        if len(tokens) < 15:
            if code_kw_hits >= 2 and stop_ratio < 0.05 and code_punct_ratio > 0.12:
                return CodeSnippetFilter._found_code_snippet()

        # Longer text with moderate code signals
        if len(tokens) >= 30:
            if code_kw_hits >= 5 and code_kw_ratio > 0.15 and stop_ratio < 0.10:
                return CodeSnippetFilter._found_code_snippet()

        return FilterResult.keep()
