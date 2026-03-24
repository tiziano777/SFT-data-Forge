import re
import math
import json
from collections import Counter
from datatrove.data import Document
from datatrove.data import DocumentsPipeline
from datatrove.io import DataFolder
from datatrove.pipeline.stats.base import BaseStats
from datatrove.pipeline.stats.config import DEFAULT_TOP_K_CONFIG, GROUP, TopKConfig
from datatrove.utils.text import PUNCTUATION

import os


STOPWORD_PATH= os.getenv("STOPWORD_PATH")

ELIPSIS = ["...", "…"]

# Definizione dei bullet points Unicode
BULLET_POINTS = [
    '\u2022',  # U+2022 (bullet point)
    '\u2023',  # U+2023 (triangular bullet point)
    '\u25B6',  # U+25B6 (black right pointing triangle)
    '\u25C0',  # U+25C0 (black left pointing triangle)
    '\u25E6',  # U+25E6 (white bullet point)
    '\u2013',  # U+2013 (en dash)
    '\u25A0',  # U+25A0 (black square)
    '\u25A1',  # U+25A1 (white square)
    '\u25AA',  # U+25AA (black small square)
    '\u25AB',  # U+25AB (white small square)
]

FERTILITY_MAP = {
    'ar': 2.277, 'bg': 2.788,  'cs': 2.679, 'da': 2.169,
    'de': 2.130, 'el': 2.532, 'en': 1.384, 'es': 1.476, 'et': 2.925,
    'fi': 3.211, 'fr': 1.672, 'ga': 2.237, 'hi': 5.760, 'hr': 2.807,
    'hu': 3.066, 'it': 1.445, 'ja': 43.644, 'lt': 2.907, 'lv': 2.830,
    'mt': 2.791, 'nl': 2.068, 'pl': 2.814, 'pt': 1.509, 'ro': 2.160,
    'ru': 3.072, 'sk': 2.593, 'sl': 2.399, 'sv': 2.523, 'sw': 2.406,
    'un': 1.500  # Valore di sicurezza per lingue non mappate
    # 'code': 2.654,
}

class DocStats(BaseStats):
    """
    Enhanced document statistics with optimized low-level metrics.
    Genera SOLO statistiche tabellari per PostgreSQL, senza aggregazioni.
    """

    name = "📜 Low Level Stats"

    def __init__(
        self,
        output_folder: DataFolder,
        groups_to_compute: list[GROUP] = None,
        histogram_round_digits: int = 3,
        top_k_config: TopKConfig = DEFAULT_TOP_K_CONFIG,
        stopwords_config_path: str = STOPWORD_PATH,
    ) -> None:
        super().__init__(output_folder, groups_to_compute or [], histogram_round_digits, top_k_config)
        
        self.elipsis_regex = re.compile("|".join([f"(?:{re.escape(elipsis)})" for elipsis in ELIPSIS]))
        self.punc_regex = re.compile("|".join([f"(?:{re.escape(punc)})" for punc in PUNCTUATION]))
        
        # Pattern simboli più inclusivo (include bullet points)
        self.symbol_pattern = re.compile(r'[#\$%\&\*\+\=\@\^\\\/\•\–\-\u2022\u2023\u25B6\u25C0\u25E6\u2013\u25A0\u25A1\u25AA\u25AB]')
        self.escape_pattern = re.compile(r'[\n\t\r]')
        self.javascript_pattern = re.compile(r'\bjavascript\b', re.IGNORECASE)
        
        # Pattern per bullet points all'inizio riga
        bullet_pattern = "|".join([re.escape(bp) for bp in BULLET_POINTS])
        self.bullet_start_pattern = re.compile(f"^\\s*[{bullet_pattern}]")
        
        # Pattern punteggiatura finale più preciso
        self.end_punctuation_pattern = re.compile(r'[.!?;:]$')
    
        # Cache per le stopwords
        self._stopwords_cache = {}
        self.stopwords_config_path = stopwords_config_path

    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1):
        """
        Sovrascrive completamente il metodo run per generare SOLO tabular stats.
        """
        tabular_stats = []

        for doc in data:
            with self.track_time():
                try:
                    doc_stats = self.extract_stats(doc)
                except Exception as e:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.error(f"Error while extracting stats from document {doc.id}", exc_info=e)
                    raise e

                # SALVA SOLO LE TABULAR STATS
                tabular_record = {
                    'id': doc.id,
                    **doc_stats  # Unpack tutte le statistiche allo stesso livello
                }
                tabular_stats.append(tabular_record)

                # OPZIONALE: aggiorna i metadati se serve per downstream
                # doc.metadata.update(doc_stats)

            yield doc

        # SALVA LE TABULAR STATS
        if tabular_stats:
            import pandas as pd
            df = pd.DataFrame(tabular_stats)
            stats_path= self.output_folder.path 
            import os
            if not os.path.exists(stats_path):
                os.makedirs(stats_path)
            df.to_parquet(stats_path + f"/low_level_stats_{rank:05d}.parquet", index=False)
            
        print(f"✅ Generate {len(tabular_stats)} tabular stats records")

    def _load_stopwords(self, lang: str) -> set:
        """Carica le stopwords per una lingua con caching."""
        if lang in self._stopwords_cache:
            return self._stopwords_cache[lang]
        
        try:
            with open(self.stopwords_config_path, 'r', encoding='utf-8') as f:
                lang_config = json.load(f)
                
                # Struttura: {"it": ["parola1", "parola2", ...], "en": ["word1", ...]}
                if lang in lang_config and isinstance(lang_config[lang], list):
                    stopwords = set(word.lower() for word in lang_config[lang])
                    self._stopwords_cache[lang] = stopwords
                    return stopwords
                else:
                    print(f"⚠️  Lingua '{lang}' non trovata nel file stopwords")
                    return set()
                    
        except Exception as e:
            print(f"❌ Errore caricamento stopwords: {e}")
            # Fallback hardcoded
            fallback_stopwords = {
                'it': {'questo', 'è', 'un', 'di', 'con', 'come', 'e', 'che', 'la', 'le', 'il', 'lo', 'questo', 'questa', 'un', 'una'},
                'en': {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'this', 'is', 'a'}
            }
            return fallback_stopwords.get(lang, set())

    def _precompute_structures(self, text: str):
        """Pre-calcola strutture dati riutilizzabili per ottimizzare le performance."""
        lines = text.splitlines()
        words = text.split()
        chars = list(text)
        
        return {
            'lines': lines,
            'words': words,
            'chars': chars,
            'line_lengths': [len(line) for line in lines],
            'word_counter': Counter(words),
            'char_counter': Counter(chars)
        }

    def extract_stats(self, doc: Document) -> dict[str, int | float]:
        """Estrae tutte le statistiche dal documento."""
        text = doc.text
        if not text:
            return self._get_empty_stats()
            
        # Pre-calcolo delle strutture dati
        precomputed = self._precompute_structures(text)
        lang = doc.metadata.get('_lang', 'en') if doc.metadata else 'en'
        
        # Calcolo più accurato delle righe che terminano con punteggiatura
        lines_end_punctuation = 0
        for line in precomputed['lines']:
            stripped_line = line.strip()
            if stripped_line and self.end_punctuation_pattern.search(stripped_line):
                lines_end_punctuation += 1
        
        # Calcolo statistiche
        stats = {
            # Metriche originali DataTrove
            "_length": len(text),
            "_white_space_ratio": sum(1 for c in text if c.isspace()) / len(text) if text else 0,
            "_non_alpha_digit_ratio": sum(1 for c in text if not c.isalpha() and not c.isdigit()) / len(text) if text else 0,
            "_digit_ratio": sum(1 for c in text if c.isdigit()) / len(text) if text else 0,
            "_uppercase_ratio": sum(1 for c in text if c.isupper()) / len(text) if text else 0,
            "_elipsis_ratio": sum(len(elipsis) for elipsis in self.elipsis_regex.findall(text)) / len(text) if text else 0,
            "_punctuation_ratio": sum(len(punc) for punc in self.punc_regex.findall(text)) / len(text) if text else 0,

            # Statistiche righe
            "_lines_count": len(precomputed['lines']),
            "_lines_empty_count": sum(1 for line in precomputed['lines'] if not line.strip()),
            "_lines_short_char_20_count": sum(1 for length in precomputed['line_lengths'] if length < 20),
            "_lines_long_char_10000_count": sum(1 for length in precomputed['line_lengths'] if length > 10000),
            "_lines_end_punctuation_count": lines_end_punctuation,
            "_lines_bulletpoint_start_count": sum(1 for line in precomputed['lines'] if self.bullet_start_pattern.search(line)),
            "_lines_end_ellipsis_count": sum(1 for line in precomputed['lines'] if line.strip().endswith(('...', '…'))),
            
            # Statistiche parole
            "_word_count": len(precomputed['words']),
            "_token_count": len(precomputed['words']) * FERTILITY_MAP.get(lang, FERTILITY_MAP['un']),  # Stima token basata sulla fertilità
            "_word_distinct_count": len(precomputed['word_counter']),
            "_no_alpha_count": sum(1 for word in precomputed['words'] if not any(c.isalpha() for c in word)),
            "_javascript_count": len(self.javascript_pattern.findall(text)),
            
            # Statistiche caratteri
            "_char_count": len(text),
            "_char_distinct_count": len(precomputed['char_counter']),
            "_char_uppercase_count": sum(1 for char in text if char.isupper()),
            "_char_symbol_count": len(self.symbol_pattern.findall(text)),
            "_char_punctuation_count": len(self.punc_regex.findall(text)),
            "_char_escape_count": len(self.escape_pattern.findall(text)),
            "_numerical_char_count": sum(1 for char in text if char.isdigit()),
        }
        
        # Calcolo delle ratio (con controllo divisione per zero)
        word_count = stats["_word_count"]
        char_count = stats["_char_count"]
        lines_count = stats["_lines_count"]
        
        # Ratio parole
        stats["_stop_word_ratio"] = self._compute_stopword_ratio(precomputed['word_counter'], lang, word_count)
        stats["_unique_word_ratio"] = stats["_word_distinct_count"] / word_count if word_count > 0 else 0
        stats["_symbol_word_ratio"] = stats["_char_symbol_count"] / word_count if word_count > 0 else 0
        
        # Entropia unigram
        stats["_unigram_entropy"] = self._compute_unigram_entropy(precomputed['word_counter'], word_count)
        
        # Lunghezza media parole
        stats["_mean_word_length"] = sum(len(word) for word in precomputed['words']) / word_count if word_count > 0 else 0
        
        # Ratio righe
        stats["_lines_short_char_20_ratio"] = stats["_lines_short_char_20_count"] / lines_count if lines_count > 0 else 0
        stats["_lines_long_char_10000_ratio"] = stats["_lines_long_char_10000_count"] / lines_count if lines_count > 0 else 0
        
        # Ratio caratteri
        stats["_char_uppercase_ratio"] = stats["_char_uppercase_count"] / char_count if char_count > 0 else 0
        stats["_numerical_char_ratio"] = stats["_numerical_char_count"] / char_count if char_count > 0 else 0
        stats["_char_symbol_ratio"] = stats["_char_symbol_count"] / char_count if char_count > 0 else 0
        stats["_char_punctuation_ratio"] = stats["_char_punctuation_count"] / char_count if char_count > 0 else 0
        stats["_char_escape_ratio"] = stats["_char_escape_count"] / char_count if char_count > 0 else 0
        
        return stats

    def _compute_stopword_ratio(self, word_counter: Counter, lang: str, total_words: int) -> float:
        """Calcola il rapporto stopwords/parole totali."""
        if total_words == 0:
            return 0.0
        
        stopwords = self._load_stopwords(lang)
        stopwords_count = sum(count for word, count in word_counter.items() if word.lower() in stopwords)
        return stopwords_count / total_words

    def _compute_unigram_entropy(self, word_counter: Counter, total_words: int) -> float:
        """Calcola l'entropia unigram per misurare la diversità del contenuto."""
        if total_words == 0:
            return 0.0
        
        entropy = 0.0
        for count in word_counter.values():
            probability = count / total_words
            entropy -= probability * math.log(probability)
        
        return entropy

    def _get_empty_stats(self) -> dict[str, int | float]:
        """Restituisce statistiche vuote per testo nullo."""
        return {
            "_length": 0,
            "_white_space_ratio": 0,
            "_non_alpha_digit_ratio": 0,
            "_digit_ratio": 0,
            "_uppercase_ratio": 0,
            "_elipsis_ratio": 0,
            "_punctuation_ratio": 0,
            "_lines_count": 0,
            "_lines_empty_count": 0,
            "_lines_short_char_20_count": 0,
            "_lines_long_char_10000_count": 0,
            "_lines_end_punctuation_count": 0,
            "_lines_bulletpoint_start_count": 0,
            "_lines_end_ellipsis_count": 0,
            "_word_count": 0,
            "_token_count": 0,
            "_word_distinct_count": 0,
            "_no_alpha_count": 0,
            "_javascript_count": 0,
            "_char_count": 0,
            "_char_distinct_count": 0,
            "_char_uppercase_count": 0,
            "_char_symbol_count": 0,
            "_char_punctuation_count": 0,
            "_char_escape_count": 0,
            "_numerical_char_count": 0,
            "_stop_word_ratio": 0,
            "_unique_word_ratio": 0,
            "_symbol_word_ratio": 0,
            "_unigram_entropy": 0,
            "_mean_word_length": 0,
            "_lines_short_char_20_ratio": 0,
            "_lines_long_char_10000_ratio": 0,
            "_char_uppercase_ratio": 0,
            "_numerical_char_ratio": 0,
            "_char_symbol_ratio": 0,
            "_char_punctuation_ratio": 0,
            "_char_escape_ratio": 0,
        }

    def __call__(self, data: DocumentsPipeline = None, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        """
            Shorthand way of calling the `run` method.
            block = Block()
            for resultdoc in block():
                ...
        Args:
            data:
            rank:
            world_size:

        Returns:

        """
        return self.run(data, rank, world_size)

