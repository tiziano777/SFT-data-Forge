import re
import pandas as pd
import logging
from datatrove.data import Document, DocumentsPipeline
from datatrove.io import DataFolder
from datatrove.pipeline.stats.base import BaseStats
from datatrove.pipeline.stats.config import DEFAULT_TOP_K_CONFIG, GROUP, TopKConfig

logger = logging.getLogger(__name__)

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

class ChatTemplateStats(BaseStats):
    """
    Chat template statistics extractor for conversation data.
    Genera statistiche tabulari per PostgreSQL basate su template JSON di chat.
    """

    name = "💬 Chat Template Stats"

    def __init__(
        self,
        output_folder: DataFolder,
        groups_to_compute: list[GROUP] = None,
        histogram_round_digits: int = 3,
        top_k_config: TopKConfig = DEFAULT_TOP_K_CONFIG,
    ) -> None:
        super().__init__(output_folder, groups_to_compute or [], histogram_round_digits, top_k_config)
        
        # Pattern di ricerca ottimizzati
        self.json_pattern = re.compile(r'\{.*\}|\[.*\]', re.DOTALL)
        self.code_pattern = re.compile(r'```[\s\S]*?```|`[^`]+`')
        self.markdown_pattern = re.compile(r'\*\*.*?\*\*|__.*?__|~~.*?~~|###|##|#')
        self.table_pattern = re.compile(r'\|.*\|.*\|')
        self.math_pattern = re.compile(r'\$\$[\s\S]*?\$\$|\$[^$]*?\$|\\\[|\\\(|\\\)|\\\]')
        self.html_tag_pattern = re.compile(r'<[^>]+>')
        self.xml_tag_pattern = re.compile(r'<[^>\s]+[^>]*>|</[^>]+>')
        self.curly_bracket_pattern = re.compile(r'[{}]')
        self.question_pattern = re.compile(r'\?\s*$')
        self.word_pattern = re.compile(r'\b\w+\b', re.UNICODE)  # Pattern per conteggio parole

    def run(self, data, rank: int = 0, world_size: int = 1):
        tabular_stats = []

        
        for doc in data:
            with self.track_time():
                try:
                    chat_stats = self.extract_stats(doc)
                    chat_stats['id'] = doc.id
                    tabular_stats.append(chat_stats)
                except Exception as e:
                    logger.error(f"Error while extracting stats from document {doc.id}", exc_info=e)
                    continue

            yield doc

        if tabular_stats:
            df = pd.DataFrame(tabular_stats)
            filename = f"template_stats_{rank:05d}.parquet"
            
            # Assicuriamoci che la directory esista tramite il file system di Datatrove
            self.output_folder.fs.makedirs(self.output_folder.path, exist_ok=True)
            
            # Apriamo il file usando il metodo open del DataFolder
            with self.output_folder.open(filename, "wb") as f:
                df.to_parquet(f, index=False)
            
            print(f"✅ Generated {len(tabular_stats)} chat template stats records: {filename} in {self.output_folder.path}")
    
    def extract_stats(self, doc: Document) -> dict:
        """
        Estrae statistiche dal documento analizzando i metadati mappati, 
        quelli originali o il testo JSON.
        """
        chat_data = None
        lang = doc.metadata.get('_lang', 'en') if doc.metadata else 'en'   
        
        # 1. PRIORITÀ: Dati processati dal MapperExtractor (dentro metadata['data'])
        if doc.metadata and 'data' in doc.metadata and isinstance(doc.metadata['data'], dict):
            if 'messages' in doc.metadata['data']:
                chat_data = doc.metadata['data']
                # logger.debug(f"Stats extracted from metadata['data'] for doc {doc.id}")
        
        # 2. SECONDARIA: Dati nel livello radice dei metadati (originali)
        if not chat_data and doc.metadata and 'messages' in doc.metadata:
            chat_data = doc.metadata
            # logger.debug(f"Stats extracted from root metadata for doc {doc.id}")
            
        # 3. FALLBACK: Caricamento da doc.text se è una stringa JSON
        if not chat_data and doc.text:
            import json
            try:
                # Tentativo di parsing se il testo sembra un oggetto JSON
                if doc.text.strip().startswith(('{', '[')):
                    chat_data = json.loads(doc.text)
            except Exception:
                pass

        # Se non troviamo messaggi dopo tutti i tentativi, restituiamo valori zero
        messages = chat_data.get('messages', []) if chat_data else []
        if not messages:
            return self._get_empty_stats()

        stats_accumulator = {
            "user_char_count": 0, "assistant_char_count": 0, "system_char_count": 0,
            "user_word_count": 0, "assistant_word_count": 0, "system_word_count": 0,  # Word counts
            "user_message_count": 0, "assistant_message_count": 0,
            "context_count": 0, "think_message_count": 0, "functioncall_message_count": 0,
            "think_char_total": 0, "think_steps_total": 0,
            "user_code_count": 0, "curly_brackets_count": 0,
            "html_tag_count": 0, "xml_tag_count": 0,
            "has_system": 0, "has_context": 0, "has_think": 0, "has_functioncall": 0,
            "user_ends_with_question": 0,
            "assistant_has_json": 0, "assistant_has_code": 0, "assistant_has_markdown": 0,
            "assistant_has_table": 0, "assistant_has_math": 0
        }
        
        last_user_len = 0
        last_assistant_len = 0
        user_word_lengths = []
        assistant_word_lengths = []
        user_message_lengths = []
        assistant_message_lengths = []

        for msg in messages:
            role = str(msg.get('role', '')).upper()
            content = msg.get('content') or ""
            content_len = len(content)
            
            # Conteggio parole nel contenuto
            word_count = len(self.word_pattern.findall(content))
            
            # 1. Processamento THOUGHTS
            think_content = msg.get('think')
            if think_content:
                stats_accumulator["has_think"] = 1
                stats_accumulator["think_message_count"] += 1
                stats_accumulator["think_char_total"] += len(think_content)
                stats_accumulator["think_steps_total"] += len(think_content.splitlines())

            # 2. Processamento FUNCTION CALL
            fc = msg.get('functioncall')
            if fc and isinstance(fc, dict) and (fc.get('payload') or fc.get('call')):
                stats_accumulator["has_functioncall"] = 1
                stats_accumulator["functioncall_message_count"] += 1

            # 3. Processamento CONTEXT
            if msg.get('context'):
                stats_accumulator["has_context"] = 1
                stats_accumulator["context_count"] += 1

            # 4. Analisi contenuto testuale
            stats_accumulator["curly_brackets_count"] += len(self.curly_bracket_pattern.findall(content))

            if role == 'USER':
                stats_accumulator["user_message_count"] += 1
                stats_accumulator["user_char_count"] += content_len
                stats_accumulator["user_word_count"] += word_count
                last_user_len = content_len
                user_message_lengths.append(content_len)
                user_word_lengths.append(word_count)
                if self.question_pattern.search(content):
                    stats_accumulator["user_ends_with_question"] = 1
                stats_accumulator["user_code_count"] += len(self.code_pattern.findall(content))
                    
            elif role == 'ASSISTANT':
                stats_accumulator["assistant_message_count"] += 1
                stats_accumulator["assistant_char_count"] += content_len
                stats_accumulator["assistant_word_count"] += word_count
                last_assistant_len = content_len
                assistant_message_lengths.append(content_len)
                assistant_word_lengths.append(word_count)
                
                if not stats_accumulator["assistant_has_json"] and self.json_pattern.search(content):
                    stats_accumulator["assistant_has_json"] = 1
                if not stats_accumulator["assistant_has_code"] and self.code_pattern.search(content):
                    stats_accumulator["assistant_has_code"] = 1
                if not stats_accumulator["assistant_has_markdown"] and self.markdown_pattern.search(content):
                    stats_accumulator["assistant_has_markdown"] = 1
                if not stats_accumulator["assistant_has_table"] and self.table_pattern.search(content):
                    stats_accumulator["assistant_has_table"] = 1
                if not stats_accumulator["assistant_has_math"] and self.math_pattern.search(content):
                    stats_accumulator["assistant_has_math"] = 1
                
                stats_accumulator["html_tag_count"] += len(self.html_tag_pattern.findall(content))
                stats_accumulator["xml_tag_count"] += len(self.xml_tag_pattern.findall(content))
                    
            elif role == 'SYSTEM':
                stats_accumulator["has_system"] = 1
                stats_accumulator["system_char_count"] += content_len
                stats_accumulator["system_word_count"] += word_count

        # Calcolo Ratio e Medie Finali
        u_count = stats_accumulator["user_message_count"]
        a_count = stats_accumulator["assistant_message_count"]
        u_chars = stats_accumulator["user_char_count"]
        a_chars = stats_accumulator["assistant_char_count"]
        u_words = stats_accumulator["user_word_count"]
        a_words = stats_accumulator["assistant_word_count"]
        s_words = stats_accumulator["system_word_count"]

        final_stats = {f"_{k}": v for k, v in stats_accumulator.items()}
        
        # Calcolo token counts usando fertility map
        fertility = FERTILITY_MAP.get(lang, FERTILITY_MAP['un'])
        
        final_stats.update({
            "_user_avg_length": u_chars / u_count if u_count > 0 else 0,
            "_assistant_avg_length": a_chars / a_count if a_count > 0 else 0,
            "_user_assistant_ratio": u_chars / a_chars if a_chars > 0 else 0,
            "_last_interaction_ratio": last_user_len / last_assistant_len if last_assistant_len > 0 else 0,
            "_message_count": len(messages),
            "_think_to_content_ratio": (stats_accumulator["think_char_total"] / a_chars) if a_chars > 0 else 0,
            
            # Nuove metriche per word counts
            "_user_word_mean": sum(user_word_lengths) / len(user_word_lengths) if user_word_lengths else 0,
            "_assistant_word_mean": sum(assistant_word_lengths) / len(assistant_word_lengths) if assistant_word_lengths else 0,
            
            # Calcolo token counts
            "_system_token_count": int(stats_accumulator["system_word_count"] * fertility),
            "_user_token_count": int(stats_accumulator["user_word_count"] * fertility),
            "_assistant_token_count": int(stats_accumulator["assistant_word_count"] * fertility),
            "_context_token_count": int(stats_accumulator["context_count"] * fertility),  # Context stimato come token
        })

        return final_stats
    
    def _get_empty_stats(self) -> dict:
        # Mantengo la tua struttura originale per coerenza con il DB
        cols = [
            "user_char_count", "assistant_char_count", "system_char_count", "user_avg_length",
            "assistant_avg_length", "user_assistant_ratio", "last_interaction_ratio",
            "user_ends_with_question", "has_system", "has_context", "has_think",
            "has_functioncall", "message_count", "context_count", "think_message_count",
            "functioncall_message_count", "think_steps_count", "think_char_count",
            "think_to_content_ratio", "assistant_has_json", "assistant_has_code",
            "assistant_has_markdown", "assistant_has_table", "assistant_has_math",
            "html_tag_count", "xml_tag_count", "curly_brackets_count", "user_code_count",
            "user_word_count", "assistant_word_count", "system_word_count", "context_count",
            "user_word_mean", "assistant_word_mean",
            "system_token_count", "user_token_count", "assistant_token_count", "context_token_count"
        ]
        return {f"_{c}": 0 for c in cols}

    def __call__(self, data: DocumentsPipeline = None, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        return self.run(data, rank, world_size)
