from datatrove.io import DataFolder
from .extensions.csv_reader import CustomCsvReader
from .extensions.ipc_reader import  CustomIpcReader
from .extensions.jsonl_reader import CustomJsonlReader
from .extensions.parquet_reader import CustomParquetReader
from .extensions.warc_reader import CustomWarcReader

import logging
logger = logging.getLogger(__name__)

class UnifiedReader:
    """
    Reader unificato che supporta multiple formati di file basandosi sul glob pattern.
    Supporta: JSONL, Parquet, IPC/Arrow, CSV.
    """
    
    # Mappatura estensioni -> classi reader
    READER_MAPPING = {
        'jsonl': CustomJsonlReader,
        'json': CustomJsonlReader,
        'parquet': CustomParquetReader,
        'arrow': CustomIpcReader,
        'ipc': CustomIpcReader,
        'csv': CustomCsvReader,
        'tsv': CustomCsvReader,  # TSV usa lo stesso reader CSV ma con delimiter='\t'
        'warc': CustomWarcReader,
    }
    
    # Formati che supportano compressione
    COMPRESSION_SUPPORTED = {'jsonl', 'csv', 'json', 'tsv'}
    
    def __init__(self, data_folder, glob_pattern, recursive=True, 
                 text_key=None, id_key=None, limit=-1, skip=0, 
                 file_progress=True, default_metadata=None, 
                 shuffle_files=False, compression=None):
        """
        Args:
            data_folder: DataFolder o percorso stringa
            glob_pattern: Pattern glob obbligatorio per selezionare i file
            recursive: Ricerca ricorsiva nelle sottocartelle
            text_key: Chiave per estrarre il testo (None per adapter personalizzato)
            id_key: Chiave per l'ID (None per adapter personalizzato)
            limit: Limite documenti (-1 per nessun limite)
            skip: Documenti da saltare
            file_progress: Mostra progresso file
            default_metadata: Metadati default
            shuffle_files: Mescola i file
            compression: Tipo compressione (solo per JSONL, CSV)
        """
        
        if not glob_pattern:
            raise ValueError("Il parametro glob_pattern è obbligatorio")
        
        self.data_folder = data_folder if isinstance(data_folder, DataFolder) else DataFolder(data_folder)
        self.glob_pattern = glob_pattern
        self.recursive = recursive
        self.text_key = text_key
        self.id_key = id_key
        self.limit = limit
        self.skip = skip
        self.file_progress = file_progress
        self.default_metadata = default_metadata or {}
        self.shuffle_files = shuffle_files
        self.compression = compression
        
        # Determina il tipo di reader basato sul glob pattern
        self.reader_type = self._detect_reader_type()
        
        # Verifica supporto compressione
        if compression and self.reader_type not in self.COMPRESSION_SUPPORTED:
            #logger.info(f"⚠️  Attenzione: compressione non supportata per formato {self.reader_type}. Ignorato.")
            self.compression = None
        
        self.reader = self._create_reader()
    
    def _detect_reader_type(self):
        """Determina il tipo di reader basato sul glob pattern"""
        # Estrai l'estensione dal glob pattern
        glob_lower = self.glob_pattern.lower()
        
        # Mappa estensioni a tipi di reader
        if any(ext in glob_lower for ext in ['.jsonl', '.json']):
            return 'jsonl'
        elif '.parquet' in glob_lower:
            return 'parquet'
        elif any(ext in glob_lower for ext in ['.arrow', '.ipc']):
            return 'ipc'
        elif '.csv' in glob_lower:
            return 'csv'
        elif '.tsv' in glob_lower:  # Aggiungi TSV
            return 'tsv'
        elif any(ext in glob_lower for ext in ['.warc', '.arc']):
            return 'warc'
        else:
            # Prova a determinare dai file trovati
            files = self.data_folder.list_files(recursive=self.recursive, glob_pattern=self.glob_pattern)
            if not files:
                raise ValueError(f"Nessun file trovato con il pattern: {self.glob_pattern}")
            
            # Analizza l'estensione del primo file trovato
            first_file = files[0].lower()
            if first_file.endswith(('.jsonl', '.json', '.json.gz')):
                return 'jsonl'
            elif first_file.endswith('.parquet'):
                return 'parquet'
            elif first_file.endswith(('.arrow', '.ipc')):
                return 'ipc'
            elif first_file.endswith('.csv'):
                return 'csv'
            elif '.tsv' in glob_lower:  # Aggiungi TSV
                return 'tsv'
            else:
                raise ValueError(f"Impossibile determinare il tipo di reader dal glob pattern: {self.glob_pattern}")
    
    def _create_reader(self):
        """Crea il reader appropriato basato sul tipo rilevato"""
        reader_class = self.READER_MAPPING.get(self.reader_type)
        if not reader_class:
            raise ValueError(f"Tipo di reader non supportato: {self.reader_type}")

        # Prepara i parametri base
        reader_kwargs = {
            'data_folder': self.data_folder,
            'recursive': self.recursive,
            'glob_pattern': self.glob_pattern,
            'text_key': self.text_key,
            'id_key': self.id_key,
            'limit': self.limit,
            'skip': self.skip,
            'file_progress': self.file_progress,
            'default_metadata': self.default_metadata,
            'shuffle_files': self.shuffle_files,
        }
        
        # AGGIUNGI QUESTA PARTE: Parametri specifici per formato
        if self.reader_type == 'tsv':
            reader_kwargs['delimiter'] = '\t'
            #slogger.info(f"🔧 TSV configurato con delimiter: \\t")
        elif self.reader_type == 'csv':
            # Opzionale: puoi specificare delimiter per CSV se necessario
            reader_kwargs['delimiter'] = ','  # Questo è il default, ma esplicito è meglio
            #logger.info(f"🔧 CSV configurato con delimiter: ,")
        
        # Aggiungi compressione solo per i formati che la supportano
        if self.compression and self.reader_type in self.COMPRESSION_SUPPORTED:
            reader_kwargs['compression'] = self.compression
            #logger.info(f"📦 Compressione abilitata: {self.compression}")
        
        #logger.info(f"📖 Creando reader {self.reader_type} con pattern: {self.glob_pattern}")
        
        return reader_class(**reader_kwargs)

    def __iter__(self):
        """Iteratore che delega al reader sottostante"""
        return iter(self.reader)

    def __call__(self, data=None, rank: int = 0, world_size: int = 1):
        """Chiamabile come funzione"""
        return self.reader(data=data, rank=rank, world_size=world_size)

    # AGGIUNGI QUESTI METODI PER IMPLEMENTARE L'INTERFACCIA PipelineStep
    def run(self, data=None, rank: int = 0, world_size: int = 1):
        """
        Implementa il metodo run richiesto dall'interfaccia PipelineStep.
        Questo permette a UnifiedReader di essere usato nelle pipeline DataTrove.
        """
        #logger.info(f"🔧 UnifiedReader.run chiamato - rank: {rank}, world_size: {world_size}")
            
        for document in self.reader.run(rank=rank, world_size=world_size):
            #logger.info(f"📄 Documento letto: ID={document.id}, Metadata={document.metadata}")
            yield document
    
    def _safe_limit_check(self, current_count):
        """
        Verifica sicura del limite, gestendo il caso in cui current_count sia None.
        """
        if current_count is None:
            return False
        if self.limit == -1:
            return False
        return current_count >= self.limit
    
    def __enter__(self):
        return self
    
    def get_reader_info(self):
        """Restituisce informazioni sul reader creato"""
        return {
            'reader_type': self.reader_type,
            'compression_supported': self.reader_type in self.COMPRESSION_SUPPORTED,
            'compression': self.compression if self.reader_type in self.COMPRESSION_SUPPORTED else None,
            'glob_pattern': self.glob_pattern,
            'formats_supported': list(self.READER_MAPPING.keys())
        }