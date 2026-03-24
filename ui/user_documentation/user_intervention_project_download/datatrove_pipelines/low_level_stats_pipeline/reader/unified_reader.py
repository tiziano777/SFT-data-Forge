import sys
sys.path.append("../")

from datatrove.io import DataFolder

from .extensions.jsonl_reader import CustomJsonlReader
from .extensions.parquet_reader import CustomParquetReader


class UnifiedReader:
    """
    Reader unificato che supporta multiple formati di file basandosi sul glob pattern.
    Supporta: JSONL, Parquet, IPC/Arrow, CSV.
    """
    
    # Mappatura estensioni -> classi reader
    READER_MAPPING = {
        'jsonl': CustomJsonlReader,
        'parquet': CustomParquetReader,
        }
    
    # Formati che supportano compressione
    COMPRESSION_SUPPORTED = {'jsonl'}
    
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
            print(f"⚠️  Attenzione: compressione non supportata per formato {self.reader_type}. Ignorato.")
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
            else:
                raise ValueError(f"Pipeline di mapping: Impossibile determinare il tipo di reader dal glob pattern: {self.glob_pattern}")
    
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
        
        # Aggiungi compressione solo per i formati che la supportano
        if self.compression and self.reader_type in self.COMPRESSION_SUPPORTED:
            reader_kwargs['compression'] = self.compression
            print(f"📦 Compressione abilitata: {self.compression}")
        
        #print(f"📖 Creando reader {self.reader_type} con pattern: {self.glob_pattern}")
        
        return reader_class(**reader_kwargs)

    # METODI PER IMPLEMENTARE L'INTERFACCIA PipelineStep #
    
    def __iter__(self):
        """Iteratore che delega al reader sottostante"""
        return iter(self.reader)

    def __call__(self, data=None, rank: int = 0, world_size: int = 1):
        """Chiamabile come funzione"""
        return self.reader(data=data, rank=rank, world_size=world_size)
    
    def run(self, data=None, rank: int = 0, world_size: int = 1):
        """
        Implementa il metodo run richiesto dall'interfaccia PipelineStep.
        Questo permette a UnifiedReader di essere usato nelle pipeline DataTrove.
        """
        #print(f"🔧 UnifiedReader.run chiamato - rank: {rank}, world_size: {world_size}")
        
        # Se c'è data in input, yield prima quella (per pipeline con step multipli)
        
        '''
        if data:
            yield from data
        '''
        # Poi yield i documenti dal reader sottostante
        for document in self.reader.run(rank=rank, world_size=world_size):
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
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    def get_reader_info(self):
        """Restituisce informazioni sul reader creato"""
        return {
            'reader_type': self.reader_type,
            'compression_supported': self.reader_type in self.COMPRESSION_SUPPORTED,
            'compression': self.compression if self.reader_type in self.COMPRESSION_SUPPORTED else None,
            'glob_pattern': self.glob_pattern,
            'formats_supported': list(self.READER_MAPPING.keys())
        }


# ============================================================================
# TEST E UTILIZZO
# ============================================================================

'''def test_unified_reader():
    """Test completo del reader unificato con logging dettagliato"""
    from datatrove.io import DataFolder

    # Configurazione di base
    dataset_path = "<PROJECT_ROOT>/nfs/processed-data/velvet_v1/allenai/"
    distributions = ['ARC-Challenge/en', 'ARC-Easy/en']
    dataset_name = "allenai/ai2_arc"
    
    default_metadata = {
        "_dataset_name": dataset_name,
        "_dataset_path": dataset_path,
        "distributions": distributions
    }
    
    print("🚀 TEST UNIFIED READER - AVVIO")
    print("=" * 60)
    
    # Test 1: JSONL automatico (rilevamento formato)
    print("\n🧪 TEST 1: JSONL - Rilevamento Automatico Formato")
    print("-" * 50)
    
    try:
        print("📥 CONFIGURAZIONE:")
        print(f"   • Data folder: {dataset_path}")
        print(f"   • Glob pattern: None (automatico)")
        print(f"   • Recursive: True")
        print(f"   • Text key: None")
        print(f"   • ID key: None")
        
        data_folder = DataFolder(dataset_path)

        reader1 = UnifiedReader(
            data_folder=data_folder,
            recursive=True,
            file_progress=False,
            default_metadata=default_metadata,
            glob_pattern="**/*.jsonl.gz",
            #compression="gzip", # funziona anche senza, se metti glob coretto **/*.jsonl.gz
        )
        
        # Informazioni sul reader creato
        reader_info = reader1.get_reader_info()
        print(f"\n🔍 READER CREATO:")
        print(f"   • Tipo: {reader_info['reader_type'].upper()}")
        print(f"   • Pattern usato: {reader_info['glob_pattern']}")
        print(f"   • Supporta compressione: {reader_info['compression_supported']}")
        
        # Lettura documenti
        print(f"\n📖 LETTURA DOCUMENTI:")
        docs = []
        for i, doc in enumerate(reader1()):
            docs.append(doc)
            if i >= 2:  # Leggi 3 documenti per il test
                break
        
        print(f"   • Documenti letti: {len(docs)}")
        for i, doc in enumerate(docs):
            print(doc)
        
        print("✅ TEST 1 PASSATO - JSONL automatico funzionante")
        
    except Exception as e:
        print(f"❌ TEST 1 FALLITO: {e}")
        import traceback
        traceback.print_exc()
    
    

    # Test 4: Configurazione avanzata con chiavi specifiche
    print("\n🧪 TEST 4: CONFIGURAZIONE AVANZATA - Chiavi Specifiche")
    print("-" * 50)
    
    try:
        print("📥 CONFIGURAZIONE:")
        print(f"   • Data folder: {dataset_path}")
        print(f"   • Glob pattern: **/*.jsonl")
        print(f"   • Text key: text")
        print(f"   • ID key: id")
        print(f"   • Limit: 5")
        print(f"   • Skip: 1")
        
        reader4 = UnifiedReader(
            data_folder=dataset_path,
            glob_pattern="**/*.jsonl",
            recursive=True,
            text_key='text',
            id_key='id',
            limit=5,
            skip=1,
            file_progress=False,
            default_metadata=default_metadata,
        )
        
        # Informazioni sul reader creato
        reader_info = reader4.get_reader_info()
        print(f"\n🔍 READER CREATO:")
        print(f"   • Tipo: {reader_info['reader_type'].upper()}")
        print(f"   • Pattern usato: {reader_info['glob_pattern']}")
        
        # Lettura documenti
        print(f"\n📖 LETTURA DOCUMENTI (limit=5, skip=1):")
        docs = list(reader4())
        
        print(f"   • Documenti letti: {len(docs)}")
        print(f"   • Configurazione applicata:")
        print(f"     - Text key: {reader4.text_key}")
        print(f"     - ID key: {reader4.id_key}")
        print(f"     - Limit: {reader4.limit}")
        print(f"     - Skip: {reader4.skip}")
        
        for i, doc in enumerate(docs):
            print(doc)

        print("✅ TEST 4 PASSATO - Configurazione avanzata funzionante")

    except Exception as e:
        print(f"❌ TEST 4 FALLITO: {e}")
        import traceback
        traceback.print_exc()
    
    
if __name__ == "__main__":
    test_unified_reader()

'''