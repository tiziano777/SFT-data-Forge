# /datatrove_pipelines/reader/extensions/parquet_reader.py
from datatrove.pipeline.readers.parquet import ParquetReader
import hashlib
# ============================================================================
# PARQUET Reader: SOVRASCRIVERE LA CLASSE READER
# ============================================================================

class CustomParquetReader(ParquetReader):
    """
    Reader personalizzato per Parquet che calcola automaticamente ID per dati senza ID.
    Sovrascrive il metodo _default_adapter invece di passare un adapter esterno.
    """

    def _default_adapter(self, data: dict, path: str, id_in_file: int | str) -> dict:
        """
        Sovrascrive l'adapter di default per gestire dati senza ID.
        Questa è la funzione che viene chiamata internamente da DataTrove.
        """
        
        core_metadata = ['_filename', '_subpath', '_dataset_name', '_dataset_path', '_lang', '_id_hash']
        # Calcola il testo
        text=""
        data_content = dict()
        for key, value in data.items():
            if key not in core_metadata:
                text += str(value) + " "
                data_content[key] = value

        # Calcola l'ID
        doc_id = data.get('_id_hash', hashlib.sha256((text.encode('utf-8'))).hexdigest())

        return {
            "id": doc_id,
            "text": text,
            "metadata":{
                **{k: v for k, v in data.items() if k in core_metadata},
                **data_content,
            },
        }

    def __iter__(self):
        """Iteratore che delega al reader sottostante"""
        # Chiama il reader come funzione invece di iterare direttamente
        return iter(self.reader())

    # Override per gestire meglio i metadati del percorso
    def get_document_from_dict(self, data: dict, source_file: str, id_in_file: int):
        document = super().get_document_from_dict(data, source_file, id_in_file)
        
        # Aggiungi file_path se non presente
        if document and "file_path" not in document.metadata:
            document.metadata["file_path"] = self.data_folder.resolve_paths(source_file)
        
        return document



'''
# ============================================================================
# CONFIGURAZIONE BASE DATA FOLDER TEST for dataset e distribution
# ============================================================================

distribution_path = "<PROJECT_ROOT>/nfs/data-download/velvet_v1/glaive_dataset/data"
distribution_subpath = "data"
distribution_folder = DataFolder(
    path=distribution_path,
    auto_mkdir=True,
)

dataset_path = "<PROJECT_ROOT>/nfs/data-download/velvet_v1/glaive_dataset/"
dataset_folder = DataFolder(
    path=dataset_path,
    auto_mkdir=True,
)
# ============================================================================
# READER CON CLASSE PERSONALIZZATA
# ============================================================================

# Configurazione simile al JSONL reader
lang_list = ['en', 'it', 'fr', 'de', 'es']
distributions = ['data', 'data2/subdata2']  # Lista di subfolder delle distribuzioni
dataset_name = "glaive"

# Reader per dataset completo
custom_parquet_dataset_reader = CustomParquetReader(
    data_folder=dataset_folder,
    recursive=True,
    glob_pattern="**/*.parquet",
    text_key=None,  # L'adapter gestirà il testo
    id_key=None,    # L'adapter gestirà l'ID
    limit=-1,
    skip=0,
    file_progress=True,
    default_metadata={
        "_dataset_name": dataset_name,
        "_dataset_path": dataset_folder.path,
        "distributions": distributions,
        "_available_languages": lang_list,
    },
    shuffle_files=False,
)

# Reader per distribuzione specifica
custom_parquet_distribution_reader = CustomParquetReader(
    data_folder=distribution_folder,
    recursive=True,
    glob_pattern="*.parquet",
    text_key=None,  # L'adapter gestirà il testo
    id_key=None,    # L'adapter gestirà l'ID
    limit=-1,
    skip=0,
    file_progress=True,
    default_metadata={
        "_dataset_name": dataset_name,
        "_dataset_path": dataset_path,
        "distributions": [distribution_subpath],
        "_available_languages": lang_list,
    },
    shuffle_files=False,
)

# ============================================================================
# TEST CONFIGURAZIONE
# ============================================================================

def test_reader_configuration():
    """Test rapido per verificare che il reader funzioni"""
    print("🧪 Test configurazione Parquet reader personalizzato...")
    
    # Lista file trovati
    files = dataset_folder.list_files(recursive=True, glob_pattern="**/*.parquet")
    print(f"📁 File Parquet trovati: {len(files)}")
    
    # Prova a leggere primi documenti con la versione custom
    try:
        reader = custom_parquet_dataset_reader
        sample_docs = []
        for i, doc in enumerate(reader()):
            sample_docs.append(doc)
            if i >= 2:  # Leggi solo 3 documenti per il test
                break
        
        print(f"✅ Lettura Parquet OK - Documenti esempio: {len(sample_docs)}")
        
        for i, doc in enumerate(sample_docs):
            print(f"\n--- Documento Parquet {i+1} ---")
            print(f"   ID: {doc.id}")
            print(f"   Text preview: {doc.text[:100]}..." if doc.text else "   Text: [VUOTO]")
            print(f"   Metadata: {doc.metadata}")
            
    except Exception as e:
        print(f"❌ Errore: {e}")
        traceback.print_exc()

# Esegui test
test_reader_configuration()

print("✅ File Parquet reader completato!")
'''