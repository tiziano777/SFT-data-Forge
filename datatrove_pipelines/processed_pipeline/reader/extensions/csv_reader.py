# /datatrove_pipelines/reader/extensions/csv_reader.py

from typing import Callable, Literal
import csv
from datatrove.io import DataFileLike, DataFolderLike
from datatrove.pipeline.readers.csv import CsvReader

# ============================================================================
# CSV Reader: SOVRASCRIVERE LA CLASSE READER
# ============================================================================

class CustomCsvReader(CsvReader):
    """
    Reader personalizzato per CSV che calcola automaticamente ID per dati senza ID.
    Sovrascrive il metodo _default_adapter invece di passare un adapter esterno.
    """

    def __init__(
        self,
        data_folder: DataFolderLike,
        paths_file: DataFileLike | None = None,
        compression: Literal["infer", "gzip", "zstd"] | None = "infer",
        limit: int = -1,
        skip: int = 0,
        file_progress: bool = False,
        doc_progress: bool = False,
        adapter: Callable = None,
        text_key: str = "text",
        id_key: str = "id",
        default_metadata: dict = None,
        recursive: bool = True,
        glob_pattern: str | None = None,
        shuffle_files: bool = False,
        delimiter: str | None = None,
    ):
        # Chiama il costruttore padre correttamente
        super().__init__(
            data_folder=data_folder,
            paths_file=paths_file,
            limit=limit,
            skip=skip,
            file_progress=file_progress,
            doc_progress=doc_progress,
            adapter=adapter,
            text_key=text_key,
            id_key=id_key,
            default_metadata=default_metadata,
            recursive=recursive,
            glob_pattern=glob_pattern,
            shuffle_files=shuffle_files,
        )
        self.compression = compression
        self.empty_warning = False
        self.delimiter = delimiter

    def read_file(self, filepath: str):
        """
        Override del metodo read_file per gestire encoding diversi.
        """
        encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        
        for encoding in encodings_to_try:
            try:
                with self.data_folder.open(filepath, "r", compression=self.compression, encoding=encoding) as f:
                    print(f"✅ File {filepath} aperto con encoding: {encoding}")
                    
                    # Usa il delimiter specificato o auto-rileva
                    csv_reader = csv.DictReader(f, delimiter=self.delimiter) if self.delimiter else csv.DictReader(f)
                    
                    for di, d in enumerate(csv_reader):
                        with self.track_time():
                            document = self.get_document_from_dict(d, filepath, di)
                            #print(f"Lettura riga {di} dal file {filepath}: {d}")
                            
                            # CORREZIONE: Controlla esplicitamente se il documento è valido
                            if document is None:
                                print(f"⚠️ Documento None per riga {di}, saltando...")
                                continue
                                
                            # CORREZIONE: Controlla se il documento ha testo
                            if not document.text or not document.text.strip():
                                print(f"⚠️ Documento senza testo per riga {di}, saltando...")
                                continue
                                
                            #print(f"✅ Yield documento {json.dumps(document.metadata, ensure_ascii=False)}")
                            yield document
                    
                    # Se arriviamo qui, l'encoding ha funzionato
                    break
                    
            except UnicodeDecodeError as e:
                print(f"❌ Encoding {encoding} fallito per {filepath}: {e}")
                continue
            except Exception as e:
                print(f"❌ Errore generico con encoding {encoding} per {filepath}: {e}")
                continue
        else:
            print(f"❌ Tutti gli encoding hanno fallito per il file {filepath}")
            
    # Override dell'adapter di default per calcolare text e id dopo la lettura del sample
    def _default_adapter(self, data: dict, path: str, id_in_file: int | str) -> dict:
        """
        Sovrascrive l'adapter di default per gestire dati senza ID.
        Questa è la funzione che viene chiamata internamente da DataTrove.
        """
        import sys
        sys.path.append('..')
        from ..stats_functions.core_metadata import default_adapter_aux
        return default_adapter_aux(data, path, id_in_file, self.default_metadata)

    def get_document_from_dict(self, data: dict, source_file: str, id_in_file: int):
        document = super().get_document_from_dict(data, source_file, id_in_file)
        
        if document:
            #print(f"📄 Documento creato: ID={document.id}, testo_len={len(document.text)}")
            document.metadata.setdefault("file_path", self.data_folder.resolve_paths(source_file))
            
            # Calcola il subpath in modo più robusto
            resolved_path = self.data_folder.resolve_paths(source_file)
            path_parts = resolved_path.split('/')
            
            # CORREZIONE: Gestione sicura dei default_metadata
            dataset_path = self.default_metadata.get("_dataset_path", "") if isinstance(self.default_metadata, dict) else ""
            if dataset_path:
                dataset_parts = dataset_path.rstrip('/').split('/')
                if path_parts[:len(dataset_parts)] == dataset_parts:
                    subpath_parts = path_parts[len(dataset_parts):-1]
                    document.metadata["_subpath"] = '/'.join(subpath_parts) if subpath_parts else ""
        else:
            print(f"❌ Documento None per file {source_file}, riga {id_in_file}")
        
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
# TEST READER CON CLASSE PERSONALIZZATA
# ============================================================================

# Configurazione come nel JSONL reader
lang_list = ['en', 'it', 'fr', 'de', 'es']
distributions = ['data', 'data2/subdata2']  # Lista di subfolder delle distribuzioni
dataset_name = "glaive"

# Reader per dataset completo
custom_csv_dataset_reader = CustomCsvReader(
    data_folder=dataset_folder,
    recursive=True,
    glob_pattern="**/*.csv", # posso gestire anche TSV?
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
custom_csv_distribution_reader = CustomCsvReader(
    data_folder=distribution_folder,
    recursive=True,
    glob_pattern="*.csv",
    text_key=None,  # L'adapter gestirà il testo
    id_key=None,    # L'adapter gestirà l'ID
    limit=-1,
    skip=0,
    file_progress=True,
    default_metadata={
        "_dataset_name": dataset_name,
        "_dataset_path": dataset_path,
        "distributions": distribution_subpath,
        "_available_languages": lang_list,
    },
    shuffle_files=False,
)

# ============================================================================
# TEST CONFIGURAZIONE
# ============================================================================

def test_reader_configuration():
    """Test rapido per verificare che il reader funzioni"""
    print("🧪 Test configurazione CSV reader personalizzato...")
    
    # Lista file trovati
    files = dataset_folder.list_files(recursive=True, glob_pattern="**/*.csv")
    print(f"📁 File CSV trovati: {len(files)}")
    
    # Prova a leggere primi documenti con la versione custom
    try:
        reader = custom_csv_dataset_reader
        sample_docs = []
        for i, doc in enumerate(reader()):
            sample_docs.append(doc)
            if i >= 2:  # Leggi solo 3 documenti per il test
                break
        
        
       
        
        print(f"✅ Lettura CSV OK - Documenti esempio: {len(sample_docs)}")
        import json
        for i, doc in enumerate(sample_docs):
            print(f"\n--- Documento CSV {i+1} ---")
            print(f"ID: {doc.id}")
            print(json.dumps(doc.metadata, ensure_ascii=False, indent=2))
        
    except Exception as e:
        print(f"❌ Errore: {e}")
        traceback.print_exc()

# Esegui test
test_reader_configuration()

print("✅ File CSV reader completato!")
'''