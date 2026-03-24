# /datatrove_pipelines/reader/extensions/jsonl_reader.py
import traceback
import json
from datatrove.pipeline.readers.jsonl import JsonlReader
import hashlib

# ============================================================================
# JSONL Reader: SOVRASCRIVERE LA CLASSE READER
# ============================================================================

class CustomJsonlReader(JsonlReader):
    """
    Reader personalizzato che calcola automaticamente ID per dati senza ID.
    Sovrascrive il metodo _default_adapter invece di passare un adapter esterno.
    Gestisce sia JSONL che JSON array.
    """
    
    def read_file(self, filepath: str):
        """
        Override del metodo read_file per gestire sia JSONL che JSON array.
        """
        try:
            with self.data_folder.open(filepath, "r", compression=self.compression) as f:
                print(f"📖 Leggendo file: {filepath}")
                
                # Leggi tutto il contenuto per determinare il formato
                content = f.read().strip()
                
                if not content:
                    print(f"⚠️ File vuoto: {filepath}")
                    return
                
                # Determina se è JSON array o JSONL
                if content.startswith('[') and content.endswith(']'):
                    # JSON array format
                    print(f"🔍 Rilevato JSON array in {filepath}")
                    try:
                        data_array = json.loads(content)
                        if not isinstance(data_array, list):
                            print(f"❌ Il file {filepath} non contiene un array JSON valido")
                            return
                        
                        #print(f"📊 Trovati {len(data_array)} documenti nel JSON array")
                        
                        for index, item in enumerate(data_array):
                            with self.track_time():
                                if not isinstance(item, dict):
                                    print(f"⚠️ Elemento {index} non è un dizionario, saltando...")
                                    continue
                                    
                                document = self.get_document_from_dict(item, filepath, index)
                                #print(f"Lettura elemento {index} dal file {filepath}")
                                if document:
                                    yield document
                                else:
                                    print(f"⚠️ Documento None per indice {index}, saltando...")
                    
                    except json.JSONDecodeError as e:
                        print(f"❌ Errore nel parsing JSON array {filepath}: {e}")
                        return
                        
                else:
                    # JSONL format (default)
                    print(f"🔍 Rilevato JSONL in {filepath}")
                    for line_num, line in enumerate(content.split('\n')):
                        line = line.strip()
                        if not line:
                            continue
                            
                        with self.track_time():
                            try:
                                data = json.loads(line)
                                document = self.get_document_from_dict(data, filepath, line_num)
                                
                                #print(f"Lettura riga {line_num} dal file {filepath}: {data}")
                                if document:
                                    yield document
                            except json.JSONDecodeError as e:
                                print(f"❌ Errore JSON alla riga {line_num} in {filepath}: {e}")
                                continue
                                
        except Exception as e:
            print(f"❌ Errore nella lettura del file {filepath}: {e}")
            traceback.print_exc()
    

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
        output_doc={
            "id": doc_id,
            "text": text,
            "metadata":{
                **{k: v for k, v in data.items() if k in core_metadata},
                **data_content,
            },
        }
        #print(f"🆔 Generato documento ID {doc_id} ")
        return output_doc

    # Override per aggiungere _subpath nei metadata
    def get_document_from_dict(self, data: dict, source_file: str, id_in_file: int):
        document = super().get_document_from_dict(data, source_file, id_in_file)
        
        if document:
            document.metadata.setdefault("file_path", self.data_folder.resolve_paths(source_file))
        
        return document
    
    def __iter__(self):
        """Iteratore che delega al reader sottostante"""
        # Chiama il reader come funzione invece di iterare direttamente
        return iter(self.reader())


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

# DA APP dovrai selezionare il dataset folder da nav
# da li dovremmo leggere ogni distribution subforlder
lang_list = ['en', 'it', 'fr', 'de', 'es']
distributions = ['data', 'data2/subdata2']  # Lista di subfolder delle distribuzioni
dataset_name = "glaive"

custom_jsonl_dataset_reader = CustomJsonlReader(
    data_folder=dataset_folder,
    recursive=True,
    glob_pattern="**/*.jsonl",
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

custom_jsonl_distribution_reader = CustomJsonlReader(
    data_folder=distribution_folder,
    recursive=True,
    glob_pattern="*.jsonl",
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
    print("🧪 Test configurazione reader personalizzato...")
    
    # Lista file trovati
    #files = distribution_folder.list_files(recursive=True, glob_pattern="**/*.jsonl")
    files = dataset_folder.list_files(recursive=True, glob_pattern="**/*.jsonl")
    print(f"📁 File trovati: {len(files)}")
    
    # Prova a leggere primi documenti con la versione custom (soluzione 2)
    try:
        reader = custom_jsonl_dataset_reader
        #reader = custom_jsonl_distribution_reader
        sample_docs = []
        for i, doc in enumerate(reader()):
            sample_docs.append(doc)

        
        
        print(f"✅ Lettura OK - Documenti esempio: {len(sample_docs)}")
        for i, doc in enumerate(sample_docs):
            print(f"\n--- Documento {i+1} ---")
            print(f"   ID: {doc.id}")
            print(f"   Text preview: {doc.text[:100]}..." if doc.text else "   Text: [VUOTO]")
            print(f"   Metadata: {doc.metadata}")
        
     
            
    except Exception as e:
        print(f"❌ Errore: {e}")
        traceback.print_exc()

# Esegui test
test_reader_configuration()

print("✅ File di riferimento completato!")
'''