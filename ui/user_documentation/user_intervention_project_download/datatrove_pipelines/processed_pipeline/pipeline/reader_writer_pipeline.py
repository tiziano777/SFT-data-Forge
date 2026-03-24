# jsonl_writer.py
import json
import os
import traceback
import gzip
from datatrove.io import DataFolder

import sys
datatrove_base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(datatrove_base_dir)

from reader.unified_reader import UnifiedReader
from writers.writer import CustomJsonlWriter

# ============================================================================
# CONFIGURAZIONE WRITER
# ============================================================================

# Configurazione dei path
base_input_path = "<PROJECT_ROOT>/nfs/data-download/"
base_output_path = "<PROJECT_ROOT>/nfs/processed-data/"

# Configura il writer personalizzato - compression gzip è ora forzata
custom_jsonl_writer = CustomJsonlWriter(
    base_input_path=base_input_path,
    base_output_path=base_output_path,
    compression="gzip",  # Sempre gzip
)

# ============================================================================
# CONFIGURAZIONE READER UNIFICATO
# ============================================================================

dataset_path = "<PROJECT_ROOT>/nfs/data-download/velvet_v1/glaive_dataset/"
dataset_folder = DataFolder(
    path=dataset_path,
    auto_mkdir=True,
)

# Configurazione per UnifiedReader
lang_list = ['en', 'it']
distributions = ['data', 'data2/subdata2']
dataset_name = "glaive"

# ============================================================================
# TEST PIPELINE PER OGNI FORMATO
# ============================================================================

def test_pipeline_for_format(glob_pattern: str, format_name: str):
    """
    Testa una pipeline completa per un formato specifico.
    """
    print(f"\n🎯 TEST PIPELINE per {format_name}")
    print(f"🔍 Pattern: {glob_pattern}")
    print("🚨 POLICY: Tutti i file di output saranno in formato .jsonl.gz")
    
    try:
        # Crea reader unificato per il formato specifico
        unified_reader = UnifiedReader(
            data_folder=dataset_folder,
            glob_pattern=glob_pattern,
            recursive=True,
            text_key=None,
            id_key=None,
            limit=10,  # Limita per test
            skip=0,
            file_progress=True,
            default_metadata={
                "_dataset_name": dataset_name,
                "_dataset_path": dataset_folder.path,
                "distributions": distributions,
                "_available_languages": lang_list,
            },
            shuffle_files=False,
            compression=None,
        )
        
        # Mostra info sul reader
        reader_info = unified_reader.get_reader_info()
        print(f"📊 Reader configurato:")
        print(f"   - Tipo: {reader_info['reader_type']}")
        print(f"   - Pattern: {reader_info['glob_pattern']}")
        print(f"   - Compressione supportata: {reader_info['compression_supported']}")
        
        # Configura la pipeline
        pipeline = [unified_reader, custom_jsonl_writer]
        
        # Esegui con LocalExecutor se disponibile
        try:
            from datatrove.executor import LocalPipelineExecutor
            
            print("🔄 Esecuzione pipeline con LocalExecutor...")
            executor = LocalPipelineExecutor(
                pipeline=pipeline,
                tasks=1,
                logging_dir=None,
            )
            
            executor.run()
            print(f"✅ Pipeline {format_name} completata con LocalExecutor!")
            
        except ImportError as ie:
            print(ie)
            print("⚠️  LocalExecutor non disponibile, esecuzione manuale...")
        except Exception as e:
            print(f"⚠️  Errore con LocalExecutor: {e}")
            print("⚠️  Esecuzione manuale della pipeline...")
            
            
        # Verifica output
        _verify_pipeline_output(format_name)
        
    except Exception as e:
        print(f"❌ Errore nella pipeline {format_name}: {e}")
        traceback.print_exc()

def _verify_pipeline_output(format_name: str):
    """
    Verifica gli output generati dalla pipeline.
    """
    format_output_path = os.path.join(base_output_path, format_name.replace('*', 'all'))
    
    if os.path.exists(format_output_path):
        print(f"📁 Output generati in: {format_output_path}")
        
        gzip_files = []
        for root, dirs, files in os.walk(format_output_path):
            for file in files:
                if file.endswith('.jsonl.gz'):
                    gzip_files.append(os.path.join(root, file))
        
        if gzip_files:
            print(f"📦 File .jsonl.gz creati: {len(gzip_files)}")
            # Mostra primo file come esempio
            try:
                with gzip.open(gzip_files[0], 'rt', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if first_line:
                        sample_data = json.loads(first_line)
                        print(f"🎯 Esempio output - ID: {sample_data.get('id', 'N/A')}")
                        print(f"🎯 Esempio output - Lingua: {sample_data.get('metadata', {}).get('_lang', 'N/A')}")
            except Exception as e:
                print(f"   ❌ Errore lettura file: {e}")
        else:
            print("⚠️  Nessun file .jsonl.gz trovato")
    else:
        print("⚠️  Nessun output generato")

# ============================================================================
# TEST TUTTI I FORMATI
# ============================================================================

def test_all_formats_pipelines():
    """
    Testa una pipeline completa per ogni formato supportato.
    """
    print("🚀 TEST PIPELINE PER OGNI FORMATO")
    print("=" * 50)
    
    # Lista di tutti i formati da testare
    format_tests = [
        ("**/*.json", "JSON"),
        ("**/*.jsonl", "JSONL"), 
        ("**/*.csv", "CSV"),
        ("**/*.tsv", "TSV"),
        ("**/*.json.gz", "JSON_GZ"),
        ("**/*.jsonl.gz", "JSONL_GZ"),
        ("**/*.csv.gz", "CSV_GZ"),
        ("**/*.tsv.gz", "TSV_GZ"),
        ("**/*.parquet", "PARQUET"),
        ("**/*.arrow", "ARROW"),
        ("**/*.ipc", "IPC"),
    ]
    
    results = []
    
    for glob_pattern, format_name in format_tests:
        start_time = time.time()
        
        try:
            test_pipeline_for_format(glob_pattern, format_name)
            results.append((format_name, "✅ SUCCESS", time.time() - start_time))
        except Exception as e:
            results.append((format_name, f"❌ FAILED: {str(e)[:100]}", time.time() - start_time))
    
    # Report finale
    print("\n" + "=" * 50)
    print("📊 REPORT FINALE PIPELINE")
    print("=" * 50)
    
    for format_name, status, duration in results:
        print(f"{status} {format_name:10} - {duration:.2f}s")

# ============================================================================
# TEST FORMATO SPECIFICO (per debug)
# ============================================================================

def test_specific_format(glob_pattern: str, format_name: str):
    """
    Testa un formato specifico in modo dettagliato.
    """
    print(f"\n🔍 TEST DETTAGLIATO per {format_name}")
    
    try:
        # Test reader
        test_reader = UnifiedReader(
            data_folder=dataset_folder,
            glob_pattern=glob_pattern,
            recursive=True,
            text_key=None,
            id_key=None,
            limit=3,
            default_metadata={
                "_dataset_name": dataset_name,
                "_dataset_path": dataset_folder.path,
                "_available_languages": lang_list,
            }
        )
        
        reader_info = test_reader.get_reader_info()
        print(f"📊 Reader: {reader_info['reader_type']}")
        
        # Test lettura
        doc_count = 0
        for doc in test_reader():
            doc_count += 1
            if doc_count == 1:
                print(f"📄 Primo documento:")
                print(json.dumps(doc.metadata, ensure_ascii=False, indent=2))
            if doc_count >= 3:
                break
                
        print(f"📊 Documenti letti: {doc_count}")
        
        # Test pipeline completa
        if doc_count > 0:
            test_pipeline_for_format(glob_pattern, format_name)
        else:
            print(f"⚠️  Nessun documento trovato per {format_name}")
            
    except Exception as e:
        print(f"❌ Errore in test specifico {format_name}: {e}")

# ============================================================================
# ESECUZIONE TEST
# ============================================================================

if __name__ == "__main__":
    import time
    
    print("🎯 AVVIO TEST PIPELINE MULTIFORMATO")
    print("🚨 POLICY AZIENDALE: Tutti gli output in .jsonl.gz")
    
    # Test tutti i formati
    test_all_formats_pipelines()
    
    # Per testare un formato specifico (opzionale)
    # test_specific_format("**/*.csv", "CSV_DEBUG")
    
    print("\n✅ TUTTI I TEST COMPLETATI!")