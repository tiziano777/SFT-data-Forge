"""
Worker per preprocessing parallelo - versione refactored.

Riceve solo URI completi (binded) e metadata essenziali.
Estrae autonomamente tutti i path relativi necessari.
"""
import os
import sys
import json
import uuid
import multiprocessing
from datetime import date
from pathlib import Path

BASE_PREFIX = os.getenv("BASE_PREFIX", "file://")
RAW_DATA_PATH = os.getenv("RAW_DATA_DIR")
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_DIR")

# Setup path per importazioni
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from datatrove.io import DataFolder
from datatrove.executor import LocalPipelineExecutor
from datatrove_pipelines.processed_pipeline.reader.unified_reader import UnifiedReader
from datatrove_pipelines.processed_pipeline.writer.unified_writer import UnifiedWriter
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.entity.table.dataset import Dataset
from data_class.entity.table.distribution import Distribution
from db.impl.postgres.loader.postgres_db_loader import get_db_manager
from utils.extract_glob import generate_filtered_globs
from utils.path_utils import to_internal_path, to_binded_path


def log(message: str):
    """Log con timestamp UTC."""
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


class PathExtractor:
    """
    Estrae informazioni sui path dalle URI.
    
    Input:
        source_dataset_uri      = file:///Users/.../nfs/data-download/velvet_v1/allenai/ai2_arc
        source_distribution_uri = file:///Users/.../nfs/data-download/velvet_v1/allenai/ai2_arc/ARC-Challenge
        processed_base_uri      = file:///Users/.../nfs/processed-data
    
    Output:
        dataset_relative_path   = velvet_v1/allenai/ai2_arc
        distribution_name       = ARC-Challenge
        
        Paths container:
            source_container      = /app/nfs/data-download/velvet_v1/allenai/ai2_arc/ARC-Challenge
            target_base_container = /app/nfs/processed-data
            target_full_container = /app/nfs/processed-data/velvet_v1/allenai/ai2_arc/ARC-Challenge
        
        Paths binded (per DB):
            target_dataset_uri    = file:///Users/.../nfs/processed-data/velvet_v1/allenai/ai2_arc
            target_dist_uri(lang) = file:///Users/.../nfs/processed-data/velvet_v1/allenai/ai2_arc/ARC-Challenge/{lang}
    """
    
    def __init__(self, source_dataset_uri: str, source_distribution_uri: str, processed_base_uri: str):
        self.source_dataset_uri = source_dataset_uri
        self.source_distribution_uri = source_distribution_uri
        self.processed_base_uri = processed_base_uri
        
        # Converti in container scope
        self.source_dataset_container = to_internal_path(source_dataset_uri).replace(BASE_PREFIX, "")
        self.source_dist_container = to_internal_path(source_distribution_uri).replace(BASE_PREFIX, "")
        self.processed_base_container = to_internal_path(processed_base_uri).replace(BASE_PREFIX, "")
        
        # Estrai path relativi
        self._extract_relative_paths()
    
    def _extract_relative_paths(self):
        """
        Estrae i path relativi dalla differenza tra dataset e distribution URI.
        
        Esempio:
            source_dataset_container = /app/nfs/data-download/velvet_v1/allenai/ai2_arc
            source_dist_container    = /app/nfs/data-download/velvet_v1/allenai/ai2_arc/ARC-Challenge
            
            RAW_DATA_PATH = /app/nfs/data-download
            
            → dataset_relative = velvet_v1/allenai/ai2_arc
            → distribution_name = ARC-Challenge
        """
        # 1. Estrai path relativo del dataset rispetto al RAW_DATA_PATH
        dataset_path = Path(self.source_dataset_container)
        raw_base = Path(RAW_DATA_PATH)
        
        try:
            self.dataset_relative_path = str(dataset_path.relative_to(raw_base))
        except ValueError:
            log(f"⚠️ Dataset path non relativo a RAW_DATA_PATH, uso path completo")
            self.dataset_relative_path = str(dataset_path)
        
        # 2. Estrai nome distribution dalla differenza
        dist_path = Path(self.source_dist_container)
        try:
            rel_to_dataset = dist_path.relative_to(dataset_path)
            self.distribution_name = str(rel_to_dataset)
        except ValueError:
            # Fallback: prendi l'ultima parte del path
            self.distribution_name = dist_path.name
        
        log(f"📂 Dataset relative: {self.dataset_relative_path}")
        log(f"📂 Distribution name: {self.distribution_name}")
    
    def get_source_container_path(self) -> str:
        """Path container della distribution sorgente (per lettura)."""
        return self.source_dist_container
    
    def get_target_base_container(self) -> str:
        """Path base container per output (senza dataset_relative)."""
        return self.processed_base_container
    
    def get_target_dataset_container(self) -> str:
        """Path container completo del dataset target."""
        return str(Path(self.processed_base_container) / self.dataset_relative_path)
    
    def get_target_dist_container(self) -> str:
        """Path container completo della distribution target (senza lingua)."""
        return str(Path(self.get_target_dataset_container()) / self.distribution_name)
    
    def get_target_dataset_uri(self) -> str:
        """URI binded del dataset target (per DB)."""
        container_path = self.get_target_dataset_container()
        return to_binded_path(f"{BASE_PREFIX}{container_path}")
    
    def get_target_dist_uri(self, lang: str) -> str:
        """URI binded della distribution target per una specifica lingua (per DB)."""
        container_path = str(Path(self.get_target_dist_container()) / lang)
        return to_binded_path(f"{BASE_PREFIX}{container_path}")


def run_datatrove_pipeline(
    path_extractor: PathExtractor,
    glob_pattern: str,
    default_metadata: dict,
    output_format: str = "jsonl.gz",
):
    """
    Esegue la pipeline datatrove.
    
    Il CustomJsonlWriter riceve:
      - target_path: path assoluto container del dataset target
      - distribution_relative: nome della distribution (es. "ARC-Challenge")
    
    Il writer salva in:
      {target_path}/{distribution_relative}/{lang}/{filename}_{rank:05d}.jsonl.gz
    """
    log("🔄 Configurazione pipeline datatrove...")

    try:
        source_path = path_extractor.get_source_container_path()
        target_dataset_path = path_extractor.get_target_dataset_container()
        distribution_name = path_extractor.distribution_name
        
        # Precondizione 1: input path esiste
        if not os.path.exists(source_path):
            raise RuntimeError(f"Input path non esistente: {source_path}")

        # Precondizione 2: ci sono file da processare
        # Prova il glob fornito; se non trova nulla rileva il formato reale dal disco.
        _KNOWN_GLOBS = ["**/*.parquet", "**/*.jsonl.gz", "**/*.jsonl", "**/*.json",
                        "**/*.arrow", "**/*.ipc", "**/*.csv", "**/*.tsv", "**/*.warc"]

        matching_files = [f for f in Path(source_path).glob(glob_pattern) if f.is_file()]

        if not matching_files and not glob_pattern.startswith("**/"):
            matching_files = [f for f in Path(source_path).glob(f"**/{glob_pattern}") if f.is_file()]
            if matching_files:
                glob_pattern = f"**/{glob_pattern}"

        if not matching_files:
            log(f"⚠️ Nessun file con pattern '{glob_pattern}', auto-rilevamento formato...")
            for candidate in _KNOWN_GLOBS:
                matching_files = [f for f in Path(source_path).glob(candidate) if f.is_file()]
                if matching_files:
                    glob_pattern = candidate
                    log(f"🔍 Formato rilevato: {glob_pattern}")
                    break

        log(f"🔍 Pattern usato: {glob_pattern}")
        log(f"📂 Input valido: {len(matching_files)} file trovati in {source_path}")

        if not matching_files:
            raise RuntimeError(f"Nessun file trovato in {source_path} (nessun formato supportato rilevato)")

        # Crea directory target
        os.makedirs(target_dataset_path, exist_ok=True)
        
        log(f"📁 Target dataset path: {target_dataset_path}")
        log(f"📁 Distribution name: {distribution_name}")

        data_folder = DataFolder(path=source_path, auto_mkdir=False)

        reader = UnifiedReader(
            data_folder=data_folder,
            glob_pattern=glob_pattern,
            recursive=True,
            text_key="text",
            id_key="id",
            limit=-1,
            skip=0,
            file_progress=True,
            default_metadata=default_metadata,
            shuffle_files=False,
            compression=None,
        )
        print(target_dataset_path, distribution_name)
        writer = UnifiedWriter(
            output_format=output_format,
            target_path=target_dataset_path,
            distribution_relative=distribution_name,
        )

        n_cores = multiprocessing.cpu_count()
        n_tasks = min(n_cores * 10, len(matching_files))
        log(f"💻 Utilizzando {n_cores} core, {n_tasks} task")

        executor = LocalPipelineExecutor(
            pipeline=[reader, writer],
            tasks=n_tasks,
            workers=int(n_cores * 0.85),
            logging_dir=None,
        )
        executor.run()

        log("✅ Pipeline datatrove completata!")
        return True

    except Exception as e:
        log(f"❌ Errore durante la pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_processed_dataset(
    source_dataset_id: str,
    path_extractor: PathExtractor,
    db_manager,
):
    """Crea il dataset processato nel DB."""
    log("📝 Creazione dataset processato...")
    try:
        dataset_repo = DatasetRepository(db_manager)
        source_dataset = dataset_repo.get_by_id(source_dataset_id)
        if not source_dataset:
            log(f"❌ Dataset sorgente non trovato: {source_dataset_id}")
            return None

        new_dataset_name = f"processed__{source_dataset.name}"
        new_dataset_uri = path_extractor.get_target_dataset_uri()

        default_languages = ["it", "en", "de", "es", "pt", "fr"]
        processed_languages = source_dataset.languages if source_dataset.languages else default_languages

        processed_dataset = Dataset(
            id=str(uuid.uuid4()),
            uri=new_dataset_uri,
            name=new_dataset_name,
            languages=processed_languages,
            derived_card=source_dataset.derived_card,
            derived_dataset=source_dataset.id,
            dataset_type=source_dataset.dataset_type,
            globs=generate_filtered_globs(to_internal_path(source_dataset.uri).replace(BASE_PREFIX, "")),
            description=f"Processed version of {source_dataset.name}",
            source=source_dataset.source,
            version=source_dataset.version,
            issued=date.today(),
            modified=date.today(),
            license=source_dataset.license,
            step=2,
        )

        try:
            created_dataset = dataset_repo.upsert_by_uri(processed_dataset)
        except Exception as uri_error:
            if "unique constraint" in str(uri_error).lower() and "name" in str(uri_error).lower():
                log("⚠️ Conflitto nome dataset — recupero per nome.")
                created_dataset = dataset_repo.upsert_by_name(processed_dataset)
            else:
                raise

        if created_dataset:
            log(f"✅ Dataset processato creato/recuperato: {created_dataset.name}")
            log(f"   URI: {created_dataset.uri}")
            return created_dataset

        log("❌ Errore creazione dataset")
        return None

    except Exception as e:
        log(f"❌ Errore: {e}")
        import traceback
        traceback.print_exc()
        return None


def create_language_distributions(
    processed_dataset: Dataset,
    source_distribution_id: str,
    path_extractor: PathExtractor,
    db_manager,
    output_format: str = "jsonl.gz",
):
    """Crea le distribuzioni per lingua nel DB."""
    log("🌐 Creazione distribuzioni per lingua...")
    created = []

    try:
        distribution_repo = DistributionRepository(db_manager)
        source_dist = distribution_repo.get_by_id(source_distribution_id)
        if not source_dist:
            log(f"❌ Distribuzione sorgente non trovata: {source_distribution_id}")
            return created

        for lang in processed_dataset.languages:
            log(f"📁 Distribuzione per lingua: {lang}")

            # URI binded per questa lingua
            lang_uri = path_extractor.get_target_dist_uri(lang)
            
            format_meta = UnifiedWriter.FORMAT_METADATA.get(output_format, {
                "glob": "*.jsonl.gz", "format": "jsonl.gz"
            })

            lang_dist = Distribution(
                id=str(uuid.uuid4()),
                uri=lang_uri,
                tokenized_uri=None,
                dataset_id=processed_dataset.id,
                glob=format_meta["glob"],
                format=format_meta["format"],
                name=f"{source_dist.name}__{lang}",
                query=source_dist.query,
                split=source_dist.split,
                derived_from=source_dist.id,
                src_schema=None,
                description=f"{source_dist.description} - {lang.upper()} version",
                lang=lang,
                tags=(source_dist.tags or []) + [f"lang:{lang}"],
                license=source_dist.license,
                version=source_dist.version,
                issued=date.today(),
                modified=date.today(),
                materialized=True,
                step=2,
            )

            created_dist = distribution_repo.upsert_by_uri(lang_dist)
            if created_dist:
                created.append(created_dist)
                log(f"✅ Distribuzione {lang} creata/recuperata")
                log(f"   URI: {created_dist.uri}")
            else:
                log(f"⚠️ Impossibile creare distribuzione per {lang}")

        log(f"✅ {len(created)} distribuzioni create/recuperate")
        return created

    except Exception as e:
        log(f"❌ Errore: {e}")
        import traceback
        traceback.print_exc()
        return created


def verify_output(path_extractor: PathExtractor, output_format: str = "jsonl.gz"):
    """Verifica che l'output sia stato generato correttamente."""
    log("🔍 Verifica output...")
    try:
        check_root = Path(path_extractor.get_target_dist_container())

        if not check_root.exists():
            log(f"❌ Directory output non trovata: {check_root}")
            return False

        ext_pattern = "*.parquet" if output_format == "parquet" else "*.jsonl.gz"
        output_files = list(check_root.rglob(ext_pattern))
        if not output_files:
            log(f"❌ Nessun file {ext_pattern} generato")
            return False

        total_size = sum(f.stat().st_size for f in output_files)
        log(f"✅ {len(output_files)} file — {total_size / (1024 * 1024):.2f} MB")

        # Statistiche per lingua
        lang_stats: dict[str, dict] = {}
        for f in output_files:
            lang = f.parent.name
            lang_stats.setdefault(lang, {"files": 0, "size": 0})
            lang_stats[lang]["files"] += 1
            lang_stats[lang]["size"] += f.stat().st_size

        for lang, s in lang_stats.items():
            log(f"  📁 {lang}: {s['files']} file, {s['size'] / (1024 * 1024):.2f} MB")

        return True

    except Exception as e:
        log(f"❌ Errore verifica: {e}")
        return False


def main():
    if len(sys.argv) < 2:
        log("❌ Parametri mancanti")
        sys.exit(1)

    try:
        params = json.loads(sys.argv[1])

        log("=" * 80)
        log("🚀 AVVIO WORKER DI PREPROCESSING PARALLELO")
        log("=" * 80)

        # Estrai parametri
        source_dataset_uri = params["source_dataset_uri"]
        source_distribution_uri = params["source_distribution_uri"]
        processed_base_uri = params["processed_base_uri"]
        glob_pattern = params["glob_pattern"]
        source_dataset_id = params["source_dataset_id"]
        source_distribution_id = params["source_distribution_id"]
        default_metadata = params["default_metadata"]
        output_format = params.get("output_format", "jsonl.gz")

        log(f"📂 Source dataset URI     : {source_dataset_uri}")
        log(f"📂 Source distribution URI: {source_distribution_uri}")
        log(f"📂 Processed base URI     : {processed_base_uri}")

        # Inizializza extractor
        path_extractor = PathExtractor(
            source_dataset_uri=source_dataset_uri,
            source_distribution_uri=source_distribution_uri,
            processed_base_uri=processed_base_uri,
        )

        log(f"📂 Target dataset path    : {path_extractor.get_target_dataset_container()}")
        log(f"📂 Target dist path       : {path_extractor.get_target_dist_container()}")

        # FASE 1 — Pipeline datatrove
        log("\n" + "=" * 40)
        log("FASE 1: PROCESSING DATATROVE")
        log("=" * 40)

        if not run_datatrove_pipeline(path_extractor, glob_pattern, default_metadata, output_format):
            log("❌ Pipeline fallita")
            sys.exit(1)

        # FASE 2 — Entità database
        log("\n" + "=" * 40)
        log("FASE 2: CREAZIONE ENTITÀ DATABASE")
        log("=" * 40)

        db_manager = get_db_manager()

        processed_dataset = create_processed_dataset(source_dataset_id, path_extractor, db_manager)
        if not processed_dataset:
            log("❌ Creazione dataset fallita")
            sys.exit(1)

        created_distributions = create_language_distributions(
            processed_dataset, source_distribution_id, path_extractor, db_manager, output_format
        )
        if not created_distributions:
            log("⚠️ Nessuna distribuzione creata")

        # FASE 3 — Verifica
        log("\n" + "=" * 40)
        log("FASE 3: VERIFICA OUTPUT")
        log("=" * 40)

        ok = verify_output(path_extractor, output_format)

        log("\n" + "=" * 80)
        if ok:
            log("✅ PREPROCESSING COMPLETATO CON SUCCESSO!")
        else:
            log("⚠️ PREPROCESSING COMPLETATO CON WARNING")
        log("=" * 80)
        log(f"📊 Dataset          : {processed_dataset.name}")
        log(f"📊 Dataset URI      : {processed_dataset.uri}")
        log(f"🌐 Distribuzioni    : {len(created_distributions)}")
        log(f"📂 Output container : {path_extractor.get_target_dist_container()}")

    except Exception as e:
        log(f"❌ ERRORE CRITICO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()