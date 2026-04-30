import gzip
import json
import logging
import os
import sys
from abc import abstractmethod
from pathlib import Path
from typing import Callable, IO

from datatrove.data import DocumentsPipeline, Document
from utils.path_utils import to_binded_path

logger = logging.getLogger(__name__)

# Importa serializzazione robusta
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from utils.serializer import process_record_for_json
except ImportError as e:
    logger.warning(f"Impossibile importare serializer: {e}")
    def process_record_for_json(obj):
        """Fallback se serializer non disponibile."""
        return obj


class BaseCustomWriter:
    """
    Classe base astratta per i writer del mapped pipeline.

    Le sottoclassi devono implementare:
        FILE_EXTENSION (class var) - es. ".jsonl.gz" o ".parquet"
        _open_file_handler(file_path) -> handle
        _write_to_handle(handle, transformed_dict)

    Le sottoclassi possono sovrascrivere:
        _close_handles() - per cleanup custom (es. flush batch Parquet)
    """

    FILE_EXTENSION: str = ""

    def __init__(
        self,
        base_input_path: str,
        base_output_path: str,
        compression: str | None = None,
        adapter: Callable = None,
        expand_metadata: bool = False,
        max_file_size: int = -1,
    ):
        self.base_input_path = Path(base_input_path)
        self.base_output_path = Path(base_output_path)
        self._file_handles = {}
        self.base_output_path.mkdir(parents=True, exist_ok=True)

    def _transform_document(self, document_dict: dict) -> dict:
        """Estrae il contenuto core da 'data' e aggiorna i metadati di sistema."""
        meta = document_dict.get("metadata", {})
        output = meta.get("data", document_dict.copy()).copy()

        # Aggiornamento metadati di sistema
        ds_name = meta.get("_dataset_name", "")
        output["_dataset_name"] = ds_name if ds_name.startswith("mapped__") else f"mapped__{ds_name}"
        output["_dataset_path"] = to_binded_path(meta.get("_dataset_path", ""))
        output["_filename"] = meta.get("_filename", "") # Riceve il valore già aggiornato in run()
        output["_subpath"] = meta.get("_subpath", "")
        output["_lang"] = meta.get("_lang", "un")
        output["_id_hash"] = meta.get("_id_hash", "")

        # Rimozione chiavi ridondanti o spurie
        for key in ["file_path", "data", "metadata"]:
            output.pop(key, None)

        return output

    def _prepare_document_data(self, document: Document) -> dict:
        """Inizializza i metadati per la pipeline di scrittura."""
        updated_metadata = document.metadata.copy()
        target_base = Path(self.base_output_path)

        if target_base.name == updated_metadata.get("_dataset_name", "").split("_")[-1]:
            updated_metadata["_dataset_path"] = str(target_base.parent)
        else:
            updated_metadata["_dataset_path"] = str(target_base)

        return {
            "id": document.id,
            "text": document.text,
            "metadata": updated_metadata
        }

    def _get_output_info_for_document(self, metadata: dict) -> tuple:
        """
        Determina il percorso di output mantenendo la struttura relativa.
        NON include directory di lingua nel path finale.
        """
        try:
            distribution_path = metadata.get("_subpath", "")
            base = self.base_output_path
            dataset_name = base.name

            # Se distribution_path è vuoto o '.', usiamo base
            if not distribution_path or distribution_path == '.':
                output_dir = base
            else:
                # Mantieni l'intero percorso originale, senza filtraggi
                output_dir = base / distribution_path

            # Crea la directory
            output_dir.mkdir(parents=True, exist_ok=True)

            # Gestione filename
            original_filename = metadata.get("_filename", "data")
            base_name = Path(original_filename).stem.split('.')[0]
            output_filename = f"{base_name}_mapped{self.FILE_EXTENSION}"

            print(f"✅ Output FINALE: {output_dir / output_filename}")
            print(f"   (corrisponde all'URI: file://{output_dir / output_filename})")

            return output_dir, output_filename

        except Exception as e:
            print(f"❌ Errore: {e}")
            import traceback
            traceback.print_exc()
            return self.base_output_path, f"fallback{self.FILE_EXTENSION}"

    @abstractmethod
    def _open_file_handler(self, file_path: Path):
        ...

    @abstractmethod
    def _write_to_handle(self, handle, transformed: dict):
        ...

    def _close_handles(self):
        """Chiusura default per file handle semplici (es. gzip)."""
        for handle in self._file_handles.values():
            try:
                handle.close()
            except Exception:
                pass
        self._file_handles.clear()

    def __call__(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1):
        return self.run(data, rank, world_size)

    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        """Loop principale della pipeline per la gestione dei documenti e sharding."""
        stats = {"total": 0, "written": 0, "errors": 0}

        try:
            for document in data:
                stats["total"] += 1
                try:
                    document_data = self._prepare_document_data(document)

                    # Calcolo path e gestione nome file univoco (sharding)
                    output_dir, output_filename = self._get_output_info_for_document(document_data["metadata"])
                    output_dir.mkdir(parents=True, exist_ok=True)

                    stem = output_filename.split('.')[0]
                    final_filename = f"{stem}_{rank:05d}{self.FILE_EXTENSION}"
                    file_path = output_dir / final_filename

                    # Allineamento cruciale: il metadato deve corrispondere al file fisico
                    document_data['metadata']['_filename'] = final_filename

                    handle_key = str(file_path)
                    if handle_key not in self._file_handles:
                        self._file_handles[handle_key] = self._open_file_handler(file_path)

                    transformed = self._transform_document(document_data)
                    self._write_to_handle(self._file_handles[handle_key], transformed)
                    stats["written"] += 1
                    yield document

                except Exception as e:
                    logger.error(
                        f"[DOC_ERROR] Documento {stats['total']} fallito (rank={rank}). "
                        f"ID: {document.id}, Lang: {document.metadata.get('_lang', 'N/A')}. "
                        f"Error: {e}",
                        exc_info=True
                    )
                    stats["errors"] += 1
                    continue

        finally:
            logger.info(f"Chiusura {len(self._file_handles)} file handle (rank={rank})...")
            self._close_handles()
            logger.info(
                f"[RANK_{rank}] Report: Total={stats['total']}, Written={stats['written']}, Errors={stats['errors']}"
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class CustomJsonlWriter(BaseCustomWriter):
    """
    Writer che salva i documenti in formato JSONL compresso gzip.
    Pattern output: {base_output_path}/{_subpath}/{base_name}_mapped_{rank:05d}.jsonl.gz
    """

    FILE_EXTENSION = ".jsonl.gz"

    def __init__(
        self,
        base_input_path: str,
        base_output_path: str,
        compression: str | None = "gzip",
        adapter: Callable = None,
        expand_metadata: bool = False,
        max_file_size: int = -1,
    ):
        super().__init__(
            base_input_path=base_input_path,
            base_output_path=base_output_path,
            compression=compression,
            adapter=adapter,
            expand_metadata=expand_metadata,
            max_file_size=max_file_size,
        )

    def _open_file_handler(self, file_path: Path) -> IO:
        return gzip.open(file_path, "wb")

    def _write_to_handle(self, handle: IO, transformed: dict):
        """Scrivi un record trasformato, con serializzazione robusta."""
        try:
            # Serializza ricorsivamente per gestire tipi complessi
            serialized = process_record_for_json(transformed)

            # JSON dump con fallback
            try:
                json_data = json.dumps(serialized, ensure_ascii=False) + '\n'
            except Exception as json_err:
                logger.error(
                    f"[JSON_DUMP_FALLBACK] Fallimento json.dumps su record. "
                    f"Keys: {list(serialized.keys()) if isinstance(serialized, dict) else 'N/A'}. "
                    f"Error: {json_err}"
                )
                json_data = json.dumps({"_serialization_error": str(serialized)}, ensure_ascii=False) + '\n'

            handle.write(json_data.encode('utf-8'))

        except Exception as write_err:
            logger.error(
                f"[WRITE_HANDLE_CRITICAL] Errore durante write. "
                f"Record keys: {list(transformed.keys()) if isinstance(transformed, dict) else 'N/A'}. "
                f"Error: {write_err}",
                exc_info=True
            )
            raise
