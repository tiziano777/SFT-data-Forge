import gzip
import json
from pathlib import Path
from typing import Callable, IO

from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.data import DocumentsPipeline, Document
from datatrove.io import DataFolder 
from utils.path_utils import to_binded_path

class CustomJsonlWriter(JsonlWriter):
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
            output_folder=DataFolder(str(base_output_path)),
            output_filename="mapped__${original_filename}.jsonl.gz",
            compression="gzip",
            adapter=adapter,
            expand_metadata=expand_metadata,
            max_file_size=max_file_size,
        )
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

    def _open_file_handler(self, file_path: Path) -> IO:
        return gzip.open(file_path, "wb")

    def _write(self, document_dict: dict, file_handler: IO, _filename: str):
        """Scrittura fisica su disco del JSON trasformato."""
        with self.track_time():
            transformed_document = self._transform_document(document_dict)
            json_data = json.dumps(transformed_document, ensure_ascii=False) + '\n'
            file_handler.write(json_data.encode('utf-8'))
    
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
            
            #print(f"\n🔍 DEBUG:")
            #print(f"  - Base output path: {base}")
            #print(f"  - Distribution path raw: '{distribution_path}'")
            #print(f"  - Dataset name: '{dataset_name}'")
            
            # Se distribution_path è vuoto o '.', usiamo base
            if not distribution_path or distribution_path == '.':
                output_dir = base
                #print(f"  ➡ Caso base: {output_dir}")
            else:
                # Mantieni l'intero percorso originale, senza filtraggi
                output_dir = base / distribution_path
                #print(f"  ➡ Output con path completo: {output_dir}")
            
            # Crea la directory
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Gestione filename
            original_filename = metadata.get("_filename", "data")
            base_name = Path(original_filename).stem.split('.')[0]
            output_filename = f"{base_name}_mapped.jsonl.gz"
            
            print(f"✅ Output FINALE: {output_dir / output_filename}")
            print(f"   (corrisponde all'URI: file://{output_dir / output_filename})")
            
            return output_dir, output_filename
            
        except Exception as e:
            print(f"❌ Errore: {e}")
            import traceback
            traceback.print_exc()
            return self.base_output_path, "fallback.jsonl.gz"
    
    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        """Loop principale della pipeline per la gestione dei documenti e sharding."""
        try:
            for document in data:
                document_data = self._prepare_document_data(document)

                # Calcolo path e gestione nome file univoco (sharding)
                output_dir, output_filename = self._get_output_info_for_document(document_data["metadata"])
                output_dir.mkdir(parents=True, exist_ok=True)

                stem = output_filename.split('.')[0]
                final_filename = f"{stem}_{rank:05d}.jsonl.gz"
                file_path = output_dir / final_filename

                # Allineamento cruciale: il metadato deve corrispondere al file fisico
                document_data['metadata']['_filename'] = final_filename

                handle_key = str(file_path)
                if handle_key not in self._file_handles:
                    self._file_handles[handle_key] = self._open_file_handler(file_path)

                self._write(document_data, self._file_handles[handle_key], str(file_path))
                yield document
        finally:
            for handle in self._file_handles.values():
                handle.close()
            self._file_handles.clear()

