import gzip
import json
from pathlib import Path
from typing import Callable

from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.data import DocumentsPipeline, Document
from datatrove.io import DataFolder 

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            output_filename="${original_filename}_processed.jsonl.gz",
            compression=compression,
            adapter=adapter,
            expand_metadata=expand_metadata,
            max_file_size=max_file_size,
        )
        self.base_input_path = base_input_path 
        self.base_output_path = Path(base_output_path)
        self._file_handles = {}

    def _prepare_document_data(self, document: Document) -> dict:
        """Prepara i metadati base per il processamento interno."""
        updated_metadata = document.metadata.copy()
        
        if "_dataset_name" not in updated_metadata:
            updated_metadata["_dataset_name"] = self.base_input_path

        return {
            "id": document.id,
            "text": document.text,
            "metadata": updated_metadata
        }

    def _get_output_info_for_document(self, document: Document) -> tuple:
        """Costruisce il path fisico sul disco."""
        try:
            ds_full_name = document.metadata.get("_dataset_name", self.base_input_path)
            lang = document.metadata.get("_lang", "un")
            
            output_dir = self.base_output_path / ds_full_name / lang
            
            original_filename = document.metadata.get("_filename", "data")
            base_filename = Path(original_filename).name.split('.')[0]
            output_filename = f"{base_filename}_processed.jsonl.gz"

            return output_dir, output_filename
        except Exception as e:
            logger.error(f"Error determining output path: {e}")
            return self.base_output_path / "error", "fallback.jsonl.gz"
    
    def _transform_document(self, document: dict, final_filename: str) -> dict:
        """Formatta il JSON finale con tutti i metadati richiesti."""
        meta = document.get("metadata", {})
        data_content = meta.get("data", {})
        
        # 1. Elaborazione nome dataset (slash -> underscore)
        raw_ds_name = meta.get("_dataset_name", "")
        clean_ds_name = raw_ds_name.replace("/", "_")
        
        # 2. Elaborazione dataset_path (solo radice provider, es: .../velvet_v1/allenai)
        first_part = raw_ds_name.split('/')[0] if '/' in raw_ds_name else raw_ds_name
        clean_ds_path = str(self.base_output_path / first_part)
        
        # 3. Elaborazione subpath (es: ai2_arc/ARC-Challenge/en)
        ds_parts = raw_ds_name.split('/')
        subpath_base = "/".join(ds_parts[1:]) if len(ds_parts) > 1 else ""
        lang = meta.get("_lang", "un")
        full_subpath = f"{subpath_base}/{lang}" if subpath_base else lang

        core_meta = {
            "_dataset_name": clean_ds_name,
            "_dataset_path": clean_ds_path,
            "_subpath": full_subpath,
            "_lang": lang,
            "_id_hash": meta.get("_id_hash", ""),
            "_filename": final_filename  # Metadato richiesto: nome completo del file scritto
        }
        
        # Combiniamo i dati eliminando esplicitamente _split se presente
        return {**data_content, **core_meta}

    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        try:
            for document in data:
                doc_to_process = self._prepare_document_data(document)
                
                # Otteniamo info per la scrittura fisica
                output_dir, output_filename = self._get_output_info_for_document(document)
                output_dir.mkdir(parents=True, exist_ok=True)
                
                # Costruiamo il nome file finale (con rank)
                final_filename = f"{output_filename.replace('.jsonl.gz', '')}_{rank:05d}.jsonl.gz"
                output_file_path = output_dir / final_filename
                
                handle_key = str(output_file_path)
                if handle_key not in self._file_handles:
                    self._file_handles[handle_key] = gzip.open(output_file_path, "wb")

                # Trasformazione finale passando il nome del file
                transformed = self._transform_document(doc_to_process, final_filename)
                
                json_line = json.dumps(transformed, ensure_ascii=False) + '\n'
                self._file_handles[handle_key].write(json_line.encode('utf-8'))

                yield document
        finally:
            for handle in self._file_handles.values():
                handle.close()
            self._file_handles.clear()