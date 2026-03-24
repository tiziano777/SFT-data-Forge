import os
import gzip
import json
from pathlib import Path
from typing import Callable, IO

from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.data import DocumentsPipeline
from datatrove.io import DataFolder

import logging
logger = logging.getLogger(__name__)

BINDED_PROCESSED_DATA_DIR = os.getenv("BINDED_PROCESSED_DATA_DIR")
BINDED_RAW_DATA_DIR = os.getenv("BINDED_RAW_DATA_DIR")

class CustomJsonlWriter(JsonlWriter):
    """
    Writer che salva i documenti in:
        {target_path}/{distribution_relative}/{lang}/{rank:05d}.jsonl.gz

    Il path di output è determinato ESCLUSIVAMENTE da:
      - target_path: path assoluto container della root processed (es. /app/nfs/processed-data/allenai/ai2_arc)
      - distribution_relative: sottocartella relativa della distribuzione (es. ARC-Challenge)
      - _lang: estratto dai metadata di ogni documento

    Non usa _dataset_path dai metadata per costruire path — era la fonte del bug.
    """

    def __init__(
        self,
        target_path: str,
        distribution_relative: str = "",
        compression: str | None = "gzip",
        adapter: Callable = None,
        expand_metadata: bool = False,
        max_file_size: int = -1,
    ):
        """
        Args:
            target_path: path assoluto container di output root
                         es. "/app/nfs/processed-data/allenai/ai2_arc"
            distribution_relative: parte relativa della distribuzione
                         es. "ARC-Challenge"  (può essere vuoto)
        """
        compression = "gzip"

        # Inizializza la classe base con una cartella fittizia — non la usiamo
        # per la scrittura effettiva, ma serve a JsonlWriter per non crashare.
        super().__init__(
            output_folder=DataFolder(str(target_path)),
            output_filename="unused.jsonl.gz",
            compression=compression,
            adapter=adapter,
            expand_metadata=expand_metadata,
            max_file_size=max_file_size,
        )

        self.target_path = Path(target_path)
        self.distribution_relative = distribution_relative.strip("/")

        # File handle aperti: key = str(Path), value = file handle gzip
        self._file_handles: dict[str, IO] = {}

        # Crea la root di output
        self.target_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Path building
    # ------------------------------------------------------------------

    def _output_dir_for_lang(self, lang: str) -> Path:
        """
        Costruisce la directory di output per una lingua:
            target_path / distribution_relative / lang
        """
        if self.distribution_relative:
            return self.target_path / self.distribution_relative / lang
        return self.target_path / lang

    def _output_filepath(self, lang: str, rank: int, base_filename: str) -> Path:
        """
        Percorso completo del file di output:
            {output_dir}/{base_filename}_{rank:05d}.jsonl.gz
        """
        output_dir = self._output_dir_for_lang(lang)
        output_dir.mkdir(parents=True, exist_ok=True)
        stem = base_filename.replace(".jsonl.gz", "").replace("_processed", "")
        return output_dir / f"{stem}_processed_{rank:05d}.jsonl.gz"

    # ------------------------------------------------------------------
    # Document transformation
    # ------------------------------------------------------------------

    def _transform_document(self, document_data: dict, output_subpath: str) -> dict:
        """
        Appiattisce il documento nel formato finale da scrivere su disco.
        data_content viene estratto da metadata.data e merged con i campi core.
        """
        metadata = document_data.get("metadata", {})
        data_content = metadata.get("data", {})

        core = {
            "_dataset_name":  "processed__" + metadata.get("_dataset_name", "").strip(),
            "_dataset_path":  metadata.get("_dataset_path", "").replace(BINDED_RAW_DATA_DIR,BINDED_PROCESSED_DATA_DIR).strip(),
            "_subpath":       output_subpath.strip(),
            "_filename":      metadata.get("_filename", "").strip(),
            "_lang":          metadata.get("_lang", "un").strip(),
            "_id_hash":       metadata.get("_id_hash", "").strip(),
        }
        #logger.info("data_content:", data_content)
        #logger.info("core_metadata:", core)
        return {**data_content, **core}

    # ------------------------------------------------------------------
    # Low-level write
    # ------------------------------------------------------------------

    def _open_file_handler(self, file_path: Path) -> IO:
        return gzip.open(file_path, "wb")

    def _write_to_handle(self, handle: IO, transformed: dict):
        #logger.info(f"✍️ Scrivendo documento ID={transformed}")
        line = json.dumps(transformed, ensure_ascii=False) + "\n"
        handle.write(line.encode("utf-8"))

    # ------------------------------------------------------------------
    # Pipeline entry point
    # ------------------------------------------------------------------

    def __call__(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        return self.run(data, rank, world_size)

    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        stats = {"total": 0, "written": 0, "errors": 0}

        try:
            for document in data:
                stats["total"] += 1
                try:
                    lang = document.metadata.get("_lang", "un") or "un"

                    # Nome file base: usa _filename dal reader se disponibile
                    raw_filename = document.metadata.get("_filename", f"doc_{stats['total']}")
                    base_filename = Path(raw_filename).name
                    # Rimuovi tutte le estensioni (es. file.parquet → file)
                    while "." in base_filename:
                        base_filename = Path(base_filename).stem

                    output_path = self._output_filepath(lang, rank, base_filename)

                    # Calcola il subpath relativo da mostrare nel documento
                    # es.  ARC-Challenge/en
                    if self.distribution_relative:
                        output_subpath = os.path.normpath(f"{self.distribution_relative}/{lang}")
                    else:
                        output_subpath = lang

                    # Prepara il documento da scrivere
                    doc_dict = {
                        "id":       document.id,
                        "text":     document.text,
                        "metadata": dict(document.metadata),
                    }
                    # Aggiorna _filename con il nome finale
                    doc_dict["metadata"]["_filename"] = output_path.name

                    transformed = self._transform_document(doc_dict, output_subpath)

                    # Apri il file handle se non esiste ancora
                    handle_key = str(output_path)
                    if handle_key not in self._file_handles:
                        #logger.info(f"📄 Apertura file: {output_path}", flush=True)
                        self._file_handles[handle_key] = self._open_file_handler(output_path)

                    self._write_to_handle(self._file_handles[handle_key], transformed)
                    stats["written"] += 1

                    yield document

                except Exception as e:
                    logger.info(f"❌ Errore documento {stats['total']}: {e}", flush=True)
                    stats["errors"] += 1
                    continue

        finally:
            logger.info(f"🔒 Chiusura {len(self._file_handles)} file handle...", flush=True)
            for path_str, handle in self._file_handles.items():
                try:
                    handle.close()
                    logger.info(f"✅ Chiuso: {path_str}", flush=True)
                except Exception:
                    pass
            self._file_handles.clear()

            logger.info(
                f"\n--- Report Finale ---\n"
                f"Documenti: {stats['total']} | Scritti: {stats['written']} | Errori: {stats['errors']}",
                flush=True,
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass