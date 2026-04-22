import os
import gzip
import json
from pathlib import Path
from typing import Callable, IO
from abc import abstractmethod

from datatrove.data import DocumentsPipeline

import logging
logger = logging.getLogger(__name__)

BINDED_PROCESSED_DATA_DIR = os.getenv("BINDED_PROCESSED_DATA_DIR")
BINDED_RAW_DATA_DIR = os.getenv("BINDED_RAW_DATA_DIR")


class BaseCustomWriter:
    """
    Classe base astratta per i writer della processed pipeline.
    Gestisce il raggruppamento file per lang/filename, la trasformazione documenti,
    e il loop run() basato su generatore.

    Le sottoclassi devono implementare:
        FILE_EXTENSION (class var)
        _open_file_handler(file_path) -> handle
        _write_to_handle(handle, transformed_dict)
    """

    FILE_EXTENSION: str = ""

    def __init__(
        self,
        target_path: str,
        distribution_relative: str = "",
    ):
        self.target_path = Path(target_path)
        self.distribution_relative = distribution_relative.strip("/")
        self._file_handles: dict[str, IO] = {}
        self.target_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Path building
    # ------------------------------------------------------------------

    def _output_dir_for_lang(self, lang: str) -> Path:
        if self.distribution_relative:
            return self.target_path / self.distribution_relative / lang
        return self.target_path / lang

    def _output_filepath(self, lang: str, rank: int, base_filename: str) -> Path:
        output_dir = self._output_dir_for_lang(lang)
        output_dir.mkdir(parents=True, exist_ok=True)
        stem = base_filename
        for ext in [".jsonl.gz", ".jsonl", ".parquet", ".json", ".csv"]:
            stem = stem.replace(ext, "")
        stem = stem.replace("_processed", "")
        return output_dir / f"{stem}_processed_{rank:05d}{self.FILE_EXTENSION}"

    # ------------------------------------------------------------------
    # Document transformation
    # ------------------------------------------------------------------

    def _transform_document(self, document_data: dict, output_subpath: str) -> dict:
        metadata = document_data.get("metadata", {})
        data_content = metadata.get("data", {})

        core = {
            "_dataset_name":  "processed__" + metadata.get("_dataset_name", "").strip(),
            "_dataset_path":  metadata.get("_dataset_path", "").replace(BINDED_RAW_DATA_DIR, BINDED_PROCESSED_DATA_DIR).strip(),
            "_subpath":       output_subpath.strip(),
            "_filename":      metadata.get("_filename", "").strip(),
            "_lang":          metadata.get("_lang", "un").strip(),
            "_id_hash":       metadata.get("_id_hash", "").strip(),
        }
        return {**data_content, **core}

    # ------------------------------------------------------------------
    # Abstract methods per le sottoclassi
    # ------------------------------------------------------------------

    @abstractmethod
    def _open_file_handler(self, file_path: Path):
        ...

    @abstractmethod
    def _write_to_handle(self, handle, transformed: dict):
        ...

    def _close_handles(self):
        for path_str, handle in self._file_handles.items():
            try:
                handle.close()
                logger.info(f"Chiuso: {path_str}")
            except Exception:
                pass
        self._file_handles.clear()

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

                    raw_filename = document.metadata.get("_filename", f"doc_{stats['total']}")
                    base_filename = Path(raw_filename).name
                    while "." in base_filename:
                        base_filename = Path(base_filename).stem

                    output_path = self._output_filepath(lang, rank, base_filename)

                    if self.distribution_relative:
                        output_subpath = os.path.normpath(f"{self.distribution_relative}/{lang}")
                    else:
                        output_subpath = lang

                    doc_dict = {
                        "id":       document.id,
                        "text":     document.text,
                        "metadata": dict(document.metadata),
                    }
                    doc_dict["metadata"]["_filename"] = output_path.name

                    transformed = self._transform_document(doc_dict, output_subpath)

                    handle_key = str(output_path)
                    if handle_key not in self._file_handles:
                        self._file_handles[handle_key] = self._open_file_handler(output_path)

                    self._write_to_handle(self._file_handles[handle_key], transformed)
                    stats["written"] += 1

                    yield document

                except Exception as e:
                    logger.info(f"Errore documento {stats['total']}: {e}")
                    stats["errors"] += 1
                    continue

        finally:
            logger.info(f"Chiusura {len(self._file_handles)} file handle...")
            self._close_handles()

            logger.info(
                f"\n--- Report Finale ---\n"
                f"Documenti: {stats['total']} | Scritti: {stats['written']} | Errori: {stats['errors']}",
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class CustomJsonlWriter(BaseCustomWriter):
    """
    Writer che salva i documenti in formato JSONL compresso gzip:
        {target_path}/{distribution_relative}/{lang}/{base_filename}_processed_{rank:05d}.jsonl.gz
    """

    FILE_EXTENSION = ".jsonl.gz"

    def __init__(
        self,
        target_path: str,
        distribution_relative: str = "",
        compression: str | None = "gzip",
        adapter: Callable = None,
        expand_metadata: bool = False,
        max_file_size: int = -1,
    ):
        super().__init__(target_path=target_path, distribution_relative=distribution_relative)

    def _open_file_handler(self, file_path: Path) -> IO:
        return gzip.open(file_path, "wb")

    def _write_to_handle(self, handle: IO, transformed: dict):
        line = json.dumps(transformed, ensure_ascii=False) + "\n"
        handle.write(line.encode("utf-8"))
